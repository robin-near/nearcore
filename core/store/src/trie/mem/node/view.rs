use super::{MemTrieNodePtr, MemTrieNodeView};
use crate::trie::mem::arena::{Arena, ArenaPtr};
use crate::trie::mem::flexible_data::children::ChildrenView;
use crate::trie::mem::node::loading::MemTrieNodePtrMut;
use crate::trie::mem::node::{InputMemTrieNode, MemTrieNodeId};
use crate::trie::TRIE_COSTS;
use crate::{NibbleSlice, RawTrieNode, RawTrieNodeWithSize, Trie, TrieChanges, TrieStorage};
use borsh::BorshSerialize;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::state::FlatStateValue;
use std::collections::HashMap;
use std::rc::Rc;

pub type UpdatedMemTrieNodeId = usize;

// Reference to either node in big trie or in small temporary trie.
#[derive(Debug, Clone)]
pub enum UpdatedNodeRef {
    // Old(MemTrieNodePtr<'a>),
    Old(usize), // raw ptr
    New(UpdatedMemTrieNodeId),
}

// Structure to handle new temporarily created nodes.
#[derive(Debug, Clone)]
pub enum UpdatedMemTrieNode {
    // Fancy edge case. Used when we create an empty child and descend to it to create new node.
    Empty,
    Leaf { extension: Box<[u8]>, value: Vec<u8> },
    Extension { extension: Box<[u8]>, child: UpdatedNodeRef },
    // United Branch&BranchWithValue because it is easier to write `match`.
    Branch { children: Vec<Option<UpdatedNodeRef>>, value: Option<Vec<u8>> },
}

pub struct MemTrieUpdate<'a> {
    arena: &'a Arena,
    // for values
    storage: Rc<dyn TrieStorage>,
    // offset -> refcount
    pub refcount_changes: HashMap<usize, i32>,
    pub value_removals: Vec<Vec<u8>>,
    pub nodes_storage: Vec<Option<UpdatedMemTrieNode>>,
}

fn convert_children(view: ChildrenView) -> Vec<Option<UpdatedNodeRef>> {
    let mut children = vec![];
    let mut j = 0;
    for i in 0..16 {
        children.push(if view.mask & (1 << i) != 0 {
            let child = Some(UpdatedNodeRef::Old(
                MemTrieNodePtr::from(view.children.read_ptr_at(j)).ptr.raw_offset(),
            ));
            j += 8;
            child
        } else {
            None
        });
    }
    children
}

enum FlattenNodesCrumb {
    Entering,
    AtChild(usize),
    Exiting,
}

impl<'a> MemTrieUpdate<'a> {
    pub fn new(arena: &'a Arena, storage: Rc<dyn TrieStorage>) -> Self {
        Self {
            arena,
            storage,
            refcount_changes: Default::default(),
            value_removals: Default::default(),
            nodes_storage: vec![],
        }
    }

    pub fn destroy(&mut self, index: UpdatedMemTrieNodeId) -> UpdatedMemTrieNode {
        self.nodes_storage.get_mut(index).unwrap().take().unwrap()
    }

    // Replace node in place due to changes
    pub fn store_at(&mut self, index: UpdatedMemTrieNodeId, node: UpdatedMemTrieNode) {
        self.nodes_storage[index] = Some(node);
    }

    // Create new node
    pub fn store(&mut self, node: UpdatedMemTrieNode) -> UpdatedMemTrieNodeId {
        let index = self.nodes_storage.len();
        self.nodes_storage.push(Some(node));
        index
    }

    pub fn move_node_to_mutable(&mut self, node: usize) -> usize {
        if node == usize::MAX {
            self.store(UpdatedMemTrieNode::Empty)
        } else {
            *self.refcount_changes.entry(node).or_insert_with(|| 0) -= 1;
            let node = MemTrieNodePtr::from(self.arena.memory().ptr(node));
            let updated_node = node.view().to_updated(self.storage.as_ref());
            self.store(updated_node)
        }
    }

    fn get_value(&self, value: FlatStateValue) -> Vec<u8> {
        match value {
            FlatStateValue::Inlined(v) => v,
            FlatStateValue::Ref(value_ref) => {
                self.storage.retrieve_raw_bytes(&value_ref.hash).unwrap().to_vec()
            }
        }
    }

    // INSERT/DELETE LOGIC

    // Starting from the root, insert `key` to trie, modifying nodes on the way from top to bottom.
    // Combination of different operations:
    // * Split some existing key into two, as new branch is created
    // * Move node from big trie to temporary one
    // * Create new node
    // * Descend to node if it corresponds to subslice of key
    // ! No need to return anything. `root_id` stays as is
    // todo: consider already dropping hash & mem usage. but maybe idc
    // todo: what are trie changes for values?
    pub fn insert(&mut self, root_id: UpdatedMemTrieNodeId, key: &[u8], value: Vec<u8>) {
        let mut value = Some(value);
        let mut node_id = root_id;
        let mut partial = NibbleSlice::new(key);

        loop {
            // Destroy node as it will be changed anyway.
            let node = self.destroy(node_id);
            match node {
                UpdatedMemTrieNode::Empty => {
                    let extension: Vec<_> = partial.encoded(true).into_vec();
                    let leaf_node = UpdatedMemTrieNode::Leaf {
                        extension: extension.into_boxed_slice(),
                        value: value.take().unwrap(),
                    };
                    self.store_at(node_id, leaf_node);
                    break;
                }
                UpdatedMemTrieNode::Branch { children, value: old_value } => {
                    if partial.is_empty() {
                        // Store value here.
                        if let Some(value) = old_value {
                            self.value_removals.push(value);
                        }
                        // store value somehow
                        // can't just move `value` because it happens inside a loop :(
                        let new_node = UpdatedMemTrieNode::Branch {
                            children: children.clone(),
                            value: value.take(),
                        };
                        self.store_at(node_id, new_node);
                        break;
                    } else {
                        let mut new_children = children.clone();
                        let child = &mut new_children[partial.at(0) as usize];
                        let new_node_id = match child.take() {
                            Some(UpdatedNodeRef::Old(node_ptr)) => {
                                self.move_node_to_mutable(node_ptr)
                            }
                            Some(UpdatedNodeRef::New(node_id)) => node_id,
                            None => self.store(UpdatedMemTrieNode::Empty),
                        };
                        *child = Some(UpdatedNodeRef::New(new_node_id));
                        self.store_at(
                            node_id,
                            UpdatedMemTrieNode::Branch {
                                children: new_children,
                                value: old_value.clone(),
                            },
                        );
                        node_id = new_node_id;
                        partial = partial.mid(1);
                        continue;
                    }
                }
                UpdatedMemTrieNode::Leaf { extension: key, value: old_value } => {
                    let existing_key = NibbleSlice::from_encoded(key.as_ref()).0;
                    let common_prefix = partial.common_prefix(&existing_key);
                    if common_prefix == existing_key.len() && common_prefix == partial.len() {
                        // Equivalent leaf, rewrite the value.
                        self.value_removals.push(old_value);
                        let node = UpdatedMemTrieNode::Leaf {
                            extension: key.clone(),
                            value: value.take().unwrap(),
                        };
                        self.store_at(node_id, node);
                        break;
                    } else if common_prefix == 0 {
                        // split leaf to branch.
                        let mut children = vec![None; 16];
                        let idx = existing_key.at(0) as usize;
                        let new_extension: Vec<_> = existing_key.mid(1).encoded(true).into_vec();
                        let new_leaf = UpdatedMemTrieNode::Leaf {
                            extension: new_extension.into_boxed_slice(),
                            value: old_value.clone(),
                        };
                        let new_node_id = self.store(new_leaf);
                        children[idx] = Some(UpdatedNodeRef::New(new_node_id));
                        // no value in current branch, as common prefix is 0
                        let branch_node = UpdatedMemTrieNode::Branch { children, value: None };
                        self.store_at(node_id, branch_node);
                        // on next iteration, we will add the second child!
                        continue;
                    } else if common_prefix == existing_key.len() {
                        // Current leaf becomes an extension.
                        // Extension ends with a branch with old value.
                        // This branch has only 1 child as value can't be stored in extension.
                        // We will continue from this branch and add a leaf to it.
                        let branch_node = UpdatedMemTrieNode::Branch {
                            children: vec![None; 16],
                            value: Some(old_value.clone()),
                        };
                        let new_node_id = self.store(branch_node);
                        let extension: Vec<_> = existing_key.encoded(false).into_vec();
                        let updated_node = UpdatedMemTrieNode::Extension {
                            extension: extension.into_boxed_slice(),
                            child: UpdatedNodeRef::New(new_node_id),
                        };
                        self.store_at(node_id, updated_node);
                        node_id = new_node_id;
                        partial = partial.mid(common_prefix);
                    } else {
                        // Opposite case: add new extension, current leaf will be its child.
                        let extension: Vec<_> =
                            existing_key.mid(common_prefix).encoded(true).into_vec();
                        let leaf_node = UpdatedMemTrieNode::Leaf {
                            extension: extension.into_boxed_slice(),
                            value: old_value.clone(),
                        };
                        let new_node_id = self.store(leaf_node);
                        let extension: Vec<_> =
                            partial.encoded_leftmost(common_prefix, false).into_vec();
                        let node = UpdatedMemTrieNode::Extension {
                            extension: extension.into_boxed_slice(),
                            child: UpdatedNodeRef::New(new_node_id),
                        };
                        self.store_at(node_id, node);
                        node_id = new_node_id;
                        partial = partial.mid(common_prefix);
                        continue;
                    }
                }
                UpdatedMemTrieNode::Extension { extension: key, child, .. } => {
                    let existing_key = NibbleSlice::from_encoded(&key).0;
                    let common_prefix = partial.common_prefix(&existing_key);
                    if common_prefix == 0 {
                        // Split Extension to Branch
                        let idx = existing_key.at(0);
                        let child = if existing_key.len() == 1 {
                            child.clone()
                        } else {
                            let extension: Vec<_> = existing_key.mid(1).encoded(false).into_vec();
                            let inner_child = UpdatedMemTrieNode::Extension {
                                extension: extension.into_boxed_slice(),
                                child: child.clone(),
                            };
                            UpdatedNodeRef::New(self.store(inner_child))
                        };

                        let mut children = vec![None; 16];
                        children[idx as usize] = Some(child);
                        let branch_node = UpdatedMemTrieNode::Branch { children, value: None };
                        self.store_at(node_id, branch_node);
                        // Start over from the same position.
                        continue;
                    } else if common_prefix == existing_key.len() {
                        // Dereference child and descend into it.
                        let child = match child {
                            UpdatedNodeRef::Old(ptr) => self.move_node_to_mutable(ptr.clone()),
                            UpdatedNodeRef::New(node_id) => node_id.clone(),
                        };
                        let node = UpdatedMemTrieNode::Extension {
                            extension: key.clone(),
                            child: UpdatedNodeRef::New(child),
                        };
                        self.store_at(node_id, node);
                        node_id = child;
                        partial = partial.mid(common_prefix);
                        continue;
                    } else {
                        // Partially shared prefix. Convert to shorter extension and descend into it.
                        // On the next step, branch will be created.
                        let extension: Vec<_> =
                            existing_key.mid(common_prefix).encoded(false).into_vec();
                        let inner_child_node = UpdatedMemTrieNode::Extension {
                            extension: extension.into_boxed_slice(),
                            child: child.clone(),
                        };
                        let inner_child_node_id = self.store(inner_child_node);
                        let extension: Vec<_> =
                            existing_key.encoded_leftmost(common_prefix, false).into_vec();
                        let child_node = UpdatedMemTrieNode::Extension {
                            extension: extension.into_boxed_slice(),
                            child: UpdatedNodeRef::New(inner_child_node_id),
                        };
                        self.store_at(node_id, child_node);
                        node_id = inner_child_node_id;
                        partial = partial.mid(common_prefix);
                        continue;
                    }
                }
            }
        }
    }

    // Delete
    pub fn delete(&mut self, root_id: UpdatedMemTrieNodeId, key: &[u8]) {
        let mut node_id = root_id;
        let mut partial = NibbleSlice::new(key);
        let mut path = vec![];

        loop {
            path.push(node_id);
            let node = self.destroy(node_id);

            match node {
                // finished
                UpdatedMemTrieNode::Empty => {
                    self.store_at(node_id, UpdatedMemTrieNode::Empty);
                    break;
                }
                UpdatedMemTrieNode::Leaf { extension: key, value } => {
                    if NibbleSlice::from_encoded(&key).0 == partial {
                        self.value_removals.push(value);
                        self.store_at(node_id, UpdatedMemTrieNode::Empty);
                        break;
                    } else {
                        panic!("key = {:?}, partial = {:?}, don't match", key, partial);
                        // ??? throw an error because key does not exist?
                    }
                }
                UpdatedMemTrieNode::Branch { children, value } => {
                    if partial.is_empty() {
                        let value = match value {
                            Some(value) => value,
                            None => {
                                panic!("no value for key {:?}", key);
                            }
                        };
                        self.value_removals.push(value);
                        // there must be at least 1 child, otherwise it shouldn't be a branch.
                        // could be even 2, but there is some weird case when 1
                        assert!(children.iter().filter(|x| x.is_some()).count() >= 1);
                        self.store_at(
                            node_id,
                            UpdatedMemTrieNode::Branch { children, value: None },
                        );
                        // if needed, branch will be squashed on the way back
                        break;
                    } else {
                        let mut children = children.clone();
                        let child = &mut children[partial.at(0) as usize];
                        let node_ref = match child.take() {
                            Some(node) => node,
                            None => {
                                // again, wtf? need to panic.
                                panic!("no value for key {:?}", key);
                            }
                        };
                        let new_node_id = match node_ref {
                            UpdatedNodeRef::Old(ptr) => self.move_node_to_mutable(ptr.clone()),
                            UpdatedNodeRef::New(node_id) => node_id,
                        };
                        *child = Some(UpdatedNodeRef::New(new_node_id));
                        self.store_at(node_id, UpdatedMemTrieNode::Branch { children, value });

                        node_id = new_node_id;
                        partial = partial.mid(1);
                        continue;
                    }
                }
                UpdatedMemTrieNode::Extension { extension: key, child } => {
                    let (common_prefix, existing_len) = {
                        let existing_key = NibbleSlice::from_encoded(&key).0;
                        (existing_key.common_prefix(&partial), existing_key.len())
                    };
                    if common_prefix == existing_len {
                        let new_node_id = match child {
                            UpdatedNodeRef::Old(ptr) => self.move_node_to_mutable(ptr.clone()),
                            UpdatedNodeRef::New(node_id) => node_id.clone(),
                        };
                        self.store_at(
                            node_id,
                            UpdatedMemTrieNode::Extension {
                                extension: key.clone(),
                                child: UpdatedNodeRef::New(new_node_id),
                            },
                        );

                        node_id = new_node_id;
                        partial = partial.mid(existing_len);
                        continue;
                    } else {
                        panic!("can't go down by {} in partial = {:?}", key.len(), partial);
                    }
                }
            }
        }

        self.squash_nodes(path);
    }

    fn squash_nodes(&mut self, path: Vec<UpdatedMemTrieNodeId>) {
        // Induction by correctness of path suffix.
        for node_id in path.into_iter().rev() {
            let node = self.destroy(node_id);
            match node {
                // First two cases - nothing to squash, just come up.
                UpdatedMemTrieNode::Empty => {
                    self.store_at(node_id, UpdatedMemTrieNode::Empty);
                }
                UpdatedMemTrieNode::Leaf { extension, value } => {
                    self.store_at(node_id, UpdatedMemTrieNode::Leaf { extension, value });
                }
                UpdatedMemTrieNode::Branch { mut children, value } => {
                    for child in children.iter_mut() {
                        if let Some(UpdatedNodeRef::New(child_node_id)) = child {
                            if let UpdatedMemTrieNode::Empty =
                                self.nodes_storage[*child_node_id as usize].as_ref().unwrap()
                            {
                                *child = None;
                            }
                        }
                    }
                    let num_children = children.iter().filter(|node| node.is_some()).count();
                    if num_children == 0 {
                        if let Some(value) = value {
                            // should it be this way? idk
                            let empty: Vec<_> = NibbleSlice::new(&[]).encoded(true).into_vec();
                            let leaf_node = UpdatedMemTrieNode::Leaf {
                                extension: empty.into_boxed_slice(),
                                value,
                            };
                            self.store_at(node_id, leaf_node);
                        } else {
                            self.store_at(node_id, UpdatedMemTrieNode::Empty);
                        }
                    } else if num_children == 1 && value.is_none() {
                        let (idx, child) = children
                            .iter()
                            .enumerate()
                            .filter(|(_, node)| node.is_some())
                            .next()
                            .unwrap();
                        let child = child.as_ref().unwrap();
                        let key: Vec<_> = NibbleSlice::new(&[(idx << 4) as u8])
                            .encoded_leftmost(1, false)
                            .into_vec();
                        self.extend_child(node_id, key, child);
                    } else {
                        self.store_at(node_id, UpdatedMemTrieNode::Branch { children, value });
                    }
                }
                UpdatedMemTrieNode::Extension { extension, child } => {
                    self.extend_child(node_id, extension.to_vec(), &child);
                }
            }
        }
    }

    // If some branch has only one child, it may end up being squashed to extension.
    // Then we need to append some existing child to a key and put it into `node_id`.
    fn extend_child(
        &mut self,
        node_id: UpdatedMemTrieNodeId,
        key: Vec<u8>,
        child: &UpdatedNodeRef,
    ) {
        let child = match child {
            UpdatedNodeRef::Old(ptr) => self.move_node_to_mutable(ptr.clone()),
            UpdatedNodeRef::New(node_id) => node_id.clone(),
        };
        let child_node = self.destroy(child);
        match child_node {
            // not sure about that... maybe we do need to kill the key, but not sure if we shouldn't panic
            UpdatedMemTrieNode::Empty => self.store_at(node_id, UpdatedMemTrieNode::Empty),
            // Make extended leaf
            UpdatedMemTrieNode::Leaf { extension: child_key, value } => {
                let child_key = NibbleSlice::from_encoded(&child_key).0;
                let key: Vec<_> =
                    NibbleSlice::from_encoded(&key).0.merge_encoded(&child_key, true).into_vec();
                self.store_at(
                    node_id,
                    UpdatedMemTrieNode::Leaf { extension: key.into_boxed_slice(), value },
                )
            }
            // Nothing to squash! Just append Branch to new Extension.
            node @ UpdatedMemTrieNode::Branch { .. } => {
                self.store_at(child, node);
                self.store_at(
                    node_id,
                    UpdatedMemTrieNode::Extension {
                        extension: key.into_boxed_slice(),
                        child: UpdatedNodeRef::New(child),
                    },
                );
            }
            // Join two Extensions into one.
            UpdatedMemTrieNode::Extension { extension, child: inner_child } => {
                let child_key = NibbleSlice::from_encoded(&extension).0;
                let key: Vec<_> =
                    NibbleSlice::from_encoded(&key).0.merge_encoded(&child_key, false).into_vec();
                self.store_at(
                    node_id,
                    UpdatedMemTrieNode::Extension {
                        extension: key.into_boxed_slice(),
                        child: inner_child,
                    },
                );
            }
        }
    }

    // For now it doesn't recompute hashes yet.
    // Just prepare DFS-ordered list of nodes for further application.
    pub fn flatten_nodes(
        self,
        root_id: UpdatedMemTrieNodeId,
    ) -> (
        Vec<UpdatedMemTrieNodeId>,
        Vec<Vec<u8>>,
        HashMap<usize, i32>,
        Vec<Option<UpdatedMemTrieNode>>,
    ) {
        let Self { refcount_changes, value_removals, nodes_storage, .. } = self;

        let mut stack: Vec<(UpdatedMemTrieNodeId, FlattenNodesCrumb)> = Vec::new();
        stack.push((root_id, FlattenNodesCrumb::Entering));
        let mut ordered_nodes = vec![];
        'outer: while let Some((node, position)) = stack.pop() {
            let updated_node = nodes_storage[node].as_ref().unwrap();
            match updated_node {
                UpdatedMemTrieNode::Empty => {
                    // panic?!
                    continue;
                }
                UpdatedMemTrieNode::Branch { children, .. } => match position {
                    FlattenNodesCrumb::Entering => {
                        stack.push((node, FlattenNodesCrumb::AtChild(0)));
                        continue;
                    }
                    FlattenNodesCrumb::AtChild(mut i) => {
                        while i < 16 {
                            match children[i].clone() {
                                Some(UpdatedNodeRef::New(child_node_id)) => {
                                    stack.push((node, FlattenNodesCrumb::AtChild(i + 1)));
                                    stack.push((child_node_id, FlattenNodesCrumb::Entering));
                                    continue 'outer;
                                }
                                _ => {}
                            }
                            i += 1;
                        }
                        // flatten value
                    }
                    FlattenNodesCrumb::Exiting => unreachable!(),
                },
                UpdatedMemTrieNode::Extension { child, .. } => match position {
                    FlattenNodesCrumb::Entering => match child {
                        UpdatedNodeRef::New(child_id) => {
                            stack.push((node, FlattenNodesCrumb::Exiting));
                            stack.push((*child_id, FlattenNodesCrumb::Entering));
                            continue;
                        }
                        UpdatedNodeRef::Old(_) => {}
                    },
                    FlattenNodesCrumb::Exiting => {}
                    _ => unreachable!(),
                },
                UpdatedMemTrieNode::Leaf { .. } => {
                    // flatten value
                }
            }
            ordered_nodes.push(node);
        }

        (ordered_nodes, value_removals, refcount_changes, nodes_storage)
    }

    // Prepare Trie changes and update Arena right now.
    // Yeah it is not atomic, but enough for testing for now.
    pub fn prepare_changes(
        id_refcount_changes: HashMap<usize, i32>,
        value_removals: Vec<Vec<u8>>,
        mut nodes_storage: Vec<Option<UpdatedMemTrieNode>>,
        old_root: CryptoHash,
        nodes: Vec<UpdatedMemTrieNodeId>,
        arena: &mut Arena,
    ) -> TrieChanges {
        let mut mapped_nodes: HashMap<UpdatedMemTrieNodeId, MemTrieNodeId> = Default::default();

        let map_node = |node: UpdatedNodeRef,
                        map: &HashMap<UpdatedMemTrieNodeId, MemTrieNodeId>|
         -> MemTrieNodeId {
            match node {
                UpdatedNodeRef::New(node) => map.get(&node).unwrap().clone(),
                UpdatedNodeRef::Old(ptr) => MemTrieNodeId::from(ptr),
            }
        };

        let mut refcount_changes: HashMap<CryptoHash, (Vec<u8>, i32)> = Default::default();
        for (node_id, rc) in id_refcount_changes {
            let node = MemTrieNodePtr::from(arena.memory().ptr(node_id));
            let view = node.view();
            let hash = view.node_hash();
            let raw_node = view.to_raw_trie_node_with_size(); // can we skip this? rc < 0, value not needed
            let (_, old_rc) =
                refcount_changes.entry(hash).or_insert_with(|| (raw_node.try_to_vec().unwrap(), 0));
            *old_rc -= rc;
        }
        for value in value_removals.into_iter() {
            let (_, old_rc) = refcount_changes.entry(hash(&value)).or_insert_with(|| (value, 0));
            *old_rc -= 1;
        }

        let mut last_node_hash = old_root; // last node must be new root, lol
        for node_id in nodes.into_iter() {
            let node = nodes_storage.get_mut(node_id).unwrap().take().unwrap();
            let node = match node {
                UpdatedMemTrieNode::Empty => unreachable!(),
                UpdatedMemTrieNode::Branch { children, value } => {
                    let children = children
                        .into_iter()
                        .map(|child| child.map(|c| map_node(c, &mapped_nodes)))
                        .collect();
                    match value {
                        Some(value) => {
                            let (_, old_rc) = refcount_changes
                                .entry(hash(&value))
                                .or_insert_with(|| (value.clone(), 0));
                            *old_rc += 1;
                            InputMemTrieNode::BranchWithValue {
                                children,
                                value: FlatStateValue::on_disk(&value),
                            }
                        }
                        None => InputMemTrieNode::Branch { children },
                    }
                }
                UpdatedMemTrieNode::Extension { extension, child } => {
                    InputMemTrieNode::Extension { extension, child: map_node(child, &mapped_nodes) }
                }
                UpdatedMemTrieNode::Leaf { extension, value } => {
                    let (_, old_rc) =
                        refcount_changes.entry(hash(&value)).or_insert_with(|| (value.clone(), 0));
                    *old_rc += 1;
                    InputMemTrieNode::Leaf { value: FlatStateValue::on_disk(&value), extension }
                }
            };
            let mem_node_id = MemTrieNodeId::new(arena, node);
            mapped_nodes.insert(node_id, mem_node_id);

            let mut node_ptr = mem_node_id.as_ptr_mut(arena.memory_mut());
            node_ptr.compute_hash_recursively();
            let node = node_ptr.as_const();
            let view = node.view();
            let hash = view.node_hash();
            let raw_node = view.to_raw_trie_node_with_size();
            let (_, rc) =
                refcount_changes.entry(hash).or_insert_with(|| (raw_node.try_to_vec().unwrap(), 0));
            *rc += 1;

            last_node_hash = hash;
        }

        let (insertions, deletions) = Trie::convert_to_insertions_and_deletions(refcount_changes);

        TrieChanges { old_root, new_root: last_node_hash, insertions, deletions }
    }
}

impl<'a> MemTrieNodeView<'a> {
    pub fn node_hash(&self) -> CryptoHash {
        match self {
            Self::Leaf { .. } => {
                let node = self.clone().to_raw_trie_node_with_size();
                hash(&node.try_to_vec().unwrap())
            }
            Self::Extension { hash, .. }
            | Self::Branch { hash, .. }
            | Self::BranchWithValue { hash, .. } => *hash,
        }
    }

    pub fn to_raw_trie_node_with_size(&self) -> RawTrieNodeWithSize {
        match self {
            Self::Leaf { value, extension } => {
                let node = RawTrieNode::Leaf(
                    extension.as_slice().to_vec(),
                    value.clone().to_flat_value().to_value_ref(),
                );
                RawTrieNodeWithSize { node, memory_usage: self.memory_usage() }
            }
            Self::Extension { extension, child, .. } => {
                let view = child.view();
                let node = RawTrieNode::Extension(extension.as_slice().to_vec(), view.node_hash());
                RawTrieNodeWithSize { node, memory_usage: self.memory_usage() }
            }
            Self::Branch { children, .. } => {
                let node = RawTrieNode::BranchNoValue(children.to_children());
                RawTrieNodeWithSize { node, memory_usage: self.memory_usage() }
            }
            Self::BranchWithValue { children, value, .. } => {
                let node = RawTrieNode::BranchWithValue(
                    value.to_flat_value().to_value_ref(),
                    children.to_children(),
                );
                RawTrieNodeWithSize { node, memory_usage: self.memory_usage() }
            }
        }
    }

    pub fn memory_usage(&self) -> u64 {
        match self {
            Self::Leaf { value, extension } => {
                TRIE_COSTS.node_cost
                    + extension.len() as u64 * TRIE_COSTS.byte_of_key
                    + value.len() as u64 * TRIE_COSTS.byte_of_value
                    + TRIE_COSTS.node_cost // yes, twice.
            }
            Self::Extension { memory_usage, .. }
            | Self::Branch { memory_usage, .. }
            | Self::BranchWithValue { memory_usage, .. } => {
                // Memory usage is computed after loading is complete.
                // For that, we use the to_raw_trie_node_with_size code path.
                // So make sure that's the case by checking here.
                assert!(*memory_usage != 0, "memory_usage is not computed yet");
                *memory_usage
            }
        }
    }

    pub(crate) fn iter_children<'b>(&'b self) -> Box<dyn Iterator<Item = MemTrieNodePtr<'a>> + 'b> {
        match self {
            MemTrieNodeView::Leaf { .. } => Box::new(std::iter::empty()),
            MemTrieNodeView::Extension { child, .. } => Box::new(std::iter::once(*child)),
            MemTrieNodeView::Branch { children, .. }
            | MemTrieNodeView::BranchWithValue { children, .. } => Box::new(children.iter()),
        }
    }

    pub fn to_updated(self, storage: &dyn TrieStorage) -> UpdatedMemTrieNode {
        match self {
            Self::Leaf { extension, value } => UpdatedMemTrieNode::Leaf {
                extension: extension.as_slice().to_vec().into_boxed_slice(),
                value: value.to_value(storage),
            },
            Self::Branch { children, .. } => {
                UpdatedMemTrieNode::Branch { children: convert_children(children), value: None }
            }
            Self::BranchWithValue { children, value, .. } => UpdatedMemTrieNode::Branch {
                children: convert_children(children),
                value: Some(value.to_value(storage)),
            },
            Self::Extension { extension, child, .. } => UpdatedMemTrieNode::Extension {
                extension: extension.as_slice().to_vec().into_boxed_slice(),
                child: UpdatedNodeRef::Old(child.ptr.raw_offset()),
            },
        }
    }
}
