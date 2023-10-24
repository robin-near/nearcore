use super::arena::ArenaMemory;
use super::flexible_data::children::ChildrenView;
use super::metrics::MEM_TRIE_NUM_NODES_CREATED_FROM_UPDATES;
use super::node::{InputMemTrieNode, MemTrieNodeId, MemTrieNodeView};
use super::MemTries;
use crate::trie::{Children, MemTrieChanges, TRIE_COSTS};
use crate::{NibbleSlice, RawTrieNode, RawTrieNodeWithSize, Trie, TrieChanges};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::state::FlatStateValue;
use near_primitives::types::BlockHeight;
use std::collections::HashMap;

pub type UpdatedMemTrieNodeId = usize;

// Reference to either node in big trie or in small temporary trie.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OldOrNewNodeId {
    Old(MemTrieNodeId),
    New(UpdatedMemTrieNodeId),
}

// Structure to handle new temporarily created nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdatedMemTrieNode {
    // Fancy edge case. Used when we create an empty child and descend to it to create new node.
    Empty,
    Leaf { extension: Box<[u8]>, value: FlatStateValue },
    Extension { extension: Box<[u8]>, child: OldOrNewNodeId },
    // United Branch&BranchWithValue because it is easier to write `match`.
    Branch { children: [Option<OldOrNewNodeId>; 16], value: Option<FlatStateValue> },
}

pub struct MemTrieUpdate<'a> {
    root: Option<MemTrieNodeId>,
    arena: &'a ArenaMemory,
    shard_uid: String, // for metrics
    pub id_refcount_changes: HashMap<MemTrieNodeId, i32>,
    pub value_changes: HashMap<CryptoHash, i32>,
    pub new_values: HashMap<CryptoHash, Vec<u8>>,
    pub nodes_storage: Vec<Option<UpdatedMemTrieNode>>,
}

impl UpdatedMemTrieNode {
    fn convert_children_to_updated(view: ChildrenView) -> [Option<OldOrNewNodeId>; 16] {
        let mut children = [None; 16];
        for i in 0..16 {
            if let Some(child) = view.get(i) {
                children[i] = Some(OldOrNewNodeId::Old(child.id()));
            }
        }
        children
    }

    pub fn from_existing_node_view(view: MemTrieNodeView<'_>) -> Self {
        match view {
            MemTrieNodeView::Leaf { extension, value } => Self::Leaf {
                extension: extension.raw_slice().to_vec().into_boxed_slice(),
                value: value.to_flat_value(),
            },
            MemTrieNodeView::Branch { children, .. } => {
                Self::Branch { children: Self::convert_children_to_updated(children), value: None }
            }
            MemTrieNodeView::BranchWithValue { children, value, .. } => Self::Branch {
                children: Self::convert_children_to_updated(children),
                value: Some(value.to_flat_value()),
            },
            MemTrieNodeView::Extension { extension, child, .. } => Self::Extension {
                extension: extension.raw_slice().to_vec().into_boxed_slice(),
                child: OldOrNewNodeId::Old(child.id()),
            },
        }
    }
}

enum FlattenNodesCrumb {
    Entering,
    AtChild(usize),
    Exiting,
}

impl<'a> MemTrieUpdate<'a> {
    pub fn new(root: Option<MemTrieNodeId>, arena: &'a ArenaMemory, shard_uid: String) -> Self {
        let mut trie_update = Self {
            root,
            arena,
            shard_uid,
            id_refcount_changes: Default::default(),
            value_changes: Default::default(),
            new_values: Default::default(),
            nodes_storage: vec![],
        };
        assert_eq!(trie_update.convert_existing_to_updated(root), 0usize);
        trie_update
    }

    pub fn take_node(&mut self, index: UpdatedMemTrieNodeId) -> UpdatedMemTrieNode {
        self.nodes_storage.get_mut(index).unwrap().take().expect("Node taken twice")
    }

    pub fn place_node(&mut self, index: UpdatedMemTrieNodeId, node: UpdatedMemTrieNode) {
        assert!(self.nodes_storage[index].is_none(), "Node placed twice");
        self.nodes_storage[index] = Some(node);
    }

    pub fn new_updated_node(&mut self, node: UpdatedMemTrieNode) -> UpdatedMemTrieNodeId {
        let index = self.nodes_storage.len();
        self.nodes_storage.push(Some(node));
        index
    }

    pub fn convert_existing_to_updated(
        &mut self,
        node: Option<MemTrieNodeId>,
    ) -> UpdatedMemTrieNodeId {
        match node {
            None => self.new_updated_node(UpdatedMemTrieNode::Empty),
            Some(node) => {
                *self.id_refcount_changes.entry(node).or_insert_with(|| 0) -= 1;
                self.new_updated_node(UpdatedMemTrieNode::from_existing_node_view(
                    node.as_ptr(self.arena).view(),
                ))
            }
        }
    }

    pub fn ensure_updated(&mut self, node: OldOrNewNodeId) -> UpdatedMemTrieNodeId {
        match node {
            OldOrNewNodeId::Old(node_id) => self.convert_existing_to_updated(Some(node_id)),
            OldOrNewNodeId::New(node_id) => node_id,
        }
    }

    pub fn notify_maybe_new_value(&mut self, value: Vec<u8>) {
        let hash = hash(&value);
        self.new_values.insert(hash, value);
    }

    /// Inserts the given key value pair into the trie.
    ///
    /// Starting from the root, insert `key` to trie, modifying nodes on the way from top to bottom.
    /// Combination of different operations:
    /// * Split some existing key into two, as new branch is created
    /// * Move node from big trie to temporary one
    /// * Create new node
    /// * Descend to node if it corresponds to subslice of key
    pub fn insert(&mut self, key: &[u8], flat_value: FlatStateValue) {
        let mut node_id = 0; // root
        let mut partial = NibbleSlice::new(key);
        let value_ref = flat_value.to_value_ref();

        loop {
            // Destroy node as it will be changed anyway.
            let node = self.take_node(node_id);
            match node {
                UpdatedMemTrieNode::Empty => {
                    // There was no node here, create a new leaf.
                    self.place_node(
                        node_id,
                        UpdatedMemTrieNode::Leaf {
                            extension: partial.encoded(true).into_vec().into_boxed_slice(),
                            value: flat_value,
                        },
                    );
                    *self.value_changes.entry(value_ref.hash).or_insert_with(|| 0) += 1;
                    break;
                }
                UpdatedMemTrieNode::Branch { children, value: old_value } => {
                    if partial.is_empty() {
                        // This branch node is exactly where the value should be added.
                        if let Some(value) = old_value {
                            *self
                                .value_changes
                                .entry(value.to_value_ref().hash)
                                .or_insert_with(|| 0) -= 1;
                        }
                        // store value somehow
                        // can't just move `value` because it happens inside a loop :(
                        self.place_node(
                            node_id,
                            UpdatedMemTrieNode::Branch { children, value: Some(flat_value) },
                        );
                        *self.value_changes.entry(value_ref.hash).or_insert_with(|| 0) += 1;
                        break;
                    } else {
                        // Continue descending into the branch.
                        let mut new_children = children;
                        let child = &mut new_children[partial.at(0) as usize];
                        let new_node_id = match child.take() {
                            Some(node_id) => self.ensure_updated(node_id),
                            None => self.new_updated_node(UpdatedMemTrieNode::Empty),
                        };
                        *child = Some(OldOrNewNodeId::New(new_node_id));
                        self.place_node(
                            node_id,
                            UpdatedMemTrieNode::Branch { children: new_children, value: old_value },
                        );
                        node_id = new_node_id;
                        partial = partial.mid(1);
                        continue;
                    }
                }
                UpdatedMemTrieNode::Leaf { extension, value: old_value } => {
                    let existing_key = NibbleSlice::from_encoded(&extension).0;
                    let common_prefix = partial.common_prefix(&existing_key);
                    if common_prefix == existing_key.len() && common_prefix == partial.len() {
                        // Equivalent leaf, rewrite the value.
                        *self
                            .value_changes
                            .entry(old_value.to_value_ref().hash)
                            .or_insert_with(|| 0) -= 1;
                        self.place_node(
                            node_id,
                            UpdatedMemTrieNode::Leaf { extension, value: flat_value },
                        );
                        *self.value_changes.entry(value_ref.hash).or_insert_with(|| 0) += 1;
                        break;
                    } else if common_prefix == 0 {
                        // split leaf to branch.
                        let mut children = [None; 16];
                        let branch_node = if existing_key.is_empty() {
                            // yeah it can be empty... if branch leads directly to value :(
                            UpdatedMemTrieNode::Branch { children, value: Some(old_value) }
                        } else {
                            let idx = existing_key.at(0) as usize;
                            let new_extension = existing_key.mid(1).encoded(true).into_vec();
                            let new_node_id = self.new_updated_node(UpdatedMemTrieNode::Leaf {
                                extension: new_extension.into_boxed_slice(),
                                value: old_value,
                            });
                            children[idx] = Some(OldOrNewNodeId::New(new_node_id));
                            // no value in current branch, as common prefix is 0
                            UpdatedMemTrieNode::Branch { children, value: None }
                        };
                        self.place_node(node_id, branch_node);
                        // on next iteration, we will add the second child!
                        continue;
                    } else if common_prefix == existing_key.len() {
                        // Current leaf becomes an extension.
                        // Extension ends with a branch with old value.
                        // This branch has only 1 child as value can't be stored in extension.
                        // We will continue from this branch and add a leaf to it.
                        let new_node_id = self.new_updated_node(UpdatedMemTrieNode::Branch {
                            children: [None; 16],
                            value: Some(old_value),
                        });
                        let updated_node = UpdatedMemTrieNode::Extension {
                            extension: existing_key.encoded(false).into_vec().into_boxed_slice(),
                            child: OldOrNewNodeId::New(new_node_id),
                        };
                        self.place_node(node_id, updated_node);
                        node_id = new_node_id;
                        partial = partial.mid(common_prefix);
                    } else {
                        // Opposite case: add new extension, current leaf will be its child.
                        let new_node_id = self.new_updated_node(UpdatedMemTrieNode::Leaf {
                            extension: existing_key
                                .mid(common_prefix)
                                .encoded(true)
                                .into_vec()
                                .into_boxed_slice(),
                            value: old_value,
                        });
                        let node = UpdatedMemTrieNode::Extension {
                            extension: partial
                                .encoded_leftmost(common_prefix, false)
                                .into_vec()
                                .into_boxed_slice(),
                            child: OldOrNewNodeId::New(new_node_id),
                        };
                        self.place_node(node_id, node);
                        node_id = new_node_id;
                        partial = partial.mid(common_prefix);
                        continue;
                    }
                }
                UpdatedMemTrieNode::Extension { extension, child: old_child, .. } => {
                    let existing_key = NibbleSlice::from_encoded(&extension).0;
                    let common_prefix = partial.common_prefix(&existing_key);
                    if common_prefix == 0 {
                        // Split Extension to Branch
                        let idx = existing_key.at(0);
                        let child = if existing_key.len() == 1 {
                            old_child
                        } else {
                            let inner_child = UpdatedMemTrieNode::Extension {
                                extension: existing_key
                                    .mid(1)
                                    .encoded(false)
                                    .into_vec()
                                    .into_boxed_slice(),
                                child: old_child,
                            };
                            OldOrNewNodeId::New(self.new_updated_node(inner_child))
                        };

                        let mut children = [None; 16];
                        children[idx as usize] = Some(child);
                        let branch_node = UpdatedMemTrieNode::Branch { children, value: None };
                        self.place_node(node_id, branch_node);
                        // Start over from the same position.
                        continue;
                    } else if common_prefix == existing_key.len() {
                        // Dereference child and descend into it.
                        let child = self.ensure_updated(old_child);
                        let node = UpdatedMemTrieNode::Extension {
                            extension,
                            child: OldOrNewNodeId::New(child),
                        };
                        self.place_node(node_id, node);
                        node_id = child;
                        partial = partial.mid(common_prefix);
                        continue;
                    } else {
                        // Partially shared prefix. Convert to shorter extension and descend into it.
                        // On the next step, branch will be created.
                        let inner_child_node = UpdatedMemTrieNode::Extension {
                            extension: existing_key
                                .mid(common_prefix)
                                .encoded(false)
                                .into_vec()
                                .into_boxed_slice(),
                            child: old_child.clone(),
                        };
                        let inner_child_node_id = self.new_updated_node(inner_child_node);
                        let child_node = UpdatedMemTrieNode::Extension {
                            extension: existing_key
                                .encoded_leftmost(common_prefix, false)
                                .into_vec()
                                .into_boxed_slice(),
                            child: OldOrNewNodeId::New(inner_child_node_id),
                        };
                        self.place_node(node_id, child_node);
                        node_id = inner_child_node_id;
                        partial = partial.mid(common_prefix);
                        continue;
                    }
                }
            }
        }
    }

    pub fn delete(&mut self, key: &[u8]) {
        let mut node_id = 0; // root
        let mut partial = NibbleSlice::new(key);
        let mut path = vec![];

        loop {
            path.push(node_id);
            let node = self.take_node(node_id);

            match node {
                UpdatedMemTrieNode::Empty => {
                    self.place_node(node_id, UpdatedMemTrieNode::Empty);
                    break;
                }
                UpdatedMemTrieNode::Leaf { extension, value } => {
                    if NibbleSlice::from_encoded(&extension).0 == partial {
                        *self
                            .value_changes
                            .entry(value.to_value_ref().hash)
                            .or_insert_with(|| 0) -= 1;
                        self.place_node(node_id, UpdatedMemTrieNode::Empty);
                        break;
                    } else {
                        self.place_node(node_id, UpdatedMemTrieNode::Leaf { extension, value });
                        break;
                    }
                }
                UpdatedMemTrieNode::Branch { children: old_children, value } => {
                    if partial.is_empty() {
                        let value = match value {
                            Some(value) => value,
                            None => {
                                self.place_node(
                                    node_id,
                                    UpdatedMemTrieNode::Branch { children: old_children, value },
                                );
                                break;
                            }
                        };
                        *self
                            .value_changes
                            .entry(value.to_value_ref().hash)
                            .or_insert_with(|| 0) -= 1;
                        // there must be at least 1 child, otherwise it shouldn't be a branch.
                        // could be even 2, but there is some weird case when 1
                        assert!(old_children.iter().filter(|x| x.is_some()).count() >= 1);
                        self.place_node(
                            node_id,
                            UpdatedMemTrieNode::Branch { children: old_children, value: None },
                        );
                        // if needed, branch will be squashed on the way back
                        break;
                    } else {
                        let mut new_children = old_children.clone();
                        let child = &mut new_children[partial.at(0) as usize];
                        let node_ref = match child.take() {
                            Some(node) => node,
                            None => {
                                self.place_node(
                                    node_id,
                                    UpdatedMemTrieNode::Branch { children: old_children, value },
                                );
                                break;
                            }
                        };
                        let new_node_id = self.ensure_updated(node_ref);
                        *child = Some(OldOrNewNodeId::New(new_node_id));
                        self.place_node(
                            node_id,
                            UpdatedMemTrieNode::Branch { children: new_children, value },
                        );

                        node_id = new_node_id;
                        partial = partial.mid(1);
                        continue;
                    }
                }
                UpdatedMemTrieNode::Extension { extension, child } => {
                    let (common_prefix, existing_len) = {
                        let extension_nibbles = NibbleSlice::from_encoded(&extension).0;
                        (extension_nibbles.common_prefix(&partial), extension_nibbles.len())
                    };
                    if common_prefix == existing_len {
                        let new_node_id = self.ensure_updated(child);
                        self.place_node(
                            node_id,
                            UpdatedMemTrieNode::Extension {
                                extension,
                                child: OldOrNewNodeId::New(new_node_id),
                            },
                        );

                        node_id = new_node_id;
                        partial = partial.mid(existing_len);
                        continue;
                    } else {
                        self.place_node(
                            node_id,
                            UpdatedMemTrieNode::Extension { extension, child },
                        );
                        break;
                    }
                }
            }
        }

        self.squash_nodes(path);
    }

    fn squash_nodes(&mut self, path: Vec<UpdatedMemTrieNodeId>) {
        // Induction by correctness of path suffix.
        for node_id in path.into_iter().rev() {
            let node = self.take_node(node_id);
            match node {
                // First two cases - nothing to squash, just come up.
                UpdatedMemTrieNode::Empty => {
                    self.place_node(node_id, UpdatedMemTrieNode::Empty);
                }
                UpdatedMemTrieNode::Leaf { extension, value } => {
                    self.place_node(node_id, UpdatedMemTrieNode::Leaf { extension, value });
                }
                UpdatedMemTrieNode::Branch { mut children, value } => {
                    for child in children.iter_mut() {
                        if let Some(OldOrNewNodeId::New(child_node_id)) = child {
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
                            let leaf_node = UpdatedMemTrieNode::Leaf {
                                extension: NibbleSlice::new(&[])
                                    .encoded(true)
                                    .into_vec()
                                    .into_boxed_slice(),
                                value,
                            };
                            self.place_node(node_id, leaf_node);
                        } else {
                            self.place_node(node_id, UpdatedMemTrieNode::Empty);
                        }
                    } else if num_children == 1 && value.is_none() {
                        let (idx, child) = children
                            .into_iter()
                            .enumerate()
                            .find_map(|(idx, node)| node.map(|node| (idx, node)))
                            .unwrap();
                        let key = NibbleSlice::new(&[(idx << 4) as u8])
                            .encoded_leftmost(1, false)
                            .into_vec();
                        self.extend_child(node_id, key, child);
                    } else {
                        self.place_node(node_id, UpdatedMemTrieNode::Branch { children, value });
                    }
                }
                UpdatedMemTrieNode::Extension { extension, child } => {
                    self.extend_child(node_id, extension.to_vec(), child);
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
        child_id: OldOrNewNodeId,
    ) {
        let child_id = self.ensure_updated(child_id);
        let child_node = self.take_node(child_id);
        match child_node {
            // not sure about that... maybe we do need to kill the key, but not sure if we shouldn't panic
            UpdatedMemTrieNode::Empty => self.place_node(node_id, UpdatedMemTrieNode::Empty),
            // Make extended leaf
            UpdatedMemTrieNode::Leaf { extension: child_key, value } => {
                let child_key = NibbleSlice::from_encoded(&child_key).0;
                let key =
                    NibbleSlice::from_encoded(&key).0.merge_encoded(&child_key, true).into_vec();
                self.place_node(
                    node_id,
                    UpdatedMemTrieNode::Leaf { extension: key.into_boxed_slice(), value },
                )
            }
            // Nothing to squash! Just append Branch to new Extension.
            node @ UpdatedMemTrieNode::Branch { .. } => {
                self.place_node(child_id, node);
                self.place_node(
                    node_id,
                    UpdatedMemTrieNode::Extension {
                        extension: key.into_boxed_slice(),
                        child: OldOrNewNodeId::New(child_id),
                    },
                );
            }
            // Join two Extensions into one.
            UpdatedMemTrieNode::Extension { extension, child: inner_child } => {
                let child_key = NibbleSlice::from_encoded(&extension).0;
                let key =
                    NibbleSlice::from_encoded(&key).0.merge_encoded(&child_key, false).into_vec();
                self.place_node(
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
    pub fn flatten_nodes(self, block_height: BlockHeight) -> TrieChanges {
        let Self {
            root,
            arena,
            shard_uid,
            id_refcount_changes,
            value_changes,
            new_values,
            nodes_storage,
        } = self;
        let root_id = 0;
        MEM_TRIE_NUM_NODES_CREATED_FROM_UPDATES
            .with_label_values(&[&shard_uid])
            .inc_by(nodes_storage.len() as u64);
        let mut stack: Vec<(UpdatedMemTrieNodeId, FlattenNodesCrumb)> = Vec::new();
        stack.push((root_id, FlattenNodesCrumb::Entering));
        let mut ordered_nodes = vec![];
        'outer: while let Some((node_id, position)) = stack.pop() {
            let updated_node = nodes_storage[node_id].as_ref().unwrap();
            match updated_node {
                UpdatedMemTrieNode::Empty => {
                    assert_eq!(node_id, 0); // only root can be empty
                    continue;
                }
                UpdatedMemTrieNode::Branch { children, .. } => match position {
                    FlattenNodesCrumb::Entering => {
                        stack.push((node_id, FlattenNodesCrumb::AtChild(0)));
                        continue;
                    }
                    FlattenNodesCrumb::AtChild(mut i) => {
                        while i < 16 {
                            if let Some(OldOrNewNodeId::New(child_node_id)) = children[i].clone() {
                                stack.push((node_id, FlattenNodesCrumb::AtChild(i + 1)));
                                stack.push((child_node_id, FlattenNodesCrumb::Entering));
                                continue 'outer;
                            }
                            i += 1;
                        }
                    }
                    FlattenNodesCrumb::Exiting => unreachable!(),
                },
                UpdatedMemTrieNode::Extension { child, .. } => match position {
                    FlattenNodesCrumb::Entering => match child {
                        OldOrNewNodeId::New(child_id) => {
                            stack.push((node_id, FlattenNodesCrumb::Exiting));
                            stack.push((*child_id, FlattenNodesCrumb::Entering));
                            continue;
                        }
                        OldOrNewNodeId::Old(_) => {}
                    },
                    FlattenNodesCrumb::Exiting => {}
                    _ => unreachable!(),
                },
                _ => {}
            }
            ordered_nodes.push(node_id);
        }

        // And now, compute hashes and memory usage, because it is heavy, and we are outside of
        // main block processing thread.
        let mut last_node_hash = CryptoHash::default();
        let mut mapped_nodes: HashMap<UpdatedMemTrieNodeId, (CryptoHash, u64)> = Default::default();
        let map_node = |node: OldOrNewNodeId,
                        map: &HashMap<UpdatedMemTrieNodeId, (CryptoHash, u64)>|
         -> (CryptoHash, u64) {
            match node {
                OldOrNewNodeId::New(node) => map.get(&node).unwrap().clone(),
                OldOrNewNodeId::Old(node_id) => {
                    let view = node_id.as_ptr(arena).view();
                    (view.node_hash(), view.memory_usage())
                }
            }
        };

        let mut refcount_changes: HashMap<CryptoHash, (Vec<u8>, i32)> = Default::default();
        for (node_id, rc) in id_refcount_changes {
            let view = node_id.as_ptr(arena).view();
            let hash = view.node_hash();
            let (_, old_rc) = refcount_changes
                .entry(hash)
                .or_insert_with(|| (borsh::to_vec(&view.to_raw_trie_node_with_size()).unwrap(), 0));
            *old_rc += rc;
        }
        for (value, rc) in value_changes.into_iter() {
            let (_, old_rc) = refcount_changes
                .entry(value)
                .or_insert_with(|| (new_values.get(&value).cloned().unwrap_or(Vec::new()), 0));
            *old_rc += rc;
        }

        let mut node_ids_with_hashes = vec![];
        for node_id in ordered_nodes.into_iter() {
            let node = nodes_storage.get(node_id).unwrap().clone().unwrap();
            let (node, memory_usage) = match node {
                UpdatedMemTrieNode::Empty => unreachable!(),
                UpdatedMemTrieNode::Branch { children, value } => {
                    let mut memory_usage = TRIE_COSTS.node_cost;
                    let mut child_hashes = vec![];
                    for child in children.into_iter() {
                        match child {
                            Some(child) => {
                                let (child_hash, child_memory_usage) =
                                    map_node(child, &mapped_nodes);
                                child_hashes.push(Some(child_hash));
                                memory_usage += child_memory_usage;
                            }
                            None => {
                                child_hashes.push(None);
                            }
                        }
                    }
                    let children = Children(child_hashes.as_slice().try_into().unwrap());
                    let value_ref = value.map(|value| value.to_value_ref());
                    memory_usage += match &value_ref {
                        Some(value_ref) => {
                            value_ref.length as u64 * TRIE_COSTS.byte_of_value
                                + TRIE_COSTS.node_cost
                        }
                        None => 0,
                    };
                    (RawTrieNode::branch(children, value_ref), memory_usage)
                }
                UpdatedMemTrieNode::Extension { extension, child } => {
                    let (child_hash, child_memory_usage) = map_node(child, &mapped_nodes);
                    let memory_usage = TRIE_COSTS.node_cost
                        + extension.len() as u64 * TRIE_COSTS.byte_of_key
                        + child_memory_usage;
                    (RawTrieNode::Extension(extension.to_vec(), child_hash), memory_usage)
                }
                UpdatedMemTrieNode::Leaf { extension, value } => {
                    let memory_usage = TRIE_COSTS.node_cost
                        + extension.len() as u64 * TRIE_COSTS.byte_of_key
                        + value.value_len() as u64 * TRIE_COSTS.byte_of_value
                        + TRIE_COSTS.node_cost;
                    (RawTrieNode::Leaf(extension.to_vec(), value.to_value_ref()), memory_usage)
                }
            };

            let raw_node_with_size = RawTrieNodeWithSize { node, memory_usage };
            let node_serialized = borsh::to_vec(&raw_node_with_size).unwrap();
            let node_hash = hash(&node_serialized);
            mapped_nodes.insert(node_id, (node_hash, memory_usage));

            let (_, rc) = refcount_changes.entry(node_hash).or_insert_with(|| (node_serialized, 0));
            *rc += 1;

            last_node_hash = node_hash;
            node_ids_with_hashes.push((node_id, node_hash));
        }

        let (insertions, deletions) = Trie::convert_to_insertions_and_deletions(refcount_changes);

        TrieChanges {
            old_root: root.map(|root| root.as_ptr(arena).view().node_hash()).unwrap_or_default(),
            new_root: last_node_hash,
            insertions,
            deletions,
            mem_trie_changes: Some(MemTrieChanges {
                node_ids_with_hashes,
                nodes_storage,
                block_height,
            }),
        }
    }
}

pub fn apply_memtrie_changes(memtries: &mut MemTries, changes: &MemTrieChanges) {
    memtries
        .construct_root(changes.block_height, |arena| {
            let mut last_node_id: Option<MemTrieNodeId> = None;
            let map_to_new_node_id = |node_id: OldOrNewNodeId,
                                      old_to_new_map: &HashMap<
                UpdatedMemTrieNodeId,
                MemTrieNodeId,
            >|
             -> MemTrieNodeId {
                match node_id {
                    OldOrNewNodeId::New(node_id) => old_to_new_map.get(&node_id).unwrap().clone(),
                    OldOrNewNodeId::Old(node_id) => node_id,
                }
            };

            let mut old_to_new_map = HashMap::<UpdatedMemTrieNodeId, MemTrieNodeId>::new();
            let nodes_storage = &changes.nodes_storage;
            let node_ids_with_hashes = &changes.node_ids_with_hashes;
            for (node_id, node_hash) in node_ids_with_hashes.iter() {
                let node = nodes_storage.get(*node_id).unwrap().clone().unwrap();
                let node = match node {
                    UpdatedMemTrieNode::Empty => unreachable!(),
                    UpdatedMemTrieNode::Branch { children, value } => {
                        let mut new_children = [None; 16];
                        for i in 0..16 {
                            if let Some(child) = children[i] {
                                new_children[i] = Some(map_to_new_node_id(child, &old_to_new_map));
                            }
                        }
                        match value {
                            Some(value) => {
                                InputMemTrieNode::BranchWithValue { children: new_children, value }
                            }
                            None => InputMemTrieNode::Branch { children: new_children },
                        }
                    }
                    UpdatedMemTrieNode::Extension { extension, child } => {
                        InputMemTrieNode::Extension {
                            extension,
                            child: map_to_new_node_id(child, &old_to_new_map),
                        }
                    }
                    UpdatedMemTrieNode::Leaf { extension, value } => {
                        InputMemTrieNode::Leaf { value, extension }
                    }
                };
                let mem_node_id = MemTrieNodeId::new_with_hash(arena, node, *node_hash);
                old_to_new_map.insert(*node_id, mem_node_id);
                last_node_id = Some(mem_node_id);
            }

            Ok::<Option<MemTrieNodeId>, ()>(last_node_id)
        })
        .unwrap();
}
