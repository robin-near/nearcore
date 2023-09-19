use super::{MemTrieNodePtr, MemTrieNodeView};
use crate::trie::mem::flexible_data::children::ChildrenView;
use crate::trie::mem::node::{InputMemTrieNode, MemTrieNodeId};
use crate::trie::TRIE_COSTS;
use crate::{NibbleSlice, RawTrieNode, RawTrieNodeWithSize};
use borsh::BorshSerialize;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::state::FlatStateValue;
use std::collections::HashMap;

pub type UpdatedMemTrieNodeId = usize;

#[derive(Clone)]
pub enum UpdatedNodeRef<'a> {
    Old(MemTrieNodePtr<'a>),
    New(UpdatedMemTrieNodeId),
}

#[derive(Clone)]
pub enum UpdatedMemTrieNode<'a> {
    // Fancy edge case. Used very widely during updates!
    Empty,
    Leaf { extension: Box<[u8]>, value: FlatStateValue },
    Extension { extension: Box<[u8]>, child: UpdatedNodeRef<'a> },
    Branch { children: Vec<Option<UpdatedNodeRef<'a>>>, value: Option<FlatStateValue> },
}

#[derive(Default)]
pub struct MemTrieUpdate<'a> {
    pub refcount_changes: HashMap<MemTrieNodeId, i32>,
    pub nodes_storage: HashMap<UpdatedMemTrieNodeId, UpdatedMemTrieNode<'a>>,
}

fn convert_children(view: ChildrenView) -> Vec<Option<UpdatedNodeRef>> {
    let mut children = vec![];
    let mut j = 0;
    for i in 0..16 {
        children.push(if view.mask & (1 << i) != 0 {
            let child =
                Some(UpdatedNodeRef::Old(MemTrieNodePtr::from(view.children.read_ptr_at(j))));
            j += 8;
            child
        } else {
            None
        });
    }
    children
}

impl<'a> MemTrieUpdate<'a> {
    pub fn store_at(&mut self, index: UpdatedMemTrieNodeId, node: UpdatedMemTrieNode<'a>) {
        self.nodes_storage.insert(index, node);
    }

    pub fn store(&mut self, node: UpdatedMemTrieNode<'a>) -> UpdatedMemTrieNodeId {
        let index = self.nodes_storage.len();
        self.nodes_storage.insert(index, node);
        index
    }

    pub fn move_node_to_mutable(&mut self, node: &MemTrieNodePtr<'a>) -> usize {
        *self.refcount_changes.entry(node.id()).or_insert_with(|| 0) -= 1;
        let updated_node = node.view().to_updated();
        self.store(updated_node)
    }

    // INSERT/DELETE LOGIC

    // insert to root
    // todo: consider already dropping hash & mem usage. but maybe idc
    // todo: what are trie changes for values?
    pub fn insert(&mut self, root_id: UpdatedMemTrieNodeId, key: &[u8], value: FlatStateValue) {
        let mut value = Some(value);
        let mut node = self.nodes_storage.get(&root_id).unwrap().clone();
        let mut node_id = root_id;
        let mut partial = NibbleSlice::new(key);
        // I think we don't need path as we can update both mem & hashes on flatten.
        // let mut path = Vec::new();

        loop {
            match &node {
                UpdatedMemTrieNode::Empty => {
                    // store value somehow
                    let extension: Vec<_> = partial.encoded(true).into_vec();
                    let leaf_node = UpdatedMemTrieNode::Leaf {
                        extension: extension.into_boxed_slice(),
                        value: value.take().unwrap(),
                    };
                    self.store_at(node_id, leaf_node);
                }
                UpdatedMemTrieNode::Branch { children, value: old_value } => {
                    if partial.is_empty() {
                        // Store value here.
                        if let Some(_value) = old_value {
                            // delete value somehow
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
                                self.move_node_to_mutable(&node_ptr)
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
                    let existing_key = NibbleSlice::from_encoded(key).0;
                    let common_prefix = partial.common_prefix(&existing_key);
                    if common_prefix == existing_key.len() && common_prefix == partial.len() {
                        // Equivalent leaf, rewrite the value.
                        // delete value somehow
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
                            UpdatedNodeRef::Old(ptr) => self.move_node_to_mutable(ptr),
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

    pub fn to_updated(self) -> UpdatedMemTrieNode<'a> {
        match self {
            Self::Leaf { extension, value } => UpdatedMemTrieNode::Leaf {
                extension: extension.as_slice().to_vec().into_boxed_slice(),
                value: value.to_flat_value(),
            },
            Self::Branch { children, .. } => {
                UpdatedMemTrieNode::Branch { children: convert_children(children), value: None }
            }
            Self::BranchWithValue { children, value, .. } => UpdatedMemTrieNode::Branch {
                children: convert_children(children),
                value: Some(value.to_flat_value()),
            },
            Self::Extension { extension, child, .. } => UpdatedMemTrieNode::Extension {
                extension: extension.as_slice().to_vec().into_boxed_slice(),
                child: UpdatedNodeRef::Old(child),
            },
        }
    }
}
