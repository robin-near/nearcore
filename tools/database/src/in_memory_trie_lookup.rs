use std::cell::RefCell;
use std::sync::Arc;

use near_primitives::state::ValueRef;
use near_primitives::types::TrieNodesCount;
use near_store::{NibbleSlice, ShardUId, Store};

use crate::in_memory_trie_loading::{InMemoryTrieNodeLite, InMemoryTrieNodeSet};

pub struct InMemoryTrie {
    shard_uid: ShardUId,
    store: Store,
    root: Arc<InMemoryTrieNodeLite>,
    cache: RefCell<InMemoryTrieNodeSet>,
    nodes_count: RefCell<TrieNodesCount>,
}

impl InMemoryTrie {
    pub fn new(shard_uid: ShardUId, store: Store, root: Arc<InMemoryTrieNodeLite>) -> Self {
        Self {
            shard_uid,
            store,
            root,
            cache: RefCell::new(InMemoryTrieNodeSet::new()),
            nodes_count: RefCell::new(TrieNodesCount { db_reads: 0, mem_reads: 0 }),
        }
    }

    pub fn get_ref(&self, path: &[u8]) -> Option<ValueRef> {
        let mut nibbles = NibbleSlice::new(path);
        let mut node = self.root.clone();
        loop {
            if self.cache.borrow_mut().insert_if_not_exists(node.clone()) {
                self.nodes_count.borrow_mut().db_reads += 1;
            } else {
                self.nodes_count.borrow_mut().mem_reads += 1;
            }
            match &node.kind {
                crate::in_memory_trie_loading::InMemoryTrieNodeKindLite::Leaf {
                    extension,
                    value,
                } => {
                    if nibbles == NibbleSlice::from_encoded(extension).0 {
                        return Some(value.clone());
                    } else {
                        return None;
                    }
                }
                crate::in_memory_trie_loading::InMemoryTrieNodeKindLite::Extension {
                    extension,
                    child,
                } => {
                    let extension_nibbles = NibbleSlice::from_encoded(extension).0;
                    if nibbles.starts_with(&extension_nibbles) {
                        nibbles = nibbles.mid(extension_nibbles.len());
                        node = child.clone();
                    } else {
                        return None;
                    }
                }
                crate::in_memory_trie_loading::InMemoryTrieNodeKindLite::Branch(children) => {
                    if nibbles.is_empty() {
                        return None;
                    }
                    let first = nibbles.at(0);
                    nibbles = nibbles.mid(1);
                    node = match children[first as usize].clone() {
                        Some(child) => child,
                        None => return None,
                    };
                }
                crate::in_memory_trie_loading::InMemoryTrieNodeKindLite::BranchWithLeaf {
                    children,
                    value,
                } => {
                    if nibbles.is_empty() {
                        return Some(value.clone());
                    }
                    let first = nibbles.at(0);
                    nibbles = nibbles.mid(1);
                    node = match children[first as usize].clone() {
                        Some(child) => child,
                        None => return None,
                    };
                }
            }
        }
    }

    pub fn get_nodes_count(&self) -> TrieNodesCount {
        self.nodes_count.borrow().clone()
    }
}
