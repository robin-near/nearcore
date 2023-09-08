use crate::in_memory_trie_compact::{TrieNodeRefHandle, TrieNodeView};
use near_primitives::state::FlatStateValue;
use near_primitives::types::TrieNodesCount;
use near_store::{NibbleSlice, ShardUId, Store};
use std::cell::RefCell;
use std::collections::HashSet;

pub struct InMemoryTrieCompact<'a> {
    shard_uid: ShardUId,
    store: Store,
    root: TrieNodeRefHandle<'a>,
    cache: RefCell<HashSet<TrieNodeRefHandle<'a>>>,
    nodes_count: RefCell<TrieNodesCount>,
}

impl<'a> InMemoryTrieCompact<'a> {
    pub fn new(shard_uid: ShardUId, store: Store, root: TrieNodeRefHandle<'a>) -> Self {
        Self {
            shard_uid,
            store,
            root,
            cache: RefCell::new(HashSet::new()),
            nodes_count: RefCell::new(TrieNodesCount { db_reads: 0, mem_reads: 0 }),
        }
    }

    pub fn get_ref(&self, path: &[u8]) -> Option<FlatStateValue> {
        let mut nibbles = NibbleSlice::new(path);
        let mut node = self.root;
        loop {
            if self.cache.borrow_mut().insert(node) {
                self.nodes_count.borrow_mut().db_reads += 1;
            } else {
                self.nodes_count.borrow_mut().mem_reads += 1;
            }
            match node.view() {
                TrieNodeView::Leaf { extension, value } => {
                    if nibbles == NibbleSlice::from_encoded(extension).0 {
                        return Some(value.to_flat_value());
                    } else {
                        return None;
                    }
                }
                TrieNodeView::Extension { extension, child, .. } => {
                    let extension_nibbles = NibbleSlice::from_encoded(extension).0;
                    if nibbles.starts_with(&extension_nibbles) {
                        nibbles = nibbles.mid(extension_nibbles.len());
                        node = child;
                    } else {
                        return None;
                    }
                }
                TrieNodeView::Branch { children, .. } => {
                    if nibbles.is_empty() {
                        return None;
                    }
                    let first = nibbles.at(0);
                    nibbles = nibbles.mid(1);
                    node = match children.get(first as usize) {
                        Some(child) => child,
                        None => return None,
                    };
                }
                TrieNodeView::BranchWithValue { children, value, .. } => {
                    if nibbles.is_empty() {
                        return Some(value.to_flat_value());
                    }
                    let first = nibbles.at(0);
                    nibbles = nibbles.mid(1);
                    node = match children.get(first as usize) {
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
