use std::cell::RefCell;
use std::collections::HashSet;

use near_primitives::state::FlatStateValue;
use near_vm_runner::logic::TrieNodesCount;

use crate::NibbleSlice;

use super::node::{MemTrieNodeId, MemTrieNodePtr, MemTrieNodeView};

pub struct MemTrieLookup<'a> {
    root: MemTrieNodePtr<'a>,
    cache: RefCell<HashSet<MemTrieNodeId>>,
    nodes_count: RefCell<TrieNodesCount>,
}

impl<'a> MemTrieLookup<'a> {
    pub fn new(root: MemTrieNodePtr<'a>) -> Self {
        Self {
            root,
            cache: RefCell::new(HashSet::new()),
            nodes_count: RefCell::new(TrieNodesCount { db_reads: 0, mem_reads: 0 }),
        }
    }

    pub fn new_with(
        root: MemTrieNodePtr<'a>,
        cache: RefCell<HashSet<MemTrieNodeId>>,
        nodes_count: RefCell<TrieNodesCount>,
    ) -> Self {
        Self { root, cache, nodes_count }
    }

    pub fn get_ref(&self, path: &[u8]) -> Option<FlatStateValue> {
        let mut nibbles = NibbleSlice::new(path);
        let mut node = self.root;
        loop {
            if self.cache.borrow_mut().insert(node.id()) {
                self.nodes_count.borrow_mut().db_reads += 1;
            } else {
                self.nodes_count.borrow_mut().mem_reads += 1;
            }
            match node.view() {
                MemTrieNodeView::Leaf { extension, value } => {
                    if nibbles == NibbleSlice::from_encoded(extension.as_slice()).0 {
                        return Some(value.to_flat_value());
                    } else {
                        return None;
                    }
                }
                MemTrieNodeView::Extension { extension, child, .. } => {
                    let extension_nibbles = NibbleSlice::from_encoded(extension.as_slice()).0;
                    if nibbles.starts_with(&extension_nibbles) {
                        nibbles = nibbles.mid(extension_nibbles.len());
                        node = child;
                    } else {
                        return None;
                    }
                }
                MemTrieNodeView::Branch { children, .. } => {
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
                MemTrieNodeView::BranchWithValue { children, value, .. } => {
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
