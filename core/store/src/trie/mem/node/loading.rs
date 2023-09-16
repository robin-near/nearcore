use super::encoding::{CommonHeader, NodeKind, NonLeafHeader};
use super::MemTrieNodePtr;
use borsh::BorshSerialize;
use near_primitives::hash::hash;

impl<'a> MemTrieNodePtr<'a> {
    unsafe fn compute_hash_and_memory_usage(&self) {
        let raw_trie_node_with_size = self.view().to_raw_trie_node_with_size();
        let mut decoder = self.decoder();
        match decoder.decode::<CommonHeader>().kind {
            NodeKind::Leaf => {}
            _ => {
                let nonleaf = NonLeafHeader {
                    memory_usage: raw_trie_node_with_size.memory_usage,
                    hash: hash(&raw_trie_node_with_size.try_to_vec().unwrap()),
                };
                decoder.overwrite(nonleaf);
            }
        }
    }

    fn is_hash_and_memory_usage_computed(&self) -> bool {
        let mut decoder = self.decoder();
        match decoder.decode::<CommonHeader>().kind {
            NodeKind::Leaf => true,
            _ => decoder.peek::<NonLeafHeader>().memory_usage != 0,
        }
    }

    /// This is used after initially constructing the in-memory trie strcuture,
    /// to compute the hash and memory usage recursively. The computation is
    /// expensive, so we defer it in order to make it parallelizable.
    pub(crate) unsafe fn compute_hash_and_memory_usage_recursively(&self) {
        if self.is_hash_and_memory_usage_computed() {
            return;
        }
        for child in self.view().iter_children() {
            child.compute_hash_and_memory_usage_recursively();
        }
        self.compute_hash_and_memory_usage();
    }

    /// TODO
    pub(crate) fn compute_subtree_node_count_and_mark_boundary_subtrees(
        &self,
        threshold: usize,
        trees: &mut Vec<MemTrieNodePtr<'a>>,
    ) -> (BoundaryNodeType, usize) {
        let mut total = 1;
        let mut any_children_above_or_at_boundary = false;
        let mut children_below_boundary = Vec::new();
        for child in self.view().iter_children() {
            let (child_boundary_type, child_count) =
                child.compute_subtree_node_count_and_mark_boundary_subtrees(threshold, trees);
            match child_boundary_type {
                BoundaryNodeType::AboveOrAtBoundary => {
                    any_children_above_or_at_boundary = true;
                }
                BoundaryNodeType::BelowBoundary => {
                    children_below_boundary.push(child);
                }
            }
            total += child_count;
        }
        if any_children_above_or_at_boundary {
            for child in children_below_boundary {
                trees.push(child);
            }
        } else if total >= threshold {
            trees.push(self.clone());
        }
        if total >= threshold {
            (BoundaryNodeType::AboveOrAtBoundary, total)
        } else {
            (BoundaryNodeType::BelowBoundary, total)
        }
    }
}

pub enum BoundaryNodeType {
    AboveOrAtBoundary,
    BelowBoundary,
}
