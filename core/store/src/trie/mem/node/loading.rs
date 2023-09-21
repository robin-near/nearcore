use crate::trie::mem::arena::{ArenaMemory, ArenaPtrMut};
use crate::trie::mem::flexible_data::encoding::RawDecoderMut;

use super::encoding::{CommonHeader, NodeKind, NonLeafHeader};
use super::{MemTrieNodeId, MemTrieNodePtr};
use borsh::BorshSerialize;
use near_primitives::hash::{hash, CryptoHash};

pub(crate) struct MemTrieNodePtrMut<'a> {
    ptr: ArenaPtrMut<'a>,
}

impl MemTrieNodeId {
    pub(crate) fn as_ptr_mut<'a>(&self, arena: &'a mut ArenaMemory) -> MemTrieNodePtrMut<'a> {
        MemTrieNodePtrMut { ptr: arena.ptr_mut(self.ptr) }
    }
}

impl<'a> MemTrieNodePtrMut<'a> {
    pub fn as_const(&'a self) -> MemTrieNodePtr<'a> {
        MemTrieNodePtr { ptr: self.ptr.as_const() }
    }

    fn copy<'b>(&'b mut self) -> MemTrieNodePtrMut<'b> {
        MemTrieNodePtrMut { ptr: self.ptr.offset(0) }
    }

    pub(crate) fn decoder_mut<'b>(&'b mut self) -> RawDecoderMut<'b> {
        RawDecoderMut::new(self.ptr.offset(0))
    }

    fn split_children_mut(mut self) -> Vec<MemTrieNodePtrMut<'a>> {
        let arena_mut = self.ptr.arena_mut() as *mut ArenaMemory;
        let mut result = Vec::new();
        let view = self.as_const().view();
        for child in view.iter_children() {
            let child_id = child.id();
            let arena_mut_ref = unsafe { &mut *arena_mut };
            result.push(child_id.as_ptr_mut(arena_mut_ref));
        }
        result
    }

    fn children_mut<'b>(&'b mut self) -> Vec<MemTrieNodePtrMut<'b>> {
        let arena_mut = self.ptr.arena_mut() as *mut ArenaMemory;
        let mut result = Vec::new();
        let view = self.as_const().view();
        for child in view.iter_children() {
            let child_id = child.id();
            let arena_mut_ref = unsafe { &mut *arena_mut };
            result.push(child_id.as_ptr_mut(arena_mut_ref));
        }
        result
    }

    fn compute_hash(&mut self) {
        let raw_trie_node_with_size = self.as_const().view().to_raw_trie_node_with_size();
        let mut decoder = self.decoder_mut();
        match decoder.decode::<CommonHeader>().kind {
            NodeKind::Leaf => {}
            _ => {
                let mut nonleaf = decoder.peek::<NonLeafHeader>();
                nonleaf.hash = hash(&raw_trie_node_with_size.try_to_vec().unwrap());
                decoder.overwrite(nonleaf);
            }
        }
    }

    fn is_hash_computed(&self) -> bool {
        let mut decoder = self.as_const().decoder();
        match decoder.decode::<CommonHeader>().kind {
            NodeKind::Leaf => true,
            _ => decoder.peek::<NonLeafHeader>().hash != CryptoHash::default(),
        }
    }

    /// This is used after initially constructing the in-memory trie strcuture,
    /// to compute the hash and memory usage recursively. The computation is
    /// expensive, so we defer it in order to make it parallelizable.
    pub(crate) fn compute_hash_recursively(&mut self) {
        if self.is_hash_computed() {
            return;
        }
        for mut child in self.children_mut() {
            child.compute_hash_recursively();
        }
        self.compute_hash();
    }

    pub(crate) fn take_small_subtrees(
        self,
        threshold_memory_usage: u64,
        trees: &mut Vec<MemTrieNodePtrMut<'a>>,
    ) {
        if self.as_const().view().memory_usage() < threshold_memory_usage {
            trees.push(self);
        } else {
            for child in self.split_children_mut() {
                child.take_small_subtrees(threshold_memory_usage, trees);
            }
        }
    }
}

pub enum BoundaryNodeType {
    AboveOrAtBoundary,
    BelowBoundary,
}
