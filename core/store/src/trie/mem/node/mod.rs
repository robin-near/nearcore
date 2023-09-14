#![allow(dead_code)] // still being implemented

mod encoding;
mod loading;
#[cfg(test)]
mod tests;
mod view;

use super::arena::{ArenaHandle, ArenaPtr, ArenaSlice};
use super::flexible_data::children::ChildrenView;
use super::flexible_data::value::ValueView;
use near_primitives::hash::CryptoHash;
use near_primitives::state::FlatStateValue;

/// An efficiently encoded in-memory trie node.
///
/// This struct is internally refcounted, similar to an Rc<T>. When all clones
/// of the same `MemTrieNode` are dropped, the associated memory allocation is
/// freed.
///
/// To construct a `MemTrieNode`, call `MemTrieNode::new`. To read its contents,
/// call `MemTrieNode::view()`, which returns a `MemTrieNodeView`.
#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct MemTrieNode {
    pub(crate) ptr: ArenaPtr,
}

impl MemTrieNode {
    pub fn from(ptr: ArenaPtr) -> Self {
        Self { ptr }
    }

    pub fn new(arena: &ArenaHandle, input: InputMemTrieNode) -> Self {
        Self::new_impl(arena, input)
    }

    pub fn view(&self) -> MemTrieNodeView {
        self.view_impl()
    }

    pub fn hash(&self) -> CryptoHash {
        self.view().node_hash()
    }

    pub fn memory_usage(&self) -> u64 {
        self.view().memory_usage()
    }
}

/// Used to construct a new `MemTrieNode`.
#[derive(PartialEq, Eq, Debug, Clone)]
pub enum InputMemTrieNode {
    Leaf { value: FlatStateValue, extension: Box<[u8]> },
    Extension { extension: Box<[u8]>, child: MemTrieNode },
    Branch { children: Vec<Option<MemTrieNode>> },
    BranchWithValue { children: Vec<Option<MemTrieNode>>, value: FlatStateValue },
}

/// A view of the encoded data of `MemTrieNode`, obtainable via
/// `MemTrieNode::view()`.
#[derive(Debug, Clone)]
pub enum MemTrieNodeView {
    Leaf {
        extension: ArenaSlice,
        value: ValueView,
    },
    Extension {
        hash: CryptoHash,
        memory_usage: u64,
        extension: ArenaSlice,
        child: MemTrieNode,
    },
    Branch {
        hash: CryptoHash,
        memory_usage: u64,
        children: ChildrenView,
    },
    BranchWithValue {
        hash: CryptoHash,
        memory_usage: u64,
        children: ChildrenView,
        value: ValueView,
    },
}
