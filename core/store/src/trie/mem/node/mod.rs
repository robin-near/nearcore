#![allow(dead_code)] // still being implemented

mod encoding;
mod loading;
#[cfg(test)]
mod tests;
mod view;

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

use super::arena::{Arena, ArenaPtr, ArenaSlice};
use super::flexible_data::children::ChildrenView;
use super::flexible_data::value::ValueView;
use crate::NibbleSlice;
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
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub struct MemTrieNodeId {
    pub(crate) ptr: usize,
}

impl MemTrieNodeId {
    pub fn from(ptr: usize) -> Self {
        Self { ptr }
    }

    pub fn new(arena: &mut Arena, input: InputMemTrieNode) -> Self {
        Self::new_impl(arena, input)
    }

    pub fn to_ref<'a>(&self, arena: &'a Arena) -> MemTrieNodePtr<'a> {
        MemTrieNodePtr { ptr: arena.ptr(self.ptr) }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct MemTrieNodePtr<'a> {
    ptr: ArenaPtr<'a>,
}

impl<'a> Debug for MemTrieNodePtr<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.id().fmt(f)
    }
}

impl<'a> MemTrieNodePtr<'a> {
    pub fn from(ptr: ArenaPtr<'a>) -> Self {
        Self { ptr }
    }

    pub fn view(&self) -> MemTrieNodeView<'a> {
        self.view_impl()
    }

    pub fn id(&self) -> MemTrieNodeId {
        MemTrieNodeId { ptr: self.ptr.raw_offset() }
    }

    pub fn move_node_to_mutable(
        &self,
        changes: &mut HashMap<MemTrieNodeId, i32>,
    ) -> MemTrieNodeView<'a> {
        *changes.entry(self.id()).or_insert_with(|| 0) -= 1;
        self.view()
    }

    // todo: take state changes and gen trie updates
    pub fn update(&self, key: &[u8], value: FlatStateValue) {
        let mut changes: HashMap<MemTrieNodeId, i32> = Default::default();
        let root = self.move_node_to_mutable(&mut changes);
        root.insert(key, value)
    }
}

/// Used to construct a new `MemTrieNode`.
#[derive(PartialEq, Eq, Debug, Clone)]
pub enum InputMemTrieNode {
    Leaf { value: FlatStateValue, extension: Box<[u8]> },
    Extension { extension: Box<[u8]>, child: MemTrieNodeId },
    Branch { children: Vec<Option<MemTrieNodeId>> },
    BranchWithValue { children: Vec<Option<MemTrieNodeId>>, value: FlatStateValue },
}

/// A view of the encoded data of `MemTrieNode`, obtainable via
/// `MemTrieNode::view()`.
#[derive(Debug, Clone)]
pub enum MemTrieNodeView<'a> {
    Leaf {
        extension: ArenaSlice<'a>,
        value: ValueView<'a>,
    },
    Extension {
        hash: CryptoHash,
        memory_usage: u64,
        extension: ArenaSlice<'a>,
        child: MemTrieNodePtr<'a>,
    },
    Branch {
        hash: CryptoHash,
        memory_usage: u64,
        children: ChildrenView<'a>,
    },
    BranchWithValue {
        hash: CryptoHash,
        memory_usage: u64,
        children: ChildrenView<'a>,
        value: ValueView<'a>,
    },
}
