use derive_where::derive_where;
use std::fmt::{Debug, Formatter};

use near_primitives::hash::CryptoHash;
use near_primitives::state::FlatStateValue;

use super::arena::{ArenaMemory, ArenaPos, ArenaPtr, ArenaPtrMut, IArena, IArenaMemory};
use super::flexible_data::children::ChildrenView;
use super::flexible_data::value::ValueView;
use crate::trie::{Children, TRIE_COSTS};
use crate::{RawTrieNode, RawTrieNodeWithSize};

mod encoding;
mod mutation;
#[cfg(test)]
mod tests;
mod view;

/// The memory position of an encoded in-memory trie node.
/// With an `ArenaMemory`, this can be turned into a `MemTrieNodePtr`
/// to access to the node's contents.
///
/// In-memory trie nodes are completely stored in an `Arena`. Allocation and
/// deallocation of these nodes are done on the arena. Nodes allow multiple
/// references in the case of multiple state roots (trie roots), and are
/// internally refcounted. See `MemTries` for more details on the lifecycle
/// of trie nodes.
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub struct MemTrieNodeId {
    pub(crate) pos: ArenaPos,
}

impl MemTrieNodeId {
    pub fn new(arena: &mut impl IArena, input: InputMemTrieNode) -> Self {
        Self::new_impl(arena, input, None)
    }

    pub fn new_with_hash(
        arena: &mut impl IArena,
        input: InputMemTrieNode,
        hash: CryptoHash,
    ) -> Self {
        Self::new_impl(arena, input, Some(hash))
    }

    pub fn as_ptr<'a, Memory: IArenaMemory>(
        &self,
        arena: &'a Memory,
    ) -> MemTrieNodePtrGeneric<'a, Memory> {
        MemTrieNodePtrGeneric { ptr: arena.ptr(self.pos) }
    }

    pub(crate) fn as_ptr_mut<'a, Memory: IArenaMemory>(
        &self,
        arena: &'a mut Memory,
    ) -> MemTrieNodePtrMut<'a, Memory> {
        MemTrieNodePtrMut { ptr: arena.ptr_mut(self.pos) }
    }
}

/// This is for internal use only, so that we can put `MemTrieNodeId` in an
/// ElasticArray.
impl Default for MemTrieNodeId {
    fn default() -> Self {
        Self { pos: ArenaPos::invalid() }
    }
}

/// Pointer to an in-memory trie node that allows read-only access to the node
/// and all its descendants.
#[derive_where(Clone, Copy, PartialEq, Eq)]
pub struct MemTrieNodePtrGeneric<'a, Memory: IArenaMemory> {
    ptr: ArenaPtr<'a, Memory>,
}

pub type MemTrieNodePtr<'a> = MemTrieNodePtrGeneric<'a, ArenaMemory>;

/// Pointer to an in-memory trie node that allows mutable access to the node
/// and all its descendants. This is only for computing hashes, and internal
/// reference counting.
pub struct MemTrieNodePtrMut<'a, Memory: IArenaMemory> {
    ptr: ArenaPtrMut<'a, Memory>,
}

impl<'a, Memory: IArenaMemory> Debug for MemTrieNodePtrGeneric<'a, Memory> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.id().fmt(f)
    }
}

impl<'a, Memory: IArenaMemory> MemTrieNodePtrGeneric<'a, Memory> {
    pub fn from(ptr: ArenaPtr<'a, Memory>) -> Self {
        Self { ptr }
    }

    pub fn view(&self) -> MemTrieNodeViewGeneric<'a, Memory> {
        self.view_impl()
    }

    pub fn id(&self) -> MemTrieNodeId {
        MemTrieNodeId { pos: self.ptr.raw_pos() }
    }
}

/// Used to construct a new in-memory trie node.
#[derive(PartialEq, Eq, Debug, Clone)]
pub enum InputMemTrieNode<'a> {
    Leaf { value: &'a FlatStateValue, extension: &'a [u8] },
    Extension { extension: &'a [u8], child: MemTrieNodeId },
    Branch { children: [Option<MemTrieNodeId>; 16] },
    BranchWithValue { children: [Option<MemTrieNodeId>; 16], value: &'a FlatStateValue },
}

/// A view of the encoded data of `MemTrieNode`, obtainable via
/// `MemTrieNode::view()`.
#[derive_where(Debug, Clone)]
pub enum MemTrieNodeViewGeneric<'a, Memory: IArenaMemory> {
    Leaf {
        extension: &'a [u8],
        value: ValueView<'a>,
    },
    Extension {
        hash: CryptoHash,
        memory_usage: u64,
        extension: &'a [u8],
        child: MemTrieNodePtrGeneric<'a, Memory>,
    },
    Branch {
        hash: CryptoHash,
        memory_usage: u64,
        children: ChildrenView<'a, Memory>,
    },
    BranchWithValue {
        hash: CryptoHash,
        memory_usage: u64,
        children: ChildrenView<'a, Memory>,
        value: ValueView<'a>,
    },
}

pub type MemTrieNodeView<'a> = MemTrieNodeViewGeneric<'a, ArenaMemory>;
