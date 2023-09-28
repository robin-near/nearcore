use crate::trie::mem::arena::{Arena, BorshFixedSize};
use crate::trie::mem::flexible_data::children::EncodedChildrenHeader;
use crate::trie::mem::flexible_data::encoding::{RawDecoder, RawEncoder};
use crate::trie::mem::flexible_data::extension::EncodedExtensionHeader;
use crate::trie::mem::flexible_data::value::EncodedValueHeader;
use crate::trie::mem::flexible_data::FlexibleDataHeader;
use crate::trie::TRIE_COSTS;
use borsh::{BorshDeserialize, BorshSerialize};
use elastic_array::ElasticArray16;
use near_primitives::hash::CryptoHash;
use near_primitives::state::FlatStateValue;

use super::{InputMemTrieNode, MemTrieNodeId, MemTrieNodePtr, MemTrieNodeView};

#[repr(u8)]
#[derive(PartialEq, Eq, Clone, Copy, Debug, BorshSerialize, BorshDeserialize)]
pub(crate) enum NodeKind {
    Leaf,
    Extension,
    Branch,
    BranchWithValue,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub(crate) struct CommonHeader {
    refcount: u32,
    pub(crate) kind: NodeKind,
}

impl BorshFixedSize for CommonHeader {
    const SERIALIZED_SIZE: usize = std::mem::size_of::<u32>() + std::mem::size_of::<u8>();
}

#[derive(BorshSerialize, BorshDeserialize)]
pub(crate) struct NonLeafHeader {
    pub(crate) hash: CryptoHash,
    pub(crate) memory_usage: u64,
}

impl NonLeafHeader {
    pub(crate) fn new(hash: CryptoHash, memory_usage: u64) -> Self {
        Self { hash, memory_usage }
    }
}

impl BorshFixedSize for NonLeafHeader {
    const SERIALIZED_SIZE: usize = std::mem::size_of::<CryptoHash>() + std::mem::size_of::<u64>();
}

#[derive(BorshSerialize, BorshDeserialize)]
pub(crate) struct LeafHeader {
    common: CommonHeader,
    value: EncodedValueHeader,
    extension: EncodedExtensionHeader,
}

impl BorshFixedSize for LeafHeader {
    const SERIALIZED_SIZE: usize = CommonHeader::SERIALIZED_SIZE
        + EncodedValueHeader::SERIALIZED_SIZE
        + EncodedExtensionHeader::SERIALIZED_SIZE;
}

#[derive(BorshSerialize, BorshDeserialize)]
pub(crate) struct ExtensionHeader {
    common: CommonHeader,
    nonleaf: NonLeafHeader,
    child: usize,
    extension: EncodedExtensionHeader,
}

impl BorshFixedSize for ExtensionHeader {
    const SERIALIZED_SIZE: usize = CommonHeader::SERIALIZED_SIZE
        + NonLeafHeader::SERIALIZED_SIZE
        + std::mem::size_of::<usize>()
        + EncodedExtensionHeader::SERIALIZED_SIZE;
}

#[derive(BorshSerialize, BorshDeserialize)]
pub(crate) struct BranchHeader {
    common: CommonHeader,
    nonleaf: NonLeafHeader,
    children: EncodedChildrenHeader,
}

impl BorshFixedSize for BranchHeader {
    const SERIALIZED_SIZE: usize = CommonHeader::SERIALIZED_SIZE
        + NonLeafHeader::SERIALIZED_SIZE
        + EncodedChildrenHeader::SERIALIZED_SIZE;
}

#[derive(BorshSerialize, BorshDeserialize)]
pub(crate) struct BranchWithValueHeader {
    common: CommonHeader,
    nonleaf: NonLeafHeader,
    value: EncodedValueHeader,
    children: EncodedChildrenHeader,
}

impl BorshFixedSize for BranchWithValueHeader {
    const SERIALIZED_SIZE: usize = CommonHeader::SERIALIZED_SIZE
        + NonLeafHeader::SERIALIZED_SIZE
        + EncodedValueHeader::SERIALIZED_SIZE
        + EncodedChildrenHeader::SERIALIZED_SIZE;
}

impl MemTrieNodeId {
    /// Encodes the data.
    pub(crate) fn new_impl(
        arena: &mut Arena,
        node: InputMemTrieNode,
        // In the live construction, hash is computed before nodes are pushed to memory.
        hash: Option<CryptoHash>,
    ) -> Self {
        let hash = hash.unwrap_or(CryptoHash::default());
        match &node {
            InputMemTrieNode::Extension { child, .. } => {
                child.add_ref(arena);
            }
            InputMemTrieNode::Branch { children }
            | InputMemTrieNode::BranchWithValue { children, .. } => {
                for child in children {
                    if let Some(child) = child {
                        child.add_ref(arena);
                    }
                }
            }
            _ => {}
        }
        let memory_usage = match &node {
            InputMemTrieNode::Leaf { .. } => 0,
            InputMemTrieNode::Extension { extension, child } => {
                TRIE_COSTS.node_cost
                    + extension.len() as u64 * TRIE_COSTS.byte_of_key
                    + child.to_ref(arena.memory()).view().memory_usage()
            }
            InputMemTrieNode::Branch { children } => {
                let mut memory_usage = TRIE_COSTS.node_cost;
                for child in children.iter() {
                    if let Some(child) = child {
                        memory_usage += child.to_ref(arena.memory()).view().memory_usage();
                    }
                }
                memory_usage
            }
            InputMemTrieNode::BranchWithValue { children, value } => {
                let value_len = match value {
                    FlatStateValue::Ref(value_ref) => value_ref.len(),
                    FlatStateValue::Inlined(value) => value.len(),
                };
                let mut memory_usage = TRIE_COSTS.node_cost
                    + value_len as u64 * TRIE_COSTS.byte_of_value
                    + TRIE_COSTS.node_cost;
                for child in children.iter() {
                    if let Some(child) = child {
                        memory_usage += child.to_ref(arena.memory()).view().memory_usage();
                    }
                }
                memory_usage
            }
        };
        let data = match node {
            InputMemTrieNode::Leaf { value, extension } => {
                let extension_header = EncodedExtensionHeader::from_input(&extension);
                let value_header = EncodedValueHeader::from_input(&value);
                let mut data = RawEncoder::new(
                    arena,
                    LeafHeader::SERIALIZED_SIZE
                        + extension_header.flexible_data_length()
                        + value_header.flexible_data_length(),
                );
                data.encode(LeafHeader {
                    common: CommonHeader { refcount: 0, kind: NodeKind::Leaf },
                    extension: extension_header,
                    value: value_header,
                });
                data.encode_flexible(&extension_header, extension);
                data.encode_flexible(&value_header, value);
                data.finish()
            }
            InputMemTrieNode::Extension { extension, child } => {
                let extension_header = EncodedExtensionHeader::from_input(&extension);
                let mut data = RawEncoder::new(
                    arena,
                    ExtensionHeader::SERIALIZED_SIZE + extension_header.flexible_data_length(),
                );
                data.encode(ExtensionHeader {
                    common: CommonHeader { refcount: 0, kind: NodeKind::Extension },
                    nonleaf: NonLeafHeader::new(hash, memory_usage),
                    child: child.ptr,
                    extension: extension_header,
                });
                data.encode_flexible(&extension_header, extension);
                data.finish()
            }
            InputMemTrieNode::Branch { children } => {
                let children_header = EncodedChildrenHeader::from_input(&children);
                let mut data = RawEncoder::new(
                    arena,
                    BranchHeader::SERIALIZED_SIZE + children_header.flexible_data_length(),
                );
                data.encode(BranchHeader {
                    common: CommonHeader { refcount: 0, kind: NodeKind::Branch },
                    nonleaf: NonLeafHeader::new(hash, memory_usage),
                    children: children_header,
                });
                data.encode_flexible(&children_header, children);
                data.finish()
            }
            InputMemTrieNode::BranchWithValue { children, value } => {
                let children_header = EncodedChildrenHeader::from_input(&children);
                let value_header = EncodedValueHeader::from_input(&value);
                let mut data = RawEncoder::new(
                    arena,
                    BranchWithValueHeader::SERIALIZED_SIZE
                        + children_header.flexible_data_length()
                        + value_header.flexible_data_length(),
                );
                data.encode(BranchWithValueHeader {
                    common: CommonHeader { refcount: 0, kind: NodeKind::BranchWithValue },
                    nonleaf: NonLeafHeader::new(hash, memory_usage),
                    children: children_header,
                    value: value_header,
                });
                data.encode_flexible(&children_header, children);
                data.encode_flexible(&value_header, value);
                data.finish()
            }
        };
        Self { ptr: data.ptr().raw_offset() }
    }

    pub(crate) fn add_ref(&self, arena: &mut Arena) -> u32 {
        let mut refcount = arena.memory_mut().slice_mut(self.ptr, 4);
        let new_refcount = refcount.read_u32_at(0) + 1;
        refcount.write_u32_at(0, new_refcount);
        new_refcount
    }

    pub(crate) fn remove_ref(&self, arena: &mut Arena) -> u32 {
        let mut refcount = arena.memory_mut().slice_mut(self.ptr, 4);
        let new_refcount = refcount.read_u32_at(0) - 1;
        refcount.write_u32_at(0, new_refcount);
        if new_refcount == 0 {
            let mut children_to_unref = ElasticArray16::new();
            let node_ptr = self.to_ref(arena.memory());
            for child in node_ptr.view().iter_children() {
                children_to_unref.push(child.id().ptr);
            }
            let alloc_size = node_ptr.size_of_allocation();
            arena.dealloc(self.ptr, alloc_size);
            for child in children_to_unref.iter() {
                MemTrieNodeId::from(*child).remove_ref(arena);
            }
        }
        new_refcount
    }
}

impl<'a> MemTrieNodePtr<'a> {
    pub(crate) fn decoder(&self) -> RawDecoder<'a> {
        RawDecoder::new(self.ptr)
    }

    /// Decodes the data.
    pub(crate) fn view_impl(&self) -> MemTrieNodeView<'a> {
        let mut decoder = self.decoder();
        let kind = decoder.peek::<CommonHeader>().kind;
        match kind {
            NodeKind::Leaf => {
                let header = decoder.decode::<LeafHeader>();
                let extension = decoder.decode_flexible(&header.extension);
                let value = decoder.decode_flexible(&header.value);
                MemTrieNodeView::Leaf { extension, value }
            }
            NodeKind::Extension => {
                let header = decoder.decode::<ExtensionHeader>();
                let extension = decoder.decode_flexible(&header.extension);
                MemTrieNodeView::Extension {
                    hash: header.nonleaf.hash,
                    memory_usage: header.nonleaf.memory_usage,
                    extension,
                    child: MemTrieNodePtr::from(self.ptr.arena().ptr(header.child)),
                }
            }
            NodeKind::Branch => {
                let header = decoder.decode::<BranchHeader>();
                let children = decoder.decode_flexible(&header.children);
                MemTrieNodeView::Branch {
                    hash: header.nonleaf.hash,
                    memory_usage: header.nonleaf.memory_usage,
                    children,
                }
            }
            NodeKind::BranchWithValue => {
                let header = decoder.decode::<BranchWithValueHeader>();
                let children = decoder.decode_flexible(&header.children);
                let value = decoder.decode_flexible(&header.value);
                MemTrieNodeView::BranchWithValue {
                    hash: header.nonleaf.hash,
                    memory_usage: header.nonleaf.memory_usage,
                    children,
                    value,
                }
            }
        }
    }

    fn size_of_allocation(&self) -> usize {
        let mut decoder = self.decoder();
        let kind = decoder.peek::<CommonHeader>().kind;
        match kind {
            NodeKind::Leaf => {
                let header = decoder.decode::<LeafHeader>();
                LeafHeader::SERIALIZED_SIZE
                    + header.extension.flexible_data_length()
                    + header.value.flexible_data_length()
            }
            NodeKind::Extension => {
                let header = decoder.decode::<ExtensionHeader>();
                ExtensionHeader::SERIALIZED_SIZE + header.extension.flexible_data_length()
            }
            NodeKind::Branch => {
                let header = decoder.decode::<BranchHeader>();
                BranchHeader::SERIALIZED_SIZE + header.children.flexible_data_length()
            }
            NodeKind::BranchWithValue => {
                let header = decoder.decode::<BranchWithValueHeader>();
                BranchWithValueHeader::SERIALIZED_SIZE
                    + header.children.flexible_data_length()
                    + header.value.flexible_data_length()
            }
        }
    }
}
