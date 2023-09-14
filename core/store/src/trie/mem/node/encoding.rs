use crate::trie::mem::arena::{ArenaHandle, BorshFixedSize};
use crate::trie::mem::flexible_data::children::EncodedChildrenHeader;
use crate::trie::mem::flexible_data::encoding::{RawDecoder, RawEncoder};
use crate::trie::mem::flexible_data::extension::EncodedExtensionHeader;
use crate::trie::mem::flexible_data::value::EncodedValueHeader;
use crate::trie::mem::flexible_data::FlexibleDataHeader;
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::hash::CryptoHash;

use super::{InputMemTrieNode, MemTrieNode, MemTrieNodeView};

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

#[derive(BorshSerialize, BorshDeserialize, Default)]
pub(crate) struct NonLeafHeader {
    pub(crate) hash: CryptoHash,
    pub(crate) memory_usage: u64,
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

impl MemTrieNode {
    /// Encodes the data.
    pub(crate) fn new_impl(arena: &ArenaHandle, node: InputMemTrieNode) -> Self {
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
                    common: CommonHeader { refcount: 1, kind: NodeKind::Leaf },
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
                    common: CommonHeader { refcount: 1, kind: NodeKind::Extension },
                    nonleaf: NonLeafHeader::default(),
                    child: child.ptr.raw_offset(),
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
                    common: CommonHeader { refcount: 1, kind: NodeKind::Branch },
                    nonleaf: NonLeafHeader::default(),
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
                    common: CommonHeader { refcount: 1, kind: NodeKind::BranchWithValue },
                    nonleaf: NonLeafHeader::default(),
                    children: children_header,
                    value: value_header,
                });
                data.encode_flexible(&children_header, children);
                data.encode_flexible(&value_header, value);
                data.finish()
            }
        };
        Self { ptr: data }
    }

    pub(crate) fn decoder(&self) -> RawDecoder {
        RawDecoder::new(self.ptr.clone())
    }

    /// Decodes the data.
    pub(crate) fn view_impl(&self) -> MemTrieNodeView {
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
                    child: MemTrieNode::from(self.ptr.arena().ptr(header.child)),
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
}
