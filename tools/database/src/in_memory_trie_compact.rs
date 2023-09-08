use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::mem;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use borsh::BorshSerialize;
use near_epoch_manager::EpochManager;
use near_primitives::block::Tip;
use near_primitives::block_header::BlockHeader;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::state::{FlatStateValue, ValueRef};
use near_primitives::types::ShardId;
use near_store::flat::store_helper::decode_flat_state_db_key;
use near_store::trie::{Children, TRIE_COSTS};
use near_store::{DBCol, NibbleSlice, RawTrieNode, RawTrieNodeWithSize, ShardUId, Store, HEAD_KEY};
use nearcore::NearConfig;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};

use crate::utils::{flat_head_state_root, open_rocksdb};

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
#[repr(C, packed(1))]
pub struct TrieNodeRef {
    data: *const u8,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum ParsedTrieNode {
    Leaf {
        value: FlatStateValue,
        extension: Box<[u8]>,
    },
    Extension {
        hash: CryptoHash,
        memory_usage: u64,
        extension: Box<[u8]>,
        child: TrieNodeRef,
    },
    Branch {
        hash: CryptoHash,
        memory_usage: u64,
        children: [Option<TrieNodeRef>; 16],
    },
    BranchWithValue {
        hash: CryptoHash,
        memory_usage: u64,
        children: [Option<TrieNodeRef>; 16],
        value: FlatStateValue,
    },
}

#[repr(C, packed(1))]
pub struct UnalignedCryptoHash {
    pub hash: CryptoHash,
}

impl Debug for UnalignedCryptoHash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.hash.fmt(f)
    }
}

impl From<CryptoHash> for UnalignedCryptoHash {
    fn from(hash: CryptoHash) -> Self {
        Self { hash }
    }
}

#[derive(Debug)]
pub enum TrieNodeView<'a> {
    Leaf {
        value: ValueView<'a>,
        extension: &'a [u8],
    },
    Extension {
        hash: &'a UnalignedCryptoHash,
        memory_usage: u64,
        extension: &'a [u8],
        child: TrieNodeRefHandle<'a>,
    },
    Branch {
        hash: &'a UnalignedCryptoHash,
        memory_usage: u64,
        children: ChildrenView<'a>,
    },
    BranchWithValue {
        hash: &'a UnalignedCryptoHash,
        memory_usage: u64,
        children: ChildrenView<'a>,
        value: ValueView<'a>,
    },
}

impl<'a> TrieNodeView<'a> {
    pub fn node_hash(&self) -> CryptoHash {
        match self {
            Self::Leaf { .. } => {
                let node = self.to_raw_trie_node_with_size();
                hash(&node.try_to_vec().unwrap())
            }
            Self::Extension { hash, .. }
            | Self::Branch { hash, .. }
            | Self::BranchWithValue { hash, .. } => hash.hash,
        }
    }

    pub fn to_raw_trie_node_with_size(&self) -> RawTrieNodeWithSize {
        match self {
            Self::Leaf { value, extension } => {
                let node =
                    RawTrieNode::Leaf(extension.to_vec(), value.to_flat_value().to_value_ref());
                let memory_usage = self.memory_usage();
                RawTrieNodeWithSize { node, memory_usage }
            }
            TrieNodeView::Extension { extension, child, .. } => {
                let node = RawTrieNode::Extension(extension.to_vec(), child.hash());
                let memory_usage = TRIE_COSTS.node_cost
                    + child.memory_usage()
                    + extension.len() as u64 * TRIE_COSTS.byte_of_key;
                RawTrieNodeWithSize { node, memory_usage }
            }
            TrieNodeView::Branch { children, .. } => {
                let node = RawTrieNode::BranchNoValue(children.to_children());
                let mut memory_usage = TRIE_COSTS.node_cost;
                for child in children.children.iter() {
                    memory_usage += child.memory_usage();
                }
                RawTrieNodeWithSize { node, memory_usage }
            }
            TrieNodeView::BranchWithValue { children, value, .. } => {
                let node = RawTrieNode::BranchWithValue(
                    value.to_flat_value().to_value_ref(),
                    children.to_children(),
                );
                let mut memory_usage =
                    TRIE_COSTS.node_cost + value.len() as u64 * TRIE_COSTS.byte_of_value;
                for child in children.children.iter() {
                    memory_usage += child.memory_usage();
                }
                RawTrieNodeWithSize { node, memory_usage }
            }
        }
    }

    pub fn memory_usage(&self) -> u64 {
        match self {
            Self::Leaf { value, extension } => {
                TRIE_COSTS.node_cost
                    + extension.len() as u64 * TRIE_COSTS.byte_of_key
                    + value.len() as u64 * TRIE_COSTS.byte_of_value
                    + TRIE_COSTS.node_cost // yes, twice.
            }
            Self::Extension { memory_usage, .. }
            | Self::Branch { memory_usage, .. }
            | Self::BranchWithValue { memory_usage, .. } => *memory_usage,
        }
    }

    pub fn to_parsed(&self) -> ParsedTrieNode {
        match self {
            Self::Leaf { value, extension } => ParsedTrieNode::Leaf {
                value: value.to_flat_value(),
                extension: extension.to_vec().into_boxed_slice(),
            },
            Self::Extension { hash, memory_usage, extension, child } => ParsedTrieNode::Extension {
                hash: hash.hash,
                memory_usage: *memory_usage,
                extension: extension.to_vec().into_boxed_slice(),
                child: child.node,
            },
            Self::Branch { hash, memory_usage, children } => ParsedTrieNode::Branch {
                hash: hash.hash,
                memory_usage: *memory_usage,
                children: children.to_parsed(),
            },
            Self::BranchWithValue { hash, memory_usage, children, value } => {
                ParsedTrieNode::BranchWithValue {
                    hash: hash.hash,
                    memory_usage: *memory_usage,
                    children: children.to_parsed(),
                    value: value.to_flat_value(),
                }
            }
        }
    }
}

trait FlexibleDataHeader {
    type InputData: ?Sized;
    type View<'a>
    where
        Self: 'a;
    fn from_input(data: &Self::InputData) -> Self;
    fn flexible_data_length(&self) -> usize;
    unsafe fn encode_flexible_data(&self, data: &Self::InputData, ptr: *mut u8);
    unsafe fn decode_flexible_data<'a>(&'a self, ptr: *const u8) -> Self::View<'a>;
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct ChildrenView<'a> {
    mask: u16,
    children: &'a [TrieNodeRefHandle<'a>],
}

impl<'a> ChildrenView<'a> {
    fn len(&self) -> usize {
        self.mask.count_ones() as usize
    }

    pub fn get(&self, i: usize) -> Option<TrieNodeRefHandle<'a>> {
        assert!(i < 16);
        let bit = 1u16 << (i as u16);
        if self.mask & bit == 0 {
            None
        } else {
            let lower_mask = self.mask & (bit - 1);
            let index = lower_mask.count_ones() as usize;
            Some(self.children[index])
        }
    }

    fn to_children(&self) -> Children {
        let mut children = Children::default();
        let mut j = 0;
        for i in 0..16 {
            if self.mask & (1 << i) != 0 {
                children.0[i] = Some(self.children[j].hash());
                j += 1;
            }
        }
        children
    }

    fn to_parsed(&self) -> [Option<TrieNodeRef>; 16] {
        let mut children = [None; 16];
        let mut j = 0;
        for i in 0..16 {
            if self.mask & (1 << i) != 0 {
                children[i] = Some(self.children[j].node);
                j += 1;
            }
        }
        children
    }
}

#[derive(Debug)]
pub enum ValueView<'a> {
    Ref { length: u32, hash: &'a UnalignedCryptoHash },
    Inlined(&'a [u8]),
}

impl<'a> ValueView<'a> {
    pub fn to_flat_value(&self) -> FlatStateValue {
        match self {
            Self::Ref { length, hash } => {
                FlatStateValue::Ref(ValueRef { length: *length, hash: hash.hash })
            }
            Self::Inlined(data) => FlatStateValue::Inlined(data.to_vec()),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Ref { length, .. } => *length as usize,
            Self::Inlined(data) => data.len(),
        }
    }
}

#[repr(C, packed(1))]
#[derive(Clone, Copy)]
struct EncodedValueHeader {
    length: u32,
}

impl EncodedValueHeader {
    fn decode(&self) -> (u32, bool) {
        (self.length & 0x7fffffff, self.length & 0x80000000 != 0)
    }
}

impl FlexibleDataHeader for EncodedValueHeader {
    type InputData = FlatStateValue;
    type View<'a> = ValueView<'a>;

    fn from_input(value: &FlatStateValue) -> Self {
        match value {
            FlatStateValue::Ref(value_ref) => EncodedValueHeader { length: value_ref.length },
            FlatStateValue::Inlined(v) => {
                EncodedValueHeader { length: 0x80000000 | v.len() as u32 }
            }
        }
    }

    fn flexible_data_length(&self) -> usize {
        let (length, inlined) = self.decode();
        if inlined {
            length as usize
        } else {
            mem::size_of::<CryptoHash>()
        }
    }

    unsafe fn encode_flexible_data(&self, value: &FlatStateValue, ptr: *mut u8) {
        let (length, inlined) = self.decode();
        match value {
            FlatStateValue::Ref(value_ref) => {
                assert!(!inlined);
                *(ptr as *mut CryptoHash) = value_ref.hash;
            }
            FlatStateValue::Inlined(v) => {
                assert!(inlined);
                std::ptr::copy_nonoverlapping(v.as_ptr(), ptr, length as usize);
            }
        }
    }

    unsafe fn decode_flexible_data<'a>(&'a self, ptr: *const u8) -> ValueView<'a> {
        let (length, inlined) = self.decode();
        if inlined {
            ValueView::Inlined(std::slice::from_raw_parts(ptr, length as usize))
        } else {
            ValueView::Ref { length, hash: &*(ptr as *const UnalignedCryptoHash) }
        }
    }
}

#[repr(C, packed(1))]
#[derive(Clone, Copy)]
struct EncodedChildrenHeader {
    mask: u16,
}

impl FlexibleDataHeader for EncodedChildrenHeader {
    type InputData = [Option<TrieNodeRef>; 16];
    type View<'a> = ChildrenView<'a>;
    fn from_input(children: &[Option<TrieNodeRef>; 16]) -> EncodedChildrenHeader {
        let mut mask = 0u16;
        for i in 0..16 {
            if children[i].is_some() {
                mask |= 1 << i;
            }
        }
        EncodedChildrenHeader { mask }
    }

    fn flexible_data_length(&self) -> usize {
        self.mask.count_ones() as usize * mem::size_of::<TrieNodeRef>()
    }

    unsafe fn encode_flexible_data(&self, children: &[Option<TrieNodeRef>; 16], mut ptr: *mut u8) {
        for i in 0..16 {
            if self.mask & (1 << i) != 0 {
                *(ptr as *mut TrieNodeRef) = children[i].unwrap();
                ptr = ptr.offset(mem::size_of::<TrieNodeRef>() as isize);
            }
        }
    }

    unsafe fn decode_flexible_data<'a>(&'a self, ptr: *const u8) -> ChildrenView<'a> {
        ChildrenView {
            children: std::slice::from_raw_parts(
                ptr as *const TrieNodeRefHandle<'a>,
                self.mask.count_ones() as usize,
            ),
            mask: self.mask,
        }
    }
}

#[repr(C, packed(1))]
#[derive(Clone, Copy)]
struct EncodedExtensionHeader {
    length: u16,
}

impl FlexibleDataHeader for EncodedExtensionHeader {
    type InputData = [u8];
    type View<'a> = &'a [u8];
    fn from_input(extension: &[u8]) -> EncodedExtensionHeader {
        EncodedExtensionHeader { length: extension.len() as u16 }
    }

    fn flexible_data_length(&self) -> usize {
        self.length as usize
    }

    unsafe fn encode_flexible_data(&self, extension: &[u8], ptr: *mut u8) {
        std::ptr::copy_nonoverlapping(extension.as_ptr(), ptr, self.length as usize);
    }

    unsafe fn decode_flexible_data<'a>(&'a self, ptr: *const u8) -> &'a [u8] {
        std::slice::from_raw_parts(ptr, self.length as usize)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TrieNodeRefHandle<'a> {
    node: TrieNodeRef,
    _marker: std::marker::PhantomData<&'a ()>,
}

#[repr(u8)]
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
enum NodeKind {
    Leaf,
    Extension,
    Branch,
    BranchWithValue,
}

#[repr(C, packed(1))]
struct CommonHeader {
    refcount: u32,
    kind: NodeKind,
}

#[repr(C, packed(1))]
struct NonLeafHeader {
    hash: UnalignedCryptoHash,
    memory_usage: u64,
}

#[repr(C, packed(1))]
struct LeafHeader {
    common: CommonHeader,
    value: EncodedValueHeader,
    extension: EncodedExtensionHeader,
}

#[repr(C, packed(1))]
struct ExtensionHeader {
    common: CommonHeader,
    nonleaf: NonLeafHeader,
    child: TrieNodeRef,
    extension: EncodedExtensionHeader,
}

#[repr(C, packed(1))]
struct BranchHeader {
    common: CommonHeader,
    nonleaf: NonLeafHeader,
    children: EncodedChildrenHeader,
}

#[repr(C, packed(1))]
struct BranchWithValueHeader {
    common: CommonHeader,
    nonleaf: NonLeafHeader,
    value: EncodedValueHeader,
    children: EncodedChildrenHeader,
}

struct RawEncoder {
    data: Box<[u8]>,
    pos: usize,
}

impl RawEncoder {
    pub fn new(n: usize) -> RawEncoder {
        let mut data = Vec::<u8>::new();
        data.reserve_exact(n);
        unsafe {
            data.set_len(n);
        }
        RawEncoder { data: data.into_boxed_slice(), pos: 0 }
    }

    unsafe fn ptr(&mut self) -> *mut u8 {
        self.data.as_mut_ptr().offset(self.pos as isize)
    }

    pub unsafe fn encode<T>(&mut self, data: T) {
        assert!(self.pos + mem::size_of::<T>() <= self.data.len());
        *(self.ptr() as *mut T) = data;
        self.pos += mem::size_of::<T>();
    }

    pub unsafe fn encode_flexible<T: FlexibleDataHeader>(
        &mut self,
        header: &T,
        data: &T::InputData,
    ) {
        let length = header.flexible_data_length();
        assert!(self.pos + length <= self.data.len());
        header.encode_flexible_data(data, self.ptr());
        self.pos += length;
    }

    pub unsafe fn finish(self) -> *const u8 {
        assert_eq!(self.pos, self.data.len());
        Box::into_raw(self.data) as *const u8
    }
}

struct RawDecoder<'a> {
    data: *const u8,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> RawDecoder<'a> {
    pub fn new(data: *const u8) -> RawDecoder<'a> {
        RawDecoder { data, _marker: std::marker::PhantomData }
    }

    pub unsafe fn decode<T>(&mut self) -> &'a T {
        let result = &*(self.data as *const T);
        self.data = self.data.offset(mem::size_of::<T>() as isize);
        result
    }

    pub unsafe fn decode_as_mut<T>(&mut self) -> &'a mut T {
        let result = &mut *(self.data as *mut T);
        self.data = self.data.offset(mem::size_of::<T>() as isize);
        result
    }

    pub unsafe fn peek<T>(&mut self) -> &'a T {
        &*(self.data as *const T)
    }

    pub unsafe fn decode_flexible<T: FlexibleDataHeader>(&mut self, header: &'a T) -> T::View<'a> {
        let length = header.flexible_data_length();
        let view = header.decode_flexible_data(self.data);
        self.data = self.data.offset(length as isize);
        view
    }
}

impl TrieNodeRef {
    pub fn new_from_parsed(node: ParsedTrieNode) -> Self {
        let data = match node {
            ParsedTrieNode::Leaf { value, extension } => {
                let extension_header = EncodedExtensionHeader::from_input(&extension);
                let value_header = EncodedValueHeader::from_input(&value);
                let mut data = RawEncoder::new(
                    mem::size_of::<LeafHeader>()
                        + extension_header.flexible_data_length()
                        + value_header.flexible_data_length(),
                );
                unsafe {
                    data.encode(LeafHeader {
                        common: CommonHeader { refcount: 0, kind: NodeKind::Leaf },
                        extension: extension_header,
                        value: value_header,
                    });
                    data.encode_flexible(&extension_header, &extension);
                    data.encode_flexible(&value_header, &value);
                    data.finish()
                }
            }
            ParsedTrieNode::Extension { hash, memory_usage, extension, child } => {
                let extension_header = EncodedExtensionHeader::from_input(&extension);
                let mut data = RawEncoder::new(
                    mem::size_of::<ExtensionHeader>() + extension_header.flexible_data_length(),
                );
                unsafe {
                    data.encode(ExtensionHeader {
                        common: CommonHeader { refcount: 0, kind: NodeKind::Extension },
                        nonleaf: NonLeafHeader { hash: hash.into(), memory_usage },
                        child,
                        extension: extension_header,
                    });
                    data.encode_flexible(&extension_header, &extension);
                    data.finish()
                }
            }
            ParsedTrieNode::Branch { hash, memory_usage, children } => {
                let children_header = EncodedChildrenHeader::from_input(&children);
                let mut data = RawEncoder::new(
                    mem::size_of::<BranchHeader>() + children_header.flexible_data_length(),
                );
                unsafe {
                    data.encode(BranchHeader {
                        common: CommonHeader { refcount: 0, kind: NodeKind::Branch },
                        nonleaf: NonLeafHeader { hash: hash.into(), memory_usage },
                        children: children_header,
                    });
                    data.encode_flexible(&children_header, &children);
                    data.finish()
                }
            }
            ParsedTrieNode::BranchWithValue { hash, memory_usage, children, value } => {
                let children_header = EncodedChildrenHeader::from_input(&children);
                let value_header = EncodedValueHeader::from_input(&value);
                let mut data = RawEncoder::new(
                    mem::size_of::<BranchWithValueHeader>()
                        + children_header.flexible_data_length()
                        + value_header.flexible_data_length(),
                );
                unsafe {
                    data.encode(BranchWithValueHeader {
                        common: CommonHeader { refcount: 0, kind: NodeKind::BranchWithValue },
                        nonleaf: NonLeafHeader { hash: hash.into(), memory_usage },
                        children: children_header,
                        value: value_header,
                    });
                    data.encode_flexible(&children_header, &children);
                    data.encode_flexible(&value_header, &value);
                    data.finish()
                }
            }
        };
        Self { data }
    }

    fn delete(&self) {
        let data = self.data;
        unsafe {
            let refcount = *(data as *const u32);
            assert_eq!(refcount, 0, "Dropping TrieNodeRef {:p} with non-zero refcount", data);
            let alloc_size =
                TrieNodeRefHandle { node: *self, _marker: std::marker::PhantomData }.alloc_size();
            drop(Box::from_raw(
                std::slice::from_raw_parts_mut(data as *mut u8, alloc_size) as *mut [u8]
            ));
        }
    }
}

impl<'a> TrieNodeRefHandle<'a> {
    fn decoder(&self) -> RawDecoder<'a> {
        RawDecoder::new(self.node.data)
    }

    pub fn view(self) -> TrieNodeView<'a> {
        unsafe {
            let mut decoder = self.decoder();
            let kind = decoder.peek::<CommonHeader>().kind;
            match kind {
                NodeKind::Leaf => {
                    let header = decoder.decode::<LeafHeader>();
                    let extension = decoder.decode_flexible(&header.extension);
                    let value = decoder.decode_flexible(&header.value);
                    TrieNodeView::Leaf { extension, value }
                }
                NodeKind::Extension => {
                    let header = decoder.decode::<ExtensionHeader>();
                    let extension = decoder.decode_flexible(&header.extension);
                    let child = TrieNodeRefHandle::<'a> {
                        node: header.child,
                        _marker: std::marker::PhantomData,
                    };
                    TrieNodeView::Extension {
                        hash: &header.nonleaf.hash,
                        memory_usage: header.nonleaf.memory_usage,
                        extension,
                        child,
                    }
                }
                NodeKind::Branch => {
                    let header = decoder.decode::<BranchHeader>();
                    let children = decoder.decode_flexible(&header.children);
                    TrieNodeView::Branch {
                        hash: &header.nonleaf.hash,
                        memory_usage: header.nonleaf.memory_usage,
                        children,
                    }
                }
                NodeKind::BranchWithValue => {
                    let header = decoder.decode::<BranchWithValueHeader>();
                    let children = decoder.decode_flexible(&header.children);
                    let value = decoder.decode_flexible(&header.value);
                    TrieNodeView::BranchWithValue {
                        hash: &header.nonleaf.hash,
                        memory_usage: header.nonleaf.memory_usage,
                        children,
                        value,
                    }
                }
            }
        }
    }

    pub fn add_ref(&self, stats: &mut TrieStats) {
        let mut decoder = self.decoder();
        let header = unsafe { decoder.decode_as_mut::<CommonHeader>() };
        header.refcount += 1;
        if header.refcount == 1 {
            self.increment_stats(stats, 1);
            // If this is the first time we ref the node (basically as
            // a result of retaining it as a state root), add ref to
            // its children as well.
            for child in self.iter_children() {
                child.add_ref(stats);
            }
        }
    }

    pub fn deref(&self, stats: &mut TrieStats) {
        let mut decoder = self.decoder();
        let header = unsafe { decoder.decode_as_mut::<CommonHeader>() };
        header.refcount -= 1;
        if header.refcount == 0 {
            // If dereference is down to zero, deref children, and delete self.
            for child in self.iter_children() {
                child.deref(stats);
            }
            self.increment_stats(stats, -1);
            self.node.delete();
        }
    }

    fn node_kind(&self) -> NodeKind {
        let mut decoder = self.decoder();
        unsafe { decoder.peek::<CommonHeader>().kind }
    }

    fn hash(&self) -> CryptoHash {
        self.view().node_hash()
    }

    fn memory_usage(&self) -> u64 {
        self.view().memory_usage()
    }

    fn alloc_size(&self) -> usize {
        unsafe {
            let mut decoder = self.decoder();
            match decoder.peek::<CommonHeader>().kind {
                NodeKind::Leaf => {
                    let header = decoder.decode::<LeafHeader>();
                    mem::size_of::<LeafHeader>()
                        + header.extension.flexible_data_length()
                        + header.value.flexible_data_length()
                }
                NodeKind::Extension => {
                    let header = decoder.decode::<ExtensionHeader>();
                    mem::size_of::<ExtensionHeader>() + header.extension.flexible_data_length()
                }
                NodeKind::Branch => {
                    let header = decoder.decode::<BranchHeader>();
                    mem::size_of::<BranchHeader>() + header.children.flexible_data_length()
                }
                NodeKind::BranchWithValue => {
                    let header = decoder.decode::<BranchWithValueHeader>();
                    mem::size_of::<BranchWithValueHeader>()
                        + header.children.flexible_data_length()
                        + header.value.flexible_data_length()
                }
            }
        }
    }

    fn iter_children(&self) -> Box<dyn Iterator<Item = TrieNodeRefHandle<'a>> + 'a> {
        match self.view() {
            TrieNodeView::Leaf { .. } => Box::new(std::iter::empty()),
            TrieNodeView::Extension { child, .. } => Box::new(std::iter::once(child)),
            TrieNodeView::Branch { children, .. }
            | TrieNodeView::BranchWithValue { children, .. } => {
                Box::new(children.children.iter().copied())
            }
        }
    }

    fn increment_stats(&self, stats: &mut TrieStats, multiplier: isize) {
        let view = self.view();
        if let TrieNodeView::Leaf { value, .. } | TrieNodeView::BranchWithValue { value, .. } =
            &view
        {
            match value {
                ValueView::Ref { .. } => {
                    stats.ref_value_count += multiplier;
                }
                ValueView::Inlined(value) => {
                    stats.inlined_value_count += multiplier;
                    stats.inlined_value_bytes += value.len() as isize * multiplier;
                }
            }
        }
        if let TrieNodeView::Branch { children, .. }
        | TrieNodeView::BranchWithValue { children, .. } = &view
        {
            stats.children_ptr_count += children.len() as isize * multiplier;
        }
        if let TrieNodeView::Leaf { extension, .. } | TrieNodeView::Extension { extension, .. } =
            &view
        {
            stats.extension_total_bytes += extension.len() as isize * multiplier;
        }
        match view {
            TrieNodeView::Leaf { .. } => stats.leaf_node_count += multiplier,
            TrieNodeView::Extension { .. } => stats.extension_node_count += multiplier,
            TrieNodeView::Branch { .. } => stats.branch_node_count += multiplier,
            TrieNodeView::BranchWithValue { .. } => {
                stats.branch_nodes_with_value_count += multiplier;
            }
        }
    }
}

#[derive(Default, Debug)]
pub struct TrieStats {
    leaf_node_count: isize,
    extension_node_count: isize,
    branch_node_count: isize,
    branch_nodes_with_value_count: isize,

    inlined_value_count: isize,
    inlined_value_bytes: isize,
    ref_value_count: isize,
    children_ptr_count: isize,
    extension_total_bytes: isize,
}

pub struct TrieRootNode {
    pub root: TrieNodeRef,
}

impl TrieRootNode {
    fn new(root: TrieNodeRef, stats: &mut TrieStats) -> Self {
        let result = Self { root };
        result.handle().add_ref(stats);
        result
    }

    fn drop(self, stats: &mut TrieStats) {
        self.handle().deref(stats);
        std::mem::forget(self)
    }

    pub fn handle<'a>(&'a self) -> TrieNodeRefHandle<'a> {
        TrieNodeRefHandle { node: self.root, _marker: std::marker::PhantomData }
    }
}

impl Drop for TrieRootNode {
    fn drop(&mut self) {
        panic!("Must be dropped via TrieRootNode::drop");
    }
}

pub struct InMemoryTrie {
    pub roots: HashMap<CryptoHash, TrieRootNode>,
    pub stats: TrieStats,
}

impl Drop for InMemoryTrie {
    fn drop(&mut self) {
        for (_, root) in std::mem::take(&mut self.roots) {
            root.drop(&mut self.stats);
        }
    }
}

struct TrieConstructionState {
    segments: Vec<TrieConstructionSegment>,
}

impl Debug for TrieConstructionState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TrieConstructionState {{\n")?;
        for segment in &self.segments {
            write!(f, "  {:?}\n", segment)?;
        }
        write!(f, "}}")
    }
}

struct TrieConstructionSegment {
    is_branch: bool,
    trail: Vec<u8>,
    leaf: Option<FlatStateValue>,
    children: Vec<(u8, TrieNodeRef)>,
    child: Option<TrieNodeRef>,
}

impl Debug for TrieConstructionSegment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TrieConstructionSegment {{\n")?;
        write!(f, "  is_branch: {:?}\n", self.is_branch)?;
        write!(f, "  trail: {:?}\n", NibbleSlice::from_encoded(&self.trail).0)?;
        write!(f, "  leaf: {:?}\n", self.leaf)?;
        write!(f, "  children: {:?}\n", self.children)?;
        write!(f, "  child: {:?}\n", self.child)?;
        write!(f, "}}")
    }
}

impl TrieConstructionSegment {
    pub fn new_branch(initial_trail: u8) -> Self {
        Self {
            is_branch: true,
            trail: vec![0x10 + initial_trail],
            leaf: None,
            children: Vec::new(),
            child: None,
        }
    }

    pub fn new_extension(trail: Vec<u8>) -> Self {
        let nibbles = NibbleSlice::from_encoded(&trail);
        assert!(nibbles.1 || nibbles.0.len() > 0);
        Self { is_branch: false, trail, leaf: None, children: Vec::new(), child: None }
    }

    pub fn is_leaf(&self) -> bool {
        self.leaf.is_some() && !self.is_branch
    }

    pub fn into_node(self) -> TrieNodeRef {
        let parsed_node: ParsedTrieNode = if self.is_branch {
            assert!(!self.children.is_empty());
            assert!(self.child.is_none());
            let mut children: [Option<TrieNodeRef>; 16] = [None; 16];
            for (i, child) in self.children.into_iter() {
                children[i as usize] = Some(child);
            }
            if let Some(leaf) = self.leaf {
                ParsedTrieNode::BranchWithValue {
                    hash: CryptoHash::default(),
                    memory_usage: 0,
                    children,
                    value: leaf,
                }
            } else {
                ParsedTrieNode::Branch { hash: CryptoHash::default(), memory_usage: 0, children }
            }
        } else if let Some(leaf) = self.leaf {
            assert!(self.child.is_none());
            assert!(self.children.is_empty());
            ParsedTrieNode::Leaf { value: leaf, extension: self.trail.into_boxed_slice() }
        } else {
            assert!(self.child.is_some());
            assert!(self.children.is_empty());
            ParsedTrieNode::Extension {
                hash: CryptoHash::default(),
                memory_usage: 0,
                extension: self.trail.into_boxed_slice(),
                child: self.child.unwrap(),
            }
        };
        TrieNodeRef::new_from_parsed(parsed_node)
    }
}

impl TrieConstructionState {
    pub fn new() -> Self {
        Self { segments: vec![] }
    }

    fn pop_segment(&mut self) {
        let segment = self.segments.pop().unwrap();
        let node = segment.into_node();
        let parent = self.segments.last_mut().unwrap();
        if parent.is_branch {
            parent.children.push((NibbleSlice::from_encoded(&parent.trail).0.at(0), node));
        } else {
            assert!(parent.child.is_none());
            parent.child = Some(node);
        }
    }

    fn add_leaf(&mut self, key: &[u8], value: FlatStateValue) {
        // println!("State before adding leaf {:?}: {:?}", key, self);
        let mut nibbles = NibbleSlice::new(key);
        let mut i = 0;
        while i < self.segments.len() {
            // println!("    Sweeping index {} nibble path {:?}", i, nibbles);
            // We can't be inserting a prefix into the existing path because that
            // would violate ordering.
            assert!(nibbles.len() > 0);

            let segment = &self.segments[i];
            let (extension_nibbles, _) = NibbleSlice::from_encoded(&segment.trail);
            let common_prefix_len = nibbles.common_prefix(&extension_nibbles);
            if common_prefix_len == extension_nibbles.len() {
                nibbles = nibbles.mid(common_prefix_len);
                i += 1;
                continue;
            }

            // pop off all the extra; they have no chance to be relevant to the
            // leaf we're inserting.
            while i < self.segments.len() - 1 {
                self.pop_segment();
            }

            // If we have a common prefix, split that first.
            if common_prefix_len > 0 {
                // println!("      Splitting extension path in the middle");
                let mut segment = self.segments.pop().unwrap();
                assert!(!segment.is_branch);
                let (extension_nibbles, was_leaf) = NibbleSlice::from_encoded(&segment.trail);
                assert_eq!(was_leaf, segment.is_leaf());
                assert_eq!(was_leaf, segment.child.is_none());

                let top_segment = TrieConstructionSegment::new_extension(
                    extension_nibbles.encoded_leftmost(common_prefix_len, false).to_vec(),
                );
                segment.trail = extension_nibbles.mid(common_prefix_len).encoded(was_leaf).to_vec();
                self.segments.push(top_segment);
                self.segments.push(segment);
                nibbles = nibbles.mid(common_prefix_len);
            }

            // Now, we know that the last segment has no overlap with the leaf.
            if self.segments.last().unwrap().is_branch {
                // If it's a branch then just add another branch.
                self.segments.last_mut().unwrap().trail =
                    nibbles.encoded_leftmost(1, false).to_vec();
                nibbles = nibbles.mid(1);
                break;
            } else {
                // Otherwise we need to split the extension.
                let segment = self.segments.pop().unwrap();
                let (extension_nibbles, was_leaf) = NibbleSlice::from_encoded(&segment.trail);
                assert_eq!(was_leaf, segment.is_leaf());
                assert_eq!(was_leaf, segment.child.is_none());

                let mut top_segment = TrieConstructionSegment::new_branch(extension_nibbles.at(0));
                if extension_nibbles.len() > 1 {
                    let mut bottom_segment = TrieConstructionSegment::new_extension(
                        extension_nibbles.mid(1).encoded(was_leaf).to_vec(),
                    );
                    bottom_segment.leaf = segment.leaf;
                    bottom_segment.child = segment.child;
                    self.segments.push(top_segment);
                    self.segments.push(bottom_segment);
                    self.pop_segment();
                } else if was_leaf {
                    let mut bottom_segment = TrieConstructionSegment::new_extension(
                        extension_nibbles.mid(extension_nibbles.len()).encoded(true).to_vec(),
                    );
                    bottom_segment.leaf = segment.leaf;
                    self.segments.push(top_segment);
                    self.segments.push(bottom_segment);
                    self.pop_segment();
                } else {
                    top_segment.children.push((extension_nibbles.at(0), segment.child.unwrap()));
                    self.segments.push(top_segment);
                }
                self.segments.last_mut().unwrap().trail =
                    nibbles.encoded_leftmost(1, false).to_vec();
                nibbles = nibbles.mid(1);
                break;
            }
        }
        // When we exit the loop, either we exited because we ran out of segments
        // (in which case this leaf has the previous leaf as a prefix) or we
        // exited in the middle and we've just added a new branch.
        // println!("      Adding nibbles {:?} in the end", nibbles);
        if !self.segments.is_empty() && self.segments.last().unwrap().is_leaf() {
            // This is the case where we ran out of segments.
            assert!(nibbles.len() > 0);
            // We need to turn the leaf node into an extension node, add a branch node
            // to store the previous leaf, and add the new leaf in.
            let segment = self.segments.pop().unwrap();
            let (extension_nibbles, was_leaf) = NibbleSlice::from_encoded(&segment.trail);
            assert!(was_leaf);
            if extension_nibbles.len() > 0 {
                // Only make an extension segment if it was a leaf with an extension.
                let top_segment = TrieConstructionSegment::new_extension(
                    extension_nibbles.encoded(false).to_vec(),
                );
                self.segments.push(top_segment);
            }
            let mut mid_segment = TrieConstructionSegment::new_branch(nibbles.at(0));
            mid_segment.leaf = segment.leaf;
            let mut bottom_segment =
                TrieConstructionSegment::new_extension(nibbles.mid(1).encoded(true).to_vec());
            bottom_segment.leaf = Some(value);
            self.segments.push(mid_segment);
            self.segments.push(bottom_segment);
        } else {
            // Otherwise we're at one branch of a branch node (or we're at root),
            // so just add the leaf.
            let mut segment =
                TrieConstructionSegment::new_extension(nibbles.encoded(true).to_vec());
            segment.leaf = Some(value);
            self.segments.push(segment);
        }
    }

    pub fn finalize(mut self, stats: &mut TrieStats) -> TrieRootNode {
        while self.segments.len() > 1 {
            self.pop_segment();
        }
        TrieRootNode::new(self.segments.into_iter().next().unwrap().into_node(), stats)
    }
}

#[derive(clap::Parser)]
pub struct CompactInMemoryTrieCmd {
    #[clap(long)]
    shard_id: ShardId,
}

impl CompactInMemoryTrieCmd {
    pub fn run(&self, near_config: NearConfig, home: &Path) -> anyhow::Result<()> {
        let rocksdb = Arc::new(open_rocksdb(home, near_store::Mode::ReadOnly)?);
        let store = near_store::NodeStorage::new(rocksdb.clone()).get_hot_store();
        let genesis_config = &near_config.genesis.config;
        // Note: this is not necessarily correct; it's just an estimate of the shard layout.
        let head =
            store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY).unwrap().unwrap().last_block_hash;
        let block_header = store
            .get_ser::<BlockHeader>(DBCol::BlockHeader, &head.try_to_vec().unwrap())?
            .ok_or_else(|| anyhow::anyhow!("Block header not found"))?;
        let epoch_manager =
            EpochManager::new_from_genesis_config(store.clone(), &genesis_config).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(block_header.epoch_id()).unwrap();

        let shard_uid = ShardUId::from_shard_id_and_layout(self.shard_id, &shard_layout);
        let state_root = flat_head_state_root(&store, &shard_uid);

        let trie = load_trie_from_flat_state(&store, shard_uid, state_root)?;
        println!("Stats: {:?}", trie.stats);
        for _ in 0..1000000 {
            std::thread::sleep(Duration::from_secs(100));
        }
        Ok(())
    }
}

fn print_trie<'a>(node: TrieNodeRefHandle<'a>, indent: usize) {
    match node.view() {
        TrieNodeView::Leaf { value, extension } => {
            println!(
                "{}Leaf {:?} {:?}",
                " ".repeat(indent),
                NibbleSlice::from_encoded(extension).0,
                value.to_flat_value(),
            );
        }
        TrieNodeView::Extension { hash, memory_usage, extension, child } => {
            println!(
                "{}Extension {:?} {:?} mem={}",
                " ".repeat(indent),
                NibbleSlice::from_encoded(extension).0,
                hash,
                memory_usage,
            );
            print_trie(child, indent + 2);
        }
        TrieNodeView::Branch { hash, memory_usage, children } => {
            println!("{}Branch {:?} mem={}", " ".repeat(indent), hash, memory_usage);
            for i in 0..16 {
                if let Some(child) = children.get(i) {
                    println!("{}  {:x}: ", " ".repeat(indent), i);
                    print_trie(child, indent + 4);
                }
            }
        }
        TrieNodeView::BranchWithValue { hash, memory_usage, children, value } => {
            println!(
                "{}BranchWithValue {:?} {:?} mem={}",
                " ".repeat(indent),
                value.to_flat_value(),
                hash,
                memory_usage
            );
            for i in 0..16 {
                if let Some(child) = children.get(i) {
                    println!("{}  {:x}: ", " ".repeat(indent), i);
                    print_trie(child, indent + 4);
                }
            }
        }
    }
}

fn load_trie_from_flat_state(
    store: &Store,
    shard_uid: ShardUId,
    state_root: CryptoHash,
) -> anyhow::Result<InMemoryTrie> {
    println!("Loading trie from flat state...");
    let load_start = Instant::now();
    let mut recon = TrieConstructionState::new();
    let mut loaded = 0;
    for item in
        store.iter_prefix_ser::<FlatStateValue>(DBCol::FlatState, &shard_uid.try_to_vec().unwrap())
    {
        let (key, value) = item?;
        let (_, key) = decode_flat_state_db_key(&key)?;
        recon.add_leaf(&key, value);
        loaded += 1;
        if loaded % 1000000 == 0 {
            println!(
                "[{:?}] Loaded {} keys, current key: {}",
                load_start.elapsed(),
                loaded,
                hex::encode(&key)
            );
        }
    }
    let mut stats = TrieStats::default();
    let root = recon.finalize(&mut stats);
    let root_handle = root.handle();

    println!(
        "[{:?}] Loaded {} keys; computing hash and memory usage...",
        load_start.elapsed(),
        loaded
    );
    let mut subtrees = Vec::new();
    let (_, total_nodes) =
        root_handle.compute_subtree_node_count_and_mark_boundary_subtrees(1000, &mut subtrees);
    println!("[{:?}] Total node count = {}, parallel subtree count = {}, going to compute hash and memory for subtrees", load_start.elapsed(), total_nodes, subtrees.len());
    subtrees.into_par_iter().for_each(|subtree| {
        subtree.compute_hash_and_memory_usage_recursively();
    });
    println!(
        "[{:?}] Done computing hash and memory usage for subtrees; now computing root hash",
        load_start.elapsed()
    );
    root_handle.compute_hash_and_memory_usage_recursively();
    if root_handle.hash() != state_root {
        println!(
            "[{:?}] State root mismatch: expected {:?}, actual {:?}",
            load_start.elapsed(),
            state_root,
            root_handle.hash()
        );
    } else {
        println!("[{:?}] Done loading trie from flat state", load_start.elapsed());
    }

    Ok(InMemoryTrie { roots: HashMap::from_iter([(state_root, root)].into_iter()), stats })
}

unsafe impl Send for TrieNodeRef {}
unsafe impl Sync for TrieNodeRef {}

impl<'a> TrieNodeRefHandle<'a> {
    fn compute_subtree_node_count_and_mark_boundary_subtrees(
        &self,
        threshold: usize,
        trees: &mut Vec<TrieNodeRefHandle<'a>>,
    ) -> (BoundaryNodeType, usize) {
        let mut total = 1;
        let mut any_children_above_or_at_boundary = false;
        let mut children_below_boundary = Vec::new();

        for child in self.iter_children() {
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
            trees.push(*self);
        }
        if total >= threshold {
            (BoundaryNodeType::AboveOrAtBoundary, total)
        } else {
            (BoundaryNodeType::BelowBoundary, total)
        }
    }

    pub fn compute_hash_and_memory_usage_recursively(&self) {
        if self.node_kind() == NodeKind::Leaf {
            return;
        }
        let mut decoder = self.decoder();
        let header = unsafe {
            decoder.decode::<CommonHeader>();
            decoder.decode_as_mut::<NonLeafHeader>()
        };
        if header.memory_usage > 0 {
            return;
        }

        for child in self.iter_children() {
            child.compute_hash_and_memory_usage_recursively();
        }
        let raw_node_with_size = self.view().to_raw_trie_node_with_size();
        // the header has same layout as other non-leaf headers for hash and memory usage
        header.hash = hash(&raw_node_with_size.try_to_vec().unwrap()).into();
        header.memory_usage = raw_node_with_size.memory_usage;
    }
}

pub enum BoundaryNodeType {
    AboveOrAtBoundary,
    BelowBoundary,
}

#[cfg(test)]
mod tests {
    use near_primitives::hash::{hash, CryptoHash};
    use near_store::test_utils::{
        create_tries, simplify_changes, test_populate_flat_storage, test_populate_trie,
    };
    use near_store::{NibbleSlice, ShardUId, Trie, TrieUpdate};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use crate::in_memory_trie_compact::{
        load_trie_from_flat_state, print_trie, TrieNodeRef, TrieRootNode,
    };
    use crate::in_memory_trie_lookup::InMemoryTrieCompact;

    use super::{FlatStateValue, ParsedTrieNode, TrieConstructionState};

    #[test]
    fn test_basic_reconstruction() {
        let mut rec = TrieConstructionState::new();
        rec.add_leaf(b"aaaaa", FlatStateValue::Inlined(b"a".to_vec()));
        rec.add_leaf(b"aaaab", FlatStateValue::Inlined(b"b".to_vec()));
        rec.add_leaf(b"ab", FlatStateValue::Inlined(b"c".to_vec()));
        rec.add_leaf(b"abffff", FlatStateValue::Inlined(b"c".to_vec()));
        let root = rec.finalize(&mut Default::default());
        print_trie(root.handle(), 0);
        root.drop(&mut Default::default());
    }

    fn check(keys: Vec<Vec<u8>>) {
        let shard_tries = create_tries();
        let shard_uid = ShardUId::single_shard();
        let changes = keys.iter().map(|key| (key.to_vec(), Some(key.to_vec()))).collect::<Vec<_>>();
        let changes = simplify_changes(&changes);
        test_populate_flat_storage(
            &shard_tries,
            shard_uid,
            &CryptoHash::default(),
            &CryptoHash::default(),
            &changes,
        );
        let state_root =
            test_populate_trie(&shard_tries, &Trie::EMPTY_ROOT, shard_uid, changes.clone());

        eprintln!("Trie and flat storage populated");
        let in_memory_trie =
            load_trie_from_flat_state(&shard_tries.get_store(), shard_uid, state_root).unwrap();
        // print_trie(in_memory_trie.roots.get(&state_root).unwrap().handle(), 0);
        eprintln!("In memory trie loaded");

        let trie_update = TrieUpdate::new(shard_tries.get_trie_for_shard(shard_uid, state_root));
        trie_update.set_trie_cache_mode(near_primitives::types::TrieCacheMode::CachingChunk);
        let trie = trie_update.trie();
        let in_memory_trie = InMemoryTrieCompact::new(
            shard_uid,
            shard_tries.get_store(),
            in_memory_trie.roots.get(&state_root).unwrap().handle(),
        );
        for key in keys.iter() {
            let actual_value_ref = in_memory_trie.get_ref(key).map(|v| v.to_value_ref());
            let expected_value_ref = trie.get_ref(key, near_store::KeyLookupMode::Trie).unwrap();
            assert_eq!(actual_value_ref, expected_value_ref, "{:?}", NibbleSlice::new(key));
            assert_eq!(in_memory_trie.get_nodes_count(), trie.get_trie_nodes_count());
        }
    }

    fn check_random(max_key_len: usize, max_keys_count: usize, test_count: usize) {
        let mut rng = StdRng::seed_from_u64(42);
        for _ in 0..test_count {
            let key_cnt = rng.gen_range(1..=max_keys_count);
            let mut keys = Vec::new();
            for _ in 0..key_cnt {
                let mut key = Vec::new();
                let key_len = rng.gen_range(0..=max_key_len);
                for _ in 0..key_len {
                    let byte: u8 = rng.gen();
                    key.push(byte);
                }
                keys.push(key);
            }
            check(keys);
        }
    }

    #[test]
    fn flat_nodes_basic() {
        check(vec![vec![0, 1], vec![1, 0]]);
    }

    #[test]
    fn flat_nodes_rand_small() {
        check_random(3, 20, 10);
    }

    #[test]
    fn flat_nodes_rand_many_keys() {
        check_random(5, 1000, 10);
    }

    #[test]
    fn flat_nodes_rand_long_keys() {
        check_random(20, 100, 10);
    }

    #[test]
    fn flat_nodes_rand_long_long_keys() {
        check_random(1000, 1000, 1);
    }

    #[test]
    fn flat_nodes_rand_large_data() {
        check_random(32, 100000, 10);
    }

    #[test]
    fn check_encoding() {
        let parsed = ParsedTrieNode::Leaf {
            value: FlatStateValue::inlined(b"abcdef"),
            extension: Box::new([1, 2, 3, 4, 5]),
        };
        let encoded = TrieRootNode::new(
            TrieNodeRef::new_from_parsed(parsed.clone()),
            &mut Default::default(),
        );
        assert_eq!(encoded.handle().view().to_parsed(), parsed);

        let parsed = ParsedTrieNode::Extension {
            hash: hash(b"abcde"),
            memory_usage: 12345,
            extension: Box::new([5, 6, 7, 8, 9]),
            child: TrieNodeRef { data: 0x123456789a as *const u8 },
        };
        let encoded = TrieRootNode::new(
            TrieNodeRef::new_from_parsed(parsed.clone()),
            &mut Default::default(),
        );
        assert_eq!(encoded.handle().view().to_parsed(), parsed);

        let parsed = ParsedTrieNode::Branch {
            hash: hash(b"abcde"),
            memory_usage: 12345,
            children: [
                None,
                None,
                Some(TrieNodeRef { data: 0x123456789a01 as *const u8 }),
                None,
                None,
                Some(TrieNodeRef { data: 0x123456789a02 as *const u8 }),
                Some(TrieNodeRef { data: 0x123456789a03 as *const u8 }),
                None,
                None,
                Some(TrieNodeRef { data: 0x123456789a04 as *const u8 }),
                None,
                None,
                Some(TrieNodeRef { data: 0x123456789a05 as *const u8 }),
                None,
                Some(TrieNodeRef { data: 0x123456789a06 as *const u8 }),
                Some(TrieNodeRef { data: 0x123456789a07 as *const u8 }),
            ],
        };
        let encoded = TrieRootNode::new(
            TrieNodeRef::new_from_parsed(parsed.clone()),
            &mut Default::default(),
        );
        assert_eq!(encoded.handle().view().to_parsed(), parsed);

        let parsed = ParsedTrieNode::BranchWithValue {
            hash: hash(b"abcde"),
            memory_usage: 12345,
            children: [
                None,
                None,
                Some(TrieNodeRef { data: 0x123456789a01 as *const u8 }),
                None,
                None,
                Some(TrieNodeRef { data: 0x123456789a02 as *const u8 }),
                Some(TrieNodeRef { data: 0x123456789a03 as *const u8 }),
                None,
                None,
                Some(TrieNodeRef { data: 0x123456789a04 as *const u8 }),
                None,
                None,
                Some(TrieNodeRef { data: 0x123456789a05 as *const u8 }),
                None,
                Some(TrieNodeRef { data: 0x123456789a06 as *const u8 }),
                Some(TrieNodeRef { data: 0x123456789a07 as *const u8 }),
            ],
            value: FlatStateValue::inlined(b"abcdef"),
        };
        let encoded = TrieRootNode::new(
            TrieNodeRef::new_from_parsed(parsed.clone()),
            &mut Default::default(),
        );
        assert_eq!(encoded.handle().view().to_parsed(), parsed);
    }
}

/*
shard 0:
Loaded 59844482 nodes (58323289 after dedup), took 256.838155903s; stats: SizesStats { leaf_node_count: 33756917, extension_node_count: 5601563, branch_node_count: 18964284, branch_nodes_with_value_count: 525, children_ptr_count: 53825576, extension_total_bytes: 2336231902 }
dedupd_nodes_by_type: [1340969, 66694, 113485, 45]

shard 1:
Loaded 59555105 nodes (47203012 after dedup), took 269.64264202s; stats: SizesStats { leaf_node_count: 29864388, extension_node_count: 4441354, branch_node_count: 12897265, branch_nodes_with_value_count: 5, children_ptr_count: 49567403, extension_total_bytes: 661292598 }
dedupd_nodes_by_type: [10332897, 289876, 1729320, 0]

Shard 2:
Loaded 45629386 nodes (40310944 after dedup), took 221.615275672s; stats: SizesStats { leaf_node_count: 23220366, extension_node_count: 4438550, branch_node_count: 12630852, branch_nodes_with_value_count: 21176, children_ptr_count: 38695536, extension_total_bytes: 1249793288 }
dedupd_nodes_by_type: [4246839, 338421, 732061, 1121]

Shard 3:
Loaded 119170182 nodes (104589211 after dedup), took 528.250730273s; stats: SizesStats { leaf_node_count: 69794452, extension_node_count: 8612669, branch_node_count: 26168152, branch_nodes_with_value_count: 13938, children_ptr_count: 102318714, extension_total_bytes: 1910740734 }
dedupd_nodes_by_type: [11288153, 1345560, 1947063, 195]
*/
