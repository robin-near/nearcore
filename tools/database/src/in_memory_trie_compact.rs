use std::borrow::Borrow;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::mem;
use std::path::Path;
use std::ptr::NonNull;
use std::sync::Arc;
use std::time::{Duration, Instant};

use borsh::BorshSerialize;
use near_epoch_manager::EpochManager;
use near_primitives::block_header::BlockHeader;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::state::{FlatStateValue, ValueRef};
use near_primitives::types::ShardId;
use near_store::flat::store_helper::decode_flat_state_db_key;
use near_store::trie::{Children, TRIE_COSTS};
use near_store::{DBCol, NibbleSlice, RawTrieNode, RawTrieNodeWithSize, ShardUId, Store};
use nearcore::NearConfig;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};

use crate::utils::{flat_head, flat_head_state_root, open_rocksdb};

#[repr(C, packed(1))]
pub struct TrieNodeAlloc {
    data: TrieNodeRef,
}

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
struct LeafHeader {
    refcount: u32,
    kind: u8,
    value_len: u32,
    extension_len: u16,
}

#[repr(C, packed(1))]
struct ExtensionHeader {
    refcount: u32,
    kind: u8,
    hash: CryptoHash,
    memory_usage: u64,
    child: TrieNodeRef,
    extension_len: u16,
}

#[repr(C, packed(1))]
struct BranchHeader {
    refcount: u32,
    kind: u8,
    hash: CryptoHash,
    memory_usage: u64,
    mask: u16,
}

#[repr(C, packed(1))]
struct BranchWithValueHeader {
    refcount: u32,
    kind: u8,
    hash: CryptoHash,
    memory_usage: u64,
    value_len: u32,
    mask: u16,
}

impl TrieNodeAlloc {
    fn alloc_bytes(n: usize) -> Box<[u8]> {
        let mut data = Vec::new();
        data.reserve_exact(n);
        unsafe {
            data.set_len(n);
        }
        data.into_boxed_slice()
    }

    fn encoded_value_length(value: &FlatStateValue) -> u32 {
        match value {
            FlatStateValue::Ref(value_ref) => value_ref.length,
            FlatStateValue::Inlined(v) => 0x80000000 | v.len() as u32,
        }
    }

    fn value_extra_bytes_length(value: &FlatStateValue) -> usize {
        match value {
            FlatStateValue::Ref(_) => 32,
            FlatStateValue::Inlined(v) => v.len(),
        }
    }

    unsafe fn encode_value_data(value: &FlatStateValue, ptr: *mut u8) {
        match value {
            FlatStateValue::Ref(value_ref) => {
                *(ptr as *mut CryptoHash) = value_ref.hash;
            }
            FlatStateValue::Inlined(data) => {
                std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, data.len());
            }
        }
    }

    pub fn new_from_parsed(node: ParsedTrieNode) -> Self {
        let data = match node {
            ParsedTrieNode::Leaf { value, extension } => {
                let data = Box::into_raw(Self::alloc_bytes(
                    mem::size_of::<LeafHeader>()
                        + extension.len()
                        + Self::value_extra_bytes_length(&value),
                )) as *mut u8;
                unsafe {
                    *(data as *mut LeafHeader) = LeafHeader {
                        refcount: 0,
                        kind: 0,
                        value_len: Self::encoded_value_length(&value),
                        extension_len: extension.len() as u16,
                    };
                    std::ptr::copy_nonoverlapping(
                        extension.as_ptr(),
                        data.offset(mem::size_of::<LeafHeader>() as isize),
                        extension.len(),
                    );
                    Self::encode_value_data(
                        &value,
                        data.offset(
                            mem::size_of::<LeafHeader>() as isize + extension.len() as isize,
                        ),
                    )
                }
                data
            }
            ParsedTrieNode::Extension { hash, memory_usage, extension, child } => {
                let data = Box::into_raw(Self::alloc_bytes(
                    mem::size_of::<ExtensionHeader>() + extension.len(),
                )) as *mut u8;
                unsafe {
                    *(data as *mut ExtensionHeader) = ExtensionHeader {
                        refcount: 0,
                        kind: 1,
                        hash,
                        memory_usage,
                        child,
                        extension_len: extension.len() as u16,
                    };
                    std::ptr::copy_nonoverlapping(
                        extension.as_ptr(),
                        data.offset(mem::size_of::<ExtensionHeader>() as isize),
                        extension.len(),
                    );
                }
                data
            }
            ParsedTrieNode::Branch { hash, memory_usage, children } => {
                let mut mask = 0u16;
                let mut count = 0;
                for i in 0..16 {
                    if children[i].is_some() {
                        mask |= 1 << i;
                        count += 1;
                    }
                }
                let data =
                    Box::into_raw(Self::alloc_bytes(mem::size_of::<BranchHeader>() + 8 * count))
                        as *mut u8;
                unsafe {
                    *(data as *mut BranchHeader) =
                        BranchHeader { refcount: 0, kind: 2, hash, memory_usage, mask };

                    let mut ptr = data.offset(mem::size_of::<BranchHeader>() as isize);
                    for i in 0..16 {
                        if let Some(child) = children[i] {
                            *(ptr as *mut TrieNodeRef) = child;
                            ptr = ptr.offset(8);
                        }
                    }
                }
                data
            }
            ParsedTrieNode::BranchWithValue { hash, memory_usage, children, value } => {
                let mut mask = 0u16;
                let mut count = 0;
                for i in 0..16 {
                    if children[i].is_some() {
                        mask |= 1 << i;
                        count += 1;
                    }
                }
                let data = Box::into_raw(Self::alloc_bytes(
                    mem::size_of::<BranchWithValueHeader>()
                        + 8 * count
                        + Self::value_extra_bytes_length(&value),
                )) as *mut u8;
                unsafe {
                    *(data as *mut BranchWithValueHeader) = BranchWithValueHeader {
                        refcount: 0,
                        kind: 3,
                        hash,
                        memory_usage,
                        value_len: Self::encoded_value_length(&value),
                        mask,
                    };
                    let mut ptr = data.offset(mem::size_of::<BranchWithValueHeader>() as isize);
                    for i in 0..16 {
                        if let Some(child) = children[i] {
                            *(ptr as *mut TrieNodeRef) = child;
                            ptr = ptr.offset(8);
                        }
                    }
                    Self::encode_value_data(&value, ptr);
                }
                data
            }
        };
        Self { data: TrieNodeRef { data: data as *const u8 } }
    }

    pub fn get_ref(&self) -> TrieNodeRef {
        self.data
    }

    pub fn into_raw(self) -> TrieNodeRef {
        let data = self.data.data;
        mem::forget(self);
        TrieNodeRef { data }
    }

    pub fn from_raw(node: TrieNodeRef) -> TrieNodeAlloc {
        TrieNodeAlloc { data: node }
    }
}

impl Drop for TrieNodeAlloc {
    fn drop(&mut self) {
        let data = self.data.data;
        unsafe {
            let refcount = *(data as *const u32);
            assert_eq!(refcount, 0, "Dropping TrieNodeAlloc {:p} with non-zero refcount", data);
            let alloc_size = self.data.alloc_size();
            drop(Box::from_raw(
                std::slice::from_raw_parts_mut(data as *mut u8, alloc_size) as *mut [u8]
            ));
        }
    }
}

impl Borrow<TrieNodeRef> for TrieNodeAlloc {
    fn borrow(&self) -> &TrieNodeRef {
        &self.data
    }
}

impl TrieNodeRef {
    unsafe fn parse_value(length_encoded: u32, data_ptr: *const u8) -> FlatStateValue {
        if length_encoded & 0x80000000 != 0 {
            let length = length_encoded & 0x7fffffff;
            FlatStateValue::inlined(std::slice::from_raw_parts(data_ptr, length as usize))
        } else {
            let hash = *(data_ptr as *const CryptoHash);
            FlatStateValue::Ref(ValueRef { length: length_encoded, hash })
        }
    }

    pub fn parse(self) -> ParsedTrieNode {
        let kind = unsafe { *self.data.offset(4) };
        match kind {
            0 => unsafe {
                let header = &*(self.data as *const LeafHeader);
                let extension = std::slice::from_raw_parts(
                    self.data.offset(mem::size_of::<LeafHeader>() as isize),
                    header.extension_len as usize,
                );
                ParsedTrieNode::Leaf {
                    value: Self::parse_value(
                        header.value_len,
                        self.data.offset(
                            mem::size_of::<LeafHeader>() as isize + header.extension_len as isize,
                        ),
                    ),
                    extension: extension.to_vec().into_boxed_slice(),
                }
            },
            1 => unsafe {
                let header = &*(self.data as *const ExtensionHeader);
                let extension = std::slice::from_raw_parts(
                    self.data.offset(mem::size_of::<ExtensionHeader>() as isize),
                    header.extension_len as usize,
                );
                ParsedTrieNode::Extension {
                    hash: header.hash,
                    memory_usage: header.memory_usage,
                    extension: extension.to_vec().into_boxed_slice(),
                    child: header.child,
                }
            },
            2 => unsafe {
                let header = &*(self.data as *const BranchHeader);
                let ptr = self.data.offset(mem::size_of::<BranchHeader>() as isize);
                let mut children: [Option<TrieNodeRef>; 16] = [None; 16];
                let mut j = 0;
                for i in 0..16 {
                    if header.mask & (1 << i) != 0 {
                        children[i] = Some(*(ptr.offset(8 * j) as *const TrieNodeRef));
                        j += 1;
                    }
                }
                ParsedTrieNode::Branch {
                    hash: header.hash,
                    memory_usage: header.memory_usage,
                    children,
                }
            },
            3 => unsafe {
                let header = &*(self.data as *const BranchWithValueHeader);
                let mut ptr = self.data.offset(mem::size_of::<BranchWithValueHeader>() as isize);
                let mut children: [Option<TrieNodeRef>; 16] = [None; 16];
                for i in 0..16 {
                    if header.mask & (1 << i) != 0 {
                        children[i] = Some(*(ptr as *const TrieNodeRef));
                        ptr = ptr.offset(8);
                    }
                }
                ParsedTrieNode::BranchWithValue {
                    hash: header.hash,
                    memory_usage: header.memory_usage,
                    children,
                    value: Self::parse_value(header.value_len, ptr),
                }
            },
            _ => unreachable!("invalid trie node kind"),
        }
    }

    pub fn is_leaf(&self) -> bool {
        unsafe { *self.data.offset(4) == 0 }
    }

    pub fn node_type(&self) -> u8 {
        unsafe { *self.data.offset(4) }
    }

    pub fn hash(&self) -> CryptoHash {
        let kind = unsafe { *self.data.offset(4) };
        if kind == 0 {
            let raw_node_with_size = unsafe {
                let header = &*(self.data as *const LeafHeader);
                let extension = std::slice::from_raw_parts(
                    self.data.offset(mem::size_of::<LeafHeader>() as isize),
                    header.extension_len as usize,
                );
                let value = Self::parse_value(
                    header.value_len,
                    self.data.offset(
                        mem::size_of::<LeafHeader>() as isize + header.extension_len as isize,
                    ),
                );
                let raw_node = RawTrieNode::Leaf(extension.to_vec(), value.to_value_ref());
                let size = 50
                    + header.extension_len as u64 * 2
                    + (header.value_len & 0x7fffffff) as u64 * 1
                    + 50;
                RawTrieNodeWithSize { node: raw_node, memory_usage: size }
            };
            hash(&raw_node_with_size.try_to_vec().unwrap())
        } else {
            unsafe { (&*(self.data as *const BranchHeader)).hash }
        }
    }

    pub fn memory_usage(&self) -> u64 {
        let kind = unsafe { *self.data.offset(4) };
        if kind == 0 {
            let header = unsafe { &*(self.data as *const LeafHeader) };
            50 + header.extension_len as u64 * 2 + (header.value_len & 0x7fffffff) as u64 * 1 + 50
        } else {
            unsafe { (&*(self.data as *const BranchHeader)).memory_usage }
        }
    }

    pub fn alloc_size(&self) -> usize {
        let data = self.data;
        unsafe {
            match *data.offset(4) {
                0 => {
                    let header = &*(data as *const LeafHeader);
                    mem::size_of::<LeafHeader>() + header.extension_len as usize
                }
                1 => {
                    let header = &*(data as *const ExtensionHeader);
                    mem::size_of::<ExtensionHeader>() + header.extension_len as usize
                }
                2 => {
                    let header = &*(data as *const BranchHeader);
                    mem::size_of::<BranchHeader>() + 8 * u16::count_ones(header.mask) as usize
                }
                3 => {
                    let header = &*(data as *const BranchWithValueHeader);
                    mem::size_of::<BranchWithValueHeader>()
                        + 8 * u16::count_ones(header.mask) as usize
                }
                _ => unreachable!(),
            }
        }
    }

    pub fn add_ref(&self) {
        let ref_count = unsafe { *(self.data as *const u32) };
        unsafe { *(self.data as *mut u32) = ref_count + 1 };
    }

    pub fn deref(&self) -> bool {
        let ref_count = unsafe { *(self.data as *const u32) };
        // println!("Dereferencing node {:p}, refcount was {}", self, ref_count);
        unsafe { *(self.data as *mut u32) = ref_count - 1 };
        ref_count == 1
    }

    pub fn deref_recursively(&self, removing_from: &mut TrieNodeAllocSet) {
        // println!(
        //     "Dereferencing node {:p}; memory: {}",
        //     self,
        //     hex::encode(unsafe { std::slice::from_raw_parts(self.data, self.alloc_size()) })
        // );
        if self.deref() {
            unsafe {
                let kind = *self.data.offset(4);
                match kind {
                    0 => {}
                    1 => {
                        let header = &*(self.data as *const ExtensionHeader);
                        header.child.deref_recursively(removing_from);
                    }
                    2 => {
                        let header = &*(self.data as *const BranchHeader);
                        let ptr = self.data.offset(mem::size_of::<BranchHeader>() as isize);
                        let mut j = 0;
                        for i in 0..16 {
                            if header.mask & (1 << i) != 0 {
                                let child = &*(ptr.offset(8 * j) as *const TrieNodeRef);
                                child.deref_recursively(removing_from);
                                j += 1;
                            }
                        }
                    }
                    3 => {
                        let header = &*(self.data as *const BranchWithValueHeader);
                        let ptr =
                            self.data.offset(mem::size_of::<BranchWithValueHeader>() as isize);
                        let mut j = 0;
                        for i in 0..16 {
                            if header.mask & (1 << i) != 0 {
                                let child = &*(ptr.offset(8 * j) as *const TrieNodeRef);
                                child.deref_recursively(removing_from);
                                j += 1;
                            }
                        }
                    }
                    _ => unreachable!("invalid trie node kind"),
                }
            }
            removing_from.nodes.remove(self);
        }
    }

    pub fn deref_children(&self) {
        unsafe {
            let kind = *self.data.offset(4);
            match kind {
                0 => {}
                1 => {
                    let header = &*(self.data as *const ExtensionHeader);
                    assert!(
                        !header.child.deref(),
                        "deref_children should only be called when children have multiple refs"
                    );
                }
                2 => {
                    let header = &*(self.data as *const BranchHeader);
                    let ptr = self.data.offset(mem::size_of::<BranchHeader>() as isize);
                    let mut j = 0;
                    for i in 0..16 {
                        if header.mask & (1 << i) != 0 {
                            let child = &*(ptr.offset(8 * j) as *const TrieNodeRef);
                            assert!(!child.deref(), "deref_children should only be called when children have multiple refs");
                            j += 1;
                        }
                    }
                }
                3 => {
                    let header = &*(self.data as *const BranchWithValueHeader);
                    let ptr = self.data.offset(mem::size_of::<BranchWithValueHeader>() as isize);
                    let mut j = 0;
                    for i in 0..16 {
                        if header.mask & (1 << i) != 0 {
                            let child = &*(ptr.offset(8 * j) as *const TrieNodeRef);
                            assert!(!child.deref(), "deref_children should only be called when children have multiple refs");
                            j += 1;
                        }
                    }
                }
                _ => unreachable!("invalid trie node kind"),
            }
        }
    }

    pub fn increment_stats(&self, stats: &mut SizesStats) {
        unsafe {
            let kind = *self.data.offset(4);
            match kind {
                0 => {
                    let header = &*(self.data as *const LeafHeader);
                    stats.leaf_node_count += 1;
                    stats.extension_total_bytes += header.extension_len as usize;
                }
                1 => {
                    let header = &*(self.data as *const ExtensionHeader);
                    stats.extension_node_count += 1;
                    stats.extension_total_bytes += header.extension_len as usize;
                }
                2 => {
                    let header = &*(self.data as *const BranchHeader);
                    stats.branch_node_count += 1;
                    stats.children_ptr_count += u16::count_ones(header.mask) as usize;
                }
                3 => {
                    let header = &*(self.data as *const BranchWithValueHeader);
                    stats.branch_nodes_with_value_count += 1;
                    stats.children_ptr_count += u16::count_ones(header.mask) as usize;
                }
                _ => unreachable!("invalid trie node kind"),
            }
        }
    }

    unsafe fn decode_leaf<'a>(&'a self) -> DecodedLeaf<'a> {
        let data = self.data;
        let header = &*(data as *const LeafHeader);
        let extension = std::slice::from_raw_parts(
            data.offset(mem::size_of::<LeafHeader>() as isize),
            header.extension_len as usize,
        );
        let value_ptr =
            data.offset(mem::size_of::<LeafHeader>() as isize + header.extension_len as isize);
        let value_data = if header.value_len & 0x80000000 != 0 {
            std::slice::from_raw_parts(value_ptr, (header.value_len & 0x7fffffff) as usize)
        } else {
            std::slice::from_raw_parts(value_ptr, 32)
        };
        DecodedLeaf { extension, value_len_encoded: header.value_len, value_data }
    }
}

#[derive(PartialEq, Eq, Hash)]
struct DecodedLeaf<'a> {
    extension: &'a [u8],
    value_len_encoded: u32,
    value_data: &'a [u8],
}

impl Hash for TrieNodeAlloc {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let data = self.data.data;
        unsafe {
            if *data.offset(4) == 0 {
                self.data.decode_leaf().hash(state);
            } else {
                self.data.hash().hash(state);
            }
        }
    }
}

impl PartialEq for TrieNodeAlloc {
    fn eq(&self, other: &Self) -> bool {
        let self_data = self.data.data;
        let other_data = other.data.data;
        unsafe {
            let self_kind = *self_data.offset(4);
            let other_kind = *other_data.offset(4);
            if self_kind != other_kind {
                return false;
            }
            if self_kind == 0 {
                self.data.decode_leaf() == other.data.decode_leaf()
            } else {
                self.data.hash() == other.data.hash()
            }
        }
    }
}

impl Eq for TrieNodeAlloc {}

pub struct TrieNodeAllocSet {
    nodes: HashSet<TrieNodeAlloc>,
}

impl TrieNodeAllocSet {
    pub fn new() -> Self {
        Self { nodes: HashSet::new() }
    }

    pub fn insert_with_dedup(&mut self, node: TrieNodeAlloc) -> (TrieNodeRef, bool) {
        if let Some(existing) = self.nodes.get(&node) {
            let result = existing.get_ref();
            result.add_ref();
            node.get_ref().deref_children();
            (result, false)
        } else {
            let result = node.get_ref().clone();
            result.add_ref();
            self.nodes.insert(node);
            (result, true)
        }
    }
}

#[derive(Default, Debug)]
pub struct SizesStats {
    leaf_node_count: usize,
    extension_node_count: usize,
    branch_node_count: usize,
    branch_nodes_with_value_count: usize,
    children_ptr_count: usize,
    extension_total_bytes: usize,
    dedupd_nodes_by_type: [usize; 4],
}

pub struct LoadedInMemoryTrie {
    pub root: TrieNodeRef,
    pub set: TrieNodeAllocSet,
    pub sizes: SizesStats,
}

impl Drop for LoadedInMemoryTrie {
    fn drop(&mut self) {
        self.root.deref_recursively(&mut self.set);
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
        TrieNodeAlloc::new_from_parsed(parsed_node).into_raw()
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

    pub fn finalize(mut self) -> TrieNodeRef {
        while self.segments.len() > 1 {
            self.pop_segment();
        }
        self.segments.into_iter().next().unwrap().into_node()
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
        let head = flat_head(&store);
        let block_header = store
            .get_ser::<BlockHeader>(DBCol::BlockHeader, &head.try_to_vec().unwrap())?
            .ok_or_else(|| anyhow::anyhow!("Block header not found"))?;
        let epoch_manager =
            EpochManager::new_from_genesis_config(store.clone(), &genesis_config).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(block_header.epoch_id()).unwrap();

        let shard_uid = ShardUId::from_shard_id_and_layout(self.shard_id, &shard_layout);
        let state_root = flat_head_state_root(&store, &shard_uid);

        let _trie = load_trie_from_flat_state(&store, shard_uid, state_root)?;
        for _ in 0..1000000 {
            std::thread::sleep(Duration::from_secs(100));
        }
        Ok(())
    }
}

fn print_trie(node: TrieNodeRef, indent: usize) {
    let parsed = node.parse();
    match parsed {
        ParsedTrieNode::Leaf { value, extension } => {
            println!(
                "{}Leaf {:?} {:?}",
                " ".repeat(indent),
                NibbleSlice::from_encoded(&extension).0,
                value,
            );
        }
        ParsedTrieNode::Extension { hash, memory_usage, extension, child } => {
            println!(
                "{}Extension {:?} {:?} mem={}",
                " ".repeat(indent),
                NibbleSlice::from_encoded(&extension).0,
                hash,
                memory_usage,
            );
            print_trie(child, indent + 2);
        }
        ParsedTrieNode::Branch { hash, memory_usage, children } => {
            println!("{}Branch {:?} mem={}", " ".repeat(indent), hash, memory_usage);
            for (i, child) in children.into_iter().enumerate() {
                if let Some(child) = child {
                    println!("{}  {:x}: ", " ".repeat(indent), i);
                    print_trie(child, indent + 4);
                }
            }
        }
        ParsedTrieNode::BranchWithValue { hash, memory_usage, children, value } => {
            println!(
                "{}BranchWithValue {:?} {:?} mem={}",
                " ".repeat(indent),
                value,
                hash,
                memory_usage
            );
            for (i, child) in children.into_iter().enumerate() {
                if let Some(child) = child {
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
) -> anyhow::Result<TrieNodeRef> {
    println!("Loading trie from flat state...");
    let mut load_start = Instant::now();
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
    let root = recon.finalize();

    println!(
        "[{:?}] Loaded {} keys; computing hash and memory usage...",
        load_start.elapsed(),
        loaded
    );
    let mut subtrees = Vec::new();
    let (_, total_nodes) =
        root.compute_subtree_node_count_and_mark_boundary_subtrees(1000, &mut subtrees);
    println!("[{:?}] Total node count = {}, parallel subtree count = {}, going to compute hash and memory for subtrees", load_start.elapsed(), total_nodes, subtrees.len());
    subtrees.into_par_iter().for_each(|subtree| {
        subtree.compute_hash_and_memory_usage_recursively();
    });
    println!(
        "[{:?}] Done computing hash and memory usage for subtrees; now computing root hash",
        load_start.elapsed()
    );
    root.compute_hash_and_memory_usage_recursively();
    if root.hash() != state_root {
        println!("[{:?}] State root mismatch", load_start.elapsed());
    }
    println!("Begin verifying hash computation");

    let mut i = 0;
    root.iter_all_nodes_post_order(&[0], &mut |node, path| {
        let hash = node.hash();
        let result = store
            .get(
                DBCol::State,
                &shard_uid
                    .try_to_vec()
                    .unwrap()
                    .into_iter()
                    .chain(hash.try_to_vec().unwrap().into_iter())
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        if result.is_none() {
            println!(
                "Hash not found in DB: {:?} at path {:?}, node: {:?}",
                hash,
                NibbleSlice::from_encoded(path).0,
                node.parse()
            );
        }
        i += 1;
        if i % 1000000 == 0 {
            println!(
                "[{:?}] Verified {} nodes, current path: {:?}",
                load_start.elapsed(),
                i,
                NibbleSlice::from_encoded(path).0,
            );
        }
    });

    println!("[{:?}] Done loading trie from flat state", load_start.elapsed());

    Ok(root)
}

unsafe impl Send for TrieNodeRef {}
unsafe impl Sync for TrieNodeRef {}

impl TrieNodeRef {
    unsafe fn children(&self) -> &[TrieNodeRef] {
        match self.node_type() {
            0 => std::slice::from_raw_parts(NonNull::dangling().as_ptr(), 0),
            1 => {
                let header = &*(self.data as *const ExtensionHeader);
                std::slice::from_ref(&header.child)
            }
            2 => {
                let header = &*(self.data as *const BranchHeader);
                let num_children = header.mask.count_ones();
                std::slice::from_raw_parts(
                    self.data.offset(mem::size_of::<BranchHeader>() as isize) as *const TrieNodeRef,
                    num_children as usize,
                )
            }
            3 => {
                let header = &*(self.data as *const BranchWithValueHeader);
                let num_children = header.mask.count_ones();
                std::slice::from_raw_parts(
                    self.data.offset(mem::size_of::<BranchWithValueHeader>() as isize)
                        as *const TrieNodeRef,
                    num_children as usize,
                )
            }
            _ => unreachable!("Invalid node type"),
        }
    }

    pub fn compute_subtree_node_count_and_mark_boundary_subtrees(
        &self,
        threshold: usize,
        trees: &mut Vec<TrieNodeRef>,
    ) -> (BoundaryNodeType, usize) {
        unsafe {
            let children = self.children();
            let mut total = 1;
            let mut any_children_above_or_at_boundary = false;
            let mut children_below_boundary = Vec::new();

            for child in children {
                let (child_boundary_type, child_count) =
                    child.compute_subtree_node_count_and_mark_boundary_subtrees(threshold, trees);
                match child_boundary_type {
                    BoundaryNodeType::AboveOrAtBoundary => {
                        any_children_above_or_at_boundary = true;
                    }
                    BoundaryNodeType::BelowBoundary => {
                        children_below_boundary.push(*child);
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
    }

    fn to_raw_trie_node_with_size(&self) -> RawTrieNodeWithSize {
        unsafe {
            match self.node_type() {
                0 => {
                    unreachable!("We don't do this for leaf nodes.")
                }
                1 => {
                    let header = &*(self.data as *const ExtensionHeader);
                    let extension = std::slice::from_raw_parts(
                        self.data.offset(mem::size_of::<ExtensionHeader>() as isize),
                        header.extension_len as usize,
                    );
                    let raw_node = RawTrieNode::Extension(extension.to_vec(), header.child.hash());
                    RawTrieNodeWithSize {
                        node: raw_node,
                        memory_usage: header.child.memory_usage()
                            + TRIE_COSTS.node_cost
                            + TRIE_COSTS.byte_of_key * extension.len() as u64,
                    }
                }
                2 => {
                    let header = &*(self.data as *const BranchHeader);
                    let mut children: [Option<CryptoHash>; 16] = [None; 16];
                    let mut ptr = self.data.offset(mem::size_of::<BranchHeader>() as isize);
                    let mut total_memory_usage = TRIE_COSTS.node_cost;
                    for i in 0..16 {
                        if header.mask & (1 << i) != 0 {
                            let child = *(ptr as *const TrieNodeRef);
                            children[i] = Some(child.hash());
                            total_memory_usage += child.memory_usage();
                            ptr = ptr.offset(8);
                        }
                    }
                    let raw_node = RawTrieNode::BranchNoValue(Children(children));
                    RawTrieNodeWithSize { node: raw_node, memory_usage: total_memory_usage }
                }
                3 => {
                    let header = &*(self.data as *const BranchWithValueHeader);
                    let mut children: [Option<CryptoHash>; 16] = [None; 16];
                    let mut ptr =
                        self.data.offset(mem::size_of::<BranchWithValueHeader>() as isize);
                    let mut total_memory_usage = TRIE_COSTS.node_cost;
                    for i in 0..16 {
                        if header.mask & (1 << i) != 0 {
                            let child = *(ptr as *const TrieNodeRef);
                            children[i] = Some(child.hash());
                            total_memory_usage += child.memory_usage();
                            ptr = ptr.offset(8);
                        }
                    }
                    let value = Self::parse_value(header.value_len, ptr).to_value_ref();
                    total_memory_usage +=
                        TRIE_COSTS.node_cost + TRIE_COSTS.byte_of_value * value.length as u64;
                    let raw_node = RawTrieNode::BranchWithValue(value, Children(children));
                    RawTrieNodeWithSize { node: raw_node, memory_usage: total_memory_usage }
                }
                _ => unreachable!("Invalid node type"),
            }
        }
    }

    pub fn compute_hash_and_memory_usage_recursively(&self) {
        unsafe {
            if self.is_leaf() {
                return;
            }
            let fake_header = &mut *(self.data as *mut BranchHeader);
            if fake_header.memory_usage > 0 {
                // already computed.
                return;
            }

            for child in self.children() {
                child.compute_hash_and_memory_usage_recursively();
            }
            let raw_node_with_size = self.to_raw_trie_node_with_size();
            // the header has same layout as other non-leaf headers for hash and memory usage
            fake_header.hash = hash(&raw_node_with_size.try_to_vec().unwrap());
            fake_header.memory_usage = raw_node_with_size.memory_usage;
        }
    }

    pub fn iter_all_nodes_post_order(&self, path: &[u8], f: &mut impl FnMut(TrieNodeRef, &[u8])) {
        let nibbles: NibbleSlice<'_> = NibbleSlice::from_encoded(&path).0;
        match self.parse() {
            ParsedTrieNode::Leaf { value, extension } => {}
            ParsedTrieNode::Extension { hash, memory_usage, extension, child } => {
                let child_path =
                    nibbles.merge_encoded(&NibbleSlice::from_encoded(&extension).0, false);
                child.iter_all_nodes_post_order(&child_path, f);
            }
            ParsedTrieNode::Branch { children, .. }
            | ParsedTrieNode::BranchWithValue { children, .. } => {
                for (i, child) in children.iter().enumerate() {
                    if let Some(child) = child {
                        let child_path =
                            nibbles.merge_encoded(&NibbleSlice::new(&[0x10 + i as u8]), false);
                        child.iter_all_nodes_post_order(&child_path, f);
                    }
                }
            }
        }
        f(*self, path);
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

    use crate::in_memory_trie_compact::{load_trie_from_flat_state, print_trie, TrieNodeRef};
    use crate::in_memory_trie_lookup::InMemoryTrieCompact;

    use super::{FlatStateValue, ParsedTrieNode, TrieConstructionState, TrieNodeAlloc};

    #[test]
    fn test_basic_reconstruction() {
        let mut rec = TrieConstructionState::new();
        rec.add_leaf(b"aaaaa", FlatStateValue::Inlined(b"a".to_vec()));
        rec.add_leaf(b"aaaab", FlatStateValue::Inlined(b"b".to_vec()));
        rec.add_leaf(b"ab", FlatStateValue::Inlined(b"c".to_vec()));
        rec.add_leaf(b"abffff", FlatStateValue::Inlined(b"c".to_vec()));
        let node = rec.finalize();
        print_trie(node, 0);
    }

    fn check(keys: Vec<Vec<u8>>) {
        let description =
            if keys.len() <= 20 { format!("{keys:?}") } else { format!("{} keys", keys.len()) };
        let shard_tries = create_tries();
        let shard_uid = ShardUId::single_shard();
        let changes = keys
            .iter()
            .map(|key| {
                (
                    vec![0, 9, 2, 5, 4, 6, 7, 8]
                        .into_iter()
                        .chain(key.to_vec().into_iter())
                        .collect::<Vec<u8>>(),
                    Some(key.to_vec()),
                )
            })
            .collect::<Vec<_>>();
        let changes = simplify_changes(&changes);
        // eprintln!("TEST CASE {changes:?}");
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
        // let loaded_in_memory_trie =
        //     load_trie_in_memory_compact(&shard_tries.get_store(), shard_uid, state_root).unwrap();
        let in_memory_trie_root =
            load_trie_from_flat_state(&shard_tries.get_store(), shard_uid, state_root).unwrap();
        // print_trie(in_memory_trie_root, 0);
        eprintln!("In memory trie loaded");

        let trie_update = TrieUpdate::new(shard_tries.get_trie_for_shard(shard_uid, state_root));
        trie_update.set_trie_cache_mode(near_primitives::types::TrieCacheMode::CachingChunk);
        let trie = trie_update.trie();
        let in_memory_trie =
            InMemoryTrieCompact::new(shard_uid, shard_tries.get_store(), in_memory_trie_root);
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
        let encoded = TrieNodeAlloc::new_from_parsed(parsed.clone());
        assert_eq!(encoded.get_ref().parse(), parsed);
        drop(encoded);

        let parsed = ParsedTrieNode::Extension {
            hash: hash(b"abcde"),
            memory_usage: 12345,
            extension: Box::new([5, 6, 7, 8, 9]),
            child: TrieNodeRef { data: 0x123456789a as *const u8 },
        };
        let encoded = TrieNodeAlloc::new_from_parsed(parsed.clone());
        assert_eq!(encoded.get_ref().parse(), parsed);
        drop(encoded);

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
        let encoded = TrieNodeAlloc::new_from_parsed(parsed.clone());
        assert_eq!(encoded.get_ref().parse(), parsed);
        drop(encoded);

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
        let encoded = TrieNodeAlloc::new_from_parsed(parsed.clone());
        assert_eq!(encoded.get_ref().parse(), parsed);
        drop(encoded);
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
