use std::borrow::Borrow;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::mem;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use borsh::{BorshDeserialize, BorshSerialize};
use near_epoch_manager::EpochManager;
use near_primitives::block_header::BlockHeader;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::state::ValueRef;
use near_primitives::types::ShardId;
use near_store::{DBCol, NibbleSlice, RawTrieNode, RawTrieNodeWithSize, ShardUId, Store};
use nearcore::NearConfig;

use crate::flat_nodes::FlatNodeNibbles;
use crate::utils::{flat_head, flat_head_state_root, open_rocksdb};

#[repr(C, packed(1))]
pub struct TrieNodeAlloc {
    data: TrieNodeRef,
}

#[derive(Clone, Copy)]
#[repr(C, packed(1))]
pub struct TrieNodeRef {
    data: *const u8,
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub struct ParsedTrieNodePtr {
    data: *const u8,
}

impl From<TrieNodeRef> for ParsedTrieNodePtr {
    fn from(node: TrieNodeRef) -> Self {
        Self { data: node.data }
    }
}

impl From<ParsedTrieNodePtr> for TrieNodeRef {
    fn from(node: ParsedTrieNodePtr) -> Self {
        Self { data: node.data }
    }
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum ParsedTrieNode {
    Leaf {
        value: ValueRef,
        extension: Box<[u8]>,
    },
    Extension {
        hash: CryptoHash,
        memory_usage: u64,
        extension: Box<[u8]>,
        child: ParsedTrieNodePtr,
    },
    Branch {
        hash: CryptoHash,
        memory_usage: u64,
        children: [Option<ParsedTrieNodePtr>; 16],
    },
    BranchWithValue {
        hash: CryptoHash,
        memory_usage: u64,
        children: [Option<ParsedTrieNodePtr>; 16],
        value: ValueRef,
    },
}

#[repr(C, packed(1))]
struct LeafHeader {
    refcount: u32,
    kind: u8,
    value_hash: CryptoHash,
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
    value_hash: CryptoHash,
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
    pub fn new_from_parsed(node: ParsedTrieNode) -> Self {
        let data = match node {
            ParsedTrieNode::Leaf { value, extension } => {
                let data = Box::into_raw(Self::alloc_bytes(
                    mem::size_of::<LeafHeader>() + extension.len(),
                )) as *mut u8;
                unsafe {
                    *(data as *mut LeafHeader) = LeafHeader {
                        refcount: 0,
                        kind: 0,
                        value_hash: value.hash,
                        value_len: value.length as u32,
                        extension_len: extension.len() as u16,
                    };
                    std::ptr::copy_nonoverlapping(
                        extension.as_ptr(),
                        data.offset(mem::size_of::<LeafHeader>() as isize),
                        extension.len(),
                    );
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
                        child: child.into(),
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

                    let ptr = data.offset(mem::size_of::<BranchHeader>() as isize);
                    let mut j = 0;
                    for i in 0..16 {
                        if let Some(child) = children[i] {
                            *(ptr.offset(8 * j) as *mut TrieNodeRef) = child.into();
                            j += 1;
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
                    mem::size_of::<BranchWithValueHeader>() + 8 * count,
                )) as *mut u8;
                unsafe {
                    *(data as *mut BranchWithValueHeader) = BranchWithValueHeader {
                        refcount: 0,
                        kind: 3,
                        hash,
                        memory_usage,
                        value_hash: value.hash,
                        value_len: value.length as u32,
                        mask,
                    };
                    let ptr = data.offset(mem::size_of::<BranchWithValueHeader>() as isize);
                    let mut j = 0;
                    for i in 0..16 {
                        if let Some(child) = children[i] {
                            *(ptr.offset(8 * j) as *mut TrieNodeRef) = child.into();
                            j += 1;
                        }
                    }
                }
                data
            }
        };
        Self { data: TrieNodeRef { data: data as *const u8 } }
    }

    pub fn get_ref(&self) -> TrieNodeRef {
        self.data
    }
}

impl Drop for TrieNodeAlloc {
    fn drop(&mut self) {
        let data = self.data.data;
        unsafe {
            let refcount = *(data as *const u32);
            assert_eq!(refcount, 0);
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
                    value: ValueRef { hash: header.value_hash, length: header.value_len },
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
                    child: header.child.into(),
                }
            },
            2 => unsafe {
                let header = &*(self.data as *const BranchHeader);
                let ptr = self.data.offset(mem::size_of::<BranchHeader>() as isize);
                let mut children: [Option<ParsedTrieNodePtr>; 16] = [None; 16];
                let mut j = 0;
                for i in 0..16 {
                    if header.mask & (1 << i) != 0 {
                        children[i] = Some((*(ptr.offset(8 * j) as *const TrieNodeRef)).into());
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
                let ptr = self.data.offset(mem::size_of::<BranchWithValueHeader>() as isize);
                let mut children: [Option<ParsedTrieNodePtr>; 16] = [None; 16];
                let mut j = 0;
                for i in 0..16 {
                    if header.mask & (1 << i) != 0 {
                        children[i] = Some((*(ptr.offset(8 * j) as *const TrieNodeRef)).into());
                        j += 1;
                    }
                }
                ParsedTrieNode::BranchWithValue {
                    hash: header.hash,
                    memory_usage: header.memory_usage,
                    children,
                    value: ValueRef { hash: header.value_hash, length: header.value_len },
                }
            },
            _ => unreachable!("invalid trie node kind"),
        }
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
                let raw_node = RawTrieNode::Leaf(
                    extension.to_vec(),
                    ValueRef { hash: header.value_hash, length: header.value_len },
                );
                let size = 50 + header.extension_len as u64 * 2 + header.value_len as u64 * 1 + 50;
                RawTrieNodeWithSize { node: raw_node, memory_usage: size }
            };
            hash(&raw_node_with_size.try_to_vec().unwrap())
        } else {
            unsafe { *(self.data.offset(4 + 1) as *const CryptoHash) }
        }
    }

    pub fn memory_usage(&self) -> u64 {
        let kind = unsafe { *self.data.offset(4) };
        if kind == 0 {
            let header = unsafe { &*(self.data as *const LeafHeader) };
            50 + header.extension_len as u64 * 2 + header.value_len as u64 * 1 + 50
        } else {
            unsafe { *(self.data.offset(4 + 2) as *const u64) }
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
}

struct InMemoryTrieNodeBuilder {
    path: FlatNodeNibbles,
    hash_and_size: Option<(CryptoHash, u64)>,
    leaf: Option<ValueRef>,
    extension: Option<Box<[u8]>>,
    children: [Option<ParsedTrieNodePtr>; 16],
    expected_children: Vec<Option<CryptoHash>>,
    next_child_index: usize,
    placeholder_length: Option<usize>,
    pending_children: Vec<ParsedTrieNodePtr>,
}

impl Hash for TrieNodeAlloc {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Hash::hash(&self.get_ref(), state);
    }
}

impl Hash for TrieNodeRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        unsafe {
            if *self.data.offset(4) == 0 {
                let header = &*(self.data as *const LeafHeader);
                header.value_hash.hash(state);
                std::slice::from_raw_parts(
                    self.data.offset(mem::size_of::<LeafHeader>() as isize),
                    header.extension_len as usize,
                )
                .hash(state);
            } else {
                self.hash().hash(state);
            }
        }
    }
}

impl PartialEq for TrieNodeRef {
    fn eq(&self, other: &Self) -> bool {
        unsafe {
            let self_kind = *self.data.offset(4);
            let other_kind = *other.data.offset(4);
            if self_kind != other_kind {
                return false;
            }
            if self_kind == 0 {
                let self_header = &*(self.data as *const LeafHeader);
                let other_header = &*(other.data as *const LeafHeader);
                if self_header.extension_len != other_header.extension_len {
                    return false;
                }
                if self_header.value_hash != other_header.value_hash {
                    return false;
                }
                let self_extension = std::slice::from_raw_parts(
                    self.data.offset(mem::size_of::<LeafHeader>() as isize),
                    self_header.extension_len as usize,
                );
                let other_extension = std::slice::from_raw_parts(
                    other.data.offset(mem::size_of::<LeafHeader>() as isize),
                    other_header.extension_len as usize,
                );
                self_extension == other_extension
            } else {
                self.hash() == other.hash()
            }
        }
    }
}

impl Eq for TrieNodeRef {}

impl PartialEq for TrieNodeAlloc {
    fn eq(&self, other: &Self) -> bool {
        let a: &TrieNodeRef = self.borrow();
        let b: &TrieNodeRef = other.borrow();
        a == b
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
            (result, false)
        } else {
            let result = node.get_ref().clone();
            result.add_ref();
            self.nodes.insert(node);
            (result, true)
        }
    }
}

impl InMemoryTrieNodeBuilder {
    pub fn placeholder(
        path: FlatNodeNibbles,
        placeholder_length: usize,
    ) -> InMemoryTrieNodeBuilder {
        InMemoryTrieNodeBuilder {
            path,
            hash_and_size: None,
            leaf: None,
            extension: None,
            children: Default::default(),
            expected_children: Vec::new(),
            next_child_index: 0,
            placeholder_length: Some(placeholder_length),
            pending_children: Vec::new(),
        }
    }

    pub fn from_raw_node(
        path: FlatNodeNibbles,
        node: RawTrieNodeWithSize,
    ) -> InMemoryTrieNodeBuilder {
        let mut builder = InMemoryTrieNodeBuilder::placeholder(path, 0);
        builder.set_raw_node(node);
        builder
    }

    pub fn set_raw_node(&mut self, node: RawTrieNodeWithSize) {
        assert!(self.placeholder_length.is_some());
        self.placeholder_length = None;
        self.hash_and_size = Some((hash(&node.try_to_vec().unwrap()), node.memory_usage));
        match node.node {
            RawTrieNode::Leaf(extension, leaf) => {
                self.extension = Some(extension.into_boxed_slice());
                self.leaf = Some(leaf);
            }
            RawTrieNode::Extension(extension, node) => {
                self.extension = Some(extension.into_boxed_slice());
                self.expected_children.push(Some(node));
            }
            RawTrieNode::BranchNoValue(children) => {
                let mut first_child_index = None;
                for (i, child) in children.0.iter().enumerate() {
                    if child.is_some() && first_child_index.is_none() {
                        first_child_index = Some(i);
                    }
                    self.expected_children.push(*child);
                }
                self.next_child_index = first_child_index.unwrap_or(16);
            }
            RawTrieNode::BranchWithValue(leaf, children) => {
                self.leaf = Some(leaf);
                let mut first_child_index = None;
                for (i, child) in children.0.iter().enumerate() {
                    if child.is_some() && first_child_index.is_none() {
                        first_child_index = Some(i);
                    }
                    self.expected_children.push(*child);
                }
                self.next_child_index = first_child_index.unwrap_or(16);
            }
        }
        let children = std::mem::take(&mut self.pending_children);
        for child in children {
            self.add_child(child.into());
        }
    }

    pub fn build(mut self) -> TrieNodeAlloc {
        assert!(self.placeholder_length.is_none());
        let parsed_node = match (self.leaf, self.extension) {
            (Some(value), Some(extension)) => ParsedTrieNode::Leaf { value, extension },
            (None, Some(extension)) => {
                assert_eq!(
                    self.next_child_index,
                    1,
                    "{:?}: Expected 1 child, found {}",
                    self.hash_and_size.unwrap().0,
                    self.next_child_index
                );
                ParsedTrieNode::Extension {
                    extension,
                    hash: self.hash_and_size.unwrap().0,
                    memory_usage: self.hash_and_size.unwrap().1,
                    child: std::mem::take(&mut self.children[0]).unwrap(),
                }
            }
            (None, None) => {
                assert_eq!(
                    self.next_child_index,
                    16,
                    "{:?}: Expected 16 children, found {}",
                    self.hash_and_size.unwrap().0,
                    self.next_child_index
                );
                ParsedTrieNode::Branch {
                    hash: self.hash_and_size.unwrap().0,
                    memory_usage: self.hash_and_size.unwrap().1,
                    children: std::mem::take(&mut self.children),
                }
            }
            (Some(value), None) => {
                assert_eq!(
                    self.next_child_index,
                    16,
                    "{:?}: Expected 16 children, found {}",
                    self.hash_and_size.unwrap().0,
                    self.next_child_index
                );
                ParsedTrieNode::BranchWithValue {
                    hash: self.hash_and_size.unwrap().0,
                    memory_usage: self.hash_and_size.unwrap().1,
                    children: std::mem::take(&mut self.children),
                    value,
                }
            }
        };
        TrieNodeAlloc::new_from_parsed(parsed_node)
    }

    pub fn add_child(&mut self, child: TrieNodeRef) {
        if self.placeholder_length.is_some() {
            self.pending_children.push(child.into());
            return;
        }
        assert!(
            self.next_child_index < self.expected_children.len(),
            "Too many children; expected {}, actual index {}",
            self.expected_children.len(),
            self.next_child_index
        );
        assert_eq!(
            self.expected_children[self.next_child_index],
            Some(child.hash()),
            "Expected child {:?} at index {}, found {:?}. Expected children: {:?}",
            self.expected_children[self.next_child_index],
            self.next_child_index,
            child.hash(),
            self.expected_children
        );
        self.children[self.next_child_index] = Some(child.into());
        self.next_child_index += 1;
        while self.next_child_index < self.expected_children.len()
            && self.expected_children[self.next_child_index].is_none()
        {
            self.next_child_index += 1;
        }
    }

    pub fn is_complete(&self) -> bool {
        self.next_child_index == self.expected_children.len()
    }

    pub fn child_path_length(&self) -> usize {
        if let Some(len) = self.placeholder_length {
            self.path.len() + len
        } else {
            if let Some(ext) = &self.extension {
                self.path.len() + NibbleSlice::from_encoded(ext.as_ref()).0.len()
            } else {
                self.path.len() + 1
            }
        }
    }

    pub fn split_placeholder(&mut self, child_path: FlatNodeNibbles) -> InMemoryTrieNodeBuilder {
        assert!(self.placeholder_length.is_some());
        assert!(self.child_path_length() > child_path.len());
        let new_placeholder_len = self.child_path_length() - child_path.len();
        *self.placeholder_length.as_mut().unwrap() -= new_placeholder_len;
        let mut new_builder = InMemoryTrieNodeBuilder::placeholder(child_path, new_placeholder_len);
        new_builder.pending_children = std::mem::take(&mut self.pending_children);
        new_builder
    }
}

impl Debug for InMemoryTrieNodeBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.path)?;
        if let Some(placeholder_length) = self.placeholder_length {
            write!(f, " placeholder({})", placeholder_length)?;
        } else {
            if let Some(extension) = &self.extension {
                let mut nibble = FlatNodeNibbles::new();
                nibble.append_encoded_slice(&extension);
                write!(f, " extension({:?})", nibble)?;
            }
            if self.leaf.is_some() {
                write!(f, " leaf")?;
            }
            if self.expected_children.len() > 1 {
                write!(f, " branch({})", self.next_child_index)?;
            }
        }
        Ok(())
    }
}

fn calculate_first_child_path(
    parent: &FlatNodeNibbles,
    raw_node: &RawTrieNodeWithSize,
) -> FlatNodeNibbles {
    let mut result = parent.clone();
    match &raw_node.node {
        RawTrieNode::Leaf(extension, _) | RawTrieNode::Extension(extension, _) => {
            result.append_encoded_slice(&extension)
        }
        RawTrieNode::BranchNoValue(children) | RawTrieNode::BranchWithValue(_, children) => {
            let index = children
                .0
                .iter()
                .enumerate()
                .find(|(_, child)| child.is_some())
                .map(|(i, _)| i)
                .expect("Branch has no children");
            result.push(index as u8);
        }
    }
    result
}

struct BuilderStack {
    stack: Vec<InMemoryTrieNodeBuilder>,
    root: Option<TrieNodeRef>,
    set: TrieNodeAllocSet,
    sizes: SizesStats,
}

#[derive(Default, Debug)]
pub struct SizesStats {
    leaf_node_count: usize,
    extension_node_count: usize,
    branch_node_count: usize,
    branch_nodes_with_value_count: usize,
    children_ptr_count: usize,
    extension_total_bytes: usize,
}

impl BuilderStack {
    pub fn new() -> BuilderStack {
        BuilderStack {
            stack: Vec::new(),
            root: None,
            set: TrieNodeAllocSet::new(),
            sizes: SizesStats::default(),
        }
    }

    pub fn print(&self) {
        for (i, builder) in self.stack.iter().enumerate() {
            println!("{}: {:?}", i, builder);
        }
    }

    pub fn add_node(&mut self, path: FlatNodeNibbles, raw_node: RawTrieNodeWithSize) {
        while !self.stack.is_empty() {
            let top = self.stack.last().unwrap();
            if top.path.is_prefix_of(&path) {
                break;
            } else if !path.is_prefix_of(&top.path) {
                self.pop();
            } else {
                break;
            }
        }

        // Let's separate out the parts of the stack that are longer than the path
        // later we'll put them back.
        let mut top_part = Vec::new();
        while !self.stack.is_empty() {
            let top = self.stack.last().unwrap();
            if path.is_prefix_of(&top.path) && &path != &top.path {
                top_part.push(self.stack.pop().unwrap());
            } else {
                break;
            }
        }

        if self.stack.is_empty() {
            // this is the root node.
            assert!(top_part.is_empty());
            assert_eq!(path.len(), 0);
            self.stack.push(InMemoryTrieNodeBuilder::from_raw_node(path, raw_node));
            return;
        }

        // Now look at the top of the stack. There are three cases:
        //  1. The top of the stack is exactly the desired path, just return that.
        //  2. The top of the stack is not the desired path, but the desired path
        //     is exactly what should be placed on top of the stack. In this case,
        //     we add a new node to the stack and return that.
        //  3. The top of the stack is not the desired path, and the desired path
        //     is shorter than what should be placed on the stack. In this case,
        //     we need to split the top of the stack into two nodes.
        let top = self.stack.last_mut().unwrap();
        if &path == &top.path {
            assert!(
                top.placeholder_length.is_some(),
                "Top of the stack should be a placeholder when inserting a node at that exact path"
            );
            let first_child_path = calculate_first_child_path(&path, &raw_node);
            assert!(
                first_child_path.len() <= top.child_path_length(),
                "Raw node has path length {} greater than placeholder length {}",
                first_child_path.len(),
                top.child_path_length()
            );
            let longer = if first_child_path.len() < top.child_path_length() {
                Some(top.split_placeholder(first_child_path))
            } else {
                None
            };
            top.set_raw_node(raw_node);
            if let Some(longer) = longer {
                self.stack.push(longer);
            }
        } else {
            let child_path_length = top.child_path_length();
            if child_path_length <= path.len() {
                if child_path_length < path.len() {
                    // We need a new placeholder in between, and then we can add the node.
                    self.stack.push(InMemoryTrieNodeBuilder::placeholder(
                        path.prefix(child_path_length),
                        path.len() - child_path_length,
                    ));
                }
                self.stack.push(InMemoryTrieNodeBuilder::from_raw_node(path, raw_node));
                assert!(
                    top_part.is_empty(),
                    "Top part should be empty when inserting a new node at the right path"
                );
            } else {
                // The node is a placeholder that represented more than 1 node, so split it.
                assert!(top.placeholder_length.is_some(), "Top of the stack should be a placeholder when inserting a node into the middle of the path");
                let mut longer = top.split_placeholder(path);
                longer.set_raw_node(raw_node);
                self.stack.push(longer);
            }
        };
        for top in top_part.into_iter().rev() {
            self.stack.push(top);
        }
    }

    fn pop(&mut self) {
        let top = self.stack.pop().unwrap();
        let (built, is_new) = self.set.insert_with_dedup(top.build());
        if is_new {
            built.increment_stats(&mut self.sizes);
        }
        if self.stack.is_empty() {
            assert!(self.root.is_none(), "Root already set");
            self.root = Some(built);
        } else {
            self.stack.last_mut().unwrap().add_child(built);
        }
    }

    pub fn finalize(mut self) -> LoadedInMemoryTrie {
        while !self.stack.is_empty() {
            self.pop();
        }
        LoadedInMemoryTrie { root: self.root.unwrap(), set: self.set, sizes: self.sizes }
    }
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

pub fn load_trie_in_memory_compact(
    store: &Store,
    shard_uid: ShardUId,
    state_root: CryptoHash,
) -> anyhow::Result<LoadedInMemoryTrie> {
    let start_time = Instant::now();
    let mut node_stack = BuilderStack::new();
    let mut last_print = Instant::now();
    let mut nodes_iterated = 0;
    for item in store.iter_prefix(DBCol::FlatNodes, &shard_uid.try_to_vec().unwrap()) {
        let item = item?;
        let key = FlatNodeNibbles::from_encoded_key(&item.0.as_ref()[8..]);
        let node = RawTrieNodeWithSize::try_from_slice(item.1.as_ref())?;
        // println!("Adding node: {:?} -> {:?}", key, node);
        node_stack.add_node(key, node);

        nodes_iterated += 1;
        if last_print.elapsed() > Duration::from_secs(10) {
            println!(
                "Loaded {} nodes ({} after dedup), stats: {:?}, current stack:",
                nodes_iterated,
                node_stack.set.nodes.len(),
                node_stack.sizes,
            );
            node_stack.print();
            last_print = Instant::now();
        }
    }
    let trie = node_stack.finalize();
    println!(
        "Loaded {} nodes ({} after dedup), took {:?}; stats: {:?}",
        nodes_iterated,
        trie.set.nodes.len(),
        start_time.elapsed(),
        trie.sizes
    );
    assert_eq!(trie.root.hash(), state_root);
    Ok(trie)
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

        let _trie = load_trie_in_memory_compact(&store, shard_uid, state_root)?;
        for _ in 0..1000000 {
            std::thread::sleep(Duration::from_secs(100));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use near_primitives::hash::hash;
    use near_primitives::state::ValueRef;
    use near_store::test_utils::{create_tries, test_populate_trie};
    use near_store::{ShardUId, Trie, TrieUpdate};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use crate::flat_nodes::creator::create_flat_nodes;
    use crate::in_memory_trie_compact::{load_trie_in_memory, ParsedTrieNodePtr};
    use crate::in_memory_trie_lookup::InMemoryTrieCompact;

    use super::{ParsedTrieNode, TrieNodeAlloc};

    fn check(keys: Vec<Vec<u8>>) {
        let description =
            if keys.len() <= 20 { format!("{keys:?}") } else { format!("{} keys", keys.len()) };
        eprintln!("TEST CASE {description}");
        let shard_tries = create_tries();
        let shard_uid = ShardUId::single_shard();
        let state_root = test_populate_trie(
            &shard_tries,
            &Trie::EMPTY_ROOT,
            shard_uid,
            keys.iter().map(|key| (key.to_vec(), Some(key.to_vec()))).collect(),
        );

        create_flat_nodes(shard_tries.get_store(), shard_uid, &state_root);
        eprintln!("flat nodes created");
        let loaded_in_memory_trie =
            load_trie_in_memory_compact(&shard_tries.get_store(), shard_uid, state_root).unwrap();
        eprintln!("In memory trie loaded");

        let trie_update = TrieUpdate::new(shard_tries.get_trie_for_shard(shard_uid, state_root));
        trie_update.set_trie_cache_mode(near_primitives::types::TrieCacheMode::CachingChunk);
        let trie = trie_update.trie();
        let in_memory_trie = InMemoryTrieCompact::new(
            shard_uid,
            shard_tries.get_store(),
            loaded_in_memory_trie.root.clone(),
        );
        for key in keys.iter() {
            let actual_value_ref = in_memory_trie.get_ref(key);
            let expected_value_ref = trie.get_ref(key, near_store::KeyLookupMode::Trie).unwrap();
            assert_eq!(actual_value_ref, expected_value_ref);
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
    fn flat_nodes_rand_large_data() {
        check_random(32, 100000, 1);
    }

    #[test]
    fn check_encoding() {
        let parsed = ParsedTrieNode::Leaf {
            value: ValueRef { hash: hash(b"abcde"), length: 123 },
            extension: Box::new([1, 2, 3, 4, 5]),
        };
        let encoded = TrieNodeAlloc::new_from_parsed(parsed.clone());
        assert_eq!(encoded.get_ref().parse(), parsed);
        drop(encoded);

        let parsed = ParsedTrieNode::Extension {
            hash: hash(b"abcde"),
            memory_usage: 12345,
            extension: Box::new([5, 6, 7, 8, 9]),
            child: ParsedTrieNodePtr { data: 0x123456789a as *const u8 },
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
                Some(ParsedTrieNodePtr { data: 0x123456789a01 as *const u8 }),
                None,
                None,
                Some(ParsedTrieNodePtr { data: 0x123456789a02 as *const u8 }),
                Some(ParsedTrieNodePtr { data: 0x123456789a03 as *const u8 }),
                None,
                None,
                Some(ParsedTrieNodePtr { data: 0x123456789a04 as *const u8 }),
                None,
                None,
                Some(ParsedTrieNodePtr { data: 0x123456789a05 as *const u8 }),
                None,
                Some(ParsedTrieNodePtr { data: 0x123456789a06 as *const u8 }),
                Some(ParsedTrieNodePtr { data: 0x123456789a07 as *const u8 }),
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
                Some(ParsedTrieNodePtr { data: 0x123456789a01 as *const u8 }),
                None,
                None,
                Some(ParsedTrieNodePtr { data: 0x123456789a02 as *const u8 }),
                Some(ParsedTrieNodePtr { data: 0x123456789a03 as *const u8 }),
                None,
                None,
                Some(ParsedTrieNodePtr { data: 0x123456789a04 as *const u8 }),
                None,
                None,
                Some(ParsedTrieNodePtr { data: 0x123456789a05 as *const u8 }),
                None,
                Some(ParsedTrieNodePtr { data: 0x123456789a06 as *const u8 }),
                Some(ParsedTrieNodePtr { data: 0x123456789a07 as *const u8 }),
            ],
            value: ValueRef { hash: hash(b"abcdef"), length: 123 },
        };
        let encoded = TrieNodeAlloc::new_from_parsed(parsed.clone());
        assert_eq!(encoded.get_ref().parse(), parsed);
        drop(encoded);
    }
}

/*
shard 0:
Loaded 59844482 nodes (58323289 after dedup), took 256.838155903s; stats: SizesStats { leaf_node_count: 33756917, extension_node_count: 5601563, branch_node_count: 18964284, branch_nodes_with_value_count: 525, children_ptr_count: 53825576, extension_total_bytes: 2336231902 }

shard 1:
Loaded 59555105 nodes (47203012 after dedup), took 269.64264202s; stats: SizesStats { leaf_node_count: 29864388, extension_node_count: 4441354, branch_node_count: 12897265, branch_nodes_with_value_count: 5, children_ptr_count: 49567403, extension_total_bytes: 661292598 }

Shard 2:
Loaded 45629386 nodes (40310944 after dedup), took 221.615275672s; stats: SizesStats { leaf_node_count: 23220366, extension_node_count: 4438550, branch_node_count: 12630852, branch_nodes_with_value_count: 21176, children_ptr_count: 38695536, extension_total_bytes: 1249793288 }

Shard 3:
Loaded 119170182 nodes (104589211 after dedup), took 528.250730273s; stats: SizesStats { leaf_node_count: 69794452, extension_node_count: 8612669, branch_node_count: 26168152, branch_nodes_with_value_count: 13938, children_ptr_count: 102318714, extension_total_bytes: 1910740734 }
*/
