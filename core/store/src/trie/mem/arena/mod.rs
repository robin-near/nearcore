mod alloc;

use crate::trie::mem::arena::alloc::{MINIMUM_ARENA_SIZE_IN_MB, PAGE_SIZE};

use self::alloc::{allocate, deallocate, initialize_allocator};
use borsh::{BorshDeserialize, BorshSerialize};
use mmap_rs::{MmapFlags, MmapMut, MmapOptions, Reserved, UnsafeMmapFlags};
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::hash::Hash;
use std::mem::size_of;

pub struct Arena {
    mmap: MmapMut,
    file: Option<File>,
    reserved: Option<Reserved>,
}

unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

impl Arena {
    pub fn new(mb_size: usize) -> Arena {
        assert!(
            mb_size > MINIMUM_ARENA_SIZE_IN_MB,
            "Arena size must be at least {}MB",
            MINIMUM_ARENA_SIZE_IN_MB
        );
        let mmap = MmapOptions::new(mb_size * PAGE_SIZE).unwrap().map_mut().expect("mmap failed");
        println!("Arena: {:x}..{:x}", mmap.as_ptr() as usize, mmap.as_ptr() as usize + mmap.len());
        let mut result = Self { mmap, file: None, reserved: None };
        result.init_allocator();
        result
    }

    fn remap_reserved_with_file(reserved: Reserved, file: &std::fs::File, offset: u64) -> MmapMut {
        let mmap = unsafe {
            MmapOptions::new(reserved.size())
                .unwrap()
                .with_flags(MmapFlags::SHARED)
                .with_unsafe_flags(UnsafeMmapFlags::MAP_FIXED)
                .with_address(reserved.start())
                .with_file(file, offset)
                .map_mut()
                .expect("mmap failed")
        };
        std::mem::forget(reserved);
        mmap
    }

    pub fn new_with_file_backing(file: &std::path::PathBuf, max_size_in_pages: usize) -> Arena {
        unimplemented!();
        // let file =
        //     std::fs::OpenOptions::new().read(true).append(true).create(true).open(file).unwrap();
        // let mut size = file.metadata().unwrap().len() as usize;
        // if size % MmapOptions::page_size() != 0 {
        //     panic!("File size is not a multiple of page size");
        // }
        // println!("Mapping a file of size {}", size);
        // let need_initialization = size == 0;
        // if need_initialization {
        //     size = Self::EXPANSION_STEP;
        //     file.set_len(size as u64).unwrap();
        // }
        // let mut reserved = MmapOptions::new(max_size_in_pages * MmapOptions::page_size())
        //     .unwrap()
        //     .reserve()
        //     .expect("reserve failed");
        // let split = reserved.split_to(size).unwrap();
        // let mmap = Self::remap_reserved_with_file(split, &file, 0);
        // let mut result = Self { mmap, file: Some(file), reserved: Some(reserved) };
        // if need_initialization {
        //     result.init_allocator();
        // }
        // println!(
        //     "Arena: {:x}..{:x}, reserved: {:x}..{:x}",
        //     result.mmap.as_ptr() as usize,
        //     result.mmap.as_ptr() as usize + result.mmap.len(),
        //     result.reserved.as_ref().unwrap().start() as usize,
        //     result.reserved.as_ref().unwrap().start() as usize
        //         + result.reserved.as_ref().unwrap().size()
        // );
        // result
    }

    fn init_allocator(&mut self) {
        initialize_allocator(self);
    }

    fn raw_slice<'a>(&'a self, pos: usize, len: usize) -> &'a [u8] {
        &self.mmap.as_slice()[pos..pos + len]
    }

    fn raw_slice_mut<'a>(&'a mut self, pos: usize, len: usize) -> &'a mut [u8] {
        &mut self.mmap.as_mut_slice()[pos..pos + len]
    }

    pub fn slice<'a>(&'a self, pos: usize, len: usize) -> ArenaSlice<'a> {
        ArenaSlice { arena: self, pos, len }
    }

    pub fn slice_mut<'a>(&'a mut self, pos: usize, len: usize) -> ArenaSliceMut<'a> {
        ArenaSliceMut { arena: self, pos, len }
    }

    pub fn ptr<'a>(self: &'a Self, pos: usize) -> ArenaPtr<'a> {
        ArenaPtr { arena: self, pos }
    }

    pub fn ptr_mut<'a>(self: &'a mut Self, pos: usize) -> ArenaPtrMut<'a> {
        ArenaPtrMut { arena: self, pos }
    }

    // fn map_more_pages(&mut self) {
    //     let offset = self.mmap.len();
    //     assert!(self.file.is_some(), "Out of memory");
    //     assert!(self.reserved.is_some());
    //     let file = self.file.as_ref().unwrap();
    //     assert_eq!(file.metadata().unwrap().len(), offset as u64);
    //     // println!("Mapping more pages at {:x}", offset);
    //     file.set_len(offset as u64 + Self::EXPANSION_STEP as u64).unwrap();
    //     let reserved =
    //         self.reserved.as_mut().unwrap().split_to(Self::EXPANSION_STEP).expect("Out of memory");
    //     let mmap = Self::remap_reserved_with_file(reserved, file, offset as u64);
    //     self.mmap.merge(mmap).unwrap();
    // }

    pub fn alloc<'a>(&'a mut self, size: usize) -> ArenaSliceMut<'a> {
        allocate(self, size)
    }

    pub fn flush(&self) {
        self.mmap.flush(0..self.mmap.len()).unwrap();
    }
}

#[derive(Clone, Copy)]
pub struct ArenaPtr<'a> {
    arena: &'a Arena,
    pos: usize,
}

impl<'a> Debug for ArenaPtr<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "arena[{:x}]", self.pos)
    }
}

impl<'a> Hash for ArenaPtr<'a> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.pos.hash(state);
    }
}

impl<'a> PartialEq for ArenaPtr<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.arena as *const Arena == other.arena as *const Arena && self.pos == other.pos
    }
}

impl<'a> Eq for ArenaPtr<'a> {}

impl<'a> ArenaPtr<'a> {
    pub fn offset(&self, offset: usize) -> ArenaPtr<'a> {
        ArenaPtr { arena: self.arena, pos: self.pos + offset }
    }

    pub fn slice(&self, len: usize) -> ArenaSlice<'a> {
        ArenaSlice { arena: self.arena, pos: self.pos, len }
    }

    pub fn raw_offset(&self) -> usize {
        self.pos
    }

    pub fn arena(&self) -> &'a Arena {
        self.arena
    }

    pub fn read_usize(&self) -> usize {
        usize::try_from_slice(&self.arena.raw_slice(self.pos, 8)).unwrap()
    }
}

#[derive(Clone)]
pub struct ArenaSlice<'a> {
    arena: &'a Arena,
    pos: usize,
    len: usize,
}

impl<'a> Debug for ArenaSlice<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "arena[{:x}..{:x}]", self.pos, self.pos + self.len)
    }
}

impl<'a> ArenaSlice<'a> {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn ptr(&self) -> ArenaPtr<'a> {
        ArenaPtr { arena: self.arena, pos: self.pos }
    }

    pub fn as_slice(&self) -> &'a [u8] {
        &self.arena.raw_slice(self.pos, self.len)
    }

    pub unsafe fn as_slice_mut(&self) -> *mut [u8] {
        self.arena.raw_slice(self.pos, self.len) as *const [u8] as *mut [u8]
    }

    pub fn read_ptr_at(&self, pos: usize) -> ArenaPtr<'a> {
        let pos = usize::try_from_slice(&self.as_slice()[pos..][..size_of::<usize>()]).unwrap();
        ArenaPtr { arena: self.arena, pos }
    }

    pub fn read_bit_at(&self, pos: usize) -> bool {
        let byte = self.as_slice()[pos / 8];
        let bit = pos % 8;
        (byte & (1 << bit)) != 0
    }

    pub fn read_u8_at(&self, pos: usize) -> u8 {
        self.as_slice()[pos]
    }

    pub fn read_u32_at(&self, pos: usize) -> u32 {
        u32::try_from_slice(&self.as_slice()[pos..][..size_of::<u32>()]).unwrap()
    }

    pub fn subslice(&self, start: usize, len: usize) -> ArenaSlice<'a> {
        assert!(start + len <= self.len);
        ArenaSlice { arena: self.arena, pos: self.pos + start, len }
    }
}

pub struct ArenaPtrMut<'a> {
    arena: &'a mut Arena,
    pos: usize,
}

impl<'a> ArenaPtrMut<'a> {
    pub fn offset(&'a mut self, offset: usize) -> ArenaPtrMut<'a> {
        ArenaPtrMut { arena: self.arena, pos: self.pos + offset }
    }

    pub fn slice(&'a mut self, len: usize) -> ArenaSliceMut<'a> {
        ArenaSliceMut { arena: self.arena, pos: self.pos, len }
    }

    pub fn raw_offset(&self) -> usize {
        self.pos
    }

    pub fn arena(&'a self) -> &'a Arena {
        self.arena
    }

    pub fn write_usize(&'a mut self, value: usize) {
        value.serialize(&mut &mut self.arena.raw_slice_mut(self.pos, 8)).unwrap();
    }
}

pub struct ArenaSliceMut<'a> {
    arena: &'a mut Arena,
    pos: usize,
    len: usize,
}

impl<'a> ArenaSliceMut<'a> {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn ptr(self) -> ArenaPtrMut<'a> {
        ArenaPtrMut { arena: self.arena, pos: self.pos }
    }

    pub fn read_ptr_at(&self, pos: usize) -> usize {
        assert!(pos + 8 <= self.len);
        usize::try_from_slice(self.arena.raw_slice(self.pos + pos, size_of::<usize>())).unwrap()
    }

    pub fn read_u32_at(&self, pos: usize) -> u32 {
        assert!(pos + 4 <= self.len);
        u32::try_from_slice(self.arena.raw_slice(self.pos + pos, size_of::<u32>())).unwrap()
    }

    pub fn write_ptr_at(&mut self, pos: usize, ptr: usize) {
        assert!(pos + 8 <= self.len);
        ptr.serialize(&mut &mut self.arena.raw_slice_mut(self.pos + pos, 8)).unwrap();
    }

    pub fn write_bit_at(&mut self, pos: usize, value: bool) {
        assert!(pos / 8 < self.len);
        let byte = &mut self.arena.raw_slice_mut(self.pos + pos / 8, 1)[0];
        let bit = pos % 8;
        if value {
            *byte |= 1 << bit;
        } else {
            *byte &= !(1 << bit);
        }
    }

    pub fn write_u8_at(&mut self, pos: usize, value: u8) {
        assert!(pos < self.len);
        self.arena.raw_slice_mut(self.pos + pos, 1)[0] = value;
    }

    pub fn write_u32_at(&mut self, pos: usize, value: u32) {
        assert!(pos + 4 <= self.len);
        value.serialize(&mut &mut self.arena.raw_slice_mut(self.pos + pos, 4)).unwrap();
    }

    pub fn with_raw_slice(&mut self, f: impl FnOnce(&mut [u8])) {
        f(&mut self.arena.raw_slice_mut(self.pos, self.len));
    }

    pub fn raw_slice_mut<'b>(&'b mut self) -> &'b mut [u8] {
        self.arena.raw_slice_mut(self.pos, self.len)
    }

    pub fn subslice<'b>(&'b mut self, start: usize, len: usize) -> ArenaSliceMut<'b> {
        assert!(start + len <= self.len);
        ArenaSliceMut { arena: self.arena, pos: self.pos + start, len }
    }

    pub fn dealloc(self) {
        deallocate(self);
    }
}

pub trait BorshFixedSize {
    const SERIALIZED_SIZE: usize;
}
