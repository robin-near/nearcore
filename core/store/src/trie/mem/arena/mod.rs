mod alloc;
use self::alloc::Allocator;
use borsh::{BorshDeserialize, BorshSerialize};
use mmap_rs::{MmapFlags, MmapMut, MmapOptions};
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::mem::size_of;

pub struct Arena {
    memory: ArenaMemory,
    allocator: Allocator,
}

pub struct ArenaMemory {
    mmap: MmapMut,
}

unsafe impl Send for ArenaMemory {}

impl ArenaMemory {
    fn new(max_size_in_bytes: usize) -> Self {
        let mmap =
            MmapOptions::new(max_size_in_bytes.max(Allocator::minimum_arena_size_required()))
                .unwrap()
                .with_flags(MmapFlags::NO_RESERVE)
                .map_mut()
                .expect("mmap failed");
        Self { mmap }
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

    pub fn flush(&self) {
        self.mmap.flush(0..self.mmap.len()).unwrap();
    }
}

impl Arena {
    pub fn new(max_size_in_bytes: usize) -> Self {
        Self { memory: ArenaMemory::new(max_size_in_bytes), allocator: Allocator::new() }
    }

    pub fn alloc<'a>(&'a mut self, size: usize) -> ArenaSliceMut<'a> {
        self.allocator.allocate(&mut self.memory, size)
    }

    pub fn dealloc(&mut self, pos: usize, len: usize) {
        self.allocator.deallocate(self.memory.slice_mut(pos, len));
    }

    pub fn memory(&self) -> &ArenaMemory {
        &self.memory
    }

    pub fn memory_mut(&mut self) -> &mut ArenaMemory {
        &mut self.memory
    }
}

#[derive(Clone, Copy)]
pub struct ArenaPtr<'a> {
    arena: &'a ArenaMemory,
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
        self.arena as *const ArenaMemory == other.arena as *const ArenaMemory
            && self.pos == other.pos
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

    pub fn arena(&self) -> &'a ArenaMemory {
        self.arena
    }

    pub fn arena_mut(&mut self) -> &'a mut ArenaMemory {
        &mut self.arena
    }

    pub fn read_usize(&self) -> usize {
        usize::try_from_slice(&self.arena.raw_slice(self.pos, 8)).unwrap()
    }
}

#[derive(Clone)]
pub struct ArenaSlice<'a> {
    arena: &'a ArenaMemory,
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
    arena: &'a mut ArenaMemory,
    pos: usize,
}

impl<'a> ArenaPtrMut<'a> {
    pub fn offset<'b>(&'b mut self, offset: usize) -> ArenaPtrMut<'b> {
        ArenaPtrMut { arena: self.arena, pos: self.pos + offset }
    }

    pub fn slice<'b>(&'b self, len: usize) -> ArenaSlice<'b> {
        ArenaSlice { arena: self.arena, pos: self.pos, len }
    }

    pub fn slice_mut<'b>(&'b mut self, len: usize) -> ArenaSliceMut<'b> {
        ArenaSliceMut { arena: self.arena, pos: self.pos, len }
    }

    pub fn raw_offset(&self) -> usize {
        self.pos
    }

    pub fn arena_mut<'b>(&'b mut self) -> &'b mut ArenaMemory {
        &mut self.arena
    }

    pub fn write_usize(&'a mut self, value: usize) {
        value.serialize(&mut &mut self.arena.raw_slice_mut(self.pos, 8)).unwrap();
    }

    pub fn as_const(&'a self) -> ArenaPtr<'a> {
        ArenaPtr { arena: self.arena, pos: self.pos }
    }
}

pub struct ArenaSliceMut<'a> {
    arena: &'a mut ArenaMemory,
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
}

pub trait BorshFixedSize {
    const SERIALIZED_SIZE: usize;
}
