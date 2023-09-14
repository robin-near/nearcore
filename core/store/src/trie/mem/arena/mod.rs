use borsh::{BorshDeserialize, BorshSerialize};
use mmap_rs::{MmapFlags, MmapMut, MmapOptions, Reserved, UnsafeMmapFlags};
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::hash::Hash;
use std::ptr::NonNull;
use std::sync::Arc;

pub struct Arena {
    mmap: MmapMut,
    file: Option<File>,
    reserved: Option<Reserved>,
}

unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

pub type ArenaHandle = Arc<Arena>;

impl Arena {
    const EXPANSION_STEP: usize = 2 * 1024 * 1024;
    pub fn new(size_in_pages: usize) -> ArenaHandle {
        let mmap = MmapOptions::new(size_in_pages * MmapOptions::page_size())
            .unwrap()
            .map_mut()
            .expect("mmap failed");
        println!("Arena: {:x}..{:x}", mmap.as_ptr() as usize, mmap.as_ptr() as usize + mmap.len());
        let result = Arc::new(Self { mmap, file: None, reserved: None });
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

    pub fn new_with_file_backing(
        file: &std::path::PathBuf,
        max_size_in_pages: usize,
    ) -> ArenaHandle {
        let file =
            std::fs::OpenOptions::new().read(true).append(true).create(true).open(file).unwrap();
        let mut size = file.metadata().unwrap().len() as usize;
        if size % MmapOptions::page_size() != 0 {
            panic!("File size is not a multiple of page size");
        }
        println!("Mapping a file of size {}", size);
        let need_initialization = size == 0;
        if need_initialization {
            size = Self::EXPANSION_STEP;
            file.set_len(size as u64).unwrap();
        }
        let mut reserved = MmapOptions::new(max_size_in_pages * MmapOptions::page_size())
            .unwrap()
            .reserve()
            .expect("reserve failed");
        let split = reserved.split_to(size).unwrap();
        let mmap = Self::remap_reserved_with_file(split, &file, 0);
        let result = Arc::new(Self { mmap, file: Some(file), reserved: Some(reserved) });
        if need_initialization {
            result.init_allocator();
        }
        println!(
            "Arena: {:x}..{:x}, reserved: {:x}..{:x}",
            result.mmap.as_ptr() as usize,
            result.mmap.as_ptr() as usize + result.mmap.len(),
            result.reserved.as_ref().unwrap().start() as usize,
            result.reserved.as_ref().unwrap().start() as usize
                + result.reserved.as_ref().unwrap().size()
        );
        result
    }

    fn init_allocator(&self) {
        (ArenaPtr::SIZE as usize).serialize(&mut self.as_slice_mut()).unwrap();
    }

    fn as_slice(&self) -> &[u8] {
        self.mmap.as_slice()
    }

    fn as_slice_mut(&self) -> &mut [u8] {
        self.unsafe_as_mut().mmap.as_mut_slice()
    }

    pub fn slice(self: &Arc<Self>, pos: usize, len: usize) -> ArenaSlice {
        ArenaSlice { arena: self.clone(), pos, len }
    }

    pub fn ptr(self: &Arc<Self>, pos: usize) -> ArenaPtr {
        ArenaPtr { arena: self.clone(), pos }
    }

    fn unsafe_as_mut(&self) -> &mut Self {
        unsafe { &mut *(self as *const Self as *mut Self) }
    }

    fn map_more_pages(&mut self) {
        let offset = self.mmap.len();
        assert!(self.file.is_some(), "Out of memory");
        assert!(self.reserved.is_some());
        let file = self.file.as_ref().unwrap();
        assert_eq!(file.metadata().unwrap().len(), offset as u64);
        // println!("Mapping more pages at {:x}", offset);
        file.set_len(offset as u64 + Self::EXPANSION_STEP as u64).unwrap();
        let reserved =
            self.reserved.as_mut().unwrap().split_to(Self::EXPANSION_STEP).expect("Out of memory");
        let mmap = Self::remap_reserved_with_file(reserved, file, offset as u64);
        self.mmap.merge(mmap).unwrap();
    }

    pub fn alloc(self: &Arc<Self>, size: usize) -> ArenaSlice {
        // TODO: this is a very dumb allocator for now.
        let pos = usize::try_from_slice(&self.as_slice()[0..ArenaPtr::SIZE]).unwrap();
        while pos + size > self.mmap.len() {
            self.unsafe_as_mut().map_more_pages();
        }
        let slice = ArenaSlice { arena: self.clone(), pos, len: size };
        (pos + size).serialize(&mut &mut self.as_slice_mut()[0..ArenaPtr::SIZE]).unwrap();
        slice
    }

    pub fn dealloc(&self, _slice: ArenaSlice) {
        panic!("Not supported");
    }

    pub fn flush(&self) {
        self.mmap.flush(0..self.mmap.len()).unwrap();
    }
}

#[derive(Clone)]
pub struct ArenaPtr {
    arena: ArenaHandle,
    pos: usize,
}

impl Debug for ArenaPtr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "arena[{:x}]", self.pos)
    }
}

impl Hash for ArenaPtr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.pos.hash(state);
    }
}

impl PartialEq for ArenaPtr {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.arena, &other.arena) && self.pos == other.pos
    }
}

impl Eq for ArenaPtr {}

impl ArenaPtr {
    pub const SIZE: usize = std::mem::size_of::<usize>();

    pub fn offset(&self, offset: usize) -> ArenaPtr {
        ArenaPtr { arena: self.arena.clone(), pos: self.pos + offset }
    }

    pub fn slice(&self, len: usize) -> ArenaSlice {
        ArenaSlice { arena: self.arena.clone(), pos: self.pos, len }
    }

    pub fn raw_offset(&self) -> usize {
        self.pos
    }

    pub fn arena(&self) -> &ArenaHandle {
        &self.arena
    }
}

#[derive(Clone)]
pub struct ArenaSlice {
    arena: ArenaHandle,
    pos: usize,
    len: usize,
}

impl Debug for ArenaSlice {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "arena[{:x}..{:x}]", self.pos, self.pos + self.len)
    }
}

impl ArenaSlice {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn ptr(self) -> ArenaPtr {
        ArenaPtr { arena: self.arena.clone(), pos: self.pos }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.arena.as_slice()[self.pos..self.pos + self.len]
    }

    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        &mut self.arena.as_slice_mut()[self.pos..self.pos + self.len]
    }

    pub fn read_ptr_at(&self, pos: usize) -> ArenaPtr {
        let pos = usize::try_from_slice(&self.as_slice()[pos..][..ArenaPtr::SIZE]).unwrap();
        ArenaPtr { arena: self.arena.clone(), pos }
    }

    pub fn write_ptr_at(&mut self, pos: usize, ptr: &ArenaPtr) {
        debug_assert!(Arc::ptr_eq(&ptr.arena, &self.arena));
        ptr.pos.serialize(&mut &mut self.as_slice_mut()[pos..][..ArenaPtr::SIZE]).unwrap();
    }

    pub fn subslice(&self, start: usize, len: usize) -> ArenaSlice {
        assert!(start + len <= self.len);
        ArenaSlice { arena: self.arena.clone(), pos: self.pos + start, len }
    }
}

pub trait BorshFixedSize {
    const SERIALIZED_SIZE: usize;
}
