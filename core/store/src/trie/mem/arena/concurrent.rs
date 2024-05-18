use super::alloc::CHUNK_SIZE;
use super::{Arena, ArenaPos, ArenaSliceMut, IArena, IArenaMemory};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct ConcurrentArenaSharedState {
    next_chunk_pos: AtomicUsize,
}

#[derive(Clone)]
pub struct ConcurrentArena {
    shared: Arc<ConcurrentArenaSharedState>,
}

impl ConcurrentArena {
    pub fn new() -> Self {
        Self {
            shared: Arc::new(ConcurrentArenaSharedState { next_chunk_pos: AtomicUsize::new(0) }),
        }
    }

    pub fn for_thread(&self) -> ConcurrentArenaForThread {
        ConcurrentArenaForThread::new(self.shared.clone())
    }

    pub fn to_single_threaded(self, name: String, threads: Vec<ConcurrentArenaForThread>) -> Arena {
        let mut chunks = Vec::new();
        for thread in threads {
            let memory = thread.memory;
            let mut chunk_pos = Vec::with_capacity(memory.chunks.len());
            chunk_pos.resize(memory.chunks.len(), 0);
            for (pos, index) in memory.chunk_pos_to_index.into_iter().enumerate() {
                if index == usize::MAX {
                    continue;
                }
                chunk_pos[index] = pos;
            }
            for (pos, chunk) in chunk_pos.into_iter().zip(memory.chunks.into_iter()) {
                while chunks.len() <= pos {
                    chunks.push(Vec::new());
                }
                chunks[pos] = chunk;
            }
        }
        Arena::new_from_existing_chunks(name, chunks)
    }
}

pub struct ConcurrentArenaForThread {
    memory: ConcurrentArenaMemory,
    allocator: ConcurrentArenaAllocator,
}

pub struct ConcurrentArenaMemory {
    chunks: Vec<Vec<u8>>,
    chunk_pos_to_index: Vec<usize>,
}

impl ConcurrentArenaMemory {
    pub fn new() -> Self {
        Self { chunks: Vec::new(), chunk_pos_to_index: Vec::new() }
    }

    pub fn add_chunk(&mut self, pos: usize) {
        while self.chunk_pos_to_index.len() <= pos {
            self.chunk_pos_to_index.push(usize::MAX);
        }
        self.chunk_pos_to_index[pos] = self.chunks.len();
        self.chunks.push(vec![0; CHUNK_SIZE]);
    }

    pub fn chunk(&self, pos: usize) -> &[u8] {
        let index = self.chunk_pos_to_index[pos];
        &self.chunks[index]
    }

    pub fn chunk_mut(&mut self, pos: usize) -> &mut [u8] {
        let index = self.chunk_pos_to_index[pos];
        &mut self.chunks[index]
    }
}

impl IArenaMemory for ConcurrentArenaMemory {
    fn raw_slice(&self, pos: ArenaPos, len: usize) -> &[u8] {
        &self.chunk(pos.chunk())[pos.pos()..pos.pos() + len]
    }

    fn raw_slice_mut(&mut self, pos: ArenaPos, len: usize) -> &mut [u8] {
        &mut self.chunk_mut(pos.chunk())[pos.pos()..pos.pos() + len]
    }
}

pub struct ConcurrentArenaAllocator {
    shared: Arc<ConcurrentArenaSharedState>,
    next_pos: ArenaPos,
}

impl ConcurrentArenaAllocator {
    fn new(shared: Arc<ConcurrentArenaSharedState>) -> Self {
        Self { shared, next_pos: ArenaPos::invalid() }
    }

    pub fn allocate<'a>(
        &mut self,
        arena: &'a mut ConcurrentArenaMemory,
        size: usize,
    ) -> ArenaSliceMut<'a, ConcurrentArenaMemory> {
        if self.next_pos.is_invalid() || self.next_pos.pos() + size > CHUNK_SIZE {
            let next_chunk_pos = self.shared.next_chunk_pos.fetch_add(1, Ordering::Relaxed);
            self.next_pos = ArenaPos { chunk: next_chunk_pos as u32, pos: 0 };
            arena.add_chunk(next_chunk_pos);
        }
        let pos = self.next_pos;
        self.next_pos = pos.offset_by(size);
        ArenaSliceMut::new(arena, pos, size)
    }
}

impl ConcurrentArenaForThread {
    fn new(shared: Arc<ConcurrentArenaSharedState>) -> Self {
        Self {
            memory: ConcurrentArenaMemory::new(),
            allocator: ConcurrentArenaAllocator::new(shared),
        }
    }
}

impl IArena for ConcurrentArenaForThread {
    type Memory = ConcurrentArenaMemory;

    fn memory(&self) -> &Self::Memory {
        &self.memory
    }

    fn memory_mut(&mut self) -> &mut Self::Memory {
        &mut self.memory
    }

    fn alloc(&mut self, size: usize) -> ArenaSliceMut<Self::Memory> {
        self.allocator.allocate(&mut self.memory, size)
    }
}
