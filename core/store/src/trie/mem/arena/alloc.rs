use super::{ArenaMemory, ArenaSliceMut};

const PAGE_SIZE: usize = 256 * 1024;
const ALLOC_BY_8_BYTES_BELOW: usize = 256;
const ALLOC_BY_64_BYTES_BELOW: usize = 512;

pub struct Allocator {
    freelists: [usize; NUM_ALLOCATION_CLASSES],
    allocated_pages: usize,
}

const fn allocation_class(size: usize) -> usize {
    if size <= ALLOC_BY_8_BYTES_BELOW {
        (size + 7) / 8 - 1
    } else if size <= ALLOC_BY_64_BYTES_BELOW {
        (size - ALLOC_BY_8_BYTES_BELOW + 63) / 64 + allocation_class(ALLOC_BY_8_BYTES_BELOW)
    } else {
        ((ALLOC_BY_64_BYTES_BELOW - 1).leading_zeros() - (size - 1).leading_zeros()) as usize
            + allocation_class(ALLOC_BY_64_BYTES_BELOW)
    }
}

const fn allocation_size(size_class: usize) -> usize {
    if size_class <= allocation_class(ALLOC_BY_8_BYTES_BELOW) {
        (size_class + 1) * 8
    } else if size_class <= allocation_class(ALLOC_BY_64_BYTES_BELOW) {
        (size_class - allocation_class(ALLOC_BY_8_BYTES_BELOW)) * 64 + ALLOC_BY_8_BYTES_BELOW
    } else {
        ALLOC_BY_64_BYTES_BELOW << (size_class - allocation_class(ALLOC_BY_64_BYTES_BELOW))
    }
}

const NUM_ALLOCATION_CLASSES: usize = allocation_class(PAGE_SIZE) + 1;

impl Allocator {
    pub fn minimum_arena_size_required() -> usize {
        NUM_ALLOCATION_CLASSES * PAGE_SIZE
    }

    pub fn new() -> Self {
        Self { freelists: [usize::MAX; NUM_ALLOCATION_CLASSES], allocated_pages: 0 }
    }

    pub fn allocate<'a>(&mut self, arena: &'a mut ArenaMemory, size: usize) -> ArenaSliceMut<'a> {
        assert!(size <= PAGE_SIZE, "Cannot allocate {} bytes", size);
        let size_class = allocation_class(size);
        let allocation_size = allocation_size(size_class);
        if self.freelists[size_class] == usize::MAX {
            if arena.mmap.len() < (self.allocated_pages + 1) * PAGE_SIZE {
                panic!("Arena out of memory");
            }
            let page_index = self.allocated_pages;
            self.allocated_pages += 1;
            let mut page_slice = arena.slice_mut(page_index * PAGE_SIZE, PAGE_SIZE);
            let num_allocations_per_page = PAGE_SIZE / allocation_size;
            for i in 0..num_allocations_per_page - 1 {
                page_slice
                    .write_ptr_at(i * allocation_size, page_slice.pos + (i + 1) * allocation_size);
            }
            page_slice.write_ptr_at((num_allocations_per_page - 1) * allocation_size, usize::MAX);
            self.freelists[size_class] = page_slice.pos;
        }
        let pos = self.freelists[size_class];
        self.freelists[size_class] = arena.ptr(pos).read_usize();
        arena.slice_mut(pos, size)
    }

    pub fn deallocate(&mut self, mut slice: ArenaSliceMut<'_>) {
        let size_class = allocation_class(slice.len);
        let pos = slice.pos;
        slice.write_ptr_at(0, self.freelists[size_class]);
        self.freelists[size_class] = pos;
    }
}

#[cfg(test)]
mod test {
    use crate::trie::mem::arena::Arena;

    use super::PAGE_SIZE;

    #[test]
    fn test_allocate() {
        let mut arena = Arena::new(100);
        let slice = arena.alloc(10);
        let first_alloc_addr = slice.pos;
        let len = slice.len;
        arena.dealloc(first_alloc_addr, len);
        let slice = arena.alloc(10);
        assert_eq!(first_alloc_addr, slice.pos);
        let pos = slice.pos;
        let len = slice.len;
        arena.dealloc(pos, len);
    }

    #[test]
    fn test_size_classes() {
        for i in 1..=PAGE_SIZE {
            let size_class = super::allocation_class(i);
            assert!(size_class < super::NUM_ALLOCATION_CLASSES,);
            let size = super::allocation_size(size_class);
            assert!(size >= i);
            if size_class > 0 {
                assert!(super::allocation_size(size_class - 1) < i);
            }
        }
    }
}
