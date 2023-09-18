use std::mem::size_of;

use super::{Arena, ArenaSliceMut};

pub(crate) const PAGE_SIZE: usize = 1024 * 1024;
pub(crate) const MINIMUM_ARENA_SIZE_IN_MB: usize =
    (PAGE_SIZE + NUM_ALLOCATION_CLASSES * PAGE_SIZE) / 1024 / 1024;

const fn allocation_class(size: usize) -> usize {
    if size <= 128 {
        (size + 8) / 8 - 1
    } else if size <= 256 {
        (size - 128 + 63) / 64 + allocation_class(128)
    } else {
        ((255 as usize).leading_zeros() - (size - 1).leading_zeros()) as usize
            + allocation_class(256)
    }
}

const fn allocation_size(size_class: usize) -> usize {
    if size_class <= allocation_class(128) {
        (size_class + 1) * 8
    } else if size_class <= allocation_class(256) {
        (size_class - allocation_class(128)) * 64 + 256
    } else {
        256 << (size_class - allocation_class(256))
    }
}

const NUM_ALLOCATION_CLASSES: usize = allocation_class(PAGE_SIZE) + 1;

const ENABLE_ARENA_DEBUG_CHECKS: bool = true;

pub fn initialize_allocator(arena: &mut Arena) {
    arena.ptr_mut(0).write_usize(1);
    for i in 0..NUM_ALLOCATION_CLASSES {
        arena.ptr_mut((1 + i) * 8).write_usize(usize::MAX);
    }
}

fn initialize_page_with_debug_checks(
    arena: &mut Arena,
    page_index: usize,
    allocation_class: usize,
) -> usize {
    let mut slice = arena.slice_mut(page_index * PAGE_SIZE, PAGE_SIZE);
    slice.write_u8_at(0, allocation_class as u8);
    let allocation_size = allocation_size(allocation_class);
    let num_allocs = (PAGE_SIZE * 8 - 5 * 8 - 7) / (allocation_size * 8 + 1);
    let flag_bytes_count = (num_allocs + 7) / 8;
    let first_alloc_offset = flag_bytes_count + 5;
    for i in 0..flag_bytes_count {
        slice.write_u8_at(i + 5, 0);
    }
    slice.write_u32_at(1, first_alloc_offset as u32);
    let first_alloc_ptr = page_index * PAGE_SIZE + first_alloc_offset;
    for i in 0..num_allocs {
        let next = if i == num_allocs - 1 {
            usize::MAX
        } else {
            first_alloc_ptr + (i + 1) * allocation_size
        };
        slice.write_ptr_at(first_alloc_offset + i * allocation_size, next);
    }
    first_alloc_ptr
}

fn initialize_page(arena: &mut Arena, page_index: usize, allocation_class: usize) -> usize {
    if ENABLE_ARENA_DEBUG_CHECKS {
        return initialize_page_with_debug_checks(arena, page_index, allocation_class);
    }
    let mut slice = arena.slice_mut(page_index * PAGE_SIZE, PAGE_SIZE);
    let allocation_size = allocation_size(allocation_class);
    let num_allocs = PAGE_SIZE / allocation_size;
    for i in 0..num_allocs {
        let next = if i == num_allocs - 1 {
            usize::MAX
        } else {
            page_index * PAGE_SIZE + (i + 1) * allocation_size
        };
        slice.write_ptr_at(i * allocation_size, next);
    }
    page_index * PAGE_SIZE
}

fn alloc_check(arena: &mut Arena, ptr: usize, size_class: usize, is_dealloc: bool) {
    let page_start = ptr & !(PAGE_SIZE - 1);
    let slice = arena.slice(page_start, PAGE_SIZE);
    assert_eq!(size_class, slice.read_u8_at(0) as usize);
    let first_alloc_offset = slice.read_u32_at(1) as usize;
    let alloc_index = (ptr - first_alloc_offset - page_start) / allocation_size(size_class);
    assert_eq!(ptr, page_start + first_alloc_offset + alloc_index * allocation_size(size_class));
    let is_allocated = slice.read_bit_at(5 * 8 + alloc_index);
    assert!(is_allocated == is_dealloc);
    arena.slice_mut(page_start, PAGE_SIZE).write_bit_at(5 * 8 + alloc_index, !is_dealloc);
}

pub fn allocate<'a>(arena: &'a mut Arena, size: usize) -> ArenaSliceMut<'a> {
    assert!(size != 0);
    let size_class = allocation_class(size);
    let freelist_ptr = (1 + size_class) * 8;
    let freelist = arena.ptr(freelist_ptr).read_usize();
    if freelist == usize::MAX {
        let page_index = arena.ptr(0).read_usize();
        arena.ptr_mut(0).write_usize(page_index + 1);
        let first_alloc = initialize_page(arena, page_index, size_class);
        arena.ptr_mut(freelist_ptr).write_usize(first_alloc);
    }
    let ptr = arena.ptr(freelist_ptr).read_usize();
    assert!(ptr != usize::MAX);
    let next = arena.slice(ptr, size_of::<usize>()).read_ptr_at(0).raw_offset();
    if ENABLE_ARENA_DEBUG_CHECKS {
        alloc_check(arena, ptr, size_class, false);
    }
    arena.ptr_mut(freelist_ptr).write_usize(next);
    arena.slice_mut(ptr, size)
}

pub fn deallocate<'a>(slice: ArenaSliceMut<'a>) {
    let arena = slice.arena;
    let ptr = slice.pos;
    let size_class = allocation_class(slice.len);
    let freelist_ptr = (1 + size_class) * 8;
    let freelist = arena.ptr(freelist_ptr).read_usize();
    if ENABLE_ARENA_DEBUG_CHECKS {
        alloc_check(arena, ptr, size_class, true);
    }
    arena.slice_mut(ptr, size_of::<usize>()).write_ptr_at(0, freelist);
    arena.ptr_mut(freelist_ptr).write_usize(ptr);
}

pub fn print_alloc_stats(arena: &Arena) {
    if !ENABLE_ARENA_DEBUG_CHECKS {
        println!("Arena debug checks are disabled");
        return;
    }
    let last_page = arena.ptr(0).read_usize();
    println!("Number of pages used: {}", last_page);
    let mut page_by_allocation_class = [0 as usize; NUM_ALLOCATION_CLASSES];
    let mut allocs_by_allocation_class = [0 as usize; NUM_ALLOCATION_CLASSES];
    for i in 1..last_page {
        let slice = arena.slice(i * PAGE_SIZE, PAGE_SIZE);
        let allocation_class = slice.read_u8_at(0) as usize;
        page_by_allocation_class[allocation_class] += 1;
        let first_alloc_offset = slice.read_u32_at(1) as usize;
        for i in 5..first_alloc_offset {
            let flag = slice.read_u8_at(i);
            allocs_by_allocation_class[allocation_class] += flag.count_ones() as usize;
        }
    }
    for i in 0..NUM_ALLOCATION_CLASSES {
        let slots_per_page = (PAGE_SIZE * 8 - 5 * 8 - 7) / (allocation_size(i) * 8 + 1);
        println!(
            "Allocation class {} ({} bytes): {} pages, {}/{} allocations",
            i,
            allocation_size(i),
            page_by_allocation_class[i],
            allocs_by_allocation_class[i],
            page_by_allocation_class[i] * slots_per_page
        );
    }
}

#[cfg(test)]
mod test {
    use crate::trie::mem::arena::Arena;

    #[test]
    fn test_allocate() {
        let mut arena = Arena::new(100);
        let slice = arena.alloc(10);
        let first_alloc_addr = slice.pos;
        slice.dealloc();
        let slice = arena.alloc(10);
        assert_eq!(first_alloc_addr, slice.pos);
        slice.dealloc();
    }
}
