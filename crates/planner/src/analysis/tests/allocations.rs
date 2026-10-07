//! Independent allocation requests observed on the calling test thread only.
//! Counters include successful allocations and reallocations, not retained RSS.
use std::{alloc, cell::Cell};

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Count {
    pub allocations: usize,
    pub bytes: usize,
}

thread_local! {
    static COUNT: Cell<Option<Count>> = const { Cell::new(None) };
}
struct ObservedAllocator;
#[global_allocator]
static ALLOCATOR: ObservedAllocator = ObservedAllocator;

fn allocated(bytes: usize) {
    let _ = COUNT.try_with(|cell| {
        let Some(mut count) = cell.get() else {
            return;
        };
        count.allocations = count.allocations.saturating_add(1);
        count.bytes = count.bytes.saturating_add(bytes);
        cell.set(Some(count));
    });
}

// SAFETY: All allocation operations delegate unchanged valid pointers/layouts
// to System. Observation performs only allocation-free thread-local arithmetic.
unsafe impl alloc::GlobalAlloc for ObservedAllocator {
    unsafe fn alloc(&self, layout: alloc::Layout) -> *mut u8 {
        // SAFETY: The allocator caller supplies a valid layout.
        let pointer = unsafe { alloc::System.alloc(layout) };
        if !pointer.is_null() {
            allocated(layout.size());
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: alloc::Layout) -> *mut u8 {
        // SAFETY: The allocator caller supplies a valid layout.
        let pointer = unsafe { alloc::System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            allocated(layout.size());
        }
        pointer
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: alloc::Layout, size: usize) -> *mut u8 {
        // SAFETY: Forward the original allocation and valid requested size.
        let next = unsafe { alloc::System.realloc(pointer, layout, size) };
        if !next.is_null() {
            allocated(size);
        }
        next
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: alloc::Layout) {
        // SAFETY: Pointer and layout remain those of their original allocation.
        unsafe { alloc::System.dealloc(pointer, layout) };
    }
}

pub(crate) fn observe<T>(operation: impl FnOnce() -> T) -> (T, Count) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            COUNT.with(|cell| cell.set(None));
        }
    }
    COUNT.with(|cell| {
        assert!(cell.get().is_none(), "allocation observations cannot nest");
        cell.set(Some(Count::default()));
    });
    let reset = Reset;
    let result = operation();
    let count = COUNT.with(|cell| cell.get().expect("active allocation observation"));
    drop(reset);
    (result, count)
}
