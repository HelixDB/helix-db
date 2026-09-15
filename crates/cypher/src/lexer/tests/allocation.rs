//! Calling-thread allocation requests, excluding source and assertion setup.
use std::{alloc, cell::Cell};

thread_local! {
    static COUNT: Cell<Option<(usize, usize)>> = const { Cell::new(None) };
}
struct Observer;
#[global_allocator]
static ALLOCATOR: Observer = Observer;

fn record(bytes: usize) {
    let _ = COUNT.try_with(|cell| {
        let Some((allocations, total)) = cell.get() else {
            return;
        };
        cell.set(Some((allocations + 1, total + bytes)));
    });
}

// SAFETY: Every operation forwards the allocator's original valid arguments
// to System. The observer performs allocation-free thread-local arithmetic.
unsafe impl alloc::GlobalAlloc for Observer {
    unsafe fn alloc(&self, layout: alloc::Layout) -> *mut u8 {
        // SAFETY: The allocator caller supplies a valid layout.
        let pointer = unsafe { alloc::System.alloc(layout) };
        if !pointer.is_null() {
            record(layout.size());
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: alloc::Layout) -> *mut u8 {
        // SAFETY: The allocator caller supplies a valid layout.
        let pointer = unsafe { alloc::System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record(layout.size());
        }
        pointer
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: alloc::Layout, size: usize) -> *mut u8 {
        // SAFETY: Forward the original allocation and valid requested size.
        let next = unsafe { alloc::System.realloc(pointer, layout, size) };
        if !next.is_null() {
            record(size);
        }
        next
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: alloc::Layout) {
        // SAFETY: Pointer and layout still identify the original allocation.
        unsafe { alloc::System.dealloc(pointer, layout) };
    }
}

pub(super) fn observe<T>(operation: impl FnOnce() -> T) -> (T, (usize, usize)) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            COUNT.with(|cell| cell.set(None));
        }
    }
    COUNT.with(|cell| {
        assert!(cell.get().is_none(), "allocation observations cannot nest");
        cell.set(Some((0, 0)));
    });
    let reset = Reset;
    let result = operation();
    let count = COUNT.with(|cell| cell.get().expect("active observation"));
    drop(reset);
    (result, count)
}
