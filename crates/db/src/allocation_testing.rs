//! Independent allocation requests observed on the calling test thread only.
//! Counters include successful allocations and reallocations, not retained RSS.
use std::{alloc, cell::Cell};

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Count {
    pub allocations: usize,
    pub bytes: usize,
    /// Peak live bytes when every released allocation was created inside the
    /// observation. Reallocation includes old/new overlap. Other observations
    /// should use the cumulative request counters above.
    pub peak_bytes: usize,
    live_bytes: i128,
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
        count.live_bytes += bytes as i128;
        count.peak_bytes = count.peak_bytes.max(count.live_bytes.max(0) as usize);
        cell.set(Some(count));
    });
}

fn released(bytes: usize) {
    let _ = COUNT.try_with(|cell| {
        let Some(mut count) = cell.get() else {
            return;
        };
        count.live_bytes -= bytes as i128;
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
            released(layout.size());
        }
        next
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: alloc::Layout) {
        released(layout.size());
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

#[test]
fn owned_allocation_peaks_include_reallocation_overlap_and_release() {
    let (_, count) = observe(|| {
        let mut bytes = Vec::<u8>::with_capacity(64);
        bytes.extend_from_slice(&[7; 64]);
        bytes.reserve_exact(64);
        assert_eq!(bytes.capacity(), 128);
        drop(bytes);
        let final_buffer = vec![9_u8; 32];
        std::hint::black_box(&final_buffer);
    });
    assert_eq!(count.allocations, 3);
    assert_eq!(count.bytes, 64 + 128 + 32);
    assert_eq!(count.peak_bytes, 64 + 128);
    assert_eq!(count.live_bytes, 0);
}
