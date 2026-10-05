//! Measured heap per retained index-queue operation.
//!
//! Requires `production-coverage`. The binary installs a global allocator
//! that charges each thread for its own allocations at glibc's chunk size
//! (the product image's allocator), so concurrent tests never pollute a
//! measurement and small allocations cost what they cost in production.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use db::production_coverage::{AllocationProbe, QueueMemorySample};

thread_local! {
    static ALLOCATED: Cell<isize> = const { Cell::new(0) };
    static PEAK: Cell<isize> = const { Cell::new(0) };
}

/// glibc's chunk for a request on 64-bit targets: the size plus an 8-byte
/// header, rounded up to 16 bytes, at least 32.
const fn chunk(size: usize) -> isize {
    let chunk = (size + 8 + 15) & !15;
    (if chunk < 32 { 32 } else { chunk }) as isize
}

fn record(delta: isize) {
    let _ = ALLOCATED.try_with(|allocated| {
        let now = allocated.get() + delta;
        allocated.set(now);
        let _ = PEAK.try_with(|peak| peak.set(peak.get().max(now)));
    });
}

struct ThreadCountingAllocator;

// SAFETY: every method forwards to `System` with the caller's arguments and
// only updates this thread's counters afterwards.
unsafe impl GlobalAlloc for ThreadCountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller upholds `GlobalAlloc::alloc`'s contract.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record(chunk(layout.size()));
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller upholds `GlobalAlloc::alloc_zeroed`'s contract.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record(chunk(layout.size()));
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: the caller upholds `GlobalAlloc::dealloc`'s contract.
        unsafe { System.dealloc(pointer, layout) };
        record(-chunk(layout.size()));
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the caller upholds `GlobalAlloc::realloc`'s contract.
        let resized = unsafe { System.realloc(pointer, layout, new_size) };
        if !resized.is_null() {
            record(chunk(new_size) - chunk(layout.size()));
        }
        resized
    }
}

#[global_allocator]
static ALLOCATOR: ThreadCountingAllocator = ThreadCountingAllocator;

struct ThreadProbe;

impl AllocationProbe for ThreadProbe {
    fn allocated(&self) -> isize {
        ALLOCATED.with(Cell::get)
    }

    fn reset_peak(&self) {
        PEAK.with(|peak| peak.set(ALLOCATED.with(Cell::get)));
    }

    fn peak(&self) -> isize {
        PEAK.with(Cell::get)
    }
}

/// Reports the measured heap per retained operation of every queued shape.
#[test]
fn retained_operation_memory_per_shape() {
    let samples = db::production_coverage::index_operation_queue_memory_samples(&ThreadProbe);
    println!(
        "{:<24} {:>8} {:>8} {:>8} {:>10} {:>10}",
        "shape", "encoded", "ledger", "decoded", "res/base", "res/ops"
    );
    for QueueMemorySample {
        shape,
        encoded,
        ledger,
        decoded,
        resolution_over_base,
        resolution_of_operands,
    } in &samples
    {
        println!(
            "{shape:<24} {encoded:>8} {ledger:>8} {decoded:>8} {resolution_over_base:>10} \
             {resolution_of_operands:>10}"
        );
    }
}
