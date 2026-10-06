//! Measured heap per retained index-queue operation against its admission
//! charge.
//!
//! Requires `production-coverage`. The binary installs a global allocator
//! that charges each thread for its own allocations at glibc's chunk size
//! (the product image's allocator), so concurrent tests never pollute a
//! measurement and small allocations cost what they cost in production.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use db::config::IndexOperationQueueTuning;
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

/// The heap one retained operation holds in the admission ledger, in one
/// decoded copy of its queue, and in one resolution of its queue's merge
/// operands stays within twice what it is charged, for every operation shape;
/// the fixed overhead never charges more than the smallest operation costs.
#[test]
fn retained_operation_memory_stays_within_twice_its_charge() {
    let overhead = IndexOperationQueueTuning::OPERATION_OVERHEAD_BYTES;
    let samples = db::production_coverage::index_operation_queue_memory_samples(&ThreadProbe);
    println!(
        "{:<24} {:>7} {:>7} {:>7} {:>7} {:>8} {:>7} {:>9} {:>10}",
        "shape",
        "encoded",
        "charged",
        "ledger",
        "decoded",
        "res/base",
        "res/ops",
        "heap/chg",
        "heap/enc"
    );
    let heap = |sample: &QueueMemorySample| {
        sample.ledger
            + sample.decoded
            + sample
                .resolution_over_base
                .max(sample.resolution_of_operands)
    };
    // Every shape is reported before any is checked, so one failure still
    // shows the whole calibration.
    for sample in &samples {
        let QueueMemorySample {
            shape,
            encoded,
            charged,
            ledger,
            decoded,
            resolution_over_base,
            resolution_of_operands,
        } = *sample;
        println!(
            "{shape:<24} {encoded:>7} {charged:>7} {ledger:>7} {decoded:>7} \
             {resolution_over_base:>8} {resolution_of_operands:>7} {:>9.2} {:>10.2}",
            heap(sample) as f64 / charged as f64,
            heap(sample) as f64 / encoded as f64,
        );
    }
    for sample in &samples {
        let shape = sample.shape;
        assert_eq!(sample.charged, sample.encoded + overhead, "{shape}");
        assert!(
            heap(sample) <= 2 * sample.charged,
            "{shape}: {} heap bytes for {sample:?}",
            heap(sample)
        );
        assert!(
            heap(sample) >= overhead,
            "{shape}: the overhead exceeds what it covers"
        );
    }
}
