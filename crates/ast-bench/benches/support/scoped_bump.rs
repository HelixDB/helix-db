//! A global allocator that serves every allocation a thread makes inside
//! [`scope`] from that thread's bump region, and frees the whole region in
//! one step when the scope ends.
//!
//! Inside a scope, allocating is a pointer bump, growing the most recent
//! allocation happens in place, and freeing costs nothing (the most recent
//! allocation is popped, as bumpalo does). That makes it the upper bound of
//! any arena the scoped work could adopt: a real arena would still pay for
//! whatever must outlive it. Allocations made outside a scope, or that no
//! longer fit the region, go to mimalloc.
//!
//! [`scope`] asserts that nothing it allocated is still live when it ends,
//! so the region is only ever reset after the work freed everything it
//! allocated there.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

/// Address space reserved per thread; pages are committed when first used.
const REGION_BYTES: usize = 1 << 30;

const REGION_ALIGN: usize = 4096;

#[derive(Clone, Copy)]
struct Region {
    base: usize,
    end: usize,
    top: usize,
    /// Start of the most recent allocation, which may grow in place; zero
    /// when there is none.
    last: usize,
    /// Allocations in the region not yet freed.
    live: usize,
    active: bool,
}

impl Region {
    const EMPTY: Self = Self {
        base: 0,
        end: 0,
        top: 0,
        last: 0,
        live: 0,
        active: false,
    };

    fn contains(self, pointer: *mut u8) -> bool {
        (self.base..self.end).contains(&(pointer as usize))
    }
}

/// The region's layout; its size is non-zero.
fn region_layout() -> Layout {
    Layout::from_size_align(REGION_BYTES, REGION_ALIGN).expect("region layout is valid")
}

/// Owns the calling thread's region and returns it when the thread exits.
struct Owner;

impl Owner {
    fn reserve() -> Self {
        // SAFETY: The region layout has a non-zero size.
        let base = unsafe { System.alloc(region_layout()) } as usize;
        assert_ne!(base, 0, "the region reserves its address space");
        REGION.set(Region {
            base,
            end: base + REGION_BYTES,
            top: base,
            ..Region::EMPTY
        });
        Self
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        let region = REGION.replace(Region::EMPTY);
        // SAFETY: `base` came from `System.alloc` with this layout, and the
        // region no longer routes any pointer to itself.
        unsafe { System.dealloc(region.base as *mut u8, region_layout()) };
    }
}

thread_local! {
    // A `const`, drop-free thread local, so the allocator reads it without
    // allocating and still can while other thread locals are destroyed.
    static REGION: Cell<Region> = const { Cell::new(Region::EMPTY) };
    static OWNER: Owner = Owner::reserve();
}

/// Run `work` with every allocation this thread makes served from its
/// region, then reset the region.
///
/// # Panics
///
/// Panics when scopes nest, or when an allocation `work` made in the region
/// is still live once it returns.
pub fn scope(work: impl FnOnce()) {
    OWNER.with(|_| ());
    let region = REGION.get();
    assert!(!region.active, "scopes do not nest");
    REGION.set(Region {
        active: true,
        ..region
    });
    work();
    let region = REGION.get();
    assert_eq!(region.live, 0, "an allocation outlived its scope");
    REGION.set(Region {
        top: region.base,
        last: 0,
        active: false,
        ..region
    });
}

/// Bump allocation inside [`scope`], mimalloc everywhere else.
pub struct ScopedBump;

// SAFETY: Region allocations are distinct, suitably aligned ranges inside
// the calling thread's reserved region, handed out only while a scope is
// active, and the region is reset only once every one of them was freed.
// Every other operation forwards its caller's valid arguments to mimalloc.
// A region pointer is never freed on another thread: the benchmarks scope
// only single-threaded planning.
unsafe impl GlobalAlloc for ScopedBump {
    // Each operation reads the thread local once: on macOS every access is a
    // call through the TLV getter, which would otherwise dominate a bump.
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let bumped = REGION.with(|cell| {
            let region = cell.get();
            let start = region.top.next_multiple_of(layout.align());
            let top = start + layout.size();
            (region.active && top <= region.end).then(|| {
                cell.set(Region {
                    top,
                    last: start,
                    live: region.live + 1,
                    ..region
                });
                start as *mut u8
            })
        });
        // SAFETY: The caller upholds `GlobalAlloc::alloc`'s contract.
        bumped.unwrap_or_else(|| unsafe { mimalloc::MiMalloc.alloc(layout) })
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        let freed = REGION.with(|cell| {
            let region = cell.get();
            let contained = region.contains(pointer);
            if contained {
                let (top, last) = match pointer as usize == region.last {
                    true => (region.last, 0),
                    false => (region.top, region.last),
                };
                cell.set(Region {
                    top,
                    last,
                    live: region.live - 1,
                    ..region
                });
            }
            contained
        });
        if !freed {
            // SAFETY: The pointer is not the region's, so mimalloc allocated it.
            unsafe { mimalloc::MiMalloc.dealloc(pointer, layout) }
        }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let (contained, active) = REGION.with(|cell| {
            let region = cell.get();
            let contained = region.contains(pointer);
            let grows_in_place =
                contained && pointer as usize == region.last && region.last + size <= region.end;
            if grows_in_place {
                cell.set(Region {
                    top: region.last + size,
                    ..region
                });
            }
            (contained.then_some(grows_in_place), region.active)
        });
        match (contained, active) {
            (Some(true), _) => return pointer,
            // SAFETY: The pointer is not the region's, so mimalloc
            // allocated it, and the caller upholds `realloc`'s contract.
            (None, false) => return unsafe { mimalloc::MiMalloc.realloc(pointer, layout, size) },
            (Some(false), _) | (None, true) => {}
        }
        // Move it: into the region while a scope is active, else to mimalloc.
        // SAFETY: `realloc`'s contract makes `size` a valid size for
        // `layout`'s alignment.
        let resized = unsafe { Layout::from_size_align_unchecked(size, layout.align()) };
        // SAFETY: `resized` has a non-zero size.
        let moved = unsafe { self.alloc(resized) };
        if !moved.is_null() {
            // SAFETY: Both blocks hold at least the copied length and are
            // distinct allocations; the old one is freed exactly once.
            unsafe {
                std::ptr::copy_nonoverlapping(pointer, moved, layout.size().min(size));
                self.dealloc(pointer, layout);
            }
        }
        moved
    }
}
