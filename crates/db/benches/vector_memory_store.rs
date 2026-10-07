//! Resident vector-memory store hydration and lookup benchmark.
//!
//! Measures, for an HNSW-shaped vector-memory prefix read from SSTs on disk:
//! heap bytes the hydrated store keeps alive against the bytes it charged,
//! hydration time and allocations, and warm lookup latency on one thread and
//! under contention.
//!
//! Run with:
//! `cargo bench -p db --features production-coverage --bench vector_memory_store`
//!
//! Tunables: `HELIX_VM_BENCH_NODES` (100000), `HELIX_VM_BENCH_DIMENSIONS`
//! (128), `HELIX_VM_BENCH_M` (16), `HELIX_VM_BENCH_ROUNDS` (5),
//! `HELIX_VM_BENCH_THREADS` (available parallelism), `HELIX_VM_BENCH_DIR`.

mod benchmark {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
    use std::time::Instant;

    use db::production_coverage::{
        VectorMemoryBenchmarkFixture, VectorMemoryBenchmarkShape, VectorMemoryBenchmarkStore,
    };
    use serde::Serialize;

    struct CountingAllocator;

    static TRACK: AtomicBool = AtomicBool::new(false);
    static LIVE_BYTES: AtomicI64 = AtomicI64::new(0);
    static PEAK_LIVE_BYTES: AtomicI64 = AtomicI64::new(0);
    static ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
    static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

    fn grow(bytes: usize) {
        if TRACK.load(Ordering::Relaxed) {
            let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
            ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(bytes.unsigned_abs(), Ordering::Relaxed);
            let live = LIVE_BYTES.fetch_add(bytes, Ordering::Relaxed) + bytes;
            PEAK_LIVE_BYTES.fetch_max(live, Ordering::Relaxed);
        }
    }

    fn shrink(bytes: usize) {
        if TRACK.load(Ordering::Relaxed) {
            LIVE_BYTES.fetch_sub(i64::try_from(bytes).unwrap_or(i64::MAX), Ordering::Relaxed);
        }
    }

    // SAFETY: every operation is forwarded unchanged to the system allocator.
    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            grow(layout.size());
            // SAFETY: the caller supplied `layout` under `GlobalAlloc::alloc`.
            unsafe { System.alloc(layout) }
        }

        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            shrink(layout.size());
            // SAFETY: the caller supplied the allocation and layout pair.
            unsafe { System.dealloc(pointer, layout) }
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            grow(layout.size());
            // SAFETY: the caller supplied `layout` under `GlobalAlloc::alloc_zeroed`.
            unsafe { System.alloc_zeroed(layout) }
        }

        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            shrink(layout.size());
            grow(new_size);
            // SAFETY: the caller supplied the allocation, layout, and new size.
            unsafe { System.realloc(pointer, layout, new_size) }
        }
    }

    #[global_allocator]
    static ALLOCATOR: CountingAllocator = CountingAllocator;

    #[derive(Serialize)]
    struct Round {
        record: &'static str,
        round: usize,
        nodes: u64,
        dimensions: usize,
        max_neighbors: usize,
        upper_nodes: usize,
        upper_rows: usize,
        loaded_entries: usize,
        charged_bytes: u64,
        retained_heap_bytes: i64,
        retained_per_charged: f64,
        hydration_peak_heap_bytes: i64,
        hydration_ns: u128,
        hydration_allocations: u64,
        hydration_allocated_bytes: u64,
        lookup_single_ns_per_node: f64,
        lookup_parallel_ns_per_node: f64,
        lookup_threads: usize,
    }

    fn env_usize(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .map(|value| value.parse().expect("benchmark variable is an integer"))
            .unwrap_or(default)
    }

    /// Nanoseconds per node lookup over `passes` sweeps on one thread.
    fn single_thread_lookup(
        store: &VectorMemoryBenchmarkStore,
        nodes: &[u64],
        passes: usize,
    ) -> f64 {
        let started = Instant::now();
        let bytes: usize = (0..passes)
            .map(|_| std::hint::black_box(store.lookup(nodes)))
            .sum();
        std::hint::black_box(bytes);
        started.elapsed().as_nanos() as f64 / (passes * nodes.len()) as f64
    }

    /// Wall nanoseconds per node lookup with `threads` threads sweeping
    /// concurrently, each from a different starting offset.
    fn parallel_lookup(
        store: &VectorMemoryBenchmarkStore,
        nodes: &[u64],
        passes: usize,
        threads: usize,
    ) -> f64 {
        let started = Instant::now();
        std::thread::scope(|scope| {
            for thread in 0..threads {
                let rotated: Vec<u64> = nodes
                    .iter()
                    .cycle()
                    .skip(thread * nodes.len() / threads)
                    .take(nodes.len())
                    .copied()
                    .collect();
                scope.spawn(move || {
                    for _ in 0..passes {
                        std::hint::black_box(store.lookup(&rotated));
                    }
                });
            }
        });
        started.elapsed().as_nanos() as f64 / (passes * nodes.len() * threads) as f64
    }

    pub fn main() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("vector memory benchmark runtime starts");
        runtime.block_on(run());
    }

    async fn run() {
        let shape = VectorMemoryBenchmarkShape {
            nodes: u64::try_from(env_usize("HELIX_VM_BENCH_NODES", 100_000)).unwrap(),
            dimensions: env_usize("HELIX_VM_BENCH_DIMENSIONS", 128),
            max_neighbors: env_usize("HELIX_VM_BENCH_M", 16),
        };
        let rounds = env_usize("HELIX_VM_BENCH_ROUNDS", 5);
        let threads = env_usize(
            "HELIX_VM_BENCH_THREADS",
            std::thread::available_parallelism().map_or(1, usize::from),
        );
        let root = std::env::var("HELIX_VM_BENCH_DIR")
            .map_or_else(|_| std::env::temp_dir(), std::path::PathBuf::from);
        let directory = tempfile::tempdir_in(root).expect("benchmark directory is created");
        let fixture = VectorMemoryBenchmarkFixture::build(directory.path(), shape)
            .await
            .expect("benchmark fixture builds");
        let nodes = fixture.upper_nodes().to_vec();
        // One warmup hydration pulls the SSTs into the OS page cache.
        drop(fixture.hydrate().await.expect("warmup hydration succeeds"));

        for round in 0..rounds {
            ALLOCATION_CALLS.store(0, Ordering::Relaxed);
            ALLOCATED_BYTES.store(0, Ordering::Relaxed);
            LIVE_BYTES.store(0, Ordering::Relaxed);
            PEAK_LIVE_BYTES.store(0, Ordering::Relaxed);
            TRACK.store(true, Ordering::SeqCst);
            let store = fixture
                .hydrate()
                .await
                .expect("measured hydration succeeds");
            let hydration_allocations = ALLOCATION_CALLS.load(Ordering::Relaxed);
            let hydration_allocated_bytes = ALLOCATED_BYTES.load(Ordering::Relaxed);
            let hydration_peak = PEAK_LIVE_BYTES.load(Ordering::Relaxed);
            TRACK.store(false, Ordering::SeqCst);

            let passes = (2_000_000 / nodes.len().max(1)).max(1);
            let lookup_single = single_thread_lookup(&store, &nodes, passes);
            let lookup_parallel = parallel_lookup(&store, &nodes, passes, threads);

            let charged_bytes = store.charged_bytes;
            let loaded_entries = store.loaded_entries;
            let hydration_ns = store.elapsed.as_nanos();
            TRACK.store(true, Ordering::SeqCst);
            let with_store = LIVE_BYTES.load(Ordering::Relaxed);
            drop(store);
            let retained = with_store - LIVE_BYTES.load(Ordering::Relaxed);
            TRACK.store(false, Ordering::SeqCst);

            let record = Round {
                record: "round",
                round,
                nodes: shape.nodes,
                dimensions: shape.dimensions,
                max_neighbors: shape.max_neighbors,
                upper_nodes: nodes.len(),
                upper_rows: fixture.upper_rows(),
                loaded_entries,
                charged_bytes,
                retained_heap_bytes: retained,
                retained_per_charged: retained as f64 / charged_bytes.max(1) as f64,
                hydration_peak_heap_bytes: hydration_peak,
                hydration_ns,
                hydration_allocations,
                hydration_allocated_bytes,
                lookup_single_ns_per_node: lookup_single,
                lookup_parallel_ns_per_node: lookup_parallel,
                lookup_threads: threads,
            };
            println!(
                "{}",
                serde_json::to_string(&record).expect("benchmark record serializes")
            );
        }
        fixture.close().await.expect("benchmark fixture closes");
    }
}

fn main() {
    benchmark::main();
}
