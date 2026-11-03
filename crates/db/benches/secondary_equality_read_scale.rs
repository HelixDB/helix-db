//! Secondary-equality read benchmark over ten thousand shared-value nodes.

use std::num::NonZeroUsize;

use db::bench_internals::{
    SecondaryEqualityHotPathFixture, SecondaryEqualityInsertMode, SecondaryEqualityReadMode,
};
use serde::Serialize;

mod support;

#[global_allocator]
static ALLOCATOR: support::TrackingAllocator = support::TrackingAllocator::new();

const READ_OPERATIONS: NonZeroUsize = NonZeroUsize::new(100).expect("read count is positive");

#[derive(Serialize)]
struct BenchmarkReport {
    population: db::bench_internals::SecondaryEqualityInsertSample,
    inspection: db::bench_internals::SecondaryEqualityInspection,
    read_sequential: db::bench_internals::SecondaryEqualityReadSample,
    read_concurrent: db::bench_internals::SecondaryEqualityReadSample,
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(32)
        .enable_all()
        .build()
        .expect("secondary equality read benchmark runtime starts");
    let report = runtime.block_on(async {
        let fixture =
            SecondaryEqualityHotPathFixture::open_read_scale("secondary-equality-read-scale")
                .await
                .expect("read-scale fixture opens");
        let population = fixture
            .insert(SecondaryEqualityInsertMode::Concurrent)
            .await
            .expect("read-scale population succeeds");
        let inspection = fixture
            .inspect()
            .await
            .expect("read-scale inspection succeeds");
        assert_eq!(inspection.physical_secondary_rows, 50);
        assert_eq!(inspection.v3_nonunique_rows, 0);
        assert_eq!(inspection.v4_bitmap_rows, 50);
        assert_eq!(inspection.minimum_bitmap_cardinality, 10_000);
        assert_eq!(inspection.maximum_bitmap_cardinality, 10_000);
        fixture
            .prepare_read()
            .await
            .expect("read-scale lookup warmup succeeds");

        let read_sequential = fixture
            .read_operations(SecondaryEqualityReadMode::Sequential, READ_OPERATIONS)
            .await
            .expect("sequential read-scale lookups succeed");
        let read_concurrent = fixture
            .read_operations(SecondaryEqualityReadMode::Concurrent, READ_OPERATIONS)
            .await
            .expect("concurrent read-scale lookups succeed");

        ALLOCATOR.start();
        fixture
            .read_operations(SecondaryEqualityReadMode::Sequential, READ_OPERATIONS)
            .await
            .expect("allocation-only sequential lookups succeed");
        let (sequential_allocations, sequential_allocated_bytes) = ALLOCATOR.finish();
        ALLOCATOR.start();
        fixture
            .read_operations(SecondaryEqualityReadMode::Concurrent, READ_OPERATIONS)
            .await
            .expect("allocation-only concurrent lookups succeed");
        let (concurrent_allocations, concurrent_allocated_bytes) = ALLOCATOR.finish();
        fixture.close().await.expect("read-scale fixture closes");

        BenchmarkReport {
            population,
            inspection,
            read_sequential: read_sequential
                .with_allocations(sequential_allocations, sequential_allocated_bytes),
            read_concurrent: read_concurrent
                .with_allocations(concurrent_allocations, concurrent_allocated_bytes),
        }
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("benchmark report serializes")
    );
}
