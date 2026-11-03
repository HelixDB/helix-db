//! Fixed V3/V4 secondary-equality hot-path benchmark.

use db::bench_internals::{
    benchmark_million_sequential_id_bitmap, SecondaryEqualityHotPathFixture,
    SecondaryEqualityInsertMode, SecondaryEqualityReadMode,
};
use serde::Serialize;

mod support;

#[global_allocator]
static ALLOCATOR: support::TrackingAllocator = support::TrackingAllocator::new();

#[derive(Serialize)]
struct BenchmarkReport {
    sequential: db::bench_internals::SecondaryEqualityInsertSample,
    read_sequential: db::bench_internals::SecondaryEqualityReadSample,
    read_concurrent: db::bench_internals::SecondaryEqualityReadSample,
    concurrent: db::bench_internals::SecondaryEqualityInsertSample,
    million_sequential_id_bitmap: db::bench_internals::SecondaryEqualityMillionBitmapSample,
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(32)
        .enable_all()
        .build()
        .expect("secondary equality benchmark runtime starts");
    let report = runtime.block_on(async {
        let sequential_fixture =
            SecondaryEqualityHotPathFixture::open("secondary-equality-hot-path-sequential")
                .await
                .expect("sequential hot-path fixture opens");
        let sequential = sequential_fixture
            .insert(SecondaryEqualityInsertMode::Sequential)
            .await
            .expect("sequential hot-path insertions succeed");
        sequential_fixture
            .prepare_read()
            .await
            .expect("sequential hot-path lookup warmup succeeds");
        let read_sequential = sequential_fixture
            .read(SecondaryEqualityReadMode::Sequential)
            .await
            .expect("sequential hot-path lookups succeed");
        let read_concurrent = sequential_fixture
            .read(SecondaryEqualityReadMode::Concurrent)
            .await
            .expect("concurrent hot-path lookups succeed");
        sequential_fixture
            .close()
            .await
            .expect("sequential hot-path fixture closes");

        let concurrent_fixture =
            SecondaryEqualityHotPathFixture::open("secondary-equality-hot-path-concurrent")
                .await
                .expect("concurrent hot-path fixture opens");
        let concurrent = concurrent_fixture
            .insert(SecondaryEqualityInsertMode::Concurrent)
            .await
            .expect("concurrent hot-path insertions succeed");
        concurrent_fixture
            .close()
            .await
            .expect("concurrent hot-path fixture closes");

        let allocation_fixture =
            SecondaryEqualityHotPathFixture::open("secondary-equality-hot-path-allocations")
                .await
                .expect("allocation hot-path fixture opens");
        ALLOCATOR.start();
        allocation_fixture
            .insert(SecondaryEqualityInsertMode::Sequential)
            .await
            .expect("allocation sequential insertions succeed");
        let (sequential_allocations, sequential_allocated_bytes) = ALLOCATOR.finish();
        allocation_fixture
            .prepare_read()
            .await
            .expect("allocation lookup warmup succeeds");
        ALLOCATOR.start();
        allocation_fixture
            .read(SecondaryEqualityReadMode::Sequential)
            .await
            .expect("allocation sequential lookups succeed");
        let (sequential_read_allocations, sequential_read_allocated_bytes) = ALLOCATOR.finish();
        ALLOCATOR.start();
        allocation_fixture
            .read(SecondaryEqualityReadMode::Concurrent)
            .await
            .expect("allocation concurrent lookups succeed");
        let (concurrent_read_allocations, concurrent_read_allocated_bytes) = ALLOCATOR.finish();
        allocation_fixture
            .close()
            .await
            .expect("allocation hot-path fixture closes");

        let concurrent_allocation_fixture = SecondaryEqualityHotPathFixture::open(
            "secondary-equality-hot-path-concurrent-allocations",
        )
        .await
        .expect("concurrent allocation hot-path fixture opens");
        ALLOCATOR.start();
        concurrent_allocation_fixture
            .insert(SecondaryEqualityInsertMode::Concurrent)
            .await
            .expect("allocation concurrent insertions succeed");
        let (concurrent_allocations, concurrent_allocated_bytes) = ALLOCATOR.finish();
        concurrent_allocation_fixture
            .close()
            .await
            .expect("concurrent allocation hot-path fixture closes");

        let sequential =
            sequential.with_allocations(sequential_allocations, sequential_allocated_bytes);
        let read_sequential = read_sequential
            .with_allocations(sequential_read_allocations, sequential_read_allocated_bytes);
        let read_concurrent = read_concurrent
            .with_allocations(concurrent_read_allocations, concurrent_read_allocated_bytes);
        let concurrent =
            concurrent.with_allocations(concurrent_allocations, concurrent_allocated_bytes);

        BenchmarkReport {
            sequential,
            read_sequential,
            read_concurrent,
            concurrent,
            million_sequential_id_bitmap: benchmark_million_sequential_id_bitmap(),
        }
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("benchmark report serializes")
    );
}
