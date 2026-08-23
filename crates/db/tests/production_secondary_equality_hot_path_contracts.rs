//! Isolated production contract for process-global secondary-equality I/O metrics.

/// Proves bounded sequential and concurrent V4 writes produce identical
/// bitmaps and that every configured equality lookup is one point read.
#[tokio::test(flavor = "multi_thread", worker_threads = 32)]
async fn secondary_equality_v4_bitmap_shape_and_read_io_are_exact() {
    use db::production_coverage::{SecondaryEqualityHotPathFixture, SecondaryEqualityInsertMode};

    let sequential = SecondaryEqualityHotPathFixture::open_correctness(
        "secondary-equality-v4-correctness-sequential",
    )
    .await
    .expect("sequential V4 correctness fixture opens");
    sequential
        .insert(SecondaryEqualityInsertMode::Sequential)
        .await
        .expect("sequential V4 correctness inserts succeed");
    let sequential_inspection = sequential
        .inspect()
        .await
        .expect("sequential V4 correctness inspection succeeds");
    assert_eq!(sequential_inspection.physical_secondary_rows, 50);
    assert_eq!(sequential_inspection.v3_nonunique_rows, 0);
    assert_eq!(sequential_inspection.v4_bitmap_rows, 50);
    assert_eq!(sequential_inspection.minimum_bitmap_cardinality, 100);
    assert_eq!(sequential_inspection.maximum_bitmap_cardinality, 100);
    let lookup = sequential
        .inspect_all_lookups()
        .await
        .expect("all sequential V4 equality lookups succeed");
    assert_eq!(lookup.lookups, 50);
    assert_eq!(lookup.result_count, 100);
    assert_eq!(lookup.point_reads, 50);
    assert_eq!(lookup.scans, 0);
    assert_eq!(lookup.graph_reads, 0);
    let sequential_rows = sequential
        .decoded_bitmap_rows()
        .await
        .expect("sequential V4 bitmap rows decode");

    let concurrent = SecondaryEqualityHotPathFixture::open_correctness(
        "secondary-equality-v4-correctness-concurrent",
    )
    .await
    .expect("concurrent V4 correctness fixture opens");
    concurrent
        .insert(SecondaryEqualityInsertMode::Concurrent)
        .await
        .expect("concurrent V4 correctness inserts succeed");
    let concurrent_inspection = concurrent
        .inspect()
        .await
        .expect("concurrent V4 correctness inspection succeeds");
    assert_eq!(concurrent_inspection, sequential_inspection);
    let concurrent_rows = concurrent
        .decoded_bitmap_rows()
        .await
        .expect("concurrent V4 bitmap rows decode");
    let assert_one_entity_set_per_index = |rows: &[(Vec<u8>, Vec<u64>)]| {
        let Some((_, expected_ids)) = rows.first() else {
            panic!("V4 correctness fixture must contain bitmap rows");
        };
        assert_eq!(expected_ids.len(), 100);
        assert!(rows.iter().all(|(_, ids)| ids == expected_ids));
    };
    assert_one_entity_set_per_index(&sequential_rows);
    assert_one_entity_set_per_index(&concurrent_rows);
    assert_eq!(
        concurrent_rows
            .iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>(),
        sequential_rows
            .iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>()
    );

    sequential
        .close()
        .await
        .expect("sequential V4 correctness fixture closes");
    concurrent
        .close()
        .await
        .expect("concurrent V4 correctness fixture closes");
}
