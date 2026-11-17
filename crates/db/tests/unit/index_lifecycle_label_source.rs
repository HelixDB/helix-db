//! Contracts for the label source builds read their rows through.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use slatedb::object_store::memory::InMemory;
use slatedb::{Db, DbTransaction, IsolationLevel};

use super::*;
use crate::encoding::property::Property;
use crate::encoding::v2::legacy::edge_property_pair::LegacyEdgePropertyPairKey;
use crate::index_lifecycle::OperationCounters;

async fn test_db(name: &str) -> Db {
    Db::builder(name, Arc::new(InMemory::new()))
        .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
        .build()
        .await
        .expect("label source test database opens")
}

fn row(label: &str, filler: usize) -> Vec<Property> {
    vec![
        Property::string("$label", label),
        Property::string("pad", "x".repeat(filler)),
    ]
}

fn limits(entities: usize, input_bytes: u64) -> SearchIndexBatchLimits {
    SearchIndexBatchLimits::try_new(
        NonZeroUsize::new(entities).expect("entity limit is positive"),
        NonZeroU64::new(input_bytes).expect("input limit is positive"),
        NonZeroU64::new(1 << 20).expect("operation limit is positive"),
        NonZeroU64::new(1 << 30).expect("output limit is positive"),
        NonZeroU64::new(1 << 30).expect("output limit is positive"),
    )
    .expect("test limits are consistent")
}

fn cursor(scope: DataScope, kind: IndexElementKind, id: u64) -> IndexCursor {
    IndexCursor::try_new(source_key(scope, kind, id)).expect("a source key is a cursor")
}

fn progress(
    scope: DataScope,
    kind: IndexElementKind,
    after: Option<u64>,
    upper: u64,
) -> SourceScanProgress {
    SourceScanProgress {
        inclusive_upper_bound: cursor(scope, kind, upper),
        cursor: after.map(|after| cursor(scope, kind, after)),
        counters: OperationCounters::default(),
    }
}

/// Ids of every row one step returns, and how the step's ids ended.
async fn step(
    snapshot: &DbTransaction,
    kind: IndexElementKind,
    label: &str,
    progress: &SourceScanProgress,
    limits: SearchIndexBatchLimits,
) -> Option<(Vec<u64>, Option<u64>)> {
    let scope = DataScope::LegacyUnscoped;
    let mut source = LabelSource::open(snapshot, scope, kind, label, progress, limits)
        .await
        .expect("the label source opens")?;
    let mut ids = Vec::new();
    loop {
        match source.next().await.expect("the label source reads") {
            LabelRead::Row(row) => {
                assert_eq!(row.key, source_key(scope, kind, row.entity_id.get()));
                ids.push(row.entity_id.get());
            }
            LabelRead::Exhausted => return Some((ids, None)),
            LabelRead::Paused(paused) => {
                let SourceKey::Id(paused) = source_id(scope, kind, paused.as_bytes())
                    .expect("a paused cursor is a source key")
                else {
                    panic!("a paused cursor is an id key");
                };
                return Some((ids, Some(paused)));
            }
        }
    }
}

async fn snapshot(db: &Db) -> DbTransaction {
    db.begin(IsolationLevel::Snapshot)
        .await
        .expect("snapshot opens")
}

#[tokio::test]
async fn a_label_without_members_or_ids_left_has_nothing_to_read() {
    let db = test_db("label-source-empty").await;
    let scope = DataScope::LegacyUnscoped;
    let node = IndexElementKind::Node;
    for id in 0..300 {
        testing::put_source_row(&db, scope, node, id, &row("Other", 0)).await;
    }
    testing::put_source_row(&db, scope, node, 5, &row("User", 0)).await;
    let read = snapshot(&db).await;
    let limits = limits(1024, 1 << 20);
    assert!(step(
        &read,
        node,
        "Absent",
        &progress(scope, node, None, 299),
        limits
    )
    .await
    .is_none());
    // Its only member is at or before the cursor, or above the bound.
    assert!(step(
        &read,
        node,
        "User",
        &progress(scope, node, Some(5), 299),
        limits
    )
    .await
    .is_none());
    assert!(
        step(&read, node, "User", &progress(scope, node, None, 4), limits)
            .await
            .is_none()
    );
    // A cursor at the bound is done without reading the bitmap.
    assert!(step(
        &read,
        node,
        "User",
        &progress(scope, node, Some(299), 299),
        limits
    )
    .await
    .is_none());
    assert!(matches!(
        LabelSource::open(&read, scope, node, "User", &progress(scope, node, Some(300), 299), limits).await,
        Err(HelixDbError::IndexCatalogCorruption(reason)) if reason.contains("exceeds")
    ));
}

#[tokio::test]
async fn members_are_read_in_id_order_skipping_absent_rows_and_ids_above_the_bound() {
    let db = test_db("label-source-members").await;
    let scope = DataScope::LegacyUnscoped;
    let node = IndexElementKind::Node;
    for id in [3, 9, 600, 601, 2000] {
        testing::put_source_row(&db, scope, node, id, &row("User", 0)).await;
    }
    testing::put_source_row(&db, scope, node, 4, &row("Other", 0)).await;
    let transaction = db
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("write begins");
    // A long run of members without rows, and one row of another label.
    (10..1500).for_each(|id| testing::stage_member(&transaction, scope, node, "User", id));
    testing::stage_member(&transaction, scope, node, "User", 4);
    transaction.commit().await.expect("members commit");
    let read = snapshot(&db).await;

    let all = step(
        &read,
        node,
        "User",
        &progress(scope, node, None, 1999),
        limits(4096, 1 << 20),
    )
    .await
    .expect("members remain");
    assert_eq!(all, (vec![3, 4, 9, 600, 601], None));

    // Absent ids count towards a step's entities and move its cursor.
    let mut after = None;
    let mut read_ids = Vec::new();
    let mut steps = 0;
    while let Some((ids, paused)) = step(
        &read,
        node,
        "User",
        &progress(scope, node, after, 1999),
        limits(100, 1 << 20),
    )
    .await
    {
        steps += 1;
        read_ids.extend(ids);
        let Some(paused) = paused else {
            break;
        };
        assert!(
            after.is_none_or(|after| paused > after),
            "every step advances"
        );
        after = Some(paused);
    }
    assert_eq!(read_ids, [3, 4, 9, 600, 601]);
    // 1,494 members up to the bound, 100 a step.
    assert_eq!(steps, 15);
}

#[tokio::test]
async fn no_chunk_is_read_past_the_input_byte_limit() {
    let db = test_db("label-source-bytes").await;
    let scope = DataScope::LegacyUnscoped;
    let node = IndexElementKind::Node;
    for id in 0..1000 {
        testing::put_source_row(&db, scope, node, id, &row("User", 100)).await;
    }
    let read = snapshot(&db).await;
    // The first chunk of 256 rows already passes 4 KiB: the step returns
    // that chunk and pauses after its last id.
    let (ids, paused) = step(
        &read,
        node,
        "User",
        &progress(scope, node, None, 999),
        limits(1024, 4096),
    )
    .await
    .expect("members remain");
    assert_eq!(ids, (0..256).collect::<Vec<_>>());
    assert_eq!(paused, Some(255));
    // A limit two chunks fit in reads both and the start of a third.
    let (ids, paused) = step(
        &read,
        node,
        "User",
        &progress(scope, node, Some(255), 999),
        limits(1024, 2 * 256 * 140),
    )
    .await
    .expect("members remain");
    assert_eq!(ids.first(), Some(&256));
    assert_eq!(paused, ids.last().copied());
    assert_eq!(ids.len() % CHUNK_IDS, 0);
}

#[tokio::test]
async fn edge_builds_resume_after_a_retired_pair_key_cursor() {
    let db = test_db("label-source-legacy-edge-cursor").await;
    let scope = DataScope::LegacyUnscoped;
    let edge = IndexElementKind::Edge;
    for id in 3..9 {
        testing::put_source_row(&db, scope, edge, id, &row("FOLLOWS", 0)).await;
    }
    let read = snapshot(&db).await;
    // An older build's prefix scan stopped on the retired row `[5][9]`,
    // which sorts after edge 5 and before edge 6.
    let legacy = IndexCursor::try_new(
        DataKey::Data {
            scope,
            kind: DataKeyKind::EdgePropertyPair(LegacyEdgePropertyPairKey::new(5, 9)),
        }
        .to_bytes(),
    )
    .expect("a legacy key is a cursor");
    let resumed = SourceScanProgress {
        cursor: Some(legacy.clone()),
        ..progress(scope, edge, None, 8)
    };
    assert_eq!(
        step(&read, edge, "FOLLOWS", &resumed, limits(1024, 1 << 20)).await,
        Some((vec![6, 7, 8], None))
    );
    // A node build has no pair keys, and no bound is one.
    let node = IndexElementKind::Node;
    assert!(LabelSource::open(
        &read,
        scope,
        node,
        "FOLLOWS",
        &SourceScanProgress {
            cursor: Some(legacy.clone()),
            ..progress(scope, node, None, 8)
        },
        limits(1024, 1 << 20)
    )
    .await
    .is_err());
    assert!(LabelSource::open(
        &read,
        scope,
        edge,
        "FOLLOWS",
        &SourceScanProgress {
            inclusive_upper_bound: legacy,
            cursor: None,
            counters: OperationCounters::default(),
        },
        limits(1024, 1 << 20)
    )
    .await
    .is_err());
}

#[tokio::test]
async fn a_corrupt_label_bitmap_fails_the_step() {
    let db = test_db("label-source-corrupt-bitmap").await;
    let scope = DataScope::LegacyUnscoped;
    let node = IndexElementKind::Node;
    testing::put_source_row(&db, scope, node, 1, &row("User", 0)).await;
    db.put(
        DataKey::Data {
            scope,
            kind: DataKeyKind::PropertyIndex(
                crate::encoding::v2::keys::indexes::PropertyIndexKey::Equality(
                    crate::encoding::indexes::equality::EqualityIndexKey::new(
                        crate::encoding::indexes::hash_property_name("$label"),
                        crate::encoding::indexes::hash_property_value("User"),
                    ),
                ),
            ),
        }
        .to_bytes(),
        b"not a bitmap".as_slice(),
    )
    .await
    .expect("corrupt bitmap writes");
    let read = snapshot(&db).await;
    assert!(LabelSource::open(
        &read,
        scope,
        node,
        "User",
        &progress(scope, node, None, 1),
        limits(1024, 1 << 20)
    )
    .await
    .is_err());
}
