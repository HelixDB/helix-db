//! Contracts for the label source builds read their rows through.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use slatedb::object_store::memory::InMemory;
use slatedb::{Db, DbTransaction, IsolationLevel};

use super::*;
use crate::encoding::property::Property;
use crate::encoding::v2::legacy::edge_property_pair::LegacyEdgePropertyPairKey;
use crate::index_lifecycle::OperationCounters;
use crate::migrations::LabelStage;
use roaring::RoaringTreemap;

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
    let mut source = LabelSource::open(
        snapshot,
        scope,
        kind,
        label,
        progress,
        limits,
        IndexLifecycleScanTuning::default(),
    )
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
        LabelSource::open(&read, scope, node, "User", &progress(scope, node, Some(300), 299), limits, IndexLifecycleScanTuning::default()).await,
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
    for id in 10..1500 {
        testing::stage_member(&transaction, scope, node, "User", id).await;
    }
    testing::stage_member(&transaction, scope, node, "User", 4).await;
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

/// What one step read, as [`admit`] drives it.
struct Admitted {
    ids: Vec<u64>,
    /// Key and value bytes of the rows admitted.
    bytes: u64,
    /// The largest row read.
    largest: u64,
    /// Key and value bytes the source read.
    read: u64,
}

/// Reads one step as a driver does: rows are admitted until the next would
/// pass the input-byte limit, which ends the step.
async fn admit(
    snapshot: &DbTransaction,
    progress: &SourceScanProgress,
    limits: SearchIndexBatchLimits,
) -> Admitted {
    let scope = DataScope::LegacyUnscoped;
    let mut source = LabelSource::open(
        snapshot,
        scope,
        IndexElementKind::Node,
        "User",
        progress,
        limits,
        IndexLifecycleScanTuning::default(),
    )
    .await
    .expect("the label source opens")
    .expect("members remain");
    let (mut admitted, mut admitted_bytes, mut largest) = (Vec::new(), 0_u64, 0_u64);
    while let LabelRead::Row(row) = source.next().await.expect("the label source reads") {
        let bytes = u64::try_from(row.key.len() + row.value.len()).expect("row size fits");
        largest = largest.max(bytes);
        if admitted_bytes + bytes > limits.max_input_bytes().get() {
            break;
        }
        admitted_bytes += bytes;
        admitted.push(row.entity_id.get());
    }
    Admitted {
        ids: admitted,
        bytes: admitted_bytes,
        largest,
        read: source.read.bytes,
    }
}

/// A step reads about its input-byte limit, whether it scans its range
/// (every id a member) or reads by point (every third).
#[tokio::test]
async fn a_step_reads_about_its_input_byte_limit() {
    let scope = DataScope::LegacyUnscoped;
    let node = IndexElementKind::Node;
    for stride in [1_u64, 3] {
        let db = test_db(&format!("label-source-bytes-{stride}")).await;
        for member in 0..1000 {
            testing::put_source_row(&db, scope, node, member * stride, &row("User", 100)).await;
        }
        let last = 999 * stride;
        let read = snapshot(&db).await;
        // Small rows: by point, the first chunk measures them and the next
        // fills the limit.
        let step_read = admit(
            &read,
            &progress(scope, node, None, last),
            limits(1024, 4096),
        )
        .await;
        assert_eq!(
            step_read.ids,
            (0..step_read.ids.len() as u64)
                .map(|member| member * stride)
                .collect::<Vec<_>>()
        );
        assert!(step_read.bytes > 4096 - step_read.largest);
        assert!(
            step_read.read <= step_read.bytes + 2 * step_read.largest,
            "{stride}: {} read for {} admitted",
            step_read.read,
            step_read.bytes
        );
        // A step that stops at its byte limit pauses after its last id read.
        let (ids, paused) = step(
            &read,
            node,
            "User",
            &progress(scope, node, None, last),
            limits(1024, 4096),
        )
        .await
        .expect("members remain");
        assert_eq!(paused, ids.last().copied());
        // Under a large limit, chunks grow to the most one read takes, and
        // the step takes every member.
        assert_eq!(
            step(
                &read,
                node,
                "User",
                &progress(scope, node, None, last),
                limits(1024, 1 << 30),
            )
            .await,
            Some(((0..1000).map(|member| member * stride).collect(), None))
        );
    }
}

/// A label of rows near the step's limit reads about the limit per step,
/// not a whole chunk of rows the next step reads again, whether its steps
/// scan their range (every id a member) or read by point (every third).
#[tokio::test]
async fn large_rows_read_about_one_limit_per_step() {
    const LIMIT: u64 = 8 << 20;
    let scope = DataScope::LegacyUnscoped;
    let node = IndexElementKind::Node;
    let mut random = 0x2545_F491_4F6C_DD1D_u64;
    // `None` is rows of 256 KiB to 1 MiB.
    for (name, size) in [
        ("label-source-256k", Some(256 << 10)),
        ("label-source-1m", Some(1 << 20)),
        ("label-source-mixed", None),
    ] {
        for stride in [1_u64, 3] {
            let name = format!("{name}-{stride}");
            let db = test_db(&name).await;
            for member in 0..48 {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                let filler = size.unwrap_or_else(|| {
                    (256 << 10) + usize::try_from(random % (768 << 10)).expect("fits")
                });
                testing::put_source_row(&db, scope, node, member * stride, &row("User", filler))
                    .await;
            }
            let last = 47 * stride;
            let read = snapshot(&db).await;
            assert_eq!(
                scans_range(
                    &read,
                    node,
                    &progress(scope, node, None, last),
                    limits(1024, LIMIT)
                )
                .await,
                stride == 1,
                "{name}"
            );
            let mut after = None;
            let mut steps = 0;
            while after != Some(last) {
                let step_read = admit(
                    &read,
                    &progress(scope, node, after, last),
                    limits(1024, LIMIT),
                )
                .await;
                after = step_read.ids.last().copied();
                steps += 1;
                // A step fills its limit to within a row, unless the label ends.
                assert!(
                    after == Some(last) || step_read.bytes > LIMIT - step_read.largest,
                    "{name}: {} admitted",
                    step_read.bytes
                );
                // Rows of one size are read to within two rows of what is
                // admitted; rows of mixed sizes to within twice the limit,
                // where a chunk of 256 such rows read 160 MiB.
                let bound = match size {
                    Some(_) => step_read.bytes + 2 * step_read.largest,
                    None => 2 * LIMIT,
                };
                assert!(
                    step_read.read <= bound,
                    "{name}: {} read for {} admitted",
                    step_read.read,
                    step_read.bytes
                );
            }
            assert!(steps >= 2, "{name}");
            db.close().await.expect("database closes");
        }
    }
}

/// A step whose first rows are small and the rest large reads about its
/// limit, scanning or by point: the mean of the small rows never sizes a
/// chunk of large ones larger than what the step has read already.
#[tokio::test]
async fn small_rows_then_large_ones_read_about_one_limit() {
    const LIMIT: u64 = 8 << 20;
    let scope = DataScope::LegacyUnscoped;
    let node = IndexElementKind::Node;
    for stride in [1_u64, 3] {
        let db = test_db(&format!("label-source-small-then-large-{stride}")).await;
        for member in 0..68 {
            let filler = if member < 4 { 100 } else { 512 << 10 };
            testing::put_source_row(&db, scope, node, member * stride, &row("User", filler)).await;
        }
        let read = snapshot(&db).await;
        let step_read = admit(
            &read,
            &progress(scope, node, None, 67 * stride),
            limits(1024, LIMIT),
        )
        .await;
        assert_eq!(
            step_read.ids,
            (0..19).map(|member| member * stride).collect::<Vec<_>>()
        );
        // A chunk sized by the four small rows alone read all 64 large ones:
        // 32 MiB.
        assert!(
            step_read.read <= LIMIT + 4 * step_read.largest,
            "{stride}: {} read for {} admitted",
            step_read.read,
            step_read.bytes
        );
        db.close().await.expect("database closes");
    }
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
        limits(1024, 1 << 20),
        IndexLifecycleScanTuning::default(),
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
        limits(1024, 1 << 20),
        IndexLifecycleScanTuning::default(),
    )
    .await
    .is_err());
}

/// The key of `label`'s node label row in `layout`.
fn node_label_row(scope: DataScope, layout: LabelRowLayout, label: &str) -> Bytes {
    match layout {
        LabelRowLayout::Hash => {
            crate::search::hash_label_key(scope, crate::search::HashLabelRow::Node(label))
        }
        LabelRowLayout::Exact => DataKey::Data {
            scope,
            kind: DataKeyKind::PropertyIndex(
                crate::encoding::v2::keys::indexes::PropertyIndexKey::NodeLabel(
                    crate::encoding::indexes::label::NodeLabelKey::new(
                        crate::encoding::indexes::label::LabelIdentity::new(label)
                            .expect("the label fits a key"),
                    ),
                ),
            ),
        }
        .to_bytes(),
    }
}

/// The layout a snapshot of a store at `stage` reads label rows in.
fn layout_at(stage: LabelStage) -> LabelRowLayout {
    match stage {
        LabelStage::BeforeCopy | LabelStage::Copying => LabelRowLayout::Hash,
        LabelStage::Published => LabelRowLayout::Exact,
    }
}

/// At every stage of the label identity migration, a step reads its label's
/// members in the layout its own snapshot's marker names: the hash rows
/// before the copy and while it runs, when the exact rows are incomplete,
/// and the exact rows once it has published and cleaned up. Rows written
/// before the stage and after it, a member relabelled after it and one
/// deleted after it all read as their rows say.
#[tokio::test]
async fn every_label_identity_stage_reads_the_layout_its_snapshot_names() {
    let scope = DataScope::LegacyUnscoped;
    let label_of = |id: u64| {
        if id.is_multiple_of(2) {
            "User"
        } else {
            "Other"
        }
    };
    for kind in [IndexElementKind::Node, IndexElementKind::Edge] {
        for stage in LabelStage::ALL {
            let db = test_db(&format!("label-source-stage-{kind:?}-{stage:?}")).await;
            LabelStage::rewind(&db).await;
            for id in 1..=6 {
                testing::put_source_row(&db, scope, kind, id, &row(label_of(id), 0)).await;
            }
            stage.reach(&db).await;
            for id in 7..=10 {
                testing::put_source_row(&db, scope, kind, id, &row(label_of(id), 0)).await;
            }
            let transaction = db
                .begin(IsolationLevel::Snapshot)
                .await
                .expect("write begins");
            testing::stage_labels(
                &transaction,
                scope,
                kind,
                2,
                &row("User", 0),
                &row("Other", 0),
            )
            .await;
            transaction
                .put(
                    source_key(scope, kind, 2),
                    crate::encoding::v2::values::property::encode_properties(&row("Other", 0)),
                )
                .expect("relabel stages");
            testing::stage_labels(&transaction, scope, kind, 8, &row("User", 0), &[]).await;
            transaction
                .delete(source_key(scope, kind, 8))
                .expect("delete stages");
            transaction.commit().await.expect("writes commit");

            let read = snapshot(&db).await;
            let layout = label_layout(&read).await.expect("the marker reads");
            assert_eq!(layout, layout_at(stage), "{kind:?} {stage:?}");
            assert_eq!(
                step(
                    &read,
                    kind,
                    "User",
                    &progress(scope, kind, None, 10),
                    limits(4096, 1 << 20)
                )
                .await,
                Some((vec![4, 6, 10], None)),
                "{kind:?} {stage:?}"
            );
            // While the copy runs its exact rows lack members: the copy took
            // one of the three Users written before it.
            let exact = label_members(&read, LabelRowLayout::Exact, kind, "User", scope)
                .await
                .expect("exact rows read");
            assert_eq!(
                exact == RoaringTreemap::from_iter([4, 6, 10]),
                stage == LabelStage::Published,
                "{kind:?} {stage:?}: {exact:?}"
            );
        }
    }
}

/// A corrupt bitmap of the layout a step reads fails the step, at every
/// stage of the label identity migration.
#[tokio::test]
async fn a_corrupt_label_bitmap_fails_the_step() {
    let scope = DataScope::LegacyUnscoped;
    let node = IndexElementKind::Node;
    for stage in LabelStage::ALL {
        let db = test_db(&format!("label-source-corrupt-bitmap-{stage:?}")).await;
        LabelStage::rewind(&db).await;
        testing::put_source_row(&db, scope, node, 1, &row("User", 0)).await;
        stage.reach(&db).await;
        db.put(
            node_label_row(scope, layout_at(stage), "User"),
            b"not a bitmap".as_slice(),
        )
        .await
        .expect("corrupt bitmap writes");
        let read = snapshot(&db).await;
        assert!(
            LabelSource::open(
                &read,
                scope,
                node,
                "User",
                &progress(scope, node, None, 1),
                limits(1024, 1 << 20),
                IndexLifecycleScanTuning::default(),
            )
            .await
            .is_err(),
            "{stage:?}"
        );
    }
}

/// Whether a source opened on `progress` scans its ids' key range rather
/// than reading them by point.
async fn scans_range(
    snapshot: &DbTransaction,
    kind: IndexElementKind,
    progress: &SourceScanProgress,
    limits: SearchIndexBatchLimits,
) -> bool {
    let source = LabelSource::open(
        snapshot,
        DataScope::LegacyUnscoped,
        kind,
        "User",
        progress,
        limits,
        IndexLifecycleScanTuning::default(),
    )
    .await
    .expect("the label source opens")
    .expect("members remain");
    matches!(source.reads, Reads::Range(_))
}

/// A step whose ids fill at least half their key range scans it, passing the
/// rows of other labels and the ids without rows; a sparser one reads its
/// ids by point. Steps of both kinds return every member's row once, in id
/// order, and each pauses after the last id it read or passed.
#[tokio::test]
async fn dense_steps_scan_their_range_and_sparse_steps_read_points() {
    let db = test_db("label-source-range").await;
    let scope = DataScope::LegacyUnscoped;
    let node = IndexElementKind::Node;
    // Two of every three ids below 600 are members, every seventh of them
    // without a row; above it, one id in ten.
    let member = |id: u64| {
        if id < 600 {
            id % 3 != 2
        } else {
            id.is_multiple_of(10)
        }
    };
    let mut expected = Vec::new();
    for id in 0..1200 {
        match (member(id), id.is_multiple_of(7)) {
            (false, _) => testing::put_source_row(&db, scope, node, id, &row("Other", 0)).await,
            (true, true) => {
                let transaction = db
                    .begin(IsolationLevel::Snapshot)
                    .await
                    .expect("write begins");
                testing::stage_member(&transaction, scope, node, "User", id).await;
                transaction.commit().await.expect("member commits");
            }
            (true, false) => {
                testing::put_source_row(&db, scope, node, id, &row("User", 0)).await;
                expected.push(id);
            }
        }
    }
    let read = snapshot(&db).await;
    let limits = limits(50, 1 << 20);
    assert!(scans_range(&read, node, &progress(scope, node, None, 1199), limits).await);
    assert!(!scans_range(&read, node, &progress(scope, node, Some(599), 1199), limits).await);
    let (mut after, mut read_ids) = (None, Vec::new());
    while let Some((ids, paused)) = step(
        &read,
        node,
        "User",
        &progress(scope, node, after, 1199),
        limits,
    )
    .await
    {
        assert!(
            ids.last()
                .is_none_or(|last| paused.is_none_or(|paused| *last <= paused)),
            "a step pauses at or after its last row"
        );
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
    assert_eq!(read_ids, expected);
}

/// A row of another label that takes a scanning step past its byte limit
/// ends the step, and the next step's range starts after it, so it is never
/// read again. Retired `[from][to]` edge rows in an edge range are passed.
#[tokio::test]
async fn rows_of_other_labels_past_the_limit_are_read_once() {
    let db = test_db("label-source-range-large-other").await;
    let scope = DataScope::LegacyUnscoped;
    let node = IndexElementKind::Node;
    for id in 0..10 {
        let row = match id {
            5 => row("Other", 64 << 10),
            _ => row("User", 0),
        };
        testing::put_source_row(&db, scope, node, id, &row).await;
    }
    let read = snapshot(&db).await;
    let limits = limits(1024, 4096);
    assert!(scans_range(&read, node, &progress(scope, node, None, 9), limits).await);
    assert_eq!(
        step(&read, node, "User", &progress(scope, node, None, 9), limits).await,
        Some((vec![0, 1, 2, 3, 4], Some(4)))
    );
    let mut source = LabelSource::open(
        &read,
        scope,
        node,
        "User",
        &progress(scope, node, Some(4), 9),
        limits,
        IndexLifecycleScanTuning::default(),
    )
    .await
    .expect("the label source opens")
    .expect("members remain");
    let mut ids = Vec::new();
    while let LabelRead::Row(row) = source.next().await.expect("the label source reads") {
        ids.push(row.entity_id.get());
    }
    assert_eq!(ids, [6, 7, 8, 9]);
    assert!(source.read.bytes < 4096, "{} read", source.read.bytes);

    let edge = IndexElementKind::Edge;
    for id in 3..9 {
        testing::put_source_row(&db, scope, edge, id, &row("FOLLOWS", 0)).await;
    }
    db.put(
        DataKey::Data {
            scope,
            kind: DataKeyKind::EdgePropertyPair(LegacyEdgePropertyPairKey::new(5, 9)),
        }
        .to_bytes(),
        crate::encoding::v2::values::property::encode_properties(&row("FOLLOWS", 0)),
    )
    .await
    .expect("retired pair row writes");
    let read = snapshot(&db).await;
    let mut source = LabelSource::open(
        &read,
        scope,
        edge,
        "FOLLOWS",
        &progress(scope, edge, None, 8),
        limits,
        IndexLifecycleScanTuning::default(),
    )
    .await
    .expect("the label source opens")
    .expect("members remain");
    assert!(matches!(source.reads, Reads::Range(_)));
    let mut ids = Vec::new();
    let end = loop {
        match source.next().await.expect("the label source reads") {
            LabelRead::Row(row) => ids.push(row.entity_id.get()),
            end @ (LabelRead::Exhausted | LabelRead::Paused(_)) => break end,
        }
    };
    assert_eq!(ids, [3, 4, 5, 6, 7, 8]);
    assert!(matches!(end, LabelRead::Exhausted));
}
