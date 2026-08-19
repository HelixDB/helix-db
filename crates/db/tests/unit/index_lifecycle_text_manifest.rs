use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use slatedb::object_store::memory::InMemory;
use slatedb::{Db, IsolationLevel};

use super::*;
use crate::index_lifecycle::text::test_support;
use crate::index_lifecycle::{IndexId, OperationCounters};

#[tokio::test]
async fn prepared_ranges_and_observations_reject_every_stale_shape() {
    let db = Db::open("text-manifest-stale-contracts", Arc::new(InMemory::new()))
        .await
        .unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let operation = test_support::operation();
    let scope = DataScope::LegacyUnscoped;
    let first =
        test_support::artifact_row(scope, &operation, work::TextPartition::Unpartitioned, 0, 1);
    let second =
        test_support::artifact_row(scope, &operation, work::TextPartition::Unpartitioned, 1, 2);
    transaction.put(first.0.clone(), first.1.clone()).unwrap();
    transaction.put(second.0.clone(), second.1.clone()).unwrap();

    let prefix = Key::data_prefix(
        scope,
        index_keys::ScopedKey::generation_prefix(
            index_keys::RecordKind::TextBuildArtifact,
            operation.index_id(),
            operation.generation(),
        ),
    );
    let second_suffix = Bytes::copy_from_slice(second.0.strip_prefix(prefix.as_ref()).unwrap());
    let range = PreparedArtifactRange {
        prefix: prefix.clone(),
        start: Bound::Unbounded,
        end: Bound::Included(second_suffix.clone()),
        rows: vec![first.clone(), second.clone()],
    };
    assert!(range.is_current(&transaction).await.unwrap());

    transaction.delete(second.0.clone()).unwrap();
    assert!(!range.is_current(&transaction).await.unwrap());
    transaction.put(second.0.clone(), second.1.clone()).unwrap();

    let truncated = PreparedArtifactRange {
        prefix,
        start: Bound::Unbounded,
        end: Bound::Included(second_suffix),
        rows: vec![first],
    };
    assert!(!truncated.is_current(&transaction).await.unwrap());

    let progress = PrefixScanProgress {
        cursor: None,
        counters: OperationCounters::default(),
    };
    let ManifestSelection::Page(prepared) = select_page(
        &transaction,
        scope,
        &operation,
        &progress,
        test_support::batch_limits(u64::MAX, u64::MAX, u64::MAX),
        test_support::compaction_limits(8, 1_024, 2_048, 1_024, 1_024),
    )
    .await
    .unwrap() else {
        panic!("valid artifacts prepare a page");
    };
    let root_key = prepared.observations[0].key.clone();
    transaction
        .put(root_key, Bytes::from_static(b"concurrent-root"))
        .unwrap();
    assert!(!prepared.stage(&transaction).await.unwrap());
}

#[tokio::test]
async fn selection_validates_resume_source_root_and_destination_contracts() {
    let db = Db::open(
        "text-manifest-selection-contracts",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let operation = test_support::operation();
    let scope = DataScope::LegacyUnscoped;
    let limits = test_support::batch_limits(u64::MAX, u64::MAX, u64::MAX);
    let manifest = test_support::compaction_limits(8, 1_024, 2_048, 1_024, 1_024);

    let invalid_progress = PrefixScanProgress {
        cursor: Some(IndexCursor::try_new(Bytes::from_static(b"foreign-prefix")).unwrap()),
        counters: OperationCounters::default(),
    };
    assert!(matches!(
        select_page(
            &transaction,
            scope,
            &operation,
            &invalid_progress,
            limits,
            manifest,
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));

    let artifact =
        test_support::artifact_row(scope, &operation, work::TextPartition::Unpartitioned, 0, 1);
    transaction
        .put(artifact.0.clone(), Bytes::from_static(b"invalid-artifact"))
        .unwrap();
    let progress = PrefixScanProgress {
        cursor: None,
        counters: OperationCounters::default(),
    };
    assert!(
        select_page(&transaction, scope, &operation, &progress, limits, manifest,)
            .await
            .is_err()
    );
    transaction
        .put(artifact.0.clone(), artifact.1.clone())
        .unwrap();

    let root_typed = index_keys::TextManifestRootKey {
        index_id: operation.index_id(),
        generation: operation.generation(),
        partition: work::TextPartition::Unpartitioned.fingerprint(),
    };
    let root_key = scoped_key(scope, index_keys::ScopedKey::TextManifestRoot(root_typed));
    let foreign_root = work::TextManifestRootValue::try_new(
        IndexId::new(operation.index_id().get() + 1).unwrap(),
        operation.generation(),
        work::TextPartition::Unpartitioned,
        TextManifestRevision::initial(),
        1,
        1,
    )
    .unwrap();
    transaction
        .put(
            root_key.clone(),
            index_values::encode_manifest_root(&foreign_root),
        )
        .unwrap();
    assert!(matches!(
        select_page(&transaction, scope, &operation, &progress, limits, manifest,).await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));

    let root = work::TextManifestRootValue::try_new(
        operation.index_id(),
        operation.generation(),
        work::TextPartition::Unpartitioned,
        TextManifestRevision::new(2).unwrap(),
        1,
        1,
    )
    .unwrap();
    transaction
        .put(root_key, index_values::encode_manifest_root(&root))
        .unwrap();
    let occupied_page = scoped_key(
        scope,
        index_keys::ScopedKey::TextManifestPage(index_keys::TextManifestPageKey {
            root: root_typed,
            page: 1,
        }),
    );
    transaction
        .put(occupied_page, Bytes::from_static(b"occupied"))
        .unwrap();
    assert!(matches!(
        select_page(&transaction, scope, &operation, &progress, limits, manifest,).await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
}

#[tokio::test]
async fn selection_encodes_page_exhaustion_and_every_bounded_stop() {
    let db = Db::open("text-manifest-bounds-contracts", Arc::new(InMemory::new()))
        .await
        .unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let operation = test_support::operation();
    let scope = DataScope::LegacyUnscoped;
    let progress = PrefixScanProgress {
        cursor: None,
        counters: OperationCounters::default(),
    };
    let artifact =
        test_support::artifact_row(scope, &operation, work::TextPartition::Unpartitioned, 0, 1);
    transaction.put(artifact.0, artifact.1).unwrap();

    let root_typed = index_keys::TextManifestRootKey {
        index_id: operation.index_id(),
        generation: operation.generation(),
        partition: work::TextPartition::Unpartitioned.fingerprint(),
    };
    let root_key = scoped_key(scope, index_keys::ScopedKey::TextManifestRoot(root_typed));
    let exhausted_root = work::TextManifestRootValue::try_new(
        operation.index_id(),
        operation.generation(),
        work::TextPartition::Unpartitioned,
        TextManifestRevision::new(u64::from(u32::MAX) + 2).unwrap(),
        u32::MAX,
        u64::from(u32::MAX),
    )
    .unwrap();
    transaction
        .put(
            root_key.clone(),
            index_values::encode_manifest_root(&exhausted_root),
        )
        .unwrap();
    let ManifestSelection::Blocked { blocker, .. } = select_page(
        &transaction,
        scope,
        &operation,
        &progress,
        test_support::batch_limits(u64::MAX, u64::MAX, u64::MAX),
        test_support::compaction_limits(8, 1_024, 2_048, 1_024, 1_024),
    )
    .await
    .unwrap() else {
        panic!("exhausted page count is a durable blocker");
    };
    assert!(matches!(
        blocker,
        IndexOperationBlocker::ManifestLimit { .. }
    ));
    transaction.delete(root_key).unwrap();

    let tiny_manifest = test_support::compaction_limits(8, 1_024, 2_048, 1_024, 1);
    let ManifestSelection::Blocked { blocker, .. } = select_page(
        &transaction,
        scope,
        &operation,
        &progress,
        test_support::batch_limits(u64::MAX, u64::MAX, u64::MAX),
        tiny_manifest,
    )
    .await
    .unwrap() else {
        panic!("indivisible manifest bytes are blocked");
    };
    assert!(matches!(
        blocker,
        IndexOperationBlocker::ManifestLimit { .. }
    ));

    let one_entity = SearchIndexBatchLimits::try_new(
        NonZeroUsize::MIN,
        NonZeroU64::new(u64::MAX).unwrap(),
        NonZeroU64::new(u64::MAX).unwrap(),
        NonZeroU64::new(u64::MAX).unwrap(),
        NonZeroU64::new(u64::MAX).unwrap(),
    )
    .unwrap();
    let ManifestSelection::Page(page) = select_page(
        &transaction,
        scope,
        &operation,
        &progress,
        one_entity,
        test_support::compaction_limits(8, 1_024, 2_048, 1_024, 1_024),
    )
    .await
    .unwrap() else {
        panic!("one entity limit admits exactly one artifact");
    };
    assert_eq!(page.output_operations(), 3);
}

#[tokio::test]
async fn resume_and_partition_boundaries_select_only_the_encoded_run() {
    let db = Db::open("text-manifest-run-contracts", Arc::new(InMemory::new()))
        .await
        .unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let operation = test_support::operation();
    let scope = DataScope::LegacyUnscoped;
    let first =
        test_support::artifact_row(scope, &operation, work::TextPartition::Unpartitioned, 0, 1);
    let second =
        test_support::artifact_row(scope, &operation, work::TextPartition::Unpartitioned, 1, 2);
    transaction.put(first.0.clone(), first.1).unwrap();
    transaction.put(second.0.clone(), second.1).unwrap();
    let progress = PrefixScanProgress {
        cursor: Some(IndexCursor::try_new(first.0).unwrap()),
        counters: OperationCounters::default(),
    };
    let ManifestSelection::Page(page) = select_page(
        &transaction,
        scope,
        &operation,
        &progress,
        test_support::batch_limits(u64::MAX, u64::MAX, u64::MAX),
        test_support::compaction_limits(8, 1_024, 2_048, 1_024, 1_024),
    )
    .await
    .unwrap() else {
        panic!("resume selects the strict successor");
    };
    assert_eq!(page.completed_cursor().as_bytes(), &second.0);
}
