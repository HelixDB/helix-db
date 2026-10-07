//! Production-codec contracts for Active text serving reads.
//!
//! This feature-gated child of the production serving module exercises its
//! private validated-root capability without widening the default crate API.
//! All fixtures use current V1 keys and values. The scenarios cover successful
//! root/page/state projection and every family, partition, presence, value-kind,
//! ownership, and revision rejection before Tantivy or object-store access.

use std::sync::Arc;

use slatedb::object_store::memory::InMemory;
use slatedb::Db;

use super::*;
use crate::config::{SecondaryIndexDefinition, TextAnalyzerKind};
use crate::index_lifecycle::{
    IndexOperationId, IndexRecordV2, IndexRevision, IndexStateTransition, PhysicalGeneration,
    ValidatedDynamicIndexDefinition,
};

/// Opens one isolated database for serving row contracts.
async fn raw_db(name: &str) -> Db {
    Db::open(name, Arc::new(InMemory::new()))
        .await
        .expect("text serving database opens")
}

/// Constructs family-refined authority for one Active text definition.
fn text_authority(tenant_property: Option<&str>) -> ActiveTextServingAuthority {
    let definition = ValidatedTextIndexDefinition::try_new(
        IndexElementKind::Node,
        "Document",
        "body",
        tenant_property.map(str::to_owned),
        TextAnalyzerKind::Standard,
        false,
    )
    .expect("text definition validates");
    let record = IndexRecordV2::building(
        IndexId::initial(),
        ValidatedDynamicIndexDefinition::Text(definition),
        IndexRevision::initial(),
        PhysicalGeneration::Text {
            generation: IndexGenerationId::initial(),
        },
        IndexOperationId::from_bytes([1; 16]).expect("operation ID is non-nil"),
    )
    .expect("text building record validates")
    .transition(IndexStateTransition::Activate)
    .expect("text record activates");
    let active = ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &record)
        .expect("Active text record projects a handle");
    ActiveTextServingAuthority::try_from_active(&active)
        .expect("text handle refines to serving authority")
}

/// Constructs one Active secondary handle for family-refinement rejection.
fn secondary_handle() -> ActiveIndexHandle {
    let definition = ValidatedDynamicIndexDefinition::try_from(
        SecondaryIndexDefinition::node_equality("User", "email")
            .expect("secondary definition validates"),
    )
    .expect("secondary definition converts to V2");
    let record = IndexRecordV2::building(
        IndexId::new(2).expect("index ID is non-zero"),
        definition,
        IndexRevision::initial(),
        PhysicalGeneration::Secondary {
            generation: IndexGenerationId::new(2).expect("generation is non-zero"),
        },
        IndexOperationId::from_bytes([2; 16]).expect("operation ID is non-nil"),
    )
    .expect("secondary building record validates")
    .transition(IndexStateTransition::Activate)
    .expect("secondary record activates");
    ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &record)
        .expect("Active secondary record projects a handle")
}

/// Returns the exact scoped key for a text manifest root.
fn root_key(
    authority: &ActiveTextServingAuthority,
    partition: &work::TextPartition,
) -> (index_keys::TextManifestRootKey, bytes::Bytes) {
    let typed = index_keys::TextManifestRootKey {
        index_id: authority.index_id(),
        generation: authority.generation(),
        partition: partition.fingerprint(),
    };
    (
        typed,
        scoped_key(
            authority.scope(),
            index_keys::ScopedKey::TextManifestRoot(typed),
        ),
    )
}

/// Writes one valid root and returns its checked serving capability.
async fn put_root(
    db: &Db,
    authority: &ActiveTextServingAuthority,
    partition: &work::TextPartition,
    page_count: u32,
    split_count: u64,
) -> ValidatedActiveTextManifestRoot {
    let (typed, key) = root_key(authority, partition);
    db.put(
        key,
        index_values::encode_manifest_root(
            &work::TextManifestRootValue::try_new(
                authority.index_id(),
                authority.generation(),
                partition.clone(),
                TextManifestRevision::new(2).expect("root revision is non-zero"),
                page_count,
                split_count,
            )
            .expect("manifest root validates"),
        ),
    )
    .await
    .expect("manifest root writes");
    if split_count != 0 {
        db.put(
            super::super::statistics::corpus_key(
                authority.scope(),
                authority.index_id(),
                authority.generation(),
                partition,
            ),
            index_values::encode_corpus_statistics(
                &work::TextCorpusStatisticsValue::try_new(
                    authority.index_id(),
                    authority.generation(),
                    partition.clone(),
                    1,
                    1,
                )
                .expect("non-empty root corpus statistics validate"),
            ),
        )
        .await
        .expect("non-empty root corpus statistics write");
    }
    let root = load_active_manifest_root(db, authority, partition)
        .await
        .expect("manifest root loads")
        .expect("manifest root exists");
    assert_eq!(root.index_id(), typed.index_id);
    assert_eq!(root.generation(), typed.generation);
    assert_eq!(root.partition(), partition);
    assert_eq!(root.page_count(), page_count);
    assert_eq!(root.split_count(), split_count);
    root
}

/// Encodes a typed non-root value used to exercise wrong-kind rejection.
fn wrong_kind_value(authority: &ActiveTextServingAuthority) -> bytes::Bytes {
    let blob = work::BlobRef::new([9; 32], 10);
    let split =
        work::SplitRef::try_new(blob, 0, 0, 0, blob.size(), work::SplitPruning::Unavailable)
            .expect("wrong-kind split validates");
    index_values::encode_build_artifact(&work::TextBuildArtifactValue {
        index_id: authority.index_id(),
        generation: authority.generation(),
        partition: work::TextPartition::Unpartitioned,
        artifact_ordinal: 0,
        split,
    })
}

/// Proves family refinement, getters, and partition-shape validation.
async fn exercise_authority_and_partition_rejections() {
    assert!(matches!(
        ActiveTextServingAuthority::try_from_active(&secondary_handle()),
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    let unpartitioned = text_authority(None);
    assert_eq!(unpartitioned.scope(), DataScope::LegacyUnscoped);
    assert_eq!(unpartitioned.index_id(), IndexId::initial());
    assert_eq!(unpartitioned.generation(), IndexGenerationId::initial());
    assert_eq!(unpartitioned.definition().property().as_str(), "body");
    let tenant = work::TextPartition::try_tenant_value(bytes::Bytes::from_static(b"tenant-a"))
        .expect("tenant partition validates");
    let db = raw_db("production-text-serving-partition-shape").await;
    assert!(matches!(
        load_active_manifest_root(&db, &unpartitioned, &tenant).await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    assert!(matches!(
        load_active_manifest_root(
            &db,
            &text_authority(Some("tenant")),
            &work::TextPartition::Unpartitioned,
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    db.close().await.expect("partition-shape database closes");
}

/// Proves missing and malformed manifest roots fail according to partition mode.
async fn exercise_root_rejections() {
    let db = raw_db("production-text-serving-root-errors").await;
    let unpartitioned = text_authority(None);
    assert!(matches!(
        load_active_manifest_root(&db, &unpartitioned, &work::TextPartition::Unpartitioned).await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    let tenant_authority = text_authority(Some("tenant"));
    let tenant = work::TextPartition::try_tenant_value(bytes::Bytes::from_static(b"tenant-b"))
        .expect("tenant partition validates");
    assert!(load_active_manifest_root(&db, &tenant_authority, &tenant)
        .await
        .expect("missing tenant root is not corruption")
        .is_none());

    let (_, key) = root_key(&unpartitioned, &work::TextPartition::Unpartitioned);
    db.put(key.clone(), wrong_kind_value(&unpartitioned))
        .await
        .expect("wrong-kind root value writes");
    assert!(matches!(
        load_active_manifest_root(&db, &unpartitioned, &work::TextPartition::Unpartitioned).await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    db.put(
        key,
        index_values::encode_manifest_root(
            &work::TextManifestRootValue::try_new(
                IndexId::new(99).expect("wrong index ID is non-zero"),
                unpartitioned.generation(),
                work::TextPartition::Unpartitioned,
                TextManifestRevision::initial(),
                0,
                0,
            )
            .expect("cross-owned root value remains structurally valid"),
        ),
    )
    .await
    .expect("cross-owned root value writes");
    assert!(matches!(
        load_active_manifest_root(&db, &unpartitioned, &work::TextPartition::Unpartitioned).await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    db.close().await.expect("root-error database closes");
}

/// Proves page bounds, presence, value kind, and ownership checks.
async fn exercise_page_rejections() {
    let db = raw_db("production-text-serving-page-errors").await;
    let authority = text_authority(None);
    let partition = work::TextPartition::Unpartitioned;
    let root = put_root(&db, &authority, &partition, 1, 1).await;
    assert!(matches!(
        load_active_manifest_page(&db, &root, 1).await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    assert!(matches!(
        load_active_manifest_page(&db, &root, 0).await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    let page_key = scoped_key(
        authority.scope(),
        index_keys::ScopedKey::TextManifestPage(index_keys::TextManifestPageKey {
            root: root.key,
            page: 0,
        }),
    );
    db.put(page_key.clone(), wrong_kind_value(&authority))
        .await
        .expect("wrong-kind page value writes");
    assert!(matches!(
        load_active_manifest_page(&db, &root, 0).await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    let split = work::SplitRef::try_new(
        work::BlobRef::new([7; 32], 10),
        0,
        0,
        0,
        10,
        work::SplitPruning::Unavailable,
    )
    .expect("split validates");
    db.put(
        page_key.clone(),
        index_values::encode_manifest_page(
            &work::TextManifestPageValue::try_new(
                IndexId::new(99).expect("wrong index ID is non-zero"),
                authority.generation(),
                partition.clone(),
                0,
                vec![split],
            )
            .expect("cross-owned page remains structurally valid"),
        ),
    )
    .await
    .expect("cross-owned page value writes");
    assert!(matches!(
        load_active_manifest_page(&db, &root, 0).await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    db.put(
        page_key,
        index_values::encode_manifest_page(
            &work::TextManifestPageValue::try_new(
                authority.index_id(),
                authority.generation(),
                partition,
                0,
                vec![split],
            )
            .expect("owned page validates"),
        ),
    )
    .await
    .expect("owned page value writes");
    assert_eq!(
        load_active_manifest_page(&db, &root, 0)
            .await
            .expect("owned page loads"),
        vec![split]
    );
    db.close().await.expect("page-error database closes");
}

/// Proves entity-state presence, value-kind, ownership, revision, and live
/// statistics-marker checks.
async fn exercise_entity_state_rejections() {
    let db = raw_db("production-text-serving-state-errors").await;
    let authority = text_authority(None);
    let partition = work::TextPartition::Unpartitioned;
    let root = put_root(&db, &authority, &partition, 0, 0).await;
    assert!(matches!(
        load_active_entity_state(&db, &root, 42).await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    let entity = index_keys::IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(42),
    };
    let state_key = scoped_key(
        authority.scope(),
        index_keys::ScopedKey::TextEntityState(index_keys::TextEntityStateKey {
            root: root.key,
            entity,
        }),
    );
    db.put(state_key.clone(), wrong_kind_value(&authority))
        .await
        .expect("wrong-kind state value writes");
    assert!(matches!(
        load_active_entity_state(&db, &root, 42).await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    db.put(
        state_key.clone(),
        index_values::encode_text_entity_state(&work::TextEntityStateValue {
            index_id: authority.index_id(),
            generation: authority.generation(),
            partition: partition.clone(),
            entity_kind: entity.kind,
            entity_id: entity.id,
            logical_version: TextLogicalVersion::new(3).expect("logical version is non-zero"),
            live: true,
        }),
    )
    .await
    .expect("future state value writes");
    assert!(matches!(
        load_active_entity_state(&db, &root, 42).await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    db.put(
        state_key.clone(),
        index_values::encode_text_entity_state(&work::TextEntityStateValue {
            index_id: authority.index_id(),
            generation: authority.generation(),
            partition: partition.clone(),
            entity_kind: entity.kind,
            entity_id: entity.id,
            logical_version: TextLogicalVersion::new(2).expect("logical version is non-zero"),
            live: false,
        }),
    )
    .await
    .expect("owned state value writes");
    let state = load_active_entity_state(&db, &root, 42)
        .await
        .expect("owned state loads");
    assert_eq!(state.logical_version(), 2);
    assert!(!state.is_live());
    // A live state also needs the statistics marker that counts its document,
    // and this root has none.
    db.put(
        state_key,
        index_values::encode_text_entity_state(&work::TextEntityStateValue {
            index_id: authority.index_id(),
            generation: authority.generation(),
            partition,
            entity_kind: entity.kind,
            entity_id: entity.id,
            logical_version: TextLogicalVersion::new(2).expect("logical version is non-zero"),
            live: true,
        }),
    )
    .await
    .expect("live state value writes");
    assert!(matches!(
        load_active_entity_state(&db, &root, 42).await,
        Err(HelixDbError::IndexCatalogCorruption(message))
            if message.contains("no statistics marker")
    ));
    db.close().await.expect("state-error database closes");
}

/// Runs every Active text serving read contract.
pub(crate) async fn run() {
    exercise_authority_and_partition_rejections().await;
    exercise_root_rejections().await;
    exercise_page_rejections().await;
    exercise_entity_state_rejections().await;
}
