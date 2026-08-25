//! Production-codec contracts for state-only Active text retirement.
//!
//! This feature-gated child of the production retirement module keeps its
//! prepared and validated capabilities private. Every fixture uses current V1
//! keys and values. The scenarios prove family and entity authority, exact
//! canonical/root/state ownership, resource admission, serialized input
//! revalidation, and atomic root plus dead-state staging.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use slatedb::object_store::memory::InMemory;
use slatedb::{Db, IsolationLevel};

use super::*;
use crate::config::{
    SearchIndexBackfillLimits, SearchIndexBatchLimits, SecondaryIndexDefinition, TextAnalyzerKind,
    TextBackfillCompactionLimits, TextBuildArtifactLimits,
};
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::ManagedIndexKey;
use crate::index_lifecycle::{
    IndexElementKind, IndexEntityId, IndexGenerationId, IndexId, IndexOperationId, IndexRecordV2,
    IndexRevision, IndexStateTransition, PhysicalGeneration, TextLogicalVersion,
    TextManifestRevision, ValidatedDynamicIndexDefinition, ValidatedTextIndexDefinition,
};

/// Complete canonical/root/state ownership for one text entity.
struct RetirementFixture {
    handle: index_lifecycle::ActiveIndexHandle,
    root_key: Bytes,
    state_key: Bytes,
    entity: index_keys::IndexEntity,
}

/// Opens one isolated retirement database.
async fn raw_db(name: &str) -> Db {
    Db::open(name, Arc::new(InMemory::new()))
        .await
        .expect("retirement database opens")
}

/// Encodes one scoped V2 key through the current typed boundary.
fn scoped_key(scope: DataScope, logical: index_keys::ScopedKey) -> Bytes {
    ManagedIndexKey::Data {
        scope,
        kind: logical,
    }
    .to_bytes()
}

/// Seeds one exact Active text record and returns its physical row keys.
async fn seed_text_fixture(db: &Db, scope: DataScope) -> RetirementFixture {
    let definition = ValidatedTextIndexDefinition::try_new(
        IndexElementKind::Node,
        "Document",
        "body",
        None::<String>,
        TextAnalyzerKind::Standard,
        false,
    )
    .expect("text definition validates");
    let active = IndexRecordV2::building(
        IndexId::initial(),
        ValidatedDynamicIndexDefinition::Text(definition),
        IndexRevision::initial(),
        PhysicalGeneration::Text {
            generation: IndexGenerationId::initial(),
        },
        IndexOperationId::from_bytes([0x81; 16]).expect("operation ID is non-nil"),
    )
    .expect("text building record validates")
    .transition(IndexStateTransition::Activate)
    .expect("text record activates");
    let handle = index_lifecycle::ActiveIndexHandle::try_from_record(scope, &active)
        .expect("Active text record projects a handle");
    db.put(
        scoped_key(
            scope,
            index_keys::ScopedKey::index_record(active.identity().clone()),
        ),
        index_values::encode_index_record(&active),
    )
    .await
    .expect("canonical Active record writes");
    let entity = index_keys::IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(81),
    };
    let root_typed = index_keys::TextManifestRootKey {
        index_id: handle.index_id(),
        generation: handle.generation(),
        partition: work::TextPartition::Unpartitioned.fingerprint(),
    };
    RetirementFixture {
        handle,
        root_key: scoped_key(scope, index_keys::ScopedKey::TextManifestRoot(root_typed)),
        state_key: scoped_key(
            scope,
            index_keys::ScopedKey::TextEntityState(index_keys::TextEntityStateKey {
                root: root_typed,
                entity,
            }),
        ),
        entity,
    }
}

/// Constructs Active mutation limits with one exact operation ceiling.
fn limits_with_max_output_operations(output_operations: u64) -> ActiveTextMutationLimits {
    SearchIndexBackfillLimits::try_new(
        SearchIndexBatchLimits::try_new(
            NonZeroUsize::MIN,
            NonZeroU64::MAX,
            NonZeroU64::new(output_operations).expect("operation limit is non-zero"),
            NonZeroU64::MAX,
            NonZeroU64::MIN,
        )
        .expect("batch limits validate"),
        NonZeroUsize::MIN,
        TextBuildArtifactLimits::new(NonZeroUsize::MIN, NonZeroU64::MIN),
        TextBackfillCompactionLimits::new(
            NonZeroUsize::MIN,
            NonZeroU64::MAX,
            NonZeroU64::MIN,
            NonZeroU64::MAX,
            NonZeroU64::MAX,
        ),
    )
    .expect("backfill limits validate")
    .active_text_mutation()
}

/// Writes one structurally valid manifest root for the fixture owner.
async fn put_root(
    db: &Db,
    fixture: &RetirementFixture,
    revision: TextManifestRevision,
    page_count: u32,
) {
    db.put(
        fixture.root_key.clone(),
        index_values::encode_manifest_root(
            &work::TextManifestRootValue::try_new(
                fixture.handle.index_id(),
                fixture.handle.generation(),
                work::TextPartition::Unpartitioned,
                revision,
                page_count,
                u64::from(page_count),
            )
            .expect("manifest root validates"),
        ),
    )
    .await
    .expect("manifest root writes");
}

/// Writes one structurally valid entity state for the fixture owner.
async fn put_state(db: &Db, fixture: &RetirementFixture, version: u64, live: bool) {
    db.put(
        fixture.state_key.clone(),
        index_values::encode_text_entity_state(&work::TextEntityStateValue {
            index_id: fixture.handle.index_id(),
            generation: fixture.handle.generation(),
            partition: work::TextPartition::Unpartitioned,
            entity_kind: fixture.entity.kind,
            entity_id: fixture.entity.id,
            logical_version: TextLogicalVersion::new(version).expect("logical version is non-zero"),
            live,
        }),
    )
    .await
    .expect("entity state writes");
}

/// Proves non-text, wrong-kind, and missing-root requests fail before staging.
async fn exercise_handle_and_root_presence_rejections() {
    let db = raw_db("production-active-text-retirement-handle").await;
    let scope = DataScope::LegacyUnscoped;
    let secondary = IndexRecordV2::building(
        IndexId::new(2).expect("index ID is non-zero"),
        ValidatedDynamicIndexDefinition::try_from(
            SecondaryIndexDefinition::node_equality("Document", "slug")
                .expect("secondary definition validates"),
        )
        .expect("secondary definition converts to V2"),
        IndexRevision::initial(),
        PhysicalGeneration::Secondary {
            generation: IndexGenerationId::initial(),
        },
        IndexOperationId::from_bytes([0x82; 16]).expect("operation ID is non-nil"),
    )
    .expect("secondary building record validates")
    .transition(IndexStateTransition::Activate)
    .expect("secondary record activates");
    let secondary_handle = index_lifecycle::ActiveIndexHandle::try_from_record(scope, &secondary)
        .expect("Active secondary record projects a handle");
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("non-text transaction opens");
    assert!(matches!(
        prepare_active_text_retirement(
            &transaction,
            &secondary_handle,
            work::TextPartition::Unpartitioned,
            index_keys::IndexEntity {
                kind: IndexElementKind::Node,
                id: IndexEntityId::new(1),
            },
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    drop(transaction);

    let fixture = seed_text_fixture(&db, scope).await;
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("wrong-kind transaction opens");
    assert!(matches!(
        prepare_active_text_retirement(
            &transaction,
            &fixture.handle,
            work::TextPartition::Unpartitioned,
            index_keys::IndexEntity {
                kind: IndexElementKind::Edge,
                id: fixture.entity.id,
            },
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    drop(transaction);

    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("missing-root transaction opens");
    assert!(matches!(
        prepare_active_text_retirement(
            &transaction,
            &fixture.handle,
            work::TextPartition::Unpartitioned,
            fixture.entity,
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    db.close().await.expect("handle-shape database closes");
}

/// Proves mistyped, cross-owned, empty, and exhausted roots fail closed.
async fn exercise_root_shape_rejections() {
    let db = raw_db("production-active-text-retirement-roots").await;
    let scope = DataScope::LegacyUnscoped;
    let fixture = seed_text_fixture(&db, scope).await;
    db.put(
        fixture.root_key.clone(),
        index_values::encode_text_entity_state(&work::TextEntityStateValue {
            index_id: fixture.handle.index_id(),
            generation: fixture.handle.generation(),
            partition: work::TextPartition::Unpartitioned,
            entity_kind: fixture.entity.kind,
            entity_id: fixture.entity.id,
            logical_version: TextLogicalVersion::initial(),
            live: true,
        }),
    )
    .await
    .expect("mistyped root writes");
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("mistyped-root transaction opens");
    assert!(matches!(
        prepare_active_text_retirement(
            &transaction,
            &fixture.handle,
            work::TextPartition::Unpartitioned,
            fixture.entity,
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    drop(transaction);

    let other = work::TextPartition::try_tenant_value(Bytes::from_static(b"other"))
        .expect("other partition validates");
    db.put(
        fixture.root_key.clone(),
        index_values::encode_manifest_root(
            &work::TextManifestRootValue::try_new(
                fixture.handle.index_id(),
                fixture.handle.generation(),
                other,
                TextManifestRevision::initial(),
                1,
                1,
            )
            .expect("cross-owned root remains structurally valid"),
        ),
    )
    .await
    .expect("cross-owned root writes");
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("cross-owned-root transaction opens");
    assert!(matches!(
        prepare_active_text_retirement(
            &transaction,
            &fixture.handle,
            work::TextPartition::Unpartitioned,
            fixture.entity,
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    drop(transaction);

    db.put(
        fixture.root_key.clone(),
        index_values::encode_manifest_root(&work::TextManifestRootValue::empty(
            fixture.handle.index_id(),
            fixture.handle.generation(),
            work::TextPartition::Unpartitioned,
        )),
    )
    .await
    .expect("empty root writes");
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("empty-root transaction opens");
    assert!(matches!(
        prepare_active_text_retirement(
            &transaction,
            &fixture.handle,
            work::TextPartition::Unpartitioned,
            fixture.entity,
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    drop(transaction);

    put_root(
        &db,
        &fixture,
        TextManifestRevision::new(u64::MAX).expect("maximum revision is non-zero"),
        1,
    )
    .await;
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("exhausted-root transaction opens");
    assert!(matches!(
        prepare_active_text_retirement(
            &transaction,
            &fixture.handle,
            work::TextPartition::Unpartitioned,
            fixture.entity,
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    db.close().await.expect("root-shape database closes");
}

/// Proves state integrity, resource admission, revalidation, and final staging.
async fn exercise_state_and_staging_contracts() {
    let db = raw_db("production-active-text-retirement-state").await;
    let scope = DataScope::LegacyUnscoped;
    let fixture = seed_text_fixture(&db, scope).await;
    put_root(
        &db,
        &fixture,
        TextManifestRevision::new(2).expect("revision is non-zero"),
        1,
    )
    .await;
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("missing-state transaction opens");
    assert!(matches!(
        prepare_active_text_retirement(
            &transaction,
            &fixture.handle,
            work::TextPartition::Unpartitioned,
            fixture.entity,
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    drop(transaction);

    db.put(
        fixture.state_key.clone(),
        index_values::encode_manifest_root(&work::TextManifestRootValue::empty(
            fixture.handle.index_id(),
            fixture.handle.generation(),
            work::TextPartition::Unpartitioned,
        )),
    )
    .await
    .expect("mistyped state writes");
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("mistyped-state transaction opens");
    assert!(matches!(
        prepare_active_text_retirement(
            &transaction,
            &fixture.handle,
            work::TextPartition::Unpartitioned,
            fixture.entity,
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    drop(transaction);

    put_state(&db, &fixture, 2, false).await;
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("dead-state transaction opens");
    assert!(matches!(
        prepare_active_text_retirement(
            &transaction,
            &fixture.handle,
            work::TextPartition::Unpartitioned,
            fixture.entity,
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    drop(transaction);

    put_state(&db, &fixture, 2, true).await;
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("resource-limited transaction opens");
    assert!(matches!(
        prepare_active_text_retirement(
            &transaction,
            &fixture.handle,
            work::TextPartition::Unpartitioned,
            fixture.entity,
            limits_with_max_output_operations(1),
        )
        .await,
        Err(HelixDbError::ActiveTextMutationLimitExceeded {
            resource: crate::error::ActiveTextMutationResource::OutputOperations,
            observed: 2,
            limit: 1,
        })
    ));
    drop(transaction);

    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("successful preflight transaction opens");
    let prepared = prepare_active_text_retirement(
        &transaction,
        &fixture.handle,
        work::TextPartition::Unpartitioned,
        fixture.entity,
        SearchIndexBackfillLimits::default().active_text_mutation(),
    )
    .await
    .expect("exact live state prepares retirement");
    assert_eq!(prepared.measurements().output_operations(), 2);
    transaction
        .put(
            fixture.state_key.clone(),
            index_values::encode_text_entity_state(&work::TextEntityStateValue {
                index_id: fixture.handle.index_id(),
                generation: fixture.handle.generation(),
                partition: work::TextPartition::Unpartitioned,
                entity_kind: fixture.entity.kind,
                entity_id: fixture.entity.id,
                logical_version: TextLogicalVersion::new(3)
                    .expect("changed logical version is non-zero"),
                live: true,
            }),
        )
        .expect("conflicting state stages");
    assert!(matches!(
        validate_active_text_retirement(&transaction, &prepared).await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    drop(transaction);

    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("retirement commit transaction opens");
    let validated = validate_active_text_retirement(&transaction, &prepared)
        .await
        .expect("unchanged inputs validate");
    stage_validated_active_text_retirement(&transaction, validated)
        .expect("validated retirement stages");
    transaction
        .commit()
        .await
        .expect("retirement transaction commits");
    let state = index_values::decode_text_entity_state(
        &db.get(&fixture.state_key)
            .await
            .expect("state lookup succeeds")
            .expect("dead state exists"),
    )
    .expect("dead state decodes");
    assert!(!state.live);
    assert_eq!(state.logical_version.get(), 3);
    db.close().await.expect("state-contract database closes");
}

/// Runs every state-only Active text retirement contract.
pub(crate) async fn run() {
    exercise_handle_and_root_presence_rejections().await;
    exercise_root_shape_rejections().await;
    exercise_state_and_staging_contracts().await;
}
