//! Production contracts for queued vector publication's planner boundaries.
//!
//! This feature-gated child of the vector lifecycle driver proves that queue
//! publication fails closed on inconsistent state it cannot reach through a
//! consistent database: a tenant partition whose zero-count metadata still
//! names a graph, disagrees with its generation, or keeps physical rows; a
//! publication or planning target of another family; routed-catalog entries
//! of another family; and non-numeric vector payloads. Fixture rows use the
//! current key and value codecs and are staged only in uncommitted
//! transactions, so no row family or encoding is introduced.

use slatedb::object_store::memory::InMemory;

use super::driver_contracts::{
    create_build, definition, drive_to_terminal, driver, mapping_values, properties, put_source,
    read_index, test_db,
};
use super::*;
use crate::config::{SearchIndexBackfillLimits, TextAnalyzerKind};
use crate::encoding::property::property_value::PropertyValue;
use crate::encoding::property::Property;
use crate::encoding::v2::legacy::vector::transaction_guard::{
    encode_active_txn_guard, LegacyVectorTxnGuardKey,
};
use crate::index_lifecycle::mutation_catalog::MutationCatalogEntry;
use crate::index_lifecycle::outbox::CommittedOperationStep;
use crate::index_lifecycle::queue::publication::VectorPublicationResources;
use crate::index_lifecycle::queue::storage::AcknowledgementOutput;
use crate::index_lifecycle::vector::publication::stage_active_effects;
use crate::index_lifecycle::{
    IndexRevision, IndexScopeGates, IndexStateTransition, ValidatedTextIndexDefinition,
};
use crate::search::vector::{
    SimHasherRegistry, ValidatedVectorGenerationHandle, VectorCacheRegistry, VectorCacheWriteSet,
};

type Euclidean = vector::distance::Euclidean;

/// Runs every queued vector publication planner contract.
pub(crate) async fn run() {
    empty_tenant_reclamation_fails_closed().await;
    other_family_targets_fail_closed().await;
    vector_payload_conversions_are_numeric().await;
}

/// Returns one Active text record and its handle.
fn text_generation() -> (IndexRecordV2, ActiveIndexHandle) {
    let definition = ValidatedTextIndexDefinition::try_new(
        IndexElementKind::Node,
        "Document",
        "body",
        None::<String>,
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
        IndexOperationId::from_bytes([7; 16]).expect("operation ID is non-nil"),
    )
    .expect("text building record validates")
    .transition(IndexStateTransition::Activate)
    .expect("text record activates");
    let handle = ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &record)
        .expect("Active text record projects a handle");
    (record, handle)
}

/// Proves an emptied tenant partition is reclaimed only when its metadata and
/// rows prove it empty.
///
/// A zero count whose metadata still names an entry point, metadata that
/// disagrees with the Active generation, a malformed transaction guard, and
/// any physical row other than the metadata and a well-formed guard each fail
/// closed before anything is deleted.
async fn empty_tenant_reclamation_fails_closed() {
    let db = test_db("vector-publication-reclamation").await;
    let scope = DataScope::LegacyUnscoped;
    let definition = definition(Some("account_id"));
    put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], Some(10))).await;
    let (build_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
    let mut claim_sequence = 1;
    assert_eq!(
        drive_to_terminal(&db, &driver(), build_id, &mut claim_sequence).await,
        CommittedOperationStep::Completed
    );
    let active =
        ActiveIndexHandle::try_from_record(scope, &read_index(&db, scope, &definition).await)
            .expect("partitioned Active record projects a handle");
    let [mapping] = &mapping_values(&db, scope, index_id, generation).await[..] else {
        panic!("one tenant partition is mapped");
    };
    let handle = ValidatedVectorGenerationHandle::try_from_active::<Euclidean>(
        &active,
        mapping.physical_index_id,
    )
    .expect("partition mapping validates against the Active handle");
    let physical_index_id = handle.physical_index_id();
    let metadata_key = DataKey::Data {
        scope,
        kind: DataKeyKind::Vector(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
            physical_index_id,
        ))),
    }
    .to_bytes();
    let guard_key = DataKey::Data {
        scope,
        kind: DataKeyKind::Vector(VectorKey::TxnGuard(LegacyVectorTxnGuardKey::new(
            physical_index_id,
        ))),
    }
    .to_bytes();
    let stored = db
        .get(&metadata_key)
        .await
        .expect("partition metadata is readable")
        .expect("partition metadata exists");
    let metadata = || vector::decode_metadata(&stored).expect("partition metadata decodes");

    // Stages `metadata`, optionally clears every other row of the namespace,
    // and stages an optional guard in a transaction that is never committed,
    // then asks for the partition's reclamation.
    let reclaim = |metadata: vector::VectorIndexMetadata, clear: bool, guard: Option<Bytes>| {
        let (db, handle, metadata_key, guard_key) = (&db, &handle, &metadata_key, &guard_key);
        async move {
            let transaction = db
                .begin(IsolationLevel::Snapshot)
                .await
                .expect("reclamation transaction opens");
            if clear {
                for lane in VectorStorageLane::ALL {
                    let prefix =
                        DataKey::data_prefix(scope, lane.prefix_key(physical_index_id).to_bytes());
                    let mut rows = transaction
                        .scan_prefix(&prefix, ..)
                        .await
                        .expect("namespace rows are readable");
                    let mut keys = Vec::new();
                    while let Some(row) = rows.next().await.expect("namespace row is readable") {
                        keys.push(row.key);
                    }
                    for key in keys.into_iter().filter(|key| key != metadata_key) {
                        transaction.delete(key).expect("fixture row delete stages");
                    }
                }
            }
            transaction
                .put(metadata_key, vector::encode_metadata(&metadata).as_slice())
                .expect("fixture metadata stages");
            if let Some(guard) = guard {
                transaction
                    .put(guard_key, guard)
                    .expect("fixture guard stages");
            }
            let write = MeasuredVectorTransaction::new(&transaction);
            super::super::publication::stage_empty_tenant_reclamation::<Euclidean>(&write, handle)
                .await
        }
    };

    assert!(
        !reclaim(metadata(), false, None)
            .await
            .expect("a populated partition is not reclaimed"),
        "a partition that still counts an entity is kept"
    );
    let mut populated = metadata();
    populated.count = 0;
    assert!(matches!(
        reclaim(populated, false, None).await,
        Err(HelixDbError::InvariantViolation(reason)) if reason.contains("populated metadata")
    ));
    let empty = || {
        let mut empty = metadata();
        empty.count = 0;
        empty.entry_point = None;
        empty.max_layer = 0;
        empty
    };
    let mut foreign = empty();
    foreign.config.property_name = "contradicting_embedding".to_string();
    assert!(matches!(
        reclaim(foreign, false, None).await,
        Err(HelixDbError::IndexCatalogCorruption(reason)) if reason.contains("conflicts")
    ));
    assert!(matches!(
        reclaim(empty(), false, None).await,
        Err(HelixDbError::InvariantViolation(reason)) if reason.contains("retains a")
    ));
    assert!(matches!(
        reclaim(empty(), true, Some(Bytes::from_static(b"malformed guard"))).await,
        Err(HelixDbError::InvariantViolation(reason))
            if reason.contains("malformed transaction guard")
    ));
    // Only the metadata and a well-formed guard remain: both are deleted.
    assert!(reclaim(empty(), true, Some(encode_active_txn_guard()))
        .await
        .expect("an empty partition with a guard is reclaimed"));
    assert_eq!(
        db.get(&metadata_key)
            .await
            .expect("partition metadata stays readable"),
        Some(stored),
        "no reclamation committed"
    );
    db.close().await.expect("reclamation database closes");
}

/// Proves publication and routed-catalog entry points reject another family.
///
/// Staging queued effects or targeting planning at a text generation fails
/// closed, as do a routed ordinal outside the classified catalog, a
/// classifier entry whose record or handle is another family's, and a text
/// build route whose record is not building.
async fn other_family_targets_fail_closed() {
    let db = Db::builder("vector-publication-other-family", Arc::new(InMemory::new()))
        .build()
        .await
        .expect("other-family database opens");
    let (text_record, text_handle) = text_generation();
    let cache_writes = VectorCacheWriteSet::default();
    assert!(matches!(
        VectorPlanTarget::publication(&text_handle, &cache_writes),
        Err(HelixDbError::IndexCatalogCorruption(reason)) if reason.contains("another family")
    ));
    let gates = IndexScopeGates::default();
    let permit = gates
        .publication_permit(QueueTarget::new(
            DataScope::LegacyUnscoped,
            IndexId::initial(),
            IndexGenerationId::initial(),
        ))
        .await;
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("publication transaction opens");
    let resources = VectorPublicationResources {
        cache_registry: Arc::new(VectorCacheRegistry::default()),
        simhasher_registry: Arc::new(SimHasherRegistry::default()),
        batch_reads: crate::batch_reads::BatchReads::Single,
        planning_cache: Arc::new(VectorBuildCache::new(
            NonZeroU64::new(1 << 20).expect("positive"),
        )),
    };
    assert!(matches!(
        stage_active_effects(
            &db,
            &transaction,
            &permit,
            &text_handle,
            &[],
            SearchIndexBackfillLimits::default().batch(),
            AcknowledgementOutput {
                operations: 1,
                bytes: 1,
            },
            &resources,
            &cache_writes,
            None,
            NonZeroU64::MIN,
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(reason)) if reason.contains("another family")
    ));
    drop(transaction);

    let mut routed = super::super::VectorMutationSet::default();
    assert!(matches!(
        routed.queued_target(0),
        Err(HelixDbError::IndexCatalogCorruption(reason)) if reason.contains("outside its catalog")
    ));
    assert!(matches!(
        routed.include_catalog_entry(MutationCatalogEntry::Building(&text_record)),
        Err(HelixDbError::IndexCatalogCorruption(reason)) if reason.contains("another family")
    ));
    assert!(matches!(
        routed.include_catalog_entry(MutationCatalogEntry::Active {
            record: &text_record,
            handle: &text_handle,
        }),
        Err(HelixDbError::IndexCatalogCorruption(reason)) if reason.contains("another family handle")
    ));
    // A text build route whose record is not building has no owning build.
    let mut text_routed = crate::index_lifecycle::text::mutation::TextMutationSet::default();
    text_routed
        .include_catalog_entry(MutationCatalogEntry::Building(&text_record))
        .expect("the text classifier checks only the family");
    assert!(matches!(
        text_routed.queued_target(
            crate::index_lifecycle::mutation_catalog::MutationRouteTarget::TextBuilding(0)
        ),
        Err(HelixDbError::IndexCatalogCorruption(reason)) if reason.contains("not building")
    ));
    db.close().await.expect("other-family database closes");
}

/// Proves queued vector payloads accept every numeric array representation
/// and reject non-numeric ones.
async fn vector_payload_conversions_are_numeric() {
    let ValidatedDynamicIndexDefinition::Vector(definition) = definition(None) else {
        unreachable!("fixture definition is vector");
    };
    let document = |embedding: PropertyValue| {
        super::super::vector_document(
            &definition,
            &[
                Property::new("$label", PropertyValue::String("Document".to_string())),
                Property::new("embedding", embedding),
            ],
        )
    };
    for embedding in [
        PropertyValue::F64Array(vec![1.0, 2.0, 3.0]),
        PropertyValue::I64Array(vec![1, 2, 3]),
        PropertyValue::Array(vec![
            PropertyValue::I64(1),
            PropertyValue::F64(2.0),
            PropertyValue::F32(3.0),
        ]),
    ] {
        let converted = document(embedding.clone())
            .expect("numeric arrays convert")
            .expect("labelled documents are indexed");
        assert_eq!(converted.vector(), [1.0, 2.0, 3.0], "{embedding:?}");
    }
    for embedding in [
        PropertyValue::Array(vec![
            PropertyValue::I64(1),
            PropertyValue::String("two".to_string()),
            PropertyValue::F32(3.0),
        ]),
        PropertyValue::String("1,2,3".to_string()),
    ] {
        assert!(
            matches!(document(embedding.clone()), Err(HelixDbError::Query(_))),
            "{embedding:?}"
        );
    }
}

/// Holds `cache`'s retained sessions, so every planning checkout and
/// retention waits until the returned guard drops.
#[cfg(feature = "index-lifecycle-testing")]
pub(crate) async fn hold_planning_sessions(cache: &VectorBuildCache) -> impl Send + use<> {
    Arc::clone(&cache.retained).lock_owned().await
}

/// Returns how many holders of `cache`'s retained-session lock exist outside
/// the cache: held guards plus checkouts or retentions waiting for one.
#[cfg(feature = "index-lifecycle-testing")]
pub(crate) fn planning_session_lock_holders(cache: &VectorBuildCache) -> usize {
    Arc::strong_count(&cache.retained) - 1
}
