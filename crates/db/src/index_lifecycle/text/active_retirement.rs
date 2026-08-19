//! State-only retirement for one Active text-index partition.
//!
//! Removing an indexed document, or moving it to another tenant partition,
//! requires no new blob. The authoritative graph transaction must still bump
//! the old partition's manifest revision and write an exact dead entity-state
//! row so readers reject every older split version. This module prepares those
//! two canonical V1 rows, retains their exact Active-record/root/state reads,
//! and separates fallible revalidation from infallible staging. Request-level
//! orchestration can therefore admit and validate all retirements, uploads, and
//! hidden-build deltas before buffering any graph or index write.

use bytes::Bytes;
use slatedb::DbTransaction;

use crate::config::ActiveTextMutationLimits;
use crate::encoding::v2::keys as index_keys;
use crate::encoding::v2::values as index_values;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::{self, work};

use super::active_preflight::ActiveTextMutationMeasurements;

/// One exact row retained from retirement planning through final validation.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveRetirementObservation {
    key: Bytes,
    value: Option<Bytes>,
}

/// Measured state-only retirement prepared before request admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PreparedActiveTextRetirement {
    observations: Vec<ActiveRetirementObservation>,
    root_key: Bytes,
    root_value: Bytes,
    state_key: Bytes,
    state_value: Bytes,
    measurements: ActiveTextMutationMeasurements,
}

#[cfg(any(test, feature = "production-coverage"))]
impl PreparedActiveTextRetirement {
    /// Returns exact work contributed to request-level resource admission.
    pub(super) const fn measurements(&self) -> ActiveTextMutationMeasurements {
        self.measurements
    }
}

/// Fully revalidated retirement rows ready for infallible staging.
#[cfg(any(test, feature = "production-coverage"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ValidatedActiveTextRetirement {
    prepared: PreparedActiveTextRetirement,
}

/// Prepares one exact live-to-dead Active entity-state transition.
pub(super) async fn prepare_active_text_retirement(
    transaction: &DbTransaction,
    handle: &index_lifecycle::ActiveIndexHandle,
    partition: work::TextPartition,
    entity: index_keys::IndexEntity,
    limits: ActiveTextMutationLimits,
) -> Result<PreparedActiveTextRetirement> {
    let index_lifecycle::ActiveIndexHandle::Text { .. } = handle else {
        return Err(corruption(
            "Active text retirement received a non-text generation handle",
        ));
    };
    if handle.identity().element_kind() != entity.kind {
        return Err(corruption(
            "Active text retirement entity kind disagrees with its Active index",
        ));
    }
    let scope = handle.scope();
    let index_id = handle.index_id();
    let generation = handle.generation();
    let (record_key, record_value) =
        index_lifecycle::repository::revalidate_active_handle_row(transaction, handle).await?;
    let mut observations = vec![ActiveRetirementObservation {
        key: record_key,
        value: Some(record_value),
    }];

    let root_typed = index_keys::TextManifestRootKey {
        index_id,
        generation,
        partition: partition.fingerprint(),
    };
    let root_key =
        super::attachment::scoped_key(scope, index_keys::ScopedKey::TextManifestRoot(root_typed));
    let Some(root_bytes) = transaction.get(&root_key).await? else {
        return Err(corruption("Active text retirement found no manifest root"));
    };
    observations.push(ActiveRetirementObservation {
        key: root_key.clone(),
        value: Some(root_bytes.clone()),
    });
    let root = index_lifecycle::expect_typed_value(
        index_values::decode_manifest_root(&root_bytes),
        "Active text retirement root key contains another value kind",
    )?;
    if root.index_id() != index_id
        || root.generation() != generation
        || root.partition() != &partition
        || root_typed.partition != root.partition().fingerprint()
    {
        return Err(corruption(
            "Active text retirement root key/value ownership mismatch",
        ));
    }
    if root.page_count() == 0 {
        return Err(corruption(
            "Active text retirement found a live entity in an empty manifest",
        ));
    }
    let next_revision = root
        .revision()
        .checked_next()
        .map_err(|_| corruption("Active text retirement manifest revision is exhausted"))?;
    let logical_version =
        index_lifecycle::TextLogicalVersion::new(next_revision.get()).map_err(|_| {
            corruption("non-zero manifest revision did not form a text logical version")
        })?;

    let state_typed = index_keys::TextEntityStateKey {
        root: root_typed,
        entity,
    };
    let state_key =
        super::attachment::scoped_key(scope, index_keys::ScopedKey::TextEntityState(state_typed));
    let Some(state_bytes) = transaction.get(&state_key).await? else {
        return Err(corruption(
            "Active text retirement found no live entity state",
        ));
    };
    observations.push(ActiveRetirementObservation {
        key: state_key.clone(),
        value: Some(state_bytes.clone()),
    });
    let state = index_lifecycle::expect_typed_value(
        index_values::decode_text_entity_state(&state_bytes),
        "Active text retirement state key contains another value kind",
    )?;
    if state.index_id != index_id
        || state.generation != generation
        || state.partition != partition
        || state.entity_kind != entity.kind
        || state.entity_id != entity.id
        || !state.live
        || state.logical_version.get() > root.revision().get()
    {
        return Err(corruption(
            "Active text retirement state ownership or live version mismatch",
        ));
    }

    let next_root = work::TextManifestRootValue::try_new(
        index_id,
        generation,
        partition.clone(),
        next_revision,
        root.page_count(),
        root.split_count(),
    )
    .map_err(|error| corruption(format!("Active text retirement root is invalid: {error}")))?;
    let root_value = index_values::encode_manifest_root(&next_root);
    let state_value = index_values::encode_text_entity_state(&work::TextEntityStateValue {
        index_id,
        generation,
        partition: partition.clone(),
        entity_kind: entity.kind,
        entity_id: entity.id,
        logical_version,
        live: false,
    });
    let observed_bytes = observations.iter().fold(0_u64, |bytes, observation| {
        bytes
            .saturating_add(u64::try_from(observation.key.len()).unwrap_or(u64::MAX))
            .saturating_add(
                observation
                    .value
                    .as_ref()
                    .map_or(0, |value| u64::try_from(value.len()).unwrap_or(u64::MAX)),
            )
    });
    let output_bytes = u64::try_from(root_key.len())
        .unwrap_or(u64::MAX)
        .saturating_add(u64::try_from(root_value.len()).unwrap_or(u64::MAX))
        .saturating_add(u64::try_from(state_key.len()).unwrap_or(u64::MAX))
        .saturating_add(u64::try_from(state_value.len()).unwrap_or(u64::MAX));
    let measurements = ActiveTextMutationMeasurements::try_admit(
        limits,
        observed_bytes.saturating_mul(2),
        2,
        output_bytes,
        0,
        0,
    )?;

    Ok(PreparedActiveTextRetirement {
        observations,
        root_key,
        root_value,
        state_key,
        state_value,
        measurements,
    })
}

/// Revalidates one retirement without staging either replacement row.
#[cfg(any(test, feature = "production-coverage"))]
pub(super) async fn validate_active_text_retirement(
    transaction: &DbTransaction,
    prepared: &PreparedActiveTextRetirement,
) -> Result<ValidatedActiveTextRetirement> {
    for observation in &prepared.observations {
        if transaction.get(&observation.key).await? != observation.value {
            return Err(corruption(
                "Active text retirement input changed after serialized preflight",
            ));
        }
    }
    Ok(ValidatedActiveTextRetirement {
        prepared: prepared.clone(),
    })
}

/// Stages one retirement only after every request input has validated.
#[cfg(any(test, feature = "production-coverage"))]
pub(super) fn stage_validated_active_text_retirement(
    transaction: &DbTransaction,
    validated: ValidatedActiveTextRetirement,
) -> Result<()> {
    transaction.put(validated.prepared.root_key, validated.prepared.root_value)?;
    transaction.put(validated.prepared.state_key, validated.prepared.state_value)?;
    Ok(())
}

/// Constructs the stable corruption category for retirement disagreement.
fn corruption(reason: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(reason.into())
}

#[cfg(any(test, feature = "production-coverage"))]
#[path = "../../../tests/production_support/index_lifecycle_active_text_retirement.rs"]
pub(crate) mod production_contracts;

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::config::{
        SearchIndexBackfillLimits, SearchIndexBatchLimits, SecondaryIndexDefinition,
        TextAnalyzerKind, TextBackfillCompactionLimits, TextBuildArtifactLimits,
    };

    #[tokio::test]
    async fn production_active_text_retirement_matrix_runs_in_workspace_tests() {
        production_contracts::run().await;
    }
    use crate::encoding::v1::keys::tenant::DataScope;
    use crate::encoding::v2::keys::Key;
    use crate::index_lifecycle::{
        IndexElementKind, IndexEntityId, IndexGenerationId, IndexId, IndexOperationId,
        IndexRecordV2, IndexRevision, IndexStateTransition, PhysicalGeneration, TextLogicalVersion,
        TextManifestRevision, ValidatedDynamicIndexDefinition, ValidatedTextIndexDefinition,
    };

    struct RetirementFixture {
        handle: index_lifecycle::ActiveIndexHandle,
        root_key: Bytes,
        state_key: Bytes,
        entity: index_keys::IndexEntity,
    }

    async fn raw_db(name: &str) -> Db {
        Db::open(name, Arc::new(InMemory::new())).await.unwrap()
    }

    fn scoped_key(scope: DataScope, logical: index_keys::ScopedKey) -> Bytes {
        Key::Data {
            scope,
            kind: logical,
        }
        .to_bytes()
    }

    async fn seed_text_fixture(db: &Db, scope: DataScope) -> RetirementFixture {
        let definition = ValidatedTextIndexDefinition::try_new(
            IndexElementKind::Node,
            "Document",
            "body",
            None::<String>,
            TextAnalyzerKind::Standard,
            false,
        )
        .unwrap();
        let building = IndexRecordV2::building(
            IndexId::initial(),
            ValidatedDynamicIndexDefinition::Text(definition),
            IndexRevision::initial(),
            PhysicalGeneration::Text {
                generation: IndexGenerationId::initial(),
            },
            IndexOperationId::from_bytes([0x81; 16]).unwrap(),
        )
        .unwrap();
        let active = building.transition(IndexStateTransition::Activate).unwrap();
        let handle = index_lifecycle::ActiveIndexHandle::try_from_record(scope, &active).unwrap();
        db.put(
            scoped_key(
                scope,
                index_keys::ScopedKey::index_record(active.identity().clone()),
            ),
            index_values::encode_index_record(&active),
        )
        .await
        .unwrap();
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(81),
        };
        let root_typed = index_keys::TextManifestRootKey {
            index_id: handle.index_id(),
            generation: handle.generation(),
            partition: work::TextPartition::Unpartitioned.fingerprint(),
        };
        let root_key = scoped_key(scope, index_keys::ScopedKey::TextManifestRoot(root_typed));
        let state_key = scoped_key(
            scope,
            index_keys::ScopedKey::TextEntityState(index_keys::TextEntityStateKey {
                root: root_typed,
                entity,
            }),
        );
        RetirementFixture {
            handle,
            root_key,
            state_key,
            entity,
        }
    }

    fn limits_with_max_output_operations(output_operations: u64) -> ActiveTextMutationLimits {
        SearchIndexBackfillLimits::try_new(
            SearchIndexBatchLimits::try_new(
                NonZeroUsize::MIN,
                NonZeroU64::new(u64::MAX).unwrap(),
                NonZeroU64::new(output_operations).unwrap(),
                NonZeroU64::new(u64::MAX).unwrap(),
                NonZeroU64::MIN,
            )
            .unwrap(),
            NonZeroUsize::MIN,
            TextBuildArtifactLimits::new(NonZeroUsize::MIN, NonZeroU64::MIN),
            TextBackfillCompactionLimits::new(
                NonZeroUsize::MIN,
                NonZeroU64::new(u64::MAX).unwrap(),
                NonZeroU64::MIN,
                NonZeroU64::new(u64::MAX).unwrap(),
                NonZeroU64::new(u64::MAX).unwrap(),
            ),
        )
        .unwrap()
        .active_text_mutation()
    }

    #[tokio::test]
    async fn rejects_non_text_wrong_entity_kind_and_missing_root() {
        let db = raw_db("active-text-retirement-handle-shapes").await;
        let scope = DataScope::LegacyUnscoped;
        let secondary_definition = ValidatedDynamicIndexDefinition::try_from(
            SecondaryIndexDefinition::node_equality("Document", "slug").unwrap(),
        )
        .unwrap();
        let secondary = IndexRecordV2::building(
            IndexId::new(2).unwrap(),
            secondary_definition,
            IndexRevision::initial(),
            PhysicalGeneration::Secondary {
                generation: IndexGenerationId::initial(),
            },
            IndexOperationId::from_bytes([0x82; 16]).unwrap(),
        )
        .unwrap()
        .transition(IndexStateTransition::Activate)
        .unwrap();
        let secondary_handle =
            index_lifecycle::ActiveIndexHandle::try_from_record(scope, &secondary).unwrap();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
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
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "Active text retirement received a non-text generation handle"
        ));
        transaction.commit().await.unwrap();

        let fixture = seed_text_fixture(&db, scope).await;
        let wrong_kind = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            prepare_active_text_retirement(
                &wrong_kind,
                &fixture.handle,
                work::TextPartition::Unpartitioned,
                index_keys::IndexEntity {
                    kind: IndexElementKind::Edge,
                    id: fixture.entity.id,
                },
                SearchIndexBackfillLimits::default().active_text_mutation(),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason
                    == "Active text retirement entity kind disagrees with its Active index"
        ));
        wrong_kind.commit().await.unwrap();

        let missing_root = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            prepare_active_text_retirement(
                &missing_root,
                &fixture.handle,
                work::TextPartition::Unpartitioned,
                fixture.entity,
                SearchIndexBackfillLimits::default().active_text_mutation(),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "Active text retirement found no manifest root"
        ));
        missing_root.commit().await.unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn rejects_mistyped_mismatched_empty_and_exhausted_roots() {
        let db = raw_db("active-text-retirement-root-shapes").await;
        let scope = DataScope::LegacyUnscoped;
        let fixture = seed_text_fixture(&db, scope).await;
        let wrong_value = work::TextEntityStateValue {
            index_id: fixture.handle.index_id(),
            generation: fixture.handle.generation(),
            partition: work::TextPartition::Unpartitioned,
            entity_kind: fixture.entity.kind,
            entity_id: fixture.entity.id,
            logical_version: TextLogicalVersion::initial(),
            live: true,
        };
        db.put(
            fixture.root_key.clone(),
            index_values::encode_text_entity_state(&wrong_value),
        )
        .await
        .unwrap();
        let mistyped = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            prepare_active_text_retirement(
                &mistyped,
                &fixture.handle,
                work::TextPartition::Unpartitioned,
                fixture.entity,
                SearchIndexBackfillLimits::default().active_text_mutation(),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "Active text retirement root key contains another value kind"
        ));
        mistyped.commit().await.unwrap();

        let other_partition =
            work::TextPartition::try_tenant_value(Bytes::from_static(b"other")).unwrap();
        db.put(
            fixture.root_key.clone(),
            index_values::encode_manifest_root(
                &work::TextManifestRootValue::try_new(
                    fixture.handle.index_id(),
                    fixture.handle.generation(),
                    other_partition,
                    TextManifestRevision::initial(),
                    1,
                    1,
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        let mismatched = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            prepare_active_text_retirement(
                &mismatched,
                &fixture.handle,
                work::TextPartition::Unpartitioned,
                fixture.entity,
                SearchIndexBackfillLimits::default().active_text_mutation(),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "Active text retirement root key/value ownership mismatch"
        ));
        mismatched.commit().await.unwrap();

        db.put(
            fixture.root_key.clone(),
            index_values::encode_manifest_root(&work::TextManifestRootValue::empty(
                fixture.handle.index_id(),
                fixture.handle.generation(),
                work::TextPartition::Unpartitioned,
            )),
        )
        .await
        .unwrap();
        let empty = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            prepare_active_text_retirement(
                &empty,
                &fixture.handle,
                work::TextPartition::Unpartitioned,
                fixture.entity,
                SearchIndexBackfillLimits::default().active_text_mutation(),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "Active text retirement found a live entity in an empty manifest"
        ));
        empty.commit().await.unwrap();

        db.put(
            fixture.root_key.clone(),
            index_values::encode_manifest_root(
                &work::TextManifestRootValue::try_new(
                    fixture.handle.index_id(),
                    fixture.handle.generation(),
                    work::TextPartition::Unpartitioned,
                    TextManifestRevision::new(u64::MAX).unwrap(),
                    1,
                    1,
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        let exhausted = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            prepare_active_text_retirement(
                &exhausted,
                &fixture.handle,
                work::TextPartition::Unpartitioned,
                fixture.entity,
                SearchIndexBackfillLimits::default().active_text_mutation(),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "Active text retirement manifest revision is exhausted"
        ));
        exhausted.commit().await.unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn rejects_missing_mistyped_dead_state_and_exact_resource_overflow() {
        let db = raw_db("active-text-retirement-state-shapes").await;
        let scope = DataScope::LegacyUnscoped;
        let fixture = seed_text_fixture(&db, scope).await;
        db.put(
            fixture.root_key.clone(),
            index_values::encode_manifest_root(
                &work::TextManifestRootValue::try_new(
                    fixture.handle.index_id(),
                    fixture.handle.generation(),
                    work::TextPartition::Unpartitioned,
                    TextManifestRevision::new(2).unwrap(),
                    1,
                    1,
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        let missing = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            prepare_active_text_retirement(
                &missing,
                &fixture.handle,
                work::TextPartition::Unpartitioned,
                fixture.entity,
                SearchIndexBackfillLimits::default().active_text_mutation(),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "Active text retirement found no live entity state"
        ));
        missing.commit().await.unwrap();

        db.put(
            fixture.state_key.clone(),
            index_values::encode_manifest_root(&work::TextManifestRootValue::empty(
                fixture.handle.index_id(),
                fixture.handle.generation(),
                work::TextPartition::Unpartitioned,
            )),
        )
        .await
        .unwrap();
        let mistyped = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            prepare_active_text_retirement(
                &mistyped,
                &fixture.handle,
                work::TextPartition::Unpartitioned,
                fixture.entity,
                SearchIndexBackfillLimits::default().active_text_mutation(),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "Active text retirement state key contains another value kind"
        ));
        mistyped.commit().await.unwrap();

        let dead_state = work::TextEntityStateValue {
            index_id: fixture.handle.index_id(),
            generation: fixture.handle.generation(),
            partition: work::TextPartition::Unpartitioned,
            entity_kind: fixture.entity.kind,
            entity_id: fixture.entity.id,
            logical_version: TextLogicalVersion::new(2).unwrap(),
            live: false,
        };
        db.put(
            fixture.state_key.clone(),
            index_values::encode_text_entity_state(&dead_state.clone()),
        )
        .await
        .unwrap();
        let dead = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            prepare_active_text_retirement(
                &dead,
                &fixture.handle,
                work::TextPartition::Unpartitioned,
                fixture.entity,
                SearchIndexBackfillLimits::default().active_text_mutation(),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "Active text retirement state ownership or live version mismatch"
        ));
        dead.commit().await.unwrap();

        let live_state = work::TextEntityStateValue {
            live: true,
            ..dead_state
        };
        db.put(
            fixture.state_key.clone(),
            index_values::encode_text_entity_state(&live_state),
        )
        .await
        .unwrap();
        let limited = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            prepare_active_text_retirement(
                &limited,
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
        limited.commit().await.unwrap();
        db.close().await.unwrap();
    }
}
