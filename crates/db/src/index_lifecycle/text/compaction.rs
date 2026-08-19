//! Typed artifact selection and retirement for V2 text-build compaction.
//!
//! This module owns the database half of compaction. It scans only the exact
//! generation-qualified build-artifact prefix, admits a same-partition input
//! set under fan-in, immutable-input, temporary-disk, and transaction limits,
//! and point-reads the generation-qualified entity state used to prune stale
//! documents. Object materialization and CPU-heavy merging remain in
//! [`crate::search::text::compaction`], while the driver persists the exact
//! immutable replacement before attaching it and retiring its inputs in one
//! transaction. Retired blob objects are intentionally left in storage.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::Bound;

use bytes::Bytes;
use slatedb::DbTransaction;

use crate::config::{SearchIndexBatchLimits, TextBackfillCompactionLimits};
use crate::encoding::v1::keys::tenant::DataScope;
use crate::encoding::v2::keys as index_keys;
use crate::encoding::v2::keys::Key;
use crate::encoding::v2::values as index_values;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::work;
use crate::index_lifecycle::{
    IndexCursor, IndexEntityId, IndexOperationRecord, PrefixScanProgress,
};

const MAX_COMPACTION_INPUT_ARTIFACTS: usize = 1024;

/// Exact row value observed while preparing work outside the commit transaction.
#[derive(Debug, Clone)]
pub(super) struct RowObservation {
    pub(super) key: Bytes,
    pub(super) value: Option<Bytes>,
}

/// Bounded artifact-prefix decision made from one short-lived snapshot.
#[derive(Debug)]
pub(super) enum ArtifactSelection {
    /// No artifact remains after the strict resume cursor.
    Exhausted,
    /// One artifact cannot participate in a useful bounded merge and is final.
    Advance {
        cursor: IndexCursor,
        observation: RowObservation,
    },
    /// At least two exact same-partition artifacts can be merged safely.
    Compact(SelectedArtifactBatch),
}

/// Complete, valid-by-construction input to one physical merge.
#[derive(Debug)]
pub(super) struct SelectedArtifactBatch {
    pub(super) partition: work::TextPartition,
    pub(super) artifact_keys: Vec<IndexCursor>,
    pub(super) split_refs: Vec<crate::search::text::TextSplitRef>,
    pub(super) pruning: work::SplitPruning,
    pub(super) observations: Vec<RowObservation>,
    pub(super) input_blob_bytes: u64,
    pub(super) retirement_output_operations: u64,
    pub(super) retirement_output_bytes: u64,
}

/// Authoritative live versions plus every state row that must remain unchanged.
#[derive(Debug)]
pub(super) struct ResolvedLiveVersions {
    pub(super) live_versions: HashMap<u64, u64>,
    pub(super) observations: Vec<RowObservation>,
}

/// Selects one useful same-partition artifact set without retaining the snapshot.
///
/// A configured fan-in of one, an input whose partner would exceed a byte or
/// transaction limit, or a temporary budget unable to reserve the maximum
/// output simply advances that single artifact. Such artifacts remain valid
/// final-manifest inputs; compaction is an optimization and must not turn a
/// valid immutable split into permanently blocked build work.
pub(super) async fn select_artifacts(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &PrefixScanProgress,
    batch_limits: SearchIndexBatchLimits,
    compaction_limits: TextBackfillCompactionLimits,
) -> Result<ArtifactSelection> {
    let prefix = Key::data_prefix(
        scope,
        index_keys::ScopedKey::generation_prefix(
            index_keys::RecordKind::TextBuildArtifact,
            operation.index_id(),
            operation.generation(),
        ),
    );
    let start = match progress.cursor.as_ref() {
        Some(cursor) => {
            let Some(suffix) = cursor.as_bytes().strip_prefix(prefix.as_ref()) else {
                return Err(corruption(
                    "text compaction cursor is outside its exact artifact prefix",
                ));
            };
            Bound::Excluded(Bytes::copy_from_slice(suffix))
        }
        None => Bound::Unbounded,
    };
    let mut rows = transaction
        .scan_prefix(&prefix, (start, Bound::Unbounded))
        .await?;
    let Some(first_row) = rows.next().await? else {
        return Ok(ArtifactSelection::Exhausted);
    };
    let (first_key, first_artifact) = super::attachment::decode_build_artifact(
        scope,
        operation,
        &first_row.key,
        &first_row.value,
    )?;
    let first_cursor = IndexCursor::try_new(first_row.key.clone())
        .map_err(|error| corruption(format!("invalid text artifact cursor: {error}")))?;
    let first_observation = RowObservation {
        key: first_row.key.clone(),
        value: Some(first_row.value.clone()),
    };
    let maximum_inputs = compaction_limits
        .max_fan_in()
        .get()
        .min(MAX_COMPACTION_INPUT_ARTIFACTS);
    let temporary_input_limit = compaction_limits
        .max_temporary_disk_bytes()
        .get()
        .saturating_sub(compaction_limits.max_output_blob_bytes().get());
    let input_limit = compaction_limits
        .max_input_bytes()
        .get()
        .min(temporary_input_limit);
    if maximum_inputs < 2 || first_artifact.split.total_size() > input_limit {
        return Ok(ArtifactSelection::Advance {
            cursor: first_cursor,
            observation: first_observation,
        });
    }

    let first_runtime_split = runtime_split(first_artifact.split);
    let (first_retirement_operations, first_retirement_bytes) =
        retirement_measurement(scope, operation, first_key, &first_artifact, true);
    if first_retirement_operations > batch_limits.max_output_operations().get()
        || first_retirement_bytes > batch_limits.max_output_bytes().get()
    {
        return Ok(ArtifactSelection::Advance {
            cursor: first_cursor,
            observation: first_observation,
        });
    }

    let partition = first_artifact.partition.clone();
    let mut artifact_keys = vec![first_cursor.clone()];
    let mut split_refs = vec![first_runtime_split];
    let mut pruning = first_artifact.split.pruning();
    let mut observations = vec![first_observation.clone()];
    let mut input_blob_bytes = first_artifact.split.total_size();
    let mut retirement_output_operations = first_retirement_operations;
    let mut retirement_output_bytes = first_retirement_bytes;
    let mut candidate_hashes = HashSet::from([first_artifact.split.blob().hash]);

    while artifact_keys.len() < maximum_inputs {
        let Some(row) = rows.next().await? else {
            break;
        };
        let (key, artifact) =
            super::attachment::decode_build_artifact(scope, operation, &row.key, &row.value)?;
        if key.root.partition != first_key.root.partition {
            break;
        }
        if artifact.partition != partition {
            return Err(corruption(
                "text artifact partition fingerprint collision changed canonical ownership",
            ));
        }
        let next_input_bytes = input_blob_bytes
            .checked_add(artifact.split.total_size())
            .ok_or_else(|| corruption("text compaction input bytes overflowed"))?;
        if next_input_bytes > input_limit {
            break;
        }
        let creates_candidate = candidate_hashes.insert(artifact.split.blob().hash);
        let (next_operations, next_bytes) =
            retirement_measurement(scope, operation, key, &artifact, creates_candidate);
        let admitted_operations = retirement_output_operations
            .checked_add(next_operations)
            .ok_or_else(|| corruption("text compaction retirement operations overflowed"))?;
        let admitted_bytes = retirement_output_bytes
            .checked_add(next_bytes)
            .ok_or_else(|| corruption("text compaction retirement bytes overflowed"))?;
        if admitted_operations > batch_limits.max_output_operations().get()
            || admitted_bytes > batch_limits.max_output_bytes().get()
        {
            break;
        }
        artifact_keys.push(
            IndexCursor::try_new(row.key.clone())
                .map_err(|error| corruption(format!("invalid text artifact cursor: {error}")))?,
        );
        split_refs.push(runtime_split(artifact.split));
        pruning = pruning.union(artifact.split.pruning());
        observations.push(RowObservation {
            key: row.key,
            value: Some(row.value),
        });
        input_blob_bytes = next_input_bytes;
        retirement_output_operations = admitted_operations;
        retirement_output_bytes = admitted_bytes;
    }

    if artifact_keys.len() < 2 {
        return Ok(ArtifactSelection::Advance {
            cursor: first_cursor,
            observation: first_observation,
        });
    }
    Ok(ArtifactSelection::Compact(SelectedArtifactBatch {
        partition,
        artifact_keys,
        split_refs,
        pruning,
        observations,
        input_blob_bytes,
        retirement_output_operations,
        retirement_output_bytes,
    }))
}

/// Resolves the current live version of every entity found in selected splits.
///
/// The caller must retain the returned observations until its operation/child
/// transaction commits. A concurrent catch-up or mutation then conflicts with
/// compaction instead of allowing a split built from a stale state snapshot to
/// retire its exact inputs.
pub(super) async fn resolve_live_versions(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    partition: &work::TextPartition,
    document_versions: &[(u64, u64)],
) -> Result<ResolvedLiveVersions> {
    let mut entity_ids = BTreeSet::new();
    for (entity_id, logical_version) in document_versions {
        if *logical_version == 0 {
            return Err(corruption(
                "text compaction input contains a zero logical version",
            ));
        }
        entity_ids.insert(*entity_id);
    }
    let mut live_versions = HashMap::with_capacity(entity_ids.len());
    let mut observations = Vec::with_capacity(entity_ids.len());
    for entity_id in entity_ids {
        let entity_id = IndexEntityId::new(entity_id);
        let key = scoped_key(
            scope,
            index_keys::ScopedKey::TextEntityState(index_keys::TextEntityStateKey {
                root: index_keys::TextManifestRootKey {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    partition: partition.fingerprint(),
                },
                entity: index_keys::IndexEntity {
                    kind: operation.identity().element_kind(),
                    id: entity_id,
                },
            }),
        );
        let value = transaction.get(&key).await?;
        if let Some(value) = value.as_ref() {
            let state = index_values::decode_text_entity_state(value)?;
            if state.index_id != operation.index_id()
                || state.generation != operation.generation()
                || state.partition != *partition
                || state.entity_kind != operation.identity().element_kind()
                || state.entity_id != entity_id
            {
                return Err(corruption(
                    "text compaction entity-state ownership disagrees with its key",
                ));
            }
            if state.live {
                live_versions.insert(entity_id.get(), state.logical_version.get());
            }
        }
        observations.push(RowObservation { key, value });
    }
    Ok(ResolvedLiveVersions {
        live_versions,
        observations,
    })
}

/// Atomically retires exact replaced artifact metadata.
pub(super) async fn stage_input_retirement(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    input_artifact_keys: &[IndexCursor],
) -> Result<()> {
    if !(2..=MAX_COMPACTION_INPUT_ARTIFACTS).contains(&input_artifact_keys.len()) {
        return Err(corruption(
            "text compaction retirement requires a useful bounded input set",
        ));
    }
    let mut retirements = Vec::with_capacity(input_artifact_keys.len());
    let mut expected_partition = None;
    for cursor in input_artifact_keys {
        let artifact_key = cursor.as_bytes().clone();
        let Some(artifact_value) = transaction.get(&artifact_key).await? else {
            return Err(corruption(
                "text compaction input artifact disappeared before atomic retirement",
            ));
        };
        let (_, artifact) = super::attachment::decode_build_artifact(
            scope,
            operation,
            &artifact_key,
            &artifact_value,
        )?;
        match expected_partition.as_ref() {
            Some(partition) if partition != &artifact.partition => {
                return Err(corruption(
                    "text compaction retirement mixed canonical partitions",
                ));
            }
            Some(_) => {}
            None => expected_partition = Some(artifact.partition.clone()),
        }
        retirements.push(artifact_key);
    }
    for artifact_key in retirements {
        transaction.delete(artifact_key)?;
    }
    Ok(())
}

/// Converts a validated durable split into the unchanged search-layer DTO.
fn runtime_split(split: work::SplitRef) -> crate::search::text::TextSplitRef {
    crate::search::text::TextSplitRef {
        blob: crate::search::text::TextBlobRef {
            sha256: *split.blob().hash(),
            size_bytes: split.blob().size(),
        },
        footer_offset: split.footer_offset(),
        footer_len: split.footer_length(),
        hotcache_len: split.hot_cache_length(),
        total_size_bytes: split.total_size(),
    }
}

/// Measures exact retirement writes for one source artifact.
fn retirement_measurement(
    scope: DataScope,
    _operation: &IndexOperationRecord,
    key: index_keys::TextBuildArtifactKey,
    _artifact: &work::TextBuildArtifactValue,
    _creates_candidate: bool,
) -> (u64, u64) {
    let artifact_key = scoped_key(scope, index_keys::ScopedKey::TextBuildArtifact(key));
    (1, u64::try_from(artifact_key.len()).unwrap_or(u64::MAX))
}

/// Encodes one scoped V2 key through the canonical `encoding/v1` boundary.
fn scoped_key(scope: DataScope, key: index_keys::ScopedKey) -> Bytes {
    Key::Data { scope, kind: key }.to_bytes()
}

fn corruption(reason: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(reason.into())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::index_lifecycle::text::test_support;
    use crate::index_lifecycle::{
        IndexElementKind, IndexEntityId, OperationCounters, TextLogicalVersion,
    };

    #[tokio::test]
    async fn compaction_selection_resolution_and_retirement_are_exact() {
        let db = Db::open("text-compaction-contracts", Arc::new(InMemory::new()))
            .await
            .expect("text compaction test database opens");
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let operation = test_support::operation();
        let scope = DataScope::LegacyUnscoped;
        let partition = work::TextPartition::Unpartitioned;
        let progress = PrefixScanProgress {
            cursor: None,
            counters: OperationCounters::default(),
        };
        let batch = test_support::batch_limits(u64::MAX, u64::MAX, u64::MAX);
        let generous = test_support::compaction_limits(8, 1_024, 2_048, 1_024, 1_024);

        assert!(matches!(
            select_artifacts(&transaction, scope, &operation, &progress, batch, generous,)
                .await
                .unwrap(),
            ArtifactSelection::Exhausted
        ));

        let first = test_support::artifact_row(scope, &operation, partition.clone(), 0, 1);
        let second = test_support::artifact_row(scope, &operation, partition.clone(), 1, 2);
        transaction.put(first.0.clone(), first.1.clone()).unwrap();
        transaction.put(second.0.clone(), second.1.clone()).unwrap();

        let single = select_artifacts(
            &transaction,
            scope,
            &operation,
            &progress,
            batch,
            test_support::compaction_limits(1, 1_024, 2_048, 1_024, 1_024),
        )
        .await
        .unwrap();
        let ArtifactSelection::Advance {
            cursor,
            observation,
        } = single
        else {
            panic!("fan-in one advances the exact first artifact")
        };
        assert_eq!(cursor.as_bytes(), &first.0);
        assert_eq!(observation.value, Some(first.1.clone()));

        assert!(select_artifacts(
            &transaction,
            scope,
            &operation,
            &PrefixScanProgress {
                cursor: Some(IndexCursor::try_new(Bytes::from_static(b"wrong-prefix")).unwrap()),
                counters: OperationCounters::default(),
            },
            batch,
            generous,
        )
        .await
        .is_err());

        let selected =
            select_artifacts(&transaction, scope, &operation, &progress, batch, generous)
                .await
                .unwrap();
        let ArtifactSelection::Compact(selected) = selected else {
            panic!("two same-partition artifacts produce one exact merge")
        };
        assert_eq!(selected.partition, partition);
        assert_eq!(selected.artifact_keys.len(), 2);
        assert_eq!(selected.split_refs.len(), 2);
        assert_eq!(selected.observations.len(), 2);
        assert_eq!(selected.input_blob_bytes, 256);
        assert_eq!(selected.retirement_output_operations, 2);
        assert!(selected.retirement_output_bytes > 0);
        assert!(selected.pruning.may_match_any([b"term-1".as_slice()]));

        assert!(
            resolve_live_versions(&transaction, scope, &operation, &partition, &[(7, 0)],)
                .await
                .is_err()
        );
        let state_key = scoped_key(
            scope,
            index_keys::ScopedKey::TextEntityState(index_keys::TextEntityStateKey {
                root: index_keys::TextManifestRootKey {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    partition: partition.fingerprint(),
                },
                entity: index_keys::IndexEntity {
                    kind: IndexElementKind::Node,
                    id: IndexEntityId::new(7),
                },
            }),
        );
        transaction
            .put(
                state_key,
                index_values::encode_text_entity_state(&work::TextEntityStateValue {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    partition: partition.clone(),
                    entity_kind: IndexElementKind::Node,
                    entity_id: IndexEntityId::new(7),
                    logical_version: TextLogicalVersion::new(3).unwrap(),
                    live: true,
                }),
            )
            .unwrap();
        let resolved = resolve_live_versions(
            &transaction,
            scope,
            &operation,
            &partition,
            &[(7, 1), (7, 3), (8, 1)],
        )
        .await
        .unwrap();
        assert_eq!(resolved.live_versions.get(&7), Some(&3));
        assert!(!resolved.live_versions.contains_key(&8));
        assert_eq!(resolved.observations.len(), 2);

        assert!(stage_input_retirement(&transaction, scope, &operation, &[])
            .await
            .is_err());
        assert!(stage_input_retirement(
            &transaction,
            scope,
            &operation,
            &selected.artifact_keys[..1],
        )
        .await
        .is_err());
        stage_input_retirement(&transaction, scope, &operation, &selected.artifact_keys)
            .await
            .unwrap();
        assert!(transaction.get(&first.0).await.unwrap().is_none());
        assert!(transaction.get(&second.0).await.unwrap().is_none());
    }
}
