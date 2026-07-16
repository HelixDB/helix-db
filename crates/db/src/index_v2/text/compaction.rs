//! Typed artifact selection and retirement for V2 text-build compaction.
//!
//! This module owns the database half of compaction. It scans only the exact
//! generation-qualified build-artifact prefix, admits a same-partition input
//! set under fan-in, immutable-input, temporary-disk, and transaction limits,
//! and point-reads the generation-qualified entity state used to prune stale
//! documents. Object materialization and CPU-heavy merging remain in
//! [`crate::search::text::compaction`], while the driver persists the exact
//! upload child before publishing a replacement.
//!
//! Replaced artifacts are retired atomically with their global reachability
//! entries and operation-owned GC candidates. Candidate creation happens while
//! every input owner is still present, so a later GC pass can never mistake a
//! partially retired source set for an unreferenced blob.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::Bound;

use bytes::Bytes;
use slatedb::DbTransaction;

use crate::config::{SearchIndexBatchLimits, TextBackfillCompactionLimits};
use crate::encoding::v1::keys::index_v2 as index_keys;
use crate::encoding::v1::keys::tenant::DataScope;
use crate::encoding::v1::keys::{DataKeyKind, Key};
use crate::encoding::v1::values::index_v2 as index_values;
use crate::error::{HelixDbError, Result};
use crate::index_v2::work;
use crate::index_v2::{
    IndexCursor, IndexEntityId, IndexOperationRecord, PrefixScanProgress,
    TextCompactionUploadProgress,
};

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
        index_keys::IndexV2Key::generation_prefix(
            index_keys::IndexV2RecordKind::TextBuildArtifact,
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
        .min(TextCompactionUploadProgress::MAX_INPUT_ARTIFACTS);
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
    let mut observations = vec![first_observation];
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
            observation: observations
                .into_iter()
                .next()
                .expect("one decoded artifact has one observation"),
        });
    }
    Ok(ArtifactSelection::Compact(SelectedArtifactBatch {
        partition,
        artifact_keys,
        split_refs,
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
            index_keys::IndexV2Key::TextEntityState(index_keys::TextEntityStateKey {
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
            let index_values::IndexV2WorkValue::TextEntityState(state) =
                index_values::decode_work_value(value)?
            else {
                return Err(corruption(
                    "text compaction entity-state key contains another value kind",
                ));
            };
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

/// Atomically retires exact replaced artifacts into generation-owned candidates.
///
/// All source artifacts, global reachability rows, and candidate collisions are
/// validated before staging any write. This function is used both when every
/// selected document is stale and after a replacement child has attached its
/// exact output artifact.
pub(super) async fn stage_input_retirement(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    input_artifact_keys: &[IndexCursor],
) -> Result<()> {
    if !(2..=TextCompactionUploadProgress::MAX_INPUT_ARTIFACTS).contains(&input_artifact_keys.len())
    {
        return Err(corruption(
            "text compaction retirement requires a useful bounded input set",
        ));
    }
    struct Retirement {
        artifact_key: Bytes,
        reachability_key: Bytes,
    }
    let mut retirements = Vec::with_capacity(input_artifact_keys.len());
    let mut candidates = Vec::new();
    let mut candidate_hashes = HashSet::new();
    let mut expected_partition = None;
    for cursor in input_artifact_keys {
        let artifact_key = cursor.as_bytes().clone();
        let Some(artifact_value) = transaction.get(&artifact_key).await? else {
            return Err(corruption(
                "text compaction input artifact disappeared before atomic retirement",
            ));
        };
        let (typed_key, artifact) = super::attachment::decode_build_artifact(
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
        let (reachability_key, reachability_value) =
            super::attachment::build_artifact_reachability_row(
                artifact.split.blob(),
                scope,
                typed_key,
            );
        if transaction.get(&reachability_key).await?.as_deref() != Some(reachability_value.as_ref())
        {
            return Err(corruption(
                "text compaction input is missing its exact global reachability row",
            ));
        }
        if candidate_hashes.insert(artifact.split.blob().hash) {
            let (candidate_key, candidate_value) =
                generation_candidate_row(scope, operation, artifact.split.blob());
            match transaction.get(&candidate_key).await? {
                Some(existing) if existing != candidate_value => {
                    return Err(corruption(
                        "text compaction candidate key is owned by another value",
                    ));
                }
                Some(_) => {}
                None => candidates.push((candidate_key, candidate_value)),
            }
        }
        retirements.push(Retirement {
            artifact_key,
            reachability_key,
        });
    }
    for (candidate_key, candidate_value) in candidates {
        transaction.put(candidate_key, candidate_value)?;
    }
    for retirement in retirements {
        transaction.delete(retirement.artifact_key)?;
        transaction.delete(retirement.reachability_key)?;
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
    operation: &IndexOperationRecord,
    key: index_keys::TextBuildArtifactKey,
    artifact: &work::TextBuildArtifactValue,
    creates_candidate: bool,
) -> (u64, u64) {
    let artifact_key = scoped_key(scope, index_keys::IndexV2Key::TextBuildArtifact(key));
    let (reachability_key, _) =
        super::attachment::build_artifact_reachability_row(artifact.split.blob(), scope, key);
    let mut operations = 2_u64;
    let mut bytes = u64::try_from(artifact_key.len().saturating_add(reachability_key.len()))
        .unwrap_or(u64::MAX);
    if creates_candidate {
        let (candidate_key, candidate_value) =
            generation_candidate_row(scope, operation, artifact.split.blob());
        operations = operations.saturating_add(1);
        bytes = bytes.saturating_add(
            u64::try_from(candidate_key.len().saturating_add(candidate_value.len()))
                .unwrap_or(u64::MAX),
        );
    }
    (operations, bytes)
}

/// Constructs the exact operation-owned candidate for one replaced blob.
fn generation_candidate_row(
    scope: DataScope,
    operation: &IndexOperationRecord,
    blob: work::BlobRef,
) -> (Bytes, Bytes) {
    let key = scoped_key(
        scope,
        index_keys::IndexV2Key::BlobGcCandidate(index_keys::BlobGcCandidateKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            owner: index_keys::BlobGcCandidateKeyOwner::GenerationCleanup,
            blob_hash: index_keys::BlobHash::new(*blob.hash()),
        }),
    );
    let value = index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcCandidate(
        work::BlobGcCandidateValue {
            owner: work::BlobGcCandidateOwner::GenerationCleanup(operation.operation_id()),
            index_id: operation.index_id(),
            generation: operation.generation(),
            blob,
        },
    ));
    (key, value)
}

/// Encodes one scoped V2 key through the canonical `encoding/v1` boundary.
fn scoped_key(scope: DataScope, key: index_keys::IndexV2Key) -> Bytes {
    Key::Data {
        scope,
        kind: DataKeyKind::IndexV2(key),
    }
    .to_bytes()
}

fn corruption(reason: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(reason.into())
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::index_v2::{
        IndexComponent, IndexElementKind, IndexGenerationId, IndexId, IndexIdentity,
        IndexIdentityFamily, IndexOperationExecutionState, IndexOperationFamily, IndexOperationId,
        IndexOperationKind, IndexOperationProgress, IndexOperationRevision, IndexRevision,
        OperationCounters, TextBuildProgress, TextBuildStage, TextLogicalVersion,
        TextUploadIntentId,
    };

    /// Opens one isolated database for the compaction storage-contract tests.
    async fn test_db(name: &str) -> Db {
        Db::open(name, Arc::new(InMemory::new()))
            .await
            .expect("compaction contract database opens")
    }

    /// Returns one queued text build at the exact compaction checkpoint.
    fn operation() -> IndexOperationRecord {
        let identity = IndexIdentity::new(
            IndexIdentityFamily::Text,
            IndexElementKind::Node,
            IndexComponent::try_new("label", "Document").expect("label component validates"),
            IndexComponent::try_new("property", "body").expect("property component validates"),
        );
        IndexOperationRecord::try_new(
            IndexOperationId::from_bytes([1; 16]).expect("operation ID is non-nil"),
            IndexId::initial(),
            identity,
            IndexGenerationId::initial(),
            IndexRevision::initial(),
            IndexOperationRevision::initial(),
            IndexOperationKind::Build,
            IndexOperationFamily::Text,
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::Compact(PrefixScanProgress {
                    cursor: None,
                    counters: OperationCounters::default(),
                }),
            )),
            0,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .expect("text compaction operation is valid")
    }

    /// Constructs transaction limits with only the output ceilings variable.
    fn batch_limits(max_output_operations: u64, max_output_bytes: u64) -> SearchIndexBatchLimits {
        SearchIndexBatchLimits::try_new(
            NonZeroUsize::MIN,
            NonZeroU64::MAX,
            NonZeroU64::new(max_output_operations).expect("operation limit is positive"),
            NonZeroU64::new(max_output_bytes).expect("output byte limit is positive"),
            NonZeroU64::MIN,
        )
        .expect("test transaction limits are valid")
    }

    /// Constructs independent compaction admission ceilings.
    fn compaction_limits(
        max_fan_in: usize,
        max_input_bytes: u64,
        max_temporary_disk_bytes: u64,
        max_output_blob_bytes: u64,
    ) -> TextBackfillCompactionLimits {
        TextBackfillCompactionLimits::new(
            NonZeroUsize::new(max_fan_in).expect("fan-in is positive"),
            NonZeroU64::new(max_input_bytes).expect("input limit is positive"),
            NonZeroU64::new(max_temporary_disk_bytes).expect("temporary limit is positive"),
            NonZeroU64::new(max_output_blob_bytes).expect("output limit is positive"),
            NonZeroU64::MAX,
        )
    }

    /// Writes one typed artifact and, when requested, its exact reachability row.
    async fn put_artifact(
        db: &Db,
        scope: DataScope,
        operation: &IndexOperationRecord,
        partition: work::TextPartition,
        ordinal: u32,
        blob: work::BlobRef,
        with_reachability: bool,
    ) -> (
        IndexCursor,
        index_keys::TextBuildArtifactKey,
        work::TextBuildArtifactValue,
    ) {
        let key = index_keys::TextBuildArtifactKey {
            root: index_keys::TextManifestRootKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                partition: partition.fingerprint(),
            },
            ordinal,
        };
        let split = work::SplitRef::try_new(blob, 0, 0, 0, blob.size())
            .expect("matching split sizes are valid");
        let value = work::TextBuildArtifactValue {
            index_id: operation.index_id(),
            generation: operation.generation(),
            partition,
            artifact_ordinal: ordinal,
            split,
            source_intent_id: TextUploadIntentId::from_bytes([blob.hash()[0].max(1); 16])
                .expect("source intent ID is non-nil"),
        };
        let encoded_key = scoped_key(scope, index_keys::IndexV2Key::TextBuildArtifact(key));
        db.put(
            encoded_key.clone(),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextBuildArtifact(
                value.clone(),
            )),
        )
        .await
        .expect("artifact row is written");
        if with_reachability {
            let (reachability_key, reachability_value) =
                super::super::attachment::build_artifact_reachability_row(blob, scope, key);
            db.put(reachability_key, reachability_value)
                .await
                .expect("artifact reachability row is written");
        }
        (
            IndexCursor::try_new(encoded_key).expect("typed artifact key is a valid cursor"),
            key,
            value,
        )
    }

    /// Encodes the exact generation-qualified state key for one node.
    fn entity_state_key(
        scope: DataScope,
        operation: &IndexOperationRecord,
        partition: &work::TextPartition,
        entity_id: IndexEntityId,
    ) -> Bytes {
        scoped_key(
            scope,
            index_keys::IndexV2Key::TextEntityState(index_keys::TextEntityStateKey {
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
        )
    }

    /// Extracts the stable reason from a fail-closed compaction error.
    fn corruption_reason(error: HelixDbError) -> String {
        let HelixDbError::IndexCatalogCorruption(reason) = error else {
            panic!("compaction contract returns catalog corruption")
        };
        reason
    }

    #[tokio::test]
    async fn selection_handles_empty_resume_and_single_artifact_progress() {
        let db = test_db("text-compaction-selection-progress").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let progress = PrefixScanProgress {
            cursor: None,
            counters: OperationCounters::default(),
        };
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("empty selection transaction opens");
        assert!(matches!(
            select_artifacts(
                &transaction,
                scope,
                &operation,
                &progress,
                batch_limits(u64::MAX, u64::MAX),
                compaction_limits(4, u64::MAX, u64::MAX, 1),
            )
            .await
            .expect("empty selection succeeds"),
            ArtifactSelection::Exhausted
        ));
        drop(transaction);

        let (cursor, _, _) = put_artifact(
            &db,
            scope,
            &operation,
            work::TextPartition::Unpartitioned,
            1,
            work::BlobRef::new([11; 32], 10),
            false,
        )
        .await;
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("single selection transaction opens");
        let ArtifactSelection::Advance {
            cursor: advanced,
            observation,
        } = select_artifacts(
            &transaction,
            scope,
            &operation,
            &progress,
            batch_limits(u64::MAX, u64::MAX),
            compaction_limits(1, u64::MAX, u64::MAX, 1),
        )
        .await
        .expect("fan-in one advances the final artifact")
        else {
            panic!("one artifact must advance")
        };
        assert_eq!(advanced, cursor);
        assert_eq!(observation.key, *cursor.as_bytes());
        assert!(observation.value.is_some());
        drop(transaction);

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("resumed selection transaction opens");
        assert!(matches!(
            select_artifacts(
                &transaction,
                scope,
                &operation,
                &PrefixScanProgress {
                    cursor: Some(cursor),
                    counters: OperationCounters::default(),
                },
                batch_limits(u64::MAX, u64::MAX),
                compaction_limits(4, u64::MAX, u64::MAX, 1),
            )
            .await
            .expect("strict resume succeeds"),
            ArtifactSelection::Exhausted
        ));
        assert_eq!(
            corruption_reason(
                select_artifacts(
                    &transaction,
                    scope,
                    &operation,
                    &PrefixScanProgress {
                        cursor: Some(
                            IndexCursor::try_new(Bytes::from_static(b"outside-artifact-prefix"))
                                .expect("small invalid cursor is bounded"),
                        ),
                        counters: OperationCounters::default(),
                    },
                    batch_limits(u64::MAX, u64::MAX),
                    compaction_limits(4, u64::MAX, u64::MAX, 1),
                )
                .await
                .expect_err("a cursor from another keyspace fails closed"),
            ),
            "text compaction cursor is outside its exact artifact prefix"
        );
    }

    #[tokio::test]
    async fn selection_applies_input_temporary_transaction_and_shared_blob_limits() {
        let db = test_db("text-compaction-selection-limits").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        for ordinal in [1, 2] {
            put_artifact(
                &db,
                scope,
                &operation,
                work::TextPartition::Unpartitioned,
                ordinal,
                work::BlobRef::new([21; 32], 10),
                false,
            )
            .await;
        }
        let progress = PrefixScanProgress {
            cursor: None,
            counters: OperationCounters::default(),
        };
        for limits in [
            compaction_limits(4, 9, u64::MAX, 1),
            compaction_limits(4, u64::MAX, 10, 10),
        ] {
            let transaction = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .expect("bounded selection transaction opens");
            assert!(matches!(
                select_artifacts(
                    &transaction,
                    scope,
                    &operation,
                    &progress,
                    batch_limits(u64::MAX, u64::MAX),
                    limits,
                )
                .await
                .expect("insufficient input budget advances"),
                ArtifactSelection::Advance { .. }
            ));
        }

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("operation-limited selection transaction opens");
        assert!(matches!(
            select_artifacts(
                &transaction,
                scope,
                &operation,
                &progress,
                batch_limits(2, u64::MAX),
                compaction_limits(4, u64::MAX, u64::MAX, 1),
            )
            .await
            .expect("indivisible retirement advances"),
            ArtifactSelection::Advance { .. }
        ));
        assert!(matches!(
            select_artifacts(
                &transaction,
                scope,
                &operation,
                &progress,
                batch_limits(u64::MAX, 1),
                compaction_limits(4, u64::MAX, u64::MAX, 1),
            )
            .await
            .expect("indivisible retirement bytes advance"),
            ArtifactSelection::Advance { .. }
        ));
        let ArtifactSelection::Compact(selected) = select_artifacts(
            &transaction,
            scope,
            &operation,
            &progress,
            batch_limits(5, u64::MAX),
            compaction_limits(4, u64::MAX, u64::MAX, 1),
        )
        .await
        .expect("one shared candidate keeps two retirements within five operations") else {
            panic!("two shared-blob artifacts form one useful merge")
        };
        assert_eq!(selected.partition, work::TextPartition::Unpartitioned);
        assert_eq!(selected.artifact_keys.len(), 2);
        assert_eq!(selected.split_refs.len(), 2);
        assert_eq!(selected.observations.len(), 2);
        assert_eq!(selected.input_blob_bytes, 20);
        assert_eq!(selected.retirement_output_operations, 5);
        assert!(selected.retirement_output_bytes > 0);
    }

    #[tokio::test]
    async fn selection_stops_before_the_next_input_and_rejects_size_overflow() {
        let db = test_db("text-compaction-selection-next-input").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        for (ordinal, hash) in [(1, 31), (2, 32)] {
            put_artifact(
                &db,
                scope,
                &operation,
                work::TextPartition::Unpartitioned,
                ordinal,
                work::BlobRef::new([hash; 32], 10),
                false,
            )
            .await;
        }
        let progress = PrefixScanProgress {
            cursor: None,
            counters: OperationCounters::default(),
        };
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("next-input selection transaction opens");
        assert!(matches!(
            select_artifacts(
                &transaction,
                scope,
                &operation,
                &progress,
                batch_limits(5, u64::MAX),
                compaction_limits(4, 19, u64::MAX, 1),
            )
            .await
            .expect("second input exceeding bytes leaves one final artifact"),
            ArtifactSelection::Advance { .. }
        ));
        assert!(matches!(
            select_artifacts(
                &transaction,
                scope,
                &operation,
                &progress,
                batch_limits(5, u64::MAX),
                compaction_limits(4, u64::MAX, u64::MAX, 1),
            )
            .await
            .expect("second distinct candidate exceeding operations leaves one artifact"),
            ArtifactSelection::Advance { .. }
        ));
        drop(transaction);

        let overflow_db = test_db("text-compaction-selection-overflow").await;
        for (ordinal, hash, size) in [(1, 41, u64::MAX - 1), (2, 42, 2)] {
            put_artifact(
                &overflow_db,
                scope,
                &operation,
                work::TextPartition::Unpartitioned,
                ordinal,
                work::BlobRef::new([hash; 32], size),
                false,
            )
            .await;
        }
        let transaction = overflow_db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("overflow selection transaction opens");
        assert_eq!(
            corruption_reason(
                select_artifacts(
                    &transaction,
                    scope,
                    &operation,
                    &progress,
                    batch_limits(u64::MAX, u64::MAX),
                    compaction_limits(4, u64::MAX, u64::MAX, 1),
                )
                .await
                .expect_err("overflowing aggregate input fails closed"),
            ),
            "text compaction input bytes overflowed"
        );

        let partition_db = test_db("text-compaction-selection-partition-boundary").await;
        let unpartitioned = work::TextPartition::Unpartitioned;
        let tenant = work::TextPartition::try_tenant_value(Bytes::from_static(b"tenant-b"))
            .expect("tenant partition validates");
        let (first_partition, second_partition) =
            if unpartitioned.fingerprint() < tenant.fingerprint() {
                (unpartitioned, tenant)
            } else {
                (tenant, unpartitioned)
            };
        put_artifact(
            &partition_db,
            scope,
            &operation,
            first_partition,
            1,
            work::BlobRef::new([43; 32], 10),
            false,
        )
        .await;
        put_artifact(
            &partition_db,
            scope,
            &operation,
            second_partition,
            2,
            work::BlobRef::new([44; 32], 10),
            false,
        )
        .await;
        let transaction = partition_db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("partition-boundary transaction opens");
        assert!(matches!(
            select_artifacts(
                &transaction,
                scope,
                &operation,
                &progress,
                batch_limits(u64::MAX, u64::MAX),
                compaction_limits(4, u64::MAX, u64::MAX, 1),
            )
            .await
            .expect("a new partition ends the current selection"),
            ArtifactSelection::Advance { .. }
        ));
    }

    #[tokio::test]
    async fn selection_rejects_wrong_kind_and_mismatched_artifacts() {
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let progress = PrefixScanProgress {
            cursor: None,
            counters: OperationCounters::default(),
        };
        let db = test_db("text-compaction-mismatched-artifact").await;
        let (cursor, _, mut value) = put_artifact(
            &db,
            scope,
            &operation,
            work::TextPartition::Unpartitioned,
            1,
            work::BlobRef::new([51; 32], 10),
            false,
        )
        .await;
        value.artifact_ordinal = 2;
        db.put(
            cursor.as_bytes().clone(),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextBuildArtifact(
                value,
            )),
        )
        .await
        .expect("mismatched artifact value is installed");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("corrupt selection transaction opens");
        assert_eq!(
            corruption_reason(
                select_artifacts(
                    &transaction,
                    scope,
                    &operation,
                    &progress,
                    batch_limits(u64::MAX, u64::MAX),
                    compaction_limits(4, u64::MAX, u64::MAX, 1),
                )
                .await
                .expect_err("mismatched artifact fails closed"),
            ),
            "text build artifact key/value ownership mismatch"
        );

        let db = test_db("text-compaction-wrong-artifact-value").await;
        let (cursor, _, value) = put_artifact(
            &db,
            scope,
            &operation,
            work::TextPartition::Unpartitioned,
            1,
            work::BlobRef::new([52; 32], 10),
            false,
        )
        .await;
        db.put(
            cursor.as_bytes().clone(),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcCandidate(
                work::BlobGcCandidateValue {
                    owner: work::BlobGcCandidateOwner::GenerationCleanup(operation.operation_id()),
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    blob: value.split.blob(),
                },
            )),
        )
        .await
        .expect("wrong artifact value kind is installed");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("wrong-kind selection transaction opens");
        assert_eq!(
            corruption_reason(
                select_artifacts(
                    &transaction,
                    scope,
                    &operation,
                    &progress,
                    batch_limits(u64::MAX, u64::MAX),
                    compaction_limits(4, u64::MAX, u64::MAX, 1),
                )
                .await
                .expect_err("wrong artifact value kind fails closed"),
            ),
            "text artifact key contains another typed value kind"
        );
    }

    #[tokio::test]
    async fn live_version_resolution_deduplicates_and_filters_authoritative_state() {
        let db = test_db("text-compaction-live-versions").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let partition = work::TextPartition::Unpartitioned;
        for (entity, version, live) in [(1, 3, true), (2, 4, false)] {
            let entity_id = IndexEntityId::new(entity);
            db.put(
                entity_state_key(scope, &operation, &partition, entity_id),
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextEntityState(
                    work::TextEntityStateValue {
                        index_id: operation.index_id(),
                        generation: operation.generation(),
                        partition: partition.clone(),
                        entity_kind: operation.identity().element_kind(),
                        entity_id,
                        logical_version: TextLogicalVersion::new(version)
                            .expect("logical version is non-zero"),
                        live,
                    },
                )),
            )
            .await
            .expect("entity state is written");
        }
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("live-version transaction opens");
        let resolved = resolve_live_versions(
            &transaction,
            scope,
            &operation,
            &partition,
            &[(1, 1), (1, 2), (2, 1), (3, 1)],
        )
        .await
        .expect("authoritative state resolves");
        assert_eq!(resolved.live_versions, HashMap::from([(1, 3)]));
        assert_eq!(resolved.observations.len(), 3);
        assert!(resolved.observations[2].value.is_none());
        assert_eq!(
            corruption_reason(
                resolve_live_versions(&transaction, scope, &operation, &partition, &[(1, 0)],)
                    .await
                    .expect_err("zero source version fails closed"),
            ),
            "text compaction input contains a zero logical version"
        );
    }

    #[tokio::test]
    async fn live_version_resolution_rejects_wrong_kind_and_ownership() {
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let partition = work::TextPartition::Unpartitioned;
        let entity_id = IndexEntityId::new(1);
        let key = entity_state_key(scope, &operation, &partition, entity_id);

        let wrong_kind_db = test_db("text-compaction-state-wrong-kind").await;
        let blob = work::BlobRef::new([61; 32], 10);
        wrong_kind_db
            .put(
                key.clone(),
                index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcCandidate(
                    work::BlobGcCandidateValue {
                        owner: work::BlobGcCandidateOwner::GenerationCleanup(
                            operation.operation_id(),
                        ),
                        index_id: operation.index_id(),
                        generation: operation.generation(),
                        blob,
                    },
                )),
            )
            .await
            .expect("wrong state value kind is written");
        let transaction = wrong_kind_db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("wrong state transaction opens");
        assert_eq!(
            corruption_reason(
                resolve_live_versions(&transaction, scope, &operation, &partition, &[(1, 1)])
                    .await
                    .expect_err("wrong state value kind fails closed"),
            ),
            "text compaction entity-state key contains another value kind"
        );
        drop(transaction);

        let ownership_db = test_db("text-compaction-state-wrong-owner").await;
        ownership_db
            .put(
                key,
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextEntityState(
                    work::TextEntityStateValue {
                        index_id: operation.index_id(),
                        generation: operation.generation(),
                        partition: partition.clone(),
                        entity_kind: operation.identity().element_kind(),
                        entity_id: IndexEntityId::new(2),
                        logical_version: TextLogicalVersion::initial(),
                        live: true,
                    },
                )),
            )
            .await
            .expect("wrong state owner is written");
        let transaction = ownership_db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("wrong owner transaction opens");
        assert_eq!(
            corruption_reason(
                resolve_live_versions(&transaction, scope, &operation, &partition, &[(1, 1)])
                    .await
                    .expect_err("wrong state ownership fails closed"),
            ),
            "text compaction entity-state ownership disagrees with its key"
        );
    }

    #[tokio::test]
    async fn retirement_is_idempotent_for_one_shared_candidate() {
        let db = test_db("text-compaction-retirement-idempotent").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let mut cursors = Vec::new();
        let mut reachability_keys = Vec::new();
        let mut blob = None;
        for ordinal in [1, 2] {
            let (cursor, key, value) = put_artifact(
                &db,
                scope,
                &operation,
                work::TextPartition::Unpartitioned,
                ordinal,
                work::BlobRef::new([71; 32], 10),
                true,
            )
            .await;
            cursors.push(cursor);
            reachability_keys.push(
                super::super::attachment::build_artifact_reachability_row(
                    value.split.blob(),
                    scope,
                    key,
                )
                .0,
            );
            blob = Some(value.split.blob());
        }
        let (candidate_key, candidate_value) = generation_candidate_row(
            scope,
            &operation,
            blob.expect("shared artifact blob exists"),
        );
        db.put(candidate_key.clone(), candidate_value.clone())
            .await
            .expect("exact candidate replay is installed");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("retirement transaction opens");
        stage_input_retirement(&transaction, scope, &operation, &cursors)
            .await
            .expect("exact candidate replay is idempotent");
        transaction
            .commit()
            .await
            .expect("retirement transaction commits");
        for cursor in cursors {
            assert!(db
                .get(cursor.as_bytes())
                .await
                .expect("artifact lookup succeeds")
                .is_none());
        }
        for key in reachability_keys {
            assert!(db
                .get(key)
                .await
                .expect("reachability lookup succeeds")
                .is_none());
        }
        assert_eq!(
            db.get(candidate_key)
                .await
                .expect("candidate lookup succeeds"),
            Some(candidate_value)
        );
    }

    #[tokio::test]
    async fn retirement_rejects_invalid_cardinality_and_prevalidates_all_rows() {
        let db = test_db("text-compaction-retirement-prevalidation").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let (first, _, first_value) = put_artifact(
            &db,
            scope,
            &operation,
            work::TextPartition::Unpartitioned,
            1,
            work::BlobRef::new([81; 32], 10),
            true,
        )
        .await;
        let missing = IndexCursor::try_new(scoped_key(
            scope,
            index_keys::IndexV2Key::TextBuildArtifact(index_keys::TextBuildArtifactKey {
                root: index_keys::TextManifestRootKey {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    partition: work::TextPartition::Unpartitioned.fingerprint(),
                },
                ordinal: 2,
            }),
        ))
        .expect("missing artifact key is a bounded cursor");
        let wrong_key = entity_state_key(
            scope,
            &operation,
            &work::TextPartition::Unpartitioned,
            IndexEntityId::new(99),
        );
        db.put(
            wrong_key.clone(),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextBuildArtifact(
                first_value.clone(),
            )),
        )
        .await
        .expect("wrong typed key fixture is written");
        let wrong_cursor =
            IndexCursor::try_new(wrong_key).expect("wrong typed key is a bounded cursor");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("retirement validation transaction opens");
        assert_eq!(
            corruption_reason(
                stage_input_retirement(&transaction, scope, &operation, &[])
                    .await
                    .expect_err("empty retirement fails closed"),
            ),
            "text compaction retirement requires a useful bounded input set"
        );
        assert_eq!(
            corruption_reason(
                stage_input_retirement(
                    &transaction,
                    scope,
                    &operation,
                    &[wrong_cursor, first.clone()],
                )
                .await
                .expect_err("another typed key kind fails closed"),
            ),
            "text artifact prefix yielded another typed key kind"
        );
        assert_eq!(
            corruption_reason(
                stage_input_retirement(
                    &transaction,
                    scope,
                    &operation,
                    std::slice::from_ref(&first),
                )
                .await
                .expect_err("one-input retirement fails closed"),
            ),
            "text compaction retirement requires a useful bounded input set"
        );
        let oversized = vec![first.clone(); TextCompactionUploadProgress::MAX_INPUT_ARTIFACTS + 1];
        assert_eq!(
            corruption_reason(
                stage_input_retirement(&transaction, scope, &operation, &oversized)
                    .await
                    .expect_err("oversized retirement fails closed"),
            ),
            "text compaction retirement requires a useful bounded input set"
        );
        assert_eq!(
            corruption_reason(
                stage_input_retirement(&transaction, scope, &operation, &[first.clone(), missing],)
                    .await
                    .expect_err("missing later artifact fails before writes"),
            ),
            "text compaction input artifact disappeared before atomic retirement"
        );
        drop(transaction);
        assert!(db
            .get(first.as_bytes())
            .await
            .expect("first artifact remains readable")
            .is_some());
        let (candidate_key, _) =
            generation_candidate_row(scope, &operation, first_value.split.blob());
        assert!(db
            .get(candidate_key)
            .await
            .expect("candidate absence is readable")
            .is_none());
    }

    #[tokio::test]
    async fn retirement_rejects_mixed_partitions_missing_references_and_candidate_conflicts() {
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let tenant = work::TextPartition::try_tenant_value(Bytes::from_static(b"tenant-a"))
            .expect("tenant partition validates");

        let mixed_db = test_db("text-compaction-retirement-mixed-partitions").await;
        let (first, _, _) = put_artifact(
            &mixed_db,
            scope,
            &operation,
            work::TextPartition::Unpartitioned,
            1,
            work::BlobRef::new([91; 32], 10),
            true,
        )
        .await;
        let (second, _, _) = put_artifact(
            &mixed_db,
            scope,
            &operation,
            tenant,
            2,
            work::BlobRef::new([92; 32], 10),
            true,
        )
        .await;
        let transaction = mixed_db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("mixed-partition transaction opens");
        assert_eq!(
            corruption_reason(
                stage_input_retirement(&transaction, scope, &operation, &[first, second])
                    .await
                    .expect_err("mixed partitions fail closed"),
            ),
            "text compaction retirement mixed canonical partitions"
        );
        drop(transaction);

        let missing_reference_db = test_db("text-compaction-retirement-missing-reference").await;
        let (first, _, first_value) = put_artifact(
            &missing_reference_db,
            scope,
            &operation,
            work::TextPartition::Unpartitioned,
            1,
            work::BlobRef::new([93; 32], 10),
            true,
        )
        .await;
        let (second, _, _) = put_artifact(
            &missing_reference_db,
            scope,
            &operation,
            work::TextPartition::Unpartitioned,
            2,
            work::BlobRef::new([94; 32], 10),
            false,
        )
        .await;
        let transaction = missing_reference_db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("missing-reference transaction opens");
        assert_eq!(
            corruption_reason(
                stage_input_retirement(&transaction, scope, &operation, &[first.clone(), second],)
                    .await
                    .expect_err("missing exact reachability fails closed"),
            ),
            "text compaction input is missing its exact global reachability row"
        );
        drop(transaction);
        let (candidate_key, _) =
            generation_candidate_row(scope, &operation, first_value.split.blob());
        assert!(missing_reference_db
            .get(candidate_key)
            .await
            .expect("candidate absence is readable")
            .is_none());

        let conflict_db = test_db("text-compaction-retirement-candidate-conflict").await;
        let mut cursors = Vec::new();
        let mut first_blob = None;
        for (ordinal, hash) in [(1, 95), (2, 96)] {
            let (cursor, _, value) = put_artifact(
                &conflict_db,
                scope,
                &operation,
                work::TextPartition::Unpartitioned,
                ordinal,
                work::BlobRef::new([hash; 32], 10),
                true,
            )
            .await;
            cursors.push(cursor);
            first_blob.get_or_insert(value.split.blob());
        }
        let (candidate_key, _) = generation_candidate_row(
            scope,
            &operation,
            first_blob.expect("first conflict blob exists"),
        );
        conflict_db
            .put(
                candidate_key,
                Bytes::from_static(b"foreign-candidate-value"),
            )
            .await
            .expect("foreign candidate value is written");
        let transaction = conflict_db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("candidate-conflict transaction opens");
        assert_eq!(
            corruption_reason(
                stage_input_retirement(&transaction, scope, &operation, &cursors)
                    .await
                    .expect_err("candidate owner conflict fails closed"),
            ),
            "text compaction candidate key is owned by another value"
        );
    }
}
