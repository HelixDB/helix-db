//! Bounded, restartable proof of prepared V2 text manifests.
//!
//! Validation runs after every build artifact has been relocated into immutable
//! manifest pages and before the canonical index can become `Active`. Each
//! call observes exactly one page, root, or upload intent, or proves one lane
//! exhausted. The returned preparation retains exact range and point reads so
//! the serializable operation transaction can reject stale physical state.
//!
//! Page selection deliberately performs no object-store or coordinator work.
//! The driver acquires those runtime guards only after the database snapshot is
//! dropped and retains them through the commit that advances the checkpoint.

use std::collections::HashSet;
use std::ops::Bound;

use bytes::Bytes;
use slatedb::DbTransaction;

use crate::config::SearchIndexBatchLimits;
use crate::encoding::v1::keys::index_v2 as index_keys;
use crate::encoding::v1::keys::tenant::DataScope;
use crate::encoding::v1::keys::{DataKeyKind, Key};
use crate::encoding::v1::values::index_v2 as index_values;
use crate::error::{HelixDbError, Result};
use crate::index_v2::outbox::IndexOperationStepResult;
use crate::index_v2::work;
use crate::index_v2::{
    IndexCursor, IndexOperationBlocker, IndexOperationProgress, IndexOperationRecord,
    OperationCounters, PrefixScanProgress, TextBuildProgress, TextBuildStage,
    TextManifestPageValidationProgress, TextManifestPartitionValidation, TextManifestRevision,
    TextManifestValidationProgress, TextPartition, ValidatedTextIndexDefinition,
};

/// One prepared validation decision that needs no external blob authority.
#[derive(Debug)]
pub(crate) struct PreparedDatabaseValidation {
    ranges: Vec<PreparedValidationRange>,
    observations: Vec<RowObservation>,
    result: IndexOperationStepResult,
}

impl PreparedDatabaseValidation {
    /// Revalidates every exact range and point observation before returning its result.
    pub(super) async fn stage(
        &self,
        transaction: &DbTransaction,
    ) -> Result<IndexOperationStepResult> {
        for range in &self.ranges {
            if !range.is_current(transaction).await? {
                return Ok(IndexOperationStepResult::TransientFailure);
            }
        }
        for observation in &self.observations {
            if transaction.get(&observation.key).await? != observation.value {
                return Ok(IndexOperationStepResult::TransientFailure);
            }
        }
        Ok(self.result.clone())
    }
}

/// One page proof whose distinct blobs still need runtime reference guards.
#[derive(Debug)]
pub(crate) struct PreparedPageValidation {
    database: PreparedDatabaseValidation,
    blobs: Vec<work::BlobRef>,
}

impl PreparedPageValidation {
    /// Borrows every distinct page blob requiring a coordinator and size proof.
    pub(super) fn blobs(&self) -> &[work::BlobRef] {
        &self.blobs
    }

    /// Revalidates the database proof while the caller retains blob authority.
    pub(super) async fn stage(
        &self,
        transaction: &DbTransaction,
    ) -> Result<IndexOperationStepResult> {
        self.database.stage(transaction).await
    }

    /// Converts a failed external proof into a range-fenced database result.
    pub(super) fn into_database_with_result(
        mut self,
        result: IndexOperationStepResult,
    ) -> PreparedDatabaseValidation {
        self.database.result = result;
        self.database
    }
}

/// Closed preparation shape for one validation checkpoint.
#[derive(Debug)]
pub(super) enum ValidationSelection {
    /// Root, intent, exhaustion, or a durable invariant blocker.
    Database(PreparedDatabaseValidation),
    /// Valid page metadata that still needs external blob validation.
    Page(PreparedPageValidation),
}

/// Exact ordered rows retained for serializable range revalidation.
#[derive(Debug)]
struct PreparedValidationRange {
    prefix: Bytes,
    start: Bound<Bytes>,
    end: Bound<Bytes>,
    rows: Vec<(Bytes, Bytes)>,
}

impl PreparedValidationRange {
    /// Replays one selected or exhausted interval inside the commit transaction.
    async fn is_current(&self, transaction: &DbTransaction) -> Result<bool> {
        let bounds = (self.start.clone(), self.end.clone());
        let mut current = transaction.scan_prefix(&self.prefix, bounds).await?;
        for (expected_key, expected_value) in &self.rows {
            let Some(row) = current.next().await? else {
                return Ok(false);
            };
            if row.key != expected_key || row.value != expected_value {
                return Ok(false);
            }
        }
        Ok(current.next().await?.is_none())
    }
}

/// Exact point-read retained with one prepared validation result.
#[derive(Debug)]
struct RowObservation {
    key: Bytes,
    value: Option<Bytes>,
}

/// Selects one bounded validation checkpoint from the current closed lane.
pub(super) async fn select(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    definition: &ValidatedTextIndexDefinition,
    progress: &TextManifestValidationProgress,
    limits: SearchIndexBatchLimits,
) -> Result<ValidationSelection> {
    match progress {
        TextManifestValidationProgress::Pages(progress) => {
            select_page(transaction, scope, operation, progress, limits).await
        }
        TextManifestValidationProgress::Roots(progress) => {
            select_root(transaction, scope, operation, definition, progress, limits).await
        }
        TextManifestValidationProgress::UploadIntents(progress) => {
            select_upload_intent(transaction, scope, operation, progress, limits).await
        }
    }
}

/// Validates one immutable page and its exact root/reachability relationships.
async fn select_page(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &TextManifestPageValidationProgress,
    limits: SearchIndexBatchLimits,
) -> Result<ValidationSelection> {
    let prefix = generation_prefix(
        scope,
        index_keys::IndexV2RecordKind::TextManifestPage,
        operation,
    );
    let (range, row) = select_one(transaction, prefix, progress.cursor()).await?;
    let Some((row_key, row_value)) = row else {
        let result = if progress.partition().is_some() {
            IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation)
        } else {
            progressed(TextBuildStage::ValidateManifests(
                TextManifestValidationProgress::Roots(PrefixScanProgress {
                    cursor: None,
                    counters: progress.counters(),
                }),
            ))
        };
        return Ok(ValidationSelection::Database(PreparedDatabaseValidation {
            ranges: vec![range],
            observations: Vec::new(),
            result,
        }));
    };

    let page_key = match Key::parse_from_slice(scope, &row_key) {
        Ok(Key::Data {
            kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::TextManifestPage(key)),
            ..
        }) => key,
        Ok(Key::Data { .. } | Key::Global { .. }) | Err(_) => {
            return Ok(blocked_database(vec![range], Vec::new()));
        }
    };
    let page = match index_values::decode_work_value(&row_value) {
        Ok(index_values::IndexV2WorkValue::TextManifestPage(page)) => page,
        Ok(_) | Err(_) => return Ok(blocked_database(vec![range], Vec::new())),
    };
    let root_key = scoped_key(
        scope,
        index_keys::IndexV2Key::TextManifestRoot(page_key.root),
    );
    let root_value = transaction.get(&root_key).await?;
    let mut observations = vec![RowObservation {
        key: root_key,
        value: root_value.clone(),
    }];
    let Some(root_value) = root_value.as_ref() else {
        return Ok(blocked_database(vec![range], observations));
    };
    let root = match index_values::decode_work_value(root_value) {
        Ok(index_values::IndexV2WorkValue::TextManifestRoot(root)) => root,
        Ok(_) | Err(_) => return Ok(blocked_database(vec![range], observations)),
    };

    let expected_revision = u64::from(root.page_count()).saturating_add(1);
    if page_key.root.index_id != operation.index_id()
        || page_key.root.generation != operation.generation()
        || page.index_id() != operation.index_id()
        || page.generation() != operation.generation()
        || page_key.root.partition != page.partition().fingerprint()
        || page_key.page != page.page()
        || root.index_id() != operation.index_id()
        || root.generation() != operation.generation()
        || page_key.root.partition != root.partition().fingerprint()
        || root.partition() != page.partition()
        || root.page_count() == 0
        || root.split_count() == 0
        || root.revision().get() != expected_revision
        || page_key.page >= root.page_count()
    {
        return Ok(blocked_database(vec![range], observations));
    }

    let observed_before = match progress.partition() {
        Some(partition)
            if partition.partition_fingerprint() == page_key.root.partition.as_bytes()
                && partition.root_revision() == root.revision()
                && partition.page_count() == root.page_count()
                && partition.split_count() == root.split_count()
                && partition.next_page() == page_key.page =>
        {
            partition.observed_split_count()
        }
        None if page_key.page == 0 => 0,
        Some(_) | None => return Ok(blocked_database(vec![range], observations)),
    };
    let page_split_count =
        u64::try_from(page.entries().len()).expect("bounded manifest-page length fits u64");
    // Both values are bounded by a valid root's page count times MAX_ENTRIES.
    let observed_split_count = observed_before + page_split_count;
    // `page < root.page_count <= u32::MAX` proves this addition cannot overflow.
    let next_page = page_key.page + 1;
    let next_partition = if next_page == root.page_count() {
        if observed_split_count != root.split_count() {
            return Ok(blocked_database(vec![range], observations));
        }
        None
    } else {
        let Ok(partition) = TextManifestPartitionValidation::try_new(
            *page_key.root.partition.as_bytes(),
            root.revision(),
            root.page_count(),
            root.split_count(),
            next_page,
            observed_split_count,
        ) else {
            return Ok(blocked_database(vec![range], observations));
        };
        Some(partition)
    };

    let mut blobs = Vec::with_capacity(page.entries().len());
    let mut distinct_blobs = HashSet::with_capacity(page.entries().len());
    for (slot, split) in page.entries().iter().copied().enumerate() {
        let blob = split.blob();
        if !crate::search::text::split_reference_layout_is_exact(
            split.footer_offset(),
            split.footer_length(),
            split.hot_cache_length(),
            split.total_size(),
        ) || !distinct_blobs.insert(blob)
        {
            return Ok(blocked_database(vec![range], observations));
        }
        let slot = u32::try_from(slot).expect("bounded manifest-page slot fits u32");
        let (reference_key, reference_value) =
            super::attachment::manifest_page_reachability_row(blob, scope, page_key, slot);
        let observed_reference = transaction.get(&reference_key).await?;
        let exact_reference = observed_reference.as_deref() == Some(reference_value.as_ref());
        observations.push(RowObservation {
            key: reference_key,
            value: observed_reference,
        });
        if !exact_reference {
            return Ok(blocked_database(vec![range], observations));
        }
        blobs.push(blob);
    }

    let input_bytes = row_bytes(&row_key, Some(&row_value)).saturating_add(
        observations.iter().fold(0_u64, |bytes, observation| {
            bytes.saturating_add(row_bytes(&observation.key, observation.value.as_ref()))
        }),
    );
    if input_bytes > limits.max_input_bytes().get() {
        return Ok(ValidationSelection::Database(PreparedDatabaseValidation {
            ranges: vec![range],
            observations,
            result: IndexOperationStepResult::Blocked(IndexOperationBlocker::ManifestLimit {
                partition: page.partition().clone(),
                observed: input_bytes,
                limit: limits.max_input_bytes().get(),
            }),
        }));
    }
    let Some(input_bytes) = progress.counters().input_bytes.checked_add(input_bytes) else {
        return Err(corruption(
            "text manifest-validation input counter overflowed",
        ));
    };
    let counters = OperationCounters {
        input_bytes,
        ..progress.counters()
    };
    let cursor = IndexCursor::try_new(row_key)
        .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?;
    let next = TextManifestPageValidationProgress::try_new(Some(cursor), next_partition, counters)
        .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?;
    Ok(ValidationSelection::Page(PreparedPageValidation {
        database: PreparedDatabaseValidation {
            ranges: vec![range],
            observations,
            result: progressed(TextBuildStage::ValidateManifests(
                TextManifestValidationProgress::Pages(next),
            )),
        },
        blobs,
    }))
}

/// Validates one manifest root, including the canonical empty representation.
async fn select_root(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    definition: &ValidatedTextIndexDefinition,
    progress: &PrefixScanProgress,
    limits: SearchIndexBatchLimits,
) -> Result<ValidationSelection> {
    let prefix = generation_prefix(
        scope,
        index_keys::IndexV2RecordKind::TextManifestRoot,
        operation,
    );
    let (range, row) = select_one(transaction, prefix, progress.cursor.as_ref()).await?;
    let Some((row_key, row_value)) = row else {
        return Ok(ValidationSelection::Database(PreparedDatabaseValidation {
            ranges: vec![range],
            observations: Vec::new(),
            result: progressed(TextBuildStage::ValidateManifests(
                TextManifestValidationProgress::UploadIntents(PrefixScanProgress {
                    cursor: None,
                    counters: progress.counters,
                }),
            )),
        }));
    };
    let root_key = match Key::parse_from_slice(scope, &row_key) {
        Ok(Key::Data {
            kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::TextManifestRoot(key)),
            ..
        }) => key,
        Ok(Key::Data { .. } | Key::Global { .. }) | Err(_) => {
            return Ok(blocked_database(vec![range], Vec::new()));
        }
    };
    let root = match index_values::decode_work_value(&row_value) {
        Ok(index_values::IndexV2WorkValue::TextManifestRoot(root)) => root,
        Ok(_) | Err(_) => return Ok(blocked_database(vec![range], Vec::new())),
    };
    let partition_mode_is_valid = match (definition.tenant_property(), root.partition()) {
        (None, TextPartition::Unpartitioned) | (Some(_), TextPartition::TenantValue(_)) => true,
        (None, TextPartition::TenantValue(_)) | (Some(_), TextPartition::Unpartitioned) => false,
    };
    let revision_is_valid = if root.page_count() == 0 {
        root.revision() == TextManifestRevision::initial() && root.split_count() == 0
    } else {
        root.revision().get() == u64::from(root.page_count()).saturating_add(1)
            && root.split_count() != 0
    };
    if root_key.index_id != operation.index_id()
        || root_key.generation != operation.generation()
        || root.index_id() != operation.index_id()
        || root.generation() != operation.generation()
        || root_key.partition != root.partition().fingerprint()
        || !partition_mode_is_valid
        || !revision_is_valid
    {
        return Ok(blocked_database(vec![range], Vec::new()));
    }

    let mut observations = Vec::new();
    if root.page_count() != 0 {
        let page_key = scoped_key(
            scope,
            index_keys::IndexV2Key::TextManifestPage(index_keys::TextManifestPageKey {
                root: root_key,
                page: 0,
            }),
        );
        let page_value = transaction.get(&page_key).await?;
        let exact_page_zero = page_value.as_ref().is_some_and(|value| {
            matches!(
                index_values::decode_work_value(value),
                Ok(index_values::IndexV2WorkValue::TextManifestPage(page))
                    if page.index_id() == operation.index_id()
                        && page.generation() == operation.generation()
                        && page.partition() == root.partition()
                        && page.page() == 0
            )
        });
        observations.push(RowObservation {
            key: page_key,
            value: page_value,
        });
        if !exact_page_zero {
            return Ok(blocked_database(vec![range], observations));
        }
    }
    let input_bytes = row_bytes(&row_key, Some(&row_value)).saturating_add(
        observations.iter().fold(0_u64, |bytes, observation| {
            bytes.saturating_add(row_bytes(&observation.key, observation.value.as_ref()))
        }),
    );
    if input_bytes > limits.max_input_bytes().get() {
        return Ok(ValidationSelection::Database(PreparedDatabaseValidation {
            ranges: vec![range],
            observations,
            result: IndexOperationStepResult::Blocked(IndexOperationBlocker::ManifestLimit {
                partition: root.partition().clone(),
                observed: input_bytes,
                limit: limits.max_input_bytes().get(),
            }),
        }));
    }
    let Some(input_bytes) = progress.counters.input_bytes.checked_add(input_bytes) else {
        return Err(corruption("text root-validation input counter overflowed"));
    };
    let counters = OperationCounters {
        input_bytes,
        ..progress.counters
    };
    Ok(ValidationSelection::Database(PreparedDatabaseValidation {
        ranges: vec![range],
        observations,
        result: progressed(TextBuildStage::ValidateManifests(
            TextManifestValidationProgress::Roots(PrefixScanProgress {
                cursor: Some(
                    IndexCursor::try_new(row_key)
                        .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?,
                ),
                counters,
            }),
        )),
    }))
}

/// Validates one retained reclaim intent or proves activation prerequisites absent.
async fn select_upload_intent(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &PrefixScanProgress,
    limits: SearchIndexBatchLimits,
) -> Result<ValidationSelection> {
    let prefix = generation_prefix(
        scope,
        index_keys::IndexV2RecordKind::TextUploadIntent,
        operation,
    );
    let (intent_range, row) = select_one(transaction, prefix, progress.cursor.as_ref()).await?;
    let Some((row_key, row_value)) = row else {
        return select_activation_prerequisites(
            transaction,
            scope,
            operation,
            progress.counters,
            intent_range,
        )
        .await;
    };
    let intent_key = match Key::parse_from_slice(scope, &row_key) {
        Ok(Key::Data {
            kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::TextUploadIntent(key)),
            ..
        }) => key,
        Ok(Key::Data { .. } | Key::Global { .. }) | Err(_) => {
            return Ok(blocked_database(vec![intent_range], Vec::new()));
        }
    };
    let intent = match index_values::decode_work_value(&row_value) {
        Ok(index_values::IndexV2WorkValue::TextUploadIntent(intent)) => intent,
        Ok(_) | Err(_) => return Ok(blocked_database(vec![intent_range], Vec::new())),
    };
    let owner_is_exact = matches!(
        intent.owner,
        work::TextUploadOwner::Build { operation_id, .. }
            if operation_id == operation.operation_id()
    );
    if intent_key.index_id != operation.index_id()
        || intent_key.generation != operation.generation()
        || intent_key.intent_id != intent.intent_id
        || intent.index_id != operation.index_id()
        || intent.identity != *operation.identity()
        || intent.generation != operation.generation()
        || !owner_is_exact
        || !matches!(intent.phase, work::TextUploadPhase::Reclaimable(_))
    {
        return Ok(blocked_database(vec![intent_range], Vec::new()));
    }

    let candidate_key = scoped_key(
        scope,
        index_keys::IndexV2Key::BlobGcCandidate(index_keys::BlobGcCandidateKey {
            index_id: intent.index_id,
            generation: intent.generation,
            owner: index_keys::BlobGcCandidateKeyOwner::UploadIntent(intent.intent_id),
            blob_hash: index_keys::BlobHash::new(*intent.blob.hash()),
        }),
    );
    let candidate_value = transaction.get(&candidate_key).await?;
    let exact_candidate = candidate_value.as_ref().is_some_and(|value| {
        matches!(
            index_values::decode_work_value(value),
            Ok(index_values::IndexV2WorkValue::BlobGcCandidate(candidate))
                if candidate.owner == work::BlobGcCandidateOwner::UploadIntent(intent.intent_id)
                    && candidate.index_id == intent.index_id
                    && candidate.generation == intent.generation
                    && candidate.blob == intent.blob
        )
    });
    let (reference_key, _) = super::reclaim::intent_reachability_row(scope, &intent);
    let reference_value = transaction.get(&reference_key).await?;
    let observations = vec![
        RowObservation {
            key: candidate_key,
            value: candidate_value,
        },
        RowObservation {
            key: reference_key,
            value: reference_value.clone(),
        },
    ];
    if !exact_candidate || reference_value.is_some() {
        return Ok(blocked_database(vec![intent_range], observations));
    }
    let input_bytes = row_bytes(&row_key, Some(&row_value)).saturating_add(
        observations.iter().fold(0_u64, |bytes, observation| {
            bytes.saturating_add(row_bytes(&observation.key, observation.value.as_ref()))
        }),
    );
    if input_bytes > limits.max_input_bytes().get() {
        return Ok(ValidationSelection::Database(PreparedDatabaseValidation {
            ranges: vec![intent_range],
            observations,
            result: IndexOperationStepResult::Blocked(IndexOperationBlocker::ManifestLimit {
                partition: intent.partition.clone(),
                observed: input_bytes,
                limit: limits.max_input_bytes().get(),
            }),
        }));
    }
    let Some(input_bytes) = progress.counters.input_bytes.checked_add(input_bytes) else {
        return Err(corruption(
            "text upload-validation input counter overflowed",
        ));
    };
    let counters = OperationCounters {
        input_bytes,
        ..progress.counters
    };
    Ok(ValidationSelection::Database(PreparedDatabaseValidation {
        ranges: vec![intent_range],
        observations,
        result: progressed(TextBuildStage::ValidateManifests(
            TextManifestValidationProgress::UploadIntents(PrefixScanProgress {
                cursor: Some(
                    IndexCursor::try_new(row_key)
                        .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?,
                ),
                counters,
            }),
        )),
    }))
}

/// Proves no late delta, artifact, or mutation proof can cross activation.
async fn select_activation_prerequisites(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    counters: OperationCounters,
    intent_range: PreparedValidationRange,
) -> Result<ValidationSelection> {
    let delta_prefix =
        generation_prefix(scope, index_keys::IndexV2RecordKind::BuildDelta, operation);
    let (delta_range, delta) = select_one(transaction, delta_prefix, None).await?;
    let artifact_prefix = generation_prefix(
        scope,
        index_keys::IndexV2RecordKind::TextBuildArtifact,
        operation,
    );
    let (artifact_range, artifact) = select_one(transaction, artifact_prefix, None).await?;
    let proof_prefix = generation_prefix(
        scope,
        index_keys::IndexV2RecordKind::ActiveMutationCommitProof,
        operation,
    );
    let (proof_range, proof) = select_one(transaction, proof_prefix, None).await?;
    let result = if delta.is_some() {
        progressed(TextBuildStage::CatchUp(PrefixScanProgress {
            cursor: None,
            counters,
        }))
    } else if artifact.is_some() || proof.is_some() {
        IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation)
    } else {
        progressed(TextBuildStage::Activate(
            crate::index_v2::NoCursorProgress { counters },
        ))
    };
    Ok(ValidationSelection::Database(PreparedDatabaseValidation {
        ranges: vec![intent_range, delta_range, artifact_range, proof_range],
        observations: Vec::new(),
        result,
    }))
}

/// Selects one exact row or one exact exhausted suffix from a typed prefix.
async fn select_one(
    transaction: &DbTransaction,
    prefix: Bytes,
    cursor: Option<&IndexCursor>,
) -> Result<(PreparedValidationRange, Option<(Bytes, Bytes)>)> {
    let start = match cursor {
        Some(cursor) => {
            let Some(suffix) = cursor.as_bytes().strip_prefix(prefix.as_ref()) else {
                return Err(corruption(
                    "text manifest-validation cursor is outside its exact prefix",
                ));
            };
            Bound::Excluded(Bytes::copy_from_slice(suffix))
        }
        None => Bound::Unbounded,
    };
    let bounds = (start.clone(), Bound::<Bytes>::Unbounded);
    let mut rows = transaction.scan_prefix(&prefix, bounds).await?;
    let Some(row) = rows.next().await? else {
        return Ok((
            PreparedValidationRange {
                prefix,
                start,
                end: Bound::Unbounded,
                rows: Vec::new(),
            },
            None,
        ));
    };
    let suffix = row
        .key
        .strip_prefix(prefix.as_ref())
        .expect("scan_prefix returns only keys with the requested prefix");
    let end = Bound::Included(Bytes::copy_from_slice(suffix));
    let selected = (row.key, row.value);
    Ok((
        PreparedValidationRange {
            prefix,
            start,
            end,
            rows: vec![selected.clone()],
        },
        Some(selected),
    ))
}

/// Constructs a range-fenced durable invariant blocker.
fn blocked_database(
    ranges: Vec<PreparedValidationRange>,
    observations: Vec<RowObservation>,
) -> ValidationSelection {
    ValidationSelection::Database(PreparedDatabaseValidation {
        ranges,
        observations,
        result: IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation),
    })
}

/// Encodes a complete generation prefix through the canonical V1 key codec.
fn generation_prefix(
    scope: DataScope,
    kind: index_keys::IndexV2RecordKind,
    operation: &IndexOperationRecord,
) -> Bytes {
    Key::data_prefix(
        scope,
        index_keys::IndexV2Key::generation_prefix(
            kind,
            operation.index_id(),
            operation.generation(),
        ),
    )
}

/// Encodes one scoped logical key through the canonical V1 key codec.
fn scoped_key(scope: DataScope, key: index_keys::IndexV2Key) -> Bytes {
    Key::Data {
        scope,
        kind: DataKeyKind::IndexV2(key),
    }
    .to_bytes()
}

/// Measures one exact observed key/value row without allocation.
fn row_bytes(key: &Bytes, value: Option<&Bytes>) -> u64 {
    u64::try_from(key.len().saturating_add(value.map_or(0, Bytes::len))).unwrap_or(u64::MAX)
}

/// Wraps a validation stage in the only legal constructing progress shape.
fn progressed(stage: TextBuildStage) -> IndexOperationStepResult {
    IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
        TextBuildProgress::Constructing(stage),
    ))
}

fn corruption(message: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(message.into())
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::config::{SearchIndexBackfillLimits, TextAnalyzerKind};
    use crate::encoding::v1::keys::index_v2::{
        BlobGcCandidateKey, BlobGcCandidateKeyOwner, BlobHash, IndexEntity, IndexEntityStateKey,
        TextIntentOwnedKey, TextManifestPageKey, TextManifestRootKey,
    };
    use crate::index_v2::{
        BlobPublicationPermitId, IndexElementKind, IndexEntityId, IndexGenerationId, IndexId,
        IndexOperationExecutionState, IndexOperationFamily, IndexOperationId, IndexOperationKind,
        IndexOperationRevision, IndexRevision, TextIntentRevision, TextUploadIntentId,
        ValidatedDynamicIndexDefinition,
    };

    async fn test_db(name: &str) -> Db {
        Db::builder(name, Arc::new(InMemory::new()))
            .build()
            .await
            .expect("validation test database opens")
    }

    fn definition(tenant_property: Option<&str>) -> ValidatedDynamicIndexDefinition {
        ValidatedDynamicIndexDefinition::Text(
            ValidatedTextIndexDefinition::try_new(
                IndexElementKind::Node,
                "Document",
                "body",
                tenant_property,
                TextAnalyzerKind::Standard,
                false,
            )
            .expect("text validation definition is valid"),
        )
    }

    fn operation(
        definition: &ValidatedDynamicIndexDefinition,
        progress: TextManifestValidationProgress,
    ) -> IndexOperationRecord {
        IndexOperationRecord::try_new(
            IndexOperationId::from_bytes([0x11; 16]).expect("operation ID is non-nil"),
            IndexId::initial(),
            definition.identity().clone(),
            IndexGenerationId::initial(),
            IndexRevision::initial(),
            IndexOperationRevision::initial(),
            IndexOperationKind::Build,
            IndexOperationFamily::Text,
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::ValidateManifests(progress),
            )),
            0,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .expect("text validation operation is valid")
    }

    fn exact_split(seed: u8) -> work::SplitRef {
        work::SplitRef::try_new(work::BlobRef::new([seed; 32], 128), 80, 16, 4, 128)
            .expect("exact split layout is structurally valid")
    }

    fn root_key(
        operation: &IndexOperationRecord,
        partition: &TextPartition,
    ) -> TextManifestRootKey {
        TextManifestRootKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            partition: partition.fingerprint(),
        }
    }

    struct ManifestFixture {
        partition: TextPartition,
        revision: TextManifestRevision,
        page_number: u32,
        root_page_count: u32,
        root_split_count: u64,
        entries: Vec<work::SplitRef>,
        include_reachability: bool,
    }

    async fn put_manifest(db: &Db, operation: &IndexOperationRecord, fixture: ManifestFixture) {
        let ManifestFixture {
            partition,
            revision,
            page_number,
            root_page_count,
            root_split_count,
            entries,
            include_reachability,
        } = fixture;
        let scope = DataScope::LegacyUnscoped;
        let root_key = root_key(operation, &partition);
        db.put(
            scoped_key(scope, index_keys::IndexV2Key::TextManifestRoot(root_key)),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestRoot(
                work::TextManifestRootValue::try_new(
                    operation.index_id(),
                    operation.generation(),
                    partition.clone(),
                    revision,
                    root_page_count,
                    root_split_count,
                )
                .expect("test manifest root counts are structurally valid"),
            )),
        )
        .await
        .expect("test manifest root is written");
        let page_key = TextManifestPageKey {
            root: root_key,
            page: page_number,
        };
        db.put(
            scoped_key(scope, index_keys::IndexV2Key::TextManifestPage(page_key)),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestPage(
                work::TextManifestPageValue::try_new(
                    operation.index_id(),
                    operation.generation(),
                    partition,
                    page_number,
                    entries.clone(),
                )
                .expect("test manifest page is structurally valid"),
            )),
        )
        .await
        .expect("test manifest page is written");
        if include_reachability {
            for (slot, split) in entries.into_iter().enumerate() {
                let (key, value) = super::super::attachment::manifest_page_reachability_row(
                    split.blob(),
                    scope,
                    page_key,
                    u32::try_from(slot).expect("test slot fits u32"),
                );
                db.put(key, value)
                    .await
                    .expect("test manifest reachability is written");
            }
        }
    }

    async fn stage_selection(db: &Db, selection: &ValidationSelection) -> IndexOperationStepResult {
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("validation stage transaction opens");
        match selection {
            ValidationSelection::Database(prepared) => prepared
                .stage(&transaction)
                .await
                .expect("database validation stages"),
            ValidationSelection::Page(prepared) => prepared
                .stage(&transaction)
                .await
                .expect("page validation stages"),
        }
    }

    /// Runs one default-limit selection and its serializable revalidation commit.
    async fn select_and_stage(
        db: &Db,
        operation: &IndexOperationRecord,
        definition: &ValidatedTextIndexDefinition,
        progress: &TextManifestValidationProgress,
    ) -> IndexOperationStepResult {
        let snapshot = db
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("validation selection snapshot opens");
        let selection = select(
            &snapshot,
            DataScope::LegacyUnscoped,
            operation,
            definition,
            progress,
            SearchIndexBackfillLimits::default().batch(),
        )
        .await
        .expect("validation selection succeeds");
        drop(snapshot);
        stage_selection(db, &selection).await
    }

    fn progressed_validation(result: IndexOperationStepResult) -> TextManifestValidationProgress {
        let IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
            TextBuildProgress::Constructing(TextBuildStage::ValidateManifests(progress)),
        )) = result
        else {
            panic!("validation checkpoint must progress to another validation lane")
        };
        progress
    }

    #[tokio::test]
    async fn valid_page_root_and_empty_intent_lanes_reach_activation() {
        let db = test_db("text-validation-happy-lanes").await;
        let definition = definition(None);
        let mut progress = TextManifestValidationProgress::initial(OperationCounters::default());
        let operation = operation(&definition, progress.clone());
        put_manifest(
            &db,
            &operation,
            ManifestFixture {
                partition: TextPartition::Unpartitioned,
                revision: TextManifestRevision::new(3).unwrap(),
                page_number: 0,
                root_page_count: 2,
                root_split_count: 2,
                entries: vec![exact_split(0x21)],
                include_reachability: true,
            },
        )
        .await;
        put_manifest(
            &db,
            &operation,
            ManifestFixture {
                partition: TextPartition::Unpartitioned,
                revision: TextManifestRevision::new(3).unwrap(),
                page_number: 1,
                root_page_count: 2,
                root_split_count: 2,
                entries: vec![exact_split(0x22)],
                include_reachability: true,
            },
        )
        .await;
        let ValidatedDynamicIndexDefinition::Text(text_definition) = &definition else {
            panic!("test definition is text")
        };

        let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let page = select(
            &snapshot,
            DataScope::LegacyUnscoped,
            &operation,
            text_definition,
            &progress,
            SearchIndexBackfillLimits::default().batch(),
        )
        .await
        .unwrap();
        drop(snapshot);
        assert!(matches!(page, ValidationSelection::Page(_)));
        progress = progressed_validation(stage_selection(&db, &page).await);

        let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let second_page = select(
            &snapshot,
            DataScope::LegacyUnscoped,
            &operation,
            text_definition,
            &progress,
            SearchIndexBackfillLimits::default().batch(),
        )
        .await
        .unwrap();
        drop(snapshot);
        progress = progressed_validation(stage_selection(&db, &second_page).await);
        assert!(matches!(progress, TextManifestValidationProgress::Pages(_)));

        let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let pages_exhausted = select(
            &snapshot,
            DataScope::LegacyUnscoped,
            &operation,
            text_definition,
            &progress,
            SearchIndexBackfillLimits::default().batch(),
        )
        .await
        .unwrap();
        drop(snapshot);
        progress = progressed_validation(stage_selection(&db, &pages_exhausted).await);
        assert!(matches!(progress, TextManifestValidationProgress::Roots(_)));

        let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let root = select(
            &snapshot,
            DataScope::LegacyUnscoped,
            &operation,
            text_definition,
            &progress,
            SearchIndexBackfillLimits::default().batch(),
        )
        .await
        .unwrap();
        drop(snapshot);
        progress = progressed_validation(stage_selection(&db, &root).await);
        let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let roots_exhausted = select(
            &snapshot,
            DataScope::LegacyUnscoped,
            &operation,
            text_definition,
            &progress,
            SearchIndexBackfillLimits::default().batch(),
        )
        .await
        .unwrap();
        drop(snapshot);
        progress = progressed_validation(stage_selection(&db, &roots_exhausted).await);
        assert!(matches!(
            progress,
            TextManifestValidationProgress::UploadIntents(_)
        ));

        let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let intents_exhausted = select(
            &snapshot,
            DataScope::LegacyUnscoped,
            &operation,
            text_definition,
            &progress,
            SearchIndexBackfillLimits::default().batch(),
        )
        .await
        .unwrap();
        drop(snapshot);
        assert!(matches!(
            stage_selection(&db, &intents_exhausted).await,
            IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
                TextBuildProgress::Constructing(TextBuildStage::Activate(_))
            ))
        ));
    }

    #[tokio::test]
    async fn page_validation_rejects_stale_ranges_and_every_local_shape_violation() {
        let definition = definition(None);
        let ValidatedDynamicIndexDefinition::Text(text_definition) = &definition else {
            panic!("test definition is text")
        };
        let limits = SearchIndexBackfillLimits::default().batch();

        let stale_db = test_db("text-validation-stale-page").await;
        let stale_progress = TextManifestValidationProgress::initial(OperationCounters::default());
        let stale_operation = operation(&definition, stale_progress.clone());
        let stale_split = exact_split(0x31);
        put_manifest(
            &stale_db,
            &stale_operation,
            ManifestFixture {
                partition: TextPartition::Unpartitioned,
                revision: TextManifestRevision::new(2).unwrap(),
                page_number: 0,
                root_page_count: 1,
                root_split_count: 1,
                entries: vec![stale_split],
                include_reachability: true,
            },
        )
        .await;
        let snapshot = stale_db.begin(IsolationLevel::Snapshot).await.unwrap();
        let stale = select(
            &snapshot,
            DataScope::LegacyUnscoped,
            &stale_operation,
            text_definition,
            &stale_progress,
            limits,
        )
        .await
        .unwrap();
        drop(snapshot);
        let page_key = TextManifestPageKey {
            root: root_key(&stale_operation, &TextPartition::Unpartitioned),
            page: 0,
        };
        let (reference_key, _) = super::super::attachment::manifest_page_reachability_row(
            stale_split.blob(),
            DataScope::LegacyUnscoped,
            page_key,
            0,
        );
        stale_db.delete(reference_key).await.unwrap();
        assert!(matches!(
            stage_selection(&stale_db, &stale).await,
            IndexOperationStepResult::TransientFailure
        ));

        for (name, fixture) in [
            (
                "text-validation-root-revision",
                ManifestFixture {
                    partition: TextPartition::Unpartitioned,
                    revision: TextManifestRevision::initial(),
                    page_number: 0,
                    root_page_count: 1,
                    root_split_count: 1,
                    entries: vec![exact_split(0x32)],
                    include_reachability: true,
                },
            ),
            (
                "text-validation-noncontiguous-page",
                ManifestFixture {
                    partition: TextPartition::Unpartitioned,
                    revision: TextManifestRevision::new(3).unwrap(),
                    page_number: 1,
                    root_page_count: 2,
                    root_split_count: 2,
                    entries: vec![exact_split(0x33)],
                    include_reachability: true,
                },
            ),
            (
                "text-validation-invalid-layout",
                ManifestFixture {
                    partition: TextPartition::Unpartitioned,
                    revision: TextManifestRevision::new(2).unwrap(),
                    page_number: 0,
                    root_page_count: 1,
                    root_split_count: 1,
                    entries: vec![work::SplitRef::try_new(
                        work::BlobRef::new([0x34; 32], 128),
                        80,
                        15,
                        4,
                        128,
                    )
                    .unwrap()],
                    include_reachability: true,
                },
            ),
            (
                "text-validation-missing-reference",
                ManifestFixture {
                    partition: TextPartition::Unpartitioned,
                    revision: TextManifestRevision::new(2).unwrap(),
                    page_number: 0,
                    root_page_count: 1,
                    root_split_count: 1,
                    entries: vec![exact_split(0x35)],
                    include_reachability: false,
                },
            ),
            (
                "text-validation-final-split-count",
                ManifestFixture {
                    partition: TextPartition::Unpartitioned,
                    revision: TextManifestRevision::new(2).unwrap(),
                    page_number: 0,
                    root_page_count: 1,
                    root_split_count: 2,
                    entries: vec![exact_split(0x37)],
                    include_reachability: true,
                },
            ),
            (
                "text-validation-impossible-partial-counts",
                ManifestFixture {
                    partition: TextPartition::Unpartitioned,
                    revision: TextManifestRevision::new(3).unwrap(),
                    page_number: 0,
                    root_page_count: 2,
                    root_split_count: 2,
                    entries: vec![exact_split(0x38), exact_split(0x39)],
                    include_reachability: true,
                },
            ),
        ] {
            let db = test_db(name).await;
            let progress = TextManifestValidationProgress::initial(OperationCounters::default());
            let operation = operation(&definition, progress.clone());
            put_manifest(&db, &operation, fixture).await;
            let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
            let selection = select(
                &snapshot,
                DataScope::LegacyUnscoped,
                &operation,
                text_definition,
                &progress,
                limits,
            )
            .await
            .unwrap();
            drop(snapshot);
            assert!(matches!(
                stage_selection(&db, &selection).await,
                IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation)
            ));
        }

        let duplicate_db = test_db("text-validation-duplicate-page-blob").await;
        let duplicate_progress =
            TextManifestValidationProgress::initial(OperationCounters::default());
        let duplicate_operation = operation(&definition, duplicate_progress.clone());
        let duplicate = exact_split(0x36);
        put_manifest(
            &duplicate_db,
            &duplicate_operation,
            ManifestFixture {
                partition: TextPartition::Unpartitioned,
                revision: TextManifestRevision::new(2).unwrap(),
                page_number: 0,
                root_page_count: 1,
                root_split_count: 2,
                entries: vec![duplicate, duplicate],
                include_reachability: true,
            },
        )
        .await;
        let snapshot = duplicate_db.begin(IsolationLevel::Snapshot).await.unwrap();
        let selection = select(
            &snapshot,
            DataScope::LegacyUnscoped,
            &duplicate_operation,
            text_definition,
            &duplicate_progress,
            limits,
        )
        .await
        .unwrap();
        drop(snapshot);
        assert!(matches!(
            stage_selection(&duplicate_db, &selection).await,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation)
        ));
    }

    #[tokio::test]
    async fn page_validation_revalidates_rows_and_rejects_malformed_ownership() {
        let definition = definition(None);
        let ValidatedDynamicIndexDefinition::Text(text_definition) = &definition else {
            panic!("test definition is text")
        };
        let progress = TextManifestValidationProgress::initial(OperationCounters::default());

        for (name, replacement) in [
            ("text-validation-missing-selected-page", None),
            (
                "text-validation-changed-selected-page",
                Some(Bytes::from_static(b"changed-page")),
            ),
        ] {
            let db = test_db(name).await;
            let operation = operation(&definition, progress.clone());
            put_manifest(
                &db,
                &operation,
                ManifestFixture {
                    partition: TextPartition::Unpartitioned,
                    revision: TextManifestRevision::new(2).unwrap(),
                    page_number: 0,
                    root_page_count: 1,
                    root_split_count: 1,
                    entries: vec![exact_split(0x71)],
                    include_reachability: true,
                },
            )
            .await;
            let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
            let prepared = select(
                &snapshot,
                DataScope::LegacyUnscoped,
                &operation,
                text_definition,
                &progress,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await
            .unwrap();
            drop(snapshot);
            let page_key = scoped_key(
                DataScope::LegacyUnscoped,
                index_keys::IndexV2Key::TextManifestPage(TextManifestPageKey {
                    root: root_key(&operation, &TextPartition::Unpartitioned),
                    page: 0,
                }),
            );
            match replacement {
                Some(value) => db.put(page_key, value).await.unwrap(),
                None => db.delete(page_key).await.unwrap(),
            };
            assert!(matches!(
                stage_selection(&db, &prepared).await,
                IndexOperationStepResult::TransientFailure
            ));
        }

        let malformed_key_db = test_db("text-validation-malformed-page-key").await;
        let malformed_key_operation = operation(&definition, progress.clone());
        let mut malformed_key = generation_prefix(
            DataScope::LegacyUnscoped,
            index_keys::IndexV2RecordKind::TextManifestPage,
            &malformed_key_operation,
        )
        .to_vec();
        malformed_key.push(0xFF);
        malformed_key_db
            .put(Bytes::from(malformed_key), Bytes::from_static(b"malformed"))
            .await
            .unwrap();
        assert!(matches!(
            select_and_stage(
                &malformed_key_db,
                &malformed_key_operation,
                text_definition,
                &progress,
            )
            .await,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation)
        ));

        let malformed_value_db = test_db("text-validation-malformed-page-value").await;
        let malformed_value_operation = operation(&definition, progress.clone());
        malformed_value_db
            .put(
                scoped_key(
                    DataScope::LegacyUnscoped,
                    index_keys::IndexV2Key::TextManifestPage(TextManifestPageKey {
                        root: root_key(&malformed_value_operation, &TextPartition::Unpartitioned),
                        page: 0,
                    }),
                ),
                Bytes::from_static(b"malformed"),
            )
            .await
            .unwrap();
        assert!(matches!(
            select_and_stage(
                &malformed_value_db,
                &malformed_value_operation,
                text_definition,
                &progress,
            )
            .await,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation)
        ));

        for (name, replacement) in [
            ("text-validation-missing-root", None),
            (
                "text-validation-malformed-root-value",
                Some(Bytes::from_static(b"malformed-root")),
            ),
        ] {
            let db = test_db(name).await;
            let operation = operation(&definition, progress.clone());
            put_manifest(
                &db,
                &operation,
                ManifestFixture {
                    partition: TextPartition::Unpartitioned,
                    revision: TextManifestRevision::new(2).unwrap(),
                    page_number: 0,
                    root_page_count: 1,
                    root_split_count: 1,
                    entries: vec![exact_split(0x72)],
                    include_reachability: true,
                },
            )
            .await;
            let root_key = scoped_key(
                DataScope::LegacyUnscoped,
                index_keys::IndexV2Key::TextManifestRoot(root_key(
                    &operation,
                    &TextPartition::Unpartitioned,
                )),
            );
            match replacement {
                Some(value) => db.put(root_key, value).await.unwrap(),
                None => db.delete(root_key).await.unwrap(),
            };
            assert!(matches!(
                select_and_stage(&db, &operation, text_definition, &progress).await,
                IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation)
            ));
        }
    }

    #[tokio::test]
    async fn page_validation_rejects_incomplete_partition_exhaustion() {
        let definition = definition(None);
        let ValidatedDynamicIndexDefinition::Text(text_definition) = &definition else {
            panic!("test definition is text")
        };
        let partition = TextPartition::Unpartitioned;
        let page_zero_key = scoped_key(
            DataScope::LegacyUnscoped,
            index_keys::IndexV2Key::TextManifestPage(TextManifestPageKey {
                root: TextManifestRootKey {
                    index_id: IndexId::initial(),
                    generation: IndexGenerationId::initial(),
                    partition: partition.fingerprint(),
                },
                page: 0,
            }),
        );
        let page_zero_cursor = IndexCursor::try_new(page_zero_key).unwrap();

        let incomplete_partition = TextManifestPartitionValidation::try_new(
            *partition.fingerprint().as_bytes(),
            TextManifestRevision::new(3).unwrap(),
            2,
            2,
            1,
            1,
        )
        .unwrap();
        let incomplete = TextManifestValidationProgress::Pages(
            TextManifestPageValidationProgress::try_new(
                Some(page_zero_cursor.clone()),
                Some(incomplete_partition),
                OperationCounters::default(),
            )
            .unwrap(),
        );
        let empty_db = test_db("text-validation-incomplete-exhaustion").await;
        let empty_operation = operation(&definition, incomplete.clone());
        assert!(matches!(
            select_and_stage(&empty_db, &empty_operation, text_definition, &incomplete).await,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation)
        ));
    }

    #[tokio::test]
    async fn root_validation_accepts_only_canonical_partition_shapes() {
        let unpartitioned_definition = definition(None);
        let ValidatedDynamicIndexDefinition::Text(unpartitioned) = &unpartitioned_definition else {
            panic!("test definition is text")
        };
        let limits = SearchIndexBackfillLimits::default().batch();

        let empty_db = test_db("text-validation-empty-root").await;
        let progress = TextManifestValidationProgress::Roots(PrefixScanProgress {
            cursor: None,
            counters: OperationCounters::default(),
        });
        let empty_operation = operation(&unpartitioned_definition, progress.clone());
        empty_db
            .put(
                scoped_key(
                    DataScope::LegacyUnscoped,
                    index_keys::IndexV2Key::TextManifestRoot(root_key(
                        &empty_operation,
                        &TextPartition::Unpartitioned,
                    )),
                ),
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestRoot(
                    work::TextManifestRootValue::empty(
                        empty_operation.index_id(),
                        empty_operation.generation(),
                        TextPartition::Unpartitioned,
                    ),
                )),
            )
            .await
            .unwrap();
        let snapshot = empty_db.begin(IsolationLevel::Snapshot).await.unwrap();
        let selection = select(
            &snapshot,
            DataScope::LegacyUnscoped,
            &empty_operation,
            unpartitioned,
            &progress,
            limits,
        )
        .await
        .unwrap();
        drop(snapshot);
        assert!(matches!(
            stage_selection(&empty_db, &selection).await,
            IndexOperationStepResult::Progressed(_)
        ));

        let invalid_revision_db = test_db("text-validation-empty-root-revision").await;
        let invalid_operation = operation(&unpartitioned_definition, progress.clone());
        invalid_revision_db
            .put(
                scoped_key(
                    DataScope::LegacyUnscoped,
                    index_keys::IndexV2Key::TextManifestRoot(root_key(
                        &invalid_operation,
                        &TextPartition::Unpartitioned,
                    )),
                ),
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestRoot(
                    work::TextManifestRootValue::try_new(
                        invalid_operation.index_id(),
                        invalid_operation.generation(),
                        TextPartition::Unpartitioned,
                        TextManifestRevision::new(2).unwrap(),
                        0,
                        0,
                    )
                    .unwrap(),
                )),
            )
            .await
            .unwrap();
        let snapshot = invalid_revision_db
            .begin(IsolationLevel::Snapshot)
            .await
            .unwrap();
        let selection = select(
            &snapshot,
            DataScope::LegacyUnscoped,
            &invalid_operation,
            unpartitioned,
            &progress,
            limits,
        )
        .await
        .unwrap();
        drop(snapshot);
        assert!(matches!(
            stage_selection(&invalid_revision_db, &selection).await,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation)
        ));

        let tenant_db = test_db("text-validation-wrong-partition-mode").await;
        let tenant_operation = operation(&unpartitioned_definition, progress.clone());
        let tenant_partition = TextPartition::try_tenant_value(Bytes::from_static(b"tenant"))
            .expect("tenant partition is valid");
        tenant_db
            .put(
                scoped_key(
                    DataScope::LegacyUnscoped,
                    index_keys::IndexV2Key::TextManifestRoot(root_key(
                        &tenant_operation,
                        &tenant_partition,
                    )),
                ),
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestRoot(
                    work::TextManifestRootValue::empty(
                        tenant_operation.index_id(),
                        tenant_operation.generation(),
                        tenant_partition,
                    ),
                )),
            )
            .await
            .unwrap();
        let snapshot = tenant_db.begin(IsolationLevel::Snapshot).await.unwrap();
        let selection = select(
            &snapshot,
            DataScope::LegacyUnscoped,
            &tenant_operation,
            unpartitioned,
            &progress,
            limits,
        )
        .await
        .unwrap();
        drop(snapshot);
        assert!(matches!(
            stage_selection(&tenant_db, &selection).await,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation)
        ));
    }

    #[tokio::test]
    async fn root_and_upload_validation_reject_malformed_rows_and_external_cursors() {
        let definition = definition(None);
        let ValidatedDynamicIndexDefinition::Text(text_definition) = &definition else {
            panic!("test definition is text")
        };
        let root_progress = TextManifestValidationProgress::Roots(PrefixScanProgress {
            cursor: None,
            counters: OperationCounters::default(),
        });

        let malformed_root_key_db = test_db("text-validation-malformed-root-key").await;
        let malformed_root_key_operation = operation(&definition, root_progress.clone());
        let mut malformed_root_key = generation_prefix(
            DataScope::LegacyUnscoped,
            index_keys::IndexV2RecordKind::TextManifestRoot,
            &malformed_root_key_operation,
        )
        .to_vec();
        malformed_root_key.push(0xFF);
        malformed_root_key_db
            .put(
                Bytes::from(malformed_root_key),
                Bytes::from_static(b"malformed"),
            )
            .await
            .unwrap();
        assert!(matches!(
            select_and_stage(
                &malformed_root_key_db,
                &malformed_root_key_operation,
                text_definition,
                &root_progress,
            )
            .await,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation)
        ));

        let malformed_root_value_db = test_db("text-validation-malformed-root-value-row").await;
        let malformed_root_value_operation = operation(&definition, root_progress.clone());
        malformed_root_value_db
            .put(
                scoped_key(
                    DataScope::LegacyUnscoped,
                    index_keys::IndexV2Key::TextManifestRoot(root_key(
                        &malformed_root_value_operation,
                        &TextPartition::Unpartitioned,
                    )),
                ),
                Bytes::from_static(b"malformed"),
            )
            .await
            .unwrap();
        assert!(matches!(
            select_and_stage(
                &malformed_root_value_db,
                &malformed_root_value_operation,
                text_definition,
                &root_progress,
            )
            .await,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation)
        ));

        let missing_page_db = test_db("text-validation-missing-page-zero").await;
        let missing_page_operation = operation(&definition, root_progress.clone());
        put_manifest(
            &missing_page_db,
            &missing_page_operation,
            ManifestFixture {
                partition: TextPartition::Unpartitioned,
                revision: TextManifestRevision::new(2).unwrap(),
                page_number: 0,
                root_page_count: 1,
                root_split_count: 1,
                entries: vec![exact_split(0x75)],
                include_reachability: true,
            },
        )
        .await;
        missing_page_db
            .delete(scoped_key(
                DataScope::LegacyUnscoped,
                index_keys::IndexV2Key::TextManifestPage(TextManifestPageKey {
                    root: root_key(&missing_page_operation, &TextPartition::Unpartitioned),
                    page: 0,
                }),
            ))
            .await
            .unwrap();
        assert!(matches!(
            select_and_stage(
                &missing_page_db,
                &missing_page_operation,
                text_definition,
                &root_progress,
            )
            .await,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation)
        ));

        let upload_progress = TextManifestValidationProgress::UploadIntents(PrefixScanProgress {
            cursor: None,
            counters: OperationCounters::default(),
        });
        let malformed_intent_key_db = test_db("text-validation-malformed-intent-key").await;
        let malformed_intent_key_operation = operation(&definition, upload_progress.clone());
        let mut malformed_intent_key = generation_prefix(
            DataScope::LegacyUnscoped,
            index_keys::IndexV2RecordKind::TextUploadIntent,
            &malformed_intent_key_operation,
        )
        .to_vec();
        malformed_intent_key.push(0xFF);
        malformed_intent_key_db
            .put(
                Bytes::from(malformed_intent_key),
                Bytes::from_static(b"malformed"),
            )
            .await
            .unwrap();
        assert!(matches!(
            select_and_stage(
                &malformed_intent_key_db,
                &malformed_intent_key_operation,
                text_definition,
                &upload_progress,
            )
            .await,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation)
        ));

        let malformed_intent_value_db = test_db("text-validation-malformed-intent-value").await;
        let malformed_intent_value_operation = operation(&definition, upload_progress.clone());
        malformed_intent_value_db
            .put(
                scoped_key(
                    DataScope::LegacyUnscoped,
                    index_keys::IndexV2Key::TextUploadIntent(TextIntentOwnedKey {
                        index_id: malformed_intent_value_operation.index_id(),
                        generation: malformed_intent_value_operation.generation(),
                        intent_id: TextUploadIntentId::from_bytes([0x76; 16]).unwrap(),
                    }),
                ),
                Bytes::from_static(b"malformed"),
            )
            .await
            .unwrap();
        assert!(matches!(
            select_and_stage(
                &malformed_intent_value_db,
                &malformed_intent_value_operation,
                text_definition,
                &upload_progress,
            )
            .await,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation)
        ));

        let external_cursor = TextManifestValidationProgress::Roots(PrefixScanProgress {
            cursor: Some(IndexCursor::try_new(Bytes::from_static(b"external")).unwrap()),
            counters: OperationCounters::default(),
        });
        let external_cursor_db = test_db("text-validation-external-cursor").await;
        let external_cursor_operation = operation(&definition, external_cursor.clone());
        let snapshot = external_cursor_db
            .begin(IsolationLevel::Snapshot)
            .await
            .unwrap();
        assert!(matches!(
            select(
                &snapshot,
                DataScope::LegacyUnscoped,
                &external_cursor_operation,
                text_definition,
                &external_cursor,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("outside its exact prefix")
        ));
    }

    fn reclaimable_intent(
        operation: &IndexOperationRecord,
        split: work::SplitRef,
    ) -> work::TextUploadIntentValue {
        work::TextUploadIntentValue::try_new(
            TextUploadIntentId::from_bytes([0x41; 16]).unwrap(),
            TextIntentRevision::initial(),
            operation.index_id(),
            operation.identity().clone(),
            operation.generation(),
            TextPartition::Unpartitioned,
            split.blob(),
            BlobPublicationPermitId::from_bytes([0x42; 16]).unwrap(),
            work::TextUploadOwner::Build {
                operation_id: operation.operation_id(),
                expected_operation_revision: operation.operation_revision(),
            },
            work::TextUploadAttachment::ManifestSplit(split),
            work::TextUploadPhase::Reclaimable(work::ReclaimAssignment::Unassigned),
            0,
            work::TextUploadWorkState::Queued {
                not_before_unix_millis: None,
            },
        )
        .expect("reclaimable build intent is valid")
    }

    async fn put_intent_candidate(
        db: &Db,
        operation: &IndexOperationRecord,
        intent: &work::TextUploadIntentValue,
    ) {
        let scope = DataScope::LegacyUnscoped;
        db.put(
            scoped_key(
                scope,
                index_keys::IndexV2Key::TextUploadIntent(TextIntentOwnedKey {
                    index_id: intent.index_id,
                    generation: intent.generation,
                    intent_id: intent.intent_id,
                }),
            ),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextUploadIntent(
                Box::new(intent.clone()),
            )),
        )
        .await
        .unwrap();
        db.put(
            scoped_key(
                scope,
                index_keys::IndexV2Key::BlobGcCandidate(BlobGcCandidateKey {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    owner: BlobGcCandidateKeyOwner::UploadIntent(intent.intent_id),
                    blob_hash: BlobHash::new(*intent.blob.hash()),
                }),
            ),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcCandidate(
                work::BlobGcCandidateValue {
                    owner: work::BlobGcCandidateOwner::UploadIntent(intent.intent_id),
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    blob: intent.blob,
                },
            )),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn upload_validation_requires_reclaimable_candidate_without_live_reference() {
        let db = test_db("text-validation-reclaim-intent").await;
        let definition = definition(None);
        let ValidatedDynamicIndexDefinition::Text(text_definition) = &definition else {
            panic!("test definition is text")
        };
        let progress = TextManifestValidationProgress::UploadIntents(PrefixScanProgress {
            cursor: None,
            counters: OperationCounters::default(),
        });
        let operation = operation(&definition, progress.clone());
        let intent = reclaimable_intent(&operation, exact_split(0x43));
        put_intent_candidate(&db, &operation, &intent).await;
        let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let selection = select(
            &snapshot,
            DataScope::LegacyUnscoped,
            &operation,
            text_definition,
            &progress,
            SearchIndexBackfillLimits::default().batch(),
        )
        .await
        .unwrap();
        drop(snapshot);
        assert!(matches!(
            stage_selection(&db, &selection).await,
            IndexOperationStepResult::Progressed(_)
        ));

        let (reference_key, reference_value) =
            super::super::reclaim::intent_reachability_row(DataScope::LegacyUnscoped, &intent);
        db.put(reference_key, reference_value).await.unwrap();
        let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let blocked = select(
            &snapshot,
            DataScope::LegacyUnscoped,
            &operation,
            text_definition,
            &progress,
            SearchIndexBackfillLimits::default().batch(),
        )
        .await
        .unwrap();
        drop(snapshot);
        assert!(matches!(
            stage_selection(&db, &blocked).await,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation)
        ));

        let mut unfinished = intent;
        unfinished.phase = work::TextUploadPhase::Uploaded;
        db.put(
            scoped_key(
                DataScope::LegacyUnscoped,
                index_keys::IndexV2Key::TextUploadIntent(TextIntentOwnedKey {
                    index_id: unfinished.index_id,
                    generation: unfinished.generation,
                    intent_id: unfinished.intent_id,
                }),
            ),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextUploadIntent(
                Box::new(unfinished),
            )),
        )
        .await
        .unwrap();
        let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let blocked = select(
            &snapshot,
            DataScope::LegacyUnscoped,
            &operation,
            text_definition,
            &progress,
            SearchIndexBackfillLimits::default().batch(),
        )
        .await
        .unwrap();
        drop(snapshot);
        assert!(matches!(
            stage_selection(&db, &blocked).await,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation)
        ));
    }

    #[tokio::test]
    async fn activation_prerequisites_route_late_delta_and_block_orphan_physical_rows() {
        let definition = definition(None);
        let ValidatedDynamicIndexDefinition::Text(text_definition) = &definition else {
            panic!("test definition is text")
        };
        let progress = TextManifestValidationProgress::UploadIntents(PrefixScanProgress {
            cursor: None,
            counters: OperationCounters::default(),
        });
        let operation = operation(&definition, progress.clone());

        let delta_db = test_db("text-validation-late-delta").await;
        delta_db
            .put(
                scoped_key(
                    DataScope::LegacyUnscoped,
                    index_keys::IndexV2Key::BuildDelta(IndexEntityStateKey {
                        index_id: operation.index_id(),
                        generation: operation.generation(),
                        entity: IndexEntity {
                            kind: IndexElementKind::Node,
                            id: IndexEntityId::new(1),
                        },
                    }),
                ),
                Bytes::from_static(b"late-delta"),
            )
            .await
            .unwrap();
        let snapshot = delta_db.begin(IsolationLevel::Snapshot).await.unwrap();
        let selection = select(
            &snapshot,
            DataScope::LegacyUnscoped,
            &operation,
            text_definition,
            &progress,
            SearchIndexBackfillLimits::default().batch(),
        )
        .await
        .unwrap();
        drop(snapshot);
        assert!(matches!(
            stage_selection(&delta_db, &selection).await,
            IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
                TextBuildProgress::Constructing(TextBuildStage::CatchUp(_))
            ))
        ));

        for (name, key) in [
            (
                "text-validation-orphan-artifact",
                index_keys::IndexV2Key::TextBuildArtifact(index_keys::TextBuildArtifactKey {
                    root: root_key(&operation, &TextPartition::Unpartitioned),
                    ordinal: 0,
                }),
            ),
            (
                "text-validation-orphan-proof",
                index_keys::IndexV2Key::ActiveMutationCommitProof(TextIntentOwnedKey {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    intent_id: TextUploadIntentId::from_bytes([0x51; 16]).unwrap(),
                }),
            ),
        ] {
            let db = test_db(name).await;
            db.put(
                scoped_key(DataScope::LegacyUnscoped, key),
                Bytes::from_static(b"orphan"),
            )
            .await
            .unwrap();
            let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
            let selection = select(
                &snapshot,
                DataScope::LegacyUnscoped,
                &operation,
                text_definition,
                &progress,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await
            .unwrap();
            drop(snapshot);
            assert!(matches!(
                stage_selection(&db, &selection).await,
                IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation)
            ));
        }
    }

    #[tokio::test]
    async fn validation_input_limits_and_counters_fail_before_external_work() {
        let db = test_db("text-validation-input-limit").await;
        let definition = definition(None);
        let ValidatedDynamicIndexDefinition::Text(text_definition) = &definition else {
            panic!("test definition is text")
        };
        let progress = TextManifestValidationProgress::initial(OperationCounters::default());
        let build_operation = operation(&definition, progress.clone());
        put_manifest(
            &db,
            &build_operation,
            ManifestFixture {
                partition: TextPartition::Unpartitioned,
                revision: TextManifestRevision::new(2).unwrap(),
                page_number: 0,
                root_page_count: 1,
                root_split_count: 1,
                entries: vec![exact_split(0x61)],
                include_reachability: true,
            },
        )
        .await;
        let default = SearchIndexBackfillLimits::default().batch();
        let tiny = SearchIndexBatchLimits::try_new(
            NonZeroUsize::MIN,
            NonZeroU64::MIN,
            default.max_output_operations(),
            default.max_output_bytes(),
            default.max_single_vector_output_bytes(),
        )
        .unwrap();
        let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let selection = select(
            &snapshot,
            DataScope::LegacyUnscoped,
            &build_operation,
            text_definition,
            &progress,
            tiny,
        )
        .await
        .unwrap();
        drop(snapshot);
        assert!(matches!(
            stage_selection(&db, &selection).await,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::ManifestLimit {
                limit: 1,
                ..
            })
        ));

        let overflow = TextManifestValidationProgress::initial(OperationCounters {
            input_bytes: u64::MAX,
            ..OperationCounters::default()
        });
        let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(matches!(
            select(
                &snapshot,
                DataScope::LegacyUnscoped,
                &build_operation,
                text_definition,
                &overflow,
                default,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("input counter overflowed")
        ));

        let root_progress = TextManifestValidationProgress::Roots(PrefixScanProgress {
            cursor: None,
            counters: OperationCounters::default(),
        });
        let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let root_limit = select(
            &snapshot,
            DataScope::LegacyUnscoped,
            &build_operation,
            text_definition,
            &root_progress,
            tiny,
        )
        .await
        .unwrap();
        drop(snapshot);
        assert!(matches!(
            stage_selection(&db, &root_limit).await,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::ManifestLimit {
                limit: 1,
                ..
            })
        ));

        let root_overflow = TextManifestValidationProgress::Roots(PrefixScanProgress {
            cursor: None,
            counters: OperationCounters {
                input_bytes: u64::MAX,
                ..OperationCounters::default()
            },
        });
        let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(matches!(
            select(
                &snapshot,
                DataScope::LegacyUnscoped,
                &build_operation,
                text_definition,
                &root_overflow,
                default,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("root-validation input counter overflowed")
        ));

        let intent_db = test_db("text-validation-intent-limits").await;
        let intent_progress = TextManifestValidationProgress::UploadIntents(PrefixScanProgress {
            cursor: None,
            counters: OperationCounters::default(),
        });
        let intent_operation = operation(&definition, intent_progress.clone());
        let intent = reclaimable_intent(&intent_operation, exact_split(0x62));
        put_intent_candidate(&intent_db, &intent_operation, &intent).await;
        let snapshot = intent_db.begin(IsolationLevel::Snapshot).await.unwrap();
        let intent_limit = select(
            &snapshot,
            DataScope::LegacyUnscoped,
            &intent_operation,
            text_definition,
            &intent_progress,
            tiny,
        )
        .await
        .unwrap();
        drop(snapshot);
        assert!(matches!(
            stage_selection(&intent_db, &intent_limit).await,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::ManifestLimit {
                limit: 1,
                ..
            })
        ));

        let intent_overflow = TextManifestValidationProgress::UploadIntents(PrefixScanProgress {
            cursor: None,
            counters: OperationCounters {
                input_bytes: u64::MAX,
                ..OperationCounters::default()
            },
        });
        let snapshot = intent_db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(matches!(
            select(
                &snapshot,
                DataScope::LegacyUnscoped,
                &intent_operation,
                text_definition,
                &intent_overflow,
                default,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("upload-validation input counter overflowed")
        ));
    }
}
