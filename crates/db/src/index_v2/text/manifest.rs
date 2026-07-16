//! Bounded artifact-to-manifest relocation for V2 text builds.
//!
//! Each step scans only the generation-owned build-artifact prefix, selects a
//! contiguous run from one canonical partition, and prepares one immutable
//! manifest page plus its revisioned root. The prepared value retains every
//! source/destination observation so repository dispatch can reject stale work
//! before staging any write.
//!
//! Blob reachability moves in the same transaction as ownership: destination
//! page-slot references are put before exact artifact owners and their global
//! references are deleted. The driver holds the process-local GC permit and
//! coordinator reference guards across that commit; this module owns only the
//! typed database rows and bounded measurements.

use std::collections::HashSet;
use std::num::NonZeroU32;
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
    IndexCursor, IndexOperationBlocker, IndexOperationRecord, PrefixScanProgress,
    TextManifestRevision,
};

/// Exact row value observed while preparing one page outside its commit.
#[derive(Debug, Clone)]
pub(super) struct RowObservation {
    pub(super) key: Bytes,
    pub(super) value: Option<Bytes>,
}

/// Exact ordered artifact rows retained for serializable range revalidation.
#[derive(Debug)]
pub(super) struct PreparedArtifactRange {
    prefix: Bytes,
    start: Bound<Bytes>,
    end: Bound<Bytes>,
    rows: Vec<(Bytes, Bytes)>,
}

impl PreparedArtifactRange {
    /// Re-scans the prepared source interval inside the commit transaction.
    ///
    /// Comparing the full ordered row sequence prevents an artifact inserted
    /// before the prepared cursor from being skipped. The transaction retains
    /// the range read so a later concurrent insertion conflicts at commit.
    pub(super) async fn is_current(&self, transaction: &DbTransaction) -> Result<bool> {
        let mut current = transaction
            .scan_prefix(&self.prefix, (self.start.clone(), self.end.clone()))
            .await?;
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

/// One bounded decision from the strict artifact resume cursor.
#[derive(Debug)]
pub(super) enum ManifestSelection {
    /// No artifact remains after the strict cursor.
    Exhausted(PreparedArtifactRange),
    /// The first indivisible page transition cannot fit a configured limit.
    Blocked {
        blocker: IndexOperationBlocker,
        range: PreparedArtifactRange,
        observations: Vec<RowObservation>,
    },
    /// One non-empty page and exact ownership relocation can commit atomically.
    Page(PreparedManifestPage),
}

/// Complete typed writes and observations for one immutable manifest page.
#[derive(Debug)]
pub(super) struct PreparedManifestPage {
    completed_cursor: IndexCursor,
    range: PreparedArtifactRange,
    observations: Vec<RowObservation>,
    puts: Vec<(Bytes, Bytes)>,
    deletes: Vec<Bytes>,
    blobs: Vec<work::BlobRef>,
    input_bytes: u64,
    output_operations: u64,
    output_bytes: u64,
}

impl PreparedManifestPage {
    /// Returns the last exact artifact incorporated by this page.
    pub(super) fn completed_cursor(&self) -> &IndexCursor {
        &self.completed_cursor
    }

    /// Returns every distinct blob that needs a coordinator reference guard.
    pub(super) fn blobs(&self) -> &[work::BlobRef] {
        &self.blobs
    }

    /// Returns the exact observed source bytes charged to operation counters.
    pub(super) const fn input_bytes(&self) -> u64 {
        self.input_bytes
    }

    /// Returns the exact put/delete count charged to operation counters.
    pub(super) const fn output_operations(&self) -> u64 {
        self.output_operations
    }

    /// Returns the exact encoded key/value bytes charged to operation counters.
    pub(super) const fn output_bytes(&self) -> u64 {
        self.output_bytes
    }

    /// Revalidates every source/destination row before staging the closed write set.
    pub(super) async fn stage(&self, transaction: &DbTransaction) -> Result<bool> {
        if !self.range.is_current(transaction).await? {
            return Ok(false);
        }
        for observation in &self.observations {
            if transaction.get(&observation.key).await? != observation.value {
                return Ok(false);
            }
        }
        for (key, value) in &self.puts {
            transaction.put(key, value)?;
        }
        for key in &self.deletes {
            transaction.delete(key)?;
        }
        Ok(true)
    }
}

/// Selects and prepares one non-empty contiguous manifest page.
pub(super) async fn select_page(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &PrefixScanProgress,
    batch_limits: SearchIndexBatchLimits,
    manifest_limits: TextBackfillCompactionLimits,
) -> Result<ManifestSelection> {
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
                    "text manifest cursor is outside its exact artifact prefix",
                ));
            };
            Bound::Excluded(Bytes::copy_from_slice(suffix))
        }
        None => Bound::Unbounded,
    };
    let mut rows = transaction
        .scan_prefix(&prefix, (start.clone(), Bound::Unbounded))
        .await?;
    let Some(first_row) = rows.next().await? else {
        return Ok(ManifestSelection::Exhausted(PreparedArtifactRange {
            prefix,
            start,
            end: Bound::Unbounded,
            rows: Vec::new(),
        }));
    };
    let first_suffix = Bytes::copy_from_slice(
        first_row
            .key
            .strip_prefix(prefix.as_ref())
            .expect("a prefix scan returns a key inside its requested prefix"),
    );
    let (first_key, first_artifact) = super::attachment::decode_build_artifact(
        scope,
        operation,
        &first_row.key,
        &first_row.value,
    )?;
    let root_key = scoped_key(
        scope,
        index_keys::IndexV2Key::TextManifestRoot(first_key.root),
    );
    let existing_root_value = transaction.get(&root_key).await?;
    let existing_root = match existing_root_value.as_ref() {
        Some(value) => {
            let index_values::IndexV2WorkValue::TextManifestRoot(root) =
                index_values::decode_work_value(value)?
            else {
                return Err(corruption(
                    "text manifest root key contains another typed value kind",
                ));
            };
            if root.index_id() != operation.index_id()
                || root.generation() != operation.generation()
                || root.partition() != &first_artifact.partition
                || first_key.root.partition != root.partition().fingerprint()
            {
                return Err(corruption(
                    "text manifest root key/value ownership mismatch",
                ));
            }
            Some(root)
        }
        None => None,
    };
    let page = existing_root
        .as_ref()
        .map_or(0, work::TextManifestRootValue::page_count);
    let page_key_typed = index_keys::TextManifestPageKey {
        root: first_key.root,
        page,
    };
    let page_key = scoped_key(
        scope,
        index_keys::IndexV2Key::TextManifestPage(page_key_typed),
    );
    let existing_page = transaction.get(&page_key).await?;
    if existing_page.is_some() {
        return Err(corruption(
            "text manifest next contiguous page destination is occupied",
        ));
    }

    let root_template = match existing_root.as_ref() {
        Some(root) => match root.append_page(page, NonZeroU32::MIN) {
            Ok(root) => root,
            Err(work::IndexWorkModelError::ManifestPageCountExhausted) => {
                return Ok(ManifestSelection::Blocked {
                    blocker: IndexOperationBlocker::ManifestLimit {
                        partition: first_artifact.partition,
                        observed: u64::from(u32::MAX) + 1,
                        limit: u64::from(u32::MAX),
                    },
                    range: PreparedArtifactRange {
                        prefix,
                        start,
                        end: Bound::Included(first_suffix),
                        rows: vec![(first_row.key, first_row.value)],
                    },
                    observations: vec![
                        RowObservation {
                            key: root_key,
                            value: existing_root_value,
                        },
                        RowObservation {
                            key: page_key,
                            value: None,
                        },
                    ],
                });
            }
            Err(work::IndexWorkModelError::ManifestRevisionExhausted) => {
                return Ok(ManifestSelection::Blocked {
                    blocker: IndexOperationBlocker::InvariantViolation,
                    range: PreparedArtifactRange {
                        prefix,
                        start,
                        end: Bound::Included(first_suffix),
                        rows: vec![(first_row.key, first_row.value)],
                    },
                    observations: vec![
                        RowObservation {
                            key: root_key,
                            value: existing_root_value,
                        },
                        RowObservation {
                            key: page_key,
                            value: None,
                        },
                    ],
                });
            }
            Err(error) => {
                return Err(HelixDbError::IndexCatalogCorruption(format!(
                    "validated text manifest root rejected its contiguous page: {error}"
                )));
            }
        },
        None => work::TextManifestRootValue::try_new(
            operation.index_id(),
            operation.generation(),
            first_artifact.partition.clone(),
            TextManifestRevision::initial(),
            1,
            1,
        )
        .expect("one initial page and split form a valid manifest root"),
    };
    let encoded_root_template = index_values::encode_work_value(
        &index_values::IndexV2WorkValue::TextManifestRoot(root_template),
    );
    let one_entry_page =
        index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestPage(
            work::TextManifestPageValue::try_new(
                operation.index_id(),
                operation.generation(),
                first_artifact.partition.clone(),
                page,
                vec![first_artifact.split],
            )
            .expect("one validated split forms a non-empty manifest page"),
        ));
    let two_entry_page =
        index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestPage(
            work::TextManifestPageValue::try_new(
                operation.index_id(),
                operation.generation(),
                first_artifact.partition.clone(),
                page,
                vec![first_artifact.split, first_artifact.split],
            )
            .expect("two validated splits form a bounded manifest page"),
        ));
    let split_entry_bytes =
        u64::try_from(two_entry_page.len() - one_entry_page.len()).unwrap_or(u64::MAX);
    let page_base_bytes = u64::try_from(one_entry_page.len())
        .unwrap_or(u64::MAX)
        .saturating_sub(split_entry_bytes);
    let mut observations = vec![
        RowObservation {
            key: root_key.clone(),
            value: existing_root_value,
        },
        RowObservation {
            key: page_key.clone(),
            value: None,
        },
    ];
    let mut artifact_rows = Vec::new();
    let mut entries = Vec::new();
    let mut reachability_puts = Vec::new();
    let mut deletes = Vec::new();
    let mut blobs = Vec::new();
    let mut distinct_blobs = HashSet::new();
    let mut input_bytes = row_bytes(&root_key, observations[0].value.as_ref())
        .saturating_add(row_bytes(&page_key, None));
    let mut output_operations = 2_u64;
    let mut output_bytes = row_bytes(&root_key, Some(&encoded_root_template)).saturating_add(
        u64::try_from(page_key.len())
            .unwrap_or(u64::MAX)
            .saturating_add(page_base_bytes),
    );
    let entry_limit = batch_limits
        .max_entities()
        .get()
        .min(work::TextManifestPageValue::MAX_ENTRIES);
    let partition = first_artifact.partition.clone();
    let mut completed_cursor = None;
    let mut next_row = Some((first_row, first_key, first_artifact));

    while let Some((row, key, artifact)) = next_row.take() {
        if key.root.partition != first_key.root.partition {
            break;
        }
        if artifact.partition != partition {
            return Err(corruption(
                "text manifest partition fingerprint collision changed canonical ownership",
            ));
        }
        let slot = u32::try_from(entries.len()).expect("bounded manifest page slot fits u32");
        let (artifact_reference_key, artifact_reference_value) =
            super::attachment::build_artifact_reachability_row(artifact.split.blob(), scope, key);
        let existing_artifact_reference = transaction.get(&artifact_reference_key).await?;
        if existing_artifact_reference.as_deref() != Some(artifact_reference_value.as_ref()) {
            return Err(corruption(
                "text manifest artifact is missing its exact global reachability row",
            ));
        }
        let (manifest_reference_key, manifest_reference_value) =
            super::attachment::manifest_page_reachability_row(
                artifact.split.blob(),
                scope,
                page_key_typed,
                slot,
            );
        let existing_manifest_reference = transaction.get(&manifest_reference_key).await?;
        if existing_manifest_reference.is_some() {
            return Err(corruption(
                "text manifest destination reachability slot is occupied",
            ));
        }

        let candidate_input_bytes = input_bytes
            .saturating_add(row_bytes(&row.key, Some(&row.value)))
            .saturating_add(row_bytes(
                &artifact_reference_key,
                existing_artifact_reference.as_ref(),
            ))
            .saturating_add(row_bytes(
                &manifest_reference_key,
                existing_manifest_reference.as_ref(),
            ));
        let candidate_page_bytes = page_base_bytes.saturating_add(
            split_entry_bytes.saturating_mul(u64::try_from(entries.len() + 1).unwrap_or(u64::MAX)),
        );
        let candidate_output_operations = output_operations.saturating_add(3);
        let candidate_output_bytes = output_bytes
            .saturating_add(split_entry_bytes)
            .saturating_add(row_bytes(
                &manifest_reference_key,
                Some(&manifest_reference_value),
            ))
            .saturating_add(u64::try_from(row.key.len()).unwrap_or(u64::MAX))
            .saturating_add(u64::try_from(artifact_reference_key.len()).unwrap_or(u64::MAX));
        let exceeded = [
            (candidate_input_bytes, batch_limits.max_input_bytes().get()),
            (
                candidate_page_bytes,
                manifest_limits.max_manifest_bytes().get(),
            ),
            (
                candidate_output_operations,
                batch_limits.max_output_operations().get(),
            ),
            (
                candidate_output_bytes,
                batch_limits.max_output_bytes().get(),
            ),
        ]
        .into_iter()
        .find(|(observed, limit)| observed > limit);
        if let Some((observed, limit)) = exceeded {
            if entries.is_empty() {
                let blocked_end = Bound::Included(Bytes::copy_from_slice(
                    row.key
                        .strip_prefix(prefix.as_ref())
                        .expect("a prefix scan row retains its requested prefix"),
                ));
                observations.extend([
                    RowObservation {
                        key: artifact_reference_key,
                        value: existing_artifact_reference,
                    },
                    RowObservation {
                        key: manifest_reference_key,
                        value: existing_manifest_reference,
                    },
                ]);
                return Ok(ManifestSelection::Blocked {
                    blocker: IndexOperationBlocker::ManifestLimit {
                        partition,
                        observed,
                        limit,
                    },
                    range: PreparedArtifactRange {
                        prefix,
                        start,
                        end: blocked_end,
                        rows: vec![(row.key, row.value)],
                    },
                    observations,
                });
            }
            break;
        }

        observations.extend([
            RowObservation {
                key: artifact_reference_key.clone(),
                value: existing_artifact_reference,
            },
            RowObservation {
                key: manifest_reference_key.clone(),
                value: existing_manifest_reference,
            },
        ]);
        artifact_rows.push((row.key.clone(), row.value));
        entries.push(artifact.split);
        reachability_puts.push((manifest_reference_key, manifest_reference_value));
        deletes.push(row.key.clone());
        deletes.push(artifact_reference_key);
        if distinct_blobs.insert(artifact.split.blob()) {
            blobs.push(artifact.split.blob());
        }
        input_bytes = candidate_input_bytes;
        output_operations = candidate_output_operations;
        output_bytes = candidate_output_bytes;
        completed_cursor = Some(
            IndexCursor::try_new(row.key)
                .expect("a decoded bounded artifact key is a valid operation cursor"),
        );
        if entries.len() == entry_limit {
            break;
        }
        let Some(row) = rows.next().await? else {
            break;
        };
        let (key, artifact) =
            super::attachment::decode_build_artifact(scope, operation, &row.key, &row.value)?;
        next_row = Some((row, key, artifact));
    }

    let entry_count = NonZeroU32::new(
        u32::try_from(entries.len()).expect("bounded non-empty manifest page fits u32"),
    )
    .expect("a prepared manifest page contains at least one entry");
    let root = match existing_root {
        Some(root) => root
            .append_page(page, entry_count)
            .expect("the prevalidated root accepts the admitted bounded page"),
        None => work::TextManifestRootValue::try_new(
            operation.index_id(),
            operation.generation(),
            partition.clone(),
            TextManifestRevision::initial(),
            1,
            u64::from(entry_count.get()),
        )
        .expect("one initial bounded page forms a valid manifest root"),
    };
    let page_value = work::TextManifestPageValue::try_new(
        operation.index_id(),
        operation.generation(),
        partition.clone(),
        page,
        entries,
    )
    .expect("admitted validated splits form one non-empty bounded page");
    let encoded_root =
        index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestRoot(root));
    let encoded_page = index_values::encode_work_value(
        &index_values::IndexV2WorkValue::TextManifestPage(page_value),
    );
    if u64::try_from(encoded_page.len()).unwrap_or(u64::MAX)
        > manifest_limits.max_manifest_bytes().get()
    {
        return Err(corruption(
            "prepared text manifest page exceeded its admitted byte bound",
        ));
    }
    let mut puts = Vec::with_capacity(reachability_puts.len() + 2);
    puts.push((root_key, encoded_root));
    puts.push((page_key, encoded_page));
    puts.extend(reachability_puts);
    let completed_cursor = completed_cursor.expect("one admitted entry has one exact cursor");
    let completed_suffix = Bytes::copy_from_slice(
        completed_cursor
            .as_bytes()
            .strip_prefix(prefix.as_ref())
            .expect("a prepared artifact cursor retains its generation prefix"),
    );
    Ok(ManifestSelection::Page(PreparedManifestPage {
        completed_cursor,
        range: PreparedArtifactRange {
            prefix,
            start,
            end: Bound::Included(completed_suffix),
            rows: artifact_rows,
        },
        observations,
        puts,
        deletes,
        blobs,
        input_bytes,
        output_operations,
        output_bytes,
    }))
}

/// Measures one key plus its optional observed/encoded value without overflow.
fn row_bytes(key: &[u8], value: Option<&Bytes>) -> u64 {
    u64::try_from(key.len().saturating_add(value.map_or(0, Bytes::len))).unwrap_or(u64::MAX)
}

/// Encodes one scoped V2 key through the canonical `encoding/v1` boundary.
fn scoped_key(scope: DataScope, key: index_keys::IndexV2Key) -> Bytes {
    Key::Data {
        scope,
        kind: DataKeyKind::IndexV2(key),
    }
    .to_bytes()
}

/// Converts a violated persisted manifest contract into the public DB error boundary.
fn corruption(reason: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(reason.into())
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::sync::Arc;

    use sha2::{Digest, Sha256};
    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::index_v2::{
        IndexComponent, IndexElementKind, IndexGenerationId, IndexId, IndexIdentity,
        IndexIdentityFamily, IndexOperationExecutionState, IndexOperationFamily, IndexOperationId,
        IndexOperationKind, IndexOperationProgress, IndexOperationRevision, IndexRevision,
        OperationCounters, TextBuildProgress, TextBuildStage, TextUploadIntentId,
    };

    async fn test_db(name: &str) -> Db {
        Db::open(name, Arc::new(InMemory::new()))
            .await
            .expect("manifest contract database opens")
    }

    fn operation() -> IndexOperationRecord {
        let identity = IndexIdentity::new(
            IndexIdentityFamily::Text,
            IndexElementKind::Node,
            IndexComponent::try_new("label", "Document").unwrap(),
            IndexComponent::try_new("property", "body").unwrap(),
        );
        IndexOperationRecord::try_new(
            IndexOperationId::from_bytes([1; 16]).unwrap(),
            IndexId::initial(),
            identity,
            IndexGenerationId::initial(),
            IndexRevision::initial(),
            IndexOperationRevision::initial(),
            IndexOperationKind::Build,
            IndexOperationFamily::Text,
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::PrepareManifests(PrefixScanProgress {
                    cursor: None,
                    counters: OperationCounters::default(),
                }),
            )),
            0,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap()
    }

    fn split(seed: u8) -> work::SplitRef {
        let payload = [seed; 100];
        work::SplitRef::try_new(
            work::BlobRef::new(Sha256::digest(payload).into(), payload.len() as u64),
            80,
            20,
            10,
            payload.len() as u64,
        )
        .unwrap()
    }

    async fn put_artifact(
        db: &Db,
        scope: DataScope,
        operation: &IndexOperationRecord,
        partition: work::TextPartition,
        ordinal: u32,
        split: work::SplitRef,
    ) -> (Bytes, Bytes, Bytes) {
        let key = index_keys::TextBuildArtifactKey {
            root: index_keys::TextManifestRootKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                partition: partition.fingerprint(),
            },
            ordinal,
        };
        let encoded_key = scoped_key(scope, index_keys::IndexV2Key::TextBuildArtifact(key));
        let encoded_value = index_values::encode_work_value(
            &index_values::IndexV2WorkValue::TextBuildArtifact(work::TextBuildArtifactValue {
                index_id: operation.index_id(),
                generation: operation.generation(),
                partition,
                artifact_ordinal: ordinal,
                split,
                source_intent_id: TextUploadIntentId::from_bytes(
                    [u8::try_from(ordinal).unwrap_or(u8::MAX).saturating_add(2); 16],
                )
                .unwrap(),
            }),
        );
        let (reference_key, reference_value) =
            super::super::attachment::build_artifact_reachability_row(split.blob(), scope, key);
        db.put(&encoded_key, &encoded_value).await.unwrap();
        db.put(&reference_key, reference_value).await.unwrap();
        (encoded_key, encoded_value, reference_key)
    }

    fn default_limits() -> (SearchIndexBatchLimits, TextBackfillCompactionLimits) {
        let limits = crate::config::SearchIndexBackfillLimits::default();
        (limits.batch(), limits.text_compaction())
    }

    fn batch_limits(
        entities: usize,
        input_bytes: u64,
        output_operations: u64,
        output_bytes: u64,
    ) -> SearchIndexBatchLimits {
        SearchIndexBatchLimits::try_new(
            NonZeroUsize::new(entities).unwrap(),
            NonZeroU64::new(input_bytes).unwrap(),
            NonZeroU64::new(output_operations).unwrap(),
            NonZeroU64::new(output_bytes).unwrap(),
            NonZeroU64::new(output_bytes).unwrap(),
        )
        .unwrap()
    }

    fn manifest_limits(bytes: u64) -> TextBackfillCompactionLimits {
        let current = crate::config::SearchIndexBackfillLimits::default().text_compaction();
        TextBackfillCompactionLimits::new(
            current.max_fan_in(),
            current.max_input_bytes(),
            current.max_temporary_disk_bytes(),
            current.max_output_blob_bytes(),
            NonZeroU64::new(bytes).unwrap(),
        )
    }

    fn decode_root(value: &[u8]) -> work::TextManifestRootValue {
        let index_values::IndexV2WorkValue::TextManifestRoot(root) =
            index_values::decode_work_value(value).unwrap()
        else {
            panic!("root key contains a root value")
        };
        root
    }

    fn decode_page(value: &[u8]) -> work::TextManifestPageValue {
        let index_values::IndexV2WorkValue::TextManifestPage(page) =
            index_values::decode_work_value(value).unwrap()
        else {
            panic!("page key contains a page value")
        };
        page
    }

    #[tokio::test]
    async fn page_relocation_atomically_transfers_exact_artifact_reachability() {
        let db = test_db("manifest-relocation").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let partition = work::TextPartition::Unpartitioned;
        let first_split = split(3);
        let second_split = split(4);
        let (first_key, _, first_reference) =
            put_artifact(&db, scope, &operation, partition.clone(), 0, first_split).await;
        let (second_key, _, second_reference) =
            put_artifact(&db, scope, &operation, partition.clone(), 1, second_split).await;
        let (batch, manifest) = default_limits();
        let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let ManifestSelection::Page(prepared) = select_page(
            &snapshot,
            scope,
            &operation,
            &PrefixScanProgress {
                cursor: None,
                counters: OperationCounters::default(),
            },
            batch,
            manifest,
        )
        .await
        .unwrap() else {
            panic!("two artifacts prepare one page")
        };
        drop(snapshot);
        assert_eq!(prepared.blobs(), &[first_split.blob(), second_split.blob()]);
        assert_eq!(prepared.output_operations(), 8);

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(prepared.stage(&transaction).await.unwrap());
        transaction.commit().await.unwrap();

        let root_typed = index_keys::TextManifestRootKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            partition: partition.fingerprint(),
        };
        let root_key = scoped_key(scope, index_keys::IndexV2Key::TextManifestRoot(root_typed));
        let root = decode_root(&db.get(&root_key).await.unwrap().unwrap());
        assert_eq!(root.revision(), TextManifestRevision::initial());
        assert_eq!(root.page_count(), 1);
        assert_eq!(root.split_count(), 2);
        let page_typed = index_keys::TextManifestPageKey {
            root: root_typed,
            page: 0,
        };
        let page_key = scoped_key(scope, index_keys::IndexV2Key::TextManifestPage(page_typed));
        let page = decode_page(&db.get(&page_key).await.unwrap().unwrap());
        assert_eq!(page.entries(), &[first_split, second_split]);
        assert!(db.get(first_key).await.unwrap().is_none());
        assert!(db.get(second_key).await.unwrap().is_none());
        assert!(db.get(first_reference).await.unwrap().is_none());
        assert!(db.get(second_reference).await.unwrap().is_none());
        for (slot, split) in [first_split, second_split].into_iter().enumerate() {
            let (key, value) = super::super::attachment::manifest_page_reachability_row(
                split.blob(),
                scope,
                page_typed,
                u32::try_from(slot).unwrap(),
            );
            assert_eq!(db.get(key).await.unwrap().as_deref(), Some(value.as_ref()));
        }

        let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(matches!(
            select_page(
                &snapshot,
                scope,
                &operation,
                &PrefixScanProgress {
                    cursor: Some(prepared.completed_cursor().clone()),
                    counters: OperationCounters::default(),
                },
                batch,
                manifest,
            )
            .await
            .unwrap(),
            ManifestSelection::Exhausted(_)
        ));
    }

    #[tokio::test]
    async fn pages_are_bounded_resumable_and_root_revisions_remain_contiguous() {
        let db = test_db("manifest-paging").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let partition = work::TextPartition::Unpartitioned;
        for ordinal in 0..3 {
            put_artifact(
                &db,
                scope,
                &operation,
                partition.clone(),
                ordinal,
                split(u8::try_from(ordinal).unwrap().saturating_add(10)),
            )
            .await;
        }
        let (_, manifest) = default_limits();
        let batch = batch_limits(1, u64::MAX, u64::MAX, u64::MAX);
        let mut cursor = None;
        for expected_page in 0..3 {
            let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
            let ManifestSelection::Page(prepared) = select_page(
                &snapshot,
                scope,
                &operation,
                &PrefixScanProgress {
                    cursor: cursor.clone(),
                    counters: OperationCounters::default(),
                },
                batch,
                manifest,
            )
            .await
            .unwrap() else {
                panic!("one admitted artifact prepares one page")
            };
            drop(snapshot);
            cursor = Some(prepared.completed_cursor().clone());
            let transaction = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .unwrap();
            assert!(prepared.stage(&transaction).await.unwrap());
            transaction.commit().await.unwrap();

            let root = index_keys::TextManifestRootKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                partition: partition.fingerprint(),
            };
            let page_key = scoped_key(
                scope,
                index_keys::IndexV2Key::TextManifestPage(index_keys::TextManifestPageKey {
                    root,
                    page: expected_page,
                }),
            );
            assert_eq!(
                decode_page(&db.get(page_key).await.unwrap().unwrap()).page(),
                expected_page
            );
        }
        let root_key = scoped_key(
            scope,
            index_keys::IndexV2Key::TextManifestRoot(index_keys::TextManifestRootKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                partition: partition.fingerprint(),
            }),
        );
        let root = decode_root(&db.get(root_key).await.unwrap().unwrap());
        assert_eq!(root.revision().get(), 3);
        assert_eq!(root.page_count(), 3);
        assert_eq!(root.split_count(), 3);
    }

    #[tokio::test]
    async fn first_indivisible_page_blocks_at_every_configured_transaction_limit() {
        let db = test_db("manifest-limits").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        put_artifact(
            &db,
            scope,
            &operation,
            work::TextPartition::Unpartitioned,
            0,
            split(20),
        )
        .await;
        let (default_batch, default_manifest) = default_limits();
        let cases = [
            (batch_limits(1, 1, u64::MAX, u64::MAX), default_manifest),
            (batch_limits(1, u64::MAX, 1, u64::MAX), default_manifest),
            (batch_limits(1, u64::MAX, u64::MAX, 1), default_manifest),
            (default_batch, manifest_limits(1)),
        ];
        for (batch, manifest) in cases {
            let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
            let ManifestSelection::Blocked {
                blocker:
                    IndexOperationBlocker::ManifestLimit {
                        observed, limit, ..
                    },
                range,
                observations,
            } = select_page(
                &snapshot,
                scope,
                &operation,
                &PrefixScanProgress {
                    cursor: None,
                    counters: OperationCounters::default(),
                },
                batch,
                manifest,
            )
            .await
            .unwrap()
            else {
                panic!("indivisible first entry blocks at the selected limit")
            };
            assert!(observed > limit);
            assert_eq!(range.rows.len(), 1);
            assert!(observations.len() >= 4);
        }
    }

    #[tokio::test]
    async fn later_row_limit_stops_one_page_and_resumes_at_the_exact_artifact() {
        enum LimitedResource {
            InputBytes,
            ManifestBytes,
            OutputOperations,
            OutputBytes,
        }

        for (resource, database_name) in [
            (LimitedResource::InputBytes, "manifest-later-input-limit"),
            (LimitedResource::ManifestBytes, "manifest-later-page-limit"),
            (
                LimitedResource::OutputOperations,
                "manifest-later-operation-limit",
            ),
            (LimitedResource::OutputBytes, "manifest-later-output-limit"),
        ] {
            let db = test_db(database_name).await;
            let scope = DataScope::LegacyUnscoped;
            let operation = operation();
            let partition = work::TextPartition::Unpartitioned;
            let first_split = split(21);
            let second_split = split(22);
            let (first_key, _, _) =
                put_artifact(&db, scope, &operation, partition.clone(), 0, first_split).await;
            let (second_key, _, _) =
                put_artifact(&db, scope, &operation, partition.clone(), 1, second_split).await;
            let (default_batch, default_manifest) = default_limits();
            let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
            let ManifestSelection::Page(one_entry) = select_page(
                &snapshot,
                scope,
                &operation,
                &PrefixScanProgress {
                    cursor: None,
                    counters: OperationCounters::default(),
                },
                batch_limits(1, u64::MAX, u64::MAX, u64::MAX),
                default_manifest,
            )
            .await
            .unwrap() else {
                panic!("the calibration page admits exactly one artifact")
            };
            drop(snapshot);
            let one_page_bytes = u64::try_from(
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestPage(
                    work::TextManifestPageValue::try_new(
                        operation.index_id(),
                        operation.generation(),
                        partition,
                        0,
                        vec![first_split],
                    )
                    .unwrap(),
                ))
                .len(),
            )
            .unwrap();
            let (batch, manifest) = match resource {
                LimitedResource::InputBytes => (
                    batch_limits(2, one_entry.input_bytes(), u64::MAX, u64::MAX),
                    default_manifest,
                ),
                LimitedResource::ManifestBytes => (default_batch, manifest_limits(one_page_bytes)),
                LimitedResource::OutputOperations => (
                    batch_limits(2, u64::MAX, one_entry.output_operations(), u64::MAX),
                    default_manifest,
                ),
                LimitedResource::OutputBytes => (
                    batch_limits(2, u64::MAX, u64::MAX, one_entry.output_bytes()),
                    default_manifest,
                ),
            };

            let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
            let ManifestSelection::Page(prepared) = select_page(
                &snapshot,
                scope,
                &operation,
                &PrefixScanProgress {
                    cursor: None,
                    counters: OperationCounters::default(),
                },
                batch,
                manifest,
            )
            .await
            .unwrap() else {
                panic!("the first row fits and the second row ends the page")
            };
            drop(snapshot);
            assert_eq!(
                prepared.completed_cursor(),
                &IndexCursor::try_new(first_key).unwrap()
            );
            assert_eq!(prepared.blobs(), &[first_split.blob()]);
            let transaction = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .unwrap();
            assert!(prepared.stage(&transaction).await.unwrap());
            transaction.commit().await.unwrap();

            let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
            let ManifestSelection::Page(resumed) = select_page(
                &snapshot,
                scope,
                &operation,
                &PrefixScanProgress {
                    cursor: Some(prepared.completed_cursor().clone()),
                    counters: OperationCounters::default(),
                },
                default_batch,
                default_manifest,
            )
            .await
            .unwrap() else {
                panic!("the next page resumes at the second artifact")
            };
            assert_eq!(
                resumed.completed_cursor(),
                &IndexCursor::try_new(second_key).unwrap()
            );
            assert_eq!(resumed.blobs(), &[second_split.blob()]);
        }
    }

    #[tokio::test]
    async fn stale_preparation_stages_nothing_and_corrupt_reachability_fails_closed() {
        let db = test_db("manifest-stale").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let split = split(30);
        let (artifact_key, artifact_value, reference_key) = put_artifact(
            &db,
            scope,
            &operation,
            work::TextPartition::Unpartitioned,
            0,
            split,
        )
        .await;
        let (batch, manifest) = default_limits();
        let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let ManifestSelection::Page(prepared) = select_page(
            &snapshot,
            scope,
            &operation,
            &PrefixScanProgress {
                cursor: None,
                counters: OperationCounters::default(),
            },
            batch,
            manifest,
        )
        .await
        .unwrap() else {
            panic!("one artifact prepares one page")
        };
        drop(snapshot);
        db.delete(&artifact_key).await.unwrap();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(!prepared.stage(&transaction).await.unwrap());
        transaction.commit().await.unwrap();
        let root_key = scoped_key(
            scope,
            index_keys::IndexV2Key::TextManifestRoot(index_keys::TextManifestRootKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                partition: work::TextPartition::Unpartitioned.fingerprint(),
            }),
        );
        assert!(db.get(root_key).await.unwrap().is_none());

        db.put(&artifact_key, artifact_value).await.unwrap();
        db.delete(reference_key).await.unwrap();
        let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(matches!(
            select_page(
                &snapshot,
                scope,
                &operation,
                &PrefixScanProgress {
                    cursor: None,
                    counters: OperationCounters::default(),
                },
                batch,
                manifest,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
    }

    #[tokio::test]
    async fn range_revalidation_rejects_phantoms_and_conflicts_with_late_insertion() {
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let partition = work::TextPartition::Unpartitioned;
        let (batch, manifest) = default_limits();

        let stale_db = test_db("manifest-range-phantom").await;
        put_artifact(
            &stale_db,
            scope,
            &operation,
            partition.clone(),
            1,
            split(31),
        )
        .await;
        let snapshot = stale_db.begin(IsolationLevel::Snapshot).await.unwrap();
        let ManifestSelection::Page(stale) = select_page(
            &snapshot,
            scope,
            &operation,
            &PrefixScanProgress {
                cursor: None,
                counters: OperationCounters::default(),
            },
            batch,
            manifest,
        )
        .await
        .unwrap() else {
            panic!("one artifact prepares one range-validated page")
        };
        drop(snapshot);
        put_artifact(
            &stale_db,
            scope,
            &operation,
            partition.clone(),
            0,
            split(32),
        )
        .await;
        let transaction = stale_db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(!stale.stage(&transaction).await.unwrap());

        let conflict_db = test_db("manifest-range-conflict").await;
        put_artifact(
            &conflict_db,
            scope,
            &operation,
            partition.clone(),
            1,
            split(33),
        )
        .await;
        let snapshot = conflict_db.begin(IsolationLevel::Snapshot).await.unwrap();
        let ManifestSelection::Page(conflicting) = select_page(
            &snapshot,
            scope,
            &operation,
            &PrefixScanProgress {
                cursor: None,
                counters: OperationCounters::default(),
            },
            batch,
            manifest,
        )
        .await
        .unwrap() else {
            panic!("one artifact prepares one serializable page")
        };
        drop(snapshot);
        let transaction = conflict_db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(conflicting.stage(&transaction).await.unwrap());
        put_artifact(
            &conflict_db,
            scope,
            &operation,
            partition.clone(),
            0,
            split(34),
        )
        .await;
        assert!(transaction.commit().await.is_err());
        let root_key = scoped_key(
            scope,
            index_keys::IndexV2Key::TextManifestRoot(index_keys::TextManifestRootKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                partition: partition.fingerprint(),
            }),
        );
        assert!(conflict_db.get(root_key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn shared_blob_uses_distinct_page_slots_but_one_runtime_guard_identity() {
        let db = test_db("manifest-shared-blob").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let shared = split(40);
        put_artifact(
            &db,
            scope,
            &operation,
            work::TextPartition::Unpartitioned,
            0,
            shared,
        )
        .await;
        put_artifact(
            &db,
            scope,
            &operation,
            work::TextPartition::Unpartitioned,
            1,
            shared,
        )
        .await;
        let (batch, manifest) = default_limits();
        let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let ManifestSelection::Page(prepared) = select_page(
            &snapshot,
            scope,
            &operation,
            &PrefixScanProgress {
                cursor: None,
                counters: OperationCounters::default(),
            },
            batch,
            manifest,
        )
        .await
        .unwrap() else {
            panic!("shared blob artifacts prepare one page")
        };
        assert_eq!(prepared.blobs(), &[shared.blob()]);
        let page_owner = index_keys::TextManifestPageKey {
            root: index_keys::TextManifestRootKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                partition: work::TextPartition::Unpartitioned.fingerprint(),
            },
            page: 0,
        };
        let first = super::super::attachment::manifest_page_reachability_row(
            shared.blob(),
            scope,
            page_owner,
            0,
        )
        .0;
        let second = super::super::attachment::manifest_page_reachability_row(
            shared.blob(),
            scope,
            page_owner,
            1,
        )
        .0;
        assert_ne!(first, second);
    }

    #[tokio::test]
    async fn pages_never_mix_canonical_partitions() {
        let db = test_db("manifest-partition-boundary").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let first_partition =
            work::TextPartition::try_tenant_value(Bytes::from_static(b"alpha")).unwrap();
        let second_partition =
            work::TextPartition::try_tenant_value(Bytes::from_static(b"beta")).unwrap();
        let first_split = split(41);
        let second_split = split(42);
        put_artifact(
            &db,
            scope,
            &operation,
            first_partition.clone(),
            0,
            first_split,
        )
        .await;
        put_artifact(
            &db,
            scope,
            &operation,
            second_partition.clone(),
            0,
            second_split,
        )
        .await;
        let mut expected_partitions = [first_partition, second_partition];
        expected_partitions.sort_by_key(work::TextPartition::fingerprint);
        let (batch, manifest) = default_limits();
        let mut cursor = None;
        for expected_partition in &expected_partitions {
            let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
            let ManifestSelection::Page(prepared) = select_page(
                &snapshot,
                scope,
                &operation,
                &PrefixScanProgress {
                    cursor: cursor.clone(),
                    counters: OperationCounters::default(),
                },
                batch,
                manifest,
            )
            .await
            .unwrap() else {
                panic!("each canonical partition prepares its own page");
            };
            drop(snapshot);
            cursor = Some(prepared.completed_cursor().clone());
            let transaction = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .unwrap();
            assert!(prepared.stage(&transaction).await.unwrap());
            transaction.commit().await.unwrap();

            let page_key = scoped_key(
                scope,
                index_keys::IndexV2Key::TextManifestPage(index_keys::TextManifestPageKey {
                    root: index_keys::TextManifestRootKey {
                        index_id: operation.index_id(),
                        generation: operation.generation(),
                        partition: expected_partition.fingerprint(),
                    },
                    page: 0,
                }),
            );
            let page = decode_page(&db.get(page_key).await.unwrap().unwrap());
            assert_eq!(page.partition(), expected_partition);
            assert_eq!(page.entries().len(), 1);
        }
    }

    #[tokio::test]
    async fn occupied_page_or_reachability_destination_fails_closed() {
        for occupied_page in [true, false] {
            let db = test_db(if occupied_page {
                "manifest-occupied-page"
            } else {
                "manifest-occupied-reference"
            })
            .await;
            let scope = DataScope::LegacyUnscoped;
            let operation = operation();
            let partition = work::TextPartition::Unpartitioned;
            let split = split(if occupied_page { 43 } else { 44 });
            put_artifact(&db, scope, &operation, partition.clone(), 0, split).await;
            let page_owner = index_keys::TextManifestPageKey {
                root: index_keys::TextManifestRootKey {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    partition: partition.fingerprint(),
                },
                page: 0,
            };
            if occupied_page {
                db.put(
                    scoped_key(scope, index_keys::IndexV2Key::TextManifestPage(page_owner)),
                    index_values::encode_work_value(
                        &index_values::IndexV2WorkValue::TextManifestPage(
                            work::TextManifestPageValue::try_new(
                                operation.index_id(),
                                operation.generation(),
                                partition,
                                0,
                                vec![split],
                            )
                            .unwrap(),
                        ),
                    ),
                )
                .await
                .unwrap();
            } else {
                let (key, value) = super::super::attachment::manifest_page_reachability_row(
                    split.blob(),
                    scope,
                    page_owner,
                    0,
                );
                db.put(key, value).await.unwrap();
            }

            let (batch, manifest) = default_limits();
            let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
            assert!(matches!(
                select_page(
                    &snapshot,
                    scope,
                    &operation,
                    &PrefixScanProgress {
                        cursor: None,
                        counters: OperationCounters::default(),
                    },
                    batch,
                    manifest,
                )
                .await,
                Err(HelixDbError::IndexCatalogCorruption(_))
            ));
        }
    }

    #[tokio::test]
    async fn corrupt_manifest_root_kind_or_ownership_fails_closed() {
        for wrong_value_kind in [true, false] {
            let db = test_db(if wrong_value_kind {
                "manifest-root-wrong-kind"
            } else {
                "manifest-root-wrong-owner"
            })
            .await;
            let scope = DataScope::LegacyUnscoped;
            let operation = operation();
            let partition = work::TextPartition::Unpartitioned;
            let split = split(if wrong_value_kind { 45 } else { 46 });
            put_artifact(&db, scope, &operation, partition.clone(), 0, split).await;
            let root_typed = index_keys::TextManifestRootKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                partition: partition.fingerprint(),
            };
            let root_key = scoped_key(scope, index_keys::IndexV2Key::TextManifestRoot(root_typed));
            let value = if wrong_value_kind {
                index_values::IndexV2WorkValue::TextManifestPage(
                    work::TextManifestPageValue::try_new(
                        operation.index_id(),
                        operation.generation(),
                        partition,
                        0,
                        vec![split],
                    )
                    .unwrap(),
                )
            } else {
                index_values::IndexV2WorkValue::TextManifestRoot(
                    work::TextManifestRootValue::try_new(
                        operation.index_id(),
                        operation.generation().checked_next().unwrap(),
                        partition,
                        TextManifestRevision::initial(),
                        1,
                        1,
                    )
                    .unwrap(),
                )
            };
            db.put(root_key, index_values::encode_work_value(&value))
                .await
                .unwrap();

            let (batch, manifest) = default_limits();
            let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
            assert!(matches!(
                select_page(
                    &snapshot,
                    scope,
                    &operation,
                    &PrefixScanProgress {
                        cursor: None,
                        counters: OperationCounters::default(),
                    },
                    batch,
                    manifest,
                )
                .await,
                Err(HelixDbError::IndexCatalogCorruption(_))
            ));
        }
    }

    #[tokio::test]
    async fn empty_invalid_cursor_and_exhausted_root_domains_are_closed() {
        let db = test_db("manifest-closed-domains").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let (batch, manifest) = default_limits();
        let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(matches!(
            select_page(
                &snapshot,
                scope,
                &operation,
                &PrefixScanProgress {
                    cursor: None,
                    counters: OperationCounters::default(),
                },
                batch,
                manifest,
            )
            .await
            .unwrap(),
            ManifestSelection::Exhausted(_)
        ));
        assert!(matches!(
            select_page(
                &snapshot,
                scope,
                &operation,
                &PrefixScanProgress {
                    cursor: Some(IndexCursor::try_new(Bytes::from_static(b"wrong")).unwrap()),
                    counters: OperationCounters::default(),
                },
                batch,
                manifest,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        drop(snapshot);

        let partition = work::TextPartition::Unpartitioned;
        put_artifact(&db, scope, &operation, partition.clone(), 0, split(50)).await;
        let root_key = scoped_key(
            scope,
            index_keys::IndexV2Key::TextManifestRoot(index_keys::TextManifestRootKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                partition: partition.fingerprint(),
            }),
        );
        db.put(
            &root_key,
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestRoot(
                work::TextManifestRootValue::try_new(
                    operation.index_id(),
                    operation.generation(),
                    partition.clone(),
                    TextManifestRevision::initial(),
                    u32::MAX,
                    u64::from(u32::MAX),
                )
                .unwrap(),
            )),
        )
        .await
        .unwrap();
        let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(matches!(
            select_page(
                &snapshot,
                scope,
                &operation,
                &PrefixScanProgress {
                    cursor: None,
                    counters: OperationCounters::default(),
                },
                batch,
                manifest,
            )
            .await
            .unwrap(),
            ManifestSelection::Blocked {
                blocker: IndexOperationBlocker::ManifestLimit { .. },
                ..
            }
        ));

        drop(snapshot);
        db.put(
            root_key,
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestRoot(
                work::TextManifestRootValue::try_new(
                    operation.index_id(),
                    operation.generation(),
                    partition,
                    TextManifestRevision::new(u64::MAX).unwrap(),
                    1,
                    1,
                )
                .unwrap(),
            )),
        )
        .await
        .unwrap();
        let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(matches!(
            select_page(
                &snapshot,
                scope,
                &operation,
                &PrefixScanProgress {
                    cursor: None,
                    counters: OperationCounters::default(),
                },
                batch,
                manifest,
            )
            .await
            .unwrap(),
            ManifestSelection::Blocked {
                blocker: IndexOperationBlocker::InvariantViolation,
                ..
            }
        ));
    }
}
