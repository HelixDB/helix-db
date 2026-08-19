//! Bounded artifact-to-manifest relocation for V2 text builds.
//!
//! Each step scans only the generation-owned build-artifact prefix, selects a
//! contiguous run from one canonical partition, and prepares one immutable
//! manifest page plus its revisioned root. The prepared value retains every
//! source/destination observation so repository dispatch can reject stale work
//! before staging any write.
//!
//! Artifact ownership moves to manifest rows in one transaction. Immutable blob
//! objects remain untouched.

use std::num::NonZeroU32;
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
    input_bytes: u64,
    output_operations: u64,
    output_bytes: u64,
    manifest_page_bytes: u64,
    manifest_root_bytes: u64,
}

impl PreparedManifestPage {
    /// Returns the last exact artifact incorporated by this page.
    pub(super) fn completed_cursor(&self) -> &IndexCursor {
        &self.completed_cursor
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

    /// Returns the exact encoded page row size staged by this turn.
    pub(super) const fn manifest_page_bytes(&self) -> u64 {
        self.manifest_page_bytes
    }

    /// Returns the exact encoded root row size staged by this turn.
    pub(super) const fn manifest_root_bytes(&self) -> u64 {
        self.manifest_root_bytes
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
    let Some(first_suffix) = first_row.key.strip_prefix(prefix.as_ref()) else {
        return Err(corruption(
            "text manifest prefix scan returned a row outside its requested prefix",
        ));
    };
    let first_suffix = Bytes::copy_from_slice(first_suffix);
    let (first_key, first_artifact) = super::attachment::decode_build_artifact(
        scope,
        operation,
        &first_row.key,
        &first_row.value,
    )?;
    let root_key = scoped_key(
        scope,
        index_keys::ScopedKey::TextManifestRoot(first_key.root),
    );
    let existing_root_value = transaction.get(&root_key).await?;
    let existing_root = match existing_root_value.as_ref() {
        Some(value) => {
            let root = index_values::decode_manifest_root(value)?;
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
        index_keys::ScopedKey::TextManifestPage(page_key_typed),
    );
    let existing_page = transaction.get(&page_key).await?;
    if existing_page.is_some() {
        return Err(corruption(
            "text manifest next contiguous page destination is occupied",
        ));
    }

    let root_template = match existing_root.as_ref() {
        Some(root) => match root.append_build_page(page, NonZeroU32::MIN) {
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
        .map_err(|error| corruption(format!("initial text manifest root is invalid: {error}")))?,
    };
    let encoded_root_template = index_values::encode_manifest_root(&root_template);
    let one_entry_page = index_values::encode_manifest_page(
        &work::TextManifestPageValue::try_new(
            operation.index_id(),
            operation.generation(),
            first_artifact.partition.clone(),
            page,
            vec![first_artifact.split],
        )
        .map_err(|error| corruption(format!("single-entry manifest page is invalid: {error}")))?,
    );
    let two_entry_page = index_values::encode_manifest_page(
        &work::TextManifestPageValue::try_new(
            operation.index_id(),
            operation.generation(),
            first_artifact.partition.clone(),
            page,
            vec![first_artifact.split, first_artifact.split],
        )
        .map_err(|error| corruption(format!("two-entry manifest page is invalid: {error}")))?,
    );
    let split_entry_bytes =
        u64::try_from(two_entry_page.len() - one_entry_page.len()).unwrap_or(u64::MAX);
    let page_base_bytes = u64::try_from(one_entry_page.len())
        .unwrap_or(u64::MAX)
        .saturating_sub(split_entry_bytes);
    let observations = vec![
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
    let mut deletes = Vec::new();
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
        let candidate_input_bytes =
            input_bytes.saturating_add(row_bytes(&row.key, Some(&row.value)));
        let candidate_page_bytes = page_base_bytes.saturating_add(
            split_entry_bytes.saturating_mul(u64::try_from(entries.len() + 1).unwrap_or(u64::MAX)),
        );
        let candidate_output_operations = output_operations.saturating_add(1);
        let candidate_output_bytes = output_bytes
            .saturating_add(split_entry_bytes)
            .saturating_add(u64::try_from(row.key.len()).unwrap_or(u64::MAX));
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
                let Some(blocked_suffix) = row.key.strip_prefix(prefix.as_ref()) else {
                    return Err(corruption(
                        "text manifest prefix scan returned a blocked row outside its prefix",
                    ));
                };
                let blocked_end = Bound::Included(Bytes::copy_from_slice(blocked_suffix));
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

        artifact_rows.push((row.key.clone(), row.value));
        entries.push(artifact.split);
        deletes.push(row.key.clone());
        input_bytes = candidate_input_bytes;
        output_operations = candidate_output_operations;
        output_bytes = candidate_output_bytes;
        completed_cursor = Some(
            IndexCursor::try_new(row.key)
                .map_err(|error| corruption(format!("invalid text artifact cursor: {error}")))?,
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

    let entry_count = u32::try_from(entries.len())
        .map_err(|_| corruption("bounded manifest entry count does not fit u32"))?;
    let Some(entry_count) = NonZeroU32::new(entry_count) else {
        return Err(corruption(
            "text manifest preparation produced an empty admitted page",
        ));
    };
    let root = match existing_root {
        Some(root) => root.append_build_page(page, entry_count).map_err(|error| {
            corruption(format!(
                "text manifest root rejected its admitted page: {error}"
            ))
        })?,
        None => work::TextManifestRootValue::try_new(
            operation.index_id(),
            operation.generation(),
            partition.clone(),
            TextManifestRevision::initial(),
            1,
            u64::from(entry_count.get()),
        )
        .map_err(|error| corruption(format!("initial text manifest root is invalid: {error}")))?,
    };
    let page_value = work::TextManifestPageValue::try_new(
        operation.index_id(),
        operation.generation(),
        partition.clone(),
        page,
        entries,
    )
    .map_err(|error| corruption(format!("admitted text manifest page is invalid: {error}")))?;
    let encoded_root = index_values::encode_manifest_root(&root);
    let encoded_page = index_values::encode_manifest_page(&page_value);
    let manifest_root_bytes =
        u64::try_from(root_key.len().saturating_add(encoded_root.len())).unwrap_or(u64::MAX);
    let manifest_page_bytes =
        u64::try_from(page_key.len().saturating_add(encoded_page.len())).unwrap_or(u64::MAX);
    if u64::try_from(encoded_page.len()).unwrap_or(u64::MAX)
        > manifest_limits.max_manifest_bytes().get()
    {
        return Err(corruption(
            "prepared text manifest page exceeded its admitted byte bound",
        ));
    }
    let puts = vec![(root_key, encoded_root), (page_key, encoded_page)];
    let Some(completed_cursor) = completed_cursor else {
        return Err(corruption(
            "text manifest page has entries but no completed artifact cursor",
        ));
    };
    let Some(completed_suffix) = completed_cursor.as_bytes().strip_prefix(prefix.as_ref()) else {
        return Err(corruption(
            "prepared text artifact cursor escaped its generation prefix",
        ));
    };
    let completed_suffix = Bytes::copy_from_slice(completed_suffix);
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
        input_bytes,
        output_operations,
        output_bytes,
        manifest_page_bytes,
        manifest_root_bytes,
    }))
}

/// Measures one key plus its optional observed/encoded value without overflow.
fn row_bytes(key: &[u8], value: Option<&Bytes>) -> u64 {
    u64::try_from(key.len().saturating_add(value.map_or(0, Bytes::len))).unwrap_or(u64::MAX)
}

/// Encodes one scoped V2 key through the canonical `encoding/v1` boundary.
fn scoped_key(scope: DataScope, key: index_keys::ScopedKey) -> Bytes {
    Key::Data { scope, kind: key }.to_bytes()
}

/// Converts a violated persisted manifest contract into the public DB error boundary.
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
    use crate::index_lifecycle::OperationCounters;

    #[tokio::test]
    async fn manifest_selection_revalidates_ranges_and_stages_only_the_encoded_page() {
        let db = Db::open("text-manifest-contracts", Arc::new(InMemory::new()))
            .await
            .expect("text manifest test database opens");
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let operation = test_support::operation();
        let scope = DataScope::LegacyUnscoped;
        let progress = PrefixScanProgress {
            cursor: None,
            counters: OperationCounters::default(),
        };
        let batch = test_support::batch_limits(u64::MAX, u64::MAX, u64::MAX);
        let manifest = test_support::compaction_limits(8, 1_024, 2_048, 1_024, 1_024);

        let ManifestSelection::Exhausted(empty_range) =
            select_page(&transaction, scope, &operation, &progress, batch, manifest)
                .await
                .unwrap()
        else {
            panic!("empty artifact prefix is exhausted")
        };
        assert!(empty_range.is_current(&transaction).await.unwrap());

        let first =
            test_support::artifact_row(scope, &operation, work::TextPartition::Unpartitioned, 0, 1);
        let second =
            test_support::artifact_row(scope, &operation, work::TextPartition::Unpartitioned, 1, 2);
        transaction.put(first.0.clone(), first.1.clone()).unwrap();
        transaction.put(second.0.clone(), second.1.clone()).unwrap();
        assert!(!empty_range.is_current(&transaction).await.unwrap());

        let blocked = select_page(
            &transaction,
            scope,
            &operation,
            &progress,
            test_support::batch_limits(1, u64::MAX, u64::MAX),
            manifest,
        )
        .await
        .unwrap();
        let ManifestSelection::Blocked {
            blocker,
            range,
            observations,
        } = blocked
        else {
            panic!("an indivisible first artifact reports its exact manifest limit")
        };
        assert!(matches!(
            blocker,
            IndexOperationBlocker::ManifestLimit { .. }
        ));
        assert!(range.is_current(&transaction).await.unwrap());
        assert_eq!(observations.len(), 2);

        let prepared = select_page(&transaction, scope, &operation, &progress, batch, manifest)
            .await
            .unwrap();
        let ManifestSelection::Page(prepared) = prepared else {
            panic!("two bounded artifacts produce one physical manifest page")
        };
        assert_eq!(prepared.completed_cursor().as_bytes(), &second.0);
        assert!(prepared.input_bytes() > 0);
        assert_eq!(prepared.output_operations(), 4);
        assert!(prepared.output_bytes() > 0);
        assert!(prepared.manifest_page_bytes() > 0);
        assert!(prepared.manifest_root_bytes() > 0);

        transaction
            .put(first.0.clone(), Bytes::from_static(b"stale-artifact"))
            .unwrap();
        assert!(!prepared.stage(&transaction).await.unwrap());
        transaction.put(first.0.clone(), first.1.clone()).unwrap();

        let ManifestSelection::Page(prepared) =
            select_page(&transaction, scope, &operation, &progress, batch, manifest)
                .await
                .unwrap()
        else {
            panic!("restored artifacts prepare the same manifest page")
        };
        assert!(prepared.stage(&transaction).await.unwrap());
        assert!(transaction.get(&first.0).await.unwrap().is_none());
        assert!(transaction.get(&second.0).await.unwrap().is_none());

        let third =
            test_support::artifact_row(scope, &operation, work::TextPartition::Unpartitioned, 2, 3);
        transaction.put(third.0.clone(), third.1).unwrap();
        let ManifestSelection::Page(next_page) =
            select_page(&transaction, scope, &operation, &progress, batch, manifest)
                .await
                .unwrap()
        else {
            panic!("an existing root admits its next contiguous page")
        };
        assert_eq!(next_page.completed_cursor().as_bytes(), &third.0);
        assert!(next_page.stage(&transaction).await.unwrap());
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/index_lifecycle_text_manifest.rs"]
mod external_contracts;
