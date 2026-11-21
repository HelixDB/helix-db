//! Bounded metadata-only cleanup for text-index generations.
//!
//! A DROP or an aborted BUILD deletes every generation-qualified SlateDB row,
//! unless the store admits dropped-generation entries: then it deletes
//! nothing and leaves an entry, and compactions discard the rows (see
//! [`dropped`]). Immutable content-addressed split objects are deliberately
//! retained.

use std::ops::Bound;

use bytes::Bytes;

use crate::config::SearchIndexBatchLimits;
use crate::encoding::v2::keys as index_keys;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::ManagedIndexKey;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::outbox::IndexOperationStepResult;
use crate::index_lifecycle::{
    dropped, BuildOperationOutcome, DroppedGenerationRows, DroppedGenerationValue, IndexCursor,
    IndexOperationOutcome, IndexOperationProgress, IndexOperationRecord, NoCursorProgress,
    OperationCounters, PrefixScanProgress, TextBuildProgress, TextCleanupProgress,
};

/// Runs one bounded metadata cleanup transition.
pub(super) async fn step_cleanup(
    transaction: &impl crate::transaction::Mutation,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &TextCleanupProgress,
    aborting: bool,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    match progress {
        // Every row this cleanup deletes is keyed by generation, so a drop
        // or abort that will leave a dropped-generation entry at `Finalize`
        // deletes nothing. One that already deleted rows one by one, on an
        // older store, finishes one by one: it has a cursor or counted a row.
        TextCleanupProgress::DeleteMetadata(progress)
            if progress.cursor.is_none()
                && progress.counters == OperationCounters::default()
                && dropped::admitted(transaction).await? =>
        {
            Ok(progressed_cleanup(
                aborting,
                TextCleanupProgress::Finalize(NoCursorProgress {
                    counters: progress.counters,
                }),
            ))
        }
        TextCleanupProgress::DeleteMetadata(progress) => {
            delete_metadata(transaction, scope, operation, progress, aborting, limits).await
        }
        TextCleanupProgress::Finalize(_) => {
            // Staged here, the entry commits in the transaction that moves
            // the record to `Dropped`. The outbox holds the generation's
            // publication permit across that commit, so no active text
            // compaction writes a row of the generation after the entry.
            if dropped::admitted(transaction).await? {
                dropped::stage_entry(
                    transaction,
                    dropped::DroppedGeneration {
                        index_id: operation.index_id(),
                        generation: operation.generation(),
                    },
                    DroppedGenerationValue {
                        scope,
                        rows: DroppedGenerationRows::Text,
                    },
                )?;
            }
            Ok(IndexOperationStepResult::Completed(if aborting {
                IndexOperationOutcome::Build(BuildOperationOutcome::Aborted)
            } else {
                IndexOperationOutcome::DropSucceeded
            }))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CleanupLane {
    BuildArtifact,
    ManifestPage,
    ManifestRoot,
    EntityState,
    CorpusStatistics,
    TermStatistics,
    StatisticsEntity,
    BuildDelta,
    AppliedState,
}

impl CleanupLane {
    const fn first() -> Self {
        Self::BuildArtifact
    }

    const fn next(self) -> Option<Self> {
        match self {
            Self::BuildArtifact => Some(Self::ManifestPage),
            Self::ManifestPage => Some(Self::ManifestRoot),
            Self::ManifestRoot => Some(Self::EntityState),
            Self::EntityState => Some(Self::CorpusStatistics),
            Self::CorpusStatistics => Some(Self::TermStatistics),
            Self::TermStatistics => Some(Self::StatisticsEntity),
            Self::StatisticsEntity => Some(Self::BuildDelta),
            Self::BuildDelta => Some(Self::AppliedState),
            Self::AppliedState => None,
        }
    }

    const fn record_kind(self) -> index_keys::RecordKind {
        match self {
            Self::BuildArtifact => index_keys::RecordKind::TextBuildArtifact,
            Self::ManifestPage => index_keys::RecordKind::TextManifestPage,
            Self::ManifestRoot => index_keys::RecordKind::TextManifestRoot,
            Self::EntityState => index_keys::RecordKind::TextEntityState,
            Self::CorpusStatistics => index_keys::RecordKind::TextCorpusStatistics,
            Self::TermStatistics => index_keys::RecordKind::TextTermStatistics,
            Self::StatisticsEntity => index_keys::RecordKind::TextStatisticsEntity,
            Self::BuildDelta => index_keys::RecordKind::BuildDelta,
            Self::AppliedState => index_keys::RecordKind::AppliedState,
        }
    }
}

async fn delete_metadata(
    transaction: &impl crate::transaction::Mutation,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &PrefixScanProgress,
    aborting: bool,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    let mut lane = match progress.cursor.as_ref() {
        Some(cursor) => cleanup_lane_from_cursor(scope, operation, cursor)?,
        None => CleanupLane::first(),
    };
    let mut resume = progress.cursor.as_ref().map(IndexCursor::as_bytes);

    loop {
        let prefix = ManagedIndexKey::data_prefix(
            scope,
            index_keys::ScopedKey::generation_prefix(
                lane.record_kind(),
                operation.index_id(),
                operation.generation(),
            ),
        );
        let start = match resume {
            Some(cursor) => {
                let Some(suffix) = cursor.strip_prefix(prefix.as_ref()) else {
                    return Err(corruption(
                        "text cleanup cursor is outside its exact generation prefix",
                    ));
                };
                Bound::Excluded(Bytes::copy_from_slice(suffix))
            }
            None => Bound::Unbounded,
        };
        let mut rows = transaction
            .scan_prefix(&prefix, (start, Bound::Unbounded))
            .await?;
        let mut counters = progress.counters;
        let mut completed_cursor = None;
        let mut admitted = 0_usize;
        let mut batch_input_bytes = 0_u64;
        let mut batch_output_operations = 0_u64;
        let mut batch_output_bytes = 0_u64;

        while admitted < limits.max_entities().get() {
            let Some(row) = rows.next().await? else {
                break;
            };
            validate_owned_key(scope, operation, lane, &row.key)?;
            let input_bytes =
                u64::try_from(row.key.len().saturating_add(row.value.len())).unwrap_or(u64::MAX);
            let output_bytes = u64::try_from(row.key.len()).unwrap_or(u64::MAX);
            let next_batch_input_bytes = batch_input_bytes.checked_add(input_bytes);
            let next_batch_output_operations = batch_output_operations.checked_add(1);
            let next_batch_output_bytes = batch_output_bytes.checked_add(output_bytes);
            let fits = next_batch_input_bytes
                .is_some_and(|value| value <= limits.max_input_bytes().get())
                && next_batch_output_operations
                    .is_some_and(|value| value <= limits.max_output_operations().get())
                && next_batch_output_bytes
                    .is_some_and(|value| value <= limits.max_output_bytes().get());
            if !fits {
                if admitted == 0 {
                    return Ok(IndexOperationStepResult::Blocked(
                        crate::index_lifecycle::IndexOperationBlocker::InvariantViolation,
                    ));
                }
                break;
            }

            counters = OperationCounters {
                entities: counters
                    .entities
                    .checked_add(1)
                    .ok_or_else(|| corruption("text cleanup entity counter overflowed"))?,
                input_bytes: counters
                    .input_bytes
                    .checked_add(input_bytes)
                    .ok_or_else(|| corruption("text cleanup input counter overflowed"))?,
                output_operations: counters
                    .output_operations
                    .checked_add(1)
                    .ok_or_else(|| corruption("text cleanup operation counter overflowed"))?,
                output_bytes: counters
                    .output_bytes
                    .checked_add(output_bytes)
                    .ok_or_else(|| corruption("text cleanup output counter overflowed"))?,
            };
            batch_input_bytes = next_batch_input_bytes
                .ok_or_else(|| corruption("text cleanup batch input overflowed"))?;
            batch_output_operations = next_batch_output_operations
                .ok_or_else(|| corruption("text cleanup batch operation overflowed"))?;
            batch_output_bytes = next_batch_output_bytes
                .ok_or_else(|| corruption("text cleanup batch output overflowed"))?;
            completed_cursor =
                Some(IndexCursor::try_new(row.key.clone()).map_err(|error| {
                    corruption(format!("invalid text cleanup cursor: {error}"))
                })?);
            transaction.delete(row.key)?;
            admitted = admitted.saturating_add(1);
        }

        if let Some(cursor) = completed_cursor {
            return Ok(progressed_cleanup(
                aborting,
                TextCleanupProgress::DeleteMetadata(PrefixScanProgress {
                    cursor: Some(cursor),
                    counters,
                }),
            ));
        }

        let Some(next) = lane.next() else {
            return Ok(progressed_cleanup(
                aborting,
                TextCleanupProgress::Finalize(NoCursorProgress {
                    counters: progress.counters,
                }),
            ));
        };
        lane = next;
        resume = None;
    }
}

fn cleanup_lane_from_cursor(
    scope: DataScope,
    operation: &IndexOperationRecord,
    cursor: &IndexCursor,
) -> Result<CleanupLane> {
    let ManagedIndexKey::Data {
        scope: cursor_scope,
        kind: key,
    } = ManagedIndexKey::parse_from_slice(scope, cursor.as_bytes())?
    else {
        return Err(corruption("text cleanup cursor is not a scoped V2 key"));
    };
    if cursor_scope != scope {
        return Err(corruption("text cleanup cursor names another scope"));
    }
    let (lane, index_id, generation) = key_owner(key)?;
    if index_id != operation.index_id() || generation != operation.generation() {
        return Err(corruption(
            "text cleanup cursor names another index generation",
        ));
    }
    Ok(lane)
}

fn validate_owned_key(
    scope: DataScope,
    operation: &IndexOperationRecord,
    expected_lane: CleanupLane,
    bytes: &[u8],
) -> Result<()> {
    let ManagedIndexKey::Data {
        scope: key_scope,
        kind: key,
    } = ManagedIndexKey::parse_from_slice(scope, bytes)?
    else {
        return Err(corruption("text cleanup scan yielded a non-data key"));
    };
    let (lane, index_id, generation) = key_owner(key)?;
    if key_scope != scope
        || lane != expected_lane
        || index_id != operation.index_id()
        || generation != operation.generation()
    {
        return Err(corruption(
            "text cleanup scan yielded metadata owned by another generation",
        ));
    }
    Ok(())
}

fn key_owner(
    key: index_keys::ScopedKey,
) -> Result<(
    CleanupLane,
    crate::index_lifecycle::IndexId,
    crate::index_lifecycle::IndexGenerationId,
)> {
    Ok(match key {
        index_keys::ScopedKey::TextBuildArtifact(key) => (
            CleanupLane::BuildArtifact,
            key.root.index_id,
            key.root.generation,
        ),
        index_keys::ScopedKey::TextManifestPage(key) => (
            CleanupLane::ManifestPage,
            key.root.index_id,
            key.root.generation,
        ),
        index_keys::ScopedKey::TextManifestRoot(key) => {
            (CleanupLane::ManifestRoot, key.index_id, key.generation)
        }
        index_keys::ScopedKey::TextEntityState(key) => (
            CleanupLane::EntityState,
            key.root.index_id,
            key.root.generation,
        ),
        index_keys::ScopedKey::TextCorpusStatistics(key) => {
            (CleanupLane::CorpusStatistics, key.index_id, key.generation)
        }
        index_keys::ScopedKey::TextTermStatistics(key) => (
            CleanupLane::TermStatistics,
            key.corpus.index_id,
            key.corpus.generation,
        ),
        index_keys::ScopedKey::TextStatisticsEntity(key) => {
            (CleanupLane::StatisticsEntity, key.index_id, key.generation)
        }
        index_keys::ScopedKey::BuildDelta(key) => {
            (CleanupLane::BuildDelta, key.index_id, key.generation)
        }
        index_keys::ScopedKey::AppliedState(key) => {
            (CleanupLane::AppliedState, key.index_id, key.generation)
        }
        index_keys::ScopedKey::IndexRecord(_)
        | index_keys::ScopedKey::Operation(_)
        | index_keys::ScopedKey::SecondaryEntry(_)
        | index_keys::ScopedKey::SecondaryEqualityBitmap(_)
        | index_keys::ScopedKey::VectorPartitionMapping(_)
        | index_keys::ScopedKey::IndexOperationQueue(_)
        | index_keys::ScopedKey::IndexOperationRow(_) => {
            return Err(corruption(
                "text cleanup cursor is outside its metadata lane set",
            ));
        }
    })
}

fn progressed_cleanup(aborting: bool, progress: TextCleanupProgress) -> IndexOperationStepResult {
    IndexOperationStepResult::Progressed(if aborting {
        IndexOperationProgress::TextBuild(TextBuildProgress::Aborting(progress))
    } else {
        IndexOperationProgress::TextCleanup(progress)
    })
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
    use crate::index_lifecycle::{IndexElementKind, IndexEntityId};

    fn initial_progress() -> TextCleanupProgress {
        TextCleanupProgress::DeleteMetadata(PrefixScanProgress {
            cursor: None,
            counters: OperationCounters::default(),
        })
    }

    #[tokio::test]
    async fn cleanup_walks_typed_lanes_and_preserves_build_drop_terminal_shapes() {
        // A store that admits no dropped-generation entries: every cleanup
        // deletes its rows itself.
        let db = Db::open("text-cleanup-contracts", Arc::new(InMemory::new()))
            .await
            .expect("text cleanup test database opens");
        crate::migrations::startup::bootstrap_writer(&db)
            .await
            .expect("text cleanup test database bootstraps V2 metadata");
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let operation = test_support::operation();
        let scope = DataScope::LegacyUnscoped;
        let limits = test_support::batch_limits(u64::MAX, u64::MAX, u64::MAX);

        let empty = step_cleanup(
            &transaction,
            scope,
            &operation,
            &initial_progress(),
            false,
            limits,
        )
        .await
        .unwrap();
        let IndexOperationStepResult::Progressed(IndexOperationProgress::TextCleanup(
            TextCleanupProgress::Finalize(final_progress),
        )) = empty
        else {
            panic!("empty metadata lanes advance to drop finalization")
        };
        assert_eq!(final_progress.counters, OperationCounters::default());
        assert!(matches!(
            step_cleanup(
                &transaction,
                scope,
                &operation,
                &TextCleanupProgress::Finalize(final_progress),
                false,
                limits,
            )
            .await
            .unwrap(),
            IndexOperationStepResult::Completed(IndexOperationOutcome::DropSucceeded)
        ));
        assert!(matches!(
            step_cleanup(
                &transaction,
                scope,
                &operation,
                &TextCleanupProgress::Finalize(NoCursorProgress {
                    counters: OperationCounters::default(),
                }),
                true,
                limits,
            )
            .await
            .unwrap(),
            IndexOperationStepResult::Completed(IndexOperationOutcome::Build(
                BuildOperationOutcome::Aborted
            ))
        ));

        let artifact = test_support::artifact_row(
            scope,
            &operation,
            crate::index_lifecycle::work::TextPartition::Unpartitioned,
            0,
            1,
        );
        transaction
            .put(artifact.0.clone(), artifact.1.clone())
            .unwrap();
        assert!(matches!(
            step_cleanup(
                &transaction,
                scope,
                &operation,
                &initial_progress(),
                false,
                test_support::batch_limits(1, u64::MAX, u64::MAX),
            )
            .await
            .unwrap(),
            IndexOperationStepResult::Blocked(
                crate::index_lifecycle::IndexOperationBlocker::InvariantViolation
            )
        ));

        let deleted = step_cleanup(
            &transaction,
            scope,
            &operation,
            &initial_progress(),
            true,
            limits,
        )
        .await
        .unwrap();
        let IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
            TextBuildProgress::Aborting(TextCleanupProgress::DeleteMetadata(progress)),
        )) = deleted
        else {
            panic!("aborted build retains its exact cleanup cursor")
        };
        assert_eq!(progress.cursor.as_ref().unwrap().as_bytes(), &artifact.0);
        assert_eq!(progress.counters.entities, 1);
        assert!(transaction.get(&artifact.0).await.unwrap().is_none());
        assert!(matches!(
            step_cleanup(
                &transaction,
                scope,
                &operation,
                &TextCleanupProgress::DeleteMetadata(progress),
                true,
                limits,
            )
            .await
            .unwrap(),
            IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
                TextBuildProgress::Aborting(TextCleanupProgress::Finalize(_))
            ))
        ));
    }

    /// Reads the dropped-generation entries `transaction` has staged or sees.
    async fn entries(
        transaction: &slatedb::DbTransaction,
    ) -> Vec<(dropped::DroppedGeneration, DroppedGenerationValue)> {
        dropped::load_entries(transaction).await.unwrap()
    }

    /// Proves a drop or an aborted build on an admitted store deletes no row
    /// and stages its entry with the outcome, and that one which already
    /// deleted rows finishes row by row and stages the entry all the same.
    #[tokio::test]
    async fn an_admitted_cleanup_leaves_its_rows_to_compactions_and_stages_one_entry() {
        // The cleanup progress a step moved to, in the shape of its operation.
        fn progressed(result: IndexOperationStepResult, aborting: bool) -> TextCleanupProgress {
            match (result, aborting) {
                (
                    IndexOperationStepResult::Progressed(IndexOperationProgress::TextCleanup(
                        progress,
                    )),
                    false,
                )
                | (
                    IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
                        TextBuildProgress::Aborting(progress),
                    )),
                    true,
                ) => progress,
                (result, aborting) => {
                    panic!("a cleanup step progresses in its own shape: {aborting} {result:?}")
                }
            }
        }
        let db = Db::open("text-cleanup-admitted", Arc::new(InMemory::new()))
            .await
            .expect("text cleanup test database opens");
        crate::migrations::startup::bootstrap_writer(&db)
            .await
            .expect("text cleanup test database bootstraps V2 metadata");
        dropped::admit_fixture(&db).await;
        let operation = test_support::operation();
        let scope = DataScope::LegacyUnscoped;
        let limits = test_support::batch_limits(u64::MAX, u64::MAX, u64::MAX);
        let partition = crate::index_lifecycle::work::TextPartition::Unpartitioned;
        let artifacts = [0, 1].map(|ordinal| {
            test_support::artifact_row(scope, &operation, partition.clone(), ordinal, 1)
        });
        let entry = vec![(
            dropped::DroppedGeneration {
                index_id: operation.index_id(),
                generation: operation.generation(),
            },
            DroppedGenerationValue {
                scope,
                rows: DroppedGenerationRows::Text,
            },
        )];
        let (first, second) = (&artifacts[0].0, &artifacts[1].0);

        for (aborting, outcome) in [
            (false, IndexOperationOutcome::DropSucceeded),
            (
                true,
                IndexOperationOutcome::Build(BuildOperationOutcome::Aborted),
            ),
        ] {
            // A cleanup that has deleted nothing goes straight to its last
            // step, which stages the entry and deletes no row.
            let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
            for (key, value) in &artifacts {
                transaction.put(key.clone(), value.clone()).unwrap();
            }
            let finalize = progressed(
                step_cleanup(
                    &transaction,
                    scope,
                    &operation,
                    &initial_progress(),
                    aborting,
                    limits,
                )
                .await
                .unwrap(),
                aborting,
            );
            assert!(
                matches!(finalize, TextCleanupProgress::Finalize(_)),
                "an admitted cleanup skips the row deletes: {aborting} {finalize:?}"
            );
            assert_eq!(entries(&transaction).await, vec![]);
            let IndexOperationStepResult::Completed(completed) =
                step_cleanup(&transaction, scope, &operation, &finalize, aborting, limits)
                    .await
                    .unwrap()
            else {
                panic!("the last step completes the operation: {aborting}")
            };
            assert_eq!(completed, outcome);
            assert_eq!(entries(&transaction).await, entry, "{aborting}");
            for (key, _) in &artifacts {
                assert!(transaction.get(key).await.unwrap().is_some());
            }
            drop(transaction);

            // A cleanup that counted a deleted row, or holds a cursor, deletes
            // the rest row by row, and its last step stages the entry too.
            for half_way in [
                PrefixScanProgress {
                    cursor: None,
                    counters: OperationCounters {
                        entities: 1,
                        ..OperationCounters::default()
                    },
                },
                PrefixScanProgress {
                    cursor: Some(IndexCursor::try_new(first.clone()).unwrap()),
                    counters: OperationCounters::default(),
                },
            ] {
                let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
                transaction
                    .put(second.clone(), artifacts[1].1.clone())
                    .unwrap();
                let TextCleanupProgress::DeleteMetadata(progress) = progressed(
                    step_cleanup(
                        &transaction,
                        scope,
                        &operation,
                        &TextCleanupProgress::DeleteMetadata(half_way.clone()),
                        aborting,
                        limits,
                    )
                    .await
                    .unwrap(),
                    aborting,
                ) else {
                    panic!("a cleanup half-way through its rows keeps deleting them: {half_way:?}")
                };
                assert_eq!(
                    progress.counters.entities,
                    half_way.counters.entities + 1,
                    "{aborting} {half_way:?}"
                );
                assert!(transaction.get(second).await.unwrap().is_none());
                assert_eq!(entries(&transaction).await, vec![]);
                let IndexOperationStepResult::Completed(completed) = step_cleanup(
                    &transaction,
                    scope,
                    &operation,
                    &TextCleanupProgress::Finalize(NoCursorProgress {
                        counters: progress.counters,
                    }),
                    aborting,
                    limits,
                )
                .await
                .unwrap() else {
                    panic!("the last step completes the operation: {aborting}")
                };
                assert_eq!(completed, outcome);
                assert_eq!(entries(&transaction).await, entry, "{aborting}");
            }
        }
        db.close().await.expect("text cleanup test database closes");
    }

    #[test]
    fn every_cleanup_lane_has_one_exact_owner_and_rejects_other_key_families() {
        let operation = test_support::operation();
        let index_id = operation.index_id();
        let generation = operation.generation();
        let partition = crate::index_lifecycle::work::TextPartition::Unpartitioned;
        let root = index_keys::TextManifestRootKey {
            index_id,
            generation,
            partition: partition.fingerprint(),
        };
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(7),
        };
        let corpus = index_keys::TextCorpusStatisticsKey {
            index_id,
            generation,
            partition: partition.fingerprint(),
        };
        let keys = [
            (
                CleanupLane::BuildArtifact,
                index_keys::ScopedKey::TextBuildArtifact(index_keys::TextBuildArtifactKey {
                    root,
                    ordinal: 0,
                }),
            ),
            (
                CleanupLane::ManifestPage,
                index_keys::ScopedKey::TextManifestPage(index_keys::TextManifestPageKey {
                    root,
                    page: 0,
                }),
            ),
            (
                CleanupLane::ManifestRoot,
                index_keys::ScopedKey::TextManifestRoot(root),
            ),
            (
                CleanupLane::EntityState,
                index_keys::ScopedKey::TextEntityState(index_keys::TextEntityStateKey {
                    root,
                    entity,
                }),
            ),
            (
                CleanupLane::CorpusStatistics,
                index_keys::ScopedKey::TextCorpusStatistics(corpus),
            ),
            (
                CleanupLane::TermStatistics,
                index_keys::ScopedKey::TextTermStatistics(index_keys::TextTermStatisticsKey {
                    corpus,
                    term: index_keys::TextTermFingerprint::new([1; 32]),
                }),
            ),
            (
                CleanupLane::StatisticsEntity,
                index_keys::ScopedKey::TextStatisticsEntity(index_keys::TextStatisticsEntityKey {
                    index_id,
                    generation,
                    entity,
                }),
            ),
            (
                CleanupLane::BuildDelta,
                index_keys::ScopedKey::BuildDelta(index_keys::IndexEntityStateKey {
                    index_id,
                    generation,
                    entity,
                }),
            ),
            (
                CleanupLane::AppliedState,
                index_keys::ScopedKey::AppliedState(index_keys::IndexEntityStateKey {
                    index_id,
                    generation,
                    entity,
                }),
            ),
        ];
        for (expected, key) in keys {
            assert_eq!(key_owner(key).unwrap().0, expected);
        }
        assert!(key_owner(index_keys::ScopedKey::index_record(
            operation.identity().clone()
        ))
        .is_err());

        let artifact =
            test_support::artifact_row(DataScope::LegacyUnscoped, &operation, partition, 0, 1);
        let cursor = IndexCursor::try_new(artifact.0.clone()).unwrap();
        assert_eq!(
            cleanup_lane_from_cursor(DataScope::LegacyUnscoped, &operation, &cursor).unwrap(),
            CleanupLane::BuildArtifact
        );
        validate_owned_key(
            DataScope::LegacyUnscoped,
            &operation,
            CleanupLane::BuildArtifact,
            &artifact.0,
        )
        .unwrap();
        assert!(validate_owned_key(
            DataScope::LegacyUnscoped,
            &operation,
            CleanupLane::ManifestPage,
            &artifact.0,
        )
        .is_err());
    }
}
