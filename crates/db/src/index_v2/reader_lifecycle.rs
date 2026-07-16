//! Durable lifecycle preparation for cross-process reader coordination.
//!
//! External coordinator calls happen before the outbox repository opens its
//! SlateDB transaction. The resulting closed preparation either lets ordinary
//! activation staging continue or supplies the exact progress result that the
//! repository must commit. This keeps coordinator side effects idempotent
//! across ambiguous database commits while giving all three index families one
//! activation-and-drain contract.

use std::sync::Arc;

use crate::encoding::v1::keys::tenant::DataScope;
use crate::index_v2::outbox::IndexOperationStepResult;
use crate::index_v2::reader_lease::{
    DrainFence, IndexLeaseCoordinator, IndexLeaseError, LeaseGenerationKey,
};
use crate::index_v2::{
    DrainProgress, IndexOperationBlocker, IndexOperationProgress, IndexOperationRecord,
    NoCursorProgress, PrefixScanProgress, SecondaryBuildProgress, SecondaryCleanupProgress,
    TextBuildProgress, TextCleanupProgress, VectorBuildProgress, VectorCleanupProgress,
};

/// Coordinator preparation retained through one outbox checkpoint commit.
pub(crate) enum PreparedReaderLifecycleStep {
    /// Registration succeeded, so family validation may stage activation.
    ContinueActivation,
    /// Coordinator state selected the complete repository result for this step.
    RepositoryResult(Box<IndexOperationStepResult>),
}

impl PreparedReaderLifecycleStep {
    /// Returns an override result or authorizes the ordinary family stage.
    pub(crate) fn repository_result(&self) -> Option<IndexOperationStepResult> {
        match self {
            Self::ContinueActivation => None,
            Self::RepositoryResult(result) => Some((**result).clone()),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ReaderLifecycleRequest {
    Activate,
    BeginDrain {
        lane: CleanupProgressLane,
        progress: DrainProgress,
    },
    FinishDrain {
        lane: CleanupProgressLane,
        progress: DrainProgress,
    },
}

/// Exact family and operation kind whose cleanup owns one drain.
#[derive(Debug, Clone, Copy)]
enum CleanupProgressLane {
    SecondaryAbort,
    SecondaryDrop,
    VectorAbort,
    VectorDrop,
    TextAbort,
    TextDrop,
}

/// Prepares reader coordination only for activation and cleanup boundaries.
pub(crate) async fn prepare_reader_lifecycle_step(
    coordinator: Option<&Arc<dyn IndexLeaseCoordinator>>,
    scope: DataScope,
    operation: &IndexOperationRecord,
) -> Option<PreparedReaderLifecycleStep> {
    let request = lifecycle_request(operation.progress())?;
    let Some(coordinator) = coordinator else {
        return Some(blocked(
            IndexOperationBlocker::ReaderCoordinationUnavailable,
        ));
    };
    let generation = LeaseGenerationKey::new(scope, operation.index_id(), operation.generation());
    Some(match request {
        ReaderLifecycleRequest::Activate => match coordinator.register_generation(generation).await
        {
            Ok(()) => PreparedReaderLifecycleStep::ContinueActivation,
            Err(error) => classify_activation_error(error),
        },
        ReaderLifecycleRequest::BeginDrain { lane, progress } => {
            prepare_begin_drain(coordinator.as_ref(), generation, lane, progress).await
        }
        ReaderLifecycleRequest::FinishDrain { lane, progress } => {
            prepare_finish_drain(coordinator.as_ref(), generation, lane, progress).await
        }
    })
}

fn lifecycle_request(progress: &IndexOperationProgress) -> Option<ReaderLifecycleRequest> {
    match progress {
        IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
            crate::index_v2::SecondaryBuildStage::Activate(_),
        ))
        | IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
            crate::index_v2::VectorBuildStage::Activate(_),
        ))
        | IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            crate::index_v2::TextBuildStage::Activate(_),
        )) => Some(ReaderLifecycleRequest::Activate),
        IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Aborting(
            SecondaryCleanupProgress::BeginDrain(progress),
        )) => Some(ReaderLifecycleRequest::BeginDrain {
            lane: CleanupProgressLane::SecondaryAbort,
            progress: *progress,
        }),
        IndexOperationProgress::SecondaryCleanup(SecondaryCleanupProgress::BeginDrain(
            progress,
        )) => Some(ReaderLifecycleRequest::BeginDrain {
            lane: CleanupProgressLane::SecondaryDrop,
            progress: *progress,
        }),
        IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(
            VectorCleanupProgress::BeginDrain(progress),
        )) => Some(ReaderLifecycleRequest::BeginDrain {
            lane: CleanupProgressLane::VectorAbort,
            progress: *progress,
        }),
        IndexOperationProgress::VectorCleanup(VectorCleanupProgress::BeginDrain(progress)) => {
            Some(ReaderLifecycleRequest::BeginDrain {
                lane: CleanupProgressLane::VectorDrop,
                progress: *progress,
            })
        }
        IndexOperationProgress::TextBuild(TextBuildProgress::Aborting(
            TextCleanupProgress::BeginDrain(progress),
        )) => Some(ReaderLifecycleRequest::BeginDrain {
            lane: CleanupProgressLane::TextAbort,
            progress: *progress,
        }),
        IndexOperationProgress::TextCleanup(TextCleanupProgress::BeginDrain(progress)) => {
            Some(ReaderLifecycleRequest::BeginDrain {
                lane: CleanupProgressLane::TextDrop,
                progress: *progress,
            })
        }
        IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Aborting(
            SecondaryCleanupProgress::FinishDrain(progress),
        )) => Some(ReaderLifecycleRequest::FinishDrain {
            lane: CleanupProgressLane::SecondaryAbort,
            progress: *progress,
        }),
        IndexOperationProgress::SecondaryCleanup(SecondaryCleanupProgress::FinishDrain(
            progress,
        )) => Some(ReaderLifecycleRequest::FinishDrain {
            lane: CleanupProgressLane::SecondaryDrop,
            progress: *progress,
        }),
        IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(
            VectorCleanupProgress::FinishDrain(progress),
        )) => Some(ReaderLifecycleRequest::FinishDrain {
            lane: CleanupProgressLane::VectorAbort,
            progress: *progress,
        }),
        IndexOperationProgress::VectorCleanup(VectorCleanupProgress::FinishDrain(progress)) => {
            Some(ReaderLifecycleRequest::FinishDrain {
                lane: CleanupProgressLane::VectorDrop,
                progress: *progress,
            })
        }
        IndexOperationProgress::TextBuild(TextBuildProgress::Aborting(
            TextCleanupProgress::FinishDrain(progress),
        )) => Some(ReaderLifecycleRequest::FinishDrain {
            lane: CleanupProgressLane::TextAbort,
            progress: *progress,
        }),
        IndexOperationProgress::TextCleanup(TextCleanupProgress::FinishDrain(progress)) => {
            Some(ReaderLifecycleRequest::FinishDrain {
                lane: CleanupProgressLane::TextDrop,
                progress: *progress,
            })
        }
        IndexOperationProgress::SecondaryBuild(_)
        | IndexOperationProgress::VectorBuild(_)
        | IndexOperationProgress::TextBuild(_)
        | IndexOperationProgress::SecondaryCleanup(_)
        | IndexOperationProgress::VectorCleanup(_)
        | IndexOperationProgress::TextCleanup(_) => None,
    }
}

async fn prepare_begin_drain(
    coordinator: &dyn IndexLeaseCoordinator,
    generation: LeaseGenerationKey,
    lane: CleanupProgressLane,
    progress: DrainProgress,
) -> PreparedReaderLifecycleStep {
    let fence = match progress.drain_epoch {
        Some(epoch) => {
            let Ok(persisted) = DrainFence::try_from_persisted(generation, epoch) else {
                return blocked(IndexOperationBlocker::InvariantViolation);
            };
            match coordinator.begin_drain(generation, Some(&persisted)).await {
                Ok(fence) if fence == persisted => fence,
                Ok(_) => return blocked(IndexOperationBlocker::InvariantViolation),
                Err(error) => return classify_coordination_error(error),
            }
        }
        None => {
            let fence = match coordinator.begin_drain(generation, None).await {
                Ok(fence) => fence,
                Err(IndexLeaseError::GenerationUnavailable) => {
                    match coordinator.register_generation(generation).await {
                        Ok(())
                        | Err(IndexLeaseError::GenerationDraining)
                        | Err(IndexLeaseError::GenerationClosed) => {}
                        Err(error) => return classify_coordination_error(error),
                    }
                    match coordinator.begin_drain(generation, None).await {
                        Ok(fence) => fence,
                        Err(error) => return classify_coordination_error(error),
                    }
                }
                Err(error) => return classify_coordination_error(error),
            };
            return repository_progress(begin_progress_with_drain_epoch(
                lane,
                progress.counters,
                fence.epoch().get(),
            ));
        }
    };
    match coordinator.check_drained(&fence).await {
        Ok(true) => repository_progress(progress_after_begin_drain(lane, progress.counters)),
        Ok(false) | Err(IndexLeaseError::ReadersRemain) => transient(),
        Err(error) => classify_coordination_error(error),
    }
}

async fn prepare_finish_drain(
    coordinator: &dyn IndexLeaseCoordinator,
    generation: LeaseGenerationKey,
    lane: CleanupProgressLane,
    progress: DrainProgress,
) -> PreparedReaderLifecycleStep {
    let fence = match progress.drain_epoch {
        Some(epoch) => {
            let Ok(persisted) = DrainFence::try_from_persisted(generation, epoch) else {
                return blocked(IndexOperationBlocker::InvariantViolation);
            };
            match coordinator.begin_drain(generation, Some(&persisted)).await {
                Ok(fence) if fence == persisted => fence,
                Ok(_) => return blocked(IndexOperationBlocker::InvariantViolation),
                Err(error) => return classify_coordination_error(error),
            }
        }
        None => match coordinator.begin_drain(generation, None).await {
            Ok(fence) => {
                return repository_progress(finish_progress_with_drain_epoch(
                    lane,
                    progress.counters,
                    fence.epoch().get(),
                ));
            }
            Err(error) => return classify_coordination_error(error),
        },
    };
    match coordinator.check_drained(&fence).await {
        Ok(false) | Err(IndexLeaseError::ReadersRemain) => transient(),
        Ok(true) => match coordinator.finish_drain(&fence).await {
            Ok(()) => repository_progress(progress_after_finish_drain(lane, progress.counters)),
            Err(IndexLeaseError::ReadersRemain) => transient(),
            Err(error) => classify_coordination_error(error),
        },
        Err(error) => classify_coordination_error(error),
    }
}

fn begin_progress_with_drain_epoch(
    lane: CleanupProgressLane,
    counters: crate::index_v2::OperationCounters,
    drain_epoch: u64,
) -> IndexOperationProgress {
    let progress = DrainProgress {
        drain_epoch: Some(drain_epoch),
        counters,
    };
    match lane {
        CleanupProgressLane::SecondaryAbort => IndexOperationProgress::SecondaryBuild(
            SecondaryBuildProgress::Aborting(SecondaryCleanupProgress::BeginDrain(progress)),
        ),
        CleanupProgressLane::SecondaryDrop => {
            IndexOperationProgress::SecondaryCleanup(SecondaryCleanupProgress::BeginDrain(progress))
        }
        CleanupProgressLane::VectorAbort => IndexOperationProgress::VectorBuild(
            VectorBuildProgress::Aborting(VectorCleanupProgress::BeginDrain(progress)),
        ),
        CleanupProgressLane::VectorDrop => {
            IndexOperationProgress::VectorCleanup(VectorCleanupProgress::BeginDrain(progress))
        }
        CleanupProgressLane::TextAbort => IndexOperationProgress::TextBuild(
            TextBuildProgress::Aborting(TextCleanupProgress::BeginDrain(progress)),
        ),
        CleanupProgressLane::TextDrop => {
            IndexOperationProgress::TextCleanup(TextCleanupProgress::BeginDrain(progress))
        }
    }
}

fn finish_progress_with_drain_epoch(
    lane: CleanupProgressLane,
    counters: crate::index_v2::OperationCounters,
    drain_epoch: u64,
) -> IndexOperationProgress {
    let progress = DrainProgress {
        drain_epoch: Some(drain_epoch),
        counters,
    };
    match lane {
        CleanupProgressLane::SecondaryAbort => IndexOperationProgress::SecondaryBuild(
            SecondaryBuildProgress::Aborting(SecondaryCleanupProgress::FinishDrain(progress)),
        ),
        CleanupProgressLane::SecondaryDrop => IndexOperationProgress::SecondaryCleanup(
            SecondaryCleanupProgress::FinishDrain(progress),
        ),
        CleanupProgressLane::VectorAbort => IndexOperationProgress::VectorBuild(
            VectorBuildProgress::Aborting(VectorCleanupProgress::FinishDrain(progress)),
        ),
        CleanupProgressLane::VectorDrop => {
            IndexOperationProgress::VectorCleanup(VectorCleanupProgress::FinishDrain(progress))
        }
        CleanupProgressLane::TextAbort => IndexOperationProgress::TextBuild(
            TextBuildProgress::Aborting(TextCleanupProgress::FinishDrain(progress)),
        ),
        CleanupProgressLane::TextDrop => {
            IndexOperationProgress::TextCleanup(TextCleanupProgress::FinishDrain(progress))
        }
    }
}

fn progress_after_begin_drain(
    lane: CleanupProgressLane,
    counters: crate::index_v2::OperationCounters,
) -> IndexOperationProgress {
    match lane {
        CleanupProgressLane::SecondaryAbort => {
            IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Aborting(
                SecondaryCleanupProgress::DeleteEntries(PrefixScanProgress {
                    cursor: None,
                    counters,
                }),
            ))
        }
        CleanupProgressLane::SecondaryDrop => IndexOperationProgress::SecondaryCleanup(
            SecondaryCleanupProgress::DeleteEntries(PrefixScanProgress {
                cursor: None,
                counters,
            }),
        ),
        CleanupProgressLane::VectorAbort => {
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(
                VectorCleanupProgress::RetireCache(NoCursorProgress { counters }),
            ))
        }
        CleanupProgressLane::VectorDrop => IndexOperationProgress::VectorCleanup(
            VectorCleanupProgress::RetireCache(NoCursorProgress { counters }),
        ),
        CleanupProgressLane::TextAbort => {
            IndexOperationProgress::TextBuild(TextBuildProgress::Aborting(
                TextCleanupProgress::PrepareCandidates(PrefixScanProgress {
                    cursor: None,
                    counters,
                }),
            ))
        }
        CleanupProgressLane::TextDrop => IndexOperationProgress::TextCleanup(
            TextCleanupProgress::PrepareCandidates(PrefixScanProgress {
                cursor: None,
                counters,
            }),
        ),
    }
}

fn progress_after_finish_drain(
    lane: CleanupProgressLane,
    counters: crate::index_v2::OperationCounters,
) -> IndexOperationProgress {
    match lane {
        CleanupProgressLane::SecondaryAbort => {
            IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Aborting(
                SecondaryCleanupProgress::Finalize(NoCursorProgress { counters }),
            ))
        }
        CleanupProgressLane::SecondaryDrop => IndexOperationProgress::SecondaryCleanup(
            SecondaryCleanupProgress::Finalize(NoCursorProgress { counters }),
        ),
        CleanupProgressLane::VectorAbort => {
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(
                VectorCleanupProgress::Finalize(NoCursorProgress { counters }),
            ))
        }
        CleanupProgressLane::VectorDrop => IndexOperationProgress::VectorCleanup(
            VectorCleanupProgress::Finalize(NoCursorProgress { counters }),
        ),
        CleanupProgressLane::TextAbort => {
            IndexOperationProgress::TextBuild(TextBuildProgress::Aborting(
                TextCleanupProgress::Finalize(NoCursorProgress { counters }),
            ))
        }
        CleanupProgressLane::TextDrop => {
            IndexOperationProgress::TextCleanup(TextCleanupProgress::Finalize(NoCursorProgress {
                counters,
            }))
        }
    }
}

fn classify_activation_error(error: IndexLeaseError) -> PreparedReaderLifecycleStep {
    match error {
        IndexLeaseError::Coordinator(_)
        | IndexLeaseError::GenerationUnavailable
        | IndexLeaseError::BackendClockOverflow => {
            blocked(IndexOperationBlocker::ReaderCoordinationUnavailable)
        }
        IndexLeaseError::GenerationDraining
        | IndexLeaseError::GenerationClosed
        | IndexLeaseError::InvalidTiming(_)
        | IndexLeaseError::NilUuid { .. }
        | IndexLeaseError::MinimumValidityOverflow
        | IndexLeaseError::LeaseNotCurrent
        | IndexLeaseError::LeaseCredentialMismatch
        | IndexLeaseError::LeaseValidityInsufficient
        | IndexLeaseError::StaleDrainFence
        | IndexLeaseError::ReadersRemain
        | IndexLeaseError::IdentifierAllocationExhausted
        | IndexLeaseError::GenerationEpochExhausted => {
            blocked(IndexOperationBlocker::InvariantViolation)
        }
    }
}

fn classify_coordination_error(error: IndexLeaseError) -> PreparedReaderLifecycleStep {
    match error {
        IndexLeaseError::Coordinator(_)
        | IndexLeaseError::GenerationUnavailable
        | IndexLeaseError::BackendClockOverflow => {
            blocked(IndexOperationBlocker::ReaderCoordinationUnavailable)
        }
        IndexLeaseError::ReadersRemain => transient(),
        IndexLeaseError::InvalidTiming(_)
        | IndexLeaseError::NilUuid { .. }
        | IndexLeaseError::MinimumValidityOverflow
        | IndexLeaseError::GenerationDraining
        | IndexLeaseError::GenerationClosed
        | IndexLeaseError::LeaseNotCurrent
        | IndexLeaseError::LeaseCredentialMismatch
        | IndexLeaseError::LeaseValidityInsufficient
        | IndexLeaseError::StaleDrainFence
        | IndexLeaseError::IdentifierAllocationExhausted
        | IndexLeaseError::GenerationEpochExhausted => {
            blocked(IndexOperationBlocker::InvariantViolation)
        }
    }
}

fn repository_progress(progress: IndexOperationProgress) -> PreparedReaderLifecycleStep {
    PreparedReaderLifecycleStep::RepositoryResult(Box::new(IndexOperationStepResult::Progressed(
        progress,
    )))
}

fn transient() -> PreparedReaderLifecycleStep {
    PreparedReaderLifecycleStep::RepositoryResult(Box::new(
        IndexOperationStepResult::TransientFailure,
    ))
}

fn blocked(blocker: IndexOperationBlocker) -> PreparedReaderLifecycleStep {
    PreparedReaderLifecycleStep::RepositoryResult(Box::new(IndexOperationStepResult::Blocked(
        blocker,
    )))
}
