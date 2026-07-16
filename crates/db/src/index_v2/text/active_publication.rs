//! Intent-first publication for request-owned Active text mutations.
//!
//! A prepared request has already derived and measured every graph, build,
//! retirement, and attachment row. This module owns the next external boundary:
//! it retains the process-local GC gate, reserves each exact content-addressed
//! blob, registers its complete request owner before committing the independent
//! upload outbox, and only then submits the paired payload to the publication
//! coordinator. The returned capability keeps every owner in flight until the
//! caller resolves the authoritative graph transaction.
//!
//! Durable `Uploaded` checkpointing is best-effort after definitive publication.
//! A failed checkpoint leaves the original `Prepared` intent discoverable; its
//! coordinator status remains authoritative and the terminal owner lets the
//! upload worker converge after request outcome resolution.

use std::sync::Arc;

use slatedb::{Db, IsolationLevel};

use crate::error::HelixDbError;

use super::super::{blob_publication, work, TextUploadIntentId, WriterEpoch};
use super::active_mutation::{
    ActiveTextMutationGuard, ActiveTextMutationRegistry, ActiveTextMutationRegistryError,
    TerminalActiveTextMutation,
};
use super::active_request::{PreparedActiveTextMutation, PreparedActiveTextUpload};
use super::upload::{
    self, PreparedTextUploadIntent, PreparedUploadObservation, PreparedUploadStageOutcome,
};

const ACTIVE_UPLOAD_INTENT_ALLOCATION_ATTEMPTS: usize = 16;

/// Failure while making one prepared Active request publication-ready.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ActiveTextPublicationError {
    /// SlateDB preparation or exact commit-resolution failed.
    #[error("Active text publication database failure: {0}")]
    Database(#[from] HelixDbError),
    /// Publication-coordinator reservation or object I/O failed.
    #[error("Active text publication coordinator failure: {0}")]
    Coordinator(#[from] blob_publication::BlobPublicationError),
    /// Process-local request ownership could not be established or advanced.
    #[error("Active text publication owner failure: {0}")]
    Owner(#[from] ActiveTextMutationRegistryError),
    /// A primary failure was followed by failure to cancel an unused owner or permit.
    #[error("Active text publication failed ({primary}); unused reservation cleanup also failed ({cleanup})")]
    FailureAndCleanup {
        primary: Box<ActiveTextPublicationError>,
        cleanup: Box<ActiveTextPublicationError>,
    },
    /// A failed commit could not be classified by its exact outbox triple.
    #[error("Active text upload commit outcome is unknown ({commit}); resolution also failed ({resolution})")]
    CommitOutcomeUnknown {
        commit: Box<HelixDbError>,
        resolution: Box<HelixDbError>,
    },
    /// Coordinator publication returned without a definitive matching success.
    #[error("Active text publication for intent {intent_id:?} was not definitively successful: {status:?}")]
    PublicationNotDefinitive {
        intent_id: TextUploadIntentId,
        status: blob_publication::BlobPublicationStatus,
    },
}

impl ActiveTextPublicationError {
    /// Preserves typed database/coordinator failures at the graph API boundary.
    pub(crate) fn into_database_error(self) -> HelixDbError {
        match self {
            Self::Database(error) => error,
            Self::Coordinator(error) => error.into(),
            Self::CommitOutcomeUnknown { commit, resolution } => {
                HelixDbError::InvariantViolation(format!(
                    "Active text upload commit outcome is unknown ({commit}); resolution failed ({resolution})"
                ))
            }
            Self::PublicationNotDefinitive { intent_id, status } => {
                blob_publication::BlobPublicationError::PublicationOutcomeAmbiguous(format!(
                    "intent {} retained status {status:?}",
                    intent_id.as_uuid()
                ))
                .into()
            }
            error @ (Self::Owner(_) | Self::FailureAndCleanup { .. }) => {
                HelixDbError::InvariantViolation(error.to_string())
            }
        }
    }
}

/// Runtime authority required only by upload-bearing Active mutations.
///
/// Keeping coordinator and writer epoch in one closed variant prevents a
/// request from publishing with a coordinator that cannot be reconciled by the
/// current writer's worker epoch. Upload-free retirements need neither and may
/// still stage while this service is unavailable.
#[derive(Clone)]
pub(crate) enum ActiveTextMutationRuntime {
    /// Shared topology has no trusted coordinator installed yet.
    Unavailable,
    /// One coordinator is shared by request publication and worker recovery.
    Ready {
        coordinator: Arc<dyn blob_publication::BlobPublicationCoordinator>,
        writer_epoch: WriterEpoch,
    },
}

impl core::fmt::Debug for ActiveTextMutationRuntime {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Unavailable => formatter.write_str("Unavailable"),
            Self::Ready { writer_epoch, .. } => formatter
                .debug_struct("Ready")
                .field("writer_epoch", writer_epoch)
                .finish_non_exhaustive(),
        }
    }
}

impl ActiveTextMutationRuntime {
    /// Returns the inseparable coordinator and current writer epoch.
    pub(crate) fn ready(
        &self,
    ) -> Option<(
        Arc<dyn blob_publication::BlobPublicationCoordinator>,
        WriterEpoch,
    )> {
        let Self::Ready {
            coordinator,
            writer_epoch,
        } = self
        else {
            return None;
        };
        Some((Arc::clone(coordinator), *writer_epoch))
    }
}

/// Runtime ownership retained from first reservation through graph outcome.
enum ActiveTextPublicationOwnership {
    /// A retirement-only request needs no blob-publication authority.
    NoUploads,
    /// Every uploaded destination remains guarded by the same runtime services.
    Uploads {
        guards: Vec<ActiveTextMutationGuard>,
        _reference_guards: Vec<blob_publication::BlobReferenceGuard>,
        _local_gate: crate::search::text::BlobPublicationPermit,
        coordinator: Arc<dyn blob_publication::BlobPublicationCoordinator>,
    },
}

/// One terminal request owner paired with its last in-memory upload value.
pub(super) struct TerminalActiveTextUpload {
    pub(super) value: work::TextUploadIntentValue,
    pub(super) owner: TerminalActiveTextMutation,
}

/// Publication services and terminal owners available to outcome finalization.
pub(super) struct TerminalActiveTextPublication {
    pub(super) uploads: Vec<TerminalActiveTextUpload>,
}

/// Definitively published values paired with their unresolved request owners.
///
/// The caller may borrow [`Self::uploaded`] to stage the graph transaction. It
/// must retain this capability until commit/abort/ambiguous-outcome resolution,
/// then call [`Self::finish`] to expose the durable intents to reconciliation.
/// Dropping it early is still safe: every in-flight guard becomes terminal.
pub(crate) struct ActiveTextPublication {
    uploaded: Vec<work::TextUploadIntentValue>,
    ownership: ActiveTextPublicationOwnership,
}

impl ActiveTextPublication {
    /// Retains an upload-free Active request through graph commit resolution.
    pub(crate) const fn without_uploads() -> Self {
        Self {
            uploaded: Vec::new(),
            ownership: ActiveTextPublicationOwnership::NoUploads,
        }
    }

    /// Borrows the definitively published values in prepared attachment order.
    pub(crate) fn uploaded(&self) -> &[work::TextUploadIntentValue] {
        &self.uploaded
    }

    /// Borrows the exact coordinator retained by an upload-bearing request.
    pub(super) fn coordinator(
        &self,
    ) -> Option<&Arc<dyn blob_publication::BlobPublicationCoordinator>> {
        match &self.ownership {
            ActiveTextPublicationOwnership::NoUploads => None,
            ActiveTextPublicationOwnership::Uploads { coordinator, .. } => Some(coordinator),
        }
    }

    /// Makes every request owner terminal after graph outcome resolution.
    pub(crate) fn finish(self) -> Result<(), ActiveTextMutationRegistryError> {
        self.into_terminal().map(|_terminal| ())
    }

    /// Transfers resolved request owners to the durable finalization boundary.
    pub(super) fn into_terminal(
        self,
    ) -> Result<TerminalActiveTextPublication, ActiveTextMutationRegistryError> {
        let ActiveTextPublication {
            uploaded,
            ownership,
        } = self;
        let ActiveTextPublicationOwnership::Uploads { guards, .. } = ownership else {
            debug_assert!(uploaded.is_empty());
            return Ok(TerminalActiveTextPublication {
                uploads: Vec::new(),
            });
        };
        debug_assert_eq!(uploaded.len(), guards.len());
        let uploads = uploaded
            .into_iter()
            .zip(guards)
            .map(|(value, guard)| {
                guard
                    .finish()
                    .map(|owner| TerminalActiveTextUpload { value, owner })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(TerminalActiveTextPublication { uploads })
    }
}

/// Publishes every request-owned Active split after durable intent preparation.
///
/// All upload triples are committed in independent serializable transactions;
/// no object I/O begins before its matching triple is known durable. A failure
/// after one or more durable intents leaves their dropped guards terminal, so
/// the global upload lane can reconcile them without guessing request liveness.
pub(crate) async fn publish_active_text_mutation(
    db: &Db,
    coordinator: Arc<dyn blob_publication::BlobPublicationCoordinator>,
    gc_gate: &crate::search::text::BlobGcGate,
    active_mutations: &ActiveTextMutationRegistry,
    writer_epoch: WriterEpoch,
    mutation_id: super::super::MutationId,
    prepared: &PreparedActiveTextMutation,
) -> Result<ActiveTextPublication, ActiveTextPublicationError> {
    let uploads = prepared
        .uploads(writer_epoch, mutation_id)
        .collect::<Vec<_>>();
    if uploads.is_empty() {
        return Ok(ActiveTextPublication::without_uploads());
    }

    let local_gate = gc_gate.acquire_publication().await;
    let mut uploaded = Vec::with_capacity(uploads.len());
    let mut guards = Vec::with_capacity(uploads.len());
    let mut reference_guards = Vec::with_capacity(uploads.len());
    for upload in uploads {
        let published = publish_one_active_upload(
            db,
            coordinator.as_ref(),
            active_mutations,
            prepared.scope(),
            writer_epoch,
            upload,
        )
        .await?;
        uploaded.push(published.value);
        guards.push(published.guard);
        reference_guards.push(published.reference_guard);
    }

    Ok(ActiveTextPublication {
        uploaded,
        ownership: ActiveTextPublicationOwnership::Uploads {
            guards,
            _reference_guards: reference_guards,
            _local_gate: local_gate,
            coordinator,
        },
    })
}

/// One publication whose durable intent and local owner are both retained.
struct PublishedActiveUpload {
    value: work::TextUploadIntentValue,
    guard: ActiveTextMutationGuard,
    reference_guard: blob_publication::BlobReferenceGuard,
}

/// Reserves, registers, commits, and publishes one exact payload/spec pair.
async fn publish_one_active_upload(
    db: &Db,
    coordinator: &dyn blob_publication::BlobPublicationCoordinator,
    active_mutations: &ActiveTextMutationRegistry,
    scope: crate::encoding::v1::keys::tenant::DataScope,
    writer_epoch: WriterEpoch,
    upload: PreparedActiveTextUpload,
) -> Result<PublishedActiveUpload, ActiveTextPublicationError> {
    let (payload, spec) = upload.into_parts();
    for intent_id in std::iter::repeat_with(TextUploadIntentId::new_v4)
        .take(ACTIVE_UPLOAD_INTENT_ALLOCATION_ATTEMPTS)
    {
        let permit = coordinator
            .reserve(spec.blob(), intent_id, writer_epoch)
            .await?;
        let prepared = PreparedTextUploadIntent::from_spec(intent_id, permit, spec.clone());
        let guard = match active_mutations.register(scope, prepared.value()) {
            Ok(guard) => guard,
            Err(ActiveTextMutationRegistryError::AlreadyRegistered) => {
                coordinator
                    .release(
                        &permit,
                        blob_publication::BlobPermitReleaseAuthority::definitive_non_publication(
                            permit.id(),
                        ),
                    )
                    .await?;
                continue;
            }
            Err(error) => {
                let primary = ActiveTextPublicationError::Owner(error);
                let cleanup = coordinator
                    .release(
                        &permit,
                        blob_publication::BlobPermitReleaseAuthority::definitive_non_publication(
                            permit.id(),
                        ),
                    )
                    .await;
                return Err(match cleanup {
                    Ok(()) => primary,
                    Err(cleanup) => ActiveTextPublicationError::FailureAndCleanup {
                        primary: Box::new(primary),
                        cleanup: Box::new(cleanup.into()),
                    },
                });
            }
        };

        let transaction = match db.begin(IsolationLevel::SerializableSnapshot).await {
            Ok(transaction) => transaction,
            Err(error) => {
                let primary = ActiveTextPublicationError::Database(error.into());
                return Err(combine_with_unused_cleanup(primary, guard, coordinator, permit).await);
            }
        };
        let stage = match upload::stage_prepared_upload(&transaction, scope, &prepared).await {
            Ok(stage) => stage,
            Err(error) => {
                return Err(
                    combine_with_unused_cleanup(error.into(), guard, coordinator, permit).await,
                );
            }
        };
        if stage == PreparedUploadStageOutcome::IdentifierCollision {
            cancel_unused(guard, coordinator, permit).await?;
            continue;
        }

        let commit = transaction.commit().await.map_err(HelixDbError::from);
        if let Err(commit) = commit {
            match upload::observe_prepared_upload(db, scope, &prepared).await {
                Ok(PreparedUploadObservation::Exact) => {}
                Ok(PreparedUploadObservation::Absent)
                | Ok(PreparedUploadObservation::IdentifierCollision) => {
                    return Err(combine_with_unused_cleanup(
                        ActiveTextPublicationError::Database(commit),
                        guard,
                        coordinator,
                        permit,
                    )
                    .await);
                }
                Err(resolution) => {
                    return Err(ActiveTextPublicationError::CommitOutcomeUnknown {
                        commit: Box::new(commit),
                        resolution: Box::new(resolution),
                    });
                }
            }
        }

        let status = coordinator.publish(&permit, payload.clone()).await?;
        let blob = spec.blob();
        let blob_publication::BlobPublicationStatus::Succeeded(metadata) = status else {
            return Err(ActiveTextPublicationError::PublicationNotDefinitive { intent_id, status });
        };
        if metadata.blob() != blob {
            return Err(ActiveTextPublicationError::PublicationNotDefinitive { intent_id, status });
        }
        let reference_guard = coordinator.validate_reference(blob).await?;
        if reference_guard.blob() != blob {
            return Err(HelixDbError::IndexCatalogCorruption(
                "Active text reference guard names another published blob".to_string(),
            )
            .into());
        }

        let value = prepared
            .value()
            .active_request_publication_succeeded()
            .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?;
        match upload::checkpoint_active_request_publication(db, scope, &prepared).await {
            Ok(durable) if durable == value => {}
            Ok(_) => {
                return Err(HelixDbError::IndexCatalogCorruption(
                    "Active text publication checkpoint returned another upload value".to_string(),
                )
                .into());
            }
            Err(error) => {
                tracing::warn!(
                    intent_id = %intent_id.as_uuid(),
                    %error,
                    "definitive Active text publication remains Prepared for worker reconciliation"
                );
            }
        }
        return Ok(PublishedActiveUpload {
            value,
            guard,
            reference_guard,
        });
    }

    Err(HelixDbError::IdentifierAllocationFailed {
        kind: "Active text upload intent ID",
        attempts: ACTIVE_UPLOAD_INTENT_ALLOCATION_ATTEMPTS,
    }
    .into())
}

/// Cancels an owner and releases its permit when no durable intent exists.
async fn cancel_unused(
    guard: ActiveTextMutationGuard,
    coordinator: &dyn blob_publication::BlobPublicationCoordinator,
    permit: blob_publication::BlobPublicationPermit,
) -> Result<(), ActiveTextPublicationError> {
    let owner = guard.cancel_before_durable_intent();
    let release = coordinator
        .release(
            &permit,
            blob_publication::BlobPermitReleaseAuthority::definitive_non_publication(permit.id()),
        )
        .await;
    match (owner, release) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(owner), Ok(())) => Err(owner.into()),
        (Ok(()), Err(release)) => Err(release.into()),
        (Err(owner), Err(release)) => Err(ActiveTextPublicationError::FailureAndCleanup {
            primary: Box::new(owner.into()),
            cleanup: Box::new(release.into()),
        }),
    }
}

/// Preserves a primary failure if cancellation also fails.
async fn combine_with_unused_cleanup(
    primary: ActiveTextPublicationError,
    guard: ActiveTextMutationGuard,
    coordinator: &dyn blob_publication::BlobPublicationCoordinator,
    permit: blob_publication::BlobPublicationPermit,
) -> ActiveTextPublicationError {
    match cancel_unused(guard, coordinator, permit).await {
        Ok(()) => primary,
        Err(cleanup) => ActiveTextPublicationError::FailureAndCleanup {
            primary: Box::new(primary),
            cleanup: Box::new(cleanup),
        },
    }
}
