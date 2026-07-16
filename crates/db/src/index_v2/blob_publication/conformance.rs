//! Reusable conformance suite for shared text-blob coordinator adapters.
//!
//! The suite exercises one content-addressed blob through reservation,
//! publication, reference protection, delete fencing, terminal cleanup, and
//! same-hash reopening. Embedding deployments must supply fresh intent, epoch,
//! and GC-run identities for each invocation.

use core::fmt::Debug;
use std::num::NonZeroU64;

use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::{
    BeginBlobDelete, BlobDeleteFence, BlobDeleteFenceKey, BlobDeleteOutcome, BlobOperationDuration,
    BlobPermitReleaseAuthority, BlobPublicationCoordinator, BlobPublicationError,
    BlobPublicationStatus, CleanupCommittedAuthority, FencedBlobObservation,
};
use crate::index_v2::{BlobGcRunId, BlobRef, TextUploadIntentId, WriterEpoch};

/// Fresh identities and payload consumed by one publication conformance run.
#[derive(Debug, Clone)]
pub struct BlobPublicationConformanceFixture {
    payload: Bytes,
    first_intent: TextUploadIntentId,
    first_epoch: WriterEpoch,
    blocked_intent: TextUploadIntentId,
    blocked_epoch: WriterEpoch,
    first_run: BlobGcRunId,
    other_run: BlobGcRunId,
}

impl BlobPublicationConformanceFixture {
    /// Validates that the suite can prove identity and run isolation.
    pub fn try_new(
        payload: Bytes,
        first_intent: TextUploadIntentId,
        first_epoch: WriterEpoch,
        blocked_intent: TextUploadIntentId,
        blocked_epoch: WriterEpoch,
        first_run: BlobGcRunId,
        other_run: BlobGcRunId,
    ) -> Result<Self, BlobPublicationConformanceFailure> {
        if first_intent == blocked_intent {
            return Err(BlobPublicationConformanceFailure::InvalidFixture(
                "publication intent IDs must be distinct",
            ));
        }
        if first_epoch == blocked_epoch {
            return Err(BlobPublicationConformanceFailure::InvalidFixture(
                "writer epochs must be distinct",
            ));
        }
        if first_run == other_run {
            return Err(BlobPublicationConformanceFailure::InvalidFixture(
                "blob GC run IDs must be distinct",
            ));
        }
        Ok(Self {
            payload,
            first_intent,
            first_epoch,
            blocked_intent,
            blocked_epoch,
            first_run,
            other_run,
        })
    }
}

/// Exact step failure produced by the reusable blob coordinator contract.
#[derive(Debug, thiserror::Error)]
pub enum BlobPublicationConformanceFailure {
    /// Fixture identities could not exercise an isolation boundary.
    #[error("invalid blob-publication conformance fixture: {0}")]
    InvalidFixture(&'static str),
    /// The adapter returned a failure where the contract required success.
    #[error("blob-publication conformance step `{step}` failed: {source}")]
    Adapter {
        /// Stable step name for CI diagnostics.
        step: &'static str,
        /// Adapter failure returned by the tested implementation.
        #[source]
        source: BlobPublicationError,
    },
    /// The adapter returned a successful but contractually wrong observation.
    #[error("blob-publication conformance step `{step}` expected {expected}, observed {observed}")]
    Contract {
        /// Stable step name for CI diagnostics.
        step: &'static str,
        /// Required contract outcome.
        expected: &'static str,
        /// Debug representation of the observed outcome.
        observed: String,
    },
}

/// Exercises the complete publication/reference/delete-fence lifecycle.
///
/// The first publication is deleted and its fence reopened on success. A
/// second reservation proves same-hash admission is restored and is then
/// released without object I/O, leaving no live fixture work behind.
pub async fn verify_blob_publication_coordinator(
    coordinator: &dyn BlobPublicationCoordinator,
    fixture: BlobPublicationConformanceFixture,
) -> Result<(), BlobPublicationConformanceFailure> {
    let blob = BlobRef::new(
        Sha256::digest(&fixture.payload).into(),
        u64::try_from(fixture.payload.len()).map_err(|_| {
            BlobPublicationConformanceFailure::InvalidFixture(
                "payload length must fit the persisted u64 domain",
            )
        })?,
    );
    let permit = succeed(
        "initial reservation",
        coordinator
            .reserve(blob, fixture.first_intent, fixture.first_epoch)
            .await,
    )?;
    let repeated = succeed(
        "idempotent reservation",
        coordinator
            .reserve(blob, fixture.first_intent, fixture.first_epoch)
            .await,
    )?;
    if repeated != permit {
        return Err(BlobPublicationConformanceFailure::Contract {
            step: "idempotent reservation",
            expected: "the original exact permit",
            observed: format!("first={permit:?}, repeated={repeated:?}"),
        });
    }
    let status = succeed(
        "reserved status",
        coordinator.publication_status(&permit).await,
    )?;
    if status != BlobPublicationStatus::Reserved {
        return Err(BlobPublicationConformanceFailure::Contract {
            step: "reserved status",
            expected: "Reserved",
            observed: format!("{status:?}"),
        });
    }
    let minimum = BlobOperationDuration::from_millis(NonZeroU64::MIN);
    succeed(
        "reservation validation",
        coordinator.validate_for(&permit, minimum).await,
    )?;
    let published = succeed(
        "publication",
        coordinator.publish(&permit, fixture.payload.clone()).await,
    )?;
    let BlobPublicationStatus::Succeeded(metadata) = published else {
        return Err(BlobPublicationConformanceFailure::Contract {
            step: "publication",
            expected: "Succeeded with exact metadata",
            observed: format!("{published:?}"),
        });
    };
    if metadata.blob() != blob {
        return Err(BlobPublicationConformanceFailure::Contract {
            step: "publication metadata",
            expected: "the declared content-addressed blob",
            observed: format!("{metadata:?}"),
        });
    }
    let repeated = succeed(
        "idempotent publication",
        coordinator.publish(&permit, fixture.payload.clone()).await,
    )?;
    if repeated != published {
        return Err(BlobPublicationConformanceFailure::Contract {
            step: "idempotent publication",
            expected: "the original definitive success",
            observed: format!("first={published:?}, repeated={repeated:?}"),
        });
    }
    let retained = succeed(
        "retained publication status",
        coordinator.publication_status(&permit).await,
    )?;
    if retained != published {
        return Err(BlobPublicationConformanceFailure::Contract {
            step: "retained publication status",
            expected: "the original definitive success",
            observed: format!("{retained:?}"),
        });
    }
    let reference = succeed(
        "reference validation",
        coordinator.validate_reference(blob).await,
    )?;
    if reference.blob() != blob {
        return Err(BlobPublicationConformanceFailure::Contract {
            step: "reference validation identity",
            expected: "the requested blob",
            observed: format!("{reference:?}"),
        });
    }
    let release = BlobPermitReleaseAuthority::reference_committed(permit.id());
    succeed(
        "terminal permit release",
        coordinator.release(&permit, release).await,
    )?;
    succeed(
        "idempotent terminal permit release",
        coordinator.release(&permit, release).await,
    )?;

    let key = BlobDeleteFenceKey::new(blob, fixture.first_run);
    let fence = match succeed("begin delete", coordinator.begin_delete(key).await)? {
        BeginBlobDelete::Acquired(fence) => fence,
        observed @ (BeginBlobDelete::AlreadyHeldSameRun(_) | BeginBlobDelete::BusyOtherRun) => {
            return Err(BlobPublicationConformanceFailure::Contract {
                step: "begin delete",
                expected: "Acquired for a fresh run",
                observed: format!("{observed:?}"),
            });
        }
    };
    match succeed(
        "idempotent begin delete",
        coordinator.begin_delete(key).await,
    )? {
        BeginBlobDelete::AlreadyHeldSameRun(repeated) if repeated == fence => {}
        observed @ (BeginBlobDelete::Acquired(_) | BeginBlobDelete::BusyOtherRun)
        | observed @ BeginBlobDelete::AlreadyHeldSameRun(_) => {
            return Err(BlobPublicationConformanceFailure::Contract {
                step: "idempotent begin delete",
                expected: "AlreadyHeldSameRun with the original fence",
                observed: format!("{observed:?}"),
            });
        }
    }
    let other_key = BlobDeleteFenceKey::new(blob, fixture.other_run);
    let other = succeed(
        "competing delete run",
        coordinator.begin_delete(other_key).await,
    )?;
    if other != BeginBlobDelete::BusyOtherRun {
        return Err(BlobPublicationConformanceFailure::Contract {
            step: "competing delete run",
            expected: "BusyOtherRun",
            observed: format!("{other:?}"),
        });
    }
    expect_error(
        "reservation behind delete fence",
        coordinator
            .reserve(blob, fixture.blocked_intent, fixture.blocked_epoch)
            .await,
        |error| matches!(error, BlobPublicationError::DeleteFenceClosed),
        "DeleteFenceClosed",
    )?;
    if succeed(
        "quiescence with reference",
        coordinator.check_quiescent(&fence).await,
    )? {
        return Err(BlobPublicationConformanceFailure::Contract {
            step: "quiescence with reference",
            expected: "false while a reference guard remains",
            observed: "true".to_string(),
        });
    }
    drop(reference);
    if !succeed(
        "quiescence after reference",
        coordinator.check_quiescent(&fence).await,
    )? {
        return Err(BlobPublicationConformanceFailure::Contract {
            step: "quiescence after reference",
            expected: "true after reference release",
            observed: "false".to_string(),
        });
    }
    let observation = succeed(
        "fenced object inspection",
        coordinator.inspect_fenced_blob(&fence).await,
    )?;
    if observation != FencedBlobObservation::Exact {
        return Err(BlobPublicationConformanceFailure::Contract {
            step: "fenced object inspection",
            expected: "Exact",
            observed: format!("{observation:?}"),
        });
    }
    let outcome = succeed("fenced delete", coordinator.delete(&fence).await)?;
    if outcome != BlobDeleteOutcome::DeletedOrAbsent {
        return Err(BlobPublicationConformanceFailure::Contract {
            step: "fenced delete",
            expected: "DeletedOrAbsent",
            observed: format!("{outcome:?}"),
        });
    }
    let repeated = succeed("idempotent fenced delete", coordinator.delete(&fence).await)?;
    if repeated != BlobDeleteOutcome::DeletedOrAbsent {
        return Err(BlobPublicationConformanceFailure::Contract {
            step: "idempotent fenced delete",
            expected: "DeletedOrAbsent",
            observed: format!("{repeated:?}"),
        });
    }
    let observation = succeed(
        "post-delete inspection",
        coordinator.inspect_fenced_blob(&fence).await,
    )?;
    if observation != FencedBlobObservation::Absent {
        return Err(BlobPublicationConformanceFailure::Contract {
            step: "post-delete inspection",
            expected: "Absent",
            observed: format!("{observation:?}"),
        });
    }
    let stale = BlobDeleteFence::new(other_key);
    expect_error(
        "stale delete fence",
        coordinator.check_quiescent(&stale).await,
        |error| matches!(error, BlobPublicationError::InvalidDeleteFence),
        "InvalidDeleteFence",
    )?;
    let cleanup = CleanupCommittedAuthority::new(key);
    succeed(
        "finish delete",
        coordinator.finish_delete(key, cleanup).await,
    )?;
    succeed(
        "idempotent finish delete",
        coordinator.finish_delete(key, cleanup).await,
    )?;
    expect_error(
        "finished delete fence",
        coordinator.check_quiescent(&fence).await,
        |error| matches!(error, BlobPublicationError::InvalidDeleteFence),
        "InvalidDeleteFence",
    )?;

    let reopened = succeed(
        "same-hash reservation after finish",
        coordinator
            .reserve(blob, fixture.blocked_intent, fixture.blocked_epoch)
            .await,
    )?;
    succeed(
        "reopened reservation release",
        coordinator
            .release(
                &reopened,
                BlobPermitReleaseAuthority::definitive_non_publication(reopened.id()),
            )
            .await,
    )
}

fn succeed<T>(
    step: &'static str,
    result: Result<T, BlobPublicationError>,
) -> Result<T, BlobPublicationConformanceFailure> {
    result.map_err(|source| BlobPublicationConformanceFailure::Adapter { step, source })
}

fn expect_error<T: Debug>(
    step: &'static str,
    result: Result<T, BlobPublicationError>,
    matches_expected: impl FnOnce(&BlobPublicationError) -> bool,
    expected: &'static str,
) -> Result<(), BlobPublicationConformanceFailure> {
    match result {
        Err(error) if matches_expected(&error) => Ok(()),
        observed => Err(BlobPublicationConformanceFailure::Contract {
            step,
            expected,
            observed: format!("{observed:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;

    use super::*;
    use crate::index_v2::blob_publication::{
        BlobPublicationTiming, ProcessLocalBlobPublicationCoordinator,
    };

    fn fixture() -> BlobPublicationConformanceFixture {
        BlobPublicationConformanceFixture::try_new(
            Bytes::from_static(b"shared-adapter-conformance"),
            TextUploadIntentId::from_bytes([1; 16]).unwrap(),
            WriterEpoch::from_bytes([2; 16]).unwrap(),
            TextUploadIntentId::from_bytes([3; 16]).unwrap(),
            WriterEpoch::from_bytes([4; 16]).unwrap(),
            BlobGcRunId::from_bytes([5; 16]).unwrap(),
            BlobGcRunId::from_bytes([6; 16]).unwrap(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn process_local_adapter_passes_the_reusable_contract() {
        let coordinator = ProcessLocalBlobPublicationCoordinator::new(
            Arc::new(InMemory::new()),
            "blob-conformance",
            BlobPublicationTiming::default(),
        );

        verify_blob_publication_coordinator(&coordinator, fixture())
            .await
            .unwrap();
    }

    #[test]
    fn fixture_rejects_aliasing_contract_inputs() {
        let intent = TextUploadIntentId::from_bytes([1; 16]).unwrap();
        let epoch = WriterEpoch::from_bytes([2; 16]).unwrap();
        let run = BlobGcRunId::from_bytes([3; 16]).unwrap();
        assert!(matches!(
            BlobPublicationConformanceFixture::try_new(
                Bytes::new(),
                intent,
                epoch,
                intent,
                epoch,
                run,
                run,
            ),
            Err(BlobPublicationConformanceFailure::InvalidFixture(_))
        ));
    }
}
