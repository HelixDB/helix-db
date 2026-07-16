//! Reusable conformance suite for shared reader-lease adapters.
//!
//! Embedding deployments run [`verify_index_lease_coordinator`] against the
//! same adapter instance that every runtime for a database will receive. The
//! fixture identities must be fresh for that backend because the suite closes
//! both generations permanently.

use core::fmt::Debug;
use std::time::Duration;

use super::{
    DrainFence, IndexLeaseCoordinator, IndexLeaseError, LeaseGenerationKey, LeaseHolderId,
    LeaseMinimumValidity,
};

/// Fresh identities consumed by one reader-lease conformance run.
#[derive(Debug, Clone, Copy)]
pub struct IndexLeaseConformanceFixture {
    first_generation: LeaseGenerationKey,
    second_generation: LeaseGenerationKey,
    first_holder: LeaseHolderId,
    second_holder: LeaseHolderId,
}

impl IndexLeaseConformanceFixture {
    /// Validates that the suite can prove generation and holder isolation.
    pub fn try_new(
        first_generation: LeaseGenerationKey,
        second_generation: LeaseGenerationKey,
        first_holder: LeaseHolderId,
        second_holder: LeaseHolderId,
    ) -> Result<Self, IndexLeaseConformanceFailure> {
        if first_generation == second_generation {
            return Err(IndexLeaseConformanceFailure::InvalidFixture(
                "conformance generations must be distinct",
            ));
        }
        if first_holder == second_holder {
            return Err(IndexLeaseConformanceFailure::InvalidFixture(
                "conformance lease holders must be distinct",
            ));
        }
        Ok(Self {
            first_generation,
            second_generation,
            first_holder,
            second_holder,
        })
    }
}

/// Exact step failure produced by the reusable lease contract.
#[derive(Debug, thiserror::Error)]
pub enum IndexLeaseConformanceFailure {
    /// Fixture identities could not exercise an isolation boundary.
    #[error("invalid reader-lease conformance fixture: {0}")]
    InvalidFixture(&'static str),
    /// The adapter returned a failure where the contract required success.
    #[error("reader-lease conformance step `{step}` failed: {source}")]
    Adapter {
        /// Stable step name for CI diagnostics.
        step: &'static str,
        /// Adapter failure returned by the tested implementation.
        #[source]
        source: IndexLeaseError,
    },
    /// The adapter returned a successful but contractually wrong observation.
    #[error("reader-lease conformance step `{step}` expected {expected}, observed {observed}")]
    Contract {
        /// Stable step name for CI diagnostics.
        step: &'static str,
        /// Required contract outcome.
        expected: &'static str,
        /// Debug representation of the observed outcome.
        observed: String,
    },
}

/// Exercises registration, exact acquisition, drain, fencing, and closure.
///
/// Both fixture generations are terminally closed on success. The function
/// intentionally uses no client wall-clock expiry calculation; validity is
/// decided only by the adapter's [`IndexLeaseCoordinator::validate_for`].
pub async fn verify_index_lease_coordinator(
    coordinator: &dyn IndexLeaseCoordinator,
    fixture: IndexLeaseConformanceFixture,
) -> Result<(), IndexLeaseConformanceFailure> {
    expect_error(
        "unregistered acquisition",
        coordinator
            .acquire(fixture.first_generation, fixture.first_holder)
            .await,
        IndexLeaseError::GenerationUnavailable,
    )?;
    succeed(
        "initial registration",
        coordinator
            .register_generation(fixture.first_generation)
            .await,
    )?;
    succeed(
        "idempotent registration",
        coordinator
            .register_generation(fixture.first_generation)
            .await,
    )?;
    let lease = succeed(
        "registered acquisition",
        coordinator
            .acquire(fixture.first_generation, fixture.first_holder)
            .await,
    )?;
    if lease.generation() != fixture.first_generation || lease.holder_id() != fixture.first_holder {
        return Err(IndexLeaseConformanceFailure::Contract {
            step: "registered acquisition identity",
            expected: "the requested generation and holder",
            observed: format!("{lease:?}"),
        });
    }
    let minimum = LeaseMinimumValidity::try_new(Duration::from_nanos(1)).map_err(|source| {
        IndexLeaseConformanceFailure::Adapter {
            step: "minimum-validity fixture",
            source,
        }
    })?;
    succeed(
        "open validation",
        coordinator.validate_for(&lease, minimum).await,
    )?;
    succeed("open renewal", coordinator.renew(&lease).await)?;
    expect_error(
        "other generation remains absent",
        coordinator
            .acquire(fixture.second_generation, fixture.second_holder)
            .await,
        IndexLeaseError::GenerationUnavailable,
    )?;

    let fence = succeed(
        "begin drain",
        coordinator
            .begin_drain(fixture.first_generation, None)
            .await,
    )?;
    let repeated = succeed(
        "idempotent begin drain",
        coordinator
            .begin_drain(fixture.first_generation, None)
            .await,
    )?;
    let persisted = succeed(
        "persisted begin drain",
        coordinator
            .begin_drain(fixture.first_generation, Some(&fence))
            .await,
    )?;
    if repeated != fence || persisted != fence {
        return Err(IndexLeaseConformanceFailure::Contract {
            step: "idempotent drain fence",
            expected: "the original exact fence",
            observed: format!("first={fence:?}, repeated={repeated:?}, persisted={persisted:?}"),
        });
    }
    expect_error(
        "draining acquisition",
        coordinator
            .acquire(fixture.first_generation, fixture.second_holder)
            .await,
        IndexLeaseError::GenerationDraining,
    )?;
    expect_error(
        "draining renewal",
        coordinator.renew(&lease).await,
        IndexLeaseError::GenerationDraining,
    )?;
    succeed(
        "inflight drain validation",
        coordinator.validate_for(&lease, minimum).await,
    )?;
    if succeed("drain with reader", coordinator.check_drained(&fence).await)? {
        return Err(IndexLeaseConformanceFailure::Contract {
            step: "drain with reader",
            expected: "false while an unexpired lease remains",
            observed: "true".to_string(),
        });
    }
    succeed("lease release", coordinator.release(&lease).await)?;
    succeed(
        "idempotent lease release",
        coordinator.release(&lease).await,
    )?;
    if !succeed(
        "drain after release",
        coordinator.check_drained(&fence).await,
    )? {
        return Err(IndexLeaseConformanceFailure::Contract {
            step: "drain after release",
            expected: "true after every lease is released",
            observed: "false".to_string(),
        });
    }
    let Some(stale_epoch) = fence.epoch().get().checked_add(1) else {
        return Err(IndexLeaseConformanceFailure::Contract {
            step: "stale fence fixture",
            expected: "a non-terminal drain epoch",
            observed: fence.epoch().get().to_string(),
        });
    };
    let stale = DrainFence::try_from_persisted(fixture.first_generation, stale_epoch).map_err(
        |source| IndexLeaseConformanceFailure::Adapter {
            step: "stale fence fixture",
            source,
        },
    )?;
    expect_error(
        "stale persisted drain",
        coordinator
            .begin_drain(fixture.first_generation, Some(&stale))
            .await,
        IndexLeaseError::StaleDrainFence,
    )?;
    expect_error(
        "stale drain check",
        coordinator.check_drained(&stale).await,
        IndexLeaseError::StaleDrainFence,
    )?;
    succeed("finish drain", coordinator.finish_drain(&fence).await)?;
    succeed(
        "idempotent finish drain",
        coordinator.finish_drain(&fence).await,
    )?;
    expect_error(
        "closed registration",
        coordinator
            .register_generation(fixture.first_generation)
            .await,
        IndexLeaseError::GenerationClosed,
    )?;
    expect_error(
        "closed acquisition",
        coordinator
            .acquire(fixture.first_generation, fixture.first_holder)
            .await,
        IndexLeaseError::GenerationClosed,
    )?;

    succeed(
        "isolated generation registration",
        coordinator
            .register_generation(fixture.second_generation)
            .await,
    )?;
    let second_lease = succeed(
        "isolated generation acquisition",
        coordinator
            .acquire(fixture.second_generation, fixture.second_holder)
            .await,
    )?;
    succeed(
        "isolated generation release",
        coordinator.release(&second_lease).await,
    )?;
    let second_fence = succeed(
        "isolated generation drain",
        coordinator
            .begin_drain(fixture.second_generation, None)
            .await,
    )?;
    if !succeed(
        "isolated generation drained",
        coordinator.check_drained(&second_fence).await,
    )? {
        return Err(IndexLeaseConformanceFailure::Contract {
            step: "isolated generation drained",
            expected: "true after release",
            observed: "false".to_string(),
        });
    }
    succeed(
        "isolated generation finish",
        coordinator.finish_drain(&second_fence).await,
    )
}

fn succeed<T>(
    step: &'static str,
    result: Result<T, IndexLeaseError>,
) -> Result<T, IndexLeaseConformanceFailure> {
    result.map_err(|source| IndexLeaseConformanceFailure::Adapter { step, source })
}

fn expect_error<T: Debug>(
    step: &'static str,
    result: Result<T, IndexLeaseError>,
    expected: IndexLeaseError,
) -> Result<(), IndexLeaseConformanceFailure> {
    match result {
        Err(observed) if observed == expected => Ok(()),
        observed => Err(IndexLeaseConformanceFailure::Contract {
            step,
            expected: match expected {
                IndexLeaseError::InvalidTiming(_) => "InvalidTiming",
                IndexLeaseError::NilUuid { .. } => "NilUuid",
                IndexLeaseError::MinimumValidityOverflow => "MinimumValidityOverflow",
                IndexLeaseError::GenerationUnavailable => "GenerationUnavailable",
                IndexLeaseError::GenerationDraining => "GenerationDraining",
                IndexLeaseError::GenerationClosed => "GenerationClosed",
                IndexLeaseError::LeaseNotCurrent => "LeaseNotCurrent",
                IndexLeaseError::LeaseCredentialMismatch => "LeaseCredentialMismatch",
                IndexLeaseError::LeaseValidityInsufficient => "LeaseValidityInsufficient",
                IndexLeaseError::StaleDrainFence => "StaleDrainFence",
                IndexLeaseError::ReadersRemain => "ReadersRemain",
                IndexLeaseError::IdentifierAllocationExhausted => "IdentifierAllocationExhausted",
                IndexLeaseError::GenerationEpochExhausted => "GenerationEpochExhausted",
                IndexLeaseError::BackendClockOverflow => "BackendClockOverflow",
                IndexLeaseError::Coordinator(_) => "Coordinator",
            },
            observed: format!("{observed:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::v1::keys::tenant::DataScope;
    use crate::index_v2::reader_lease::{ProcessLocalIndexLeaseCoordinator, ReaderLeaseTiming};
    use crate::index_v2::{IndexGenerationId, IndexId};

    #[tokio::test]
    async fn process_local_adapter_passes_the_reusable_contract() {
        let coordinator = ProcessLocalIndexLeaseCoordinator::new(ReaderLeaseTiming::default());
        let fixture = IndexLeaseConformanceFixture::try_new(
            LeaseGenerationKey::new(
                DataScope::LegacyUnscoped,
                IndexId::initial(),
                IndexGenerationId::initial(),
            ),
            LeaseGenerationKey::new(
                DataScope::LegacyUnscoped,
                IndexId::initial(),
                IndexGenerationId::new(2).unwrap(),
            ),
            LeaseHolderId::new_v4(),
            LeaseHolderId::new_v4(),
        )
        .unwrap();

        verify_index_lease_coordinator(&coordinator, fixture)
            .await
            .unwrap();
    }

    #[test]
    fn fixture_rejects_aliasing_generation_and_holder_inputs() {
        let generation = LeaseGenerationKey::new(
            DataScope::LegacyUnscoped,
            IndexId::initial(),
            IndexGenerationId::initial(),
        );
        let holder = LeaseHolderId::new_v4();
        assert!(matches!(
            IndexLeaseConformanceFixture::try_new(generation, generation, holder, holder),
            Err(IndexLeaseConformanceFailure::InvalidFixture(_))
        ));
    }
}
