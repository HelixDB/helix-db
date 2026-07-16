//! Request-owned reader guards for canonical Active index generations.
//!
//! A request first resolves an [`super::ActiveIndexHandle`], acquires the
//! coordinator lease, and then re-reads that exact canonical record before any
//! physical access. Every physical batch is admitted by backend-authoritative
//! lease validation and a client monotonic timeout. The request retains its
//! narrow guards until one final validation immediately before result
//! publication, then releases every exact lease.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use slatedb::DbReadOps;
use tokio::sync::Mutex;

use crate::error::{HelixDbError, IndexFamily, IndexLifecycleUnavailableReason, Result};

use super::reader_lease::{
    IndexLeaseCoordinator, IndexLeaseError, LeaseGenerationKey, LeaseHolderId,
    LeaseMinimumValidity, ReadLease, ReaderLeaseTiming,
};
use super::ActiveIndexHandle;

const DEFAULT_PHYSICAL_BATCH_TIMEOUT: Duration = Duration::from_secs(10);

/// Validated client timing applied uniformly to every physical index family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IndexReadLeasePolicy {
    batch_timeout: Duration,
    renewal_interval: Duration,
    batch_minimum: LeaseMinimumValidity,
    publication_minimum: LeaseMinimumValidity,
}

impl IndexReadLeasePolicy {
    /// Validates the client timeout, renewal cadence, and safety margin.
    fn try_new(
        batch_timeout: Duration,
        renewal_interval: Duration,
        io_safety_margin: Duration,
    ) -> std::result::Result<Self, IndexLeaseError> {
        if batch_timeout.is_zero() {
            return Err(IndexLeaseError::InvalidTiming(
                "physical batch timeout must be positive",
            ));
        }
        if renewal_interval.is_zero() {
            return Err(IndexLeaseError::InvalidTiming(
                "client renewal interval must be positive",
            ));
        }
        let batch_minimum = LeaseMinimumValidity::for_batch(batch_timeout, io_safety_margin)?;
        let publication_minimum = LeaseMinimumValidity::try_new(io_safety_margin)?;
        Ok(Self {
            batch_timeout,
            renewal_interval,
            batch_minimum,
            publication_minimum,
        })
    }

    fn batch_minimum(self) -> LeaseMinimumValidity {
        self.batch_minimum
    }

    fn publication_minimum(self) -> LeaseMinimumValidity {
        self.publication_minimum
    }
}

impl Default for IndexReadLeasePolicy {
    fn default() -> Self {
        let coordinator = ReaderLeaseTiming::default();
        Self::try_new(
            DEFAULT_PHYSICAL_BATCH_TIMEOUT,
            coordinator.renewal_interval(),
            coordinator.io_safety_margin(),
        )
        .expect("frozen request reader-lease defaults are valid")
    }
}

/// One exact lease retained until its request either aborts or publishes.
struct IndexReadGuard {
    family: IndexFamily,
    coordinator: Arc<dyn IndexLeaseCoordinator>,
    lease: ReadLease,
    policy: IndexReadLeasePolicy,
    last_renewal: Instant,
}

impl IndexReadGuard {
    async fn prepare_batch(&mut self) -> Result<Duration> {
        self.renew_if_due().await?;
        self.coordinator
            .validate_for(&self.lease, self.policy.batch_minimum())
            .await
            .map_err(|error| coordination_unavailable(self.family, error))?;
        Ok(self.policy.batch_timeout)
    }

    async fn validate_for_publication(&mut self) -> Result<()> {
        self.renew_if_due().await?;
        self.coordinator
            .validate_for(&self.lease, self.policy.publication_minimum())
            .await
            .map_err(|error| coordination_unavailable(self.family, error))
    }

    async fn renew_if_due(&mut self) -> Result<()> {
        if self.last_renewal.elapsed() < self.policy.renewal_interval {
            return Ok(());
        }
        self.coordinator
            .renew(&self.lease)
            .await
            .map_err(|error| coordination_unavailable(self.family, error))?;
        self.last_renewal = Instant::now();
        Ok(())
    }

    async fn release(&self) -> Result<()> {
        self.coordinator
            .release(&self.lease)
            .await
            .map_err(|error| coordination_unavailable(self.family, error))
    }
}

/// Complete lease set owned by one interpreter request.
///
/// The map deduplicates repeated access to one immutable generation while its
/// guards remain narrow: each value owns only coordinator authority, a typed
/// lease, family diagnostics, and client timing.
#[derive(Clone)]
pub(crate) struct RequestIndexReadLeases {
    guards: Arc<Mutex<HashMap<LeaseGenerationKey, Arc<Mutex<IndexReadGuard>>>>>,
    policy: IndexReadLeasePolicy,
}

impl RequestIndexReadLeases {
    /// Creates an empty request set with an explicit validated policy.
    #[cfg(test)]
    pub(crate) fn with_policy(policy: IndexReadLeasePolicy) -> Self {
        Self {
            guards: Arc::new(Mutex::new(HashMap::new())),
            policy,
        }
    }

    /// Installs a registered lease for lower-level physical storage tests.
    ///
    /// Production callers must use [`Self::acquire`], which performs canonical
    /// post-acquisition revalidation. This test-only boundary lets physical
    /// vector tests remain focused on descriptor/cache behavior without
    /// fabricating a second canonical catalog fixture.
    #[cfg(test)]
    pub(crate) async fn install_registered_for_storage_test(
        &self,
        generation: LeaseGenerationKey,
        family: IndexFamily,
        holder_id: LeaseHolderId,
    ) -> Result<()> {
        let coordinator: Arc<dyn IndexLeaseCoordinator> =
            Arc::new(super::reader_lease::ProcessLocalIndexLeaseCoordinator::new(
                ReaderLeaseTiming::default(),
            ));
        coordinator
            .register_generation(generation)
            .await
            .map_err(|error| coordination_unavailable(family, error))?;
        let lease = coordinator
            .acquire(generation, holder_id)
            .await
            .map_err(|error| coordination_unavailable(family, error))?;
        self.guards.lock().await.insert(
            generation,
            Arc::new(Mutex::new(IndexReadGuard {
                family,
                coordinator,
                lease,
                policy: self.policy,
                last_renewal: Instant::now(),
            })),
        );
        Ok(())
    }

    /// Acquires once and revalidates the exact Active record after acquisition.
    pub(crate) async fn acquire(
        &self,
        reader: &(impl DbReadOps + Sync),
        coordinator: Option<Arc<dyn IndexLeaseCoordinator>>,
        holder_id: LeaseHolderId,
        handle: &ActiveIndexHandle,
    ) -> Result<LeaseGenerationKey> {
        let generation =
            LeaseGenerationKey::new(handle.scope(), handle.index_id(), handle.generation());
        let family = handle.family();
        let mut guards = self.guards.lock().await;
        if guards.contains_key(&generation) {
            super::repository::revalidate_active_handle(reader, handle).await?;
            return Ok(generation);
        }
        let Some(coordinator) = coordinator else {
            return Err(coordination_unavailable(
                family,
                IndexLeaseError::GenerationUnavailable,
            ));
        };
        let lease = coordinator
            .acquire(generation, holder_id)
            .await
            .map_err(|error| coordination_unavailable(family, error))?;
        if let Err(error) = super::repository::revalidate_active_handle(reader, handle).await {
            coordinator
                .release(&lease)
                .await
                .map_err(|release_error| coordination_unavailable(family, release_error))?;
            return Err(error);
        }
        guards.insert(
            generation,
            Arc::new(Mutex::new(IndexReadGuard {
                family,
                coordinator,
                lease,
                policy: self.policy,
                last_renewal: Instant::now(),
            })),
        );
        Ok(generation)
    }

    /// Runs one admitted physical/blob batch under its monotonic timeout.
    pub(crate) async fn run_batch<T>(
        &self,
        generation: LeaseGenerationKey,
        batch: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        let guard = {
            let guards = self.guards.lock().await;
            let Some(guard) = guards.get(&generation) else {
                return Err(HelixDbError::InvariantViolation(
                    "physical index batch has no request-owned reader lease".to_string(),
                ));
            };
            Arc::clone(guard)
        };
        let timeout = guard.lock().await.prepare_batch().await?;
        tokio::time::timeout(timeout, batch).await.map_err(|_| {
            HelixDbError::Query("physical index read batch exceeded its monotonic timeout".into())
        })?
    }

    /// Revalidates all leases immediately before publication and releases them.
    pub(crate) async fn validate_and_release(&self) -> Result<()> {
        let guards = {
            let mut retained = self.guards.lock().await;
            core::mem::take(&mut *retained)
        };
        let mut first_error = None;
        for guard in guards.into_values() {
            let mut guard = guard.lock().await;
            if let Err(error) = guard.validate_for_publication().await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
            if let Err(error) = guard.release().await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Releases all retained leases after a request is already known to fail.
    pub(crate) async fn release_all(&self) {
        let guards = {
            let mut retained = self.guards.lock().await;
            core::mem::take(&mut *retained)
        };
        for guard in guards.into_values() {
            let guard = guard.lock().await;
            let _ = guard.release().await;
        }
    }
}

impl Default for RequestIndexReadLeases {
    fn default() -> Self {
        Self {
            guards: Arc::new(Mutex::new(HashMap::new())),
            policy: IndexReadLeasePolicy::default(),
        }
    }
}

fn coordination_unavailable(family: IndexFamily, error: IndexLeaseError) -> HelixDbError {
    tracing::warn!(%family, %error, "index reader coordination failed closed");
    HelixDbError::IndexLifecycleUnavailable {
        family,
        reason: IndexLifecycleUnavailableReason::ReaderCoordinationUnavailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SecondaryIndexDefinition;
    use crate::encoding::v1::keys::index_v2::IndexV2Key;
    use crate::encoding::v1::keys::tenant::DataScope;
    use crate::encoding::v1::keys::{DataKeyKind, Key};
    use crate::encoding::v1::values::index_v2::encode_index_record;
    use crate::index_v2::reader_lease::ProcessLocalIndexLeaseCoordinator;
    use crate::index_v2::{
        IndexGenerationId, IndexId, IndexOperationId, IndexRecordV2, IndexRevision,
        IndexStateTransition, PhysicalGeneration, ValidatedDynamicIndexDefinition,
    };

    async fn active_fixture(
        name: &str,
    ) -> (
        slatedb::Db,
        Arc<ProcessLocalIndexLeaseCoordinator>,
        ActiveIndexHandle,
        IndexRecordV2,
    ) {
        let store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let db = slatedb::Db::open(name, store)
            .await
            .expect("reader-guard fixture database opens");
        let definition = ValidatedDynamicIndexDefinition::try_from(
            SecondaryIndexDefinition::node_equality("User", "email")
                .expect("secondary fixture definition is valid"),
        )
        .expect("secondary fixture definition validates");
        let building = IndexRecordV2::building(
            IndexId::initial(),
            definition,
            IndexRevision::initial(),
            PhysicalGeneration::Secondary {
                generation: IndexGenerationId::initial(),
            },
            IndexOperationId::new_v4(),
        )
        .expect("building fixture record is valid");
        let active = building
            .transition(IndexStateTransition::Activate)
            .expect("building fixture activates");
        let handle = ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &active)
            .expect("active fixture projects a handle");
        db.put(
            Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::IndexV2(IndexV2Key::index_record(active.identity().clone())),
            }
            .to_bytes(),
            encode_index_record(&active),
        )
        .await
        .expect("active fixture record persists");
        let coordinator = Arc::new(ProcessLocalIndexLeaseCoordinator::new(
            ReaderLeaseTiming::default(),
        ));
        coordinator
            .register_generation(LeaseGenerationKey::new(
                handle.scope(),
                handle.index_id(),
                handle.generation(),
            ))
            .await
            .expect("active fixture generation registers");
        (db, coordinator, handle, active)
    }

    #[tokio::test]
    async fn request_guard_revalidates_batches_and_releases_before_drain() {
        let (db, coordinator, handle, _) = active_fixture("reader-guard-release").await;
        let coordinator_trait: Arc<dyn IndexLeaseCoordinator> = coordinator.clone();
        let leases = RequestIndexReadLeases::default();
        let generation = leases
            .acquire(
                &db,
                Some(coordinator_trait),
                LeaseHolderId::new_v4(),
                &handle,
            )
            .await
            .expect("active generation lease acquires and revalidates");
        let value = leases
            .run_batch(generation, async { Ok::<_, HelixDbError>(7_u64) })
            .await
            .expect("validated physical batch completes");
        assert_eq!(value, 7);
        leases
            .validate_and_release()
            .await
            .expect("publication validates and exact lease releases");

        let fence = coordinator
            .begin_drain(generation, None)
            .await
            .expect("released generation starts draining");
        assert!(coordinator.check_drained(&fence).await.unwrap());
        db.close().await.expect("reader-guard fixture closes");
    }

    #[tokio::test]
    async fn missing_coordinator_fails_before_physical_batch_authority_exists() {
        let (db, _, handle, _) = active_fixture("reader-guard-missing-coordinator").await;
        let leases = RequestIndexReadLeases::default();
        let error = leases
            .acquire(&db, None, LeaseHolderId::new_v4(), &handle)
            .await
            .expect_err("missing reader coordination fails closed");
        assert!(matches!(
            error,
            HelixDbError::IndexLifecycleUnavailable {
                family: IndexFamily::Secondary,
                reason: IndexLifecycleUnavailableReason::ReaderCoordinationUnavailable,
            }
        ));
        db.close().await.expect("reader-guard fixture closes");
    }

    #[tokio::test]
    async fn retained_generation_rejects_a_later_non_active_canonical_revision() {
        let (db, coordinator, handle, active) = active_fixture("reader-guard-stale-record").await;
        let coordinator_trait: Arc<dyn IndexLeaseCoordinator> = coordinator;
        let leases = RequestIndexReadLeases::default();
        leases
            .acquire(
                &db,
                Some(Arc::clone(&coordinator_trait)),
                LeaseHolderId::new_v4(),
                &handle,
            )
            .await
            .expect("initial active generation lease acquires");
        let dropping = active
            .transition(IndexStateTransition::BeginDrop {
                drop_operation_id: IndexOperationId::new_v4(),
            })
            .expect("active fixture enters dropping");
        db.put(
            Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::IndexV2(IndexV2Key::index_record(active.identity().clone())),
            }
            .to_bytes(),
            encode_index_record(&dropping),
        )
        .await
        .expect("dropping revision persists");

        assert!(matches!(
            leases
                .acquire(
                    &db,
                    Some(coordinator_trait),
                    LeaseHolderId::new_v4(),
                    &handle,
                )
                .await,
            Err(HelixDbError::StaleIndexGeneration { .. })
        ));
        leases.release_all().await;
        db.close().await.expect("reader-guard fixture closes");
    }

    #[tokio::test]
    async fn physical_batch_is_cancelled_at_the_validated_monotonic_timeout() {
        let (db, coordinator, handle, _) = active_fixture("reader-guard-batch-timeout").await;
        let coordinator_trait: Arc<dyn IndexLeaseCoordinator> = coordinator;
        let leases = RequestIndexReadLeases::with_policy(
            IndexReadLeasePolicy::try_new(
                Duration::from_millis(5),
                Duration::from_secs(1),
                Duration::from_millis(1),
            )
            .expect("short test policy validates"),
        );
        let generation = leases
            .acquire(
                &db,
                Some(coordinator_trait),
                LeaseHolderId::new_v4(),
                &handle,
            )
            .await
            .expect("active generation lease acquires");
        let error = leases
            .run_batch(generation, async {
                tokio::time::sleep(Duration::from_millis(25)).await;
                Ok::<_, HelixDbError>(())
            })
            .await
            .expect_err("batch exceeding the monotonic deadline is cancelled");
        assert!(
            matches!(error, HelixDbError::Query(message) if message.contains("monotonic timeout"))
        );
        leases.release_all().await;
        db.close().await.expect("reader-guard fixture closes");
    }

    #[tokio::test]
    async fn admitted_same_generation_batches_do_not_serialize_physical_io() {
        let (db, coordinator, handle, _) = active_fixture("reader-guard-parallel-batches").await;
        let coordinator_trait: Arc<dyn IndexLeaseCoordinator> = coordinator;
        let leases = RequestIndexReadLeases::with_policy(
            IndexReadLeasePolicy::try_new(
                Duration::from_millis(100),
                Duration::from_secs(1),
                Duration::from_millis(1),
            )
            .expect("parallel test policy validates"),
        );
        let generation = leases
            .acquire(
                &db,
                Some(coordinator_trait),
                LeaseHolderId::new_v4(),
                &handle,
            )
            .await
            .expect("active generation lease acquires");
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let first_barrier = Arc::clone(&barrier);
        let second_barrier = barrier;
        tokio::time::timeout(Duration::from_millis(50), async {
            let (first, second) = tokio::join!(
                leases.run_batch(generation, async move {
                    first_barrier.wait().await;
                    Ok::<_, HelixDbError>(1_u8)
                }),
                leases.run_batch(generation, async move {
                    second_barrier.wait().await;
                    Ok::<_, HelixDbError>(2_u8)
                }),
            );
            assert_eq!(first.unwrap(), 1);
            assert_eq!(second.unwrap(), 2);
        })
        .await
        .expect("validated batches reach physical I/O concurrently");
        leases.release_all().await;
        db.close().await.expect("reader-guard fixture closes");
    }
}
