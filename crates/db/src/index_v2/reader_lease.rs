//! Cross-process reader leases for exact physical index generations.
//!
//! Canonical `Active` state authorizes a reader to request a lease, while this
//! coordinator prevents physical cleanup from racing already-authorized reads.
//! The coordinator identity deliberately excludes canonical record revision:
//! DROP increments that revision before draining leases acquired for the same
//! immutable generation.
//!
//! Implementations must use backend-authoritative time. A client receives no
//! absolute expiry and therefore cannot compare a backend clock with its local
//! wall clock. Before each physical batch and before publishing its result, the
//! client asks the coordinator to validate the lease for a typed minimum
//! duration.
//!
//! ```
//! use std::time::Duration;
//!
//! use db::encoding::v1::keys::tenant::DataScope;
//! use db::index_v2::reader_lease::{
//!     LeaseGenerationKey, LeaseMinimumValidity, ReaderLeaseTiming,
//! };
//! use db::index_v2::{IndexGenerationId, IndexId};
//!
//! let generation = LeaseGenerationKey::new(
//!     DataScope::LegacyUnscoped,
//!     IndexId::initial(),
//!     IndexGenerationId::initial(),
//! );
//! assert_eq!(generation.generation(), IndexGenerationId::initial());
//!
//! let timing = ReaderLeaseTiming::default();
//! let minimum = LeaseMinimumValidity::for_batch(
//!     Duration::from_secs(2),
//!     timing.io_safety_margin(),
//! )
//! .unwrap();
//! assert_eq!(minimum.get(), Duration::from_secs(7));
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use uuid::Uuid;

use crate::encoding::v1::keys::tenant::DataScope;

use super::{IndexGenerationId, IndexId};

pub mod conformance;

const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(30);
const DEFAULT_RENEWAL_INTERVAL: Duration = Duration::from_secs(10);
const DEFAULT_IO_SAFETY_MARGIN: Duration = Duration::from_secs(5);
const LEASE_ID_ALLOCATION_ATTEMPTS: usize = 32;

/// Failure reported by an index reader-lease coordinator.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IndexLeaseError {
    /// The requested timing contains a zero or internally inconsistent value.
    #[error("invalid reader-lease timing: {0}")]
    InvalidTiming(&'static str),
    /// A UUID-backed lease identity is nil.
    #[error("{kind} must be a non-nil UUID")]
    NilUuid {
        /// Stable identity name used by adapters and diagnostics.
        kind: &'static str,
    },
    /// A batch timeout plus safety margin overflowed [`Duration`].
    #[error("reader-lease minimum-validity duration overflowed")]
    MinimumValidityOverflow,
    /// The generation was never registered or its compacted closed state is absent.
    #[error("index generation is not registered with the reader-lease coordinator")]
    GenerationUnavailable,
    /// The generation has started draining and rejects new acquisition or renewal.
    #[error("index generation is draining")]
    GenerationDraining,
    /// The generation is permanently closed and rejects new acquisition or renewal.
    #[error("index generation is closed")]
    GenerationClosed,
    /// The supplied lease is unknown, expired, or no longer current.
    #[error("index reader lease is not current")]
    LeaseNotCurrent,
    /// A lease ID resolved to different opaque credentials.
    #[error("index reader lease credentials do not match")]
    LeaseCredentialMismatch,
    /// Backend-authoritative remaining validity is shorter than requested.
    #[error("index reader lease cannot cover the requested minimum validity")]
    LeaseValidityInsufficient,
    /// A caller retried cleanup with a fence other than the current drain fence.
    #[error("index reader drain fence is stale")]
    StaleDrainFence,
    /// The exact drain fence still has unexpired readers.
    #[error("index generation still has active reader leases")]
    ReadersRemain,
    /// UUID allocation repeatedly collided in the coordinator namespace.
    #[error("index reader lease identifier allocation exhausted")]
    IdentifierAllocationExhausted,
    /// The per-generation monotonic epoch exhausted its integer domain.
    #[error("index reader-lease generation epoch exhausted")]
    GenerationEpochExhausted,
    /// Backend-authoritative expiry arithmetic overflowed.
    #[error("index reader lease expiry overflowed the backend clock")]
    BackendClockOverflow,
    /// A trusted external adapter could not complete an operation.
    #[error("index reader-lease coordinator failure: {0}")]
    Coordinator(String),
}

/// Stable coordinator identity for one immutable physical generation.
///
/// Canonical record revision is intentionally absent. Generations are never
/// reused, and the revision remains a separate post-acquisition revalidation
/// concern for [`super::ActiveIndexHandle`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LeaseGenerationKey {
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
}

impl LeaseGenerationKey {
    /// Constructs the stable identity used by every coordinator method.
    pub const fn new(scope: DataScope, index_id: IndexId, generation: IndexGenerationId) -> Self {
        Self {
            scope,
            index_id,
            generation,
        }
    }

    /// Returns the storage scope containing the canonical index record.
    pub const fn scope(self) -> DataScope {
        self.scope
    }

    /// Returns the stable logical index ID.
    pub const fn index_id(self) -> IndexId {
        self.index_id
    }

    /// Returns the immutable physical generation ID.
    pub const fn generation(self) -> IndexGenerationId {
        self.generation
    }
}

/// Runtime identity of the process or service instance holding reader leases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LeaseHolderId(Uuid);

impl LeaseHolderId {
    /// Creates a fresh non-nil holder identity.
    pub fn new_v4() -> Self {
        Self(Uuid::new_v4())
    }

    /// Validates an externally assigned holder UUID.
    pub fn try_from_uuid(value: Uuid) -> Result<Self, IndexLeaseError> {
        if value.is_nil() {
            return Err(IndexLeaseError::NilUuid {
                kind: "reader-lease holder ID",
            });
        }
        Ok(Self(value))
    }

    /// Returns the opaque UUID for diagnostics and external adapter transport.
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for LeaseHolderId {
    fn default() -> Self {
        Self::new_v4()
    }
}

/// Monotonic per-generation coordinator epoch.
///
/// Epoch zero is the initial open state. Every drain fence uses a checked
/// successor, so persisted cleanup always carries a non-zero value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LeaseGenerationEpoch(u64);

impl LeaseGenerationEpoch {
    const INITIAL: Self = Self(0);

    /// Validates an epoch returned by a trusted external adapter.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the value persisted by lifecycle cleanup progress.
    pub const fn get(self) -> u64 {
        self.0
    }

    fn checked_next(self) -> Result<Self, IndexLeaseError> {
        let Some(next) = self.0.checked_add(1) else {
            return Err(IndexLeaseError::GenerationEpochExhausted);
        };
        Ok(Self(next))
    }
}

/// Opaque coordinator-issued reader authorization.
///
/// There is deliberately no expiry accessor. Callers may only return the
/// complete value to [`IndexLeaseCoordinator`] methods.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadLease {
    generation: LeaseGenerationKey,
    holder_id: LeaseHolderId,
    lease_id: Uuid,
    token: Uuid,
    epoch: LeaseGenerationEpoch,
}

impl ReadLease {
    /// Constructs an opaque grant returned by a trusted external adapter.
    ///
    /// Nil IDs cannot become lease credentials. Constructing this value does
    /// not itself grant authority: every use remains subject to coordinator
    /// validation.
    pub fn try_from_parts(
        generation: LeaseGenerationKey,
        holder_id: LeaseHolderId,
        lease_id: Uuid,
        token: Uuid,
        epoch: LeaseGenerationEpoch,
    ) -> Result<Self, IndexLeaseError> {
        if lease_id.is_nil() {
            return Err(IndexLeaseError::NilUuid {
                kind: "reader lease ID",
            });
        }
        if token.is_nil() {
            return Err(IndexLeaseError::NilUuid {
                kind: "reader lease token",
            });
        }
        Ok(Self {
            generation,
            holder_id,
            lease_id,
            token,
            epoch,
        })
    }

    /// Returns the exact physical generation authorized by this lease.
    pub const fn generation(&self) -> LeaseGenerationKey {
        self.generation
    }

    /// Returns the runtime holder identity for diagnostics.
    pub const fn holder_id(&self) -> LeaseHolderId {
        self.holder_id
    }

    /// Returns the generation epoch observed at acquisition.
    pub const fn epoch(&self) -> LeaseGenerationEpoch {
        self.epoch
    }

    /// Returns the opaque lease ID for trusted adapter transport.
    pub const fn lease_id(&self) -> Uuid {
        self.lease_id
    }

    /// Returns the opaque validation token for trusted adapter transport.
    pub const fn token(&self) -> Uuid {
        self.token
    }
}

/// Exact monotonic fence retained by a durable cleanup operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DrainFence {
    generation: LeaseGenerationKey,
    epoch: LeaseGenerationEpoch,
}

impl DrainFence {
    /// Reconstructs a fence from durable cleanup progress.
    ///
    /// Epoch zero cannot represent a drain and is rejected.
    pub fn try_from_persisted(
        generation: LeaseGenerationKey,
        epoch: u64,
    ) -> Result<Self, IndexLeaseError> {
        if epoch == 0 {
            return Err(IndexLeaseError::StaleDrainFence);
        }
        Ok(Self {
            generation,
            epoch: LeaseGenerationEpoch::new(epoch),
        })
    }

    /// Returns the exact generation closed by this fence.
    pub const fn generation(self) -> LeaseGenerationKey {
        self.generation
    }

    /// Returns the monotonic value persisted by cleanup progress.
    pub const fn epoch(self) -> LeaseGenerationEpoch {
        self.epoch
    }
}

/// Positive coordinator-authoritative validity requested for one I/O boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LeaseMinimumValidity(Duration);

impl LeaseMinimumValidity {
    /// Validates a non-zero minimum validity duration.
    pub fn try_new(value: Duration) -> Result<Self, IndexLeaseError> {
        if value.is_zero() {
            return Err(IndexLeaseError::InvalidTiming(
                "minimum validity must be positive",
            ));
        }
        Ok(Self(value))
    }

    /// Adds a trusted I/O safety margin to one batch timeout.
    pub fn for_batch(
        batch_timeout: Duration,
        safety_margin: Duration,
    ) -> Result<Self, IndexLeaseError> {
        let Some(value) = batch_timeout.checked_add(safety_margin) else {
            return Err(IndexLeaseError::MinimumValidityOverflow);
        };
        Self::try_new(value)
    }

    /// Returns the duration passed to an external coordinator adapter.
    pub const fn get(self) -> Duration {
        self.0
    }
}

/// Validated process-local lease timing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReaderLeaseTiming {
    ttl: Duration,
    renewal_interval: Duration,
    io_safety_margin: Duration,
}

impl ReaderLeaseTiming {
    /// Validates TTL, renewal, and I/O safety-margin relationships.
    pub fn try_new(
        ttl: Duration,
        renewal_interval: Duration,
        io_safety_margin: Duration,
    ) -> Result<Self, IndexLeaseError> {
        if ttl.is_zero() {
            return Err(IndexLeaseError::InvalidTiming("TTL must be positive"));
        }
        if renewal_interval.is_zero() {
            return Err(IndexLeaseError::InvalidTiming(
                "renewal interval must be positive",
            ));
        }
        if io_safety_margin.is_zero() {
            return Err(IndexLeaseError::InvalidTiming(
                "I/O safety margin must be positive",
            ));
        }
        let Some(renewal_with_margin) = renewal_interval.checked_add(io_safety_margin) else {
            return Err(IndexLeaseError::InvalidTiming(
                "renewal interval plus I/O safety margin overflowed",
            ));
        };
        if renewal_with_margin >= ttl {
            return Err(IndexLeaseError::InvalidTiming(
                "renewal interval plus I/O safety margin must be shorter than TTL",
            ));
        }
        Ok(Self {
            ttl,
            renewal_interval,
            io_safety_margin,
        })
    }

    /// Returns the coordinator-authoritative lease TTL.
    pub const fn ttl(self) -> Duration {
        self.ttl
    }

    /// Returns the client renewal cadence.
    pub const fn renewal_interval(self) -> Duration {
        self.renewal_interval
    }

    /// Returns the extra validity required around physical I/O.
    pub const fn io_safety_margin(self) -> Duration {
        self.io_safety_margin
    }
}

impl Default for ReaderLeaseTiming {
    fn default() -> Self {
        Self {
            ttl: DEFAULT_LEASE_TTL,
            renewal_interval: DEFAULT_RENEWAL_INTERVAL,
            io_safety_margin: DEFAULT_IO_SAFETY_MARGIN,
        }
    }
}

/// Shared reader coordination required before public index serving or cleanup.
///
/// External implementations must linearize every method per generation and use
/// backend-authoritative time for expiry and [`Self::validate_for`]. `Closed`
/// may eventually be compacted because generation IDs are never reused; both
/// absence and `Closed` reject acquisition.
#[async_trait]
pub trait IndexLeaseCoordinator: Send + Sync {
    /// Idempotently creates the exact generation in its initial open epoch.
    async fn register_generation(
        &self,
        generation: LeaseGenerationKey,
    ) -> Result<(), IndexLeaseError>;

    /// Acquires one lease only while the exact generation is open.
    async fn acquire(
        &self,
        generation: LeaseGenerationKey,
        holder_id: LeaseHolderId,
    ) -> Result<ReadLease, IndexLeaseError>;

    /// Renews an exact current lease using backend-authoritative time.
    async fn renew(&self, lease: &ReadLease) -> Result<(), IndexLeaseError>;

    /// Idempotently releases an exact lease.
    async fn release(&self, lease: &ReadLease) -> Result<(), IndexLeaseError>;

    /// Atomically rejects new leases and returns the current monotonic fence.
    ///
    /// `persisted` is `None` before the first durable checkpoint and the exact
    /// reconstructed fence on retries after that checkpoint.
    async fn begin_drain(
        &self,
        generation: LeaseGenerationKey,
        persisted: Option<&DrainFence>,
    ) -> Result<DrainFence, IndexLeaseError>;

    /// Returns true only for the current fence with no unexpired leases.
    async fn check_drained(&self, fence: &DrainFence) -> Result<bool, IndexLeaseError>;

    /// Proves backend-authoritative validity before I/O or result publication.
    async fn validate_for(
        &self,
        lease: &ReadLease,
        minimum: LeaseMinimumValidity,
    ) -> Result<(), IndexLeaseError>;

    /// Permanently closes a fully drained generation for the exact fence.
    async fn finish_drain(&self, fence: &DrainFence) -> Result<(), IndexLeaseError>;
}

trait LeaseClock: Send + Sync {
    fn now(&self) -> Instant;
}

#[derive(Debug)]
struct MonotonicLeaseClock;

impl LeaseClock for MonotonicLeaseClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

#[derive(Debug, Clone)]
struct ProcessLocalLeaseEntry {
    lease: ReadLease,
    expires_at: Instant,
}

#[derive(Debug)]
enum ProcessLocalGenerationState {
    Open {
        epoch: LeaseGenerationEpoch,
        leases: HashMap<Uuid, ProcessLocalLeaseEntry>,
    },
    Draining {
        fence: DrainFence,
        leases: HashMap<Uuid, ProcessLocalLeaseEntry>,
    },
    Closed {
        fence: DrainFence,
    },
}

impl ProcessLocalGenerationState {
    fn prune_expired(&mut self, now: Instant) {
        match self {
            Self::Open { leases, .. } | Self::Draining { leases, .. } => {
                leases.retain(|_, entry| entry.expires_at > now);
            }
            Self::Closed { .. } => {}
        }
    }
}

/// Deterministic in-process reference implementation of the lease contract.
///
/// This adapter is valid only when every handle to the database shares the
/// same instance. The process-local database token at the public open boundary
/// enforces that identity; disk and object-storage sources must inject a shared
/// external implementation instead.
#[derive(Clone)]
pub(crate) struct ProcessLocalIndexLeaseCoordinator {
    timing: ReaderLeaseTiming,
    clock: Arc<dyn LeaseClock>,
    generations: Arc<Mutex<HashMap<LeaseGenerationKey, ProcessLocalGenerationState>>>,
}

impl ProcessLocalIndexLeaseCoordinator {
    /// Creates an empty coordinator using the process monotonic clock.
    pub(crate) fn new(timing: ReaderLeaseTiming) -> Self {
        Self {
            timing,
            clock: Arc::new(MonotonicLeaseClock),
            generations: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[cfg(test)]
    fn with_clock(timing: ReaderLeaseTiming, clock: Arc<dyn LeaseClock>) -> Self {
        Self {
            timing,
            clock,
            generations: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn expiry(&self, now: Instant) -> Result<Instant, IndexLeaseError> {
        now.checked_add(self.timing.ttl())
            .ok_or(IndexLeaseError::BackendClockOverflow)
    }

    fn validate_entry(
        entry: &ProcessLocalLeaseEntry,
        lease: &ReadLease,
    ) -> Result<(), IndexLeaseError> {
        if entry.lease != *lease {
            return Err(IndexLeaseError::LeaseCredentialMismatch);
        }
        Ok(())
    }
}

#[async_trait]
impl IndexLeaseCoordinator for ProcessLocalIndexLeaseCoordinator {
    async fn register_generation(
        &self,
        generation: LeaseGenerationKey,
    ) -> Result<(), IndexLeaseError> {
        let mut generations = self
            .generations
            .lock()
            .expect("process-local reader-lease lock is not poisoned");
        match generations.get(&generation) {
            None => {
                generations.insert(
                    generation,
                    ProcessLocalGenerationState::Open {
                        epoch: LeaseGenerationEpoch::INITIAL,
                        leases: HashMap::new(),
                    },
                );
                Ok(())
            }
            Some(ProcessLocalGenerationState::Open { .. }) => Ok(()),
            Some(ProcessLocalGenerationState::Draining { .. }) => {
                Err(IndexLeaseError::GenerationDraining)
            }
            Some(ProcessLocalGenerationState::Closed { .. }) => {
                Err(IndexLeaseError::GenerationClosed)
            }
        }
    }

    async fn acquire(
        &self,
        generation: LeaseGenerationKey,
        holder_id: LeaseHolderId,
    ) -> Result<ReadLease, IndexLeaseError> {
        let now = self.clock.now();
        let expires_at = self.expiry(now)?;
        let mut generations = self
            .generations
            .lock()
            .expect("process-local reader-lease lock is not poisoned");
        let Some(state) = generations.get_mut(&generation) else {
            return Err(IndexLeaseError::GenerationUnavailable);
        };
        state.prune_expired(now);
        match state {
            ProcessLocalGenerationState::Open { epoch, leases } => {
                for _ in 0..LEASE_ID_ALLOCATION_ATTEMPTS {
                    let lease_id = Uuid::new_v4();
                    if leases.contains_key(&lease_id) {
                        continue;
                    }
                    let lease = ReadLease::try_from_parts(
                        generation,
                        holder_id,
                        lease_id,
                        Uuid::new_v4(),
                        *epoch,
                    )?;
                    leases.insert(
                        lease_id,
                        ProcessLocalLeaseEntry {
                            lease: lease.clone(),
                            expires_at,
                        },
                    );
                    return Ok(lease);
                }
                Err(IndexLeaseError::IdentifierAllocationExhausted)
            }
            ProcessLocalGenerationState::Draining { .. } => {
                Err(IndexLeaseError::GenerationDraining)
            }
            ProcessLocalGenerationState::Closed { .. } => Err(IndexLeaseError::GenerationClosed),
        }
    }

    async fn renew(&self, lease: &ReadLease) -> Result<(), IndexLeaseError> {
        let now = self.clock.now();
        let expires_at = self.expiry(now)?;
        let mut generations = self
            .generations
            .lock()
            .expect("process-local reader-lease lock is not poisoned");
        let Some(state) = generations.get_mut(&lease.generation) else {
            return Err(IndexLeaseError::GenerationUnavailable);
        };
        state.prune_expired(now);
        match state {
            ProcessLocalGenerationState::Open { epoch, leases } => {
                if *epoch != lease.epoch {
                    return Err(IndexLeaseError::LeaseNotCurrent);
                }
                let Some(entry) = leases.get_mut(&lease.lease_id) else {
                    return Err(IndexLeaseError::LeaseNotCurrent);
                };
                Self::validate_entry(entry, lease)?;
                entry.expires_at = expires_at;
                Ok(())
            }
            ProcessLocalGenerationState::Draining { .. } => {
                Err(IndexLeaseError::GenerationDraining)
            }
            ProcessLocalGenerationState::Closed { .. } => Err(IndexLeaseError::GenerationClosed),
        }
    }

    async fn release(&self, lease: &ReadLease) -> Result<(), IndexLeaseError> {
        let mut generations = self
            .generations
            .lock()
            .expect("process-local reader-lease lock is not poisoned");
        let Some(state) = generations.get_mut(&lease.generation) else {
            return Ok(());
        };
        let leases = match state {
            ProcessLocalGenerationState::Open { leases, .. }
            | ProcessLocalGenerationState::Draining { leases, .. } => leases,
            ProcessLocalGenerationState::Closed { .. } => return Ok(()),
        };
        let Some(entry) = leases.get(&lease.lease_id) else {
            return Ok(());
        };
        Self::validate_entry(entry, lease)?;
        leases.remove(&lease.lease_id);
        Ok(())
    }

    async fn begin_drain(
        &self,
        generation: LeaseGenerationKey,
        persisted: Option<&DrainFence>,
    ) -> Result<DrainFence, IndexLeaseError> {
        let now = self.clock.now();
        let mut generations = self
            .generations
            .lock()
            .expect("process-local reader-lease lock is not poisoned");
        let Some(state) = generations.get_mut(&generation) else {
            return Err(IndexLeaseError::GenerationUnavailable);
        };
        state.prune_expired(now);
        match state {
            ProcessLocalGenerationState::Open { epoch, leases } => {
                if persisted.is_some() {
                    return Err(IndexLeaseError::StaleDrainFence);
                }
                let fence = DrainFence {
                    generation,
                    epoch: epoch.checked_next()?,
                };
                let leases = core::mem::take(leases);
                *state = ProcessLocalGenerationState::Draining { fence, leases };
                Ok(fence)
            }
            ProcessLocalGenerationState::Draining { fence, .. }
            | ProcessLocalGenerationState::Closed { fence } => {
                if persisted.is_none_or(|persisted| persisted == fence) {
                    Ok(*fence)
                } else {
                    Err(IndexLeaseError::StaleDrainFence)
                }
            }
        }
    }

    async fn check_drained(&self, fence: &DrainFence) -> Result<bool, IndexLeaseError> {
        let now = self.clock.now();
        let mut generations = self
            .generations
            .lock()
            .expect("process-local reader-lease lock is not poisoned");
        let Some(state) = generations.get_mut(&fence.generation) else {
            return Err(IndexLeaseError::GenerationUnavailable);
        };
        state.prune_expired(now);
        match state {
            ProcessLocalGenerationState::Open { .. } => Err(IndexLeaseError::StaleDrainFence),
            ProcessLocalGenerationState::Draining {
                fence: current,
                leases,
            } => {
                if current != fence {
                    return Err(IndexLeaseError::StaleDrainFence);
                }
                Ok(leases.is_empty())
            }
            ProcessLocalGenerationState::Closed { fence: current } => {
                if current != fence {
                    return Err(IndexLeaseError::StaleDrainFence);
                }
                Ok(true)
            }
        }
    }

    async fn validate_for(
        &self,
        lease: &ReadLease,
        minimum: LeaseMinimumValidity,
    ) -> Result<(), IndexLeaseError> {
        let now = self.clock.now();
        let mut generations = self
            .generations
            .lock()
            .expect("process-local reader-lease lock is not poisoned");
        let Some(state) = generations.get_mut(&lease.generation) else {
            return Err(IndexLeaseError::GenerationUnavailable);
        };
        state.prune_expired(now);
        let leases = match state {
            ProcessLocalGenerationState::Open { leases, .. }
            | ProcessLocalGenerationState::Draining { leases, .. } => leases,
            ProcessLocalGenerationState::Closed { .. } => {
                return Err(IndexLeaseError::GenerationClosed);
            }
        };
        let Some(entry) = leases.get(&lease.lease_id) else {
            return Err(IndexLeaseError::LeaseNotCurrent);
        };
        Self::validate_entry(entry, lease)?;
        let Some(required_until) = now.checked_add(minimum.get()) else {
            return Err(IndexLeaseError::BackendClockOverflow);
        };
        if entry.expires_at < required_until {
            return Err(IndexLeaseError::LeaseValidityInsufficient);
        }
        Ok(())
    }

    async fn finish_drain(&self, fence: &DrainFence) -> Result<(), IndexLeaseError> {
        let now = self.clock.now();
        let mut generations = self
            .generations
            .lock()
            .expect("process-local reader-lease lock is not poisoned");
        let Some(state) = generations.get_mut(&fence.generation) else {
            return Err(IndexLeaseError::GenerationUnavailable);
        };
        state.prune_expired(now);
        match state {
            ProcessLocalGenerationState::Open { .. } => Err(IndexLeaseError::StaleDrainFence),
            ProcessLocalGenerationState::Draining {
                fence: current,
                leases,
            } => {
                if current != fence {
                    return Err(IndexLeaseError::StaleDrainFence);
                }
                if !leases.is_empty() {
                    return Err(IndexLeaseError::ReadersRemain);
                }
                *state = ProcessLocalGenerationState::Closed { fence: *fence };
                Ok(())
            }
            ProcessLocalGenerationState::Closed { fence: current } => {
                if current != fence {
                    return Err(IndexLeaseError::StaleDrainFence);
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct ManualLeaseClock(Mutex<Instant>);

    impl ManualLeaseClock {
        fn new() -> Self {
            Self(Mutex::new(Instant::now()))
        }

        fn advance(&self, duration: Duration) {
            let mut now = self.0.lock().expect("manual clock lock is not poisoned");
            *now = now
                .checked_add(duration)
                .expect("test clock duration remains representable");
        }
    }

    impl LeaseClock for ManualLeaseClock {
        fn now(&self) -> Instant {
            *self.0.lock().expect("manual clock lock is not poisoned")
        }
    }

    fn generation(value: u64) -> LeaseGenerationKey {
        LeaseGenerationKey::new(
            DataScope::LegacyUnscoped,
            IndexId::initial(),
            IndexGenerationId::new(value).expect("test generation is non-zero"),
        )
    }

    fn coordinator(clock: Arc<ManualLeaseClock>) -> ProcessLocalIndexLeaseCoordinator {
        ProcessLocalIndexLeaseCoordinator::with_clock(ReaderLeaseTiming::default(), clock)
    }

    #[test]
    fn timing_and_minimum_validity_reject_invalid_domains() {
        assert_eq!(
            ReaderLeaseTiming::try_new(
                Duration::ZERO,
                Duration::from_secs(1),
                Duration::from_secs(1),
            ),
            Err(IndexLeaseError::InvalidTiming("TTL must be positive"))
        );
        assert_eq!(
            ReaderLeaseTiming::try_new(
                Duration::from_secs(10),
                Duration::from_secs(7),
                Duration::from_secs(4),
            ),
            Err(IndexLeaseError::InvalidTiming(
                "renewal interval plus I/O safety margin must be shorter than TTL",
            ))
        );
        assert_eq!(
            LeaseMinimumValidity::try_new(Duration::ZERO),
            Err(IndexLeaseError::InvalidTiming(
                "minimum validity must be positive",
            ))
        );
        assert_eq!(
            LeaseMinimumValidity::for_batch(Duration::MAX, Duration::from_nanos(1)),
            Err(IndexLeaseError::MinimumValidityOverflow)
        );
        assert_eq!(ReaderLeaseTiming::default().ttl(), Duration::from_secs(30));
        assert_eq!(
            ReaderLeaseTiming::default().renewal_interval(),
            Duration::from_secs(10)
        );
        assert_eq!(
            ReaderLeaseTiming::default().io_safety_margin(),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn nil_holder_and_zero_persisted_fence_fail_closed() {
        assert_eq!(
            LeaseHolderId::try_from_uuid(Uuid::nil()),
            Err(IndexLeaseError::NilUuid {
                kind: "reader-lease holder ID",
            })
        );
        assert_eq!(
            DrainFence::try_from_persisted(generation(1), 0),
            Err(IndexLeaseError::StaleDrainFence)
        );
    }

    #[tokio::test]
    async fn registration_and_acquisition_are_exact_per_generation() {
        let clock = Arc::new(ManualLeaseClock::new());
        let coordinator = coordinator(Arc::clone(&clock));
        let first = generation(1);
        let other = generation(2);
        let holder = LeaseHolderId::new_v4();

        assert_eq!(
            coordinator.acquire(first, holder).await,
            Err(IndexLeaseError::GenerationUnavailable)
        );
        coordinator.register_generation(first).await.unwrap();
        coordinator.register_generation(first).await.unwrap();
        let lease = coordinator.acquire(first, holder).await.unwrap();
        assert_eq!(lease.generation(), first);
        assert_eq!(lease.holder_id(), holder);
        assert_eq!(lease.epoch(), LeaseGenerationEpoch::INITIAL);
        assert_eq!(
            coordinator.acquire(other, holder).await,
            Err(IndexLeaseError::GenerationUnavailable)
        );
    }

    #[tokio::test]
    async fn renewal_and_validation_use_only_backend_authoritative_time() {
        let clock = Arc::new(ManualLeaseClock::new());
        let coordinator = coordinator(Arc::clone(&clock));
        let generation = generation(3);
        coordinator.register_generation(generation).await.unwrap();
        let lease = coordinator
            .acquire(generation, LeaseHolderId::new_v4())
            .await
            .unwrap();

        clock.advance(Duration::from_secs(24));
        coordinator
            .validate_for(
                &lease,
                LeaseMinimumValidity::try_new(Duration::from_secs(6)).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            coordinator
                .validate_for(
                    &lease,
                    LeaseMinimumValidity::try_new(Duration::from_secs(7)).unwrap(),
                )
                .await,
            Err(IndexLeaseError::LeaseValidityInsufficient)
        );
        coordinator.renew(&lease).await.unwrap();
        clock.advance(Duration::from_secs(29));
        coordinator
            .validate_for(
                &lease,
                LeaseMinimumValidity::try_new(Duration::from_secs(1)).unwrap(),
            )
            .await
            .unwrap();
        clock.advance(Duration::from_secs(1));
        assert_eq!(
            coordinator
                .validate_for(
                    &lease,
                    LeaseMinimumValidity::try_new(Duration::from_nanos(1)).unwrap(),
                )
                .await,
            Err(IndexLeaseError::LeaseNotCurrent)
        );
    }

    #[tokio::test]
    async fn drain_rejects_new_work_but_allows_a_valid_inflight_batch() {
        let clock = Arc::new(ManualLeaseClock::new());
        let coordinator = coordinator(Arc::clone(&clock));
        let generation = generation(4);
        coordinator.register_generation(generation).await.unwrap();
        let lease = coordinator
            .acquire(generation, LeaseHolderId::new_v4())
            .await
            .unwrap();

        let fence = coordinator.begin_drain(generation, None).await.unwrap();
        assert_eq!(fence.epoch().get(), 1);
        assert_eq!(
            coordinator
                .acquire(generation, LeaseHolderId::new_v4())
                .await,
            Err(IndexLeaseError::GenerationDraining)
        );
        assert_eq!(
            coordinator.renew(&lease).await,
            Err(IndexLeaseError::GenerationDraining)
        );
        coordinator
            .validate_for(
                &lease,
                LeaseMinimumValidity::try_new(Duration::from_secs(5)).unwrap(),
            )
            .await
            .unwrap();
        assert!(!coordinator.check_drained(&fence).await.unwrap());
        coordinator.release(&lease).await.unwrap();
        coordinator.release(&lease).await.unwrap();
        assert!(coordinator.check_drained(&fence).await.unwrap());
    }

    #[tokio::test]
    async fn persisted_fence_is_idempotent_and_stale_fences_fail_closed() {
        let clock = Arc::new(ManualLeaseClock::new());
        let coordinator = coordinator(clock);
        let generation = generation(5);
        coordinator.register_generation(generation).await.unwrap();
        let fence = coordinator.begin_drain(generation, None).await.unwrap();

        assert_eq!(
            coordinator.begin_drain(generation, None).await.unwrap(),
            fence
        );
        assert_eq!(
            coordinator
                .begin_drain(generation, Some(&fence))
                .await
                .unwrap(),
            fence
        );
        let stale = DrainFence::try_from_persisted(generation, fence.epoch().get() + 1).unwrap();
        assert_eq!(
            coordinator.begin_drain(generation, Some(&stale)).await,
            Err(IndexLeaseError::StaleDrainFence)
        );
        assert_eq!(
            coordinator.check_drained(&stale).await,
            Err(IndexLeaseError::StaleDrainFence)
        );
    }

    #[tokio::test]
    async fn expiry_drains_without_client_wall_clock_or_release() {
        let clock = Arc::new(ManualLeaseClock::new());
        let coordinator = coordinator(Arc::clone(&clock));
        let generation = generation(6);
        coordinator.register_generation(generation).await.unwrap();
        let _lease = coordinator
            .acquire(generation, LeaseHolderId::new_v4())
            .await
            .unwrap();
        let fence = coordinator.begin_drain(generation, None).await.unwrap();

        assert!(!coordinator.check_drained(&fence).await.unwrap());
        clock.advance(Duration::from_secs(30));
        assert!(coordinator.check_drained(&fence).await.unwrap());
        coordinator.finish_drain(&fence).await.unwrap();
    }

    #[tokio::test]
    async fn finish_requires_drain_and_closed_state_remains_rejecting() {
        let clock = Arc::new(ManualLeaseClock::new());
        let coordinator = coordinator(clock);
        let generation = generation(7);
        coordinator.register_generation(generation).await.unwrap();
        let lease = coordinator
            .acquire(generation, LeaseHolderId::new_v4())
            .await
            .unwrap();
        let fence = coordinator.begin_drain(generation, None).await.unwrap();

        assert_eq!(
            coordinator.finish_drain(&fence).await,
            Err(IndexLeaseError::ReadersRemain)
        );
        coordinator.release(&lease).await.unwrap();
        coordinator.finish_drain(&fence).await.unwrap();
        coordinator.finish_drain(&fence).await.unwrap();
        assert!(coordinator.check_drained(&fence).await.unwrap());
        assert_eq!(
            coordinator.register_generation(generation).await,
            Err(IndexLeaseError::GenerationClosed)
        );
        assert_eq!(
            coordinator
                .acquire(generation, LeaseHolderId::new_v4())
                .await,
            Err(IndexLeaseError::GenerationClosed)
        );
    }
}
