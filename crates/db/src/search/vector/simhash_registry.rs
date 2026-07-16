//! Bounded, single-flight retention of deterministic SimHash projection tables.
//!
//! A projection table contains 64 `f32` hyperplanes and therefore grows
//! linearly with vector dimension. [`SimHasherRegistry`] reserves the checked
//! allocation size before construction, admits at most one constructor for an
//! exact [`SimHashIdentity`], and evicts least-recently-used registry entries to
//! remain within both byte and entry limits. Existing [`Arc`] leases remain
//! valid after eviction; recreating the same identity produces identical
//! projections because the seed and algorithm version are part of the key.

use std::collections::HashMap;
use std::num::{NonZeroU16, NonZeroUsize};
use std::sync::{Arc, Condvar, Mutex};

use super::unaligned_vector::simhash::SimHasherConstructionError;
use super::unaligned_vector::SimHasher;

/// Complete deterministic identity of one SimHash projection algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct SimHashIdentity {
    dimension: NonZeroUsize,
    seed: u64,
    algorithm_version: NonZeroU16,
}

impl SimHashIdentity {
    /// Binds a validated dimension to the descriptor's seed and algorithm ID.
    pub(crate) const fn new(
        dimension: NonZeroUsize,
        seed: u64,
        algorithm_version: NonZeroU16,
    ) -> Self {
        Self {
            dimension,
            seed,
            algorithm_version,
        }
    }

    /// Returns the projection dimension used for checked allocation.
    pub(crate) const fn dimension(self) -> NonZeroUsize {
        self.dimension
    }

    /// Returns the deterministic random seed bound by the descriptor.
    pub(crate) const fn seed(self) -> u64 {
        self.seed
    }
}

/// Validated byte and entry limits for one registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SimHasherRegistryLimits {
    bytes: NonZeroUsize,
    entries: NonZeroUsize,
}

impl SimHasherRegistryLimits {
    /// Requires both limits to be non-zero so admission can always make progress.
    pub(crate) fn try_new(bytes: usize, entries: usize) -> Result<Self, SimHasherRegistryError> {
        let Some(bytes) = NonZeroUsize::new(bytes) else {
            return Err(SimHasherRegistryError::ZeroByteLimit);
        };
        let Some(entries) = NonZeroUsize::new(entries) else {
            return Err(SimHasherRegistryError::ZeroEntryLimit);
        };
        Ok(Self { bytes, entries })
    }

    /// Projects validated runtime configuration into registry admission limits.
    pub(crate) fn from_config(settings: crate::config::SimHasherCacheSettings) -> Self {
        Self::try_new(settings.bytes(), settings.entries())
            .expect("SimHasher cache configuration is already validated")
    }
}

impl Default for SimHasherRegistryLimits {
    fn default() -> Self {
        Self::from_config(crate::config::SimHasherCacheSettings::default())
    }
}

/// Owned bounded registry for deterministic SimHasher instances.
pub(crate) struct SimHasherRegistry {
    limits: SimHasherRegistryLimits,
    state: Mutex<RegistryState>,
    changed: Condvar,
}

impl std::fmt::Debug for SimHasherRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SimHasherRegistry")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
struct RegistryState {
    entries: HashMap<SimHashIdentity, RegistryEntry>,
    retained_bytes: usize,
    access_clock: u64,
}

enum RegistryEntry {
    Constructing {
        bytes: usize,
    },
    Ready {
        hasher: Arc<SimHasher>,
        bytes: usize,
        last_access: u64,
    },
    Failed {
        error: SimHasherRegistryError,
        last_access: u64,
    },
}

impl SimHasherRegistry {
    /// Creates an empty registry with exact byte and entry admission limits.
    pub(crate) fn new(limits: SimHasherRegistryLimits) -> Self {
        Self {
            limits,
            state: Mutex::new(RegistryState::default()),
            changed: Condvar::new(),
        }
    }

    /// Validates one dimension against checked projection arithmetic and cap.
    ///
    /// DDL calls this before it writes an empty physical index or descriptor,
    /// preventing a generation that can never construct its required hasher.
    pub(crate) fn validate_dimension(
        &self,
        dimension: usize,
    ) -> Result<(), SimHasherRegistryError> {
        if dimension == 0 {
            return Err(SimHasherRegistryError::ZeroDimension);
        }
        let candidate_bytes = SimHasher::allocation_bytes(dimension)?;
        if candidate_bytes > self.limits.bytes.get() {
            return Err(SimHasherRegistryError::CandidateExceedsByteLimit {
                candidate: candidate_bytes,
                limit: self.limits.bytes.get(),
            });
        }
        Ok(())
    }

    /// Returns one shared hasher, constructing it once for a concurrent miss.
    ///
    /// The candidate's complete allocation is checked and reserved before the
    /// mutex is released. Waiters for the same identity observe the published
    /// `Arc`; waiters blocked only by in-flight reservations retry after a
    /// constructor publishes or fails. A panic during construction clears the
    /// reservation before it resumes unwinding.
    pub(crate) fn get(
        &self,
        identity: SimHashIdentity,
    ) -> Result<Arc<SimHasher>, SimHasherRegistryError> {
        if identity.algorithm_version.get() != super::generation::CURRENT_SIMHASH_ALGORITHM_VERSION
        {
            return Err(SimHasherRegistryError::UnsupportedAlgorithmVersion(
                identity.algorithm_version.get(),
            ));
        }
        self.validate_dimension(identity.dimension().get())?;
        let candidate_bytes = SimHasher::allocation_bytes(identity.dimension().get())?;

        'retry: loop {
            let mut state = self
                .state
                .lock()
                .expect("SimHasher registry mutex poisoned");
            state.access_clock = state.access_clock.wrapping_add(1);
            let access = state.access_clock;
            if let Some(entry) = state.entries.get_mut(&identity) {
                match entry {
                    RegistryEntry::Ready {
                        hasher,
                        last_access,
                        ..
                    } => {
                        *last_access = access;
                        return Ok(Arc::clone(hasher));
                    }
                    RegistryEntry::Constructing { .. } => {
                        drop(
                            self.changed
                                .wait(state)
                                .expect("SimHasher registry mutex poisoned while waiting"),
                        );
                        continue;
                    }
                    RegistryEntry::Failed { error, last_access } => {
                        *last_access = access;
                        return Err(error.clone());
                    }
                }
            }

            while state.entries.len() >= self.limits.entries.get()
                || state.retained_bytes > self.limits.bytes.get() - candidate_bytes
            {
                let lru = state
                    .entries
                    .iter()
                    .filter_map(|(identity, entry)| match entry {
                        RegistryEntry::Ready {
                            bytes, last_access, ..
                        } => Some((*identity, *bytes, *last_access)),
                        RegistryEntry::Constructing { .. } => None,
                        RegistryEntry::Failed { last_access, .. } => {
                            Some((*identity, 0, *last_access))
                        }
                    })
                    .min_by_key(|(identity, _, last_access)| (*last_access, *identity));
                let Some((lru_identity, lru_bytes, _)) = lru else {
                    drop(
                        self.changed
                            .wait(state)
                            .expect("SimHasher registry mutex poisoned while capacity waits"),
                    );
                    continue 'retry;
                };
                let removed = state.entries.remove(&lru_identity);
                assert!(matches!(
                    removed,
                    Some(RegistryEntry::Ready { .. } | RegistryEntry::Failed { .. })
                ));
                state.retained_bytes -= lru_bytes;
            }

            state.entries.insert(
                identity,
                RegistryEntry::Constructing {
                    bytes: candidate_bytes,
                },
            );
            state.retained_bytes += candidate_bytes;
            drop(state);

            let construction = std::panic::catch_unwind(|| {
                SimHasher::try_new_with_seed(identity.dimension().get(), identity.seed())
                    .map(Arc::new)
            });
            let mut state = self
                .state
                .lock()
                .expect("SimHasher registry mutex poisoned");
            let reservation = state.entries.remove(&identity);
            assert!(matches!(
                reservation,
                Some(RegistryEntry::Constructing { bytes }) if bytes == candidate_bytes
            ));
            match construction {
                Ok(Ok(hasher)) => {
                    state.entries.insert(
                        identity,
                        RegistryEntry::Ready {
                            hasher: Arc::clone(&hasher),
                            bytes: candidate_bytes,
                            last_access: access,
                        },
                    );
                    self.changed.notify_all();
                    return Ok(hasher);
                }
                Ok(Err(error)) => {
                    state.retained_bytes -= candidate_bytes;
                    let shared_error = SimHasherRegistryError::from(error);
                    state.entries.insert(
                        identity,
                        RegistryEntry::Failed {
                            error: shared_error.clone(),
                            last_access: access,
                        },
                    );
                    self.changed.notify_all();
                    return Err(shared_error);
                }
                Err(payload) => {
                    state.retained_bytes -= candidate_bytes;
                    self.changed.notify_one();
                    drop(state);
                    std::panic::resume_unwind(payload);
                }
            }
        }
    }

    #[cfg(test)]
    fn retained_usage(&self) -> (usize, usize) {
        let state = self.state.lock().unwrap();
        (state.retained_bytes, state.entries.len())
    }
}

impl Default for SimHasherRegistry {
    fn default() -> Self {
        Self::new(SimHasherRegistryLimits::default())
    }
}

#[cfg(feature = "production-coverage")]
#[path = "../../../tests/production_support/vector/simhash_registry.rs"]
pub(crate) mod production_contracts;

/// Failure to validate limits, reserve a candidate, or construct projections.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum SimHasherRegistryError {
    /// Projection tables cannot represent a zero-dimensional vector space.
    #[error("SimHasher dimension must be non-zero")]
    ZeroDimension,
    /// The runtime has no constructor for this descriptor algorithm identity.
    #[error("unsupported SimHasher algorithm version {0}")]
    UnsupportedAlgorithmVersion(u16),
    /// A zero-byte registry could never admit a valid projection table.
    #[error("SimHasher registry byte limit must be non-zero")]
    ZeroByteLimit,
    /// A zero-entry registry could never admit a valid identity.
    #[error("SimHasher registry entry limit must be non-zero")]
    ZeroEntryLimit,
    /// One checked candidate cannot fit even after every ready entry is evicted.
    #[error("SimHasher candidate requires {candidate} bytes, exceeding registry limit {limit}")]
    CandidateExceedsByteLimit { candidate: usize, limit: usize },
    /// Projection construction rejected its dimension or allocation.
    #[error(transparent)]
    Construction(#[from] SimHasherConstructionError),
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::thread;

    use super::*;

    fn identity(dimension: usize, seed: u64) -> SimHashIdentity {
        SimHashIdentity::new(NonZeroUsize::new(dimension).unwrap(), seed, NonZeroU16::MIN)
    }

    #[test]
    fn exact_limits_evict_lru_and_recreation_is_deterministic() {
        let bytes = SimHasher::allocation_bytes(3).unwrap();
        let registry =
            SimHasherRegistry::new(SimHasherRegistryLimits::try_new(bytes * 2, 2).unwrap());
        let first = registry.get(identity(3, 42)).unwrap();
        let expected = first.hash_from_slice(&[1.0, 2.0, 3.0]).unwrap();
        registry.get(identity(3, 43)).unwrap();
        registry.get(identity(3, 44)).unwrap();
        assert_eq!(registry.retained_usage(), (bytes * 2, 2));

        let recreated = registry.get(identity(3, 42)).unwrap();
        assert!(!Arc::ptr_eq(&first, &recreated));
        assert_eq!(
            recreated.hash_from_slice(&[1.0, 2.0, 3.0]).unwrap(),
            expected
        );
        assert_eq!(expected.bits(), 0x6d91_a757_8862_6786);
    }

    #[test]
    fn rejects_oversized_candidate_before_construction() {
        let bytes = SimHasher::allocation_bytes(3).unwrap();
        let registry =
            SimHasherRegistry::new(SimHasherRegistryLimits::try_new(bytes - 1, 1).unwrap());
        assert!(matches!(
            registry.get(identity(3, 42)),
            Err(SimHasherRegistryError::CandidateExceedsByteLimit { .. })
        ));
        assert_eq!(registry.retained_usage(), (0, 0));
    }

    #[test]
    fn dimension_validation_accepts_exact_byte_boundary_and_rejects_next() {
        let bytes = SimHasher::allocation_bytes(3).unwrap();
        let registry = SimHasherRegistry::new(SimHasherRegistryLimits::try_new(bytes, 1).unwrap());
        assert!(registry.validate_dimension(3).is_ok());
        assert!(matches!(
            registry.validate_dimension(4),
            Err(SimHasherRegistryError::CandidateExceedsByteLimit { .. })
        ));
        assert_eq!(registry.retained_usage(), (0, 0));
    }

    #[test]
    fn unknown_algorithm_identity_fails_before_allocation() {
        let registry = SimHasherRegistry::default();
        let unknown_version = super::super::generation::CURRENT_SIMHASH_ALGORITHM_VERSION + 1;
        let unknown = SimHashIdentity::new(
            NonZeroUsize::MIN,
            42,
            NonZeroU16::new(unknown_version).unwrap(),
        );
        assert_eq!(
            registry.get(unknown).unwrap_err(),
            SimHasherRegistryError::UnsupportedAlgorithmVersion(unknown_version)
        );
        assert_eq!(registry.retained_usage(), (0, 0));
    }

    #[test]
    fn concurrent_miss_publishes_one_shared_instance() {
        let registry = Arc::new(SimHasherRegistry::default());
        let barrier = Arc::new(Barrier::new(8));
        let handles = (0..8)
            .map(|_| {
                let registry = Arc::clone(&registry);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    registry.get(identity(256, 42)).unwrap()
                })
            })
            .collect::<Vec<_>>();
        let hashers = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        assert!(hashers[1..]
            .iter()
            .all(|hasher| Arc::ptr_eq(&hashers[0], hasher)));
    }
}
