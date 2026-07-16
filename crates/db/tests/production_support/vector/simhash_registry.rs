//! Production contracts for the bounded deterministic SimHasher registry.
//!
//! This feature-gated child module exercises the real registry's typed limit,
//! identity, admission, eviction, recreation, and concurrent publication
//! boundaries. It creates only process-local projection tables; no key, value,
//! catalog, or physical vector representation is involved.

use std::sync::{Arc, Barrier};
use std::thread;

use super::*;

/// Constructs the current descriptor-bound identity for one projection table.
fn identity(dimension: usize, seed: u64) -> SimHashIdentity {
    SimHashIdentity::new(
        NonZeroUsize::new(dimension).unwrap(),
        seed,
        NonZeroU16::new(super::super::generation::CURRENT_SIMHASH_ALGORITHM_VERSION).unwrap(),
    )
}

/// Verifies non-zero limits and exact checked dimension admission.
fn run_limit_contracts() {
    assert!(matches!(
        SimHasherRegistryLimits::try_new(0, 1),
        Err(SimHasherRegistryError::ZeroByteLimit)
    ));
    assert!(matches!(
        SimHasherRegistryLimits::try_new(1, 0),
        Err(SimHasherRegistryError::ZeroEntryLimit)
    ));

    let bytes = SimHasher::allocation_bytes(3).unwrap();
    let registry = SimHasherRegistry::new(SimHasherRegistryLimits::try_new(bytes, 1).unwrap());
    assert!(registry.validate_dimension(3).is_ok());
    assert!(matches!(
        registry.validate_dimension(0),
        Err(SimHasherRegistryError::ZeroDimension)
    ));
    assert!(matches!(
        registry.validate_dimension(4),
        Err(SimHasherRegistryError::CandidateExceedsByteLimit {
            candidate,
            limit,
        }) if candidate > limit && limit == bytes
    ));
    assert!(matches!(
        registry.validate_dimension(usize::MAX),
        Err(SimHasherRegistryError::Construction(
            SimHasherConstructionError::AllocationSizeOverflow {
                dimension: usize::MAX
            }
        ))
    ));
    assert!(format!("{registry:?}").contains("SimHasherRegistry"));

    let oversized = SimHasherRegistry::new(SimHasherRegistryLimits::try_new(bytes - 1, 1).unwrap());
    assert!(matches!(
        oversized.get(identity(3, 42)),
        Err(SimHasherRegistryError::CandidateExceedsByteLimit { .. })
    ));
}

/// Verifies failed construction is shared and remains evictable under limits.
fn run_construction_failure_contracts() {
    let impossible_dimension = usize::MAX / (64 * core::mem::size_of::<f32>());
    let registry = SimHasherRegistry::new(SimHasherRegistryLimits::try_new(usize::MAX, 1).unwrap());
    let impossible = identity(impossible_dimension, 42);
    let first_error = registry.get(impossible).unwrap_err();
    assert!(matches!(
        first_error,
        SimHasherRegistryError::Construction(SimHasherConstructionError::AllocationFailed { .. })
    ));
    assert_eq!(registry.get(impossible).unwrap_err(), first_error);

    let replacement = registry.get(identity(1, 43)).unwrap();
    assert_eq!(replacement.dimension(), 1);
}

/// Verifies complete identity projection and unsupported algorithm rejection.
fn run_identity_contracts() {
    let current = identity(3, 42);
    assert_eq!(current.dimension().get(), 3);
    assert_eq!(current.seed(), 42);

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
}

/// Verifies ready-entry reuse, LRU eviction, and deterministic recreation.
fn run_eviction_contracts() {
    let bytes = SimHasher::allocation_bytes(3).unwrap();
    let registry = SimHasherRegistry::new(
        SimHasherRegistryLimits::try_new(bytes.checked_mul(2).unwrap(), 2).unwrap(),
    );
    let first = registry.get(identity(3, 42)).unwrap();
    let expected = first.hash_from_slice(&[1.0, 2.0, 3.0]).unwrap();
    let second = registry.get(identity(3, 43)).unwrap();
    let first_reused = registry.get(identity(3, 42)).unwrap();
    assert!(Arc::ptr_eq(&first, &first_reused));

    registry.get(identity(3, 44)).unwrap();
    let first_after_eviction = registry.get(identity(3, 42)).unwrap();
    assert!(Arc::ptr_eq(&first, &first_after_eviction));
    let second_recreated = registry.get(identity(3, 43)).unwrap();
    assert!(!Arc::ptr_eq(&second, &second_recreated));
    assert_eq!(
        second_recreated.hash_from_slice(&[1.0, 2.0, 3.0]).unwrap(),
        second.hash_from_slice(&[1.0, 2.0, 3.0]).unwrap()
    );
    assert_eq!(expected.bits(), 0x6d91_a757_8862_6786);
}

/// Verifies concurrent misses publish one shared deterministic instance.
fn run_single_flight_contract() {
    let registry = Arc::new(SimHasherRegistry::default());
    let barrier = Arc::new(Barrier::new(8));
    let handles = (0..8)
        .map(|_| {
            let registry = Arc::clone(&registry);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                registry.get(identity(4096, 42)).unwrap()
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

/// Verifies a distinct identity waits while the only slot is constructing.
///
/// The feature-gated child installs the registry's valid reserved state
/// directly, then waits for the requesting thread to enter the production
/// capacity loop before publishing the reservation's completion. This avoids
/// using constructor timing as a concurrency oracle.
fn run_capacity_wait_contract() {
    let candidate_bytes = SimHasher::allocation_bytes(3).unwrap();
    let registry = Arc::new(SimHasherRegistry::new(
        SimHasherRegistryLimits::try_new(candidate_bytes, 1).unwrap(),
    ));
    let constructing = identity(3, 41);
    {
        let mut state = registry.state.lock().unwrap();
        state.entries.insert(
            constructing,
            RegistryEntry::Constructing {
                bytes: candidate_bytes,
            },
        );
        state.retained_bytes = candidate_bytes;
    }

    let waiting_registry = Arc::clone(&registry);
    let waiter = thread::spawn(move || waiting_registry.get(identity(3, 42)).unwrap());
    loop {
        let access_started = registry.state.lock().unwrap().access_clock > 0;
        if access_started {
            break;
        }
        thread::yield_now();
    }

    {
        let mut state = registry.state.lock().unwrap();
        let removed = state.entries.remove(&constructing);
        assert!(matches!(removed, Some(RegistryEntry::Constructing { .. })));
        state.retained_bytes = 0;
    }
    registry.changed.notify_all();
    assert_eq!(waiter.join().unwrap().dimension(), 3);
}

/// Exercises every reachable registry identity, limit, eviction, and reuse contract.
pub(crate) fn run() {
    run_limit_contracts();
    run_identity_contracts();
    run_eviction_contracts();
    run_construction_failure_contracts();
    run_single_flight_contract();
    run_capacity_wait_contract();
}
