//! Production contracts for transactional SimHash rows.
//!
//! This feature-gated child module exercises compatibility and scoped cache
//! construction, descriptor-bound registry admission, current f32 hashing,
//! measured writes, typed reads, corruption rejection, and deletion. It uses
//! the deployed dedicated SimHash key and value codecs without adding a format.

use std::num::{NonZeroU16, NonZeroUsize};
use std::sync::Arc;

use bytes::Bytes;
use slatedb::object_store::memory::InMemory;
use slatedb::{Db, IsolationLevel};

use super::*;
use crate::encoding::keys::scope::{DataScope, TenantId};
use crate::search::vector::{SimHasherRegistryLimits, VectorDimension, VectorDistanceMetric};

/// Verifies every fallible cache-construction and descriptor-identity boundary.
fn run_constructor_contracts() {
    assert!(matches!(
        SimHashCache::try_new(1, 0),
        Err(SimHasherRegistryError::ZeroDimension)
    ));
    let legacy = SimHashCache::new(1, 3);
    assert!(legacy.simhasher().hash_from_slice(&[1.0, 2.0, 3.0]).is_ok());

    let tenant = TenantId::from_u128(7);
    let scoped = SimHashCache::new_scoped(2, 3, DataScope::Tenant(tenant));
    assert!(scoped.simhasher().hash_from_slice(&[1.0, 2.0, 3.0]).is_ok());
    assert!(SimHashCache::try_new_scoped(3, 3, DataScope::Tenant(tenant)).is_ok());

    let bytes = SimHasher::allocation_bytes(3).unwrap();
    let constrained = SimHasherRegistry::new(
        SimHasherRegistryLimits::try_new(bytes.saturating_sub(1), 1).unwrap(),
    );
    assert!(matches!(
        SimHashCache::try_new_scoped_in(4, 3, DataScope::LegacyUnscoped, &constrained),
        Err(SimHasherRegistryError::CandidateExceedsByteLimit { .. })
    ));

    let unknown_version = CURRENT_SIMHASH_ALGORITHM_VERSION + 1;
    let unknown = SimHashIdentity::new(
        NonZeroUsize::new(3).unwrap(),
        CURRENT_SIMHASH_SEED,
        NonZeroU16::new(unknown_version).unwrap(),
    );
    assert!(matches!(
        SimHashCache::try_new_scoped_with_identity(
            5,
            DataScope::LegacyUnscoped,
            unknown,
            &SimHasherRegistry::default(),
        ),
        Err(SimHasherRegistryError::UnsupportedAlgorithmVersion(version))
            if version == unknown_version
    ));

    let degenerate = SimHasher::try_new_with_projection_source(3, || 0.0).unwrap();
    assert!(degenerate
        .hyperplanes()
        .iter()
        .all(|component| *component == 0.0));
    assert_eq!(
        degenerate.hash_from_slice(&[1.0, 2.0, 3.0]).unwrap(),
        SimHash::from_bits(0)
    );
}

/// Verifies missing, present, corrupt, measured, and deleted row transitions.
async fn run_transaction_contracts() {
    let db = Db::open(
        "production-vector-simhash-contracts",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let cache = SimHashCache::new(11, 3);

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert_eq!(cache.get_counted(&txn, 1).await.unwrap(), (None, 1));
    assert_eq!(cache.get(&txn, 1).await.unwrap(), None);
    let explicit = SimHash::from_bits(0x1234);
    cache.set(&txn, 1, explicit).unwrap();
    assert_eq!(
        cache.get_counted(&txn, 1).await.unwrap(),
        (Some(explicit), 1)
    );

    let computed = cache.compute_and_cache(&txn, 2, &[1.0, 2.0, 3.0]).unwrap();
    assert_eq!(cache.get(&txn, 2).await.unwrap(), Some(computed));
    assert!(matches!(
        cache.compute_and_cache(&txn, 3, &[1.0, 2.0]),
        Err(HelixDbError::InvalidDimension {
            expected: 3,
            got: 2,
        })
    ));
    cache.delete(&txn, 1).unwrap();
    assert_eq!(cache.get(&txn, 1).await.unwrap(), None);
    txn.commit().await.unwrap();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let values = [3.0, 2.0, 1.0];
    let validated = ValidatedMetricVector::try_from_slice(
        &values,
        VectorDistanceMetric::Cosine,
        VectorDimension::try_new(values.len()).unwrap(),
    )
    .unwrap();
    measured.fail_next_write();
    assert!(cache
        .compute_and_cache_measured(&measured, 4, &validated)
        .is_err());
    let measured_hash = cache
        .compute_and_cache_measured(&measured, 4, &validated)
        .unwrap();
    assert_eq!(cache.get(&txn, 4).await.unwrap(), Some(measured_hash));
    let short_values = [3.0, 2.0];
    let validated_short = ValidatedMetricVector::try_from_slice(
        &short_values,
        VectorDistanceMetric::Cosine,
        VectorDimension::try_new(short_values.len()).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        cache.compute_and_cache_measured(&measured, 5, &validated_short),
        Err(HelixDbError::InvalidDimension {
            expected: 3,
            got: 2,
        })
    ));
    txn.commit().await.unwrap();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    txn.put(cache.make_simhash_key(6), Bytes::from_static(b"corrupt"))
        .unwrap();
    assert!(matches!(
        cache.get(&txn, 6).await,
        Err(HelixDbError::Encoding(_))
    ));
    txn.rollback();
}

/// Exercises constructors and every transactional current-row state.
pub(crate) async fn run() {
    run_constructor_contracts();
    run_transaction_contracts().await;
}
