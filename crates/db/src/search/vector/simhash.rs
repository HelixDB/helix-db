//! SimHash cache for LSM-Vec HNSW implementation
//!
//! SimHash values are persisted as dedicated vector-hot key rows:
//! `[0xF0][index_id:8][kind=0x12][node_id:8]`.
//! This keeps lookup and refresh paths independent from layer-0 edge rows.
use slatedb::DbTransaction;
use std::num::{NonZeroU16, NonZeroUsize};
use std::sync::{Arc, OnceLock};

use crate::encoding::keys::{scope::DataScope, DataKey, DataKeyKind};
use crate::encoding::v2::keys::indexes::vector::{VectorKey, VectorSimHashKey};
use crate::encoding::v2::values::indexes::vector::simhash::{decode_simhash, encode_simhash};
use crate::encoding::NodeId;
use crate::error::HelixDbError;
use crate::search::vector::generation::{CURRENT_SIMHASH_ALGORITHM_VERSION, CURRENT_SIMHASH_SEED};
use crate::search::vector::write_transaction::MeasuredVectorTransaction;

use super::{
    simhash_registry::{SimHashIdentity, SimHasherRegistry, SimHasherRegistryError},
    unaligned_vector::UnalignedVectorCodec,
    ValidatedMetricVector,
};

// Re-export core SimHash types from unaligned_vector
pub use super::unaligned_vector::{SimHash, SimHashError, SimHasher};

/// Number of bits in a SimHash code
pub const SIMHASH_BITS: usize = 64;

/// Frozen deployed default for the 64-bit SimHash collision threshold.
///
/// New configuration uses the exact value previously produced by the retired
/// heuristic for 64 bits and a 5% failure tolerance. Freezing it prevents a
/// floating-point formula change from silently changing persisted defaults.
pub const DEFAULT_SIMHASH_COLLISION_THRESHOLD: usize = 43;

/// Derive a locality ordering code from a 64-bit SimHash.
///
/// The mapping uses 4 bands of 16 bits and interleaves bit-planes
/// high-to-low: `b0[15], b1[15], b2[15], b3[15], b0[14], ...`.
///
/// The returned value is encoded with `to_be_bytes()` in ordered keys.
#[inline]
pub(crate) fn order_code_from_simhash_bits(bits: u64) -> u64 {
    let b0 = ((bits >> 48) & 0xFFFF) as u16;
    let b1 = ((bits >> 32) & 0xFFFF) as u16;
    let b2 = ((bits >> 16) & 0xFFFF) as u16;
    let b3 = (bits & 0xFFFF) as u16;

    let mut code = 0u64;
    for bit in (0..16).rev() {
        code = (code << 1) | (((b0 >> bit) & 1) as u64);
        code = (code << 1) | (((b1 >> bit) & 1) as u64);
        code = (code << 1) | (((b2 >> bit) & 1) as u64);
        code = (code << 1) | (((b3 >> bit) & 1) as u64);
    }

    code
}

/// Transactional cache for dedicated SimHash rows.
///
/// SimHash codes are stored in vector-hot keyspace (`0xF0`) which provides:
/// - Independent key-range scanning for fast in-memory refresh
/// - Transaction isolation (uncommitted SimHash only visible to owning transaction)
/// - Automatic rollback on transaction abort
/// - Small memory footprint (8 bytes per vector)
/// - Full ACID guarantees (persisted via WAL)
///
/// While persisted, SimHash codes can be safely recomputed from vectors on demand.
pub struct SimHashCache {
    /// Stable index id for key namespacing
    index_id: u64,
    /// Tenant namespace used for physical SimHash keys.
    tenant_scope: DataScope,
    /// SimHasher for computing hashes
    simhasher: Arc<SimHasher>,
}

/// Resolves the current descriptor-bound identity through one bounded owner.
fn shared_simhasher(
    registry: &SimHasherRegistry,
    identity: SimHashIdentity,
) -> Result<Arc<SimHasher>, SimHasherRegistryError> {
    registry.get(identity)
}

/// Creates the deployed compatibility identity for legacy vector metadata.
fn current_simhash_identity(dimension: usize) -> Result<SimHashIdentity, SimHasherRegistryError> {
    let Some(dimension) = NonZeroUsize::new(dimension) else {
        return Err(SimHasherRegistryError::ZeroDimension);
    };
    let algorithm_version = NonZeroU16::new(CURRENT_SIMHASH_ALGORITHM_VERSION)
        .expect("current SimHash algorithm version is non-zero");
    Ok(SimHashIdentity::new(
        dimension,
        CURRENT_SIMHASH_SEED,
        algorithm_version,
    ))
}

impl SimHashCache {
    /// Create a new SimHash cache
    ///
    /// # Arguments
    /// * `index_id` - Stable index id (for key namespacing)
    /// * `dimension` - Dimension of vectors (for SimHasher initialization)
    pub fn new(index_id: u64, dimension: usize) -> Self {
        Self::try_new(index_id, dimension)
            .expect("SimHash cache dimension must fit the configured registry")
    }

    /// Creates a legacy-unscoped cache with bounded hasher admission.
    pub(crate) fn try_new(index_id: u64, dimension: usize) -> Result<Self, SimHasherRegistryError> {
        Self::try_new_scoped(index_id, dimension, DataScope::LegacyUnscoped)
    }

    /// Creates a scoped cache after reserving its descriptor-bound projection table.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) fn new_scoped(index_id: u64, dimension: usize, tenant_scope: DataScope) -> Self {
        Self::try_new_scoped(index_id, dimension, tenant_scope)
            .expect("SimHash cache dimension must fit the configured registry")
    }

    /// Fallible scoped constructor used by production vector operations.
    pub(crate) fn try_new_scoped(
        index_id: u64,
        dimension: usize,
        tenant_scope: DataScope,
    ) -> Result<Self, SimHasherRegistryError> {
        static COMPATIBILITY_REGISTRY: OnceLock<SimHasherRegistry> = OnceLock::new();
        Self::try_new_scoped_in(
            index_id,
            dimension,
            tenant_scope,
            COMPATIBILITY_REGISTRY.get_or_init(SimHasherRegistry::default),
        )
    }

    /// Constructs a cache through the registry owned by the calling runtime.
    pub(crate) fn try_new_scoped_in(
        index_id: u64,
        dimension: usize,
        tenant_scope: DataScope,
        registry: &SimHasherRegistry,
    ) -> Result<Self, SimHasherRegistryError> {
        Self::try_new_scoped_with_identity(
            index_id,
            tenant_scope,
            current_simhash_identity(dimension)?,
            registry,
        )
    }

    /// Constructs a cache from the exact identity proven by a managed descriptor.
    pub(crate) fn try_new_scoped_with_identity(
        index_id: u64,
        tenant_scope: DataScope,
        identity: SimHashIdentity,
        registry: &SimHasherRegistry,
    ) -> Result<Self, SimHasherRegistryError> {
        Ok(Self {
            index_id,
            tenant_scope,
            simhasher: shared_simhasher(registry, identity)?,
        })
    }

    /// Generate key for dedicated SimHash row.
    ///
    /// Format: `[0xF0][index_id:8][kind:simhash][node_id:8]`
    fn make_simhash_key(&self, node_id: NodeId) -> bytes::Bytes {
        DataKey::Data {
            scope: self.tenant_scope,
            kind: DataKeyKind::Vector(VectorKey::SimHash(VectorSimHashKey::new(
                self.index_id,
                node_id,
            ))),
        }
        .to_bytes()
    }

    /// Get SimHash and return read count.
    pub async fn get_counted(
        &self,
        txn: &DbTransaction,
        node_id: NodeId,
    ) -> Result<(Option<SimHash>, usize), HelixDbError> {
        let key = self.make_simhash_key(node_id);

        match txn.get(&key).await? {
            Some(bytes) => {
                let simhash = SimHash::from_bits(decode_simhash(&bytes)?);
                Ok((Some(simhash), 1))
            }
            None => Ok((None, 1)),
        }
    }

    /// Get SimHash from dedicated key.
    ///
    /// Returns None if the SimHash hasn't been computed yet for this node.
    pub async fn get(
        &self,
        txn: &DbTransaction,
        node_id: NodeId,
    ) -> Result<Option<SimHash>, HelixDbError> {
        let (simhash, _) = self.get_counted(txn, node_id).await?;
        Ok(simhash)
    }

    /// Set SimHash in dedicated key.
    ///
    /// This operation is transaction-aware with full ACID guarantees:
    /// - Will be rolled back if the transaction aborts
    /// - Protected by SSI conflict detection
    /// - Durable after commit (persisted via WAL)
    pub fn set(
        &self,
        txn: &DbTransaction,
        node_id: NodeId,
        simhash: SimHash,
    ) -> Result<(), HelixDbError> {
        let key = self.make_simhash_key(node_id);
        txn.put(key, encode_simhash(simhash.bits()))?;
        Ok(())
    }

    /// Compute SimHash from vector and cache it
    ///
    /// This is the primary method used during vector insertion.
    /// Computes the SimHash and stores it in transactional memory.
    pub fn compute_and_cache(
        &self,
        txn: &DbTransaction,
        node_id: NodeId,
        vector: &[f32],
    ) -> Result<SimHash, HelixDbError> {
        let simhash = self.simhasher.hash_from_slice(vector).map_err(|error| {
            let SimHashError::DimensionMismatch { expected, actual } = error else {
                return HelixDbError::InvariantViolation(error.to_string());
            };
            HelixDbError::InvalidDimension {
                expected,
                got: actual,
            }
        })?;
        self.set(txn, node_id, simhash)?;
        Ok(simhash)
    }

    /// Computes and stages SimHash through the measured vector-write boundary.
    ///
    /// HNSW insertion uses this variant so the dedicated current-format SimHash
    /// row is included in the exact final transaction capacity calculation.
    pub(crate) fn compute_and_cache_measured<C>(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_id: NodeId,
        vector: &ValidatedMetricVector<'_, C>,
    ) -> Result<SimHash, HelixDbError>
    where
        C: UnalignedVectorCodec,
    {
        let simhash = C::compute_simhash(vector.values(), &self.simhasher).map_err(|error| {
            let SimHashError::DimensionMismatch { expected, actual } = error else {
                return HelixDbError::InvariantViolation(error.to_string());
            };
            HelixDbError::InvalidDimension {
                expected,
                got: actual,
            }
        })?;
        let key = self.make_simhash_key(node_id);
        txn.put(key, encode_simhash(simhash.bits()))?;
        Ok(simhash)
    }

    /// Get the SimHasher (for direct use by codecs)
    pub fn simhasher(&self) -> &SimHasher {
        &self.simhasher
    }

    /// Delete SimHash for a node.
    pub fn delete(&self, txn: &DbTransaction, node_id: NodeId) -> Result<(), HelixDbError> {
        let key = self.make_simhash_key(node_id);
        txn.delete(key)?;
        Ok(())
    }
}

#[cfg(feature = "production-coverage")]
#[path = "../../../tests/production_support/vector/simhash.rs"]
pub(crate) mod production_contracts;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::keys::scope::{DataScope, TenantId};
    use crate::encoding::v2::keys::indexes::vector::{VectorKey, VectorSimHashKey};
    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, IsolationLevel};
    use std::sync::Arc;

    async fn test_db(name: &str) -> Arc<Db> {
        Arc::new(
            Db::open(name, Arc::new(InMemory::new()))
                .await
                .expect("test db should open"),
        )
    }

    #[test]
    fn test_order_code_from_simhash_bits_basic_extremes() {
        assert_eq!(order_code_from_simhash_bits(0), 0);
        assert_eq!(order_code_from_simhash_bits(u64::MAX), u64::MAX);
    }

    #[test]
    fn test_order_code_from_simhash_bits_interleaves_band_msb() {
        let only_b0_msb = 1u64 << 63;
        let only_b1_msb = 1u64 << 47;
        let only_b2_msb = 1u64 << 31;
        let only_b3_msb = 1u64 << 15;

        assert_eq!(order_code_from_simhash_bits(only_b0_msb), 1u64 << 63);
        assert_eq!(order_code_from_simhash_bits(only_b1_msb), 1u64 << 62);
        assert_eq!(order_code_from_simhash_bits(only_b2_msb), 1u64 << 61);
        assert_eq!(order_code_from_simhash_bits(only_b3_msb), 1u64 << 60);
    }

    #[test]
    fn simhash_cache_reuses_shared_hasher_per_dimension() {
        let first = SimHashCache::new(1, 3);
        let second = SimHashCache::new(2, 3);
        let third = SimHashCache::new(3, 4);
        assert!(Arc::ptr_eq(&first.simhasher, &second.simhasher));
        assert!(!Arc::ptr_eq(&first.simhasher, &third.simhasher));
        assert!(std::ptr::eq(first.simhasher(), first.simhasher.as_ref()));
    }

    #[test]
    fn simhash_cache_builds_legacy_and_tenant_scoped_keys() {
        let legacy = SimHashCache::new(7, 3);
        let legacy_key = legacy.make_simhash_key(42);
        assert_eq!(
            VectorKey::parse_from_slice(&legacy_key).expect("legacy vector key"),
            VectorKey::SimHash(VectorSimHashKey::new(7, 42))
        );

        let tenant = TenantId::from_ulid_str("0000000000000000000000000A").expect("valid tenant");
        let scoped = SimHashCache::new_scoped(7, 3, DataScope::Tenant(tenant));
        let scoped_key = scoped.make_simhash_key(42);
        assert_eq!(
            DataScope::Tenant(tenant).strip_key(&scoped_key),
            Some(legacy_key.as_ref())
        );
        assert_ne!(scoped_key.as_ref(), legacy_key.as_ref());
    }

    #[tokio::test]
    async fn simhash_cache_computes_reads_and_deletes_transactional_rows() {
        let db = test_db("simhash_cache_lifecycle").await;
        let txn = db
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("transaction should open");
        let cache = SimHashCache::new(7, 3);
        let vector = [0.25, -0.5, 1.0];

        assert_eq!(cache.get_counted(&txn, 42).await.unwrap(), (None, 1));

        let expected = cache.simhasher().hash_from_slice(&vector).unwrap();
        assert_eq!(
            cache.compute_and_cache(&txn, 42, &vector).unwrap(),
            expected
        );
        assert_eq!(
            cache.get_counted(&txn, 42).await.unwrap(),
            (Some(expected), 1)
        );
        assert_eq!(cache.get(&txn, 42).await.unwrap(), Some(expected));
        cache.delete(&txn, 42).unwrap();
        assert_eq!(cache.get(&txn, 42).await.unwrap(), None);
    }

    #[tokio::test]
    async fn simhash_cache_rejects_malformed_persisted_values() {
        let db = test_db("simhash_cache_malformed_value").await;
        let txn = db
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("transaction should open");
        let cache = SimHashCache::new(7, 3);

        txn.put(
            cache.make_simhash_key(42),
            bytes::Bytes::from_static(&[0; 7]),
        )
        .expect("malformed test row should stage");

        assert!(matches!(
            cache.get_counted(&txn, 42).await,
            Err(HelixDbError::Encoding(
                crate::encoding::error::EncodingError::BufferTooShort {
                    expected: 8,
                    actual: 7,
                }
            ))
        ));
    }
}
