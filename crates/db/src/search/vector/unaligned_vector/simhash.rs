//! SimHash projection and locality-sensitive vector filtering.
//!
//! [`SimHasher`] deterministically derives 64 normalized `f32` hyperplanes from
//! a dimension and seed. Vector search uses those projections to reject likely
//! dissimilar candidates before loading full vectors. Construction exposes a
//! checked internal boundary so the bounded registry can reject overflow and
//! capacity failures before a projection table becomes visible.
use std::sync::Arc;

/// 64-bit SimHash code for locality-sensitive hashing
///
/// SimHash uses random hyperplanes to project high-dimensional vectors into
/// a 64-bit binary code. Vectors that are similar in the original space
/// will have similar (high collision count) SimHash codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SimHash {
    bits: u64,
}

impl SimHash {
    /// Create a SimHash from a precomputed 64-bit value
    #[inline]
    pub fn from_bits(bits: u64) -> Self {
        Self { bits }
    }

    /// Get the raw 64-bit representation
    #[inline]
    pub fn bits(&self) -> u64 {
        self.bits
    }

    /// Compute the number of matching bits (collision count) with another SimHash
    ///
    /// Higher collision count indicates higher similarity between vectors.
    #[inline]
    pub fn collision_count(&self, other: &SimHash) -> u32 {
        // Count matching bits (where XOR is 0)
        64 - (self.bits ^ other.bits).count_ones()
    }

    /// Compute Hamming distance (number of differing bits)
    #[inline]
    pub fn hamming_distance(&self, other: &SimHash) -> u32 {
        (self.bits ^ other.bits).count_ones()
    }

    /// Check if this SimHash passes a similarity threshold
    ///
    /// Returns true if the collision count meets or exceeds the threshold.
    /// Higher threshold = stricter filtering = fewer false positives
    #[inline]
    pub fn passes_threshold(&self, query: &SimHash, threshold: usize) -> bool {
        self.collision_count(query) >= threshold as u32
    }

    /// Serialize to bytes (little-endian u64)
    #[inline]
    pub fn to_bytes(&self) -> [u8; 8] {
        self.bits.to_le_bytes()
    }

    /// Deserialize from bytes (little-endian u64)
    #[inline]
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, SimHashError> {
        if bytes.len() != 8 {
            return Err(SimHashError::InvalidLength {
                expected: 8,
                actual: bytes.len(),
            });
        }
        let bits = u64::from_le_bytes(bytes.try_into().unwrap());
        Ok(Self { bits })
    }
}

/// SimHasher maintains random hyperplanes for consistent hashing
///
/// The SimHasher generates 64 random hyperplanes in the vector space and uses them
/// to compute SimHash codes. All vectors must be hashed with the same SimHasher
/// instance (or instances with identical hyperplanes) for meaningful comparisons.
#[derive(Debug, Clone)]
pub struct SimHasher {
    /// 64 random hyperplanes, each of dimension D
    /// Stored as a flat vector for cache efficiency: [plane0_dim0, plane0_dim1, ..., plane1_dim0, ...]
    hyperplanes: Arc<Vec<f32>>,
    /// Dimension of the vectors being hashed
    dimension: usize,
}

impl SimHasher {
    /// Create a new SimHasher with random hyperplanes
    ///
    /// Uses a fixed seed for reproducibility - all instances will generate
    /// the same hyperplanes for a given dimension.
    pub fn new(dimension: usize) -> Self {
        Self::new_with_seed(dimension, 42)
    }

    /// Create a new SimHasher with a specific seed
    ///
    /// Useful for testing or when you need consistent hashing across restarts.
    pub fn new_with_seed(dimension: usize, seed: u64) -> Self {
        Self::try_new_with_seed(dimension, seed)
            .expect("SimHasher dimension must have a representable allocation")
    }

    /// Returns the exact heap bytes required by all 64 `f32` hyperplanes.
    ///
    /// Bounded registries call this before construction so integer overflow and
    /// over-budget candidates are rejected without attempting an allocation.
    pub(crate) fn allocation_bytes(dimension: usize) -> Result<usize, SimHasherConstructionError> {
        64usize
            .checked_mul(dimension)
            .and_then(|elements| elements.checked_mul(core::mem::size_of::<f32>()))
            .ok_or(SimHasherConstructionError::AllocationSizeOverflow { dimension })
    }

    /// Constructs deterministic projections after checked allocation sizing.
    ///
    /// The flat allocation is filled and normalized in place, avoiding a
    /// second dimension-sized temporary allocation for each hyperplane.
    pub(crate) fn try_new_with_seed(
        dimension: usize,
        seed: u64,
    ) -> Result<Self, SimHasherConstructionError> {
        use rand::RngExt;
        use rand::SeedableRng;

        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        Self::try_new_with_projection_source(dimension, || {
            let value: f32 = rng.random();
            value * 2.0 - 1.0
        })
    }

    /// Builds and normalizes all projections from an injected component source.
    ///
    /// The seeded constructor delegates here so allocation and normalization
    /// remain one contract. Production supplies the existing seeded `f32`
    /// transformation; deterministic coverage can supply degenerate components
    /// without changing the versioned projection algorithm or persisted rows.
    pub(crate) fn try_new_with_projection_source(
        dimension: usize,
        mut next_component: impl FnMut() -> f32,
    ) -> Result<Self, SimHasherConstructionError> {
        let allocation_bytes = Self::allocation_bytes(dimension)?;
        let element_count = allocation_bytes / core::mem::size_of::<f32>();

        // Generate 64 random hyperplanes
        // Each hyperplane is a random unit vector in D-dimensional space
        let mut hyperplanes = Vec::new();
        hyperplanes
            .try_reserve_exact(element_count)
            .map_err(|_| SimHasherConstructionError::AllocationFailed { allocation_bytes })?;

        for _ in 0..64 {
            let plane_start = hyperplanes.len();
            hyperplanes.extend((0..dimension).map(|_| next_component()));
            let plane = &mut hyperplanes[plane_start..plane_start + dimension];

            // Normalize to unit vector for better numerical stability
            let norm: f32 = plane.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 1e-10 {
                for x in plane {
                    *x /= norm;
                }
            }
        }

        Ok(Self {
            hyperplanes: Arc::new(hyperplanes),
            dimension,
        })
    }

    /// Get the dimension of vectors this hasher expects
    #[inline]
    pub fn dimension(&self) -> usize {
        self.dimension
    }

    /// Computes a SimHash from an exact-size iterator of `f32` values.
    ///
    /// This is the core hashing function. For each of the 64 hyperplanes,
    /// it computes the dot product with the vector and sets the corresponding
    /// bit to 1 if positive, 0 if negative. Prefer [`Self::hash_from_slice`]
    /// when the caller already owns a contiguous vector to avoid collecting.
    ///
    /// # Errors
    ///
    /// Returns [`SimHashError::DimensionMismatch`] when the iterator length does
    /// not match the descriptor-bound dimension of this hasher.
    pub fn hash_from_iter<I>(&self, vector: I) -> Result<SimHash, SimHashError>
    where
        I: IntoIterator<Item = f32>,
        I::IntoIter: ExactSizeIterator,
    {
        let iter = vector.into_iter();
        if iter.len() != self.dimension {
            return Err(SimHashError::DimensionMismatch {
                expected: self.dimension,
                actual: iter.len(),
            });
        }

        let vector_slice: Vec<f32> = iter.collect();
        self.hash_from_slice(&vector_slice)
    }

    /// Computes a SimHash from a contiguous slice of `f32` values.
    ///
    /// This is more efficient than [`Self::hash_from_iter`] when the vector is
    /// already contiguous because it avoids an intermediate allocation.
    ///
    /// # Errors
    ///
    /// Returns [`SimHashError::DimensionMismatch`] when `vector` does not match
    /// the descriptor-bound dimension of this hasher.
    pub fn hash_from_slice(&self, vector: &[f32]) -> Result<SimHash, SimHashError> {
        if vector.len() != self.dimension {
            return Err(SimHashError::DimensionMismatch {
                expected: self.dimension,
                actual: vector.len(),
            });
        }

        let mut bits = 0u64;

        // For each of the 64 hyperplanes
        for plane_idx in 0..64 {
            // Compute dot product with the hyperplane
            let mut dot_product = 0.0f32;
            let plane_offset = plane_idx * self.dimension;

            for (dim_idx, &value) in vector.iter().enumerate() {
                dot_product += value * self.hyperplanes[plane_offset + dim_idx];
            }

            // Set bit i to 1 if dot product is positive
            if dot_product > 0.0 {
                bits |= 1u64 << plane_idx;
            }
        }

        Ok(SimHash { bits })
    }

    /// Get a reference to the underlying hyperplanes (for advanced use cases)
    pub fn hyperplanes(&self) -> &[f32] {
        &self.hyperplanes
    }
}

/// Failure to size or allocate deterministic SimHash projections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum SimHasherConstructionError {
    /// `64 * dimension * size_of::<f32>()` overflowed `usize`.
    #[error("SimHasher allocation size overflows usize for dimension {dimension}")]
    AllocationSizeOverflow { dimension: usize },
    /// The allocator rejected the already checked projection-table capacity.
    #[error("failed to reserve {allocation_bytes} bytes for SimHasher projections")]
    AllocationFailed { allocation_bytes: usize },
}

/// Errors that can occur during SimHash operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SimHashError {
    /// A persisted SimHash row did not contain one little-endian `u64`.
    #[error("Invalid SimHash byte length: expected {expected}, got {actual}")]
    InvalidLength { expected: usize, actual: usize },

    /// The caller supplied a vector incompatible with this hasher's identity.
    #[error("Vector dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simhash_collision_count() {
        // Test identical hashes
        let hash1 = SimHash::from_bits(0xAAAA_AAAA_AAAA_AAAAu64);
        let hash2 = SimHash::from_bits(0xAAAA_AAAA_AAAA_AAAAu64);
        assert_eq!(hash1.collision_count(&hash2), 64);

        // Test completely opposite hashes
        let hash3 = SimHash::from_bits(0x5555_5555_5555_5555u64); // Inverted pattern
        assert_eq!(hash1.collision_count(&hash3), 0);

        // Test partially matching
        let hash4 = SimHash::from_bits(0xAAAA_AAAA_0000_0000u64); // Upper 32 match, lower 32 differ
        let collision_count = hash1.collision_count(&hash4);
        // Upper 32 bits match completely (32 matches)
        // Lower 32 bits: 0xAAAA_AAAA has 16 bits set, so 16 differ + 16 match
        // Total: 32 + 16 = 48 matches
        assert_eq!(collision_count, 48);
    }

    #[test]
    fn test_simhash_hamming_distance() {
        // Test identical hashes
        let hash1 = SimHash::from_bits(0xFFFF_FFFF_FFFF_FFFFu64);
        let hash2 = SimHash::from_bits(0xFFFF_FFFF_FFFF_FFFFu64);
        assert_eq!(hash1.hamming_distance(&hash2), 0);

        // Test completely opposite hashes
        let hash3 = SimHash::from_bits(0x0000_0000_0000_0000u64);
        assert_eq!(hash1.hamming_distance(&hash3), 64);

        // Test one bit different
        let hash4 = SimHash::from_bits(0xFFFF_FFFF_FFFF_FFFEu64);
        assert_eq!(hash1.hamming_distance(&hash4), 1);
    }

    #[test]
    fn test_simhash_threshold() {
        // All bits set
        let hash1 = SimHash::from_bits(0xFFFF_FFFF_FFFF_FFFFu64);
        // Lower 4 bits cleared (60 bits match)
        let hash2 = SimHash::from_bits(0xFFFF_FFFF_FFFF_FFF0u64);

        // 60 bits match, should pass threshold of 60
        assert!(hash1.passes_threshold(&hash2, 60));
        // Should not pass threshold of 61
        assert!(!hash1.passes_threshold(&hash2, 61));

        // Test with lower threshold
        assert!(hash1.passes_threshold(&hash2, 50));
    }

    #[test]
    fn test_simhash_serialization() {
        let hash = SimHash::from_bits(0x0123456789ABCDEFu64);
        let bytes = hash.to_bytes();
        let decoded = SimHash::from_bytes(&bytes).unwrap();
        assert_eq!(hash, decoded);
    }

    #[test]
    fn test_simhasher_reproducibility() {
        let hasher1 = SimHasher::new_with_seed(128, 42);
        let hasher2 = SimHasher::new_with_seed(128, 42);

        let vector = vec![1.0; 128];
        let hash1 = hasher1.hash_from_slice(&vector).unwrap();
        let hash2 = hasher2.hash_from_slice(&vector).unwrap();

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn simhasher_allocation_size_rejects_overflow_before_allocation() {
        assert!(matches!(
            SimHasher::allocation_bytes(usize::MAX),
            Err(SimHasherConstructionError::AllocationSizeOverflow {
                dimension: usize::MAX
            })
        ));
    }

    #[test]
    fn test_simhasher_similar_vectors() {
        let hasher = SimHasher::new(128);

        let vector1 = vec![1.0; 128];
        let mut vector2 = vec![1.0; 128];
        // Change 10% of components
        for value in vector2.iter_mut().take(13) {
            *value = -1.0;
        }

        let hash1 = hasher.hash_from_slice(&vector1).unwrap();
        let hash2 = hasher.hash_from_slice(&vector2).unwrap();

        // Similar vectors should have high collision count
        let collisions = hash1.collision_count(&hash2);
        assert!(
            collisions > 40,
            "Expected >40 collisions, got {}",
            collisions
        );
    }

    #[test]
    fn test_simhasher_dissimilar_vectors() {
        let hasher = SimHasher::new(128);

        let vector1 = vec![1.0; 128];
        let vector2 = vec![-1.0; 128];

        let hash1 = hasher.hash_from_slice(&vector1).unwrap();
        let hash2 = hasher.hash_from_slice(&vector2).unwrap();

        // Opposite vectors should have low collision count
        let collisions = hash1.collision_count(&hash2);
        assert!(
            collisions < 20,
            "Expected <20 collisions, got {}",
            collisions
        );
    }
}
