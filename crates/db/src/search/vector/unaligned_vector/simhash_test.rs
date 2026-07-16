#[cfg(test)]
mod tests {
    use super::super::*;

    #[test]
    fn test_f32_simhash_consistency() {
        let hasher = SimHasher::new(128);

        let vector = vec![1.0; 128];
        let unaligned = UnalignedVector::<f32>::from_slice(&vector);

        // Hash the same vector twice - should get same result
        let hash1 = f32::compute_simhash(&unaligned, &hasher).unwrap();
        let hash2 = f32::compute_simhash(&unaligned, &hasher).unwrap();

        assert_eq!(hash1, hash2, "Same vector should produce same SimHash");
    }

    #[test]
    fn test_f32_simhash_similar_vectors() {
        let hasher = SimHasher::new(128);

        let vector1 = vec![1.0; 128];
        let mut vector2 = vec![1.0; 128];
        // Change 5% of components
        for value in vector2.iter_mut().take(6) {
            *value = -1.0;
        }

        let unaligned1 = UnalignedVector::<f32>::from_slice(&vector1);
        let unaligned2 = UnalignedVector::<f32>::from_slice(&vector2);

        let hash1 = f32::compute_simhash(&unaligned1, &hasher).unwrap();
        let hash2 = f32::compute_simhash(&unaligned2, &hasher).unwrap();

        // Similar vectors should have high collision count
        let collisions = hash1.collision_count(&hash2);
        assert!(
            collisions > 50,
            "Similar vectors should have >50 bit collisions, got {}",
            collisions
        );
    }

    #[test]
    fn test_f32_simhash_dissimilar_vectors() {
        let hasher = SimHasher::new(128);

        let vector1 = vec![1.0; 128];
        let vector2 = vec![-1.0; 128];

        let unaligned1 = UnalignedVector::<f32>::from_slice(&vector1);
        let unaligned2 = UnalignedVector::<f32>::from_slice(&vector2);

        let hash1 = f32::compute_simhash(&unaligned1, &hasher).unwrap();
        let hash2 = f32::compute_simhash(&unaligned2, &hasher).unwrap();

        // Dissimilar vectors should have low collision count
        let collisions = hash1.collision_count(&hash2);
        assert!(
            collisions < 20,
            "Dissimilar vectors should have <20 bit collisions, got {}",
            collisions
        );
    }

    #[test]
    fn test_binary_simhash_consistency() {
        let hasher = SimHasher::new(128);

        let vector = vec![1.0; 128];
        let unaligned = UnalignedVector::<Binary>::from_slice(&vector);

        let hash1 = Binary::compute_simhash(&unaligned, &hasher).unwrap();
        let hash2 = Binary::compute_simhash(&unaligned, &hasher).unwrap();

        assert_eq!(hash1, hash2, "Same vector should produce same SimHash");
    }

    #[test]
    fn test_binary_simhash_different_vectors() {
        let hasher = SimHasher::new(64);

        let vector1 = vec![1.0; 64];
        let mut vector2 = vec![1.0; 64];
        // Flip some bits
        for value in vector2.iter_mut().take(10) {
            *value = 0.0;
        }

        let unaligned1 = UnalignedVector::<Binary>::from_slice(&vector1);
        let unaligned2 = UnalignedVector::<Binary>::from_slice(&vector2);

        let hash1 = Binary::compute_simhash(&unaligned1, &hasher).unwrap();
        let hash2 = Binary::compute_simhash(&unaligned2, &hasher).unwrap();

        // Different vectors should produce different hashes
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_binary_quantized_simhash_consistency() {
        let hasher = SimHasher::new(128);

        let vector = vec![1.0; 128];
        let unaligned = UnalignedVector::<BinaryQuantized>::from_slice(&vector);

        let hash1 = BinaryQuantized::compute_simhash(&unaligned, &hasher).unwrap();
        let hash2 = BinaryQuantized::compute_simhash(&unaligned, &hasher).unwrap();

        assert_eq!(hash1, hash2, "Same vector should produce same SimHash");
    }

    #[test]
    fn test_binary_quantized_simhash_positive_vs_negative() {
        let hasher = SimHasher::new(64);

        let vector1 = vec![1.0; 64]; // All positive
        let vector2 = vec![-1.0; 64]; // All negative

        let unaligned1 = UnalignedVector::<BinaryQuantized>::from_slice(&vector1);
        let unaligned2 = UnalignedVector::<BinaryQuantized>::from_slice(&vector2);

        let hash1 = BinaryQuantized::compute_simhash(&unaligned1, &hasher).unwrap();
        let hash2 = BinaryQuantized::compute_simhash(&unaligned2, &hasher).unwrap();

        // Opposite vectors should have very different hashes
        let collisions = hash1.collision_count(&hash2);
        assert!(
            collisions < 20,
            "Opposite sign vectors should have <20 collisions, got {}",
            collisions
        );
    }

    #[test]
    fn test_cross_codec_consistency() {
        // Test that f32 and Binary produce related hashes for the same logical vector
        let hasher = SimHasher::new(64);

        let vector = vec![1.0; 64];

        let unaligned_f32 = UnalignedVector::<f32>::from_slice(&vector);
        let unaligned_binary = UnalignedVector::<Binary>::from_slice(&vector);

        let hash_f32 = f32::compute_simhash(&unaligned_f32, &hasher).unwrap();
        let hash_binary = Binary::compute_simhash(&unaligned_binary, &hasher).unwrap();

        // They should be similar since the logical vector is the same
        let collisions = hash_f32.collision_count(&hash_binary);
        assert!(
            collisions > 40,
            "Same logical vector should produce similar hashes, got {} collisions",
            collisions
        );
    }

    #[test]
    fn test_simhash_filtering_performance() {
        // Test that SimHash actually filters effectively
        let hasher = SimHasher::new(128);
        let threshold = crate::search::vector::DEFAULT_SIMHASH_COLLISION_THRESHOLD;

        let query = vec![1.0; 128];
        let query_unaligned = UnalignedVector::<f32>::from_slice(&query);
        let query_hash = f32::compute_simhash(&query_unaligned, &hasher).unwrap();

        // Create similar and dissimilar vectors
        let mut similar = vec![1.0; 128];
        for value in similar.iter_mut().take(5) {
            *value = 0.9; // Slightly different
        }

        let dissimilar = vec![-1.0; 128];

        let similar_unaligned = UnalignedVector::<f32>::from_slice(&similar);
        let dissimilar_unaligned = UnalignedVector::<f32>::from_slice(&dissimilar);

        let similar_hash = f32::compute_simhash(&similar_unaligned, &hasher).unwrap();
        let dissimilar_hash = f32::compute_simhash(&dissimilar_unaligned, &hasher).unwrap();

        // Similar vector should pass threshold
        assert!(
            similar_hash.passes_threshold(&query_hash, threshold),
            "Similar vector should pass SimHash threshold"
        );

        // Dissimilar vector should NOT pass threshold
        assert!(
            !dissimilar_hash.passes_threshold(&query_hash, threshold),
            "Dissimilar vector should NOT pass SimHash threshold"
        );
    }

    #[test]
    fn test_dimension_mismatch() {
        let hasher = SimHasher::new(64);

        // Create a vector with wrong dimension
        let vector = vec![1.0; 128]; // hasher expects 64
        let unaligned = UnalignedVector::<f32>::from_slice(&vector);

        assert_eq!(
            f32::compute_simhash(&unaligned, &hasher),
            Err(SimHashError::DimensionMismatch {
                expected: 64,
                actual: 128,
            })
        );
    }

    #[test]
    fn test_zero_vector_simhash() {
        let hasher = SimHasher::new(128);

        let zero_vector = vec![0.0; 128];
        let unaligned = UnalignedVector::<f32>::from_slice(&zero_vector);

        // Zero vector should still produce a valid hash
        let hash = f32::compute_simhash(&unaligned, &hasher).unwrap();

        // Hash should be deterministic
        let hash2 = f32::compute_simhash(&unaligned, &hasher).unwrap();
        assert_eq!(hash, hash2);
    }
}
