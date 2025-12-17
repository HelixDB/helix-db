use std::hash::Hasher;

/// Hashes a label into a 4 byte array.
///
/// This is used to index the label in the graph.
///
/// The hash is also used to index the label in the secondary indices.
#[inline(always)]
pub fn hash_label(label: &str, seed: Option<u32>) -> [u8; 4] {
    let mut hash = twox_hash::XxHash32::with_seed(seed.unwrap_or(0));
    hash.write(label.as_bytes());
    hash.finish_32().to_be_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_hash_label_consistency() {
        let label = "person";
        let hash1 = hash_label(label, None);
        let hash2 = hash_label(label, None);

        assert_eq!(hash1, hash2, "Hash must be deterministic");
    }

    #[test]
    fn test_hash_label_different_labels() {
        let hash_person = hash_label("person", None);
        let hash_company = hash_label("company", None);

        assert_ne!(
            hash_person, hash_company,
            "Different labels should produce different hashes"
        );
    }

    #[test]
    fn test_hash_label_with_seed() {
        let label = "person";
        let hash_no_seed = hash_label(label, None);
        let hash_seed_0 = hash_label(label, Some(0));
        let hash_seed_42 = hash_label(label, Some(42));

        // Same label with no seed vs seed 0 should be same
        assert_eq!(
            hash_no_seed, hash_seed_0,
            "No seed should be equivalent to seed 0"
        );

        // Different seed should produce different hash
        assert_ne!(
            hash_no_seed, hash_seed_42,
            "Different seeds should produce different hashes"
        );
    }

    #[test]
    fn test_hash_label_collision_rate() {
        // Test collision rate with 10,000 labels
        let labels: Vec<String> = (0..10_000).map(|i| format!("label_{}", i)).collect();

        let hashes: HashSet<[u8; 4]> = labels.iter().map(|l| hash_label(l, None)).collect();

        let collision_rate = 1.0 - (hashes.len() as f64 / labels.len() as f64);

        // Collision rate should be very low (< 1%)
        assert!(
            collision_rate < 0.01,
            "Collision rate too high: {:.2}%",
            collision_rate * 100.0
        );
    }

    #[test]
    fn test_hash_label_empty_string() {
        let hash = hash_label("", None);

        // Empty string should still produce a valid hash
        assert_eq!(hash.len(), 4);

        // Should be consistent
        let hash2 = hash_label("", None);
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_hash_label_utf8() {
        // Test with UTF-8 characters
        let labels = vec![
            "person",
            "人",     // Chinese character
            "🚀",     // Emoji
            "Ñoño",   // Spanish with tildes
            "Привет", // Russian
            "مرحبا",  // Arabic
        ];

        let hashes: Vec<[u8; 4]> = labels.iter().map(|l| hash_label(l, None)).collect();

        // All should be different
        let unique_hashes: HashSet<_> = hashes.iter().collect();
        assert_eq!(
            unique_hashes.len(),
            hashes.len(),
            "All UTF-8 labels should produce unique hashes"
        );

        // Each should be consistent
        for label in &labels {
            let hash1 = hash_label(label, None);
            let hash2 = hash_label(label, None);
            assert_eq!(hash1, hash2, "Hash for '{}' should be consistent", label);
        }
    }

    #[test]
    fn test_hash_label_case_sensitivity() {
        let hash_lower = hash_label("person", None);
        let hash_upper = hash_label("Person", None);
        let hash_all_upper = hash_label("PERSON", None);

        // Hash should be case-sensitive
        assert_ne!(hash_lower, hash_upper);
        assert_ne!(hash_lower, hash_all_upper);
        assert_ne!(hash_upper, hash_all_upper);
    }

    #[test]
    fn test_hash_label_long_strings() {
        // Test with very long label names
        let short_label = "person";
        let long_label = "a".repeat(1000);
        let very_long_label = "b".repeat(10_000);

        let hash_short = hash_label(short_label, None);
        let hash_long = hash_label(&long_label, None);
        let hash_very_long = hash_label(&very_long_label, None);

        // All should produce different hashes
        assert_ne!(hash_short, hash_long);
        assert_ne!(hash_short, hash_very_long);
        assert_ne!(hash_long, hash_very_long);

        // Should be consistent
        assert_eq!(hash_long, hash_label(&long_label, None));
        assert_eq!(hash_very_long, hash_label(&very_long_label, None));
    }

    #[test]
    fn test_hash_label_similar_strings() {
        // Test labels that differ by only one character
        let labels = ["person", "persons", "person1", "person_", "Person"];

        let hashes: Vec<[u8; 4]> = labels.iter().map(|l| hash_label(l, None)).collect();

        // All should be different
        let unique_hashes: HashSet<_> = hashes.iter().collect();
        assert_eq!(
            unique_hashes.len(),
            hashes.len(),
            "Similar labels should produce unique hashes"
        );
    }

    #[test]
    fn test_hash_label_output_format() {
        let hash = hash_label("person", None);

        // Output should be exactly 4 bytes
        assert_eq!(hash.len(), 4);

        // Should be big-endian bytes (we can convert back)
        let value = u32::from_be_bytes(hash);
        assert!(
            value > 0,
            "Hash value should be non-zero for non-empty string"
        );
    }

    #[test]
    fn test_hash_label_performance() {
        // Hash 100k labels and ensure it completes quickly
        let start = std::time::Instant::now();

        for i in 0..100_000 {
            let label = format!("label_{}", i);
            let _ = hash_label(&label, None);
        }

        let elapsed = start.elapsed();

        // Should complete in less than 1 second
        assert!(
            elapsed.as_secs() < 1,
            "Label hashing too slow: {:?}",
            elapsed
        );
    }

    #[test]
    fn test_hash_label_common_patterns() {
        // Test common label patterns used in graphs
        let common_labels = vec![
            "node",
            "edge",
            "person",
            "company",
            "knows",
            "works_at",
            "friend",
            "follows",
            "likes",
            "created_by",
        ];

        let hashes: HashSet<[u8; 4]> = common_labels.iter().map(|l| hash_label(l, None)).collect();

        // All common labels should hash uniquely
        assert_eq!(
            hashes.len(),
            common_labels.len(),
            "Common graph labels should all hash uniquely"
        );
    }
}
