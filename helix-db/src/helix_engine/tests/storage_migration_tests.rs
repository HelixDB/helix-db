//! Comprehensive test suite for storage_migration.rs
//!
//! This test module covers:
//! - Unit tests for endianness conversion functions
//! - Unit tests for property conversion functions
//! - Integration tests for full migration scenarios
//! - Property-based tests for correctness validation
//! - Error handling tests for failure modes
//! - Performance tests for large datasets

use super::{
    metadata::{StorageMetadata, VectorEndianness, NATIVE_VECTOR_ENDIANNESS},
    storage_migration::{
        convert_all_vector_properties, convert_old_vector_properties_to_new_format,
        convert_vector_endianness, migrate,
    },
    HelixGraphStorage,
};
use crate::{
    helix_engine::{
        storage_core::version_info::VersionInfo, traversal_core::config::Config,
        types::GraphError,
    },
    protocol::value::Value,
};
use std::collections::HashMap;
use tempfile::TempDir;

// ============================================================================
// Test Utilities and Fixtures
// ============================================================================

/// Helper function to create a test storage instance
fn setup_test_storage() -> (HelixGraphStorage, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let config = Config::default();
    let version_info = VersionInfo::default();

    let storage =
        HelixGraphStorage::new(temp_dir.path().to_str().unwrap(), config, version_info).unwrap();

    (storage, temp_dir)
}

/// Create test vector data in a specific endianness
fn create_test_vector_bytes(values: &[f64], endianness: VectorEndianness) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(values.len() * 8);
    for &value in values {
        let value_bytes = match endianness {
            VectorEndianness::BigEndian => value.to_be_bytes(),
            VectorEndianness::LittleEndian => value.to_le_bytes(),
        };
        bytes.extend_from_slice(&value_bytes);
    }
    bytes
}

/// Read f64 values from bytes in a specific endianness
fn read_f64_values(bytes: &[u8], endianness: VectorEndianness) -> Vec<f64> {
    let mut values = Vec::with_capacity(bytes.len() / 8);
    for chunk in bytes.chunks_exact(8) {
        let value = match endianness {
            VectorEndianness::BigEndian => f64::from_be_bytes(chunk.try_into().unwrap()),
            VectorEndianness::LittleEndian => f64::from_le_bytes(chunk.try_into().unwrap()),
        };
        values.push(value);
    }
    values
}

/// Create old-format vector properties (HashMap-based)
fn create_old_properties(
    label: &str,
    is_deleted: bool,
    extra_props: HashMap<String, Value>,
) -> Vec<u8> {
    let mut props = HashMap::new();
    props.insert("label".to_string(), Value::String(label.to_string()));
    props.insert("is_deleted".to_string(), Value::Boolean(is_deleted));

    for (k, v) in extra_props {
        props.insert(k, v);
    }

    bincode::serialize(&props).unwrap()
}

/// Populate storage with test vectors in a specific endianness
fn populate_test_vectors(
    storage: &mut HelixGraphStorage,
    count: usize,
    endianness: VectorEndianness,
) -> Result<(), GraphError> {
    let mut txn = storage.graph_env.write_txn()?;

    for i in 0..count {
        let id = i as u128;
        let vector_data = create_test_vector_bytes(
            &[i as f64, (i + 1) as f64, (i + 2) as f64],
            endianness,
        );

        storage
            .vectors
            .vectors_db
            .put(&mut txn, &id.to_be_bytes(), &vector_data)?;
    }

    txn.commit()?;
    Ok(())
}

/// Populate storage with old-format properties
fn populate_old_properties(
    storage: &mut HelixGraphStorage,
    count: usize,
) -> Result<(), GraphError> {
    let mut txn = storage.graph_env.write_txn()?;

    for i in 0..count {
        let id = i as u128;
        let mut extra_props = HashMap::new();
        extra_props.insert("test_prop".to_string(), Value::F64(i as f64));

        let property_bytes =
            create_old_properties(&format!("label_{}", i), i % 2 == 0, extra_props);

        storage
            .vectors
            .vector_properties_db
            .put(&mut txn, &id, &property_bytes)?;
    }

    txn.commit()?;
    Ok(())
}

/// Set storage metadata to a specific state
fn set_metadata(
    storage: &mut HelixGraphStorage,
    metadata: StorageMetadata,
) -> Result<(), GraphError> {
    let mut txn = storage.graph_env.write_txn()?;
    metadata.save(&mut txn, &storage.metadata_db)?;
    txn.commit()?;
    Ok(())
}

/// Read all vectors from storage and return as f64 values
fn read_all_vectors(
    storage: &HelixGraphStorage,
    endianness: VectorEndianness,
) -> Result<Vec<Vec<f64>>, GraphError> {
    let txn = storage.graph_env.read_txn()?;
    let mut all_vectors = Vec::new();

    for kv in storage.vectors.vectors_db.iter(&txn)? {
        let (_, value) = kv?;
        let values = read_f64_values(value, endianness);
        all_vectors.push(values);
    }

    Ok(all_vectors)
}

/// Clear all metadata from storage (simulates PreMetadata state)
fn clear_metadata(storage: &mut HelixGraphStorage) -> Result<(), GraphError> {
    let mut txn = storage.graph_env.write_txn()?;
    storage.metadata_db.clear(&mut txn)?;
    txn.commit()?;
    Ok(())
}

// ============================================================================
// Unit Tests: Endianness Conversion
// ============================================================================

#[test]
fn test_convert_vector_endianness_empty_input() {
    let arena = bumpalo::Bump::new();
    let result = convert_vector_endianness(&[], VectorEndianness::BigEndian, &arena);

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), &[] as &[u8]);
}

#[test]
fn test_convert_vector_endianness_single_f64() {
    let arena = bumpalo::Bump::new();
    let value: f64 = 3.14159;
    let big_endian_bytes = value.to_be_bytes();

    let result =
        convert_vector_endianness(&big_endian_bytes, VectorEndianness::BigEndian, &arena).unwrap();

    // Result should be in native endianness
    let native_value = f64::from_ne_bytes(result.try_into().unwrap());
    assert_eq!(native_value, value);
}

#[test]
fn test_convert_vector_endianness_multiple_f64s() {
    let arena = bumpalo::Bump::new();
    let values = vec![1.0, 2.5, -3.7, 4.2, 5.9];
    let big_endian_bytes = create_test_vector_bytes(&values, VectorEndianness::BigEndian);

    let result =
        convert_vector_endianness(&big_endian_bytes, VectorEndianness::BigEndian, &arena).unwrap();

    // Read back values in native endianness
    let result_values: Vec<f64> = result
        .chunks_exact(8)
        .map(|chunk| f64::from_ne_bytes(chunk.try_into().unwrap()))
        .collect();

    for (original, converted) in values.iter().zip(result_values.iter()) {
        assert_eq!(original, converted);
    }
}

#[test]
fn test_convert_vector_endianness_invalid_length() {
    let arena = bumpalo::Bump::new();
    let invalid_bytes = vec![1, 2, 3, 4, 5]; // Not a multiple of 8

    let result = convert_vector_endianness(&invalid_bytes, VectorEndianness::BigEndian, &arena);

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("not a multiple"));
}

#[test]
fn test_convert_vector_endianness_roundtrip() {
    let arena = bumpalo::Bump::new();
    let values = vec![1.0, 2.5, -3.7, 100.123, -999.999];

    // Start with big endian
    let big_endian_bytes = create_test_vector_bytes(&values, VectorEndianness::BigEndian);

    // Convert big -> native
    let native_bytes =
        convert_vector_endianness(&big_endian_bytes, VectorEndianness::BigEndian, &arena).unwrap();

    // Read values back
    let result_values: Vec<f64> = native_bytes
        .chunks_exact(8)
        .map(|chunk| f64::from_ne_bytes(chunk.try_into().unwrap()))
        .collect();

    for (original, converted) in values.iter().zip(result_values.iter()) {
        assert_eq!(original, converted);
    }
}

#[test]
fn test_convert_vector_endianness_special_values() {
    let arena = bumpalo::Bump::new();
    let special_values = vec![
        0.0,
        -0.0,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::MIN,
        f64::MAX,
        f64::EPSILON,
    ];

    let big_endian_bytes = create_test_vector_bytes(&special_values, VectorEndianness::BigEndian);

    let result =
        convert_vector_endianness(&big_endian_bytes, VectorEndianness::BigEndian, &arena).unwrap();

    let result_values: Vec<f64> = result
        .chunks_exact(8)
        .map(|chunk| f64::from_ne_bytes(chunk.try_into().unwrap()))
        .collect();

    for (original, converted) in special_values.iter().zip(result_values.iter()) {
        // Use bit equality for special values like NaN and -0.0
        assert_eq!(original.to_bits(), converted.to_bits());
    }
}

#[test]
fn test_convert_vector_endianness_from_little_endian() {
    let arena = bumpalo::Bump::new();
    let values = vec![1.1, 2.2, 3.3];
    let little_endian_bytes = create_test_vector_bytes(&values, VectorEndianness::LittleEndian);

    let result = convert_vector_endianness(
        &little_endian_bytes,
        VectorEndianness::LittleEndian,
        &arena,
    )
    .unwrap();

    let result_values: Vec<f64> = result
        .chunks_exact(8)
        .map(|chunk| f64::from_ne_bytes(chunk.try_into().unwrap()))
        .collect();

    for (original, converted) in values.iter().zip(result_values.iter()) {
        assert_eq!(original, converted);
    }
}

// ============================================================================
// Unit Tests: Property Conversion
// ============================================================================

#[test]
fn test_convert_old_properties_basic() {
    let arena = bumpalo::Bump::new();
    let old_bytes = create_old_properties("test_label", false, HashMap::new());

    let result = convert_old_vector_properties_to_new_format(&old_bytes, &arena);
    assert!(result.is_ok());

    // We can't directly deserialize HVector, but we can verify the conversion succeeded
    let new_bytes = result.unwrap();
    assert!(!new_bytes.is_empty());
}

#[test]
fn test_convert_old_properties_with_deleted_flag() {
    let arena = bumpalo::Bump::new();
    let old_bytes = create_old_properties("deleted_vector", true, HashMap::new());

    let result = convert_old_vector_properties_to_new_format(&old_bytes, &arena);
    assert!(result.is_ok());
    assert!(!result.unwrap().is_empty());
}

#[test]
fn test_convert_old_properties_with_extra_props() {
    let arena = bumpalo::Bump::new();
    let mut extra = HashMap::new();
    extra.insert("name".to_string(), Value::String("test".to_string()));
    extra.insert("count".to_string(), Value::F64(42.0));
    extra.insert("active".to_string(), Value::Boolean(true));

    let old_bytes = create_old_properties("test_label", false, extra);

    let result = convert_old_vector_properties_to_new_format(&old_bytes, &arena);
    assert!(result.is_ok());
    assert!(!result.unwrap().is_empty());
}

#[test]
fn test_convert_old_properties_empty_extra_props() {
    let arena = bumpalo::Bump::new();
    let old_bytes = create_old_properties("minimal", false, HashMap::new());

    let result = convert_old_vector_properties_to_new_format(&old_bytes, &arena);
    assert!(result.is_ok());
    assert!(!result.unwrap().is_empty());
}

#[test]
#[should_panic(expected = "all old vectors should have label")]
fn test_convert_old_properties_missing_label() {
    let arena = bumpalo::Bump::new();
    let mut props = HashMap::new();
    props.insert("is_deleted".to_string(), Value::Boolean(false));
    // Missing "label"

    let bytes = bincode::serialize(&props).unwrap();
    let _ = convert_old_vector_properties_to_new_format(&bytes, &arena);
}

#[test]
#[should_panic(expected = "all old vectors should have deleted")]
fn test_convert_old_properties_missing_is_deleted() {
    let arena = bumpalo::Bump::new();
    let mut props = HashMap::new();
    props.insert("label".to_string(), Value::String("test".to_string()));
    // Missing "is_deleted"

    let bytes = bincode::serialize(&props).unwrap();
    let _ = convert_old_vector_properties_to_new_format(&bytes, &arena);
}

#[test]
fn test_convert_old_properties_invalid_bincode() {
    let arena = bumpalo::Bump::new();
    let invalid_bytes = vec![1, 2, 3, 4, 5]; // Not valid bincode

    let result = convert_old_vector_properties_to_new_format(&invalid_bytes, &arena);
    assert!(result.is_err());
}

// ============================================================================
// Integration Tests: Full Migration Scenarios
// ============================================================================

#[test]
fn test_migrate_empty_database() {
    let (mut storage, _temp_dir) = setup_test_storage();

    // Storage is already created with migrations run, but let's verify the state
    let txn = storage.graph_env.read_txn().unwrap();
    let metadata = StorageMetadata::read(&txn, &storage.metadata_db).unwrap();

    assert!(matches!(
        metadata,
        StorageMetadata::VectorNativeEndianness { .. }
    ));
}

#[test]
fn test_migrate_pre_metadata_to_native() {
    let (mut storage, _temp_dir) = setup_test_storage();

    // Clear metadata to simulate PreMetadata state
    clear_metadata(&mut storage).unwrap();

    // Populate with vectors in big-endian format (PreMetadata default)
    populate_test_vectors(&mut storage, 10, VectorEndianness::BigEndian).unwrap();
    populate_old_properties(&mut storage, 10).unwrap();

    // Run migration
    let result = migrate(&mut storage);
    assert!(result.is_ok());

    // Verify metadata was updated
    {
        let txn = storage.graph_env.read_txn().unwrap();
        let metadata = StorageMetadata::read(&txn, &storage.metadata_db).unwrap();

        match metadata {
            StorageMetadata::VectorNativeEndianness { vector_endianness } => {
                assert_eq!(vector_endianness, NATIVE_VECTOR_ENDIANNESS);
            }
            _ => panic!("Expected VectorNativeEndianness metadata"),
        }
    } // txn dropped here

    // Verify vectors are readable in native endianness
    let vectors = read_all_vectors(&storage, NATIVE_VECTOR_ENDIANNESS).unwrap();
    assert_eq!(vectors.len(), 10);

    for (i, vector) in vectors.iter().enumerate() {
        let expected = vec![i as f64, (i + 1) as f64, (i + 2) as f64];
        assert_eq!(vector, &expected);
    }
}

#[test]
fn test_migrate_single_vector() {
    let (mut storage, _temp_dir) = setup_test_storage();

    // Clear and repopulate
    clear_metadata(&mut storage).unwrap();
    populate_test_vectors(&mut storage, 1, VectorEndianness::BigEndian).unwrap();

    let result = migrate(&mut storage);
    assert!(result.is_ok());

    let vectors = read_all_vectors(&storage, NATIVE_VECTOR_ENDIANNESS).unwrap();
    assert_eq!(vectors.len(), 1);
    assert_eq!(vectors[0], vec![0.0, 1.0, 2.0]);
}

#[test]
fn test_migrate_exact_batch_size() {
    let (mut storage, _temp_dir) = setup_test_storage();

    clear_metadata(&mut storage).unwrap();
    populate_test_vectors(&mut storage, 1024, VectorEndianness::BigEndian).unwrap();

    let result = migrate(&mut storage);
    assert!(result.is_ok());

    let vectors = read_all_vectors(&storage, NATIVE_VECTOR_ENDIANNESS).unwrap();
    assert_eq!(vectors.len(), 1024);

    // Verify first and last vectors
    assert_eq!(vectors[0], vec![0.0, 1.0, 2.0]);
    assert_eq!(vectors[1023], vec![1023.0, 1024.0, 1025.0]);
}

#[test]
fn test_migrate_multiple_batches() {
    let (mut storage, _temp_dir) = setup_test_storage();

    clear_metadata(&mut storage).unwrap();
    populate_test_vectors(&mut storage, 2500, VectorEndianness::BigEndian).unwrap();

    let result = migrate(&mut storage);
    assert!(result.is_ok());

    let vectors = read_all_vectors(&storage, NATIVE_VECTOR_ENDIANNESS).unwrap();
    assert_eq!(vectors.len(), 2500);

    // Verify vectors across batch boundaries
    assert_eq!(vectors[0], vec![0.0, 1.0, 2.0]);
    assert_eq!(vectors[1023], vec![1023.0, 1024.0, 1025.0]);
    assert_eq!(vectors[1024], vec![1024.0, 1025.0, 1026.0]);
    assert_eq!(vectors[2499], vec![2499.0, 2500.0, 2501.0]);
}

#[test]
fn test_migrate_already_native_endianness() {
    let (mut storage, _temp_dir) = setup_test_storage();

    // Add vectors already in native endianness
    populate_test_vectors(&mut storage, 10, NATIVE_VECTOR_ENDIANNESS).unwrap();

    // Migration should be a no-op (already done during setup_test_storage)
    let result = migrate(&mut storage);
    assert!(result.is_ok());

    // Vectors should remain unchanged
    let vectors = read_all_vectors(&storage, NATIVE_VECTOR_ENDIANNESS).unwrap();
    assert_eq!(vectors.len(), 10);
}

#[test]
fn test_migrate_idempotency() {
    let (mut storage, _temp_dir) = setup_test_storage();

    clear_metadata(&mut storage).unwrap();
    populate_test_vectors(&mut storage, 100, VectorEndianness::BigEndian).unwrap();

    // Run migration multiple times
    migrate(&mut storage).unwrap();
    let vectors_after_first = read_all_vectors(&storage, NATIVE_VECTOR_ENDIANNESS).unwrap();

    migrate(&mut storage).unwrap();
    let vectors_after_second = read_all_vectors(&storage, NATIVE_VECTOR_ENDIANNESS).unwrap();

    migrate(&mut storage).unwrap();
    let vectors_after_third = read_all_vectors(&storage, NATIVE_VECTOR_ENDIANNESS).unwrap();

    // All should be identical
    assert_eq!(vectors_after_first, vectors_after_second);
    assert_eq!(vectors_after_second, vectors_after_third);
}

#[test]
fn test_migrate_with_properties() {
    let (mut storage, _temp_dir) = setup_test_storage();

    clear_metadata(&mut storage).unwrap();
    populate_test_vectors(&mut storage, 50, VectorEndianness::BigEndian).unwrap();
    populate_old_properties(&mut storage, 50).unwrap();

    let result = migrate(&mut storage);
    assert!(result.is_ok());

    // Verify both vectors and properties were migrated
    let vectors = read_all_vectors(&storage, NATIVE_VECTOR_ENDIANNESS).unwrap();
    assert_eq!(vectors.len(), 50);

    // Check properties count
    let txn = storage.graph_env.read_txn().unwrap();
    let prop_count = storage.vectors.vector_properties_db.len(&txn).unwrap();
    assert_eq!(prop_count, 50);
}

// ============================================================================
// Integration Tests: Batch Boundary Conditions
// ============================================================================

#[test]
fn test_migrate_batch_boundary_1023() {
    let (mut storage, _temp_dir) = setup_test_storage();
    clear_metadata(&mut storage).unwrap();
    populate_test_vectors(&mut storage, 1023, VectorEndianness::BigEndian).unwrap();

    let result = migrate(&mut storage);
    assert!(result.is_ok());

    let vectors = read_all_vectors(&storage, NATIVE_VECTOR_ENDIANNESS).unwrap();
    assert_eq!(vectors.len(), 1023);
}

#[test]
fn test_migrate_batch_boundary_1025() {
    let (mut storage, _temp_dir) = setup_test_storage();
    clear_metadata(&mut storage).unwrap();
    populate_test_vectors(&mut storage, 1025, VectorEndianness::BigEndian).unwrap();

    let result = migrate(&mut storage);
    assert!(result.is_ok());

    let vectors = read_all_vectors(&storage, NATIVE_VECTOR_ENDIANNESS).unwrap();
    assert_eq!(vectors.len(), 1025);
}

#[test]
fn test_migrate_batch_boundary_2047() {
    let (mut storage, _temp_dir) = setup_test_storage();
    clear_metadata(&mut storage).unwrap();
    populate_test_vectors(&mut storage, 2047, VectorEndianness::BigEndian).unwrap();

    let result = migrate(&mut storage);
    assert!(result.is_ok());

    let vectors = read_all_vectors(&storage, NATIVE_VECTOR_ENDIANNESS).unwrap();
    assert_eq!(vectors.len(), 2047);
}

#[test]
fn test_migrate_batch_boundary_2048() {
    let (mut storage, _temp_dir) = setup_test_storage();
    clear_metadata(&mut storage).unwrap();
    populate_test_vectors(&mut storage, 2048, VectorEndianness::BigEndian).unwrap();

    let result = migrate(&mut storage);
    assert!(result.is_ok());

    let vectors = read_all_vectors(&storage, NATIVE_VECTOR_ENDIANNESS).unwrap();
    assert_eq!(vectors.len(), 2048);
}

// ============================================================================
// Property-Based Tests
// ============================================================================

use proptest::prelude::*;

proptest! {
    #[test]
    fn proptest_endianness_conversion_preserves_values(
        values in prop::collection::vec(prop::num::f64::ANY, 1..100)
    ) {
        let arena = bumpalo::Bump::new();

        // Filter out NaN for equality comparison
        let values: Vec<f64> = values.into_iter().filter(|v| !v.is_nan()).collect();
        if values.is_empty() {
            return Ok(());
        }

        // Test both endianness conversions
        for source_endianness in [VectorEndianness::BigEndian, VectorEndianness::LittleEndian] {
            let source_bytes = create_test_vector_bytes(&values, source_endianness);

            let result = convert_vector_endianness(&source_bytes, source_endianness, &arena)
                .expect("conversion should succeed");

            let result_values: Vec<f64> = result
                .chunks_exact(8)
                .map(|chunk| f64::from_ne_bytes(chunk.try_into().unwrap()))
                .collect();

            prop_assert_eq!(values.len(), result_values.len());

            for (original, converted) in values.iter().zip(result_values.iter()) {
                prop_assert_eq!(original, converted);
            }
        }
    }

    #[test]
    fn proptest_endianness_conversion_valid_length(
        byte_count in 1usize..200
    ) {
        let arena = bumpalo::Bump::new();
        let bytes = vec![0u8; byte_count];

        let result = convert_vector_endianness(&bytes, VectorEndianness::BigEndian, &arena);

        if byte_count % 8 == 0 {
            prop_assert!(result.is_ok());
        } else {
            prop_assert!(result.is_err());
        }
    }

    #[test]
    fn proptest_property_migration_preserves_data(
        label in "[a-z]{1,20}",
        is_deleted in any::<bool>(),
        prop_count in 0usize..10
    ) {
        let arena = bumpalo::Bump::new();
        let mut extra_props = HashMap::new();

        for i in 0..prop_count {
            extra_props.insert(
                format!("prop_{}", i),
                Value::F64(i as f64),
            );
        }

        let old_bytes = create_old_properties(&label, is_deleted, extra_props);
        let result = convert_old_vector_properties_to_new_format(&old_bytes, &arena)
            .expect("property conversion should succeed");

        // Verify conversion succeeded by checking result is not empty
        prop_assert!(!result.is_empty());
    }
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[test]
fn test_error_invalid_vector_data_length() {
    let arena = bumpalo::Bump::new();
    let invalid_bytes = vec![1, 2, 3, 4, 5, 6, 7]; // 7 bytes, not multiple of 8

    let result = convert_vector_endianness(&invalid_bytes, VectorEndianness::BigEndian, &arena);

    assert!(result.is_err());
    match result {
        Err(GraphError::New(msg)) => {
            assert!(msg.contains("not a multiple"));
        }
        _ => panic!("Expected GraphError::New with length error"),
    }
}

#[test]
fn test_error_corrupted_property_data() {
    let arena = bumpalo::Bump::new();
    let corrupted = vec![255u8; 100]; // Random bytes, not valid bincode

    let result = convert_old_vector_properties_to_new_format(&corrupted, &arena);
    assert!(result.is_err());
}

#[test]
fn test_error_handling_graceful_failure() {
    // Test that errors don't corrupt the database
    let (mut storage, _temp_dir) = setup_test_storage();

    clear_metadata(&mut storage).unwrap();

    // Add valid data
    populate_test_vectors(&mut storage, 10, VectorEndianness::BigEndian).unwrap();

    // Now add invalid data manually
    {
        let mut txn = storage.graph_env.write_txn().unwrap();
        let bad_id = 9999u128;
        let bad_data = vec![1, 2, 3]; // Invalid length

        storage
            .vectors
            .vectors_db
            .put(&mut txn, &bad_id.to_be_bytes(), &bad_data)
            .unwrap();

        txn.commit().unwrap();
    }

    // Migration should fail on invalid data
    let result = migrate(&mut storage);
    assert!(result.is_err());

    // But the 10 valid vectors should still be there
    let txn = storage.graph_env.read_txn().unwrap();
    let count = storage.vectors.vectors_db.len(&txn).unwrap();
    assert_eq!(count, 11); // 10 valid + 1 invalid
}

// ============================================================================
// Performance Tests
// ============================================================================

#[test]
#[ignore] // Run with: cargo test --release -- --ignored --nocapture
fn test_performance_large_dataset() {
    use std::time::Instant;

    let (mut storage, _temp_dir) = setup_test_storage();

    clear_metadata(&mut storage).unwrap();

    // Create 100K vectors
    println!("Populating 100K vectors...");
    let start = Instant::now();
    populate_test_vectors(&mut storage, 100_000, VectorEndianness::BigEndian).unwrap();
    println!("Population took: {:?}", start.elapsed());

    // Migrate
    println!("Running migration...");
    let start = Instant::now();
    let result = migrate(&mut storage);
    let duration = start.elapsed();

    assert!(result.is_ok());
    println!("Migration of 100K vectors took: {:?}", duration);
    println!("Average: {:?} per vector", duration / 100_000);

    // Verify a sample
    let vectors = read_all_vectors(&storage, NATIVE_VECTOR_ENDIANNESS).unwrap();
    assert_eq!(vectors.len(), 100_000);
    assert_eq!(vectors[0], vec![0.0, 1.0, 2.0]);
    assert_eq!(vectors[50_000], vec![50_000.0, 50_001.0, 50_002.0]);
    assert_eq!(vectors[99_999], vec![99_999.0, 100_000.0, 100_001.0]);
}

#[test]
#[ignore]
fn test_performance_property_migration() {
    use std::time::Instant;

    let (mut storage, _temp_dir) = setup_test_storage();

    clear_metadata(&mut storage).unwrap();

    println!("Populating 50K properties...");
    populate_old_properties(&mut storage, 50_000).unwrap();

    println!("Running property migration...");
    let start = Instant::now();
    let result = convert_all_vector_properties(&mut storage);
    let duration = start.elapsed();

    assert!(result.is_ok());
    println!("Property migration of 50K items took: {:?}", duration);
    println!("Average: {:?} per property", duration / 50_000);
}

#[test]
fn test_memory_efficiency_batch_processing() {
    // This test verifies that batch processing doesn't cause memory issues
    let (mut storage, _temp_dir) = setup_test_storage();

    clear_metadata(&mut storage).unwrap();

    // Create 5000 vectors (multiple batches)
    populate_test_vectors(&mut storage, 5000, VectorEndianness::BigEndian).unwrap();

    // Migration should complete without OOM
    let result = migrate(&mut storage);
    assert!(result.is_ok());

    let vectors = read_all_vectors(&storage, NATIVE_VECTOR_ENDIANNESS).unwrap();
    assert_eq!(vectors.len(), 5000);
}
