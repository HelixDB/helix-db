use std::{collections::HashMap, sync::atomic};

use byteorder::BE;
use heed3::{
    Database, Env, RwTxn,
    types::{Bytes, U32, U128, Unit},
};
use rand::{SeedableRng, rngs::StdRng};
use serde::Deserialize;

use crate::{
    helix_engine::{
        types::VectorError,
        vector_core::{HNSWConfig, VectorCore, VectorCoreResult},
    },
    protocol::value::Value,
    utils::properties::ImmutablePropertiesMap,
};

// Constants from old version
const OLD_VECTOR_PREFIX: &[u8] = b"v:";

#[derive(Debug)]
struct OldVectorData {
    id: u128,
    label: String,
    data: Vec<f32>,
    properties: Option<serde_json::Value>,
    deleted: bool,
}

/// Old vector properties structure for deserialization
#[derive(Deserialize)]
struct OldVectorProperties {
    label: String,
    #[serde(default)]
    deleted: bool,
    #[serde(default)]
    properties: Option<serde_json::Value>,
}

pub fn needs_migration_from_old_format(env: &Env, txn: &RwTxn) -> VectorCoreResult<bool> {
    // Check for old database structure
    let old_vectors_db = env
        .database_options()
        .types::<Bytes, Bytes>()
        .name("vectors")
        .open(txn)?;
    let old_edges_db = env
        .database_options()
        .types::<Bytes, Unit>()
        .name("hnsw_out_nodes")
        .open(txn)?;
    let new_id_map_db = env
        .database_options()
        .types::<U32<BE>, U128<BE>>()
        .name("id_map")
        .open(txn)?;

    // If old DBs exist but new ID map doesn't, we need migration
    match (old_vectors_db, old_edges_db, new_id_map_db) {
        (Some(_), Some(_), None) => {
            // Old DBs exist, new doesn't - need migration
            Ok(true)
        }
        (Some(_), Some(_), Some(id_map)) => {
            // Check if ID map is empty (incomplete migration)
            Ok(id_map.is_empty(txn)?)
        }
        _ => Ok(false),
    }
}

pub fn migrate_from_old_format(
    env: &Env,
    txn: &mut RwTxn,
    config: HNSWConfig,
) -> VectorCoreResult<VectorCore> {
    // Open old databases for reading
    let old_vectors_db: Database<Bytes, Bytes> = env
        .database_options()
        .types::<Bytes, Bytes>()
        .name("vectors")
        .open(txn)?
        .ok_or_else(|| {
            VectorError::VectorCoreError("Old vectors database not found".to_string())
        })?;
    let old_vector_properties_db: Database<U128<BE>, Bytes> = env
        .database_options()
        .types::<U128<BE>, Bytes>()
        .name("vector_data")
        .open(txn)?
        .ok_or_else(|| {
            VectorError::VectorCoreError("Old vector_data database not found".to_string())
        })?;

    // Create new VectorCore with empty databases
    let new_core = VectorCore::new(env, txn, config)?;

    // Migrate all vectors
    let migrated_vectors = extract_old_vectors(txn, &old_vectors_db, &old_vector_properties_db)?;

    if migrated_vectors.is_empty() {
        return Ok(new_core);
    }

    // Group vectors by label and migrate each group
    let mut label_groups = HashMap::<String, Vec<OldVectorData>>::new();

    for vector_data in migrated_vectors {
        label_groups
            .entry(vector_data.label.clone())
            .or_default()
            .push(vector_data);
    }

    // Migrate each label group
    for (label, vectors) in label_groups {
        migrate_label_group(&new_core, txn, &label, vectors)?;
    }

    backup_old_databases(env, txn)?;

    Ok(new_core)
}

fn extract_old_vectors(
    txn: &RwTxn,
    old_vectors_db: &Database<Bytes, Bytes>,
    old_vector_properties_db: &Database<U128<BE>, Bytes>,
) -> VectorCoreResult<Vec<OldVectorData>> {
    let mut vectors = Vec::new();
    let mut seen_ids = std::collections::HashSet::new();

    // Iterate through old vector database
    let prefix_iter = old_vectors_db
        .prefix_iter(txn, OLD_VECTOR_PREFIX)
        .map_err(|e| {
            VectorError::VectorCoreError(format!("Failed to iterate old vectors: {}", e))
        })?;

    for result in prefix_iter {
        let (key, vector_data_bytes) = result.map_err(|e| {
            VectorError::VectorCoreError(format!("Failed to read old vector key: {}", e))
        })?;

        // Parse old key format: [v:][id][level]
        if key.len() < OLD_VECTOR_PREFIX.len() + 16 + 8 {
            panic!("Malformed key: {:?}", key);
        }

        let mut id_bytes = [0u8; 16];
        id_bytes.copy_from_slice(&key[OLD_VECTOR_PREFIX.len()..OLD_VECTOR_PREFIX.len() + 16]);
        let id = u128::from_be_bytes(id_bytes);

        // Only process level 0 vectors to avoid duplicates
        let mut level_bytes = [0u8; 8];
        level_bytes
            .copy_from_slice(&key[OLD_VECTOR_PREFIX.len() + 16..OLD_VECTOR_PREFIX.len() + 16 + 8]);
        let level = usize::from_be_bytes(level_bytes);

        // Skip if we've already processed this ID or if it's not level 0
        if level != 0 || !seen_ids.insert(id) {
            continue;
        }

        // Get properties from old properties database
        let properties_bytes = old_vector_properties_db.get(txn, &id).map_err(|e| {
            VectorError::VectorCoreError(format!("Failed to read old vector properties: {}", e))
        })?;

        // Parse old vector format
        let old_vector = parse_old_vector_format(id, level, vector_data_bytes, properties_bytes)?;

        vectors.push(old_vector);
    }

    Ok(vectors)
}

fn parse_old_vector_format(
    id: u128,
    _level: usize,
    vector_data_bytes: &[u8],
    properties_bytes: Option<&[u8]>,
) -> VectorCoreResult<OldVectorData> {
    // Parse vector data (assuming old format was f64)
    let data = convert_old_vector_data(vector_data_bytes)?;

    // Parse properties using old format deserializer
    let (label, properties, deleted) = if let Some(props_bytes) = properties_bytes {
        parse_old_properties_format(props_bytes)?
    } else {
        ("unknown".to_string(), None, false)
    };

    Ok(OldVectorData {
        id,
        label,
        data,
        properties,
        deleted,
    })
}

fn convert_old_vector_data(vector_data_bytes: &[u8]) -> VectorCoreResult<Vec<f32>> {
    // Assume f64 format
    if vector_data_bytes.len().is_multiple_of(8) {
        let f64_slice: &[f64] = bytemuck::cast_slice(vector_data_bytes);
        Ok(f64_slice.iter().map(|&x| x as f32).collect())
    } else {
        Err(VectorError::ConversionError(
            "Invalid vector data format in old database".to_string(),
        ))
    }
}

fn parse_old_properties_format(
    properties_bytes: &[u8],
) -> VectorCoreResult<(String, Option<serde_json::Value>, bool)> {
    match bincode::deserialize::<OldVectorProperties>(properties_bytes) {
        Ok(old_props) => Ok((old_props.label, old_props.properties, old_props.deleted)),
        Err(e) => {
            println!("Warning: Could not parse old properties format: {}", e);
            Ok(("migrated".to_string(), None, false))
        }
    }
}

fn migrate_label_group(
    core: &VectorCore,
    txn: &mut RwTxn,
    label: &str,
    vectors: Vec<OldVectorData>,
) -> VectorCoreResult<()> {
    if vectors.is_empty() {
        return Ok(());
    }

    println!("Migrating label '{}' with {} vectors", label, vectors.len());

    let arena = bumpalo::Bump::new();

    // Get dimension from first vector
    let dimension = vectors[0].data.len();

    // Create writer for this label
    let writer = core.get_writer_or_create_index(label, dimension, txn)?;

    // Insert all vectors for this label
    for (local_idx, old_vector) in vectors.into_iter().enumerate() {
        let local_id = local_idx as u32;

        // Skip deleted vectors during migration
        if old_vector.deleted {
            continue;
        }

        // Add to HNSW index
        writer
            .add_item(txn, local_id, &old_vector.data)
            .map_err(|e| {
                VectorError::VectorCoreError(format!("Failed to add vector to HNSW: {}", e))
            })?;

        // Convert old properties to new format
        let properties = if let Some(props_json) = old_vector.properties {
            convert_old_properties_to_new(&arena, props_json)?
        } else {
            None
        };

        // Store mappings
        core.global_to_local_id
            .write()
            .unwrap()
            .insert(old_vector.id, (local_id, label.to_string()));
        core.local_to_global_id
            .put(txn, &local_id, &old_vector.id)
            .map_err(|e| {
                VectorError::VectorCoreError(format!("Failed to store ID mapping: {}", e))
            })?;

        // Store properties in new format
        if let Some(props) = &properties {
            core.vector_properties_db
                .put(
                    txn,
                    &old_vector.id,
                    &bincode::serialize(props).map_err(|e| {
                        VectorError::ConversionError(format!(
                            "Failed to serialize properties: {}",
                            e
                        ))
                    })?,
                )
                .map_err(|e| {
                    VectorError::VectorCoreError(format!(
                        "Failed to store vector properties: {}",
                        e
                    ))
                })?;
        }

        // Update vector count
        core.label_to_index
            .read()
            .unwrap()
            .get(label)
            .unwrap()
            .num_vectors
            .fetch_add(1, atomic::Ordering::SeqCst);
    }

    // Rebuild HNSW index
    let mut rng = StdRng::from_os_rng();
    let mut builder = writer.builder(&mut rng);
    builder
        .ef_construction(core.config.ef_construct)
        .build(txn)
        .map_err(|e| VectorError::VectorCoreError(format!("Failed to build HNSW index: {}", e)))?;

    Ok(())
}

fn convert_old_properties_to_new<'arena>(
    arena: &'arena bumpalo::Bump,
    old_props: serde_json::Value,
) -> VectorCoreResult<Option<ImmutablePropertiesMap<'arena>>> {
    // Convert from serde_json::Value to ImmutablePropertiesMap
    if let serde_json::Value::Object(map) = old_props {
        let mut new_props = HashMap::new();

        for (key, value) in map {
            let helix_value = match value {
                serde_json::Value::String(s) => Value::String(s),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Value::I64(i)
                    } else if let Some(f) = n.as_f64() {
                        Value::F64(f)
                    } else {
                        continue;
                    }
                }
                serde_json::Value::Bool(b) => Value::Boolean(b),
                _ => continue, // Skip complex types for now
            };
            new_props.insert(arena.alloc_str(&key), helix_value);
        }

        let props_vec: Vec<(&str, Value)> =
            new_props.into_iter().map(|(k, v)| (k as &str, v)).collect();
        Ok(Some(ImmutablePropertiesMap::new(
            props_vec.len(),
            props_vec.into_iter(),
            arena,
        )))
    } else {
        Ok(None)
    }
}

fn backup_old_databases(env: &Env, txn: &mut RwTxn) -> VectorCoreResult<()> {
    // Note: LMDB doesn't support database renaming directly.
    // Instead, we'll clear them after successful migration

    // Clear old databases after successful migration
    if let Some(old_vectors_db) = env
        .database_options()
        .types::<Bytes, Bytes>()
        .name("vectors")
        .open(txn)?
    {
        old_vectors_db.clear(txn).map_err(|e| {
            VectorError::VectorCoreError(format!("Failed to clear old vectors database: {}", e))
        })?;
    }

    if let Some(old_edges_db) = env
        .database_options()
        .types::<Bytes, Unit>()
        .name("hnsw_out_nodes")
        .open(txn)?
    {
        old_edges_db.clear(txn).map_err(|e| {
            VectorError::VectorCoreError(format!("Failed to clear old edges database: {}", e))
        })?;
    }

    Ok(())
}

#[cfg(test)]
mod migration_tests {
    use super::*;
    use crate::helix_engine::vector_core::HNSWConfig;
    use heed3::EnvOpenOptions;

    use tempfile::tempdir;

    #[test]
    fn test_migration_detection() {
        let dir = tempdir().unwrap();
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(1024 * 1024 * 1024)
                .max_dbs(10)
                .open(dir.path())
                .unwrap()
        };

        let mut txn = env.write_txn().unwrap();

        // Initially no migration needed
        assert!(!needs_migration_from_old_format(&env, &txn).unwrap());

        // Create old databases
        let _old_vectors = env
            .database_options()
            .types::<Bytes, Bytes>()
            .name("vectors")
            .create(&mut txn)
            .unwrap();
        let _old_edges = env
            .database_options()
            .types::<Bytes, Unit>()
            .name("hnsw_out_nodes")
            .create(&mut txn)
            .unwrap();

        // Now migration should be needed
        assert!(needs_migration_from_old_format(&env, &txn).unwrap());

        txn.commit().unwrap();
    }

    #[test]
    fn test_old_vector_data_conversion() {
        // Test f64 to f32 conversion
        let f64_data: Vec<f64> = vec![1.0, 2.0, 3.0];
        let f64_bytes = bytemuck::cast_slice::<f64, u8>(&f64_data);
        let converted = convert_old_vector_data(f64_bytes).unwrap();
        assert_eq!(converted, vec![1.0f32, 2.0f32, 3.0f32]);
    }

    #[test]
    fn test_old_properties_conversion() {
        let arena = bumpalo::Bump::new();

        // Test simple JSON object conversion
        let json_value = serde_json::json!({
            "name": "test_vector",
            "count": 42,
            "score": 0.85,
            "active": true
        });

        let converted = convert_old_properties_to_new(&arena, json_value).unwrap();
        assert!(converted.is_some());

        let props = converted.unwrap();
        assert_eq!(props.len(), 4);

        // Verify the properties were converted correctly
        assert!(props.get("name").is_some());
        assert!(props.get("count").is_some());
        assert!(props.get("score").is_some());
        assert!(props.get("active").is_some());
    }

    #[test]
    fn test_empty_migration() {
        let dir = tempdir().unwrap();
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(1024 * 1024 * 1024)
                .max_dbs(10)
                .open(dir.path())
                .unwrap()
        };

        let mut txn = env.write_txn().unwrap();
        let config = HNSWConfig::new(None, None, None);

        // Create old empty databases
        let _old_vectors = env
            .database_options()
            .types::<Bytes, Bytes>()
            .name("vectors")
            .create(&mut txn)
            .unwrap();
        let _old_data = env
            .database_options()
            .types::<U128<BE>, Bytes>()
            .name("vector_data")
            .create(&mut txn)
            .unwrap();
        let _old_edges = env
            .database_options()
            .types::<Bytes, Unit>()
            .name("hnsw_out_nodes")
            .create(&mut txn)
            .unwrap();

        // Migration should succeed even with empty databases
        let result = migrate_from_old_format(&env, &mut txn, config);
        assert!(result.is_ok());

        let vector_core = result.unwrap();
        assert_eq!(vector_core.num_inserted_vectors(), 0);

        txn.commit().unwrap();
    }
}
