use std::sync::Arc;

use sonic_rs::{json, JsonValueTrait};

use crate::helix_engine::types::GraphError;
use crate::helix_engine::vector_core::hnsw::HNSW;
use crate::helix_gateway::router::router::{Handler, HandlerInput, HandlerSubmission};
use crate::protocol;

/// Purge orphan vectors - vectors that exist in HNSW but have no corresponding properties.
/// This cleans up leftover vectors from deleted entries.
///
/// An orphan vector is one that:
/// - Exists in the HNSW index (vectors_db)
/// - But has NO entry in vector_properties_db (no properties/metadata)
///
/// Request body (optional):
/// - dry_run: If true, only count without deleting (default: false)
/// - purge_soft_deleted: If true, also hard-delete soft-deleted vectors (default: false)
///
/// Examples:
/// - {"dry_run": true} - Count orphans and soft-deleted without deleting
/// - {"purge_soft_deleted": true} - Hard delete all soft-deleted vectors
/// - {} - Delete orphans only (default behavior)
pub fn purge_orphan_vectors_inner(input: HandlerInput) -> Result<protocol::Response, GraphError> {
    eprintln!("[PurgeOrphanVectors] Starting...");

    // Parse options from request body
    let (dry_run, purge_soft_deleted): (bool, bool) = if input.request.body.is_empty() {
        (false, false)
    } else {
        match sonic_rs::from_slice::<sonic_rs::Value>(&input.request.body) {
            Ok(val) => {
                let dry_run = val
                    .get("dry_run")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let purge_soft_deleted = val
                    .get("purge_soft_deleted")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                (dry_run, purge_soft_deleted)
            }
            Err(_) => (false, false),
        }
    };

    if dry_run {
        eprintln!("[PurgeOrphanVectors] DRY RUN - no vectors will be deleted");
    }
    if purge_soft_deleted {
        eprintln!("[PurgeOrphanVectors] Will also purge soft-deleted vectors");
    }

    let db = Arc::clone(&input.graph.storage);

    // Step 1: Get all vector IDs from the HNSW index
    eprintln!("[PurgeOrphanVectors] Collecting vector IDs from HNSW index...");
    let vector_ids: Vec<u128> = {
        let txn = db.graph_env.read_txn().map_err(GraphError::from)?;
        db.vectors
            .get_all_vector_ids(&txn)
            .map_err(|e| GraphError::New(format!("Failed to get vector IDs: {}", e)))?
    };
    let total_vectors = vector_ids.len();
    eprintln!("[PurgeOrphanVectors] Found {} vectors in HNSW index", total_vectors);

    // Step 2: Find orphan vectors and soft-deleted vectors
    eprintln!("[PurgeOrphanVectors] Checking for orphans and soft-deleted vectors...");
    let mut orphan_ids: Vec<u128> = Vec::new();
    let mut soft_deleted_ids: Vec<u128> = Vec::new();

    {
        let txn = db.graph_env.read_txn().map_err(GraphError::from)?;
        let arena = bumpalo::Bump::new();

        for (i, &vector_id) in vector_ids.iter().enumerate() {
            if i % 5000 == 0 {
                eprintln!("[PurgeOrphanVectors] Checking: {}/{} ({:.1}%)",
                    i, total_vectors, (i as f64 / total_vectors as f64) * 100.0);
            }

            // Check if vector has properties in vector_properties_db
            match db.vectors.get_vector_properties(&txn, vector_id, &arena) {
                Ok(Some(props)) => {
                    // Vector has properties
                    if props.deleted {
                        // Vector is marked as soft-deleted
                        soft_deleted_ids.push(vector_id);
                    }
                    // else: Valid vector with properties, not an orphan
                }
                Ok(None) => {
                    // No properties found - this is an orphan
                    orphan_ids.push(vector_id);
                }
                Err(_) => {
                    // Error getting properties (VectorDeleted error) - it's soft-deleted
                    soft_deleted_ids.push(vector_id);
                }
            }
        }
    }

    let deleted_count = soft_deleted_ids.len();
    eprintln!("[PurgeOrphanVectors] Found {} vectors marked as soft-deleted", deleted_count);

    let orphan_count = orphan_ids.len();
    eprintln!("[PurgeOrphanVectors] Found {} orphan vectors", orphan_count);

    // Determine what to purge
    let to_purge_count = if purge_soft_deleted {
        orphan_count + deleted_count
    } else {
        orphan_count
    };

    if dry_run {
        let msg = if purge_soft_deleted {
            format!("Would delete {} orphans + {} soft-deleted = {} total", orphan_count, deleted_count, to_purge_count)
        } else {
            format!("Would delete {} orphans", orphan_count)
        };
        eprintln!("[PurgeOrphanVectors] DRY RUN complete. {}", msg);
        return Ok(protocol::Response {
            body: sonic_rs::to_vec(&json!({
                "status": "dry_run",
                "total_vectors": total_vectors,
                "orphan_count": orphan_count,
                "soft_deleted_count": deleted_count,
                "would_delete": to_purge_count,
                "deleted": 0
            }))
            .map_err(|e| GraphError::New(e.to_string()))?,
            fmt: Default::default(),
        });
    }

    if to_purge_count == 0 {
        eprintln!("[PurgeOrphanVectors] Nothing to purge.");
        return Ok(protocol::Response {
            body: sonic_rs::to_vec(&json!({
                "status": "success",
                "total_vectors": total_vectors,
                "orphan_count": 0,
                "soft_deleted_count": deleted_count,
                "deleted": 0
            }))
            .map_err(|e| GraphError::New(e.to_string()))?,
            fmt: Default::default(),
        });
    }

    // Step 3: Hard delete vectors
    let mut deleted = 0;
    let mut errors = 0;

    // Delete orphan vectors (hard delete - they have no properties anyway)
    if !orphan_ids.is_empty() {
        eprintln!("[PurgeOrphanVectors] Hard deleting {} orphan vectors...", orphan_count);
        for (i, &orphan_id) in orphan_ids.iter().enumerate() {
            if i % 500 == 0 {
                eprintln!("[PurgeOrphanVectors] Deleting orphans: {}/{} ({:.1}%)",
                    i, orphan_count, (i as f64 / orphan_count as f64) * 100.0);
            }

            let mut txn = db.graph_env.write_txn().map_err(GraphError::from)?;
            match db.vectors.hard_delete(&mut txn, orphan_id) {
                Ok(_) => {
                    match txn.commit() {
                        Ok(_) => deleted += 1,
                        Err(e) => {
                            eprintln!("[PurgeOrphanVectors] Error committing delete for orphan {}: {}", orphan_id, e);
                            errors += 1;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[PurgeOrphanVectors] Error deleting orphan {}: {}", orphan_id, e);
                    errors += 1;
                }
            }
        }
        eprintln!("[PurgeOrphanVectors] Orphan deletion complete.");
    }

    // Delete soft-deleted vectors if requested
    let mut soft_deleted_purged = 0;
    if purge_soft_deleted && !soft_deleted_ids.is_empty() {
        eprintln!("[PurgeOrphanVectors] Hard deleting {} soft-deleted vectors...", deleted_count);
        for (i, &soft_id) in soft_deleted_ids.iter().enumerate() {
            if i % 500 == 0 {
                eprintln!("[PurgeOrphanVectors] Deleting soft-deleted: {}/{} ({:.1}%)",
                    i, deleted_count, (i as f64 / deleted_count as f64) * 100.0);
            }

            let mut txn = db.graph_env.write_txn().map_err(GraphError::from)?;
            match db.vectors.hard_delete(&mut txn, soft_id) {
                Ok(_) => {
                    match txn.commit() {
                        Ok(_) => {
                            deleted += 1;
                            soft_deleted_purged += 1;
                        }
                        Err(e) => {
                            eprintln!("[PurgeOrphanVectors] Error committing delete for soft-deleted {}: {}", soft_id, e);
                            errors += 1;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[PurgeOrphanVectors] Error deleting soft-deleted {}: {}", soft_id, e);
                    errors += 1;
                }
            }
        }
        eprintln!("[PurgeOrphanVectors] Soft-deleted purge complete.");
    }

    eprintln!("[PurgeOrphanVectors] Purge complete! Deleted: {} (orphans: {}, soft-deleted: {}), Errors: {}",
        deleted, orphan_count, soft_deleted_purged, errors);

    Ok(protocol::Response {
        body: sonic_rs::to_vec(&json!({
            "status": "success",
            "total_vectors": total_vectors,
            "orphan_count": orphan_count,
            "soft_deleted_count": deleted_count,
            "deleted": deleted,
            "soft_deleted_purged": soft_deleted_purged,
            "errors": errors
        }))
        .map_err(|e| GraphError::New(e.to_string()))?,
        fmt: Default::default(),
    })
}

inventory::submit! {
    HandlerSubmission(
        Handler::new("PurgeOrphanVectors", purge_orphan_vectors_inner, true)
    )
}
