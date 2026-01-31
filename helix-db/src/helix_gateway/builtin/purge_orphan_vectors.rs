use std::sync::Arc;

use sonic_rs::{json, JsonValueTrait};

use crate::helix_engine::types::GraphError;
use crate::helix_engine::vector_core::hnsw::HNSW;
use crate::helix_gateway::router::router::{Handler, HandlerInput, HandlerSubmission};
use crate::protocol;

/// Purge orphan vectors - vectors that exist in HNSW but have no corresponding node in the graph.
/// This cleans up leftover vectors from deleted nodes.
///
/// Request body (optional):
/// - dry_run: If true, only count orphans without deleting (default: false)
///
/// Example: {"dry_run": true}
pub fn purge_orphan_vectors_inner(input: HandlerInput) -> Result<protocol::Response, GraphError> {
    eprintln!("[PurgeOrphanVectors] Starting...");

    // Parse dry_run from request body
    let dry_run: bool = if input.request.body.is_empty() {
        false
    } else {
        match sonic_rs::from_slice::<sonic_rs::Value>(&input.request.body) {
            Ok(val) => val
                .get("dry_run")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            Err(_) => false,
        }
    };

    if dry_run {
        eprintln!("[PurgeOrphanVectors] DRY RUN - no vectors will be deleted");
    }

    let db = Arc::clone(&input.graph.storage);

    // Step 1: Get all vector IDs
    eprintln!("[PurgeOrphanVectors] Collecting vector IDs...");
    let vector_ids: Vec<u128> = {
        let txn = db.graph_env.read_txn().map_err(GraphError::from)?;
        db.vectors
            .get_all_vector_ids(&txn)
            .map_err(|e| GraphError::New(format!("Failed to get vector IDs: {}", e)))?
    };
    let total_vectors = vector_ids.len();
    eprintln!("[PurgeOrphanVectors] Found {} vectors", total_vectors);

    // Step 2: Find orphan vectors (no corresponding node in nodes_db)
    eprintln!("[PurgeOrphanVectors] Checking for orphans...");
    let mut orphan_ids: Vec<u128> = Vec::new();

    {
        let txn = db.graph_env.read_txn().map_err(GraphError::from)?;

        for (i, &vector_id) in vector_ids.iter().enumerate() {
            if i % 5000 == 0 {
                eprintln!("[PurgeOrphanVectors] Checking: {}/{} ({:.1}%)",
                    i, total_vectors, (i as f64 / total_vectors as f64) * 100.0);
            }

            // Check if node exists in nodes_db for this vector ID
            match db.nodes_db.get(&txn, &vector_id) {
                Ok(Some(_)) => {
                    // Node exists, not an orphan
                }
                Ok(None) | Err(_) => {
                    // No node found, this is an orphan
                    orphan_ids.push(vector_id);
                }
            }
        }
    }

    let orphan_count = orphan_ids.len();
    eprintln!("[PurgeOrphanVectors] Found {} orphan vectors", orphan_count);

    if dry_run {
        eprintln!("[PurgeOrphanVectors] DRY RUN complete. Would delete {} orphans.", orphan_count);
        return Ok(protocol::Response {
            body: sonic_rs::to_vec(&json!({
                "status": "dry_run",
                "total_vectors": total_vectors,
                "orphan_count": orphan_count,
                "deleted": 0
            }))
            .map_err(|e| GraphError::New(e.to_string()))?,
            fmt: Default::default(),
        });
    }

    if orphan_count == 0 {
        eprintln!("[PurgeOrphanVectors] No orphans to purge.");
        return Ok(protocol::Response {
            body: sonic_rs::to_vec(&json!({
                "status": "success",
                "total_vectors": total_vectors,
                "orphan_count": 0,
                "deleted": 0
            }))
            .map_err(|e| GraphError::New(e.to_string()))?,
            fmt: Default::default(),
        });
    }

    // Step 3: Delete orphan vectors using the HNSW delete method
    eprintln!("[PurgeOrphanVectors] Deleting {} orphan vectors...", orphan_count);
    let mut deleted = 0;
    let mut errors = 0;

    for (i, &orphan_id) in orphan_ids.iter().enumerate() {
        if i % 500 == 0 {
            eprintln!("[PurgeOrphanVectors] Deleting: {}/{} ({:.1}%)",
                i, orphan_count, (i as f64 / orphan_count as f64) * 100.0);
        }

        let arena = bumpalo::Bump::new();
        let mut txn = db.graph_env.write_txn().map_err(GraphError::from)?;

        // Use the HNSW delete method which marks the vector as deleted
        match db.vectors.delete(&mut txn, orphan_id, &arena) {
            Ok(_) => {
                match txn.commit() {
                    Ok(_) => deleted += 1,
                    Err(e) => {
                        eprintln!("[PurgeOrphanVectors] Error committing delete for {}: {}", orphan_id, e);
                        errors += 1;
                    }
                }
            }
            Err(e) => {
                eprintln!("[PurgeOrphanVectors] Error deleting vector {}: {}", orphan_id, e);
                errors += 1;
            }
        }
    }

    eprintln!("[PurgeOrphanVectors] Purge complete! Deleted: {}, Errors: {}", deleted, errors);

    Ok(protocol::Response {
        body: sonic_rs::to_vec(&json!({
            "status": "success",
            "total_vectors": total_vectors,
            "orphan_count": orphan_count,
            "deleted": deleted,
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
