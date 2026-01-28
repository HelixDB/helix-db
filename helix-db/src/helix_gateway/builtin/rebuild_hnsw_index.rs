use std::sync::Arc;

use sonic_rs::json;
use sonic_rs::JsonValueTrait;

use crate::helix_engine::types::GraphError;
use crate::helix_engine::vector_core::hnsw::HNSW;
use crate::helix_engine::vector_core::vector_core::ENTRY_POINT_KEY;
use crate::helix_gateway::router::router::{Handler, HandlerInput, HandlerSubmission};
use crate::protocol;

const DEFAULT_BATCH_SIZE: usize = 5;

/// Rebuild the HNSW index by reconnecting all vectors.
/// This fixes graph fragmentation caused by deletions and re-insertions.
///
/// Request body (optional):
/// - batch_size: Number of vectors to process per transaction (default: 5)
///
/// Example: {"batch_size": 10}
pub fn rebuild_hnsw_index_inner(input: HandlerInput) -> Result<protocol::Response, GraphError> {
    eprintln!("[RebuildHNSWIndex] Starting rebuild...");

    // Parse batch_size from request body (default to DEFAULT_BATCH_SIZE)
    let batch_size: usize = if input.request.body.is_empty() {
        DEFAULT_BATCH_SIZE
    } else {
        match sonic_rs::from_slice::<sonic_rs::Value>(&input.request.body) {
            Ok(val) => val
                .get("batch_size")
                .and_then(|v| v.as_u64())
                .map(|v| v as usize)
                .unwrap_or(DEFAULT_BATCH_SIZE),
            Err(_) => DEFAULT_BATCH_SIZE,
        }
    };

    // Ensure batch_size is at least 1
    let batch_size = batch_size.max(1);
    eprintln!("[RebuildHNSWIndex] Using batch size: {}", batch_size);

    let db = Arc::clone(&input.graph.storage);

    // Step 1: Get vector IDs only (memory-efficient, doesn't load vector data)
    eprintln!("[RebuildHNSWIndex] Collecting vector IDs...");
    let vector_ids: Vec<u128> = {
        let txn = db.graph_env.read_txn().map_err(GraphError::from)?;
        db.vectors
            .get_all_vector_ids(&txn)
            .map_err(|e| GraphError::New(format!("Failed to get vector IDs: {}", e)))?
    };
    let vector_count = vector_ids.len();
    eprintln!("[RebuildHNSWIndex] Found {} vectors", vector_count);

    if vector_count == 0 {
        return Ok(protocol::Response {
            body: sonic_rs::to_vec(&json!({
                "status": "success",
                "message": "No vectors to rebuild",
                "vectors_rebuilt": 0
            }))
            .map_err(|e| GraphError::New(e.to_string()))?,
            fmt: Default::default(),
        });
    }

    // Step 2: Clear all HNSW edges
    {
        let mut txn = db.graph_env.write_txn().map_err(GraphError::from)?;
        eprintln!("[RebuildHNSWIndex] Clearing HNSW edges...");
        db.vectors
            .edges_db
            .clear(&mut txn)
            .map_err(|e| GraphError::New(format!("Failed to clear edges: {}", e)))?;

        eprintln!("[RebuildHNSWIndex] Clearing entry point...");
        let _ = db.vectors.vectors_db.delete(&mut txn, ENTRY_POINT_KEY);
        txn.commit().map_err(GraphError::from)?;
    }

    // Step 3: Reconnect vectors in batches (fresh arena per batch to control memory)
    eprintln!("[RebuildHNSWIndex] Reconnecting {} vectors in batches of {}...", vector_count, batch_size);

    for (batch_idx, chunk) in vector_ids.chunks(batch_size).enumerate() {
        let processed = batch_idx * batch_size;
        if processed % 500 == 0 {
            eprintln!("[RebuildHNSWIndex] Progress: {}/{} ({:.1}%)",
                processed, vector_count, (processed as f64 / vector_count as f64) * 100.0);
        }

        // Fresh arena for each batch to prevent memory growth
        let arena = bumpalo::Bump::new();
        let mut txn = db.graph_env.write_txn().map_err(GraphError::from)?;

        for &vector_id in chunk {
            match db.vectors.get_full_vector(&txn, vector_id, &arena) {
                Ok(v) => {
                    db.vectors
                        .reconnect_vector::<fn(&_, &_) -> bool>(&mut txn, &v, &arena)
                        .map_err(|e| GraphError::New(format!("Failed to reconnect vector {}: {}", vector_id, e)))?;
                }
                Err(e) => {
                    eprintln!("[RebuildHNSWIndex] Warning: skipping vector {}: {}", vector_id, e);
                }
            }
        }

        txn.commit().map_err(GraphError::from)?;
        // Arena dropped here, memory freed
    }

    eprintln!("[RebuildHNSWIndex] Rebuild complete! {} vectors reconnected", vector_count);
    Ok(protocol::Response {
        body: sonic_rs::to_vec(&json!({
            "status": "success",
            "vectors_rebuilt": vector_count,
            "batch_size": batch_size
        }))
        .map_err(|e| GraphError::New(e.to_string()))?,
        fmt: Default::default(),
    })
}

inventory::submit! {
    HandlerSubmission(
        Handler::new("RebuildHNSWIndex", rebuild_hnsw_index_inner, true)
    )
}
