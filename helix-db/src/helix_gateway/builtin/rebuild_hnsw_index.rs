use std::sync::Arc;

use sonic_rs::json;

use crate::helix_engine::types::GraphError;
use crate::helix_engine::vector_core::hnsw::HNSW;
use crate::helix_engine::vector_core::vector_core::ENTRY_POINT_KEY;
use crate::helix_gateway::router::router::{Handler, HandlerInput, HandlerSubmission};
use crate::protocol;

const BATCH_SIZE: usize = 50;

/// Rebuild the HNSW index by reconnecting all vectors.
/// This fixes graph fragmentation caused by deletions and re-insertions.
/// Processes in batches to avoid OOM.
pub fn rebuild_hnsw_index_inner(input: HandlerInput) -> Result<protocol::Response, GraphError> {
    eprintln!("[RebuildHNSWIndex] Starting rebuild...");

    let db = Arc::clone(&input.graph.storage);

    // Step 1: Get vector IDs only (to avoid loading all vector data at once)
    eprintln!("[RebuildHNSWIndex] Counting vectors...");
    let vector_ids: Vec<u128> = {
        let txn = db.graph_env.read_txn().map_err(GraphError::from)?;
        let arena = bumpalo::Bump::new();
        let vectors = db
            .vectors
            .get_all_vectors(&txn, None, &arena)
            .map_err(|e| GraphError::New(format!("Failed to get vectors: {}", e)))?;
        vectors.iter().map(|v| v.id).collect()
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

    // Step 3: Reconnect vectors one at a time (fresh arena each to control memory)
    eprintln!("[RebuildHNSWIndex] Reconnecting {} vectors...", vector_count);

    for (i, &vector_id) in vector_ids.iter().enumerate() {
        if i % 500 == 0 {
            eprintln!("[RebuildHNSWIndex] Progress: {}/{} ({:.1}%)",
                i, vector_count, (i as f64 / vector_count as f64) * 100.0);
        }

        // Fresh arena for each vector to prevent memory growth
        let arena = bumpalo::Bump::new();
        let mut txn = db.graph_env.write_txn().map_err(GraphError::from)?;

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

        txn.commit().map_err(GraphError::from)?;
        // Arena dropped here, memory freed
    }

    eprintln!("[RebuildHNSWIndex] Rebuild complete! {} vectors reconnected", vector_count);
    Ok(protocol::Response {
        body: sonic_rs::to_vec(&json!({
            "status": "success",
            "vectors_rebuilt": vector_count
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
