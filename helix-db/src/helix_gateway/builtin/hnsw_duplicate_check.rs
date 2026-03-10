use std::collections::HashMap;
use std::sync::Arc;

use sonic_rs::{json, JsonValueTrait};

use crate::helix_engine::types::GraphError;
use crate::helix_engine::vector_core::hnsw::HNSW;
use crate::helix_gateway::router::router::{Handler, HandlerInput, HandlerSubmission};
use crate::protocol;

/// Check for duplicate vectors and nodes in HNSW index.
///
/// This endpoint checks for:
/// 1. Duplicate embeddings - same vector data with different IDs
/// 2. Duplicate codes - same code property appearing multiple times
///
/// Request body (optional):
/// - fingerprint_dims: Number of dimensions to use for initial fingerprint (default: 32)
/// - max_duplicates: Maximum duplicates to report (default: 100)
///
/// Example: {"fingerprint_dims": 64, "max_duplicates": 50}
pub fn hnsw_duplicate_check_inner(input: HandlerInput) -> Result<protocol::Response, GraphError> {
    eprintln!("[HNSWDuplicateCheck] Starting...");

    // Parse options from request body
    let (fingerprint_dims, max_duplicates): (usize, usize) = if input.request.body.is_empty() {
        (32, 100)
    } else {
        match sonic_rs::from_slice::<sonic_rs::Value>(&input.request.body) {
            Ok(val) => {
                let dims = val
                    .get("fingerprint_dims")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(32) as usize;
                let max_dupes = val
                    .get("max_duplicates")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(100) as usize;
                (dims, max_dupes)
            }
            Err(_) => (32, 100),
        }
    };

    eprintln!("[HNSWDuplicateCheck] Using fingerprint_dims={}, max_duplicates={}", fingerprint_dims, max_duplicates);

    let db = Arc::clone(&input.graph.storage);

    // Step 1: Get all vector IDs
    eprintln!("[HNSWDuplicateCheck] Collecting vector IDs...");
    let vector_ids: Vec<u128> = {
        let txn = db.graph_env.read_txn().map_err(GraphError::from)?;
        db.vectors
            .get_all_vector_ids(&txn)
            .map_err(|e| GraphError::New(format!("Failed to get vector IDs: {}", e)))?
    };
    let total_vectors = vector_ids.len();
    eprintln!("[HNSWDuplicateCheck] Found {} vectors", total_vectors);

    // Step 2: Check for duplicates
    // Use a fingerprint (first N dimensions) to bucket potential duplicates
    // Then compare full vectors within each bucket

    // Map: fingerprint -> list of (id, code)
    let mut fingerprint_map: HashMap<Vec<u64>, Vec<(u128, String)>> = HashMap::new();
    // Map: code -> list of ids
    let mut code_map: HashMap<String, Vec<u128>> = HashMap::new();

    let mut checked = 0;
    let mut errors = 0;

    {
        let txn = db.graph_env.read_txn().map_err(GraphError::from)?;

        for (i, &vector_id) in vector_ids.iter().enumerate() {
            if i % 5000 == 0 {
                eprintln!("[HNSWDuplicateCheck] Checking: {}/{} ({:.1}%)",
                    i, total_vectors, (i as f64 / total_vectors as f64) * 100.0);
            }

            let arena = bumpalo::Bump::new();

            // Get vector with full data
            match db.vectors.get_full_vector(&txn, vector_id, &arena) {
                Ok(vector) => {
                    checked += 1;

                    // Create fingerprint from first N dimensions
                    let fingerprint: Vec<u64> = vector.data
                        .iter()
                        .take(fingerprint_dims)
                        .map(|&f| f.to_bits())
                        .collect();

                    // Get code property
                    let code = vector.properties
                        .as_ref()
                        .and_then(|props| props.get("code"))
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "unknown".to_string());

                    // Add to fingerprint map
                    fingerprint_map
                        .entry(fingerprint)
                        .or_insert_with(Vec::new)
                        .push((vector_id, code.clone()));

                    // Add to code map
                    code_map
                        .entry(code)
                        .or_insert_with(Vec::new)
                        .push(vector_id);
                }
                Err(e) => {
                    eprintln!("[HNSWDuplicateCheck] Error getting vector {}: {}", vector_id, e);
                    errors += 1;
                }
            }
        }
    }

    eprintln!("[HNSWDuplicateCheck] Analyzed {} vectors, {} errors", checked, errors);

    // Step 3: Find duplicates

    // Duplicate embeddings (same fingerprint = potential duplicate)
    let mut duplicate_embeddings: Vec<sonic_rs::Value> = Vec::new();
    for (fingerprint, entries) in fingerprint_map.iter() {
        if entries.len() > 1 && duplicate_embeddings.len() < max_duplicates {
            // Multiple vectors with same fingerprint
            let ids: Vec<String> = entries.iter()
                .map(|(id, code)| format!("{}:{}", uuid_to_string(*id), code))
                .collect();
            duplicate_embeddings.push(json!({
                "fingerprint_hash": format!("{:?}", &fingerprint[..fingerprint.len().min(4)]),
                "count": entries.len(),
                "vectors": ids
            }));
        }
    }

    // Duplicate codes (same code property)
    let mut duplicate_codes: Vec<sonic_rs::Value> = Vec::new();
    for (code, ids) in code_map.iter() {
        if ids.len() > 1 && duplicate_codes.len() < max_duplicates {
            let id_strings: Vec<String> = ids.iter()
                .map(|&id| uuid_to_string(id))
                .collect();
            duplicate_codes.push(json!({
                "code": code,
                "count": ids.len(),
                "vector_ids": id_strings
            }));
        }
    }

    let duplicate_embedding_count = duplicate_embeddings.len();
    let duplicate_code_count = duplicate_codes.len();

    eprintln!("[HNSWDuplicateCheck] Found {} potential duplicate embeddings, {} duplicate codes",
        duplicate_embedding_count, duplicate_code_count);

    let health = if duplicate_embedding_count == 0 && duplicate_code_count == 0 {
        "healthy"
    } else {
        "has_duplicates"
    };

    Ok(protocol::Response {
        body: sonic_rs::to_vec(&json!({
            "status": health,
            "total_vectors": total_vectors,
            "checked": checked,
            "errors": errors,
            "duplicate_embeddings_found": duplicate_embedding_count,
            "duplicate_codes_found": duplicate_code_count,
            "duplicate_embeddings": duplicate_embeddings,
            "duplicate_codes": duplicate_codes
        }))
        .map_err(|e| GraphError::New(e.to_string()))?,
        fmt: Default::default(),
    })
}

fn uuid_to_string(id: u128) -> String {
    let bytes = id.to_be_bytes();
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5],
        bytes[6], bytes[7],
        bytes[8], bytes[9],
        bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
    )
}

inventory::submit! {
    HandlerSubmission(
        Handler::new("HNSWDuplicateCheck", hnsw_duplicate_check_inner, false)
    )
}
