use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use rand::seq::SliceRandom;
use sonic_rs::{json, JsonValueTrait};

use crate::helix_engine::types::GraphError;
use crate::helix_engine::vector_core::hnsw::HNSW;
use crate::helix_engine::vector_core::vector_core::ENTRY_POINT_KEY;
use crate::helix_gateway::router::router::{Handler, HandlerInput, HandlerSubmission};
use crate::protocol;
use crate::utils::id::ID;

const DEFAULT_SAMPLE_SIZE: usize = 1000;
const DEFAULT_SEARCH_K: usize = 10;
const DEFAULT_LABEL: &str = "";

/// HNSW Diagnostics endpoint - checks graph health and identifies unreachable vectors.
///
/// Request body (JSON):
/// - mode: "quick" (sample-based) or "full" (BFS traversal), default: "quick"
/// - sample_size: Number of vectors to check in quick mode, default: 1000
/// - label: Vector label to use for searches, default: ""
///
/// Example: {"mode": "quick", "sample_size": 100, "label": "ICDCode"}
pub fn hnsw_diagnostics_inner(input: HandlerInput) -> Result<protocol::Response, GraphError> {
    let start_time = Instant::now();
    eprintln!("[HNSWDiagnostics] Starting diagnostics...");

    // Parse request parameters
    let (mode, sample_size, label) = if input.request.body.is_empty() {
        ("quick".to_string(), DEFAULT_SAMPLE_SIZE, DEFAULT_LABEL.to_string())
    } else {
        match sonic_rs::from_slice::<sonic_rs::Value>(&input.request.body) {
            Ok(val) => {
                let mode = val
                    .get("mode")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "quick".to_string());
                let sample_size = val
                    .get("sample_size")
                    .and_then(|v| v.as_u64())
                    .map(|v| v as usize)
                    .unwrap_or(DEFAULT_SAMPLE_SIZE);
                let label = val
                    .get("label")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| DEFAULT_LABEL.to_string());
                (mode, sample_size, label)
            }
            Err(_) => ("quick".to_string(), DEFAULT_SAMPLE_SIZE, DEFAULT_LABEL.to_string()),
        }
    };

    eprintln!("[HNSWDiagnostics] Mode: {}, Sample size: {}, Label: {}", mode, sample_size, label);

    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().map_err(GraphError::from)?;
    let arena = bumpalo::Bump::new();

    // Get all vector IDs
    let vector_ids: Vec<u128> = db
        .vectors
        .get_all_vector_ids(&txn)
        .map_err(|e| GraphError::New(format!("Failed to get vector IDs: {}", e)))?;

    let total_vectors = vector_ids.len();
    eprintln!("[HNSWDiagnostics] Total vectors: {}", total_vectors);

    if total_vectors == 0 {
        return Ok(protocol::Response {
            body: sonic_rs::to_vec(&json!({
                "entry_point": null,
                "total_vectors": 0,
                "total_edges": 0,
                "checked_vectors": 0,
                "unreachable_vectors": [],
                "unreachable_count": 0,
                "health_status": "healthy",
                "mode": mode,
                "diagnostics": {
                    "sample_size": 0,
                    "duration_ms": start_time.elapsed().as_millis()
                }
            }))
            .map_err(|e| GraphError::New(e.to_string()))?,
            fmt: Default::default(),
        });
    }

    // Get entry point info
    let entry_point_info = match db.vectors.vectors_db.get(&txn, ENTRY_POINT_KEY) {
        Ok(Some(ep_bytes)) => {
            let mut arr = [0u8; 16];
            let len = std::cmp::min(ep_bytes.len(), 16);
            arr[..len].copy_from_slice(&ep_bytes[..len]);
            let ep_id = u128::from_be_bytes(arr);

            // Get entry point level
            let ep_level = match db.vectors.get_full_vector(&txn, ep_id, &arena) {
                Ok(v) => v.level,
                Err(_) => 0,
            };

            Some((ID::from(ep_id).stringify(), ep_level))
        }
        _ => None,
    };

    // Count total edges (approximate by counting edge keys)
    let total_edges = db.vectors.edges_db.len(&txn).unwrap_or(0);

    let (unreachable_ids, checked_count) = match mode.as_str() {
        "full" => run_full_mode_diagnostics(&db.vectors, &txn, &vector_ids, &arena)?,
        _ => run_quick_mode_diagnostics(&db.vectors, &txn, &vector_ids, sample_size, &label, &arena)?,
    };

    let unreachable_count = unreachable_ids.len();
    let unreachable_percentage = if checked_count > 0 {
        (unreachable_count as f64 / checked_count as f64) * 100.0
    } else {
        0.0
    };

    let health_status = if entry_point_info.is_none() {
        "broken"
    } else if unreachable_percentage > 5.0 {
        "broken"
    } else if unreachable_count > 0 {
        "degraded"
    } else {
        "healthy"
    };

    let duration_ms = start_time.elapsed().as_millis();
    eprintln!(
        "[HNSWDiagnostics] Complete. Checked: {}, Unreachable: {}, Status: {}, Duration: {}ms",
        checked_count, unreachable_count, health_status, duration_ms
    );

    // Convert unreachable IDs to strings
    let unreachable_strings: Vec<String> = unreachable_ids
        .iter()
        .map(|id| ID::from(*id).stringify())
        .collect();

    let entry_point_json = match entry_point_info {
        Some((id, level)) => json!({ "id": id, "level": level }),
        None => sonic_rs::Value::default(),
    };

    Ok(protocol::Response {
        body: sonic_rs::to_vec(&json!({
            "entry_point": entry_point_json,
            "total_vectors": total_vectors,
            "total_edges": total_edges,
            "checked_vectors": checked_count,
            "unreachable_vectors": unreachable_strings,
            "unreachable_count": unreachable_count,
            "health_status": health_status,
            "mode": mode,
            "diagnostics": {
                "sample_size": if mode == "quick" { sample_size } else { total_vectors },
                "duration_ms": duration_ms
            }
        }))
        .map_err(|e| GraphError::New(e.to_string()))?,
        fmt: Default::default(),
    })
}

/// Quick mode: Sample N random vectors, search for each using its own embedding,
/// report those not found in top-K results.
fn run_quick_mode_diagnostics(
    vectors: &crate::helix_engine::vector_core::vector_core::VectorCore,
    txn: &heed3::RoTxn,
    vector_ids: &[u128],
    sample_size: usize,
    label: &str,
    arena: &bumpalo::Bump,
) -> Result<(Vec<u128>, usize), GraphError> {
    let mut rng = rand::rng();

    // Sample random vectors
    let sample_count = sample_size.min(vector_ids.len());
    let mut sampled_ids: Vec<u128> = vector_ids.to_vec();
    sampled_ids.shuffle(&mut rng);
    sampled_ids.truncate(sample_count);

    eprintln!("[HNSWDiagnostics] Quick mode: checking {} sampled vectors", sample_count);

    let mut unreachable = Vec::new();
    let label_arena = bumpalo::Bump::new();
    let label_str: &str = label_arena.alloc_str(label);

    for (idx, &vector_id) in sampled_ids.iter().enumerate() {
        if idx > 0 && idx % 100 == 0 {
            eprintln!("[HNSWDiagnostics] Progress: {}/{}", idx, sample_count);
        }

        // Get the vector's embedding
        let vector = match vectors.get_full_vector(txn, vector_id, arena) {
            Ok(v) => v,
            Err(_) => {
                // Can't load vector - consider it unreachable
                unreachable.push(vector_id);
                continue;
            }
        };

        // Search for this vector using its own embedding
        let search_results: bumpalo::collections::Vec<'_, _> = match vectors.search::<fn(&_, &_) -> bool>(
            txn,
            vector.data,
            DEFAULT_SEARCH_K,
            label_str,
            None,
            false,
            arena,
        ) {
            Ok(results) => results,
            Err(_) => {
                // Search failed - consider vector unreachable
                unreachable.push(vector_id);
                continue;
            }
        };

        // Check if the vector is in the search results
        let found = search_results.iter().any(|r| r.id == vector_id);
        if !found {
            unreachable.push(vector_id);
        }
    }

    Ok((unreachable, sample_count))
}

/// Full mode: BFS traversal from entry point through level-0 edges,
/// report all vectors not visited.
fn run_full_mode_diagnostics(
    vectors: &crate::helix_engine::vector_core::vector_core::VectorCore,
    txn: &heed3::RoTxn,
    vector_ids: &[u128],
    arena: &bumpalo::Bump,
) -> Result<(Vec<u128>, usize), GraphError> {
    eprintln!("[HNSWDiagnostics] Full mode: BFS traversal from entry point");

    // Get entry point
    let entry_point_id = match vectors.vectors_db.get(txn, ENTRY_POINT_KEY) {
        Ok(Some(ep_bytes)) => {
            let mut arr = [0u8; 16];
            let len = std::cmp::min(ep_bytes.len(), 16);
            arr[..len].copy_from_slice(&ep_bytes[..len]);
            u128::from_be_bytes(arr)
        }
        _ => {
            // No entry point - all vectors are unreachable
            eprintln!("[HNSWDiagnostics] No entry point found - all vectors unreachable");
            return Ok((vector_ids.to_vec(), vector_ids.len()));
        }
    };

    // BFS from entry point through level-0 edges
    let mut visited: HashSet<u128> = HashSet::new();
    let mut queue: VecDeque<u128> = VecDeque::new();

    queue.push_back(entry_point_id);
    visited.insert(entry_point_id);

    let mut iteration = 0;
    while let Some(current_id) = queue.pop_front() {
        iteration += 1;
        if iteration % 10000 == 0 {
            eprintln!("[HNSWDiagnostics] BFS progress: visited {} vectors", visited.len());
        }

        // Get level-0 neighbors
        let out_key = crate::helix_engine::vector_core::vector_core::VectorCore::out_edges_key(
            current_id,
            0, // level 0
            None,
        );

        let iter = match vectors.edges_db.lazily_decode_data().prefix_iter(txn, &out_key) {
            Ok(iter) => iter,
            Err(_) => continue,
        };

        let prefix_len = out_key.len();

        for result in iter {
            let (key, _) = match result {
                Ok(r) => r,
                Err(_) => continue,
            };

            if key.len() < prefix_len + 16 {
                continue;
            }

            let mut arr = [0u8; 16];
            arr[..16].copy_from_slice(&key[prefix_len..(prefix_len + 16)]);
            let neighbor_id = u128::from_be_bytes(arr);

            if neighbor_id != current_id && !visited.contains(&neighbor_id) {
                visited.insert(neighbor_id);
                queue.push_back(neighbor_id);
            }
        }
    }

    eprintln!("[HNSWDiagnostics] BFS complete: visited {} out of {} vectors", visited.len(), vector_ids.len());

    // Find all vectors not visited
    let all_ids: HashSet<u128> = vector_ids.iter().copied().collect();
    let unreachable: Vec<u128> = all_ids.difference(&visited).copied().collect();

    Ok((unreachable, vector_ids.len()))
}

inventory::submit! {
    HandlerSubmission(
        Handler::new("HNSWDiagnostics", hnsw_diagnostics_inner, false)
    )
}
