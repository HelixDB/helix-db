use std::sync::Arc;

use heed3::RoTxn;
use tempfile::TempDir;

use crate::{
    helix_engine::{
        storage_core::version_info::VersionInfo,
        traversal_core::{
            HelixGraphEngine, HelixGraphEngineOpts,
            config::Config,
            ops::{
                g::G,
                source::{
                    add_e::{AddEAdapter, EdgeType},
                    add_n::AddNAdapter,
                },
                vectors::insert::InsertVAdapter,
            },
            traversal_value::{Traversable, TraversalValue},
        },
        vector_core::{vector::HVector, vector_distance::SimilarityMethod},
    },
    helix_gateway::mcp::{mcp::MCPConnection, tools::McpTools},
};

fn setup_test_db() -> (HelixGraphEngine, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let opts = HelixGraphEngineOpts {
        path: db_path.to_string(),
        config: Config::default(),
        version_info: VersionInfo::default(),
    };
    let storage = HelixGraphEngine::new(opts).unwrap();
    (storage, temp_dir)
}

#[test]
fn test_mcp_tool_out_step() {}

#[test]
fn test_mcp_tool_out_e_step() {}

#[test]
fn test_mcp_tool_in_step() {}

#[test]
fn test_mcp_tool_in_e_step() {}

#[test]
fn test_mcp_tool_n_from_type() {}

#[test]
fn test_mcp_tool_e_from_type() {}

#[test]
fn test_mcp_tool_filter_items() {}

// TODO
#[test]
fn test_mcp_tool_search_keyword() {}

#[test]
fn test_mcp_tool_search_vector_text() {}

use rand::prelude::SliceRandom;

#[test]
fn test_mcp_tool_search_vector() {
    let (engine, _temp_dir) = setup_test_db();
    let mut txn = engine.storage.graph_env.write_txn().unwrap();

    // creates nodes and vectors
    let node = G::new_mut(Arc::clone(&engine.storage), &mut txn)
        .add_n("person", None, None)
        .collect_to_obj();
    let mut vectors = vec![
        vec![1.0, 1.0, 1.0],
        vec![0.0, 0.0, 0.0],
        vec![0.3, 0.3, 0.3],
    ];

    for _ in 3..1000 {
        vectors.push(vec![
            rand::random_range(-1.0..0.5),
            rand::random_range(-1.0..0.5),
            rand::random_range(-1.0..0.5),
        ]);
    }

    vectors.shuffle(&mut rand::rng());

    for vector in vectors {
        let vector = G::new_mut(Arc::clone(&engine.storage), &mut txn)
            .insert_v::<fn(&HVector, &RoTxn) -> bool>(
                &vector,
                "vector",
                None,
                &crate::helix_engine::vector_core::vector_distance::SimilarityMethod::default(),
            )
            .collect_to_obj();

        let _ = G::new_mut(Arc::clone(&engine.storage), &mut txn)
            .add_e("knows", None, node.id(), vector.id(), false, EdgeType::Vec)
            .collect_to_obj();
    }
    txn.commit().unwrap();
    let txn = engine.storage.graph_env.read_txn().unwrap();
    let mcp_backend = engine.mcp_backend.as_ref().unwrap();
    let mcp_connections = engine.mcp_connections.as_ref().unwrap();
    let mut mcp_connections = mcp_connections.lock().unwrap();

    // creates mcp connection
    let mcp_connection = MCPConnection::new("test".to_string(), vec![].into_iter());
    mcp_connections.add_connection(mcp_connection);
    let mut mcp_connection = mcp_connections.get_connection_owned("test").unwrap();

    // gets node
    let res = mcp_backend
        .n_from_type(&txn, &mcp_connection, "person".to_string())
        .unwrap();
    assert_eq!(res.len(), 1);
    mcp_connection.iter = res.into_iter();

    // traverses to vectors
    let res = mcp_backend
        .out_step(&txn, &mcp_connection, "knows".to_string(), EdgeType::Vec)
        .unwrap();
    mcp_connection.iter = res.into_iter();

    // brute force searches for vectors
    let res = mcp_backend
        .search_vector(
            &txn,
            &mcp_connection,
            vec![1.0, 1.0, 1.0],
            10,
            None,
            Some(SimilarityMethod::default()),
        )
        .unwrap();

    // checks that the first vector is correct
    if let TraversalValue::Vector(v) = res[0].clone() {
        assert_eq!(v.get_data(), &[1.0, 1.0, 1.0]);
    } else {
        panic!("Expected vector");
    }
}
