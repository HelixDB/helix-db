use helix_db::helix_engine::{
    batch::{BatchConfig, BatchResult},
    storage_core::{HelixGraphStorage, version_info::VersionInfo},
    traversal_core::config::Config,
};
use helix_db::utils::{id::v6_uuid, items::{Node, Edge}};
use helix_db::protocol::value::Value;

#[test]
fn test_batch_node_insert() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = Config::default();
    let storage = HelixGraphStorage::new(
        temp_dir.path().to_str().unwrap(),
        config,
        VersionInfo::default(),
    ).unwrap();
    
    // Create test nodes
    let nodes: Vec<Node> = (0..100)
        .map(|i| Node {
            id: v6_uuid(),
            label: format!("TestNode{}", i),
            version: 1,
            properties: Some(vec![
                ("index", Value::I32(i)),
                ("name", Value::String(format!("Node {}", i))),
            ].into_iter().map(|(k, v)| (k.to_string(), v)).collect()),
        })
        .collect();
    
    let batch_config = BatchConfig {
        max_batch_size: 50,
        auto_commit: true,
        validate_before_insert: true,
    };
    
    // Insert nodes in batch
    let result = storage.insert_nodes_batch(nodes.clone(), &batch_config).unwrap();
    
    // Verify results
    assert_eq!(result.successful, 100);
    assert_eq!(result.failed, 0);
    assert!(result.errors.is_empty());
    
    // Verify nodes were actually inserted
    let rtxn = storage.graph_env.read_txn().unwrap();
    for node in &nodes {
        let stored = storage.nodes_db.get(&rtxn, &node.id).unwrap();
        assert!(stored.is_some());
    }
}

#[test]
fn test_batch_edge_insert() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = Config::default();
    let storage = HelixGraphStorage::new(
        temp_dir.path().to_str().unwrap(),
        config,
        VersionInfo::default(),
    ).unwrap();
    
    // First insert some nodes
    let nodes: Vec<Node> = (0..10)
        .map(|i| Node {
            id: v6_uuid(),
            label: format!("Node{}", i),
            version: 1,
            properties: None,
        })
        .collect();
    
    let batch_config = BatchConfig::default();
    storage.insert_nodes_batch(nodes.clone(), &batch_config).unwrap();
    
    // Create edges between nodes
    let mut edges = Vec::new();
    for i in 0..9 {
        edges.push(Edge {
            id: v6_uuid(),
            from_node: nodes[i].id,
            to_node: nodes[i + 1].id,
            label: "CONNECTS".to_string(),
            version: 1,
            properties: Some(vec![
                ("order", Value::I32(i as i32)),
            ].into_iter().map(|(k, v)| (k.to_string(), v)).collect()),
        });
    }
    
    // Insert edges in batch
    let result = storage.insert_edges_batch(edges.clone(), &batch_config).unwrap();
    
    // Verify results
    assert_eq!(result.successful, 9);
    assert_eq!(result.failed, 0);
    
    // Verify edges were inserted
    let rtxn = storage.graph_env.read_txn().unwrap();
    for edge in &edges {
        let stored = storage.edges_db.get(&rtxn, &edge.id).unwrap();
        assert!(stored.is_some());
    }
}

#[test]
fn test_batch_with_failures() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = Config::default();
    let storage = HelixGraphStorage::new(
        temp_dir.path().to_str().unwrap(),
        config,
        VersionInfo::default(),
    ).unwrap();
    
    // Create nodes with some duplicates (same ID)
    let duplicate_id = v6_uuid();
    let mut nodes = vec![
        Node {
            id: duplicate_id,
            label: "Node1".to_string(),
            version: 1,
            properties: None,
        },
        Node {
            id: duplicate_id, // Duplicate!
            label: "Node2".to_string(),
            version: 1,
            properties: None,
        },
        Node {
            id: v6_uuid(),
            label: "Node3".to_string(),
            version: 1,
            properties: None,
        },
    ];
    
    let batch_config = BatchConfig {
        max_batch_size: 10,
        auto_commit: true,
        validate_before_insert: false,
    };
    
    // Insert should partially succeed
    let result = storage.insert_nodes_batch(nodes, &batch_config).unwrap();
    
    // Should have at least 2 successful (first and third)
    assert!(result.successful >= 2);
    assert!(result.failed <= 1);
}

#[test] 
fn test_large_batch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = Config::default();
    let storage = HelixGraphStorage::new(
        temp_dir.path().to_str().unwrap(),
        config,
        VersionInfo::default(),
    ).unwrap();
    
    // Create a large batch of nodes
    let nodes: Vec<Node> = (0..10000)
        .map(|i| Node {
            id: v6_uuid(),
            label: "LargeNode".to_string(),
            version: 1,
            properties: Some(vec![
                ("index", Value::I32(i)),
            ].into_iter().map(|(k, v)| (k.to_string(), v)).collect()),
        })
        .collect();
    
    let batch_config = BatchConfig {
        max_batch_size: 5000,
        auto_commit: true,
        validate_before_insert: false,
    };
    
    // Should handle large batch efficiently
    let start = std::time::Instant::now();
    let result = storage.insert_nodes_batch(nodes, &batch_config).unwrap();
    let elapsed = start.elapsed();
    
    assert_eq!(result.successful, 10000);
    assert_eq!(result.failed, 0);
    
    // Should complete reasonably fast (under 5 seconds for 10k nodes)
    assert!(elapsed.as_secs() < 5, "Batch insert took too long: {:?}", elapsed);
    
    println!("Inserted 10,000 nodes in {:?} ({:.0} nodes/sec)", 
             elapsed, 
             10000.0 / elapsed.as_secs_f64());
}