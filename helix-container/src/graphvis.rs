use helixdb::{
    helix_engine::types::GraphError,
    protocol::response::Response,
    helix_gateway::router::router::HandlerInput,
    debug_println,
};
use get_routes::get_handler;
use serde_json::Value;
use std::sync::Arc;

#[get_handler]
pub fn graphvis(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let db = Arc::clone(&input.graph.storage);
    let json_ne: String = match db.get_ne_json() {
        Ok(value) => value,
        Err(e) => {
            println!("error with json: {}", e);
            return Ok(());
        }
    };
    let json_ne_m = modify_graph_json(&json_ne).unwrap();

    let db_counts: String = match db.get_db_stats_json() {
        Ok(value) => value,
        Err(e) => {
            println!("error with json: {:?}", e);
            return Ok(());
        }
    };
    let db_counts_m: Value = match serde_json::from_str(&db_counts) {
        Ok(value) => value,
        Err(e) => {
            println!("error with json: {:?}", e);
            return Ok(());
        }
    };

    let html_template = include_str!("graphvis.html");
    let html_content = html_template
        .replace("{NODES_JSON_DATA}", &serde_json::to_string(&json_ne_m["nodes"]).unwrap())
        .replace("{EDGES_JSON_DATA}", &serde_json::to_string(&json_ne_m["edges"]).unwrap())
        .replace("{NUM_NODES}", &serde_json::to_string(&db_counts_m["num_nodes"]).unwrap())
        .replace("{NUM_EDGES}", &serde_json::to_string(&db_counts_m["num_edges"]).unwrap())
        .replace("{NUM_VECTORS}", &serde_json::to_string(&db_counts_m["num_vectors"]).unwrap());

    response.headers.insert("Content-Type".to_string(), "text/html".to_string());
    response.body = html_content.as_bytes().to_vec();
    Ok(())
}

fn modify_graph_json(input: &str) -> Result<Value, serde_json::Error> {
    let mut json: Value = serde_json::from_str(input)?;

    if let Some(nodes) = json.get_mut("nodes").and_then(|n| n.as_array_mut()) {
        for node in nodes {
            if let Some(obj) = node.as_object_mut() {
                obj.insert("color".to_string(), Value::String("#97c2fc".to_string()));
                obj.insert("shape".to_string(), Value::String("dot".to_string()));
            }
        }
    }

    if let Some(edges) = json.get_mut("edges").and_then(|e| e.as_array_mut()) {
        for edge in edges {
            if let Some(obj) = edge.as_object_mut() {
                obj.insert("arrows".to_string(), Value::String("to".to_string()));
            }
        }
    }

    Ok(json)
}

