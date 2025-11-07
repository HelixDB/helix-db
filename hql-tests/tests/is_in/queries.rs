
// DEFAULT CODE
// use helix_db::helix_engine::traversal_core::config::Config;

// pub fn config() -> Option<Config> {
//     None
// }



use bumpalo::Bump;
use heed3::RoTxn;
use helix_macros::{handler, tool_call, mcp_handler, migration};
use helix_db::{
    helix_engine::{
        traversal_core::{
            config::{Config, GraphConfig, VectorConfig},
            ops::{
                bm25::search_bm25::SearchBM25Adapter,
                g::G,
                in_::{in_::InAdapter, in_e::InEdgesAdapter, to_n::ToNAdapter, to_v::ToVAdapter},
                out::{
                    from_n::FromNAdapter, from_v::FromVAdapter, out::OutAdapter, out_e::OutEdgesAdapter,
                },
                source::{
                    add_e::AddEAdapter,
                    add_n::AddNAdapter,
                    e_from_id::EFromIdAdapter,
                    e_from_type::EFromTypeAdapter,
                    n_from_id::NFromIdAdapter,
                    n_from_index::NFromIndexAdapter,
                    n_from_type::NFromTypeAdapter,
                    v_from_id::VFromIdAdapter,
                    v_from_type::VFromTypeAdapter
                },
                util::{
                    dedup::DedupAdapter, drop::Drop, exist::Exist, filter_mut::FilterMut,
                    filter_ref::FilterRefAdapter, map::MapAdapter, paths::{PathAlgorithm, ShortestPathAdapter},
                    range::RangeAdapter, update::UpdateAdapter, order::OrderByAdapter,
                    aggregate::AggregateAdapter, group_by::GroupByAdapter, count::CountAdapter,
                },
                vectors::{
                    brute_force_search::BruteForceSearchVAdapter, insert::InsertVAdapter,
                    search::SearchVAdapter,
                },
            },
            traversal_value::TraversalValue,
        },
        types::GraphError,
        vector_core::vector::HVector,
    },
    helix_gateway::{
        embedding_providers::{EmbeddingModel, get_embedding_model},
        router::router::{HandlerInput, IoContFn},
        mcp::mcp::{MCPHandlerSubmission, MCPToolInput, MCPHandler}
    },
    node_matches, props, embed, embed_async,
    field_addition_from_old_field, field_type_cast, field_addition_from_value,
    protocol::{
        response::Response,
        value::{casting::{cast, CastType}, Value},
        format::Format,
    },
    utils::{
        count::Count,
        id::{ID, uuid_str},
        items::{Edge, Node},
        properties::ImmutablePropertiesMap,
    },
};
use sonic_rs::{Deserialize, Serialize, json};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use chrono::{DateTime, Utc};

// Re-export scalar types for generated code
type I8 = i8;
type I16 = i16;
type I32 = i32;
type I64 = i64;
type U8 = u8;
type U16 = u16;
type U32 = u32;
type U64 = u64;
type U128 = u128;
type F32 = f32;
type F64 = f64;
    
pub fn config() -> Option<Config> {
return Some(Config {
vector_config: Some(VectorConfig {
m: Some(16),
ef_construction: Some(128),
ef_search: Some(768),
}),
graph_config: Some(GraphConfig {
secondary_indices: Some(vec![]),
}),
db_max_size_gb: Some(10),
mcp: Some(true),
bm25: Some(true),
schema: Some(r#"{
  "schema": {
    "nodes": [
      {
        "name": "MyNode",
        "properties": {
          "label": "String",
          "id": "ID",
          "field": "String"
        }
      }
    ],
    "vectors": [],
    "edges": []
  },
  "queries": [
    {
      "name": "GetNodesByID",
      "parameters": {
        "node_ids": "Array(ID)"
      },
      "returns": [
        "node"
      ]
    },
    {
      "name": "GetNodes",
      "parameters": {
        "fields": "Array(String)"
      },
      "returns": [
        "node"
      ]
    }
  ]
}"#.to_string()),
embedding_model: Some("text-embedding-ada-002".to_string()),
graphvis_node_label: None,
})
}

pub struct MyNode {
    pub field: String,
}



#[derive(Serialize, Deserialize, Clone)]
pub struct GetNodesByIDInput {

pub node_ids: Vec<ID>
}
#[derive(Serialize)]
pub struct GetNodesByIDNodeReturnType<'a> {
    pub id: &'a str,
    pub label: &'a str,
    pub field: Option<&'a Value>,
}

#[handler]
pub fn GetNodesByID (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let data = input.request.in_fmt.deserialize::<GetNodesByIDInput>(&input.request.body)?;
let arena = Bump::new();
let txn = db.graph_env.read_txn().map_err(|e| GraphError::New(format!("Failed to start read transaction: {:?}", e)))?;
    let node = G::new(&db, &txn, &arena)
.n_from_type("MyNode")

.filter_ref(|val, txn|{
                if let Ok(val) = val {
                    Ok(Value::Id(ID::from(val.id)).is_in(&data.node_ids))
                } else {
                    Ok(false)
                }
            }).collect::<Result<Vec<_>, _>>()?;
let response = json!({
    "node": node.iter().map(|node| GetNodesByIDNodeReturnType {
        id: uuid_str(node.id(), &arena),
        label: node.label(),
        field: node.get_property("field"),
    }).collect::<Vec<_>>()
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}

#[derive(Serialize, Deserialize, Clone)]
pub struct GetNodesInput {

pub fields: Vec<String>
}
#[derive(Serialize)]
pub struct GetNodesNodeReturnType<'a> {
    pub id: &'a str,
    pub label: &'a str,
    pub field: Option<&'a Value>,
}

#[handler]
pub fn GetNodes (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let data = input.request.in_fmt.deserialize::<GetNodesInput>(&input.request.body)?;
let arena = Bump::new();
let txn = db.graph_env.read_txn().map_err(|e| GraphError::New(format!("Failed to start read transaction: {:?}", e)))?;
    let node = G::new(&db, &txn, &arena)
.n_from_type("MyNode")

.filter_ref(|val, txn|{
                if let Ok(val) = val {
                    Ok(val
                    .get_property("field")
                    .map_or(false, |v| v.is_in(&data.fields)))
                } else {
                    Ok(false)
                }
            }).collect::<Result<Vec<_>, _>>()?;
let response = json!({
    "node": node.iter().map(|node| GetNodesNodeReturnType {
        id: uuid_str(node.id(), &arena),
        label: node.label(),
        field: node.get_property("field"),
    }).collect::<Vec<_>>()
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}


