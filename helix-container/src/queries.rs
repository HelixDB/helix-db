
// DEFAULT CODE
// use helix_db::helix_engine::traversal_core::config::Config;

// pub fn config() -> Option<Config> {
//     None
// }



use bumpalo::Bump;
use helix_macros::{handler, tool_call, mcp_handler, migration};
use helix_db::{
    helix_engine::{
        reranker::{
            RerankAdapter,
            fusion::{RRFReranker, MMRReranker, DistanceMethod},
        },
        storage_core::txn::{ReadTransaction, WriteTransaction},
        traversal_core::{
            RTxn,
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
secondary_indices: Some(vec!["name".to_string(), "age".to_string(), "count".to_string()]),
}),
db_max_size_gb: Some(10),
mcp: Some(true),
bm25: Some(true),
schema: Some(r#"{
  "schema": {
    "nodes": [
      {
        "name": "File9",
        "properties": {
          "other_field": "String",
          "count": "F32",
          "id": "ID",
          "age": "I32",
          "name": "String",
          "label": "String"
        }
      }
    ],
    "vectors": [],
    "edges": []
  },
  "queries": [
    {
      "name": "file9",
      "parameters": {
        "name": "String",
        "id": "ID"
      },
      "returns": [
        "user",
        "node",
        "node_by_name"
      ]
    }
  ]
}"#.to_string()),
embedding_model: Some("text-embedding-ada-002".to_string()),
graphvis_node_label: None,
})
}

pub struct File9 {
    pub name: String,
    pub age: i32,
    pub count: f32,
    pub other_field: String,
}



#[derive(Serialize, Deserialize, Clone)]
pub struct file9Input {

pub name: String,
pub id: ID
}
#[derive(Serialize)]
pub struct File9UserReturnType<'a> {
    pub id: &'a str,
    pub label: &'a str,
    pub count: Option<&'a Value>,
    pub other_field: Option<&'a Value>,
    pub age: Option<&'a Value>,
    pub name: Option<&'a Value>,
}

#[derive(Serialize)]
pub struct File9NodeReturnType<'a> {
    pub id: &'a str,
    pub label: &'a str,
    pub count: Option<&'a Value>,
    pub other_field: Option<&'a Value>,
    pub age: Option<&'a Value>,
    pub name: Option<&'a Value>,
}

#[derive(Serialize)]
pub struct File9Node_by_nameReturnType<'a> {
    pub id: &'a str,
    pub label: &'a str,
    pub count: Option<&'a Value>,
    pub other_field: Option<&'a Value>,
    pub age: Option<&'a Value>,
    pub name: Option<&'a Value>,
}

#[handler]
pub fn file9 (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let data = input.request.in_fmt.deserialize::<file9Input>(&input.request.body)?;
let arena = Bump::new();
let txn = db.graph_env.read_txn().map_err(|e| GraphError::New(format!("Failed to start read transaction: {:?}", e)))?;
    let user = G::new(&db, &txn, &arena)
.n_from_id(&data.id).collect_to_obj()?;
    let node = G::new(&db, &txn, &arena)
.n_from_index("File9", "name", &data.name).collect_to_obj()?;
    let node_by_name = G::new(&db, &txn, &arena)
.n_from_index("File9", "count", &24.5).collect_to_obj()?;
let response = json!({
    "user": File9UserReturnType {
        id: uuid_str(user.id(), &arena),
        label: user.label(),
        count: user.get_property("count"),
        other_field: user.get_property("other_field"),
        age: user.get_property("age"),
        name: user.get_property("name"),
    },
    "node": File9NodeReturnType {
        id: uuid_str(node.id(), &arena),
        label: node.label(),
        count: node.get_property("count"),
        other_field: node.get_property("other_field"),
        age: node.get_property("age"),
        name: node.get_property("name"),
    },
    "node_by_name": File9Node_by_nameReturnType {
        id: uuid_str(node_by_name.id(), &arena),
        label: node_by_name.label(),
        count: node_by_name.get_property("count"),
        other_field: node_by_name.get_property("other_field"),
        age: node_by_name.get_property("age"),
        name: node_by_name.get_property("name"),
    }
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}


