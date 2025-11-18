
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
                    dedup::DedupAdapter, drop::Drop, exist::Exist,
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
secondary_indices: Some(vec!["key".to_string()]),
}),
db_max_size_gb: Some(10),
mcp: Some(true),
bm25: Some(true),
schema: Some(r#"{
  "schema": {
    "nodes": [
      {
        "name": "User",
        "properties": {
          "id": "ID",
          "country": "U8",
          "label": "String"
        }
      },
      {
        "name": "Metadata",
        "properties": {
          "value": "String",
          "id": "ID",
          "key": "String",
          "label": "String"
        }
      }
    ],
    "vectors": [
      {
        "name": "Item",
        "properties": {
          "data": "Array(F64)",
          "id": "ID",
          "category": "U16",
          "score": "F64",
          "label": "String"
        }
      }
    ],
    "edges": [
      {
        "name": "Interacted",
        "from": "User",
        "to": "Item",
        "properties": {}
      }
    ]
  },
  "queries": [
    {
      "name": "InsertItem",
      "parameters": {
        "embedding": "Array(F64)",
        "category": "U16"
      },
      "returns": []
    },
    {
      "name": "OneHopFilter",
      "parameters": {
        "category": "U16",
        "user_id": "ID"
      },
      "returns": []
    },
    {
      "name": "VectorHopFilter",
      "parameters": {
        "vector": "Array(F64)",
        "top_k": "I64",
        "country": "U8"
      },
      "returns": []
    },
    {
      "name": "PointGet",
      "parameters": {
        "item_id": "ID"
      },
      "returns": []
    },
    {
      "name": "InsertUser",
      "parameters": {
        "country": "U8"
      },
      "returns": []
    },
    {
      "name": "CreateDatasetId",
      "parameters": {
        "dataset_id": "String"
      },
      "returns": [
        "metadata"
      ]
    },
    {
      "name": "UpdateDatasetId",
      "parameters": {
        "dataset_id": "String"
      },
      "returns": [
        "metadata"
      ]
    },
    {
      "name": "GetDatasetId",
      "parameters": {},
      "returns": []
    },
    {
      "name": "OneHop",
      "parameters": {
        "user_id": "ID"
      },
      "returns": []
    },
    {
      "name": "Vector",
      "parameters": {
        "top_k": "I64",
        "vector": "Array(F64)"
      },
      "returns": []
    },
    {
      "name": "InsertInteractedEdge",
      "parameters": {
        "user_id": "ID",
        "item_id": "ID"
      },
      "returns": []
    }
  ]
}"#.to_string()),
embedding_model: Some("text-embedding-ada-002".to_string()),
graphvis_node_label: None,
})
}

pub struct User {
    pub country: u8,
}

pub struct Metadata {
    pub key: String,
    pub value: String,
}

pub struct Interacted {
    pub from: User,
    pub to: Item,
}

pub struct Item {
    pub category: u16,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct InsertItemInput {

pub embedding: Vec<f64>,
pub category: u16
}
#[handler]
pub fn InsertItem (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let data = input.request.in_fmt.deserialize::<InsertItemInput>(&input.request.body)?;
let arena = Bump::new();
let mut txn = db.graph_env.write_txn().map_err(|e| GraphError::New(format!("Failed to start write transaction: {:?}", e)))?;
    let item = G::new_mut(&db, &arena, &mut txn)
.insert_v::<fn(&HVector, &RTxn) -> bool>(&data.embedding, "Item", Some(ImmutablePropertiesMap::new(1, vec![("category", Value::from(data.category.clone()))].into_iter(), &arena))).collect_to_obj()?;
let response = json!({
    "item": uuid_str(item.id(), &arena)
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}

#[derive(Serialize, Deserialize, Clone)]
pub struct OneHopFilterInput {

pub user_id: ID,
pub category: u16
}
#[derive(Serialize)]
pub struct OneHopFilterItemsReturnType<'a> {
    pub id: &'a str,
    pub category: Option<&'a Value>,
}

#[handler]
pub fn OneHopFilter (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let data = input.request.in_fmt.deserialize::<OneHopFilterInput>(&input.request.body)?;
let arena = Bump::new();
let txn = db.graph_env.read_txn().map_err(|e| GraphError::New(format!("Failed to start read transaction: {:?}", e)))?;
    let items = G::new(&db, &txn, &arena)
.n_from_id(&data.user_id)

.out_vec("Interacted", false)

.filter_ref(|val, txn|{
                if let Ok(val) = val {
                    Ok(val
                    .get_property("category")
                    .map_or(false, |v| *v == data.category.clone()))
                } else {
                    Ok(false)
                }
            }).collect::<Result<Vec<_>, _>>()?;
let response = json!({
    "items": items.iter().map(|item| OneHopFilterItemsReturnType {
        id: uuid_str(item.id(), &arena),
        category: item.get_property("category"),
    }).collect::<Vec<_>>()
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}

#[derive(Serialize, Deserialize, Clone)]
pub struct VectorHopFilterInput {

pub vector: Vec<f64>,
pub top_k: i64,
pub country: u8
}
#[derive(Serialize)]
pub struct VectorHopFilterItemsReturnType<'a> {
    pub id: &'a str,
    pub category: Option<&'a Value>,
}

#[handler]
pub fn VectorHopFilter (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let data = input.request.in_fmt.deserialize::<VectorHopFilterInput>(&input.request.body)?;
let arena = Bump::new();
let txn = db.graph_env.read_txn().map_err(|e| GraphError::New(format!("Failed to start read transaction: {:?}", e)))?;
    let items = G::new(&db, &txn, &arena)
.search_v::<fn(&HVector, &RTxn) -> bool, _>(&data.vector, data.top_k.clone(), "Item", None)

.filter_ref(|val, txn|{
                if let Ok(val) = val {
                    Ok(Exist::exists(&mut G::from_iter(&db, &txn, std::iter::once(val.clone()), &arena)

.in_node("Interacted")

.filter_ref(|val, txn|{
                if let Ok(val) = val {
                    Ok(val
                    .get_property("country")
                    .map_or(false, |v| *v == data.country.clone()))
                } else {
                    Ok(false)
                }
            })))
                } else {
                    Ok(false)
                }
            }).collect::<Result<Vec<_>, _>>()?;
let response = json!({
    "items": items.iter().map(|item| VectorHopFilterItemsReturnType {
        id: uuid_str(item.id(), &arena),
        category: item.get_property("category"),
    }).collect::<Vec<_>>()
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PointGetInput {

pub item_id: ID
}
#[derive(Serialize)]
pub struct PointGetItemReturnType<'a> {
    pub id: &'a str,
    pub category: Option<&'a Value>,
}

#[handler]
pub fn PointGet (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let data = input.request.in_fmt.deserialize::<PointGetInput>(&input.request.body)?;
let arena = Bump::new();
let txn = db.graph_env.read_txn().map_err(|e| GraphError::New(format!("Failed to start read transaction: {:?}", e)))?;
    let item = G::new(&db, &txn, &arena)
.v_from_id(&data.item_id, false).collect_to_obj()?;
let response = json!({
    "item": PointGetItemReturnType {
        id: uuid_str(item.id(), &arena),
        category: item.get_property("category"),
    }
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}

#[derive(Serialize, Deserialize, Clone)]
pub struct InsertUserInput {

pub country: u8
}
#[handler]
pub fn InsertUser (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let data = input.request.in_fmt.deserialize::<InsertUserInput>(&input.request.body)?;
let arena = Bump::new();
let mut txn = db.graph_env.write_txn().map_err(|e| GraphError::New(format!("Failed to start write transaction: {:?}", e)))?;
    let user = G::new_mut(&db, &arena, &mut txn)
.add_n("User", Some(ImmutablePropertiesMap::new(1, vec![("country", Value::from(&data.country))].into_iter(), &arena)), None).collect_to_obj()?;
let response = json!({
    "user": uuid_str(user.id(), &arena)
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CreateDatasetIdInput {

pub dataset_id: String
}
#[derive(Serialize)]
pub struct CreateDatasetIdMetadataReturnType<'a> {
    pub id: &'a str,
    pub label: &'a str,
    pub value: Option<&'a Value>,
    pub key: Option<&'a Value>,
}

#[handler]
pub fn CreateDatasetId (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let data = input.request.in_fmt.deserialize::<CreateDatasetIdInput>(&input.request.body)?;
let arena = Bump::new();
let mut txn = db.graph_env.write_txn().map_err(|e| GraphError::New(format!("Failed to start write transaction: {:?}", e)))?;
    let metadata = G::new_mut(&db, &arena, &mut txn)
.add_n("Metadata", Some(ImmutablePropertiesMap::new(2, vec![("value", Value::from(&data.dataset_id)), ("key", Value::from("dataset_id"))].into_iter(), &arena)), Some(&["key"])).collect_to_obj()?;
let response = json!({
    "metadata": CreateDatasetIdMetadataReturnType {
        id: uuid_str(metadata.id(), &arena),
        label: metadata.label(),
        value: metadata.get_property("value"),
        key: metadata.get_property("key"),
    }
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}

#[derive(Serialize, Deserialize, Clone)]
pub struct UpdateDatasetIdInput {

pub dataset_id: String
}
#[derive(Serialize)]
pub struct UpdateDatasetIdMetadataReturnType<'a> {
    pub id: &'a str,
    pub label: &'a str,
    pub value: Option<&'a Value>,
    pub key: Option<&'a Value>,
}

#[handler]
pub fn UpdateDatasetId (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let data = input.request.in_fmt.deserialize::<UpdateDatasetIdInput>(&input.request.body)?;
let arena = Bump::new();
let mut txn = db.graph_env.write_txn().map_err(|e| GraphError::New(format!("Failed to start write transaction: {:?}", e)))?;
    Drop::drop_traversal(
                G::new(&db, &txn, &arena)
.n_from_index("Metadata", "key", &"dataset_id").collect::<Vec<_>>().into_iter(),
                &db,
                &mut txn,
            )?;;
    let metadata = G::new_mut(&db, &arena, &mut txn)
.add_n("Metadata", Some(ImmutablePropertiesMap::new(2, vec![("key", Value::from("dataset_id")), ("value", Value::from(&data.dataset_id))].into_iter(), &arena)), Some(&["key"])).collect_to_obj()?;
let response = json!({
    "metadata": UpdateDatasetIdMetadataReturnType {
        id: uuid_str(metadata.id(), &arena),
        label: metadata.label(),
        value: metadata.get_property("value"),
        key: metadata.get_property("key"),
    }
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}

#[handler]
pub fn GetDatasetId (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let arena = Bump::new();
let txn = db.graph_env.read_txn().map_err(|e| GraphError::New(format!("Failed to start read transaction: {:?}", e)))?;
    let dataset_id = G::new(&db, &txn, &arena)
.n_from_index("Metadata", "key", &"dataset_id").collect_to_obj()?;
let response = json!({
    "dataset_id": dataset_id.get_property("value")
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}

#[derive(Serialize, Deserialize, Clone)]
pub struct OneHopInput {

pub user_id: ID
}
#[derive(Serialize)]
pub struct OneHopItemsReturnType<'a> {
    pub id: &'a str,
    pub category: Option<&'a Value>,
}

#[handler]
pub fn OneHop (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let data = input.request.in_fmt.deserialize::<OneHopInput>(&input.request.body)?;
let arena = Bump::new();
let txn = db.graph_env.read_txn().map_err(|e| GraphError::New(format!("Failed to start read transaction: {:?}", e)))?;
    let items = G::new(&db, &txn, &arena)
.n_from_id(&data.user_id)

.out_vec("Interacted", false).collect::<Result<Vec<_>, _>>()?;
let response = json!({
    "items": items.iter().map(|item| OneHopItemsReturnType {
        id: uuid_str(item.id(), &arena),
        category: item.get_property("category"),
    }).collect::<Vec<_>>()
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}

#[derive(Serialize, Deserialize, Clone)]
pub struct VectorInput {

pub vector: Vec<f64>,
pub top_k: i64
}
#[derive(Serialize)]
pub struct VectorItemsReturnType<'a> {
    pub id: &'a str,
    pub score: f64,
    pub category: Option<&'a Value>,
}

#[handler]
pub fn Vector (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let data = input.request.in_fmt.deserialize::<VectorInput>(&input.request.body)?;
let arena = Bump::new();
let txn = db.graph_env.read_txn().map_err(|e| GraphError::New(format!("Failed to start read transaction: {:?}", e)))?;
    let items = G::new(&db, &txn, &arena)
.search_v::<fn(&HVector, &RTxn) -> bool, _>(&data.vector, data.top_k.clone(), "Item", None).collect::<Result<Vec<_>, _>>()?;
let response = json!({
    "items": items.iter().map(|item| VectorItemsReturnType {
        id: uuid_str(item.id(), &arena),
        score: item.score(),
        category: item.get_property("category"),
    }).collect::<Vec<_>>()
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}

#[derive(Serialize, Deserialize, Clone)]
pub struct InsertInteractedEdgeInput {

pub user_id: ID,
pub item_id: ID
}
#[handler]
pub fn InsertInteractedEdge (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let data = input.request.in_fmt.deserialize::<InsertInteractedEdgeInput>(&input.request.body)?;
let arena = Bump::new();
let mut txn = db.graph_env.write_txn().map_err(|e| GraphError::New(format!("Failed to start write transaction: {:?}", e)))?;
    let e = G::new_mut(&db, &arena, &mut txn)
.add_edge("Interacted", None, *data.user_id, *data.item_id, false).collect_to_obj()?;
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&()))
}


