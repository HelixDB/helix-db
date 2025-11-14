
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
        "name": "Post",
        "properties": {
          "label": "String",
          "title": "String",
          "content": "String",
          "id": "ID"
        }
      },
      {
        "name": "User",
        "properties": {
          "age": "U8",
          "label": "String",
          "id": "ID",
          "name": "String",
          "email": "String"
        }
      }
    ],
    "vectors": [],
    "edges": [
      {
        "name": "HasPost",
        "from": "User",
        "to": "Post",
        "properties": {}
      }
    ]
  },
  "queries": [
    {
      "name": "CreateUser",
      "parameters": {
        "name": "String",
        "age": "U8",
        "email": "String"
      },
      "returns": [
        "user"
      ]
    },
    {
      "name": "CreatePost",
      "parameters": {
        "user_id": "ID",
        "title": "String",
        "content": "String"
      },
      "returns": [
        "post"
      ]
    },
    {
      "name": "GetUserPostsWorking2",
      "parameters": {
        "user_id": "ID"
      },
      "returns": []
    },
    {
      "name": "GetUserPosts",
      "parameters": {
        "user_id": "ID"
      },
      "returns": []
    },
    {
      "name": "GetUserPostsWorking",
      "parameters": {
        "user_id": "ID"
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
    pub name: String,
    pub age: u8,
    pub email: String,
}

pub struct Post {
    pub title: String,
    pub content: String,
}

pub struct HasPost {
    pub from: User,
    pub to: Post,
}


#[derive(Serialize, Deserialize, Clone)]
pub struct CreateUserInput {

pub name: String,
pub age: u8,
pub email: String
}
#[derive(Serialize)]
pub struct CreateUserUserReturnType<'a> {
    pub id: &'a str,
    pub label: &'a str,
    pub age: Option<&'a Value>,
    pub email: Option<&'a Value>,
    pub name: Option<&'a Value>,
}

#[handler]
pub fn CreateUser (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let data = input.request.in_fmt.deserialize::<CreateUserInput>(&input.request.body)?;
let arena = Bump::new();
let mut txn = db.graph_env.write_txn().map_err(|e| GraphError::New(format!("Failed to start write transaction: {:?}", e)))?;
    let user = G::new_mut(&db, &arena, &mut txn)
.add_n("User", Some(ImmutablePropertiesMap::new(3, vec![("name", Value::from(&data.name)), ("age", Value::from(&data.age)), ("email", Value::from(&data.email))].into_iter(), &arena)), None).collect_to_obj()?;
let response = json!({
    "user": CreateUserUserReturnType {
        id: uuid_str(user.id(), &arena),
        label: user.label(),
        age: user.get_property("age"),
        email: user.get_property("email"),
        name: user.get_property("name"),
    }
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CreatePostInput {

pub user_id: ID,
pub title: String,
pub content: String
}
#[derive(Serialize)]
pub struct CreatePostPostReturnType<'a> {
    pub id: &'a str,
    pub label: &'a str,
    pub title: Option<&'a Value>,
    pub content: Option<&'a Value>,
}

#[handler]
pub fn CreatePost (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let data = input.request.in_fmt.deserialize::<CreatePostInput>(&input.request.body)?;
let arena = Bump::new();
let mut txn = db.graph_env.write_txn().map_err(|e| GraphError::New(format!("Failed to start write transaction: {:?}", e)))?;
    let user = G::new(&db, &txn, &arena)
.n_from_id(&data.user_id).collect_to_obj()?;
    let post = G::new_mut(&db, &arena, &mut txn)
.add_n("Post", Some(ImmutablePropertiesMap::new(2, vec![("content", Value::from(&data.content)), ("title", Value::from(&data.title))].into_iter(), &arena)), None).collect_to_obj()?;
    G::new_mut(&db, &arena, &mut txn)
.add_edge("HasPost", None, user.id(), post.id(), false).collect_to_obj()?;
let response = json!({
    "post": CreatePostPostReturnType {
        id: uuid_str(post.id(), &arena),
        label: post.label(),
        title: post.get_property("title"),
        content: post.get_property("content"),
    }
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}

#[derive(Serialize, Deserialize, Clone)]
pub struct GetUserPostsWorking2Input {

pub user_id: ID
}
#[derive(Serialize)]
pub struct GetUserPostsWorking2UserReturnType {
}

#[handler]
pub fn GetUserPostsWorking2 (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let data = input.request.in_fmt.deserialize::<GetUserPostsWorking2Input>(&input.request.body)?;
let arena = Bump::new();
let txn = db.graph_env.read_txn().map_err(|e| GraphError::New(format!("Failed to start read transaction: {:?}", e)))?;
    let user = G::new(&db, &txn, &arena)
.n_from_id(&data.user_id).collect_to_obj()?;
    let posts = G::from_iter(&db, &txn, std::iter::once(user.clone()), &arena)

.out_node("HasPost").collect::<Result<Vec<_>, _>>()?;
let response = json!({
    "user": GetUserPostsWorking2UserReturnType {
    }
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}

#[derive(Serialize, Deserialize, Clone)]
pub struct GetUserPostsInput {

pub user_id: ID
}
#[derive(Serialize)]
pub struct GetUserPostsUserPostsReturnType<'a> {
    pub id: Option<&'a Value>,
    pub title: Option<&'a Value>,
    pub content: Option<&'a Value>,
    pub label: Option<&'a Value>,
    pub creatorName: &'a str,
    pub creatorID: &'a str,
}


#[derive(Serialize)]
pub struct GetUserPostsUserReturnType<'a> {
    pub posts: Vec<GetUserPostsUserPostsReturnType<'a>>,
}

#[handler]
pub fn GetUserPosts (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let data = input.request.in_fmt.deserialize::<GetUserPostsInput>(&input.request.body)?;
let arena = Bump::new();
let txn = db.graph_env.read_txn().map_err(|e| GraphError::New(format!("Failed to start read transaction: {:?}", e)))?;
    let user = G::new(&db, &txn, &arena)
.n_from_id(&data.user_id).collect_to_obj()?;
    let posts = G::from_iter(&db, &txn, std::iter::once(user.clone()), &arena)

.out_node("HasPost").collect::<Result<Vec<_>, _>>()?;
let response = json!({
    "user": GetUserPostsUserReturnType {
        posts: G::from_iter(&db, &txn, std::iter::once(user.clone()), &arena).map(|item| item.map(|item| GetUserPostsUserPostsReturnType {
                        id: uuid_str(item.id(), &arena),
                        title: item.get_property("title"),
                        content: item.get_property("content"),
                        label: item.label(),
                        creatorName: item.get_property("creatorName"),
                        creatorID: item.get_property("creatorID"),
                    })).collect::<Result<Vec<_>, _>>()?,
    }
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}

#[derive(Serialize, Deserialize, Clone)]
pub struct GetUserPostsWorkingInput {

pub user_id: ID
}
#[derive(Serialize)]
pub struct GetUserPostsWorkingUserReturnType<'a> {
    pub id: &'a str,
    pub name: Option<&'a Value>,
}

#[handler]
pub fn GetUserPostsWorking (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let data = input.request.in_fmt.deserialize::<GetUserPostsWorkingInput>(&input.request.body)?;
let arena = Bump::new();
let txn = db.graph_env.read_txn().map_err(|e| GraphError::New(format!("Failed to start read transaction: {:?}", e)))?;
    let user = G::new(&db, &txn, &arena)
.n_from_id(&data.user_id).collect_to_obj()?;
    let posts = G::from_iter(&db, &txn, std::iter::once(user.clone()), &arena)

.out_node("HasPost").collect::<Result<Vec<_>, _>>()?;
let response = json!({
    "user": GetUserPostsWorkingUserReturnType {
        id: uuid_str(user.id(), &arena),
        name: user.get_property("name"),
    }
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}


