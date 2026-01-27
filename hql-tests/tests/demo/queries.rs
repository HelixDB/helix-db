
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
        reranker::{
            RerankAdapter,
            fusion::{RRFReranker, MMRReranker, DistanceMethod},
        },
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
                    upsert::UpsertAdapter,
                },
                vectors::{
                    brute_force_search::BruteForceSearchVAdapter, insert::InsertVAdapter,
                    search::SearchVAdapter,
                },
            },
            traversal_value::TraversalValue,
        },
        types::{GraphError, SecondaryIndex},
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
secondary_indices: None,
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
          "age": "U32",
          "name": "String",
          "created_at": "Date",
          "updated_at": "Date",
          "label": "String",
          "email": "String",
          "id": "ID"
        }
      },
      {
        "name": "Post",
        "properties": {
          "content": "String",
          "label": "String",
          "updated_at": "Date",
          "id": "ID",
          "created_at": "Date"
        }
      }
    ],
    "vectors": [],
    "edges": [
      {
        "name": "Follows",
        "from": "User",
        "to": "User",
        "properties": {
          "since": "Date"
        }
      },
      {
        "name": "Created",
        "from": "User",
        "to": "Post",
        "properties": {
          "created_at": "Date"
        }
      }
    ]
  },
  "queries": [
    {
      "name": "CreateUser",
      "parameters": {
        "name": "String",
        "age": "U32",
        "email": "String"
      },
      "returns": [
        "user"
      ]
    },
    {
      "name": "GetUsers",
      "parameters": {},
      "returns": [
        "users"
      ]
    },
    {
      "name": "GetPostsByUser",
      "parameters": {
        "user_id": "ID"
      },
      "returns": [
        "posts"
      ]
    },
    {
      "name": "CreateFollow",
      "parameters": {
        "follower_id": "ID",
        "followed_id": "ID"
      },
      "returns": []
    },
    {
      "name": "CreatePost",
      "parameters": {
        "user_id": "ID",
        "content": "String"
      },
      "returns": [
        "post"
      ]
    },
    {
      "name": "GetPosts",
      "parameters": {},
      "returns": [
        "posts"
      ]
    },
    {
      "name": "GetFollowedUsers",
      "parameters": {
        "user_id": "ID"
      },
      "returns": [
        "followed"
      ]
    },
    {
      "name": "GetFollowedUsersPosts",
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
    pub age: u32,
    pub email: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub struct Post {
    pub content: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub struct Follows {
    pub from: User,
    pub to: User,
    pub since: DateTime<Utc>,
}

pub struct Created {
    pub from: User,
    pub to: Post,
    pub created_at: DateTime<Utc>,
}


#[derive(Serialize, Deserialize, Clone)]
pub struct CreateUserInput {

pub name: String,
pub age: u32,
pub email: String
}
#[derive(Serialize)]
pub struct CreateUserUserReturnType<'a> {
    pub id: &'a str,
    pub label: &'a str,
    pub email: Option<&'a Value>,
    pub name: Option<&'a Value>,
    pub age: Option<&'a Value>,
    pub created_at: Option<&'a Value>,
    pub updated_at: Option<&'a Value>,
}

#[handler(is_write)]
pub fn CreateUser (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let data = input.request.in_fmt.deserialize::<CreateUserInput>(&input.request.body)?;
let arena = Bump::new();
let mut txn = db.graph_env.write_txn().map_err(|e| GraphError::New(format!("Failed to start write transaction: {:?}", e)))?;
    let user = G::new_mut(&db, &arena, &mut txn)
.add_n("User", Some(ImmutablePropertiesMap::new(5, vec![("email", Value::from(&data.email)), ("updated_at", Value::from(chrono::Utc::now().to_rfc3339())), ("name", Value::from(&data.name)), ("created_at", Value::from(chrono::Utc::now().to_rfc3339())), ("age", Value::from(&data.age))].into_iter(), &arena)), None).collect_to_obj()?;
let response = json!({
    "user": CreateUserUserReturnType {
        id: uuid_str(user.id(), &arena),
        label: user.label(),
        email: user.get_property("email"),
        name: user.get_property("name"),
        age: user.get_property("age"),
        created_at: user.get_property("created_at"),
        updated_at: user.get_property("updated_at"),
    }
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}

#[derive(Serialize)]
pub struct GetUsersUsersReturnType<'a> {
    pub id: &'a str,
    pub label: &'a str,
    pub email: Option<&'a Value>,
    pub name: Option<&'a Value>,
    pub age: Option<&'a Value>,
    pub created_at: Option<&'a Value>,
    pub updated_at: Option<&'a Value>,
}

#[handler]
pub fn GetUsers (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let arena = Bump::new();
let txn = db.graph_env.read_txn().map_err(|e| GraphError::New(format!("Failed to start read transaction: {:?}", e)))?;
    let users = G::new(&db, &txn, &arena)
.n_from_type("User").collect::<Result<Vec<_>, _>>()?;
let response = json!({
    "users": users.iter().map(|user| GetUsersUsersReturnType {
        id: uuid_str(user.id(), &arena),
        label: user.label(),
        email: user.get_property("email"),
        name: user.get_property("name"),
        age: user.get_property("age"),
        created_at: user.get_property("created_at"),
        updated_at: user.get_property("updated_at"),
    }).collect::<Vec<_>>()
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}

#[derive(Serialize, Deserialize, Clone)]
pub struct GetPostsByUserInput {

pub user_id: ID
}
#[derive(Serialize)]
pub struct GetPostsByUserPostsReturnType<'a> {
    pub id: &'a str,
    pub label: &'a str,
    pub updated_at: Option<&'a Value>,
    pub created_at: Option<&'a Value>,
    pub content: Option<&'a Value>,
}

#[handler]
pub fn GetPostsByUser (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let data = input.request.in_fmt.deserialize::<GetPostsByUserInput>(&input.request.body)?;
let arena = Bump::new();
let txn = db.graph_env.read_txn().map_err(|e| GraphError::New(format!("Failed to start read transaction: {:?}", e)))?;
    let posts = G::new(&db, &txn, &arena)
.n_from_id(&data.user_id)

.out_node("Created").collect::<Result<Vec<_>, _>>()?;
let response = json!({
    "posts": posts.iter().map(|post| GetPostsByUserPostsReturnType {
        id: uuid_str(post.id(), &arena),
        label: post.label(),
        updated_at: post.get_property("updated_at"),
        created_at: post.get_property("created_at"),
        content: post.get_property("content"),
    }).collect::<Vec<_>>()
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CreateFollowInput {

pub follower_id: ID,
pub followed_id: ID
}
#[handler(is_write)]
pub fn CreateFollow (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let data = input.request.in_fmt.deserialize::<CreateFollowInput>(&input.request.body)?;
let arena = Bump::new();
let mut txn = db.graph_env.write_txn().map_err(|e| GraphError::New(format!("Failed to start write transaction: {:?}", e)))?;
    let follower = G::new(&db, &txn, &arena)
.n_from_id(&data.follower_id).collect_to_obj()?;
    let followed = G::new(&db, &txn, &arena)
.n_from_id(&data.followed_id).collect_to_obj()?;
    G::new_mut(&db, &arena, &mut txn)
.add_edge("Follows", Some(ImmutablePropertiesMap::new(1, vec![("since", Value::from(chrono::Utc::now().to_rfc3339()))].into_iter(), &arena)), follower.id(), *data.followed_id, false, false).collect_to_obj()?;
let response = json!({
    "data": "success"
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CreatePostInput {

pub user_id: ID,
pub content: String
}
#[derive(Serialize)]
pub struct CreatePostPostReturnType<'a> {
    pub id: &'a str,
    pub label: &'a str,
    pub updated_at: Option<&'a Value>,
    pub created_at: Option<&'a Value>,
    pub content: Option<&'a Value>,
}

#[handler(is_write)]
pub fn CreatePost (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let data = input.request.in_fmt.deserialize::<CreatePostInput>(&input.request.body)?;
let arena = Bump::new();
let mut txn = db.graph_env.write_txn().map_err(|e| GraphError::New(format!("Failed to start write transaction: {:?}", e)))?;
    let user = G::new(&db, &txn, &arena)
.n_from_id(&data.user_id).collect_to_obj()?;
    let post = G::new_mut(&db, &arena, &mut txn)
.add_n("Post", Some(ImmutablePropertiesMap::new(3, vec![("updated_at", Value::from(chrono::Utc::now().to_rfc3339())), ("content", Value::from(&data.content)), ("created_at", Value::from(chrono::Utc::now().to_rfc3339()))].into_iter(), &arena)), None).collect_to_obj()?;
    G::new_mut(&db, &arena, &mut txn)
.add_edge("Created", Some(ImmutablePropertiesMap::new(1, vec![("created_at", Value::from(chrono::Utc::now().to_rfc3339()))].into_iter(), &arena)), user.id(), post.id(), false, false).collect_to_obj()?;
let response = json!({
    "post": CreatePostPostReturnType {
        id: uuid_str(post.id(), &arena),
        label: post.label(),
        updated_at: post.get_property("updated_at"),
        created_at: post.get_property("created_at"),
        content: post.get_property("content"),
    }
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}

#[derive(Serialize)]
pub struct GetPostsPostsReturnType<'a> {
    pub id: &'a str,
    pub label: &'a str,
    pub updated_at: Option<&'a Value>,
    pub created_at: Option<&'a Value>,
    pub content: Option<&'a Value>,
}

#[handler]
pub fn GetPosts (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let arena = Bump::new();
let txn = db.graph_env.read_txn().map_err(|e| GraphError::New(format!("Failed to start read transaction: {:?}", e)))?;
    let posts = G::new(&db, &txn, &arena)
.n_from_type("Post").collect::<Result<Vec<_>, _>>()?;
let response = json!({
    "posts": posts.iter().map(|post| GetPostsPostsReturnType {
        id: uuid_str(post.id(), &arena),
        label: post.label(),
        updated_at: post.get_property("updated_at"),
        created_at: post.get_property("created_at"),
        content: post.get_property("content"),
    }).collect::<Vec<_>>()
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}

#[derive(Serialize, Deserialize, Clone)]
pub struct GetFollowedUsersInput {

pub user_id: ID
}
#[derive(Serialize)]
pub struct GetFollowedUsersFollowedReturnType<'a> {
    pub id: &'a str,
    pub label: &'a str,
    pub email: Option<&'a Value>,
    pub name: Option<&'a Value>,
    pub age: Option<&'a Value>,
    pub created_at: Option<&'a Value>,
    pub updated_at: Option<&'a Value>,
}

#[handler]
pub fn GetFollowedUsers (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let data = input.request.in_fmt.deserialize::<GetFollowedUsersInput>(&input.request.body)?;
let arena = Bump::new();
let txn = db.graph_env.read_txn().map_err(|e| GraphError::New(format!("Failed to start read transaction: {:?}", e)))?;
    let followed = G::new(&db, &txn, &arena)
.n_from_id(&data.user_id)

.out_node("Follows").collect::<Result<Vec<_>, _>>()?;
let response = json!({
    "followed": followed.iter().map(|followed| GetFollowedUsersFollowedReturnType {
        id: uuid_str(followed.id(), &arena),
        label: followed.label(),
        email: followed.get_property("email"),
        name: followed.get_property("name"),
        age: followed.get_property("age"),
        created_at: followed.get_property("created_at"),
        updated_at: followed.get_property("updated_at"),
    }).collect::<Vec<_>>()
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}

#[derive(Serialize, Deserialize, Clone)]
pub struct GetFollowedUsersPostsInput {

pub user_id: ID
}
#[derive(Serialize)]
pub struct GetFollowedUsersPostsPostsReturnType<'a> {
    pub creatorID: &'a str,
}

#[handler]
pub fn GetFollowedUsersPosts (input: HandlerInput) -> Result<Response, GraphError> {
let db = Arc::clone(&input.graph.storage);
let data = input.request.in_fmt.deserialize::<GetFollowedUsersPostsInput>(&input.request.body)?;
let arena = Bump::new();
let txn = db.graph_env.read_txn().map_err(|e| GraphError::New(format!("Failed to start read transaction: {:?}", e)))?;
    let followers = G::new(&db, &txn, &arena)
.n_from_id(&data.user_id)

.out_node("Follows").collect::<Result<Vec<_>, _>>()?;
    let posts = G::from_iter(&db, &txn, followers.iter().cloned(), &arena)

.out_node("Created")

.range(0, 40).collect::<Result<Vec<_>, _>>()?;
let response = json!({
    "posts": posts.iter().map(|post| Ok::<_, GraphError>(GetFollowedUsersPostsPostsReturnType {
        creatorID: G::from_iter(&db, &txn, std::iter::once(post.clone()), &arena)

.in_node("Created")

.map(|item| item.map(|v| Value::from(uuid_str(v.id(), &arena)))),
    })).collect::<Result<Vec<_>, GraphError>>()?
});
txn.commit().map_err(|e| GraphError::New(format!("Failed to commit transaction: {:?}", e)))?;
Ok(input.request.out_fmt.create_response(&response))
}


