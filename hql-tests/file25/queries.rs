

use heed3::RoTxn;
use get_routes::handler;
use helixdb::{field_remapping, identifier_remapping, traversal_remapping, exclude_field, value_remapping};
use helixdb::helix_engine::vector_core::vector::HVector;
use helixdb::{
    helix_engine::graph_core::ops::{
        g::G,
        in_::{in_::InAdapter, in_e::InEdgesAdapter, to_n::ToNAdapter},
        out::{from_n::FromNAdapter, out::OutAdapter, out_e::OutEdgesAdapter},
        source::{
            add_e::{AddEAdapter, EdgeType},
            add_n::AddNAdapter,
            e_from_id::EFromIdAdapter,
            e_from_type::EFromTypeAdapter,
            n_from_id::NFromIdAdapter,
            n_from_type::NFromTypeAdapter,
            n_from_index::NFromIndexAdapter,
        },
        tr_val::{Traversable, TraversalVal},
        util::{
            dedup::DedupAdapter, filter_mut::FilterMut,
            filter_ref::FilterRefAdapter, range::RangeAdapter, update::UpdateAdapter,
            map::MapAdapter, paths::ShortestPathAdapter, props::PropsAdapter, drop::Drop,
        },
        vectors::{insert::InsertVAdapter, search::SearchVAdapter, brute_force_search::BruteForceSearchVAdapter},
        bm25::search_bm25::SearchBM25Adapter,
        
    },
    helix_engine::types::GraphError,
    helix_gateway::router::router::HandlerInput,
    node_matches, props,
    protocol::count::Count,
    protocol::remapping::{RemappingMap, ResponseRemapping},
    protocol::response::Response,
    protocol::traversal_value::TraversalValue,
    protocol::{
        filterable::Filterable, remapping::Remapping, return_values::ReturnValue, value::Value, id::ID,
    },
};
use sonic_rs::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use std::cell::RefCell;
use chrono::{DateTime, Utc};
    
pub struct User {
    pub name: String,
    pub age: u32,
    pub email: String,
    pub created_at: i32,
    pub updated_at: i32,
}

pub struct Post {
    pub content: String,
    pub created_at: i32,
    pub updated_at: i32,
}

pub struct Follows {
    pub from: User,
    pub to: User,
    pub since: i32,
}

pub struct Created {
    pub from: User,
    pub to: Post,
    pub created_at: i32,
}


#[derive(Serialize, Deserialize)]
pub struct get_followed_usersInput {

pub user_id: ID
}
#[handler]
pub fn get_followed_users (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: get_followed_usersInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals = RemappingMap::new();
let db = Arc::clone(&input.graph.storage);
let txn = db.graph_env.read_txn().unwrap();
    let followed = G::new(Arc::clone(&db), &txn)
.n_from_id(&data.user_id)

.out("Follows",&EdgeType::Node).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("followed".to_string(), ReturnValue::from_traversal_value_array_with_mixin(followed.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[handler]
pub fn find_users_access (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let mut remapping_vals = RemappingMap::new();
let db = Arc::clone(&input.graph.storage);
let txn = db.graph_env.read_txn().unwrap();
    let users = G::new(Arc::clone(&db), &txn)
.n_from_type("User").collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("users".to_string(), ReturnValue::from_traversal_value_array_with_mixin(G::new_from(Arc::clone(&db), &txn, users.clone())

.map_traversal(|item, txn| { traversal_remapping!(remapping_vals, item.clone(), "name" => G::new_from(Arc::clone(&db), &txn, vec![item.clone()])

.check_property("name").collect_to_obj())?;
traversal_remapping!(remapping_vals, item.clone(), "age" => G::new_from(Arc::clone(&db), &txn, vec![item.clone()])

.check_property("age").collect_to_obj())?;
 Ok(item) }).collect_to::<Vec<_>>().clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct create_userInput {

pub name: String,
pub age: u32,
pub email: String,
pub now: i32
}
#[handler]
pub fn create_user (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: create_userInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals = RemappingMap::new();
let db = Arc::clone(&input.graph.storage);
let mut txn = db.graph_env.write_txn().unwrap();
    let user = G::new_mut(Arc::clone(&db), &mut txn)
.add_n("User", Some(props! { "email" => data.email.clone(), "updated_at" => data.now.clone(), "name" => data.name.clone(), "created_at" => data.now.clone(), "age" => data.age.clone() }), None).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("user".to_string(), ReturnValue::from_traversal_value_array_with_mixin(user.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[handler]
pub fn find_users_exclusion (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let mut remapping_vals = RemappingMap::new();
let db = Arc::clone(&input.graph.storage);
let txn = db.graph_env.read_txn().unwrap();
    let users = G::new(Arc::clone(&db), &txn)
.n_from_type("User").collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("users".to_string(), ReturnValue::from_traversal_value_array_with_mixin(G::new_from(Arc::clone(&db), &txn, users.clone())

.map_traversal(|item, txn| { exclude_field!(remapping_vals, item.clone(), "name", "email")?;
 Ok(item) }).collect_to::<Vec<_>>().clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct find_user_posts_with_creator_detailsInput {

pub userID: ID
}
#[handler]
pub fn find_user_posts_with_creator_details (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: find_user_posts_with_creator_detailsInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals = RemappingMap::new();
let db = Arc::clone(&input.graph.storage);
let txn = db.graph_env.read_txn().unwrap();
    let user = G::new(Arc::clone(&db), &txn)
.n_from_id(&data.userID).collect_to::<Vec<_>>();
    let posts = G::new_from(Arc::clone(&db), &txn, user.clone())

.out("Created",&EdgeType::Node).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("user".to_string(), ReturnValue::from_traversal_value_array_with_mixin(G::new_from(Arc::clone(&db), &txn, user.clone())

.map_traversal(|creator, txn| { traversal_remapping!(remapping_vals, creator.clone(), "creatorName" => G::new_from(Arc::clone(&db), &txn, vec![creator.clone()])

.check_property("name").collect_to_obj())?;
traversal_remapping!(remapping_vals, creator.clone(), "createdPosts" => G::new_from(Arc::clone(&db), &txn, posts.clone())

.map_traversal(|item, txn| { traversal_remapping!(remapping_vals, item.clone(), "postContent" => G::new_from(Arc::clone(&db), &txn, vec![item.clone()])

.check_property("content").collect_to_obj())?;
traversal_remapping!(remapping_vals, item.clone(), "createdAt" => G::new_from(Arc::clone(&db), &txn, vec![item.clone()])

.check_property("created_at").collect_to_obj())?;
traversal_remapping!(remapping_vals, item.clone(), "updatedAt" => G::new_from(Arc::clone(&db), &txn, vec![item.clone()])

.check_property("updated_at").collect_to_obj())?;
 Ok(item) }).collect_to::<Vec<_>>())?;
 Ok(creator) }).collect_to::<Vec<_>>().clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct create_followInput {

pub follower_id: ID,
pub followed_id: ID,
pub now: i32
}
#[handler]
pub fn create_follow (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: create_followInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals = RemappingMap::new();
let db = Arc::clone(&input.graph.storage);
let mut txn = db.graph_env.write_txn().unwrap();
    let follower = G::new(Arc::clone(&db), &txn)
.n_from_id(&data.follower_id).collect_to::<Vec<_>>();
    let followed = G::new(Arc::clone(&db), &txn)
.n_from_id(&data.followed_id).collect_to::<Vec<_>>();
    G::new_mut(Arc::clone(&db), &mut txn)
.add_e("Follows", Some(props! { "since" => data.now.clone() }), follower.id(), followed.id(), true, EdgeType::Node).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("success".to_string(), ReturnValue::from(Value::from("success")));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct create_postInput {

pub user_id: ID,
pub content: String,
pub now: i32
}
#[handler]
pub fn create_post (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: create_postInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals = RemappingMap::new();
let db = Arc::clone(&input.graph.storage);
let mut txn = db.graph_env.write_txn().unwrap();
    let user = G::new(Arc::clone(&db), &txn)
.n_from_id(&data.user_id).collect_to::<Vec<_>>();
    let post = G::new_mut(Arc::clone(&db), &mut txn)
.add_n("Post", Some(props! { "created_at" => data.now.clone(), "content" => data.content.clone(), "updated_at" => data.now.clone() }), None).collect_to::<Vec<_>>();
    G::new_mut(Arc::clone(&db), &mut txn)
.add_e("Created", Some(props! { "created_at" => data.now.clone() }), user.id(), post.id(), true, EdgeType::Node).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("post".to_string(), ReturnValue::from_traversal_value_array_with_mixin(post.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[handler]
pub fn get_name_remapping_simple (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let mut remapping_vals = RemappingMap::new();
let db = Arc::clone(&input.graph.storage);
let txn = db.graph_env.read_txn().unwrap();
    let users = G::new(Arc::clone(&db), &txn)
.n_from_type("User").collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("users".to_string(), ReturnValue::from_traversal_value_array_with_mixin(G::new_from(Arc::clone(&db), &txn, users.clone())

.map_traversal(|item, txn| { traversal_remapping!(remapping_vals, item.clone(), "givenName" => G::new_from(Arc::clone(&db), &txn, vec![item.clone()])

.check_property("name").collect_to_obj())?;
 Ok(item) }).collect_to::<Vec<_>>().clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}
