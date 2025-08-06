

use heed3::RoTxn;
use get_routes::handler;
use helix_db::{field_remapping, identifier_remapping, traversal_remapping, exclude_field, value_remapping};
use helix_db::helix_engine::vector_core::vector::HVector;
use helix_db::{
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
    
pub struct Doc {
    pub content: String,
    pub number: i32,
}

pub struct Chunk {
    pub content: String,
}

pub struct EmbeddingOf {
    pub from: Doc,
    pub to: Embedding,
}

pub struct Embedding {
    pub chunk: String,
    pub chunk_id: i32,
    pub number: i32,
    pub reference: String,
}

#[derive(Serialize, Deserialize)]
pub struct searchEmbeddingInput {

pub query: Vec<f64>
}
#[handler]
pub fn searchEmbedding (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: searchEmbeddingInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals = RemappingMap::new();
let db = Arc::clone(&input.context.graph_access.storage);
let txn = db.graph_env.read_txn().unwrap();
    let c = G::new(Arc::clone(&db), &txn)
.n_from_index("number", &1).collect_to::<Vec<_>>();
    let embedding_search = G::new(Arc::clone(&db), &txn)
.search_v::<fn(&HVector, &RoTxn) -> bool>(&data.query, 10, None).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("embedding_search".to_string(), ReturnValue::from_traversal_value_array_with_mixin(G::new_from(Arc::clone(&db), &txn, embedding_search.clone())

.map_traversal(|item, txn| { traversal_remapping!(remapping_vals, item.clone(), "chunk" => G::new_from(Arc::clone(&db), &txn, vec![item.clone()])

.check_property("chunk").collect_to::<Vec<_>>())?;
traversal_remapping!(remapping_vals, item.clone(), "chunk_id" => G::new_from(Arc::clone(&db), &txn, vec![item.clone()])

.check_property("chunk_id").collect_to::<Vec<_>>())?;
traversal_remapping!(remapping_vals, item.clone(), "number" => G::new_from(Arc::clone(&db), &txn, vec![item.clone()])

.check_property("number").collect_to::<Vec<_>>())?;
traversal_remapping!(remapping_vals, item.clone(), "reference" => G::new_from(Arc::clone(&db), &txn, vec![item.clone()])

.check_property("reference").collect_to::<Vec<_>>())?;
 Ok(item) }).collect_to::<Vec<_>>().clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct addEmbeddingInput {

pub vec: Vec<f64>
}
#[handler]
pub fn addEmbedding (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: addEmbeddingInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals = RemappingMap::new();
let db = Arc::clone(&input.context.graph_access.storage);
let mut txn = db.graph_env.write_txn().unwrap();
    let doc = G::new_mut(Arc::clone(&db), &mut txn)
.add_n("Doc", Some(props! { "number" => 1, "content" => "Hello, content!" }), Some(&["number"])).collect_to::<Vec<_>>();
    let embedding = G::new_mut(Arc::clone(&db), &mut txn)
.insert_v::<fn(&HVector, &RoTxn) -> bool>(&data.vec, "Embedding", Some(props! { "chunk_id" => 1, "number" => 1, "chunk" => "Hello, chunk!", "reference" => "Hello, reference!" })).collect_to::<Vec<_>>();
    G::new_mut(Arc::clone(&db), &mut txn)
.add_e("EmbeddingOf", None, doc.id(), embedding.id(), true, EdgeType::Node).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("embedding".to_string(), ReturnValue::from_traversal_value_array_with_mixin(embedding.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[handler]
pub fn getAllEmbedding (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let mut remapping_vals = RemappingMap::new();
let db = Arc::clone(&input.context.graph_access.storage);
let txn = db.graph_env.read_txn().unwrap();
    let c = G::new(Arc::clone(&db), &txn)
.n_from_index("number", &1).collect_to::<Vec<_>>();
    let embeddings = G::new_from(Arc::clone(&db), &txn, c.clone())

.out("EmbeddingOf",&EdgeType::Vec).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("embeddings".to_string(), ReturnValue::from_traversal_value_array_with_mixin(embeddings.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}
