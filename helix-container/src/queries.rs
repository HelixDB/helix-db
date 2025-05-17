use std::collections::{HashMap, HashSet};
use std::cell::RefCell;
use std::sync::Arc;
use std::time::Instant;

use get_routes::handler;
use helixdb::helix_engine::vector_core::vector::HVector;
use helixdb::{
    node_matches,
    props,
    helix_engine::graph_core::ops::{
        g::G,
        in_::{in_::InAdapter, in_e::InEdgesAdapter, to_n::ToNAdapter},
        out::{from_n::FromNAdapter, out::OutAdapter, out_e::OutEdgesAdapter},
        vectors::{ insert::InsertVAdapter, search::SearchVAdapter},
        source::{add_e::{AddEAdapter, EdgeType}, add_n::AddNAdapter, e::EAdapter, e_from_id::EFromId, e_from_types::EFromTypes, n::NAdapter, n_from_id::NFromId, n_from_types::NFromTypesAdapter},
        tr_val::{TraversalVal, Traversable},
        util::{dedup::DedupAdapter, drop::DropAdapter, filter_mut::FilterMut, filter_ref::FilterRefAdapter, range::RangeAdapter, update::Update},
    },
    helix_engine::types::GraphError,
    helix_gateway::router::router::HandlerInput,
    protocol::count::Count,
    protocol::response::Response,
    protocol::traversal_value::TraversalValue,
    protocol::remapping::ResponseRemapping,
    protocol::{filterable::Filterable, value::Value, return_values::ReturnValue, remapping::Remapping},
};
use sonic_rs::{Deserialize, Serialize};

// Node Schema: Doc
#[derive(Serialize, Deserialize)]
struct Doc {
    content: String,
}

// Node Schema: Chunk
#[derive(Serialize, Deserialize)]
struct Chunk {
    content: String,
}

// Edge Schema: EmbeddingOf
#[derive(Serialize, Deserialize)]
struct EmbeddingOf {
}

#[handler]
pub fn ragsearchdocs(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct ragsearchdocsData {
        query: Vec<f64>,
        k: i32,
    }

    let data: ragsearchdocsData = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> = RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

        let tr = G::new(Arc::clone(&db), &txn)
        .search_v::<fn(&HVector) -> bool>(&data.query, data.k as usize, None)
;    let vec = tr.collect_to::<Vec<_>>();

        let tr = G::new_from(Arc::clone(&db), &txn, vec.clone())
.in_("EmbeddingOf")
;    let chunks = tr.collect_to::<Vec<_>>();

        let tr = G::new_from(Arc::clone(&db), &txn, chunks.clone())
        ;let tr = tr.map(|item| {
    match item {
    Ok(ref item) => {
    let content = item.check_property("content");
        let content_remapping = Remapping::new(false, None, Some(
                        match content {
                            Some(value) => ReturnValue::from(value.clone()),
                            None => return Err(GraphError::ConversionError(
                                "Property not found on content".to_string(),
                            )),
                        }
                    ));remapping_vals.borrow_mut().insert(
    item.id().clone(),
    ResponseRemapping::new(
    HashMap::from([
("content".to_string(), content_remapping),
    ]),    false    ),    );        }    Err(e) => {
    println!("Error: {:?}", e);
    return Err(GraphError::ConversionError("Error: {:?}".to_string()))    }};    item}).filter_map(|item| item.ok());
    let return_val = tr.collect::<Vec<_>>();
    return_vals.insert("chunks".to_string(), ReturnValue::from_traversal_value_array_with_mixin(return_val, remapping_vals.borrow_mut()));
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn ragloaddocs(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct ragloaddocsData {
        docs: Vec<docsData>,
    }

    #[derive(Serialize, Deserialize)]
    struct docsData {
        vectors: Vec<vectorsData>
,
        doc: String,
    }

    #[derive(Serialize, Deserialize)]
    struct vectorsData {
        vec: Vec<f64>
,
        chunk: String,
    }

    let data: ragloaddocsData = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> = RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

for data in data.docs {
        let tr = G::new_mut(Arc::clone(&db), &mut txn)
    .add_n("Doc", props!{ "content".to_string() => data.doc }, None, None);    let doc_node = tr.collect_to::<Vec<_>>();

for data in data.vectors {
        let tr = G::new_mut(Arc::clone(&db), &mut txn)
    .insert_v::<fn(&HVector) -> bool>(&data.vec, None);
        let vec = tr.collect_to::<Vec<_>>();

        let tr = G::new_mut(Arc::clone(&db), &mut txn)
    .add_n("Chunk", props!{ "content".to_string() => data.chunk }, None, None);    let chunk_node = tr.collect_to::<Vec<_>>();

    let tr = G::new_mut(Arc::clone(&db), &mut txn)
    .add_e("Contains", props!{}, None, doc_node.id(), chunk_node.id(), true, EdgeType::Vec);
let _ = tr.collect_to::<Vec<_>>();

    let tr = G::new_mut(Arc::clone(&db), &mut txn)
    .add_e("EmbeddingOf", props!{}, None, chunk_node.id(), vec.id(), true, EdgeType::Vec);
let _ = tr.collect_to::<Vec<_>>();

    }
    }
    return_vals.insert("message".to_string(), ReturnValue::from("Success"));
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    txn.commit()?;
    Ok(())
}

