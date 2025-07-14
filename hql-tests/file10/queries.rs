

use heed3::RoTxn;
use get_routes::handler;
use helix_db::{field_remapping, identifier_remapping, traversal_remapping, exclude_field};
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
        vectors::{insert::InsertVAdapter, search::SearchVAdapter},
        bm25::search_bm25::SearchBM25Adapter,
    },
    helix_engine::types::GraphError,
    helix_gateway::router::router::HandlerInput,
    node_matches, props,
    protocol::count::Count,
    protocol::remapping::ResponseRemapping,
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
    
pub struct File9 {
    pub name: String,
    pub age: i32,
    pub count: f32,
}

pub struct EFile9 {
    pub from: File9,
    pub to: File9,
}


#[derive(Serialize, Deserialize)]
pub struct file9Input {

pub other_id: ID,
pub id: ID
}
#[handler]
pub fn file9 (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: file9Input = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> = RefCell::new(HashMap::new());
let db = Arc::clone(&input.graph.storage);
let txn = db.graph_env.read_txn().unwrap();
    let path1 = G::new(Arc::clone(&db), &txn)
.n_from_id(&data.id)

.shortest_path(Some("File9"), None, Some(&data.other_id)).collect_to::<Vec<_>>();
    let path2 = G::new(Arc::clone(&db), &txn)
.n_from_id(&data.id)

.shortest_path(Some("File9"), Some(&data.other_id), None).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("path1".to_string(), ReturnValue::from_traversal_value_array_with_mixin(path1.clone(), remapping_vals.borrow_mut()));

        return_vals.insert("path2".to_string(), ReturnValue::from_traversal_value_array_with_mixin(path2.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}
