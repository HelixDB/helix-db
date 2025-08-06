

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
    
pub struct File2 {
    pub name: String,
    pub is_admin: bool,
    pub f1: i8,
    pub f2: i16,
    pub f3: i32,
    pub f4: i64,
    pub f5: f32,
    pub f6: f64,
    pub f7: String,
    pub f8: u8,
    pub f9: u16,
    pub f10: u32,
    pub f11: u64,
    pub f12: u128,
}

pub struct EdgeFile2 {
    pub from: File2,
    pub to: File2,
    pub name: String,
    pub is_admin: bool,
    pub f1: i8,
    pub f2: i16,
    pub f3: i32,
    pub f4: i64,
    pub f5: f32,
    pub f6: f64,
    pub f7: String,
    pub f8: u8,
    pub f9: u16,
    pub f10: u32,
    pub f11: u64,
    pub f12: u128,
}


#[derive(Serialize, Deserialize)]
pub struct file2Input {

pub name: String
}
#[handler]
pub fn file2 (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: file2Input = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> = RefCell::new(HashMap::new());
let db = Arc::clone(&input.context.graph_access.storage);
let mut txn = db.graph_env.write_txn().unwrap();
    let user = G::new_mut(Arc::clone(&db), &mut txn)
.add_n("File2", Some(props! { "f7" => "7", "f4" => 4, "f2" => 2, "f3" => 3, "f6" => 6.0, "is_admin" => true, "f10" => 10, "name" => data.name.clone(), "f9" => 9, "f1" => 1, "f11" => 11, "f5" => 5.0, "f12" => 12, "f8" => 8 }), None).collect_to::<Vec<_>>();
    let user2 = G::new_mut(Arc::clone(&db), &mut txn)
.add_n("File2", Some(props! { "f4" => 4, "f1" => 1, "f8" => 8, "f3" => 3, "name" => data.name.clone(), "f7" => "7", "f11" => 11, "f5" => 5.0, "is_admin" => true, "f9" => 9, "f2" => 2, "f12" => 12, "f10" => 10, "f6" => 6.5 }), None).collect_to::<Vec<_>>();
    G::new_mut(Arc::clone(&db), &mut txn)
.add_e("EdgeFile2", Some(props! { "f10" => 10, "f8" => 8, "f11" => 11, "f12" => 12, "f7" => "7", "f4" => 4, "name" => data.name.clone(), "is_admin" => true, "f6" => 6.0, "f5" => 5.3, "f2" => 2, "f1" => 1, "f3" => 3, "f9" => 9 }), user.id(), user2.id(), true, EdgeType::Node).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("user".to_string(), ReturnValue::from_traversal_value_array_with_mixin(user.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}
