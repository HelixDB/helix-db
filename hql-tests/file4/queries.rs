

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
    
pub struct File4 {
    pub name: String,
    pub age: i32,
}

pub struct EdgeFile4 {
    pub from: File4,
    pub to: File4,
}


#[handler]
pub fn file4 (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> = RefCell::new(HashMap::new());
let db = Arc::clone(&input.context.graph_access.storage);
let mut txn = db.graph_env.write_txn().unwrap();
    let user = G::new_mut(Arc::clone(&db), &mut txn)
.add_n("File4", Some(props! { "name" => "John", "age" => 20 }), None).collect_to::<Vec<_>>();
    let user2 = G::new(Arc::clone(&db), &txn)
.n_from_type("File4")

.out("EdgeFile4",&EdgeType::Node).collect_to::<Vec<_>>();
    let user3 = G::new(Arc::clone(&db), &txn)
.n_from_type("File4")

.in_("EdgeFile4",&EdgeType::Node).collect_to::<Vec<_>>();
    let edge1 = G::new(Arc::clone(&db), &txn)
.n_from_type("File4")

.out_e("EdgeFile4").collect_to::<Vec<_>>();
    let edge2 = G::new(Arc::clone(&db), &txn)
.n_from_type("File4")

.in_e("EdgeFile4").collect_to::<Vec<_>>();
    let user4 = G::new_from(Arc::clone(&db), &txn, user2.clone())

.out("EdgeFile4",&EdgeType::Node).collect_to::<Vec<_>>();
    let user5 = G::new_from(Arc::clone(&db), &txn, user3.clone())

.in_("EdgeFile4",&EdgeType::Node).collect_to::<Vec<_>>();
    let edge3 = G::new_from(Arc::clone(&db), &txn, user2.clone())

.out_e("EdgeFile4").collect_to::<Vec<_>>();
    let edge4 = G::new_from(Arc::clone(&db), &txn, user3.clone())

.in_e("EdgeFile4").collect_to::<Vec<_>>();
    let user6 = G::new_from(Arc::clone(&db), &txn, edge3.clone())

.from_n().collect_to::<Vec<_>>();
    let user7 = G::new_from(Arc::clone(&db), &txn, edge4.clone())

.to_n().collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("user".to_string(), ReturnValue::from_traversal_value_array_with_mixin(user.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}
