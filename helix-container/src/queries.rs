use get_routes::handler;
use heed3::RoTxn;
use helixdb::helix_engine::vector_core::vector::HVector;
use helixdb::{exclude_field, field_remapping, identifier_remapping, traversal_remapping};
use helixdb::{
    helix_engine::graph_core::ops::{
        g::G,
        in_::{in_::InAdapter, in_e::InEdgesAdapter, to_n::ToNAdapter},
        out::{from_n::FromNAdapter, out::OutAdapter, out_e::OutEdgesAdapter},
        source::{
            add_e::{AddEAdapter, EdgeType},
            add_n::AddNAdapter,
            e::EAdapter,
            e_from_id::EFromIdAdapter,
            e_from_type::EFromTypeAdapter,
            n::NAdapter,
            n_from_id::NFromIdAdapter,
            n_from_index::NFromIndexAdapter,
            n_from_type::NFromTypeAdapter,
        },
        tr_val::{Traversable, TraversalVal},
        util::{
            dedup::DedupAdapter, drop::Drop, drop::DropAdapter, filter_mut::FilterMut,
            filter_ref::FilterRefAdapter, map::MapAdapter, paths::ShortestPathAdapter,
            props::PropsAdapter, range::RangeAdapter, update::UpdateAdapter,
        },
        vectors::{insert::InsertVAdapter, search::SearchVAdapter},
    },
    helix_engine::types::GraphError,
    helix_gateway::router::router::HandlerInput,
    node_matches, props,
    protocol::count::Count,
    protocol::remapping::ResponseRemapping,
    protocol::response::Response,
    protocol::traversal_value::TraversalValue,
    protocol::{
        filterable::Filterable, id::ID, remapping::Remapping, return_values::ReturnValue,
        value::Value,
    },
};
use sonic_rs::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

pub struct File9 {
    pub name: String,
    pub age: i32,
}

#[derive(Serialize, Deserialize)]
pub struct file9Input {
    pub name: String,
    pub id: ID,
}
#[handler]
pub fn file9(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let data: file9Input = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();
    let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
    let user = G::new(Arc::clone(&db), &txn)
        .n_from_id(&data.id)
        .collect_to::<Vec<_>>();
    let node = G::new(Arc::clone(&db), &txn)
        .n_from_index("name", &data.name)
        .collect_to::<Vec<_>>();
    let node_by_name = G::new(Arc::clone(&db), &txn)
        .n_from_index("age", &20)
        .collect_to::<Vec<_>>();
    return_vals.insert(
        "user".to_string(),
        ReturnValue::from_traversal_value_array_with_mixin(user, remapping_vals.borrow_mut()),
    );

    return_vals.insert(
        "node".to_string(),
        ReturnValue::from_traversal_value_array_with_mixin(node, remapping_vals.borrow_mut()),
    );

    return_vals.insert(
        "node_by_name".to_string(),
        ReturnValue::from_traversal_value_array_with_mixin(
            node_by_name,
            remapping_vals.borrow_mut(),
        ),
    );

    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}
