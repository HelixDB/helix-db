use chrono::{DateTime, Utc};
use get_routes::handler;
use heed3::RoTxn;
use helix_db::helix_engine::vector_core::vector::HVector;
use helix_db::{
    exclude_field, field_remapping, identifier_remapping, traversal_remapping, value_remapping,
};
use helix_db::{
    helix_engine::graph_core::ops::{
        bm25::search_bm25::SearchBM25Adapter,
        g::G,
        in_::{in_::InAdapter, in_e::InEdgesAdapter, to_n::ToNAdapter, to_v::ToVAdapter},
        out::{
            from_n::FromNAdapter, from_v::FromVAdapter, out::OutAdapter, out_e::OutEdgesAdapter,
        },
        source::{
            add_e::{AddEAdapter, EdgeType},
            add_n::AddNAdapter,
            e_from_id::EFromIdAdapter,
            e_from_type::EFromTypeAdapter,
            n_from_id::NFromIdAdapter,
            n_from_index::NFromIndexAdapter,
            n_from_type::NFromTypeAdapter,
        },
        tr_val::{Traversable, TraversalVal},
        util::{
            dedup::DedupAdapter, drop::Drop, filter_mut::FilterMut, filter_ref::FilterRefAdapter,
            map::MapAdapter, paths::ShortestPathAdapter, props::PropsAdapter, range::RangeAdapter,
            update::UpdateAdapter,
        },
        vectors::{
            brute_force_search::BruteForceSearchVAdapter, insert::InsertVAdapter,
            search::SearchVAdapter,
        },
    },
    helix_engine::types::GraphError,
    helix_gateway::router::router::HandlerInput,
    node_matches, props,
    protocol::count::Count,
    protocol::remapping::{RemappingMap, ResponseRemapping},
    protocol::response::Response,
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

pub struct User {
    pub name: String,
    pub age: i32,
}

#[handler]
pub fn GetOrder(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let mut remapping_vals = RemappingMap::new();
    let db = Arc::clone(&input.context.graph_access.storage);
    let txn = db.graph_env.read_txn().unwrap();
    let userByAge = G::new(Arc::clone(&db), &txn)
        .n_from_type("User")
        .order_by_desc("age")?;
    let userByName = G::new(Arc::clone(&db), &txn)
        .n_from_type("User")
        .order_by_asc("name")?;
    let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
    return_vals.insert(
        "userByAge".to_string(),
        ReturnValue::from_traversal_value_array_with_mixin(
            userByAge.clone(),
            remapping_vals.borrow_mut(),
        ),
    );

    return_vals.insert(
        "userByName".to_string(),
        ReturnValue::from_traversal_value_array_with_mixin(
            userByName.clone(),
            remapping_vals.borrow_mut(),
        ),
    );

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}
