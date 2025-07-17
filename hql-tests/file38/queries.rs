use chrono::{DateTime, Utc};
use get_routes::handler;
use heed3::RoTxn;
use helix_db::helix_engine::vector_core::vector::HVector;
use helix_db::{
    embed, exclude_field, field_remapping, identifier_remapping, traversal_remapping,
    value_remapping,
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
    providers::embedding_providers::get_embedding_model,
};
use sonic_rs::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

pub struct User {
    pub name: String,
    pub age: i64,
    pub email: String,
}

#[derive(Serialize, Deserialize)]
pub struct addUserInput {
    pub name: String,
    pub age: i64,
    pub email: String,
}
#[handler]
pub fn addUser(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let data: addUserInput = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let mut remapping_vals = RemappingMap::new();
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();
    let user = G::new_mut(Arc::clone(&db), &mut txn)
        .insert_v::<fn(&HVector, &RoTxn) -> bool>(
            embed!(&data.name),
            "User",
            Some(props! { "age" => data.age, "email" => data.email, "name" => data.name }),
        )
        .collect_to::<Vec<_>>();
    let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
    return_vals.insert(
        "user".to_string(),
        ReturnValue::from_traversal_value_array_with_mixin(
            user.clone(),
            remapping_vals.borrow_mut(),
        ),
    );

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}
