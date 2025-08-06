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

pub struct Entity {
    pub entity_name: String,
}

pub struct Relationship {
    pub from: Entity,
    pub to: Entity,
    pub edge_name: String,
}

#[derive(Serialize, Deserialize)]
pub struct insert_entityInput {
    pub entity_name_in: String,
}
#[handler]
pub fn insert_entity(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let data: insert_entityInput = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let mut remapping_vals = RemappingMap::new();
    let db = Arc::clone(&input.context.graph_access.storage);
    let mut txn = db.graph_env.write_txn().unwrap();
    let node = G::new_mut(Arc::clone(&db), &mut txn)
        .add_n(
            "Entity",
            Some(props! { "entity_name" => data.entity_name_in.clone() }),
            Some(&["entity_name"]),
        )
        .collect_to::<Vec<_>>();
    let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
    return_vals.insert(
        "node".to_string(),
        ReturnValue::from_traversal_value_array_with_mixin(
            node.clone(),
            remapping_vals.borrow_mut(),
        ),
    );

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct get_entityInput {
    pub entity_name_in: String,
}
#[handler]
pub fn get_entity(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let data: get_entityInput = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let mut remapping_vals = RemappingMap::new();
    let db = Arc::clone(&input.context.graph_access.storage);
    let txn = db.graph_env.read_txn().unwrap();
    let node = G::new(Arc::clone(&db), &txn)
        .n_from_type("Entity")
        .filter_ref(|val, txn| {
            if let Ok(val) = val {
                Ok(G::new_from(Arc::clone(&db), &txn, val.clone())
                    .check_property("entity_name")
                    .map_value_or(false, |v| *v == data.entity_name_in)?)
            } else {
                Ok(false)
            }
        })
        .collect_to::<Vec<_>>();
    let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
    return_vals.insert(
        "node".to_string(),
        ReturnValue::from_traversal_value_array_with_mixin(
            node.clone(),
            remapping_vals.borrow_mut(),
        ),
    );

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct insert_relationshipInput {
    pub from_entity_label: String,
    pub to_entity_label: String,
    pub edge_name_in: String,
}
#[handler]
pub fn insert_relationship(
    input: &HandlerInput,
    response: &mut Response,
) -> Result<(), GraphError> {
    let data: insert_relationshipInput = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let mut remapping_vals = RemappingMap::new();
    let db = Arc::clone(&input.context.graph_access.storage);
    let mut txn = db.graph_env.write_txn().unwrap();
    let from_entity = G::new(Arc::clone(&db), &txn)
        .n_from_index("entity_name", &data.from_entity_label)
        .collect_to::<Vec<_>>();
    let to_entity = G::new(Arc::clone(&db), &txn)
        .n_from_index("entity_name", &data.to_entity_label)
        .collect_to::<Vec<_>>();
    let e = G::new_mut(Arc::clone(&db), &mut txn)
        .add_e(
            "Relationship",
            Some(props! { "edge_name" => data.edge_name_in.clone() }),
            from_entity.id(),
            to_entity.id(),
            true,
            EdgeType::Node,
        )
        .collect_to::<Vec<_>>();
    let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
    return_vals.insert(
        "e".to_string(),
        ReturnValue::from_traversal_value_array_with_mixin(e.clone(), remapping_vals.borrow_mut()),
    );

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}
