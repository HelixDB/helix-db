

use heed3::RoTxn;
use get_routes::handler;
use helix_db::{field_remapping, identifier_remapping, traversal_remapping, exclude_field, value_remapping, embed};
use helix_db::helix_engine::vector_core::vector::HVector;
use helix_db::{
    helix_engine::graph_core::ops::{
        g::G,
        in_::{in_::InAdapter, in_e::InEdgesAdapter, to_n::ToNAdapter, to_v::ToVAdapter},
        out::{from_n::FromNAdapter, from_v::FromVAdapter, out::OutAdapter, out_e::OutEdgesAdapter},
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
    providers::embedding_providers::get_embedding_model,
};
use sonic_rs::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use std::cell::RefCell;
use chrono::{DateTime, Utc};
    
pub struct Professor {
    pub name: String,
    pub title: String,
    pub page: String,
    pub bio: String,
}

pub struct HasResearchAreaAndDescriptionEmbedding {
    pub from: Professor,
    pub to: ResearchAreaAndDescriptionEmbedding,
    pub areas_and_descriptions: String,
}

pub struct ResearchAreaAndDescriptionEmbedding {
    pub areas_and_descriptions: String,
}

#[derive(Serialize, Deserialize)]
pub struct get_professor_research_areas_with_descriptions_v2Input {

pub professor_id: ID
}
#[handler]
pub fn get_professor_research_areas_with_descriptions_v2 (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: get_professor_research_areas_with_descriptions_v2Input = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals = RemappingMap::new();
let db = Arc::clone(&input.context.graph_access.storage);
let txn = db.graph_env.read_txn().unwrap();
    let research_areas = G::new(Arc::clone(&db), &txn)
.n_from_id(&data.professor_id)

.out("HasResearchAreaAndDescriptionEmbedding",&EdgeType::Vec)

.check_property("areas_and_descriptions").collect_to_obj();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("research_areas".to_string(), ReturnValue::from_traversal_value_array_with_mixin(G::new_from(Arc::clone(&db), &txn, research_areas.clone())

.map_traversal(|item, txn| { traversal_remapping!(remapping_vals, item.clone(), "areas_and_descriptions" => G::new_from(Arc::clone(&db), &txn, vec![item.clone()])

.check_property("areas_and_descriptions").collect_to_obj())?;
 Ok(item) }).collect_to::<Vec<_>>().clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct get_professor_research_areas_with_descriptions_v1Input {

pub professor_id: ID
}
#[handler]
pub fn get_professor_research_areas_with_descriptions_v1 (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: get_professor_research_areas_with_descriptions_v1Input = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals = RemappingMap::new();
let db = Arc::clone(&input.context.graph_access.storage);
let txn = db.graph_env.read_txn().unwrap();
    let research_areas = G::new(Arc::clone(&db), &txn)
.n_from_id(&data.professor_id)

.out("HasResearchAreaAndDescriptionEmbedding",&EdgeType::Vec).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("research_areas".to_string(), ReturnValue::from_traversal_value_array_with_mixin(G::new_from(Arc::clone(&db), &txn, research_areas.clone())

.check_property("areas_and_descriptions").collect_to_obj().clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}
