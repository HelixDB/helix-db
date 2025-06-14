

use heed3::RoTxn;
use get_routes::handler;
use helixdb::{field_remapping, identifier_remapping, traversal_remapping, exclude_field};
use helixdb::helix_engine::vector_core::vector::HVector;
use helixdb::{
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
    
pub struct Patient {
    pub name: String,
    pub age: i64,
}

pub struct Doctor {
    pub name: String,
    pub city: String,
}

pub struct Visit {
    pub from: Patient,
    pub to: Doctor,
    pub doctors_summary: String,
    pub date: i64,
}


#[derive(Serialize, Deserialize)]
pub struct get_patients_visits_in_previous_monthInput {

pub name: String,
pub date: i64
}
#[handler]
pub fn get_patients_visits_in_previous_month (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: get_patients_visits_in_previous_monthInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> = RefCell::new(HashMap::new());
let db = Arc::clone(&input.graph.storage);
let txn = db.graph_env.read_txn().unwrap();
    let patient = G::new(Arc::clone(&db), &txn)
.n_from_type("Patient")

.filter_ref(|val, txn|{
                if let Ok(val) = val { 
                    Ok(val

.check_property("name")

.map_or(false, |v| *v == data.name))
                } else {
                    Ok(false)
                }
            }).collect_to::<Vec<_>>();
    let visits = G::new_from(Arc::clone(&db), &txn, patient.clone())

.out_e("Visit")

.filter_ref(|val, txn|{
                if let Ok(val) = val { 
                    Ok(val

.check_property("date")

.map_or(false, |v| *v >= data.date))
                } else {
                    Ok(false)
                }
            }).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("visits".to_string(), ReturnValue::from_traversal_value_array_with_mixin(visits.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct create_dataInput {

pub doctor_name: String,
pub doctor_city: String,
pub patient_name: String,
pub patient_age: i64,
pub summary: String,
pub date: i64
}
#[handler]
pub fn create_data (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: create_dataInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> = RefCell::new(HashMap::new());
let db = Arc::clone(&input.graph.storage);
let mut txn = db.graph_env.write_txn().unwrap();
    let doctor = G::new_mut(Arc::clone(&db), &mut txn)
.add_n("Doctor", Some(props! { "name" => data.doctor_name.clone(), "city" => data.doctor_city.clone() }), None).collect_to::<Vec<_>>();
    let patient = G::new_mut(Arc::clone(&db), &mut txn)
.add_n("Patient", Some(props! { "age" => data.patient_age.clone(), "name" => data.patient_name.clone() }), None).collect_to::<Vec<_>>();
    G::new_mut(Arc::clone(&db), &mut txn)
.add_e("Visit", Some(props! { "date" => data.date.clone(), "doctors_summary" => data.summary.clone() }), patient.id(), doctor.id(), true, EdgeType::Node).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("patient".to_string(), ReturnValue::from_traversal_value_array_with_mixin(patient.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct get_visit_by_dateInput {

pub name: String,
pub date: i64
}
#[handler]
pub fn get_visit_by_date (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: get_visit_by_dateInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> = RefCell::new(HashMap::new());
let db = Arc::clone(&input.graph.storage);
let txn = db.graph_env.read_txn().unwrap();
    let patients = G::new(Arc::clone(&db), &txn)
.n_from_type("Patient").collect_to::<Vec<_>>();
    let patient = G::new_from(Arc::clone(&db), &txn, patients.clone())

.filter_ref(|val, txn|{
                if let Ok(val) = val { 
                    Ok(val

.check_property("name")

.map_or(false, |v| *v == data.name))
                } else {
                    Ok(false)
                }
            }).collect_to::<Vec<_>>();
    let visit = G::new_from(Arc::clone(&db), &txn, patient.clone())

.out_e("Visit")

.filter_ref(|val, txn|{
                if let Ok(val) = val { 
                    Ok(val

.check_property("date")

.map_or(false, |v| *v == data.date))
                } else {
                    Ok(false)
                }
            }).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("patient".to_string(), ReturnValue::from_traversal_value_array_with_mixin(patient.clone(), remapping_vals.borrow_mut()));

        return_vals.insert("visit".to_string(), ReturnValue::from_traversal_value_array_with_mixin(visit.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct get_patientInput {

pub name: String
}
#[handler]
pub fn get_patient (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: get_patientInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> = RefCell::new(HashMap::new());
let db = Arc::clone(&input.graph.storage);
let txn = db.graph_env.read_txn().unwrap();
    let patient = G::new(Arc::clone(&db), &txn)
.n_from_type("Patient")

.filter_ref(|val, txn|{
                if let Ok(val) = val { 
                    Ok(val

.check_property("name")

.map_or(false, |v| *v == data.name))
                } else {
                    Ok(false)
                }
            }).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("patient".to_string(), ReturnValue::from_traversal_value_array_with_mixin(patient.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}
