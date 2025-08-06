

use heed3::RoTxn;
use get_routes::handler;
use helix_db::{field_remapping, identifier_remapping, traversal_remapping, exclude_field, value_remapping};
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
};
use sonic_rs::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use std::cell::RefCell;
use chrono::{DateTime, Utc};
    
pub struct Company {
    pub company_number: String,
    pub number_of_filings: i32,
}

pub struct DocumentEdge {
    pub from: Company,
    pub to: DocumentEmbedding,
    pub filing_id: String,
    pub category: String,
    pub subcategory: String,
    pub date: String,
    pub description: String,
}

pub struct DocumentEmbedding {
    pub text: String,
    pub chunk_id: String,
    pub page_number: u16,
    pub reference: String,
    pub source_link: String,
    pub source_date: String,
}

#[derive(Serialize, Deserialize)]
pub struct GetAllCompanyEmbeddingsInput {

pub company_number: String
}
#[handler]
pub fn GetAllCompanyEmbeddings (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: GetAllCompanyEmbeddingsInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals = RemappingMap::new();
let db = Arc::clone(&input.context.graph_access.storage);
let txn = db.graph_env.read_txn().unwrap();
    let c = G::new(Arc::clone(&db), &txn)
.n_from_index("company_number", &data.company_number).collect_to::<Vec<_>>();
    let embeddings = G::new_from(Arc::clone(&db), &txn, c.clone())

.out("DocumentEdge",&EdgeType::Vec).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("embeddings".to_string(), ReturnValue::from_traversal_value_array_with_mixin(embeddings.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct AddVectorInput {

pub vector: Vec<f64>,
pub text: String,
pub chunk_id: String,
pub page_number: i32,
pub reference: String
}
#[handler]
pub fn AddVector (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: AddVectorInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals = RemappingMap::new();
let db = Arc::clone(&input.context.graph_access.storage);
let mut txn = db.graph_env.write_txn().unwrap();
    let embedding = G::new_mut(Arc::clone(&db), &mut txn)
.insert_v::<fn(&HVector, &RoTxn) -> bool>(&data.vector, "DocumentEmbedding", Some(props! { "text" => data.text, "page_number" => data.page_number, "chunk_id" => data.chunk_id, "reference" => data.reference })).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("embedding".to_string(), ReturnValue::from_traversal_value_array_with_mixin(embedding.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct AddCompanyInput {

pub company_number: String,
pub number_of_filings: i32
}
#[handler]
pub fn AddCompany (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: AddCompanyInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals = RemappingMap::new();
let db = Arc::clone(&input.context.graph_access.storage);
let mut txn = db.graph_env.write_txn().unwrap();
    let company = G::new_mut(Arc::clone(&db), &mut txn)
.add_n("Company", Some(props! { "number_of_filings" => data.number_of_filings.clone(), "company_number" => data.company_number.clone() }), Some(&["company_number"])).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("company".to_string(), ReturnValue::from_traversal_value_array_with_mixin(company.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[handler]
pub fn DeleteAll (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let mut remapping_vals = RemappingMap::new();
let db = Arc::clone(&input.context.graph_access.storage);
let mut txn = db.graph_env.write_txn().unwrap();
    Drop::<Vec<_>>::drop_traversal(
                G::new(Arc::clone(&db), &txn)
.n_from_type("Company").collect::<Vec<_>>(),
                Arc::clone(&db),
                &mut txn,
            )?;;
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("success".to_string(), ReturnValue::from(Value::from("success")));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct HasCompanyInput {

pub company_number: String
}
#[handler]
pub fn HasCompany (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: HasCompanyInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals = RemappingMap::new();
let db = Arc::clone(&input.context.graph_access.storage);
let txn = db.graph_env.read_txn().unwrap();
    let company = G::new(Arc::clone(&db), &txn)
.n_from_index("company_number", &data.company_number).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("company".to_string(), ReturnValue::from_traversal_value_array_with_mixin(company.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct DeleteCompanyInput {

pub company_number: String
}
#[handler]
pub fn DeleteCompany (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: DeleteCompanyInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals = RemappingMap::new();
let db = Arc::clone(&input.context.graph_access.storage);
let mut txn = db.graph_env.write_txn().unwrap();
    Drop::<Vec<_>>::drop_traversal(
                G::new(Arc::clone(&db), &txn)
.n_from_index("company_number", &data.company_number)

.out("DocumentEdge",&EdgeType::Vec).collect::<Vec<_>>(),
                Arc::clone(&db),
                &mut txn,
            )?;;
    Drop::<Vec<_>>::drop_traversal(
                G::new(Arc::clone(&db), &txn)
.n_from_index("company_number", &data.company_number).collect::<Vec<_>>(),
                Arc::clone(&db),
                &mut txn,
            )?;;
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("success".to_string(), ReturnValue::from(Value::from("success")));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct HasDocumentEmbeddingsInput {

pub company_number: String
}
#[handler]
pub fn HasDocumentEmbeddings (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: HasDocumentEmbeddingsInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals = RemappingMap::new();
let db = Arc::clone(&input.context.graph_access.storage);
let txn = db.graph_env.read_txn().unwrap();
    let c = G::new(Arc::clone(&db), &txn)
.n_from_index("company_number", &data.company_number).collect_to::<Vec<_>>();
    let embeddings = G::new_from(Arc::clone(&db), &txn, c.clone())

.out("DocumentEdge",&EdgeType::Vec).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("embeddings".to_string(), ReturnValue::from_traversal_value_array_with_mixin(embeddings.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[handler]
pub fn GetCompanies (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let mut remapping_vals = RemappingMap::new();
let db = Arc::clone(&input.context.graph_access.storage);
let txn = db.graph_env.read_txn().unwrap();
    let companies = G::new(Arc::clone(&db), &txn)
.n_from_type("Company").collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("companies".to_string(), ReturnValue::from_traversal_value_array_with_mixin(companies.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct embeddings_dataData {
    pub category: String,
    pub subcategory: String,
    pub reference: String,
    pub date1: String,
    pub source: String,
    pub chunk_id: String,
    pub description: String,
    pub filing_id: String,
    pub vector: Vec<f64>,
    pub page_number: i32,
    pub date2: String,
    pub text: String,
}
#[derive(Serialize, Deserialize)]
pub struct AddEmbeddingsToCompanyInput {

pub company_number: String,
pub embeddings_data: Vec<embeddings_dataData>
}
#[handler]
pub fn AddEmbeddingsToCompany (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: AddEmbeddingsToCompanyInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals = RemappingMap::new();
let db = Arc::clone(&input.context.graph_access.storage);
let mut txn = db.graph_env.write_txn().unwrap();
    let c = G::new(Arc::clone(&db), &txn)
.n_from_index("company_number", &data.company_number).collect_to::<Vec<_>>();
    for data in data.embeddings_data {
    let embedding = G::new_mut(Arc::clone(&db), &mut txn)
.insert_v::<fn(&HVector, &RoTxn) -> bool>(&data.vector, "DocumentEmbedding", Some(props! { "source_date" => data.date1, "source_link" => data.source, "page_number" => data.page_number, "reference" => data.reference, "text" => data.text, "chunk_id" => data.chunk_id })).collect_to::<Vec<_>>();
    let edges = G::new_mut(Arc::clone(&db), &mut txn)
.add_e("DocumentEdge", Some(props! { "filing_id" => data.filing_id.clone(), "date" => data.date2.clone(), "subcategory" => data.subcategory.clone(), "category" => data.category.clone(), "description" => data.description.clone() }), c.id(), embedding.id(), true, EdgeType::Node).collect_to::<Vec<_>>();
}
;
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("success".to_string(), ReturnValue::from(Value::from("success")));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct SearchVectorInput {

pub query: Vec<f64>,
pub k: i32
}
#[handler]
pub fn SearchVector (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: SearchVectorInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals = RemappingMap::new();
let db = Arc::clone(&input.context.graph_access.storage);
let txn = db.graph_env.read_txn().unwrap();
    let embedding_search = G::new(Arc::clone(&db), &txn)
.search_v::<fn(&HVector, &RoTxn) -> bool>(&data.query, data.k as usize, None).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("embedding_search".to_string(), ReturnValue::from_traversal_value_array_with_mixin(embedding_search.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct CompanyEmbeddingSearchInput {

pub company_number: String,
pub query: Vec<f64>,
pub k: i32
}
#[handler]
pub fn CompanyEmbeddingSearch (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: CompanyEmbeddingSearchInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals = RemappingMap::new();
let db = Arc::clone(&input.context.graph_access.storage);
let txn = db.graph_env.read_txn().unwrap();
    let c = G::new(Arc::clone(&db), &txn)
.n_from_index("company_number", &data.company_number)

.out_e("DocumentEdge")

.to_v().collect_to::<Vec<_>>();
    let embedding_search = G::new_from(Arc::clone(&db), &txn, c.clone())

.brute_force_search_v(&data.query, data.k as usize).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("embedding_search".to_string(), ReturnValue::from_traversal_value_array_with_mixin(embedding_search.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}
