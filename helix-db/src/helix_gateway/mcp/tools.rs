use crate::{
    debug_println,
    helix_engine::{
        helix_gateway::embedding_providers::embedding_providers::{
            EmbeddingModel, get_embedding_model,
        },
        storage_core::HelixGraphStorage,
        traversal_core::{
            bm25::bm25::{BM25, BM25Flatten, HBM25Config},
            storage_core::HelixGraphStorage,
            traversal_core::{
                ops::{
                    bm25::search_bm25::SearchBM25Adapter,
                    g::G,
                    in_::{
                        in_::{InAdapter, InNodesIterator},
                        in_e::{InEdgesAdapter, InEdgesIterator},
                    },
                    out::{
                        out::{OutAdapter, OutNodesIterator},
                        out_e::{OutEdgesAdapter, OutEdgesIterator},
                    },
                    source::{add_e::EdgeType, e_from_type::EFromType, n_from_type::NFromType},
                    util::order::OrderByAdapter,
                    vectors::{
                        brute_force_search::BruteForceSearchVAdapter, search::SearchVAdapter,
                    },
                },
                traversal_value::{Traversable, TraversalValue},
            },
            types::GraphError,
            vector_core::vector::HVector,
        },
        types::GraphError,
        vector_core::vector::HVector,
    },
    helix_gateway::{
        embedding_providers::embedding_providers::{EmbeddingModel, get_embedding_model},
        mcp::mcp::{MCPConnection, MCPHandler, MCPHandlerSubmission, MCPToolInput, McpBackend},
    },
    protocol::{response::Response, return_values::ReturnValue, value::Value},
    utils::label_hash::hash_label,
};
use heed3::{EnvOpenOptions, RoTxn};
use helix_macros::{mcp_handler, tool_calls};
use serde::Deserialize;
use std::{path::Path, sync::Arc};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "tool_name", content = "args")]
pub enum ToolArgs {
    OutStep {
        edge_label: String,
        edge_type: EdgeType,
        filter: Option<FilterTraversal>,
    },
    OutEStep {
        edge_label: String,
        filter: Option<FilterTraversal>,
    },
    InStep {
        edge_label: String,
        edge_type: EdgeType,
        filter: Option<FilterTraversal>,
    },
    InEStep {
        edge_label: String,
        filter: Option<FilterTraversal>,
    },
    NFromType {
        node_type: String,
    },
    EFromType {
        edge_type: String,
    },
    FilterItems {
        properties: Option<Vec<(String, String)>>,
        filter_traversals: Option<Vec<ToolArgs>>,
    },
    OrderBy {
        properties: String,
        order: Order,
    },
}

#[derive(Clone, Debug, Deserialize)]
pub enum Order {
    #[serde(rename = "asc")]
    Asc,
    #[serde(rename = "desc")]
    Desc,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FilterProperties {
    pub key: String,
    pub value: Value,
    pub operator: Option<Operator>,
}

#[derive(Debug, Deserialize, Clone)]
pub enum Operator {
    #[serde(rename = "==")]
    Eq,
    #[serde(rename = "!=")]
    Neq,
    #[serde(rename = ">")]
    Gt,
    #[serde(rename = "<")]
    Lt,
    #[serde(rename = ">=")]
    Gte,
    #[serde(rename = "<=")]
    Lte,
}

impl Operator {
    pub fn execute(&self, value1: &Value, value2: &Value) -> bool {
        debug_println!("operating on value1: {:?}, value2: {:?}", *value1, *value2);
        match self {
            Operator::Eq => *value1 == *value2,
            Operator::Neq => *value1 != *value2,
            Operator::Gt => *value1 > *value2,
            Operator::Lt => *value1 < *value2,
            Operator::Gte => *value1 >= *value2,
            Operator::Lte => *value1 <= *value2,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FilterTraversal {
    pub properties: Option<Vec<Vec<FilterProperties>>>,
    pub filter_traversals: Option<Vec<ToolArgs>>,
}

#[tool_calls]
pub(super) trait McpTools<'a> {
    fn out_step(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        edge_label: String,
        edge_type: EdgeType,
    ) -> Result<Vec<TraversalValue>, GraphError>;

    fn out_e_step(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        edge_label: String,
    ) -> Result<Vec<TraversalValue>, GraphError>;

    fn in_step(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        edge_label: String,
        edge_type: EdgeType,
    ) -> Result<Vec<TraversalValue>, GraphError>;

    fn in_e_step(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        edge_label: String,
    ) -> Result<Vec<TraversalValue>, GraphError>;

    fn n_from_type(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        node_type: String,
    ) -> Result<Vec<TraversalValue>, GraphError>;

    fn e_from_type(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        edge_type: String,
    ) -> Result<Vec<TraversalValue>, GraphError>;

    /// filters items based on properies and traversal existence
    /// a node or edge needs to have been search first though
    fn filter_items(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        filter: FilterTraversal,
    ) -> Result<Vec<TraversalValue>, GraphError>;

    /// BM25
    fn search_keyword(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        query: String,
        limit: usize,
        label: String,
    ) -> Result<Vec<TraversalValue>, GraphError>;

    /// HNSW Search with built int embedding model
    fn search_vector_text(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        query: String,
        label: String,
        k: Option<usize>,
    ) -> Result<Vec<TraversalValue>, GraphError>;

    fn search_vector(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        vector: Vec<f64>,
        k: usize,
        min_score: Option<f64>,
    ) -> Result<Vec<TraversalValue>, GraphError>;

    fn order_by(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        properties: String,
        order: Order,
    ) -> Result<Vec<TraversalValue>, GraphError>;
}

impl<'a> McpTools<'a> for McpBackend {
    fn out_step(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        edge_label: String,
        edge_type: EdgeType,
    ) -> Result<Vec<TraversalValue>, GraphError> {
        let db = Arc::clone(&self.db);

        let iter = connection
            .iter
            .clone()
            .filter_map(move |item| {
                let edge_label_hash = hash_label(&edge_label, None);
                let prefix = HelixGraphStorage::out_edge_key(&item.id(), &edge_label_hash);
                match db
                    .out_edges_db
                    .lazily_decode_data()
                    .get_duplicates(txn, &prefix)
                {
                    Ok(Some(iter)) => Some(OutNodesIterator {
                        iter,
                        storage: Arc::clone(&db),
                        edge_type: edge_type.clone(),
                        txn,
                    }),
                    Ok(None) => None,
                    Err(e) => {
                        println!("{} Error getting out edges: {:?}", line!(), e);
                        // return Err(e);
                        None
                    }
                }
            })
            .flatten();

        let result = iter.collect();
        debug_println!("result: {:?}", result);
        result
    }

    fn out_e_step(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        edge_label: String,
    ) -> Result<Vec<TraversalValue>, GraphError> {
        let db = Arc::clone(&self.db);

        let iter = connection
            .iter
            .clone()
            .filter_map(move |item| {
                let edge_label_hash = hash_label(&edge_label, None);
                let prefix = HelixGraphStorage::out_edge_key(&item.id(), &edge_label_hash);
                match db
                    .out_edges_db
                    .lazily_decode_data()
                    .get_duplicates(txn, &prefix)
                {
                    Ok(Some(iter)) => Some(OutEdgesIterator {
                        iter,
                        storage: Arc::clone(&db),
                        txn,
                    }),
                    Ok(None) => None,
                    Err(e) => {
                        println!("{} Error getting out edges: {:?}", line!(), e);
                        // return Err(e);
                        None
                    }
                }
            })
            .flatten();

        let result = iter.collect();
        debug_println!("result: {:?}", result);
        result
    }

    fn in_step(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        edge_label: String,
        edge_type: EdgeType,
    ) -> Result<Vec<TraversalValue>, GraphError> {
        let db = Arc::clone(&self.db);

        let iter = connection
            .iter
            .clone()
            .filter_map(move |item| {
                let edge_label_hash = hash_label(&edge_label, None);
                let prefix = HelixGraphStorage::in_edge_key(&item.id(), &edge_label_hash);
                match db
                    .in_edges_db
                    .lazily_decode_data()
                    .get_duplicates(txn, &prefix)
                {
                    Ok(Some(iter)) => Some(InNodesIterator {
                        iter,
                        storage: Arc::clone(&db),
                        edge_type: edge_type.clone(),
                        txn,
                    }),
                    Ok(None) => None,
                    Err(e) => {
                        println!("{} Error getting out edges: {:?}", line!(), e);
                        // return Err(e);
                        None
                    }
                }
            })
            .flatten();

        let result = iter.collect();
        debug_println!("result: {:?}", result);
        result
    }

    fn in_e_step(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        edge_label: String,
    ) -> Result<Vec<TraversalValue>, GraphError> {
        let db = Arc::clone(&self.db);

        let iter = connection
            .iter
            .clone()
            .filter_map(move |item| {
                let edge_label_hash = hash_label(&edge_label, None);
                let prefix = HelixGraphStorage::in_edge_key(&item.id(), &edge_label_hash);
                match db
                    .in_edges_db
                    .lazily_decode_data()
                    .get_duplicates(txn, &prefix)
                {
                    Ok(Some(iter)) => Some(InEdgesIterator {
                        iter,
                        storage: Arc::clone(&db),
                        txn,
                    }),
                    Ok(None) => None,
                    Err(_e) => {
                        debug_println!("{} Error getting out edges: {:?}", line!(), _e);
                        // return Err(e);
                        None
                    }
                }
            })
            .flatten();

        let result = iter.collect();
        debug_println!("result: {:?}", result);
        result
    }

    fn n_from_type(
        &'a self,
        txn: &'a RoTxn,
        _connection: &'a MCPConnection,
        node_type: String,
    ) -> Result<Vec<TraversalValue>, GraphError> {
        let db = Arc::clone(&self.db);

        let iter = NFromType {
            iter: db.nodes_db.lazily_decode_data().iter(txn).unwrap(),
            label: &node_type,
        };

        let result = iter.collect::<Result<Vec<_>, _>>();
        debug_println!("result: {:?}", result);
        result
    }

    fn e_from_type(
        &'a self,
        txn: &'a RoTxn,
        _connection: &'a MCPConnection,
        edge_type: String,
    ) -> Result<Vec<TraversalValue>, GraphError> {
        let db = Arc::clone(&self.db);

        let iter = EFromType {
            iter: db.edges_db.lazily_decode_data().iter(txn).unwrap(),
            label: &edge_type,
        };

        let result = iter.collect::<Result<Vec<_>, _>>();
        debug_println!("result: {:?}", result);
        result
    }

    fn filter_items(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        filter: FilterTraversal,
    ) -> Result<Vec<TraversalValue>, GraphError> {
        let result = _filter_items(Arc::clone(&self.db), txn, connection.iter.clone(), &filter);

        Ok(result)
    }

    // fn search_keyword_old(
    //     &'a self,
    //     txn: &'a RoTxn,
    //     _connection: &'a MCPConnection,
    //     query: String,
    //     limit: usize,
    //     label: String,
    // ) -> Result<Vec<TraversalValue>, GraphError> {
    //     let db = Arc::clone(&self.db);

    //     //         let items = connection.iter.clone().collect::<Vec<_>>();

    //     // Check if BM25 is enabled and has metadata
    //     if let Some(bm25) = &db.bm25 {
    //         match bm25
    //             .metadata_db
    //             .get(txn, crate::helix_engine::bm25::bm25::METADATA_KEY)
    //         {
    //             Ok(Some(_)) => {
    //                 let results = G::new(db, txn)
    //                     .search_bm25(&label, &query, limit)?
    //                     .collect_to::<Vec<_>>();

    //                 println!("BM25 search results: {results:?}");
    //                 Ok(results)
    //             }
    //             Ok(None) => {
    //                 // BM25 metadata not found - index not initialized yet
    //                 debug_println!("BM25 index not initialized yet - returning empty results");
    //                 println!("BM25 index not initialized yet - returning empty results");
    //                 Err(GraphError::from(
    //                     "BM25 index not initialized yet - returning empty results",
    //                 ))
    //             }
    //             Err(_e) => {
    //                 // Error accessing metadata database
    //                 debug_println!(
    //                     "Error checking BM25 metadata: {_e:?} - returning empty results"
    //                 );
    //                 println!("Error checking BM25 metadata: {_e:?} - returning empty results");
    //                 Err(GraphError::from(
    //                     "Error checking BM25 metadata - returning empty results",
    //                 ))
    //             }
    //         }
    //     } else {
    //         // BM25 is not enabled
    //         debug_println!("BM25 is not enabled - returning empty results");
    //         println!("BM25 is not enabled - returning empty results");
    //         Err(GraphError::from(
    //             "BM25 is not enabled - returning empty results",
    //         ))
    //     }
    // }

    fn search_keyword(
        &'a self,
        _txn: &'a RoTxn,
        connection: &'a MCPConnection,
        query: String,
        limit: usize,
        label: String,
    ) -> Result<Vec<TraversalValue>, GraphError> {
        _search_keyword(Arc::clone(&self.db), connection, query, limit, label)
    }

    fn search_vector_text(
        &'a self,
        txn: &'a RoTxn,
        _connection: &'a MCPConnection,
        query: String,
        label: String,
        k: Option<usize>,
    ) -> Result<Vec<TraversalValue>, GraphError> {
        let db = Arc::clone(&self.db);

        let model = get_embedding_model(None, None, None)?;
        let result = model.fetch_embedding(&query);
        let embedding = result?;

        let res = G::new(db, txn)
            .search_v::<fn(&HVector, &RoTxn) -> bool, _>(&embedding, k.unwrap_or(5), &label, None)
            .collect_to::<Vec<_>>();

        debug_println!("result: {res:?}");
        Ok(res)
    }

    fn search_vector(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        vector: Vec<f64>,
        k: usize,
        min_score: Option<f64>,
    ) -> Result<Vec<TraversalValue>, GraphError> {
        let db = Arc::clone(&self.db);

        let items = connection.iter.clone().collect::<Vec<_>>();

        let mut res = G::new_from(db, txn, items)
            .brute_force_search_v(&vector, k)
            .collect_to::<Vec<_>>();

        if let Some(min_score) = min_score {
            res.retain(|item| {
                if let TraversalValue::Vector(vector) = item {
                    vector.get_distance() > min_score
                } else {
                    false
                }
            });
        }

        debug_println!("result: {res:?}");
        Ok(res)
    }

    fn order_by(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        properties: String,
        order: Order,
    ) -> Result<Vec<TraversalValue>, GraphError> {
        let db = Arc::clone(&self.db);

        let iter = connection.iter.clone().collect::<Vec<_>>();

        let res = match order {
            Order::Asc => G::new_from(db, txn, iter)
                .order_by_asc(&properties)
                .collect_to::<Vec<_>>(),
            Order::Desc => G::new_from(db, txn, iter)
                .order_by_desc(&properties)
                .collect_to::<Vec<_>>(),
        };

        debug_println!("result: {res:?}");
        Ok(res)
    }
}

pub trait FilterValues {
    fn compare(&self, value: &Value, operator: Option<Operator>) -> bool;
}

pub(super) fn _filter_items(
    db: Arc<HelixGraphStorage>,
    txn: &RoTxn,
    iter: impl Iterator<Item = TraversalValue>,
    filter: &FilterTraversal,
) -> Vec<TraversalValue> {
    let db = Arc::clone(&db);

    debug_println!("properties: {:?}", filter);
    debug_println!("filter_traversals: {:?}", filter.filter_traversals);

    let initial_filtered_iter = match &filter.properties {
        Some(properties) => iter
            .filter(move |item| {
                properties.iter().any(|filters| {
                    filters.iter().all(|filter| {
                        debug_println!("filter: {:?}", filter);
                        match item.check_property(&filter.key) {
                            Ok(v) => {
                                debug_println!("item value for key: {:?} is {:?}", filter.key, v);
                                v.compare(&filter.value, filter.operator.clone())
                            }
                            Err(_) => false,
                        }
                    })
                })
            })
            .collect::<Vec<_>>(),
        None => iter.collect::<Vec<_>>(),
    };

    debug_println!("iter: {:?}", initial_filtered_iter);

    let result = initial_filtered_iter
        .into_iter()
        .filter_map(move |item| match &filter.filter_traversals {
            Some(filter_traversals) => {
                match filter_traversals.iter().all(|filter| {
                    let result = G::new_from(Arc::clone(&db), txn, vec![item.clone()]);
                    match filter {
                        ToolArgs::OutStep {
                            edge_label,
                            edge_type,
                            filter: filter_traversal_filter,
                        } => match filter_traversal_filter {
                            Some(filter_traversal_filter) => !_filter_items(
                                Arc::clone(&db),
                                txn,
                                result
                                    .out(edge_label, edge_type)
                                    .collect_to::<Vec<_>>()
                                    .into_iter(),
                                filter_traversal_filter,
                            )
                            .is_empty(),
                            None => result.out(edge_label, edge_type).next().is_some(),
                        },
                        ToolArgs::OutEStep {
                            edge_label,
                            filter: filter_traversal_filter,
                        } => match filter_traversal_filter {
                            Some(filter_traversal_filter) => !_filter_items(
                                Arc::clone(&db),
                                txn,
                                result.out_e(edge_label).collect_to::<Vec<_>>().into_iter(),
                                filter_traversal_filter,
                            )
                            .is_empty(),
                            None => result.out_e(edge_label).next().is_some(),
                        },
                        ToolArgs::InStep {
                            edge_label,
                            edge_type,
                            filter: filter_traversal_filter,
                        } => match filter_traversal_filter {
                            Some(filter_traversal_filter) => !_filter_items(
                                Arc::clone(&db),
                                txn,
                                result
                                    .in_(edge_label, edge_type)
                                    .collect_to::<Vec<_>>()
                                    .into_iter(),
                                filter_traversal_filter,
                            )
                            .is_empty(),
                            None => result.in_(edge_label, edge_type).next().is_some(),
                        },
                        ToolArgs::InEStep {
                            edge_label,
                            filter: filter_traversal_filter,
                        } => match filter_traversal_filter {
                            Some(filter_traversal_filter) => !_filter_items(
                                Arc::clone(&db),
                                txn,
                                result.in_e(edge_label).collect_to::<Vec<_>>().into_iter(),
                                filter_traversal_filter,
                            )
                            .is_empty(),
                            None => result.in_e(edge_label).next().is_some(),
                        },
                        _ => false,
                    }
                }) {
                    true => Some(item),
                    false => None,
                }
            }
            None => Some(item),
        })
        .collect::<Vec<_>>();

    debug_println!("result: {:?}", result);
    result
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tempfile::TempDir;

    use crate::{
        helix_engine::{storage_core::version_info::VersionInfo, traversal_core::config},
        protocol::value::Value,
        utils::items::Node,
    };

    use super::*;

    #[test]
    fn test_filter_items() {
        let (storage, _temp_dir) = {
            let temp_dir = TempDir::new().unwrap();
            let storage = Arc::new(
                HelixGraphStorage::new(
                    temp_dir.path().to_str().unwrap(),
                    config::Config::default(),
                    VersionInfo::default(),
                )
                .unwrap(),
            );
            (storage, temp_dir)
        };
        let items = (1..101)
            .map(|i| {
                TraversalValue::Node(Node {
                    id: i,
                    version: 1,
                    label: "test".to_string(),
                    properties: Some(HashMap::from([("age".to_string(), Value::I64(i as i64))])),
                })
            })
            .collect::<Vec<_>>();

        let filter = FilterTraversal {
            properties: Some(vec![vec![FilterProperties {
                key: "age".to_string(),
                value: Value::I64(50),
                operator: Some(Operator::Gt),
            }]]),
            filter_traversals: None,
        };

        let txn = storage.graph_env.read_txn().unwrap();

        let result = _filter_items(Arc::clone(&storage), &txn, items.into_iter(), &filter);
        assert_eq!(result.len(), 50);
    }
}

fn _search_keyword(
    db: Arc<HelixGraphStorage>,
    connection: &MCPConnection,
    query: String,
    limit: usize,
    label: String,
) -> Result<Vec<TraversalValue>, GraphError> {
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024 * 1024)
            .max_dbs(20)
            .max_readers(200)
            .open(Path::new("bm25_index"))?
    };
    let mut txn = env.write_txn()?;
    let bm25_index = HBM25Config::new_temp(&env, &mut txn, &connection.connection_id)?;

    for item in connection.iter.clone() {
        if let Some(props) = item.get_properties() {
            let mut data = props.flatten_bm25();
            data.push_str(&item.label());
            bm25_index.insert_doc(&mut txn, item.id(), &data)?;
        }
    }

    // Check if BM25 is enabled and has metadata

    let results = match bm25_index
        .metadata_db
        .get(&txn, crate::helix_engine::bm25::bm25::METADATA_KEY)
    {
        Ok(Some(_)) => {
            let results = G::new(Arc::clone(&db), &txn)
                .search_bm25(&label, &query, limit)?
                .collect_to::<Vec<_>>();

            println!("BM25 search results: {results:?}");
            Ok(results)
        }
        Ok(None) => {
            // BM25 metadata not found - index not initialized yet
            debug_println!("BM25 index not initialized yet - returning empty results");
            println!("BM25 index not initialized yet - returning empty results");
            Err(GraphError::from(
                "BM25 index not initialized yet - returning empty results",
            ))
        }
        Err(e) => {
            // Error accessing metadata database
            debug_println!(
                "Error checking BM25 metadata: {:?} - returning empty results",
                e
            );
            println!("Error checking BM25 metadata: {e:?} - returning empty results",);
            Err(GraphError::from(
                "Error checking BM25 metadata - returning empty results",
            ))
        }
    };

    // clear all data made in this function
    // can just abort as nothing is flushed to disk
    txn.abort();

    results
}
