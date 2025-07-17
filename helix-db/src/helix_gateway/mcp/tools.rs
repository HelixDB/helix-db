use crate::{
    debug_println,
    helix_engine::{
        graph_core::ops::{
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
            vectors::search::SearchVAdapter,
            bm25::search_bm25::SearchBM25Adapter,
            tr_val::{TraversalVal, Traversable},
        },
        types::GraphError,
        storage_core::storage_core::HelixGraphStorage,
        vector_core::vector::HVector,
    },
    helix_gateway::{
        embedding_providers::embedding_providers::{get_embedding_model, EmbeddingModel},
        mcp::mcp::{
            MCPConnection, McpBackend,
            MCPHandler, MCPHandlerSubmission, MCPToolInput
        },
    },
    protocol::{
        response::Response,
        return_values::ReturnValue,
    },
    utils::label_hash::hash_label,
};
use heed3::RoTxn;
use helix_macros::{tool_calls, mcp_handler};
use serde::Deserialize;
use std::{marker::PhantomData, sync::Arc};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "tool_name", content = "args")]
pub enum ToolArgs {
    OutStep {
        edge_label: String,
        edge_type: EdgeType,
    },
    OutEStep {
        edge_label: String,
    },
    InStep {
        edge_label: String,
        edge_type: EdgeType,
    },
    InEStep {
        edge_label: String,
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
}

#[tool_calls]
trait McpTools<'a> {
    fn out_step(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        edge_label: &'a str,
        edge_type: EdgeType,
    ) -> Result<Vec<TraversalVal>, GraphError>;

    fn out_e_step(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        edge_label: &'a str,
    ) -> Result<Vec<TraversalVal>, GraphError>;

    fn in_step(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        edge_label: &'a str,
        edge_type: EdgeType,
    ) -> Result<Vec<TraversalVal>, GraphError>;

    fn in_e_step(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        edge_label: &'a str,
    ) -> Result<Vec<TraversalVal>, GraphError>;

    fn n_from_type(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        node_type: &'a str,
    ) -> Result<Vec<TraversalVal>, GraphError>;

    fn e_from_type(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        edge_type: &'a str,
    ) -> Result<Vec<TraversalVal>, GraphError>;

    /// filters items based on properies and traversal existence
    /// a node or edge needs to have been search first though
    fn filter_items(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        properties: Option<Vec<(String, String)>>,
        filter_traversals: Option<Vec<ToolArgs>>,
        _marker: PhantomData<&'a ()>,
    ) -> Result<Vec<TraversalVal>, GraphError>;

    /// BM25
    fn search_keyword(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        query: String,
        limit: usize,
    ) -> Result<Vec<TraversalVal>, GraphError>;

    /// HNSW Search with built int embedding model
    fn search_vector_text(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        query: String,
    ) -> Result<Vec<TraversalVal>, GraphError>;
}

impl<'a> McpTools<'a> for McpBackend {
    fn out_step(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        edge_label: &'a str,
        edge_type: EdgeType,
    ) -> Result<Vec<TraversalVal>, GraphError> {
        let db = Arc::clone(&self.db);

        let iter = connection
            .iter
            .clone()
            .filter_map(move |item| {
                let edge_label_hash = hash_label(edge_label, None);
                let prefix = HelixGraphStorage::out_edge_key(&item.id(), &edge_label_hash);
                match db
                    .out_edges_db
                    .lazily_decode_data()
                    .get_duplicates(&txn, &prefix)
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

        let result = iter.take(100).collect();
        debug_println!("result: {:?}", result);
        result
    }

    fn out_e_step(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        edge_label: &'a str,
    ) -> Result<Vec<TraversalVal>, GraphError> {
        let db = Arc::clone(&self.db);

        let iter = connection
            .iter
            .clone()
            .filter_map(move |item| {
                let edge_label_hash = hash_label(edge_label, None);
                let prefix = HelixGraphStorage::out_edge_key(&item.id(), &edge_label_hash);
                match db
                    .out_edges_db
                    .lazily_decode_data()
                    .get_duplicates(&txn, &prefix)
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

        let result = iter.take(100).collect();
        debug_println!("result: {:?}", result);
        result
    }

    fn in_step(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        edge_label: &'a str,
        edge_type: EdgeType,
    ) -> Result<Vec<TraversalVal>, GraphError> {
        let db = Arc::clone(&self.db);

        let iter = connection
            .iter
            .clone()
            .filter_map(move |item| {
                let edge_label_hash = hash_label(edge_label, None);
                let prefix = HelixGraphStorage::in_edge_key(&item.id(), &edge_label_hash);
                match db
                    .in_edges_db
                    .lazily_decode_data()
                    .get_duplicates(&txn, &prefix)
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

        let result = iter.take(100).collect();
        debug_println!("result: {:?}", result);
        result
    }

    fn in_e_step(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        edge_label: &'a str,
    ) -> Result<Vec<TraversalVal>, GraphError> {
        let db = Arc::clone(&self.db);

        let iter = connection
            .iter
            .clone()
            .filter_map(move |item| {
                let edge_label_hash = hash_label(edge_label, None);
                let prefix = HelixGraphStorage::in_edge_key(&item.id(), &edge_label_hash);
                match db
                    .in_edges_db
                    .lazily_decode_data()
                    .get_duplicates(&txn, &prefix)
                {
                    Ok(Some(iter)) => Some(InEdgesIterator {
                        iter,
                        storage: Arc::clone(&db),
                        txn,
                    }),
                    Ok(None) => None,
                    Err(e) => {
                        debug_println!("{} Error getting out edges: {:?}", line!(), e);
                        // return Err(e);
                        None
                    }
                }
            })
            .flatten();

        let result = iter.take(100).collect();
        debug_println!("result: {:?}", result);
        result
    }

    fn n_from_type(
        &'a self,
        txn: &'a RoTxn,
        _connection: &'a MCPConnection,
        node_type: &'a str,
    ) -> Result<Vec<TraversalVal>, GraphError> {
        let db = Arc::clone(&self.db);

        let iter = NFromType {
            iter: db.nodes_db.lazily_decode_data().iter(txn).unwrap(),
            label: node_type,
        };

        let result = iter.take(100).collect::<Result<Vec<_>, _>>();
        debug_println!("result: {:?}", result);
        result
    }

    fn e_from_type(
        &'a self,
        txn: &'a RoTxn,
        _connection: &'a MCPConnection,
        edge_type: &'a str,
    ) -> Result<Vec<TraversalVal>, GraphError> {
        let db = Arc::clone(&self.db);

        let iter = EFromType {
            iter: db.edges_db.lazily_decode_data().iter(txn).unwrap(),
            label: edge_type,
        };

        let result = iter.take(100).collect::<Result<Vec<_>, _>>();
        debug_println!("result: {:?}", result);
        result
    }

    fn filter_items(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        properties: Option<Vec<(String, String)>>,
        filter_traversals: Option<Vec<ToolArgs>>,
        _marker: PhantomData<&'a ()>,
    ) -> Result<Vec<TraversalVal>, GraphError> {
        let db = Arc::clone(&self.db);

        debug_println!("properties: {:?}", properties);
        debug_println!("filter_traversals: {:?}", filter_traversals);
        debug_println!("connection: {:?}", connection.iter);

        let iter = match properties {
            Some(properties) => {
                let iter = connection
                    .iter
                    .clone()
                    .filter(move |item| {
                        properties.iter().all(|(key, value)| {
                            item.check_property(key.as_str())
                                .map_or(false, |v| *v == *value)
                        })
                    })
                    .collect::<Vec<_>>();
                iter
            }
            None => connection.iter.clone().collect::<Vec<_>>(),
        };

        debug_println!("iter: {:?}", iter);

        let result = iter
            .clone()
            .into_iter()
            .filter_map(move |item| match &filter_traversals {
                Some(filter_traversals) => {
                    filter_traversals.iter().any(|filter| {
                        let result = G::new_from(Arc::clone(&db), txn, vec![item.clone()]);
                        match filter {
                            ToolArgs::OutStep {
                                edge_label,
                                edge_type,
                            } => result.out(edge_label, edge_type).next().is_some(),
                            ToolArgs::OutEStep { edge_label } => {
                                result.out_e(edge_label).next().is_some()
                            }
                            ToolArgs::InStep {
                                edge_label,
                                edge_type,
                            } => result.in_(edge_label, edge_type).next().is_some(),
                            ToolArgs::InEStep { edge_label } => {
                                result.in_e(edge_label).next().is_some()
                            }
                            _ => return false,
                        }
                    });

                    Some(item)
                }
                None => Some(item),
            })
            .collect::<Vec<_>>();

        debug_println!("result: {:?}", result);

        Ok(result)
    }

    fn search_keyword(
        &'a self,
        txn: &'a RoTxn,
        _connection: &'a MCPConnection,
        query: String,
        limit: usize,
    ) -> Result<Vec<TraversalVal>, GraphError> {
        let db = Arc::clone(&self.db);

        let results = G::new(db, &txn)
            .search_bm25("mcp search", &query, limit)?
            .collect_to::<Vec<_>>();

        Ok(results)
    }

    fn search_vector_text(
        &'a self,
        txn: &'a RoTxn,
        _connection: &'a MCPConnection,
        query: String,
    ) -> Result<Vec<TraversalVal>, GraphError> {
        let db = Arc::clone(&self.db);

        let model = get_embedding_model(None, None, None)?;
        let result = model.fetch_embedding(&query);
        let embedding = result?;

        let res = G::new(db, &txn)
            .search_v::<fn(&HVector, &RoTxn) -> bool>(&embedding, 5, None)
            .collect_to::<Vec<_>>();

        println!("result: {:?}", res);
        Ok(res)
    }
}

