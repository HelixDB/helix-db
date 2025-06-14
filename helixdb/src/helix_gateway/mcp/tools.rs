use crate::helix_engine::graph_core::ops::g::G;
use crate::helix_engine::graph_core::ops::in_::in_::{InAdapter, InNodesIterator};
use crate::helix_engine::graph_core::ops::in_::in_e::{InEdgesAdapter, InEdgesIterator};
use crate::helix_engine::graph_core::ops::out::out::{OutAdapter, OutNodesIterator};
use crate::helix_engine::graph_core::ops::out::out_e::{OutEdgesAdapter, OutEdgesIterator};
use crate::helix_engine::graph_core::ops::source::add_e::EdgeType;
use crate::helix_engine::graph_core::ops::source::e_from_type::{EFromType, EFromTypeAdapter};
use crate::helix_engine::graph_core::ops::source::n_from_type::{NFromType, NFromTypeAdapter};
use crate::helix_engine::graph_core::ops::tr_val::{Traversable, TraversalVal};
use crate::helix_engine::storage_core::storage_core::HelixGraphStorage;
use crate::helix_engine::types::GraphError;
use crate::helix_gateway::mcp::mcp::{MCPConnection, McpBackend};
use crate::helix_gateway::router::router::HandlerInput;
use crate::protocol::label_hash::hash_label;
use crate::protocol::response::Response;
use get_routes::local_handler;
use crate::helix_storage::heed3::RoTxn;
use serde::{Deserialize, Deserializer};
use std::collections::HashMap;
use std::sync::Arc;

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
}

pub(crate) trait ToolCalls<'a> {
    fn call(
        &'a self,
        txn: &'a RoTxn,
        connection_id: &'a MCPConnection,
        args: ToolArgs,
    ) -> Result<Vec<TraversalVal>, GraphError>;
}

impl<'a> ToolCalls<'a> for McpBackend {
    fn call(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        args: ToolArgs,
    ) -> Result<Vec<TraversalVal>, GraphError> {
        let result = match args {
            ToolArgs::OutStep {
                edge_label,
                edge_type,
            } => self.out_step(connection, &edge_label, &edge_type, txn),
            ToolArgs::OutEStep { edge_label } => self.out_e_step(connection, &edge_label, txn),
            ToolArgs::InStep {
                edge_label,
                edge_type,
            } => self.in_step(connection, &edge_label, &edge_type, txn),
            ToolArgs::InEStep { edge_label } => self.in_e_step(connection, &edge_label, txn),
            ToolArgs::NFromType { node_type } => self.n_from_type(&node_type, txn),
            ToolArgs::EFromType { edge_type } => self.e_from_type(&edge_type, txn),
            _ => return Err(GraphError::New(format!("Tool {:?} not found", args))),
        }?;

        Ok(result)
    }
}

trait McpTools<'a> {
    fn out_step(
        &'a self,
        connection: &'a MCPConnection,
        edge_label: &'a str,
        edge_type: &'a EdgeType,
        txn: &'a RoTxn,
    ) -> Result<Vec<TraversalVal>, GraphError>;

    fn out_e_step(
        &'a self,
        connection: &'a MCPConnection,
        edge_label: &'a str,
        txn: &'a RoTxn,
    ) -> Result<Vec<TraversalVal>, GraphError>;

    fn in_step(
        &'a self,
        connection: &'a MCPConnection,
        edge_label: &'a str,
        edge_type: &'a EdgeType,
        txn: &'a RoTxn,
    ) -> Result<Vec<TraversalVal>, GraphError>;

    fn in_e_step(
        &'a self,
        connection: &'a MCPConnection,
        edge_label: &'a str,
        txn: &'a RoTxn,
    ) -> Result<Vec<TraversalVal>, GraphError>;

    fn n_from_type(
        &'a self,
        node_type: &'a str,
        txn: &'a RoTxn,
    ) -> Result<Vec<TraversalVal>, GraphError>;

    fn e_from_type(
        &'a self,
        edge_type: &'a str,
        txn: &'a RoTxn,
    ) -> Result<Vec<TraversalVal>, GraphError>;

    /// filters items based on properies and traversal existence
    fn filter_items(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        properties: Option<Vec<(String, String)>>,
        filter_traversals: Option<Vec<ToolArgs>>,
    ) -> Result<Vec<TraversalVal>, GraphError>;
}

impl<'a> McpTools<'a> for McpBackend {
    fn out_step(
        &'a self,
        connection: &'a MCPConnection,
        edge_label: &'a str,
        edge_type: &'a EdgeType,
        txn: &'a RoTxn,
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
                        edge_type,
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

        match edge_type {
            EdgeType::Node => {}
            EdgeType::Vec => {}
        }

        let result = iter.take(100).collect();
        println!("result: {:?}", result);
        result
    }

    fn out_e_step(
        &'a self,
        connection: &'a MCPConnection,
        edge_label: &'a str,
        txn: &'a RoTxn,
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
        println!("result: {:?}", result);
        result
    }

    fn in_step(
        &'a self,
        connection: &'a MCPConnection,
        edge_label: &'a str,
        edge_type: &'a EdgeType,
        txn: &'a RoTxn,
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
                        edge_type,
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

        match edge_type {
            EdgeType::Node => {}
            EdgeType::Vec => {}
        }

        let result = iter.take(100).collect();
        println!("result: {:?}", result);
        result
    }

    fn in_e_step(
        &'a self,
        connection: &'a MCPConnection,
        edge_label: &'a str,
        txn: &'a RoTxn,
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
                        println!("{} Error getting out edges: {:?}", line!(), e);
                        // return Err(e);
                        None
                    }
                }
            })
            .flatten();

        let result = iter.take(100).collect();
        println!("result: {:?}", result);
        result
    }

    fn n_from_type(
        &'a self,
        node_type: &'a str,
        txn: &'a RoTxn,
    ) -> Result<Vec<TraversalVal>, GraphError> {
        let db = Arc::clone(&self.db);

        let iter = NFromType {
            iter: db.nodes_db.lazily_decode_data().iter(txn).unwrap(),
            label: node_type,
        };

        let result = iter.take(100).collect::<Result<Vec<_>, _>>();
        println!("result: {:?}", result);
        result
    }

    fn e_from_type(
        &'a self,
        edge_type: &'a str,
        txn: &'a RoTxn,
    ) -> Result<Vec<TraversalVal>, GraphError> {
        let db = Arc::clone(&self.db);

        let iter = EFromType {
            iter: db.edges_db.lazily_decode_data().iter(txn).unwrap(),
            label: edge_type,
        };

        let result = iter.take(100).collect::<Result<Vec<_>, _>>();
        println!("result: {:?}", result);
        result
    }

    fn filter_items(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        properties: Option<Vec<(String, String)>>,
        filter_traversals: Option<Vec<ToolArgs>>,
    ) -> Result<Vec<TraversalVal>, GraphError> {
        let db = Arc::clone(&self.db);

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
                None => None,
            })
            .collect::<Vec<_>>();

        Ok(result)
    }
}
