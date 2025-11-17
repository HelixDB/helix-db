use crate::{
    debug_println,
    helix_engine::{storage_core::HelixGraphStorage, types::GraphError},
    utils::items::Node,
};
use heed3::{RoIter, RoTxn, types::*};
use sonic_rs::{JsonValueMutTrait, Value as JsonValue, json};
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
    sync::Arc,
};

/// Set of functions to access the nodes and edges stored to export to json
pub trait GraphVisualization {
    /// Serializes nodes and edges to JSON for graph visualization.
    fn nodes_edges_to_json(
        &self,
        txn: &RoTxn,
        k: Option<usize>,
        node_prop: Option<String>,
    ) -> Result<String, GraphError>;

    /// Retrieves database statistics in JSON format.
    fn get_db_stats_json(&self, txn: &RoTxn) -> Result<String, GraphError>;
}

impl GraphVisualization for HelixGraphStorage {
    fn nodes_edges_to_json(
        &self,
        txn: &RoTxn,
        k: Option<usize>,
        node_prop: Option<String>,
    ) -> Result<String, GraphError> {
        let k = k.unwrap_or(200);
        if k > 300 {
            return Err(GraphError::New(
                "cannot not visualize more than 300 nodes!".to_string(),
            ));
        }

        if self.nodes_db.is_empty(txn)? || self.edges_db.is_empty(txn)? {
            return Err(GraphError::New("edges or nodes db is empty!".to_string()));
        }

        let top_nodes = self.get_nodes_by_cardinality(txn, k)?;

        let ret_json = self.cards_to_json(txn, k, top_nodes, node_prop)?;
        sonic_rs::to_string(&ret_json).map_err(|e| GraphError::New(e.to_string()))
    }

    fn get_db_stats_json(&self, txn: &RoTxn) -> Result<String, GraphError> {
        let result = json!({
            "num_nodes":   self.nodes_db.len(txn).unwrap_or(0),
            "num_edges":   self.edges_db.len(txn).unwrap_or(0),
            "num_vectors": self.vectors.vectors_db.len(txn).unwrap_or(0),
        });
        debug_println!("db stats json: {:?}", result);

        sonic_rs::to_string(&result).map_err(|e| GraphError::New(e.to_string()))
    }
}

/// Implementing the helper functions needed to get the data for graph visualization
impl HelixGraphStorage {
    /// Get the top k nodes and all of the edges associated with them by checking their
    /// cardinalities (total number of in and out edges)
    #[allow(clippy::type_complexity)]
    fn get_nodes_by_cardinality(
        &self,
        txn: &RoTxn,
        k: usize,
    ) -> Result<Vec<(u128, Vec<(u128, u128, u128)>, Vec<(u128, u128, u128)>)>, GraphError> {
        let node_count = self.nodes_db.len(txn)?;

        type EdgeID = u128;
        type ToNodeId = u128;
        type FromNodeId = u128;

        struct EdgeCount {
            node_id: u128,
            edges_count: usize,
            out_edges: Vec<(EdgeID, FromNodeId, ToNodeId)>,
            in_edges: Vec<(EdgeID, FromNodeId, ToNodeId)>,
        }

        impl PartialEq for EdgeCount {
            fn eq(&self, other: &Self) -> bool {
                self.edges_count == other.edges_count
            }
        }
        impl Eq for EdgeCount {}
        impl PartialOrd for EdgeCount {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Ord for EdgeCount {
            fn cmp(&self, other: &Self) -> Ordering {
                self.edges_count.cmp(&other.edges_count)
            }
        }

        let db = Arc::new(self);
        let out_db = Arc::clone(&db);
        let in_db = Arc::clone(&db);

        #[derive(Default)]
        struct Edges<'a> {
            edge_count: usize,
            out_edges: Option<
                RoIter<
                    'a,
                    Bytes,
                    LazyDecode<Bytes>,
                    heed3::iteration_method::MoveOnCurrentKeyDuplicates,
                >,
            >,
            in_edges: Option<
                RoIter<
                    'a,
                    Bytes,
                    LazyDecode<Bytes>,
                    heed3::iteration_method::MoveOnCurrentKeyDuplicates,
                >,
            >,
        }

        let mut edge_counts: HashMap<u128, Edges> = HashMap::with_capacity(node_count as usize);
        let mut ordered_edge_counts: BinaryHeap<EdgeCount> =
            BinaryHeap::with_capacity(node_count as usize);

        // out edges - iterate through nodes by getting each unique node ID from out_edges_db
        let out_node_key_iter = out_db.out_edges_db.lazily_decode_data().iter(txn).unwrap();
        for data in out_node_key_iter {
            match data {
                Ok((key, _)) => {
                    let node_id = &key[0..16];
                    let edges = out_db
                        .out_edges_db
                        .lazily_decode_data()
                        .get_duplicates(txn, key)
                        .unwrap();

                    let edges_count = edges.iter().count();

                    let edge_count = edge_counts
                        .entry(u128::from_be_bytes(node_id.try_into().unwrap()))
                        .or_default();
                    edge_count.edge_count += edges_count;
                    edge_count.out_edges = edges;
                }
                Err(_e) => {
                    debug_println!("Error in out_node_key_iter: {:?}", _e);
                }
            }
        }

        // in edges
        let in_node_key_iter = in_db.in_edges_db.lazily_decode_data().iter(txn).unwrap();
        for data in in_node_key_iter {
            match data {
                Ok((key, _)) => {
                    let node_id = &key[0..16];
                    let edges = in_db
                        .in_edges_db
                        .lazily_decode_data()
                        .get_duplicates(txn, key)
                        .unwrap();
                    let edges_count = edges.iter().count();

                    let edge_count = edge_counts
                        .entry(u128::from_be_bytes(node_id.try_into().unwrap()))
                        .or_default();
                    edge_count.edge_count += edges_count;
                    edge_count.in_edges = edges;
                }
                Err(_e) => {
                    debug_println!("Error in in_node_key_iter: {:?}", _e);
                }
            }
        }

        // Decode edges and extract edge id and other node id
        for (node_id, edges_count) in edge_counts.into_iter() {
            let out_edges = match edges_count.out_edges {
                Some(out_edges_iter) => out_edges_iter
                    .map(|result| {
                        let (key, value) = result.unwrap();
                        let from_node = u128::from_be_bytes(key[0..16].try_into().unwrap());
                        let (edge_id, to_node) =
                            Self::unpack_adj_edge_data(value.decode().unwrap()).unwrap();
                        (edge_id, from_node, to_node)
                    })
                    .collect::<Vec<(EdgeID, FromNodeId, ToNodeId)>>(),
                None => vec![],
            };
            let in_edges = match edges_count.in_edges {
                Some(in_edges_iter) => in_edges_iter
                    .map(|result| {
                        let (key, value) = result.unwrap();
                        let to_node = u128::from_be_bytes(key[0..16].try_into().unwrap());
                        let (edge_id, from_node) =
                            Self::unpack_adj_edge_data(value.decode().unwrap()).unwrap();
                        (edge_id, from_node, to_node)
                    })
                    .collect::<Vec<(EdgeID, FromNodeId, ToNodeId)>>(),
                None => vec![],
            };

            ordered_edge_counts.push(EdgeCount {
                node_id,
                edges_count: edges_count.edge_count,
                out_edges,
                in_edges,
            });
        }

        let mut top_nodes = Vec::with_capacity(k);
        while let Some(edges_count) = ordered_edge_counts.pop() {
            top_nodes.push((
                edges_count.node_id,
                edges_count.out_edges,
                edges_count.in_edges,
            ));
            if top_nodes.len() >= k {
                break;
            }
        }

        Ok(top_nodes)
    }

    #[allow(clippy::type_complexity)]
    fn cards_to_json(
        &self,
        txn: &RoTxn,
        k: usize,
        top_nodes: Vec<(u128, Vec<(u128, u128, u128)>, Vec<(u128, u128, u128)>)>,
        node_prop: Option<String>,
    ) -> Result<JsonValue, GraphError> {
        let mut nodes = Vec::with_capacity(k);
        let mut edges = Vec::new();

        // Create temporary arena for node deserialization
        let arena = bumpalo::Bump::new();

        top_nodes
            .iter()
            .try_for_each(|(id, out_edges, _in_edges)| {
                let mut json_node = json!({ "id": id.to_string(), "title": id.to_string() });
                if let Some(prop) = &node_prop {
                    // Get node data
                    if let Some(node_data) = self.nodes_db.get(txn, id)? {
                        let node = Node::from_bincode_bytes(*id, node_data, &arena)?;
                        if let Some(props) = node.properties
                            && let Some(prop_value) = props.get(prop)
                        {
                            json_node
                                .as_object_mut()
                                .ok_or_else(|| GraphError::New("invalid JSON object".to_string()))?
                                .insert(
                                    "label",
                                    sonic_rs::to_value(&prop_value.inner_stringify())
                                        .unwrap_or_else(|_| sonic_rs::Value::from("")),
                                );
                        }
                    }
                }

                nodes.push(json_node);
                out_edges
                    .iter()
                    .for_each(|(edge_id, from_node_id, to_node_id)| {
                        edges.push(json!({
                            "from": from_node_id.to_string(),
                            "to": to_node_id.to_string(),
                            "title": edge_id.to_string(),
                        }));
                    });

                Ok::<(), GraphError>(())
            })?;

        let result = json!({
            "nodes": nodes,
            "edges": edges,
        });

        Ok(result)
    }
}
