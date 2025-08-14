use crate::{
    debug_println,
    helix_engine::{storage_core::storage_core::HelixGraphStorage, types::GraphError},
    utils::items::Node,
    utils::id::ID,
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
    ///
    /// # Arguments
    /// * `txn` - Read-only transaction for database access.
    /// * `k` - Optional number of nodes to visualize (default: 200, max: 300).
    /// * `node_prop` - Optional node property to use as label.
    ///
    /// # Returns
    /// JSON string containing nodes and edges, or a `GraphError` if the database is empty or
    /// serialization fails.
    ///
    /// # Errors
    /// Returns `GraphError` if:
    /// - More than 300 nodes are requested.
    /// - Nodes or edges database is empty.
    /// - JSON serialization fails.
    fn nodes_edges_to_json(
        &self,
        txn: &RoTxn,
        k: Option<usize>,
        node_prop: Option<String>,
    ) -> Result<String, GraphError>;

    /// Retrieves database statistics in JSON format.
    ///
    /// # Arguments
    /// * `txn` - Read-only transaction for database access.
    ///
    /// # Returns
    /// JSON string with counts of nodes, edges, and vectors, or a `GraphError` if serialization
    /// fails.
    ///
    /// # Errors
    /// Returns `GraphError` if JSON serialization fails.
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
    ///
    /// Output:
    /// Vec [
    ///     node_id: u128,
    ///     out_edges: Vec<(EdgeID, FromNodeId, ToNodeId)>,
    ///     in_edges: Vec<(EdgeID, FromNodeId, ToNodeId)>,
    /// ]
    // TODO: refactor into EdgeData type
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
        impl PartialOrd for EdgeCount {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Eq for EdgeCount {}
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

        // out edges
        // this gets each node ID from the out edges db
        // by using the out_edges_db it pulls data into os cache
        let out_node_key_iter = out_db.out_edges_db.lazily_decode_data().iter(txn).unwrap();
        for data in out_node_key_iter {
            match data {
                Ok((key, _)) => {
                    let node_id = &key[0..16];
                    // for each node id, it gets the edges which are all stored in the same key
                    // so it gets all the edges for a node at once
                    // without decoding anything. so you only ever decode the key from LMDB once
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
        // this gets each node ID from the in edges db
        // by using the in_edges_db it pulls data into os cache
        let in_node_key_iter = in_db.in_edges_db.lazily_decode_data().iter(txn).unwrap();
        for data in in_node_key_iter {
            match data {
                Ok((key, _)) => {
                    let node_id = &key[0..16];
                    // for each node id, it gets the edges which are all stored in the same key
                    // so it gets all the edges for a node at once
                    // without decoding anything. so you only ever decode the key from LMDB once
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

        // for each node, get the decode the edges and extract the edge id and other node id
        // and add to the ordered_edge_counts heap
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

    /// Output:
    /// {
    ///     "nodes": [{"id": uuid_id_node, "label": "optional_property", "title": "uuid"}],
    ///     "edges": [{"from": uuid, "to": uuid, "title": "uuid"}]
    /// }
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

        top_nodes
            .iter()
            .try_for_each(|(id, out_edges, _in_edges)| {
                let id_str = ID::from(*id).stringify();
                let mut json_node = json!({ "id": id_str.clone(), "title": id_str });
                if let Some(prop) = &node_prop {
                    let mut node = self
                        .nodes_db
                        .lazily_decode_data()
                        .prefix_iter(txn, id)
                        .unwrap();
                    if let Some((_, data)) = node.next().transpose().unwrap() {
                        let node = Node::decode_node(data.decode().unwrap(), *id)?;
                        let props = node.properties.as_ref().ok_or_else(|| {
                            GraphError::New(format!("no properties for node {id}"))
                        })?;
                        let prop_value = props.get(prop).ok_or_else(|| {
                            GraphError::New(format!("property {prop} not found for node {id}"))
                        })?;
                        json_node
                            .as_object_mut()
                            .ok_or_else(|| GraphError::New("invalid JSON object".to_string()))?
                            .insert("label", json!(prop_value));
                    }
                }

                nodes.push(json_node);
                out_edges
                    .iter()
                    .for_each(|(edge_id, from_node_id, to_node_id)| {
                        edges.push(json!({
                            "from": ID::from(*from_node_id).stringify(),
                            "to": ID::from(*to_node_id).stringify(),
                            "title": ID::from(*edge_id).stringify(),
                        }));
                    });

                /*
                   in_edges.iter().for_each(|(edge_id, from_node_id, to_node_id)| {
                   edges.push(json!({
                   "from": from_node_id.to_string(),
                   "to": to_node_id.to_string(),
                   "title": edge_id.to_string(),
                   }));
                   });
                */

                Ok::<(), GraphError>(())
            })?;

        let result = json!({
            "nodes": nodes,
            "edges": edges,
        });

        Ok(result)
    }
}
