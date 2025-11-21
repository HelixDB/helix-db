use crate::helix_engine::{
    storage_core::{HelixGraphStorage, Txn, storage_methods::StorageMethods},
    types::GraphError,
};

impl HelixGraphStorage {
    pub fn get_db_stats_json<'db>(&self, txn: &Txn<'db>) -> Result<String, GraphError> {
        let cf_nodes = self.cf_nodes();
        let cf_edges = self.cf_edges();
        let cf_vectors = self.vectors.cf_vectors();

        // Count nodes
        let mut num_nodes = 0u64;
        let mut iter = txn.raw_iterator_cf(&cf_nodes);
        iter.seek_to_first();
        while iter.valid() {
            num_nodes += 1;
            iter.next();
        }
        iter.status().map_err(GraphError::from)?;

        // Count edges
        let mut num_edges = 0u64;
        let mut iter = txn.raw_iterator_cf(&cf_edges);
        iter.seek_to_first();
        while iter.valid() {
            num_edges += 1;
            iter.next();
        }
        iter.status().map_err(GraphError::from)?;

        // Count vectors
        let mut num_vectors = 0u64;
        let mut iter = txn.raw_iterator_cf(&cf_vectors);
        iter.seek_to_first();
        while iter.valid() {
            num_vectors += 1;
            iter.next();
        }
        iter.status().map_err(GraphError::from)?;

        let result = sonic_rs::json!({
            "num_nodes": num_nodes,
            "num_edges": num_edges,
            "num_vectors": num_vectors,
        });

        sonic_rs::to_string(&result).map_err(|e| GraphError::New(e.to_string()))
    }

    /// Serialize nodes and edges to JSON for graph visualization (RocksDB implementation)
    pub fn nodes_edges_to_json<'db>(
        &self,
        txn: &Txn<'db>,
        k: Option<usize>,
        node_prop: Option<String>,
    ) -> Result<String, GraphError> {
        let k = k.unwrap_or(200);
        if k > 300 {
            return Err(GraphError::New(
                "cannot visualize more than 300 nodes!".to_string(),
            ));
        }

        let arena = bumpalo::Bump::new();

        // Get top k nodes by cardinality (number of edges)
        let top_nodes = self.get_nodes_by_cardinality_rocks(txn, k, &arena)?;

        // Convert to JSON
        self.cards_to_json_rocks(txn, k, top_nodes, node_prop, &arena)
    }

    #[allow(clippy::type_complexity)]
    fn get_nodes_by_cardinality_rocks<'db, 'arena>(
        &self,
        txn: &Txn<'db>,
        k: usize,
        _arena: &'arena bumpalo::Bump,
    ) -> Result<Vec<(u128, Vec<(u128, u128, u128)>, Vec<(u128, u128, u128)>)>, GraphError> {
        use std::cmp::Ordering;
        use std::collections::{BinaryHeap, HashMap};

        type EdgeID = u128;
        type ToNodeId = u128;
        type FromNodeId = u128;

        #[derive(Eq, PartialEq)]
        struct EdgeCount {
            node_id: u128,
            edges_count: usize,
            out_edges: Vec<(EdgeID, FromNodeId, ToNodeId)>,
            in_edges: Vec<(EdgeID, FromNodeId, ToNodeId)>,
        }

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

        let mut edge_counts: HashMap<
            u128,
            (
                Vec<(EdgeID, FromNodeId, ToNodeId)>,
                Vec<(EdgeID, FromNodeId, ToNodeId)>,
            ),
        > = HashMap::new();

        // Collect out edges
        let cf_out_edges = self.cf_out_edges();
        let mut iter = txn.raw_iterator_cf(&cf_out_edges);
        iter.seek_to_first();

        while iter.valid() {
            if let Some((key, _value)) = iter.item() {
                assert!(key.len() >= 52);
                let (from_node_id, _label, to_node_id, edge_id) = Self::unpack_adj_edge_key(key)?;
                edge_counts
                    .entry(from_node_id)
                    .or_insert_with(|| (Vec::new(), Vec::new()))
                    .0
                    .push((edge_id, from_node_id, to_node_id));
            }
            iter.next();
        }
        iter.status().map_err(GraphError::from)?;

        // Collect in edges
        let cf_in_edges = self.cf_in_edges();
        let mut iter = txn.raw_iterator_cf(&cf_in_edges);
        iter.seek_to_first();

        while iter.valid() {
            if let Some((key, _value)) = iter.item() {
                assert!(key.len() >= 52);
                let (to_node_id, _label, from_node_id, edge_id) = Self::unpack_adj_edge_key(key)?;
                edge_counts
                    .entry(to_node_id)
                    .or_insert_with(|| (Vec::new(), Vec::new()))
                    .1
                    .push((edge_id, from_node_id, to_node_id));
            }
            iter.next();
        }
        iter.status().map_err(GraphError::from)?;

        // Build heap and get top k
        let mut ordered_edge_counts: BinaryHeap<EdgeCount> = edge_counts
            .into_iter()
            .map(|(node_id, (out_edges, in_edges))| EdgeCount {
                node_id,
                edges_count: out_edges.len() + in_edges.len(),
                out_edges,
                in_edges,
            })
            .collect();

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
    fn cards_to_json_rocks<'db, 'arena>(
        &self,
        txn: &Txn<'db>,
        k: usize,
        top_nodes: Vec<(u128, Vec<(u128, u128, u128)>, Vec<(u128, u128, u128)>)>,
        node_prop: Option<String>,
        arena: &'arena bumpalo::Bump,
    ) -> Result<String, GraphError> {
        use crate::utils::id::ID;

        let mut nodes = Vec::with_capacity(k);
        let mut edges = Vec::new();

        for (id, out_edges, _in_edges) in top_nodes.iter() {
            let id_str = ID::from(*id).stringify();
            let mut json_node = sonic_rs::json!({
                "id": id_str.clone(),
                "title": id_str.clone()
            });

            if let Some(prop) = &node_prop {
                // Get node data
                use sonic_rs::JsonValueMutTrait;
                if let Ok(node) = self.get_node(txn, *id, arena)
                    && let Some(props) = node.properties
                    && let Some(prop_value) = props.get(prop)
                    && let Some(obj) = json_node.as_object_mut()
                {
                    obj.insert(
                        "label",
                        sonic_rs::to_value(&prop_value.inner_stringify())
                            .unwrap_or_else(|_| sonic_rs::Value::from("")),
                    );
                }
            }

            nodes.push(json_node);

            for (edge_id, from_node_id, to_node_id) in out_edges.iter() {
                edges.push(sonic_rs::json!({
                    "from": ID::from(*from_node_id).stringify(),
                    "to": ID::from(*to_node_id).stringify(),
                    "title": ID::from(*edge_id).stringify(),
                }));
            }
        }

        let result = sonic_rs::json!({
            "nodes": nodes,
            "edges": edges,
        });

        sonic_rs::to_string(&result).map_err(|e| GraphError::New(e.to_string()))
    }
}
