use crate::{
    helix_engine::{
        storage_core::{HelixGraphStorage, storage_methods::StorageMethods},
        traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
        types::GraphError,
    },
    protocol::value::Value,
    utils::{items::Edge, label_hash::hash_label},
};
use heed3::RoTxn;
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet, VecDeque},
    sync::Arc,
};

#[derive(Debug, Clone)]
pub enum PathType {
    From(u128),
    To(u128),
}

#[derive(Debug, Clone, PartialEq)]
pub enum PathAlgorithm {
    BFS,
    Dijkstra,
}

pub struct ShortestPathIterator<'a, I> {
    iter: I,
    path_type: PathType,
    edge_label: Option<&'a str>,
    storage: Arc<HelixGraphStorage>,
    txn: &'a RoTxn<'a>,
    algorithm: PathAlgorithm,
}

#[derive(Debug, Clone)]
struct DijkstraState {
    node_id: u128,
    distance: f64,
}

impl Eq for DijkstraState {}

impl PartialEq for DijkstraState {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id
    }
}

impl Ord for DijkstraState {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .distance
            .partial_cmp(&self.distance)
            .unwrap_or(Ordering::Equal)
            .then_with(|| self.node_id.cmp(&other.node_id))
    }
}

impl PartialOrd for DijkstraState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a, I: Iterator<Item = Result<TraversalValue, GraphError>>> Iterator
    for ShortestPathIterator<'a, I>
{
    type Item = Result<TraversalValue, GraphError>;

    /// Returns the next outgoing node by decoding the edge id and then getting the edge and node
    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok(TraversalValue::Node(node))) => {
                let (from, to) = match self.path_type {
                    PathType::From(from) => (from, node.id),
                    PathType::To(to) => (node.id, to),
                };

                match self.algorithm {
                    PathAlgorithm::BFS => self.bfs_shortest_path(from, to),
                    PathAlgorithm::Dijkstra => self.dijkstra_shortest_path(from, to),
                }
            }
            Some(other) => Some(other),
            None => None,
        }
    }
}

impl<'a, I> ShortestPathIterator<'a, I> {
    fn reconstruct_path(
        &self,
        parent: &HashMap<u128, (u128, Edge)>,
        start_id: &u128,
        end_id: &u128,
    ) -> Result<TraversalValue, GraphError> {
        let mut nodes = Vec::with_capacity(parent.len());
        let mut edges = Vec::with_capacity(parent.len() - 1);

        let mut current = end_id;

        while current != start_id {
            nodes.push(self.storage.get_node(self.txn, current)?);

            let (prev_node, edge) = &parent[current];
            edges.push(edge.clone());
            current = prev_node;
        }

        nodes.push(self.storage.get_node(self.txn, start_id)?);

        nodes.reverse();
        edges.reverse();

        Ok(TraversalValue::Path((nodes, edges)))
    }

    fn bfs_shortest_path(
        &self,
        from: u128,
        to: u128,
    ) -> Option<Result<TraversalValue, GraphError>> {
        let mut queue = VecDeque::with_capacity(32);
        let mut visited = HashSet::with_capacity(64);
        let mut parent: HashMap<u128, (u128, Edge)> = HashMap::with_capacity(32);
        queue.push_back(from);
        visited.insert(from);

        while let Some(current_id) = queue.pop_front() {
            let out_prefix = self.edge_label.map_or_else(
                || current_id.to_be_bytes().to_vec(),
                |label| {
                    HelixGraphStorage::out_edge_key(&current_id, &hash_label(label, None)).to_vec()
                },
            );

            let iter = self
                .storage
                .out_edges_db
                .prefix_iter(self.txn, &out_prefix)
                .unwrap();

            for result in iter {
                let value = match result {
                    Ok((_, value)) => value,
                    Err(e) => return Some(Err(GraphError::from(e))),
                };
                let (edge_id, to_node) = match HelixGraphStorage::unpack_adj_edge_data(value) {
                    Ok((edge_id, to_node)) => (edge_id, to_node),
                    Err(e) => return Some(Err(e)),
                };

                if !visited.contains(&to_node) {
                    visited.insert(to_node);
                    let edge = match self.storage.get_edge(self.txn, &edge_id) {
                        Ok(edge) => edge,
                        Err(e) => return Some(Err(e)),
                    };
                    parent.insert(to_node, (current_id, edge));

                    if to_node == to {
                        return Some(self.reconstruct_path(&parent, &from, &to));
                    }

                    queue.push_back(to_node);
                }
            }
        }
        Some(Err(GraphError::ShortestPathNotFound))
    }

    fn dijkstra_shortest_path(
        &self,
        from: u128,
        to: u128,
    ) -> Option<Result<TraversalValue, GraphError>> {
        let mut heap = BinaryHeap::new();
        let mut distances = HashMap::with_capacity(64);
        let mut parent: HashMap<u128, (u128, Edge)> = HashMap::with_capacity(32);

        distances.insert(from, 0.0);
        heap.push(DijkstraState {
            node_id: from,
            distance: 0.0,
        });

        while let Some(DijkstraState {
            node_id: current_id,
            distance: current_dist,
        }) = heap.pop()
        {
            // Already found a better path
            if let Some(&best_dist) = distances.get(&current_id)
                && current_dist > best_dist
            {
                continue;
            }

            // Found the target
            if current_id == to {
                return Some(self.reconstruct_path(&parent, &from, &to));
            }

            let out_prefix = self.edge_label.map_or_else(
                || current_id.to_be_bytes().to_vec(),
                |label| {
                    HelixGraphStorage::out_edge_key(&current_id, &hash_label(label, None)).to_vec()
                },
            );

            let iter = self
                .storage
                .out_edges_db
                .prefix_iter(self.txn, &out_prefix)
                .unwrap();

            for result in iter {
                let (_, value) = result.unwrap(); // TODO: handle error
                let (edge_id, to_node) = HelixGraphStorage::unpack_adj_edge_data(value).unwrap(); // TODO: handle error

                let edge = self.storage.get_edge(self.txn, &edge_id).unwrap(); // TODO: handle error

                // Extract weight from edge properties, default to 1.0 if not present
                let weight = edge
                    .properties
                    .as_ref()
                    .and_then(|props| props.get("weight"))
                    .and_then(|w| match w {
                        Value::F32(f) => Some(*f as f64),
                        Value::F64(f) => Some(*f),
                        Value::I8(i) => Some(*i as f64),
                        Value::I16(i) => Some(*i as f64),
                        Value::I32(i) => Some(*i as f64),
                        Value::I64(i) => Some(*i as f64),
                        Value::U8(i) => Some(*i as f64),
                        Value::U16(i) => Some(*i as f64),
                        Value::U32(i) => Some(*i as f64),
                        Value::U64(i) => Some(*i as f64),
                        _ => None,
                    })
                    .unwrap_or(1.0);

                if weight < 0.0 {
                    return Some(Err(GraphError::TraversalError(
                        "Negative edge weights are not supported for Dijkstra's algorithm"
                            .to_string(),
                    )));
                }

                let new_dist = current_dist + weight;

                let should_update = distances
                    .get(&to_node)
                    .is_none_or(|&existing_dist| new_dist < existing_dist);

                if should_update {
                    distances.insert(to_node, new_dist);
                    parent.insert(to_node, (current_id, edge));
                    heap.push(DijkstraState {
                        node_id: to_node,
                        distance: new_dist,
                    });
                }
            }
        }
        Some(Err(GraphError::ShortestPathNotFound))
    }
}

pub trait ShortestPathAdapter<'a, I>: Iterator<Item = Result<TraversalValue, GraphError>> {
    /// ShortestPath finds the shortest path between two nodes
    ///
    /// # Arguments
    ///
    /// * `edge_label` - The label of the edge to use
    /// * `from` - The starting node
    /// * `to` - The ending node
    ///
    /// # Example
    ///
    /// ```rust
    /// let node1 = Node { id: 1, label: "Person".to_string(), properties: None };
    /// let node2 = Node { id: 2, label: "Person".to_string(), properties: None };
    /// let traversal = G::new(storage, &txn).shortest_path(Some("knows"), Some(&node1.id), Some(&node2.id));
    /// ```
    fn shortest_path(
        self,
        edge_label: Option<&'a str>,
        from: Option<&'a u128>,
        to: Option<&'a u128>,
    ) -> RoTraversalIterator<'a, ShortestPathIterator<'a, I>>
    where
        I: 'a;

    fn shortest_path_with_algorithm(
        self,
        edge_label: Option<&'a str>,
        from: Option<&'a u128>,
        to: Option<&'a u128>,
        algorithm: PathAlgorithm,
    ) -> RoTraversalIterator<'a, ShortestPathIterator<'a, I>>
    where
        I: 'a;
}

impl<'a, I: Iterator<Item = Result<TraversalValue, GraphError>> + 'a> ShortestPathAdapter<'a, I>
    for RoTraversalIterator<'a, I>
{
    #[inline]
    fn shortest_path(
        self,
        edge_label: Option<&'a str>,
        from: Option<&'a u128>,
        to: Option<&'a u128>,
    ) -> RoTraversalIterator<'a, ShortestPathIterator<'a, I>>
    where
        I: 'a,
    {
        self.shortest_path_with_algorithm(edge_label, from, to, PathAlgorithm::BFS)
    }

    #[inline]
    fn shortest_path_with_algorithm(
        self,
        edge_label: Option<&'a str>,
        from: Option<&'a u128>,
        to: Option<&'a u128>,
        algorithm: PathAlgorithm,
    ) -> RoTraversalIterator<'a, ShortestPathIterator<'a, I>>
    where
        I: 'a,
    {
        let storage = Arc::clone(&self.storage);
        let txn = self.txn;

        RoTraversalIterator {
            inner: ShortestPathIterator {
                iter: self.inner,
                path_type: match (from, to) {
                    (Some(from), None) => PathType::From(*from),
                    (None, Some(to)) => PathType::To(*to),
                    _ => panic!("Invalid shortest path"),
                },
                edge_label,
                storage,
                txn,
                algorithm,
            },
            storage: Arc::clone(&self.storage),
            txn: self.txn,
        }
    }
}
