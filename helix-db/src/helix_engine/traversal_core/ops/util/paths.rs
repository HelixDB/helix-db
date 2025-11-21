use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
        storage_core::storage_methods::StorageMethods,
        traversal_core::{
            RTxn, traversal_iter::RoTraversalIterator, traversal_value::TraversalValue,
        },
        types::GraphError,
    },
    protocol::value::Value,
    utils::{
        items::{Edge, Node},
        label_hash::hash_label,
    },
};
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet, VecDeque},
};

/// Default weight function for backward compatibility
/// Looks for "weight" property on edge, defaults to 1.0
pub fn default_weight_fn<'arena>(
    edge: &Edge<'arena>,
    _src_node: &Node<'arena>,
    _dst_node: &Node<'arena>,
) -> Result<f64, GraphError> {
    Ok(edge
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
            Value::U128(i) => Some(*i as f64),
            _ => None,
        })
        .unwrap_or(1.0))
}

/// Reads heuristic value from a node property
/// Used for A* algorithm to get estimated cost to goal
pub fn property_heuristic<'arena>(
    node: &Node<'arena>,
    property_name: &str,
) -> Result<f64, GraphError> {
    node.properties
        .as_ref()
        .and_then(|props| props.get(property_name))
        .and_then(|v| match v {
            Value::F64(f) => Some(*f),
            Value::F32(f) => Some(*f as f64),
            Value::I64(i) => Some(*i as f64),
            Value::I32(i) => Some(*i as f64),
            Value::I16(i) => Some(*i as f64),
            Value::I8(i) => Some(*i as f64),
            Value::U128(i) => Some(*i as f64),
            Value::U64(i) => Some(*i as f64),
            Value::U32(i) => Some(*i as f64),
            Value::U16(i) => Some(*i as f64),
            Value::U8(i) => Some(*i as f64),
            _ => None,
        })
        .ok_or_else(|| {
            GraphError::TraversalError(format!(
                "Heuristic property '{}' not found or is not a numeric value",
                property_name
            ))
        })
}

#[derive(Debug, Clone)]
pub enum PathType {
    From(u128),
    To(u128),
}

#[derive(Debug, Clone, PartialEq)]
pub enum PathAlgorithm {
    BFS,
    Dijkstra,
    AStar,
}

pub struct ShortestPathIterator<
    'db,
    'arena,
    'txn,
    I,
    F,
    H = fn(&Node<'arena>) -> Result<f64, GraphError>,
> where
    'db: 'arena,
    'arena: 'txn,
    F: Fn(&Edge<'arena>, &Node<'arena>, &Node<'arena>) -> Result<f64, GraphError>,
    H: Fn(&Node<'arena>) -> Result<f64, GraphError>,
{
    pub arena: &'arena bumpalo::Bump,
    pub iter: I,
    path_type: PathType,
    edge_label: Option<&'arena str>,
    storage: &'db HelixGraphStorage,
    txn: &'txn RTxn<'db>,
    algorithm: PathAlgorithm,
    weight_fn: F,
    heuristic_fn: Option<H>,
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

#[derive(Debug, Clone)]
struct AStarState {
    node_id: u128,
    g_score: f64,
    f_score: f64,
}

impl Eq for AStarState {}

impl PartialEq for AStarState {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id
    }
}

impl Ord for AStarState {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap: lower f_score = higher priority
        other
            .f_score
            .partial_cmp(&self.f_score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| self.node_id.cmp(&other.node_id))
    }
}

impl PartialOrd for AStarState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub trait ShortestPathAdapter<'db, 'arena, 'txn, 's, I>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
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
    #[allow(clippy::type_complexity)]
    fn shortest_path(
        self,
        edge_label: Option<&'arena str>,
        from: Option<&'s u128>,
        to: Option<&'s u128>,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;

    fn shortest_path_with_algorithm<F>(
        self,
        edge_label: Option<&'arena str>,
        from: Option<&'s u128>,
        to: Option<&'s u128>,
        algorithm: PathAlgorithm,
        weight_fn: F,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        F: Fn(&Edge<'arena>, &Node<'arena>, &Node<'arena>) -> Result<f64, GraphError>;

    fn shortest_path_astar<F, H>(
        self,
        edge_label: Option<&'arena str>,
        from: Option<&'s u128>,
        to: Option<&'s u128>,
        weight_fn: F,
        heuristic_fn: H,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        F: Fn(&Edge<'arena>, &Node<'arena>, &Node<'arena>) -> Result<f64, GraphError>,
        H: Fn(&Node<'arena>) -> Result<f64, GraphError>;
}

type H = fn(&crate::utils::items::Node) -> Result<f64, crate::helix_engine::types::GraphError>;

impl<'db, 'arena, 'txn, 's, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    ShortestPathAdapter<'db, 'arena, 'txn, 's, I> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline]
    fn shortest_path(
        self,
        edge_label: Option<&'arena str>,
        from: Option<&'s u128>,
        to: Option<&'s u128>,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        self.shortest_path_with_algorithm(
            edge_label,
            from,
            to,
            PathAlgorithm::BFS,
            default_weight_fn,
        )
    }

    #[inline]
    fn shortest_path_with_algorithm<F>(
        self,
        edge_label: Option<&'arena str>,
        from: Option<&'s u128>,
        to: Option<&'s u128>,
        algorithm: PathAlgorithm,
        weight_fn: F,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        F: Fn(&Edge<'arena>, &Node<'arena>, &Node<'arena>) -> Result<f64, GraphError>,
    {
        RoTraversalIterator {
            arena: self.arena,
            inner: ShortestPathIterator::<I, F, H> {
                arena: self.arena,
                iter: self.inner,
                path_type: match (from, to) {
                    (Some(from), None) => PathType::From(*from),
                    (None, Some(to)) => PathType::To(*to),
                    _ => panic!("Invalid shortest path"),
                },
                edge_label,
                storage: self.storage,
                txn: self.txn,
                algorithm,
                weight_fn,
                heuristic_fn: None,
            },
            storage: self.storage,
            txn: self.txn,
        }
    }

    #[inline]
    fn shortest_path_astar<F, H>(
        self,
        edge_label: Option<&'arena str>,
        from: Option<&'s u128>,
        to: Option<&'s u128>,
        weight_fn: F,
        heuristic_fn: H,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        F: Fn(&Edge<'arena>, &Node<'arena>, &Node<'arena>) -> Result<f64, GraphError>,
        H: Fn(&Node<'arena>) -> Result<f64, GraphError>,
    {
        RoTraversalIterator {
            arena: self.arena,
            inner: ShortestPathIterator {
                arena: self.arena,
                iter: self.inner,
                path_type: match (from, to) {
                    (Some(from), None) => PathType::From(*from),
                    (None, Some(to)) => PathType::To(*to),
                    _ => panic!("Invalid shortest path"),
                },
                edge_label,
                storage: self.storage,
                txn: self.txn,
                algorithm: PathAlgorithm::AStar,
                weight_fn,
                heuristic_fn: Some(heuristic_fn),
            },
            storage: self.storage,
            txn: self.txn,
        }
    }
}

// ============================================================================
// LMDB Implementation
// ============================================================================
#[cfg(feature = "lmdb")]
mod lmdb {
    use super::*;

    impl<
        'db: 'arena,
        'arena: 'txn,
        'txn,
        I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
        F: Fn(&Edge<'arena>, &Node<'arena>, &Node<'arena>) -> Result<f64, GraphError>,
        H: Fn(&Node<'arena>) -> Result<f64, GraphError>,
    > Iterator for ShortestPathIterator<'db, 'arena, 'txn, I, F, H>
    {
        type Item = Result<TraversalValue<'arena>, GraphError>;

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
                        PathAlgorithm::AStar => self.astar_shortest_path(from, to),
                    }
                }
                Some(other) => Some(other),
                None => None,
            }
        }
    }

    impl<'db, 'arena, 'txn, I, F, H> ShortestPathIterator<'db, 'arena, 'txn, I, F, H>
    where
        F: Fn(&Edge<'arena>, &Node<'arena>, &Node<'arena>) -> Result<f64, GraphError>,
        H: Fn(&Node<'arena>) -> Result<f64, GraphError>,
    {
        fn reconstruct_path(
            &self,
            parent: &HashMap<u128, (u128, u128)>,
            start_id: u128,
            end_id: u128,
            arena: &'arena bumpalo::Bump,
        ) -> Result<TraversalValue<'arena>, GraphError> {
            let mut nodes = Vec::with_capacity(parent.len());
            let mut edges = Vec::with_capacity(parent.len().saturating_sub(1));

            let mut current = end_id;

            while current != start_id {
                nodes.push(self.storage.get_node(self.txn, current, arena)?);

                let (prev_node, edge) = &parent[&current];
                edges.push(self.storage.get_edge(self.txn, *edge, arena)?);
                current = *prev_node;
            }

            nodes.push(self.storage.get_node(self.txn, start_id, arena)?);

            nodes.reverse();
            edges.reverse();

            Ok(TraversalValue::Path((nodes, edges)))
        }

        fn bfs_shortest_path(
            &self,
            from: u128,
            to: u128,
        ) -> Option<Result<TraversalValue<'arena>, GraphError>> {
            let mut queue = VecDeque::with_capacity(32);
            let mut visited = HashSet::with_capacity(64);
            let mut parent: HashMap<u128, (u128, u128)> = HashMap::with_capacity(32);
            queue.push_back(from);
            visited.insert(from);

            // find shortest-path from one node to itself
            if from == to {
                return Some(self.reconstruct_path(&parent, from, to, self.arena));
            }

            while let Some(current_id) = queue.pop_front() {
                let out_prefix = self.edge_label.map_or_else(
                    || current_id.to_be_bytes().to_vec(),
                    |label| {
                        HelixGraphStorage::out_edge_key(current_id, &hash_label(label, None))
                            .to_vec()
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
                        parent.insert(to_node, (current_id, edge_id));

                        if to_node == to {
                            return Some(self.reconstruct_path(&parent, from, to, self.arena));
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
        ) -> Option<Result<TraversalValue<'arena>, GraphError>> {
            let mut heap = BinaryHeap::new();
            let mut distances = HashMap::with_capacity(64);
            let mut parent: HashMap<u128, (u128, u128)> = HashMap::with_capacity(32);

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
                    return Some(self.reconstruct_path(&parent, from, to, self.arena));
                }

                let out_prefix = self.edge_label.map_or_else(
                    || current_id.to_be_bytes().to_vec(),
                    |label| {
                        HelixGraphStorage::out_edge_key(current_id, &hash_label(label, None))
                            .to_vec()
                    },
                );

                let iter = self
                    .storage
                    .out_edges_db
                    .prefix_iter(self.txn, &out_prefix)
                    .unwrap();

                for result in iter {
                    let (_, value) = result.unwrap(); // TODO: handle error
                    let (edge_id, to_node) =
                        HelixGraphStorage::unpack_adj_edge_data(value).unwrap(); // TODO: handle error

                    let edge = match self.storage.get_edge(self.txn, edge_id, self.arena) {
                        Ok(e) => e,
                        Err(e) => return Some(Err(e)),
                    };

                    // Fetch nodes for full context in weight calculation
                    let src_node = match self.storage.get_node(self.txn, current_id, self.arena) {
                        Ok(n) => n,
                        Err(e) => return Some(Err(e)),
                    };
                    let dst_node = match self.storage.get_node(self.txn, to_node, self.arena) {
                        Ok(n) => n,
                        Err(e) => return Some(Err(e)),
                    };

                    // Call custom weight function with full context
                    let weight = match (self.weight_fn)(&edge, &src_node, &dst_node) {
                        Ok(w) => w,
                        Err(e) => return Some(Err(e)),
                    };

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
                        parent.insert(to_node, (current_id, edge_id));
                        heap.push(DijkstraState {
                            node_id: to_node,
                            distance: new_dist,
                        });
                    }
                }
            }
            Some(Err(GraphError::ShortestPathNotFound))
        }

        fn astar_shortest_path(
            &self,
            from: u128,
            to: u128,
        ) -> Option<Result<TraversalValue<'arena>, GraphError>> {
            let heuristic_fn = match &self.heuristic_fn {
                Some(h) => h,
                None => {
                    return Some(Err(GraphError::TraversalError(
                        "A* algorithm requires a heuristic function".to_string(),
                    )));
                }
            };

            let mut heap = BinaryHeap::new();
            let mut g_scores: HashMap<u128, f64> = HashMap::with_capacity(64);
            let mut parent: HashMap<u128, (u128, u128)> = HashMap::with_capacity(32);

            // Calculate initial heuristic for start node
            let start_node = match self.storage.get_node(self.txn, from, self.arena) {
                Ok(node) => node,
                Err(e) => return Some(Err(e)),
            };

            let h_start = match heuristic_fn(&start_node) {
                Ok(h) => h,
                Err(e) => return Some(Err(e)),
            };

            g_scores.insert(from, 0.0);
            heap.push(AStarState {
                node_id: from,
                g_score: 0.0,
                f_score: h_start,
            });

            while let Some(AStarState {
                node_id: current_id,
                g_score: current_g,
                ..
            }) = heap.pop()
            {
                // Found the target
                if current_id == to {
                    return Some(self.reconstruct_path(&parent, from, to, self.arena));
                }

                // Already found a better path
                if let Some(&best_g) = g_scores.get(&current_id)
                    && current_g > best_g
                {
                    continue;
                }

                let out_prefix = self.edge_label.map_or_else(
                    || current_id.to_be_bytes().to_vec(),
                    |label| {
                        HelixGraphStorage::out_edge_key(current_id, &hash_label(label, None))
                            .to_vec()
                    },
                );

                let iter = self
                    .storage
                    .out_edges_db
                    .prefix_iter(self.txn, &out_prefix)
                    .unwrap();

                for result in iter {
                    let (_, value) = result.unwrap(); // TODO: handle error
                    let (edge_id, to_node) =
                        HelixGraphStorage::unpack_adj_edge_data(value).unwrap(); // TODO: handle error

                    let edge = match self.storage.get_edge(self.txn, edge_id, self.arena) {
                        Ok(e) => e,
                        Err(e) => return Some(Err(e)),
                    };

                    // Fetch nodes for full context in weight calculation
                    let src_node = match self.storage.get_node(self.txn, current_id, self.arena) {
                        Ok(n) => n,
                        Err(e) => return Some(Err(e)),
                    };
                    let dst_node = match self.storage.get_node(self.txn, to_node, self.arena) {
                        Ok(n) => n,
                        Err(e) => return Some(Err(e)),
                    };

                    // Call custom weight function with full context
                    let weight = match (self.weight_fn)(&edge, &src_node, &dst_node) {
                        Ok(w) => w,
                        Err(e) => return Some(Err(e)),
                    };

                    if weight < 0.0 {
                        return Some(Err(GraphError::TraversalError(
                            "Negative edge weights are not supported for A* algorithm".to_string(),
                        )));
                    }

                    let tentative_g = current_g + weight;

                    let should_update = g_scores
                        .get(&to_node)
                        .is_none_or(|&existing_g| tentative_g < existing_g);

                    if should_update {
                        // Calculate heuristic for neighbor
                        let h = match heuristic_fn(&dst_node) {
                            Ok(h) => h,
                            Err(e) => return Some(Err(e)),
                        };

                        let f = tentative_g + h;

                        g_scores.insert(to_node, tentative_g);
                        parent.insert(to_node, (current_id, edge_id));
                        heap.push(AStarState {
                            node_id: to_node,
                            g_score: tentative_g,
                            f_score: f,
                        });
                    }
                }
            }
            Some(Err(GraphError::ShortestPathNotFound))
        }
    }
}

// ============================================================================
// RocksDB Implementation
// ============================================================================
#[cfg(feature = "rocks")]
mod rocks {
    use super::*;
    use crate::helix_engine::rocks_utils::RocksUtils;

    impl<
        'db: 'arena,
        'arena: 'txn,
        'txn,
        I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
        F: Fn(&Edge<'arena>, &Node<'arena>, &Node<'arena>) -> Result<f64, GraphError>,
        H: Fn(&Node<'arena>) -> Result<f64, GraphError>,
    > Iterator for ShortestPathIterator<'db, 'arena, 'txn, I, F, H>
    {
        type Item = Result<TraversalValue<'arena>, GraphError>;

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
                        PathAlgorithm::AStar => self.astar_shortest_path(from, to),
                    }
                }
                Some(other) => Some(other),
                None => None,
            }
        }
    }

    #[cfg(feature = "rocks")]
    impl<'db, 'arena, 'txn, I, F, H> ShortestPathIterator<'db, 'arena, 'txn, I, F, H>
    where
        F: Fn(&Edge<'arena>, &Node<'arena>, &Node<'arena>) -> Result<f64, GraphError>,
        H: Fn(&Node<'arena>) -> Result<f64, GraphError>,
    {
        fn reconstruct_path(
            &self,
            parent: &HashMap<u128, (u128, u128)>,
            start_id: u128,
            end_id: u128,
            arena: &'arena bumpalo::Bump,
        ) -> Result<TraversalValue<'arena>, GraphError> {
            let mut nodes = Vec::with_capacity(parent.len());
            let mut edges = Vec::with_capacity(parent.len().saturating_sub(1));

            let mut current = end_id;

            while current != start_id {
                nodes.push(self.storage.get_node(self.txn, current, arena)?);

                let (prev_node, edge) = &parent[&current];
                edges.push(self.storage.get_edge(self.txn, *edge, arena)?);
                current = *prev_node;
            }

            nodes.push(self.storage.get_node(self.txn, start_id, arena)?);

            nodes.reverse();
            edges.reverse();

            Ok(TraversalValue::Path((nodes, edges)))
        }

        fn bfs_shortest_path(
            &self,
            from: u128,
            to: u128,
        ) -> Option<Result<TraversalValue<'arena>, GraphError>> {
            let mut queue = VecDeque::with_capacity(32);
            let mut visited = HashSet::with_capacity(64);
            let mut parent: HashMap<u128, (u128, u128)> = HashMap::with_capacity(32);
            queue.push_back(from);
            visited.insert(from);

            // find shortest-path from one node to itself
            if from == to {
                return Some(self.reconstruct_path(&parent, from, to, self.arena));
            }

            while let Some(current_id) = queue.pop_front() {
                // For RocksDB, we need to create a prefix that's only 20 bytes (node_id + label)
                // since the full key is 36 bytes (node_id + label + to_node)

                use crate::helix_engine::rocks_utils::RocksUtils;
                let out_prefix = self.edge_label.map_or_else(
                    || current_id.to_be_bytes().to_vec(),
                    |label| {
                        HelixGraphStorage::out_edge_key_prefix(current_id, &hash_label(label, None))
                            .to_vec()
                    },
                );

                let mut iter = self
                    .txn
                    .raw_prefix_iter(&self.storage.cf_out_edges(), &out_prefix);

                while let Some(key) = iter.key() {
                    let (_, _, to_node_id, edge_id) =
                        HelixGraphStorage::unpack_adj_edge_key(key).unwrap();
                    if !visited.contains(&to_node_id) {
                        visited.insert(to_node_id);
                        parent.insert(to_node_id, (current_id, edge_id));

                        if to_node_id == to {
                            return Some(self.reconstruct_path(&parent, from, to, self.arena));
                        }

                        queue.push_back(to_node_id);
                    }
                    iter.next();
                }
            }
            Some(Err(GraphError::ShortestPathNotFound))
        }

        fn dijkstra_shortest_path(
            &self,
            from: u128,
            to: u128,
        ) -> Option<Result<TraversalValue<'arena>, GraphError>> {
            let mut heap = BinaryHeap::new();
            let mut distances = HashMap::with_capacity(64);
            let mut parent: HashMap<u128, (u128, u128)> = HashMap::with_capacity(32);

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
                    return Some(self.reconstruct_path(&parent, from, to, self.arena));
                }

                // For RocksDB, create a 20-byte prefix (node_id + label)
                let out_prefix = self.edge_label.map_or_else(
                    || current_id.to_be_bytes().to_vec(),
                    |label| {
                        HelixGraphStorage::out_edge_key_prefix(current_id, &hash_label(label, None))
                            .to_vec()
                    },
                );

                let mut iter = self
                    .txn
                    .raw_prefix_iter(&self.storage.cf_out_edges(), &out_prefix);

                while let Some(key) = iter.key() {
                    let (_, _, to_node_id, edge_id) =
                        HelixGraphStorage::unpack_adj_edge_key(key).unwrap();

                    let edge = match self.storage.get_edge(self.txn, edge_id, self.arena) {
                        Ok(e) => e,
                        Err(e) => return Some(Err(e)),
                    };

                    // Fetch nodes for full context in weight calculation
                    let src_node = match self.storage.get_node(self.txn, current_id, self.arena) {
                        Ok(n) => n,
                        Err(e) => return Some(Err(e)),
                    };
                    let dst_node = match self.storage.get_node(self.txn, to_node_id, self.arena) {
                        Ok(n) => n,
                        Err(e) => return Some(Err(e)),
                    };

                    // Call custom weight function with full context
                    let weight = match (self.weight_fn)(&edge, &src_node, &dst_node) {
                        Ok(w) => w,
                        Err(e) => return Some(Err(e)),
                    };

                    if weight < 0.0 {
                        return Some(Err(GraphError::TraversalError(
                            "Negative edge weights are not supported for Dijkstra's algorithm"
                                .to_string(),
                        )));
                    }

                    let new_dist = current_dist + weight;

                    let should_update = distances
                        .get(&to_node_id)
                        .is_none_or(|&existing_dist| new_dist < existing_dist);

                    if should_update {
                        distances.insert(to_node_id, new_dist);
                        parent.insert(to_node_id, (current_id, edge_id));
                        heap.push(DijkstraState {
                            node_id: to_node_id,
                            distance: new_dist,
                        });
                    }
                    iter.next();
                }
            }
            Some(Err(GraphError::ShortestPathNotFound))
        }

        fn astar_shortest_path(
            &self,
            from: u128,
            to: u128,
        ) -> Option<Result<TraversalValue<'arena>, GraphError>> {
            let heuristic_fn = match &self.heuristic_fn {
                Some(h) => h,
                None => {
                    return Some(Err(GraphError::TraversalError(
                        "A* algorithm requires a heuristic function".to_string(),
                    )));
                }
            };

            let mut heap = BinaryHeap::new();
            let mut g_scores: HashMap<u128, f64> = HashMap::with_capacity(64);
            let mut parent: HashMap<u128, (u128, u128)> = HashMap::with_capacity(32);

            // Calculate initial heuristic for start node
            let start_node = match self.storage.get_node(self.txn, from, self.arena) {
                Ok(node) => node,
                Err(e) => return Some(Err(e)),
            };

            let h_start = match heuristic_fn(&start_node) {
                Ok(h) => h,
                Err(e) => return Some(Err(e)),
            };

            g_scores.insert(from, 0.0);
            heap.push(AStarState {
                node_id: from,
                g_score: 0.0,
                f_score: h_start,
            });

            while let Some(AStarState {
                node_id: current_id,
                g_score: current_g,
                ..
            }) = heap.pop()
            {
                // Found the target
                if current_id == to {
                    return Some(self.reconstruct_path(&parent, from, to, self.arena));
                }

                // Already found a better path
                if let Some(&best_g) = g_scores.get(&current_id)
                    && current_g > best_g
                {
                    continue;
                }

                // For RocksDB, create a 20-byte prefix (node_id + label)
                let out_prefix = self.edge_label.map_or_else(
                    || current_id.to_be_bytes().to_vec(),
                    |label| {
                        HelixGraphStorage::out_edge_key_prefix(current_id, &hash_label(label, None))
                            .to_vec()
                    },
                );
                println!("iterating");
                let mut iter = self
                    .txn
                    .raw_prefix_iter(&self.storage.cf_out_edges(), &out_prefix);

                while let Some(key) = iter.key() {
                    let (_, _, to_node_id, edge_id) =
                        HelixGraphStorage::unpack_adj_edge_key(key).unwrap();

                    let edge = match self.storage.get_edge(self.txn, edge_id, self.arena) {
                        Ok(e) => e,
                        Err(e) => return Some(Err(e)),
                    };

                    // Fetch nodes for full context in weight calculation
                    let src_node = match self.storage.get_node(self.txn, current_id, self.arena) {
                        Ok(n) => n,
                        Err(e) => return Some(Err(e)),
                    };
                    let dst_node = match self.storage.get_node(self.txn, to_node_id, self.arena) {
                        Ok(n) => n,
                        Err(e) => return Some(Err(e)),
                    };

                    // Call custom weight function with full context
                    let weight = match (self.weight_fn)(&edge, &src_node, &dst_node) {
                        Ok(w) => w,
                        Err(e) => return Some(Err(e)),
                    };

                    if weight < 0.0 {
                        return Some(Err(GraphError::TraversalError(
                            "Negative edge weights are not supported for A* algorithm".to_string(),
                        )));
                    }

                    let tentative_g = current_g + weight;

                    let should_update = g_scores
                        .get(&to_node_id)
                        .is_none_or(|&existing_g| tentative_g < existing_g);

                    if should_update {
                        // Calculate heuristic for neighbor
                        let h = match heuristic_fn(&dst_node) {
                            Ok(h) => h,
                            Err(e) => return Some(Err(e)),
                        };

                        let f = tentative_g + h;

                        g_scores.insert(to_node_id, tentative_g);
                        parent.insert(to_node_id, (current_id, edge_id));
                        heap.push(AStarState {
                            node_id: to_node_id,
                            g_score: tentative_g,
                            f_score: f,
                        });
                    }
                    iter.next();
                }
            }
            Some(Err(GraphError::ShortestPathNotFound))
        }
    }
}
