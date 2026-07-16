use std::collections::{BTreeSet, VecDeque};

use serde::{Deserialize, Serialize};

use crate::{Edge, EdgeId, ExternalId, Graph, GraphError, NodeId};

/// Direction used by local graph traversals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraversalDirection {
    /// Follow stored source-to-target direction.
    Out,
    /// Follow stored target-to-source direction.
    In,
    /// Follow both directions.
    Both,
}

/// Traversal algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraversalStrategy {
    /// FIFO breadth-first traversal.
    BreadthFirst,
    /// Explicit-stack depth-first traversal.
    DepthFirst,
}

/// Whether high-degree nodes may expand their adjacency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum HubExpansionPolicy {
    /// Expand every visited node.
    ExpandAll,
    /// Include high-degree non-seeds but do not expand them.
    StopNonSeedAtOrAbove {
        /// Inclusive degree threshold.
        degree: usize,
    },
}

/// Complete Graphify-compatible traversal options.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraversalOptions {
    /// BFS or DFS.
    pub strategy: TraversalStrategy,
    /// One or more external node IDs.
    pub seeds: Vec<NodeId>,
    /// Maximum emitted depth. Zero emits only seeds.
    pub max_depth: usize,
    /// Edge direction.
    pub direction: TraversalDirection,
    /// Allowed edge labels. Empty means every label.
    pub allowed_labels: BTreeSet<String>,
    /// Optional hub suppression.
    pub hub_policy: HubExpansionPolicy,
}

impl TraversalOptions {
    /// Construct breadth-first traversal options.
    pub fn breadth_first<I>(seeds: impl IntoIterator<Item = I>, max_depth: usize) -> Self
    where
        I: Into<NodeId>,
    {
        Self {
            strategy: TraversalStrategy::BreadthFirst,
            seeds: seeds.into_iter().map(Into::into).collect(),
            max_depth,
            direction: TraversalDirection::Both,
            allowed_labels: BTreeSet::new(),
            hub_policy: HubExpansionPolicy::ExpandAll,
        }
    }

    /// Construct depth-first traversal options.
    pub fn depth_first<I>(seeds: impl IntoIterator<Item = I>, max_depth: usize) -> Self
    where
        I: Into<NodeId>,
    {
        Self {
            strategy: TraversalStrategy::DepthFirst,
            ..Self::breadth_first(seeds, max_depth)
        }
    }
}

/// One visited node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Visit {
    /// External node identity.
    pub node_id: NodeId,
    /// Minimum BFS depth or DFS discovery depth.
    pub depth: usize,
    /// Stable zero-based discovery order.
    pub discovery_order: usize,
}

/// Whether an edge was traversed with or against its stored orientation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeTraversalDirection {
    /// Stored source to stored target.
    Forward,
    /// Stored target to stored source.
    Reverse,
}

/// Edge responsible for one discovery.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TraversedEdge {
    /// Stable edge ID.
    pub edge_id: EdgeId,
    /// Optional Graphify key.
    pub graphify_key: Option<ExternalId>,
    /// Stored source identity.
    pub source: NodeId,
    /// Stored target identity.
    pub target: NodeId,
    /// Orientation used during traversal.
    pub traversal_direction: EdgeTraversalDirection,
    /// Optional edge label.
    pub label: Option<String>,
}

/// Stable traversal output.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TraversalResult {
    /// Visited nodes in discovery order.
    pub visits: Vec<Visit>,
    /// One edge per non-seed discovery.
    pub discovery_edges: Vec<TraversedEdge>,
}

/// Degree flavor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DegreeKind {
    /// Stored incoming edges.
    In,
    /// Stored outgoing edges.
    Out,
    /// Incoming plus outgoing. A self-loop contributes two.
    Total,
}

/// One deterministic node degree record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeDegree {
    /// External node identity.
    pub node_id: NodeId,
    /// Unweighted degree.
    pub degree: usize,
    /// Weighted degree, using one for edges without an explicit weight.
    pub weighted_degree: f64,
}

/// One edge in a found path.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PathEdge {
    /// Stable edge ID.
    pub edge_id: EdgeId,
    /// Optional Graphify key.
    pub graphify_key: Option<ExternalId>,
    /// Stored source identity.
    pub source: NodeId,
    /// Stored target identity.
    pub target: NodeId,
    /// Orientation used along the path.
    pub traversal_direction: EdgeTraversalDirection,
    /// Optional label.
    pub label: Option<String>,
    /// Selected edge properties.
    pub attributes: crate::Attributes,
}

/// Exhaustive local shortest-path result states.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum PathResult {
    /// Source is absent from the loaded graph.
    MissingSource,
    /// Target is absent from the loaded graph.
    MissingTarget,
    /// Both endpoints exist but no allowed path exists.
    NoPath,
    /// Found shortest path.
    Found {
        /// Node sequence including both endpoints.
        node_ids: Vec<NodeId>,
        /// Edge sequence aligned between adjacent nodes.
        edges: Vec<PathEdge>,
    },
}

impl Graph {
    /// Execute deterministic BFS or DFS over the loaded graph.
    pub fn traverse(&self, options: &TraversalOptions) -> Result<TraversalResult, GraphError> {
        if options.seeds.is_empty() {
            return Err(GraphError::InvalidOption(
                "traversal requires at least one seed".to_string(),
            ));
        }
        let mut seed_indexes = Vec::new();
        let mut seed_set = BTreeSet::new();
        for seed in &options.seeds {
            let index = self.node_index(seed)?;
            if seed_set.insert(index) {
                seed_indexes.push(index);
            }
        }
        match options.strategy {
            TraversalStrategy::BreadthFirst => self.breadth_first(options, &seed_indexes),
            TraversalStrategy::DepthFirst => self.depth_first(options, &seed_indexes),
        }
    }

    fn breadth_first(
        &self,
        options: &TraversalOptions,
        seeds: &[usize],
    ) -> Result<TraversalResult, GraphError> {
        let mut visited = vec![false; self.node_count()];
        let mut queue = VecDeque::new();
        let mut visits = Vec::new();
        for seed in seeds {
            visited[*seed] = true;
            queue.push_back((*seed, 0));
            visits.push(Visit {
                node_id: self.node_id(*seed).clone(),
                depth: 0,
                discovery_order: visits.len(),
            });
        }
        let mut discovery_edges = Vec::new();
        while let Some((node, depth)) = queue.pop_front() {
            if depth >= options.max_depth
                || (!seeds.contains(&node) && self.suppresses_hub(node, options))
            {
                continue;
            }
            for arc in self.arcs(node, options.direction) {
                let edge = self.edge_at(arc.edge);
                if !edge_label_allowed(edge, &options.allowed_labels) || visited[arc.neighbor] {
                    continue;
                }
                visited[arc.neighbor] = true;
                let next_depth = depth + 1;
                queue.push_back((arc.neighbor, next_depth));
                visits.push(Visit {
                    node_id: self.node_id(arc.neighbor).clone(),
                    depth: next_depth,
                    discovery_order: visits.len(),
                });
                discovery_edges.push(traversed_edge(self, node, edge));
            }
        }
        Ok(TraversalResult {
            visits,
            discovery_edges,
        })
    }

    fn depth_first(
        &self,
        options: &TraversalOptions,
        seeds: &[usize],
    ) -> Result<TraversalResult, GraphError> {
        let mut visited = vec![false; self.node_count()];
        let mut stack = Vec::new();
        let mut visits = Vec::new();
        for seed in seeds.iter().rev() {
            if !visited[*seed] {
                visited[*seed] = true;
                stack.push((*seed, 0, None));
            }
        }
        let mut discovery_edges = Vec::new();
        while let Some((node, depth, discovery)) = stack.pop() {
            visits.push(Visit {
                node_id: self.node_id(node).clone(),
                depth,
                discovery_order: visits.len(),
            });
            if let Some((previous, edge_index)) = discovery {
                discovery_edges.push(traversed_edge(self, previous, self.edge_at(edge_index)));
            }
            if depth >= options.max_depth
                || (!seeds.contains(&node) && self.suppresses_hub(node, options))
            {
                continue;
            }
            let mut discovered = BTreeSet::new();
            let arcs = self
                .arcs(node, options.direction)
                .filter(|arc| {
                    !visited[arc.neighbor]
                        && edge_label_allowed(self.edge_at(arc.edge), &options.allowed_labels)
                        && discovered.insert(arc.neighbor)
                })
                .collect::<Vec<_>>();
            for arc in arcs.into_iter().rev() {
                visited[arc.neighbor] = true;
                stack.push((arc.neighbor, depth + 1, Some((node, arc.edge))));
            }
        }
        Ok(TraversalResult {
            visits,
            discovery_edges,
        })
    }

    fn suppresses_hub(&self, node: usize, options: &TraversalOptions) -> bool {
        match options.hub_policy {
            HubExpansionPolicy::ExpandAll => false,
            HubExpansionPolicy::StopNonSeedAtOrAbove { degree } => {
                self.unweighted_degree_at(node, DegreeKind::Total) >= degree
            }
        }
    }

    /// Compute one node degree.
    pub fn degree(
        &self,
        node_id: impl Into<NodeId>,
        kind: DegreeKind,
    ) -> Result<NodeDegree, GraphError> {
        let node_id = node_id.into();
        let node = self.node_index(&node_id)?;
        Ok(NodeDegree {
            node_id,
            degree: self.unweighted_degree_at(node, kind),
            weighted_degree: self.weighted_degree_at(node, kind),
        })
    }

    /// Compute degrees for all nodes in deterministic ID order.
    pub fn degrees(&self, kind: DegreeKind) -> Vec<NodeDegree> {
        (0..self.node_count())
            .map(|node| NodeDegree {
                node_id: self.node_id(node).clone(),
                degree: self.unweighted_degree_at(node, kind),
                weighted_degree: self.weighted_degree_at(node, kind),
            })
            .collect()
    }

    fn unweighted_degree_at(&self, node: usize, kind: DegreeKind) -> usize {
        if !self.is_directed() {
            return self.outgoing(node).len() + self.incoming(node).len();
        }
        match kind {
            DegreeKind::In => self.incoming(node).len(),
            DegreeKind::Out => self.outgoing(node).len(),
            DegreeKind::Total => self.incoming(node).len() + self.outgoing(node).len(),
        }
    }

    fn weighted_degree_at(&self, node: usize, kind: DegreeKind) -> f64 {
        let sum = |arcs: &[crate::model::ArcRef]| {
            arcs.iter()
                .map(|arc| self.edge_at(arc.edge).weight.unwrap_or(1.0))
                .sum::<f64>()
        };
        if !self.is_directed() {
            return sum(self.outgoing(node)) + sum(self.incoming(node));
        }
        match kind {
            DegreeKind::In => sum(self.incoming(node)),
            DegreeKind::Out => sum(self.outgoing(node)),
            DegreeKind::Total => sum(self.incoming(node)) + sum(self.outgoing(node)),
        }
    }

    /// Find an unweighted shortest path in the loaded graph.
    pub fn shortest_path(
        &self,
        source: impl Into<NodeId>,
        target: impl Into<NodeId>,
        direction: TraversalDirection,
        allowed_labels: &BTreeSet<String>,
        max_depth: Option<usize>,
    ) -> PathResult {
        let source = source.into();
        let target = target.into();
        let Ok(source_index) = self.node_index(&source) else {
            return PathResult::MissingSource;
        };
        let Ok(target_index) = self.node_index(&target) else {
            return PathResult::MissingTarget;
        };
        if source_index == target_index {
            return PathResult::Found {
                node_ids: vec![source],
                edges: Vec::new(),
            };
        }
        let mut visited = vec![false; self.node_count()];
        let mut predecessor = vec![None::<(usize, usize)>; self.node_count()];
        let mut queue = VecDeque::from([(source_index, 0)]);
        visited[source_index] = true;
        while let Some((node, depth)) = queue.pop_front() {
            if max_depth.is_some_and(|bound| depth >= bound) {
                continue;
            }
            for arc in self.arcs(node, direction) {
                if visited[arc.neighbor]
                    || !edge_label_allowed(self.edge_at(arc.edge), allowed_labels)
                {
                    continue;
                }
                visited[arc.neighbor] = true;
                predecessor[arc.neighbor] = Some((node, arc.edge));
                if arc.neighbor == target_index {
                    return self.reconstruct_path(source_index, target_index, &predecessor);
                }
                queue.push_back((arc.neighbor, depth + 1));
            }
        }
        PathResult::NoPath
    }

    fn reconstruct_path(
        &self,
        source: usize,
        target: usize,
        predecessor: &[Option<(usize, usize)>],
    ) -> PathResult {
        let mut nodes = vec![target];
        let mut path_edges = Vec::new();
        let mut current = target;
        while current != source {
            let Some((previous, edge_index)) = predecessor[current] else {
                unreachable!("visited target has a complete predecessor chain")
            };
            let edge = self.edge_at(edge_index);
            path_edges.push(PathEdge {
                edge_id: edge.id.clone(),
                graphify_key: edge.graphify_key.clone(),
                source: edge.source.clone(),
                target: edge.target.clone(),
                traversal_direction: edge_direction(self, previous, edge),
                label: edge.label.clone(),
                attributes: edge.attributes.clone(),
            });
            nodes.push(previous);
            current = previous;
        }
        nodes.reverse();
        path_edges.reverse();
        PathResult::Found {
            node_ids: nodes
                .into_iter()
                .map(|node| self.node_id(node).clone())
                .collect(),
            edges: path_edges,
        }
    }

    /// Deterministic neighboring node IDs.
    pub fn neighbors(
        &self,
        node_id: impl Into<NodeId>,
        direction: TraversalDirection,
    ) -> Result<Vec<NodeId>, GraphError> {
        let node = self.node_index(node_id)?;
        let mut neighbors = self
            .arcs(node, direction)
            .map(|arc| self.node_id(arc.neighbor).clone())
            .collect::<Vec<_>>();
        neighbors.sort();
        neighbors.dedup();
        Ok(neighbors)
    }

    /// Deterministic successors under the graph's direction semantics.
    pub fn successors(&self, node_id: impl Into<NodeId>) -> Result<Vec<NodeId>, GraphError> {
        self.neighbors(node_id, TraversalDirection::Out)
    }

    /// Deterministic predecessors under the graph's direction semantics.
    pub fn predecessors(&self, node_id: impl Into<NodeId>) -> Result<Vec<NodeId>, GraphError> {
        self.neighbors(node_id, TraversalDirection::In)
    }

    /// Stable IDs of outgoing edges. Undirected graphs return every incident
    /// edge exactly once.
    pub fn out_edge_ids(&self, node_id: impl Into<NodeId>) -> Result<Vec<EdgeId>, GraphError> {
        self.edge_ids_for(node_id, TraversalDirection::Out)
    }

    /// Stable IDs of incoming edges. Undirected graphs return every incident
    /// edge exactly once.
    pub fn in_edge_ids(&self, node_id: impl Into<NodeId>) -> Result<Vec<EdgeId>, GraphError> {
        self.edge_ids_for(node_id, TraversalDirection::In)
    }

    /// Stable IDs of all incident edges, with self-loops returned once.
    pub fn incident_edge_ids(&self, node_id: impl Into<NodeId>) -> Result<Vec<EdgeId>, GraphError> {
        self.edge_ids_for(node_id, TraversalDirection::Both)
    }

    fn edge_ids_for(
        &self,
        node_id: impl Into<NodeId>,
        direction: TraversalDirection,
    ) -> Result<Vec<EdgeId>, GraphError> {
        let node = self.node_index(node_id)?;
        Ok(self
            .arcs(node, direction)
            .map(|arc| self.edge_at(arc.edge).id.clone())
            .collect())
    }

    /// All stable edge IDs between two nodes under the selected direction.
    pub fn edges_between(
        &self,
        source: impl Into<NodeId>,
        target: impl Into<NodeId>,
        direction: TraversalDirection,
    ) -> Result<Vec<EdgeId>, GraphError> {
        let source = self.node_index(source)?;
        let target = self.node_index(target)?;
        Ok(self
            .arcs(source, direction)
            .filter(|arc| arc.neighbor == target)
            .map(|arc| self.edge_at(arc.edge).id.clone())
            .collect())
    }

    /// Whether at least one edge connects an endpoint pair under the selected
    /// traversal direction.
    pub fn has_edge_between(
        &self,
        source: impl Into<NodeId>,
        target: impl Into<NodeId>,
        direction: TraversalDirection,
    ) -> Result<bool, GraphError> {
        let source = self.node_index(source)?;
        let target = self.node_index(target)?;
        Ok(self
            .arcs(source, direction)
            .any(|arc| arc.neighbor == target))
    }
}

fn edge_label_allowed(edge: &Edge, allowed: &BTreeSet<String>) -> bool {
    allowed.is_empty()
        || edge
            .label
            .as_ref()
            .is_some_and(|label| allowed.contains(label))
}

fn edge_direction(graph: &Graph, current: usize, edge: &Edge) -> EdgeTraversalDirection {
    if graph.node_id(current) == &edge.source {
        EdgeTraversalDirection::Forward
    } else {
        EdgeTraversalDirection::Reverse
    }
}

fn traversed_edge(graph: &Graph, current: usize, edge: &Edge) -> TraversedEdge {
    TraversedEdge {
        edge_id: edge.id.clone(),
        graphify_key: edge.graphify_key.clone(),
        source: edge.source.clone(),
        target: edge.target.clone(),
        traversal_direction: edge_direction(graph, current, edge),
        label: edge.label.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Edge, GraphKind, Node};

    fn graph() -> Graph {
        Graph::new(
            GraphKind::Graph,
            ["a", "b", "c", "hub", "leaf", "leaf2", "leaf3"]
                .into_iter()
                .map(Node::new),
            [
                Edge::new("ab", "a", "b").with_label("allowed"),
                Edge::new("bc", "b", "c").with_label("allowed"),
                Edge::new("bh", "b", "hub").with_label("allowed"),
                Edge::new("hl", "hub", "leaf").with_label("allowed"),
                Edge::new("hl2", "hub", "leaf2").with_label("allowed"),
                Edge::new("hl3", "hub", "leaf3").with_label("allowed"),
            ],
        )
        .unwrap()
    }

    #[test]
    fn breadth_first_returns_depth_order_and_discovery_edges() {
        let result = graph()
            .traverse(&TraversalOptions::breadth_first(["a".to_string()], 2))
            .unwrap();
        assert_eq!(
            result
                .visits
                .iter()
                .map(|visit| (visit.node_id.clone(), visit.depth))
                .collect::<Vec<_>>(),
            [
                (NodeId::from("a"), 0),
                (NodeId::from("b"), 1),
                (NodeId::from("c"), 2),
                (NodeId::from("hub"), 2),
            ]
        );
        assert_eq!(result.discovery_edges.len(), 3);
    }

    #[test]
    fn traversal_includes_but_does_not_expand_non_seed_hubs() {
        let mut options = TraversalOptions::breadth_first(["a".to_string()], 4);
        options.hub_policy = HubExpansionPolicy::StopNonSeedAtOrAbove { degree: 4 };
        let result = graph().traverse(&options).unwrap();
        assert!(result.visits.iter().any(|visit| visit.node_id == "hub"));
        assert!(!result.visits.iter().any(|visit| visit.node_id == "leaf"));
    }

    #[test]
    fn shortest_path_distinguishes_result_states_and_returns_edges() {
        let graph = graph();
        assert_eq!(
            graph.shortest_path(
                "missing",
                "a",
                TraversalDirection::Both,
                &BTreeSet::new(),
                None
            ),
            PathResult::MissingSource
        );
        let PathResult::Found { node_ids, edges } =
            graph.shortest_path("a", "c", TraversalDirection::Both, &BTreeSet::new(), None)
        else {
            panic!("path should exist")
        };
        assert_eq!(node_ids, ["a", "b", "c"]);
        assert_eq!(edges.len(), 2);
    }

    #[test]
    fn degree_counts_parallel_edges_and_self_loops() {
        let graph = Graph::new(
            GraphKind::MultiGraph,
            [Node::new("a"), Node::new("b")],
            [
                Edge::new("aa", "a", "a"),
                Edge::new("ab1", "a", "b"),
                Edge::new("ab2", "a", "b"),
            ],
        )
        .unwrap();
        assert_eq!(graph.degree("a", DegreeKind::Total).unwrap().degree, 4);
        assert_eq!(graph.degree("b", DegreeKind::Total).unwrap().degree, 2);
    }

    #[test]
    fn depth_first_marks_nodes_when_scheduled_and_uses_stable_edges() {
        let graph = Graph::new(
            GraphKind::MultiDiGraph,
            [
                Node::new("a"),
                Node::new("b"),
                Node::new("c"),
                Node::new("d"),
            ],
            [
                Edge::new("ab-first", "a", "b"),
                Edge::new("ab-second", "a", "b"),
                Edge::new("ac", "a", "c"),
                Edge::new("bd", "b", "d"),
                Edge::new("cd", "c", "d"),
            ],
        )
        .unwrap();
        let mut options = TraversalOptions::depth_first(["a".to_string()], 3);
        options.direction = TraversalDirection::Out;
        let result = graph.traverse(&options).unwrap();
        assert_eq!(
            result
                .visits
                .iter()
                .map(|visit| (visit.node_id.clone(), visit.depth))
                .collect::<Vec<_>>(),
            [
                (NodeId::from("a"), 0),
                (NodeId::from("b"), 1),
                (NodeId::from("d"), 2),
                (NodeId::from("c"), 1),
            ]
        );
        assert_eq!(
            result
                .discovery_edges
                .iter()
                .map(|edge| edge.edge_id.stored_id())
                .collect::<Vec<_>>(),
            ["ab-first", "bd", "ac"]
        );
    }

    #[test]
    fn graph_object_accessors_cover_direction_edges_and_pair_existence() {
        let directed = Graph::new(
            GraphKind::MultiDiGraph,
            ["a", "b", "c"].into_iter().map(Node::new),
            [
                Edge::new("ab1", "a", "b"),
                Edge::new("ab2", "a", "b"),
                Edge::new("ca", "c", "a"),
                Edge::new("aa", "a", "a"),
            ],
        )
        .unwrap();

        assert_eq!(
            directed.neighbors("a", TraversalDirection::Out).unwrap(),
            ["a", "b"]
        );
        assert_eq!(directed.successors("a").unwrap(), ["a", "b"]);
        assert_eq!(directed.predecessors("a").unwrap(), ["a", "c"]);
        assert_eq!(
            directed.out_edge_ids("a").unwrap(),
            ["aa", "ab1", "ab2"].map(crate::EdgeId::from)
        );
        assert_eq!(
            directed.in_edge_ids("a").unwrap(),
            ["aa", "ca"].map(crate::EdgeId::from)
        );
        assert_eq!(
            directed.incident_edge_ids("a").unwrap(),
            ["aa", "ab1", "ab2", "ca"].map(crate::EdgeId::from)
        );
        assert_eq!(
            directed
                .edges_between("a", "b", TraversalDirection::Out)
                .unwrap(),
            ["ab1", "ab2"].map(crate::EdgeId::from)
        );
        assert!(directed
            .has_edge_between("b", "a", TraversalDirection::In)
            .unwrap());
        assert!(!directed
            .has_edge_between("b", "c", TraversalDirection::Both)
            .unwrap());
        assert!(matches!(
            directed.neighbors("missing", TraversalDirection::Both),
            Err(GraphError::UnknownNode(_))
        ));
    }

    #[test]
    fn directed_degrees_filters_bounds_and_invalid_traversals_are_explicit() {
        let directed = Graph::new(
            GraphKind::DiGraph,
            ["a", "b", "c"].into_iter().map(Node::new),
            [
                Edge::new("ab", "a", "b")
                    .with_label("allowed")
                    .with_weight(2.0),
                Edge::new("bc", "b", "c").with_label("blocked"),
                Edge::new("aa", "a", "a").with_label("allowed"),
            ],
        )
        .unwrap();

        assert_eq!(directed.degree("a", DegreeKind::In).unwrap().degree, 1);
        assert_eq!(
            directed
                .degree("a", DegreeKind::Out)
                .unwrap()
                .weighted_degree,
            3.0
        );
        assert_eq!(directed.degrees(DegreeKind::Total).len(), 3);
        assert!(matches!(
            directed.degree("missing", DegreeKind::Total),
            Err(GraphError::UnknownNode(_))
        ));
        assert!(matches!(
            directed.traverse(&TraversalOptions::breadth_first(Vec::<NodeId>::new(), 1)),
            Err(GraphError::InvalidOption(_))
        ));
        assert!(matches!(
            directed.traverse(&TraversalOptions::breadth_first(["missing".to_string()], 1)),
            Err(GraphError::UnknownNode(_))
        ));

        let allowed = BTreeSet::from(["allowed".to_string()]);
        let filtered = directed
            .traverse(&TraversalOptions {
                direction: TraversalDirection::Out,
                allowed_labels: allowed.clone(),
                ..TraversalOptions::breadth_first(["a"], 3)
            })
            .unwrap();
        assert_eq!(
            filtered
                .visits
                .iter()
                .map(|visit| visit.node_id.clone())
                .collect::<Vec<_>>(),
            ["a", "b"].map(NodeId::from)
        );
        assert!(filtered
            .discovery_edges
            .iter()
            .all(|edge| edge.label.as_deref() == Some("allowed")));
        assert!(matches!(
            directed.shortest_path("a", "c", TraversalDirection::Out, &allowed, None),
            PathResult::NoPath
        ));
        assert!(matches!(
            directed.shortest_path("a", "c", TraversalDirection::Out, &BTreeSet::new(), Some(1)),
            PathResult::NoPath
        ));
        assert!(matches!(
            directed.shortest_path("a", "a", TraversalDirection::Out, &BTreeSet::new(), None),
            PathResult::Found { node_ids, edges } if node_ids == ["a"] && edges.is_empty()
        ));
        assert!(matches!(
            directed.shortest_path(
                "a",
                "missing",
                TraversalDirection::Out,
                &BTreeSet::new(),
                None
            ),
            PathResult::MissingTarget
        ));
    }
}
