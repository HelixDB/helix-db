use std::collections::{BTreeMap, BTreeSet};

use crate::{Attributes, Edge, Graph, GraphError, GraphKind, Node, NodeId};

impl Graph {
    /// Cheaply clone the immutable, reference-counted graph allocation.
    pub fn copy(&self) -> Self {
        self.clone()
    }

    /// Materialize directed traversal semantics without losing undirected
    /// edges or graph metadata.
    pub fn to_directed(&self) -> Result<Self, GraphError> {
        if self.is_directed() {
            return Ok(self.clone());
        }
        let kind = match self.kind() {
            GraphKind::Graph => GraphKind::DiGraph,
            GraphKind::MultiGraph => GraphKind::MultiDiGraph,
            GraphKind::DiGraph | GraphKind::MultiDiGraph => unreachable!("checked above"),
        };
        let mut reverse_generations =
            self.edges()
                .iter()
                .fold(BTreeMap::<String, u64>::new(), |mut generations, edge| {
                    generations
                        .entry(edge.id.stored_id().to_string())
                        .and_modify(|generation| {
                            *generation = (*generation).max(edge.id.reverse_generation());
                        })
                        .or_insert(edge.id.reverse_generation());
                    generations
                });
        let mut edges = Vec::with_capacity(self.edge_count().saturating_mul(2));
        for edge in self.edges() {
            edges.push(edge.clone());
            if edge.source != edge.target {
                let generation = reverse_generations
                    .get_mut(edge.id.stored_id())
                    .expect("every edge stored ID was indexed");
                *generation =
                    generation
                        .checked_add(1)
                        .ok_or_else(|| GraphError::EdgeIdentityExhausted {
                            stored_id: edge.id.stored_id().to_string(),
                        })?;
                let reverse_id =
                    crate::EdgeId::synthesized_reverse(edge.id.stored_id(), *generation)
                        .expect("incremented reverse generation is non-zero");
                edges.push(Edge {
                    id: reverse_id,
                    source: edge.target.clone(),
                    target: edge.source.clone(),
                    ..edge.clone()
                });
            }
        }
        Graph::with_attributes(
            kind,
            self.attributes().clone(),
            self.nodes().iter().cloned(),
            edges,
        )
    }

    /// Return an immutable graph with undirected traversal semantics while
    /// preserving every original edge record and graph attribute.
    pub fn to_undirected(&self) -> Result<Self, GraphError> {
        if !self.is_directed() {
            return Ok(self.clone());
        }
        let kind = match self.kind() {
            GraphKind::MultiDiGraph => GraphKind::MultiGraph,
            GraphKind::DiGraph => {
                let mut endpoint_pairs = BTreeSet::new();
                let has_parallel_pair = self.edges().iter().any(|edge| {
                    let pair = if edge.source <= edge.target {
                        (edge.source.clone(), edge.target.clone())
                    } else {
                        (edge.target.clone(), edge.source.clone())
                    };
                    !endpoint_pairs.insert(pair)
                });
                if has_parallel_pair {
                    GraphKind::MultiGraph
                } else {
                    GraphKind::Graph
                }
            }
            GraphKind::Graph | GraphKind::MultiGraph => unreachable!("checked above"),
        };
        Graph::with_attributes(
            kind,
            self.attributes().clone(),
            self.nodes().iter().cloned(),
            self.edges().iter().cloned(),
        )
    }

    /// Materialize an induced subgraph containing exactly the requested nodes
    /// and edges whose endpoints are both selected.
    pub fn induced_subgraph(
        &self,
        node_ids: impl IntoIterator<Item = impl Into<NodeId>>,
    ) -> Result<Self, GraphError> {
        let selected = node_ids
            .into_iter()
            .map(Into::into)
            .collect::<BTreeSet<_>>();
        for node_id in &selected {
            if !self.contains_node(node_id) {
                return Err(GraphError::UnknownNode(node_id.clone()));
            }
        }
        Graph::with_attributes(
            self.kind(),
            self.attributes().clone(),
            self.nodes()
                .iter()
                .filter(|node| selected.contains(&node.id))
                .cloned(),
            self.edges()
                .iter()
                .filter(|edge| selected.contains(&edge.source) && selected.contains(&edge.target))
                .cloned(),
        )
    }

    /// Return a graph with external node IDs replaced and every endpoint
    /// rewired. Distinct nodes may not merge implicitly.
    pub fn relabel(&self, mapping: &BTreeMap<NodeId, NodeId>) -> Result<Self, GraphError> {
        let mut targets = BTreeMap::<NodeId, NodeId>::new();
        for node in self.nodes() {
            let target = mapping
                .get(&node.id)
                .cloned()
                .unwrap_or_else(|| node.id.clone());
            if let Some(first) = targets.insert(target.clone(), node.id.clone()) {
                return Err(GraphError::RelabelCollision {
                    target,
                    first,
                    second: node.id.clone(),
                });
            }
        }
        let relabel = |node_id: &NodeId| {
            mapping
                .get(node_id)
                .cloned()
                .unwrap_or_else(|| node_id.clone())
        };
        Graph::with_attributes(
            self.kind(),
            self.attributes().clone(),
            self.nodes().iter().map(|node| Node {
                id: relabel(&node.id),
                label: node.label.clone(),
                attributes: node.attributes.clone(),
            }),
            self.edges().iter().map(|edge| Edge {
                source: relabel(&edge.source),
                target: relabel(&edge.target),
                ..edge.clone()
            }),
        )
    }

    /// Compose two immutable graphs. Right-hand attributes take precedence.
    pub fn compose(&self, right: &Self) -> Result<Self, GraphError> {
        if self.kind() != right.kind() {
            return Err(GraphError::KindMismatch);
        }
        let mut graph_attributes = self.attributes().clone();
        graph_attributes.extend(right.attributes().clone());

        let mut nodes = self
            .nodes()
            .iter()
            .cloned()
            .map(|node| (node.id.clone(), node))
            .collect::<BTreeMap<_, _>>();
        for right_node in right.nodes() {
            match nodes.get_mut(&right_node.id) {
                Some(left_node) => {
                    if right_node.label.is_some() {
                        left_node.label.clone_from(&right_node.label);
                    }
                    left_node.attributes.extend(right_node.attributes.clone());
                }
                None => {
                    nodes.insert(right_node.id.clone(), right_node.clone());
                }
            }
        }

        let mut edges = self
            .edges()
            .iter()
            .cloned()
            .map(|edge| (edge.id.clone(), edge))
            .collect::<BTreeMap<_, _>>();
        for right_edge in right.edges() {
            match edges.get_mut(&right_edge.id) {
                Some(left_edge)
                    if left_edge.source != right_edge.source
                        || left_edge.target != right_edge.target =>
                {
                    return Err(GraphError::ConflictingEdge {
                        edge_id: right_edge.id.clone(),
                    });
                }
                Some(left_edge) => {
                    if right_edge.graphify_key.is_some() {
                        left_edge.graphify_key.clone_from(&right_edge.graphify_key);
                    }
                    if right_edge.label.is_some() {
                        left_edge.label.clone_from(&right_edge.label);
                    }
                    if right_edge.weight.is_some() {
                        left_edge.weight = right_edge.weight;
                    }
                    left_edge.attributes.extend(right_edge.attributes.clone());
                }
                None => {
                    edges.insert(right_edge.id.clone(), right_edge.clone());
                }
            }
        }
        Graph::with_attributes(
            self.kind(),
            graph_attributes,
            nodes.into_values(),
            edges.into_values(),
        )
    }

    /// Create an owned export DTO with independently mutable attributes.
    pub fn export_parts(&self) -> (Attributes, Vec<Node>, Vec<Edge>) {
        (
            self.attributes().clone(),
            self.nodes().to_vec(),
            self.edges().to_vec(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn graph() -> Graph {
        Graph::with_attributes(
            GraphKind::DiGraph,
            Attributes::from([("owner".to_string(), json!("left"))]),
            [Node::new("a"), Node::new("b"), Node::new("c")],
            [Edge::new("ab", "a", "b"), Edge::new("bc", "b", "c")],
        )
        .unwrap()
    }

    #[test]
    fn induced_subgraph_keeps_only_internal_edges() {
        let subgraph = graph()
            .induced_subgraph(["a".to_string(), "b".to_string()])
            .unwrap();
        assert_eq!(subgraph.node_count(), 2);
        assert_eq!(subgraph.edge_count(), 1);
        assert!(subgraph.contains_edge("ab"));
    }

    #[test]
    fn directed_conversion_duplicates_non_loops_structurally_and_preserves_attributes() {
        let graph = Graph::with_attributes(
            GraphKind::MultiGraph,
            Attributes::from([("owner".to_string(), json!("graphify"))]),
            [Node::new("a"), Node::new("b")],
            [
                Edge::new("edge", "a", "b")
                    .with_graphify_key("user-key")
                    .with_label("REL")
                    .with_weight(2.0)
                    .with_attributes(Attributes::from([("generation".to_string(), json!(3))])),
                Edge::new("loop", "a", "a"),
            ],
        )
        .unwrap();

        let directed = graph.to_directed().unwrap();
        assert_eq!(directed.kind(), GraphKind::MultiDiGraph);
        assert_eq!(directed.edge_count(), 3);
        assert_eq!(directed.attributes(), graph.attributes());
        let reverse_id = crate::EdgeId::original("edge").reversed().unwrap();
        let reverse = directed.edge(reverse_id.clone()).unwrap();
        assert_eq!(reverse.source, NodeId::from("b"));
        assert_eq!(reverse.target, NodeId::from("a"));
        assert_eq!(reverse.graphify_key, Some("user-key".into()));
        assert_eq!(reverse.attributes["generation"], json!(3));
        assert!(!directed.contains_edge(crate::EdgeId::original("reverse(edge)")));
        assert!(directed.contains_edge(reverse_id));
        assert_eq!(directed.edge("loop").unwrap().source, "a");

        let repeated = directed.to_undirected().unwrap().to_directed().unwrap();
        assert_eq!(repeated.edge_count(), 5);
        assert!(repeated.contains_edge(
            crate::EdgeId::original("edge")
                .reversed()
                .unwrap()
                .reversed()
                .unwrap()
        ));
    }

    #[test]
    fn conversions_are_idempotent_and_reject_exhausted_reverse_identity() {
        let directed = graph();
        assert_eq!(directed.to_directed().unwrap(), directed);
        let undirected = directed.to_undirected().unwrap();
        assert_eq!(undirected.to_undirected().unwrap(), undirected);

        let exhausted = Graph::new(
            GraphKind::Graph,
            [Node::new("a"), Node::new("b")],
            [Edge {
                id: crate::EdgeId::synthesized_reverse("edge", u64::MAX).unwrap(),
                graphify_key: None,
                source: NodeId::from("a"),
                target: NodeId::from("b"),
                label: None,
                weight: None,
                attributes: Attributes::new(),
            }],
        )
        .unwrap();
        assert_eq!(
            exhausted.to_directed(),
            Err(GraphError::EdgeIdentityExhausted {
                stored_id: "edge".to_string(),
            })
        );
    }

    #[test]
    fn undirected_conversion_promotes_only_lossy_digraphs_to_multigraphs() {
        let simple = graph().to_undirected().unwrap();
        assert_eq!(simple.kind(), GraphKind::Graph);
        assert_eq!(simple.attributes()["owner"], json!("left"));

        let reciprocal = Graph::new(
            GraphKind::DiGraph,
            [Node::new("a"), Node::new("b")],
            [Edge::new("ab", "a", "b"), Edge::new("ba", "b", "a")],
        )
        .unwrap()
        .to_undirected()
        .unwrap();
        assert_eq!(reciprocal.kind(), GraphKind::MultiGraph);
        assert_eq!(reciprocal.edge_count(), 2);

        let multi = Graph::new(
            GraphKind::MultiDiGraph,
            [Node::new("a"), Node::new("b")],
            [Edge::new("ab", "a", "b")],
        )
        .unwrap()
        .to_undirected()
        .unwrap();
        assert_eq!(multi.kind(), GraphKind::MultiGraph);
    }

    #[test]
    fn relabel_rewires_edges_and_rejects_collisions() {
        let graph = graph();
        let relabeled = graph
            .relabel(&BTreeMap::from([(NodeId::from("a"), NodeId::from("z"))]))
            .unwrap();
        assert_eq!(relabeled.edge("ab").unwrap().source, "z");
        assert!(matches!(
            graph.relabel(&BTreeMap::from([(NodeId::from("a"), NodeId::from("b"))])),
            Err(GraphError::RelabelCollision { .. })
        ));
    }

    #[test]
    fn compose_applies_right_attribute_precedence() {
        let left = graph();
        let right = Graph::with_attributes(
            GraphKind::DiGraph,
            Attributes::from([("owner".to_string(), json!("right"))]),
            [Node::new("a")
                .with_attributes(Attributes::from([("name".to_string(), json!("Ada"))]))],
            [],
        )
        .unwrap();
        let composed = left.compose(&right).unwrap();
        assert_eq!(composed.attributes()["owner"], json!("right"));
        assert_eq!(composed.node("a").unwrap().attributes["name"], json!("Ada"));

        let right = Graph::new(
            GraphKind::DiGraph,
            [
                Node::new("a"),
                Node::new("b"),
                Node::new("c"),
                Node::new("d").with_label("File"),
            ],
            [
                Edge::new("ab", "a", "b")
                    .with_graphify_key("right")
                    .with_label("REL")
                    .with_weight(2.0),
                Edge::new("cd", "c", "d"),
            ],
        )
        .unwrap();
        let composed = left.compose(&right).unwrap();
        assert_eq!(composed.node("d").unwrap().label.as_deref(), Some("File"));
        assert_eq!(composed.edge("ab").unwrap().weight, Some(2.0));
        assert!(composed.contains_edge("cd"));
    }

    #[test]
    fn transformations_cover_copy_empty_unknown_direction_conflicts_and_export() {
        let graph = graph();
        assert_eq!(graph.copy(), graph);
        assert!(!graph.to_undirected().unwrap().is_directed());
        assert_eq!(
            graph
                .induced_subgraph(Vec::<NodeId>::new())
                .unwrap()
                .node_count(),
            0
        );
        assert!(matches!(
            graph.induced_subgraph(["missing".to_string()]),
            Err(GraphError::UnknownNode(_))
        ));

        let undirected = graph.to_undirected().unwrap();
        assert_eq!(graph.compose(&undirected), Err(GraphError::KindMismatch));
        let conflicting = Graph::new(
            GraphKind::DiGraph,
            [Node::new("a"), Node::new("b"), Node::new("c")],
            [Edge::new("ab", "b", "c")],
        )
        .unwrap();
        assert!(matches!(
            graph.compose(&conflicting),
            Err(GraphError::ConflictingEdge { edge_id }) if edge_id == crate::EdgeId::from("ab")
        ));

        let relabeled = graph
            .relabel(&BTreeMap::from([
                (NodeId::from("a"), NodeId::from("b")),
                (NodeId::from("b"), NodeId::from("a")),
            ]))
            .unwrap();
        assert_eq!(relabeled.edge("ab").unwrap().source, "b");
        let (mut attributes, mut nodes, mut edges) = graph.export_parts();
        attributes.insert("owner".to_string(), json!("export"));
        nodes[0].attributes.insert("local".to_string(), json!(true));
        edges[0].attributes.insert("local".to_string(), json!(true));
        assert_eq!(graph.attributes()["owner"], json!("left"));
        assert!(!graph.nodes()[0].attributes.contains_key("local"));
        assert!(!graph.edges()[0].attributes.contains_key("local"));
    }
}
