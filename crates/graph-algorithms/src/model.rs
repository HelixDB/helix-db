use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::ops::Deref;
use std::slice;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::ExternalId;

/// Opaque external node identifier returned to SDK callers.
pub type NodeId = ExternalId;
/// Selected immutable properties attached to a node, edge, or graph.
pub type Attributes = BTreeMap<String, Value>;

/// Collision-free identity for a stored edge or a synthesized reversal.
///
/// Generation zero is the original Helix edge. Positive generations identify
/// synthesized reversals without modifying or reserving user-controlled IDs.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EdgeId {
    stored_id: String,
    reverse_generation: u64,
}

impl EdgeId {
    /// Construct an original stored-edge identity.
    pub fn original(id: impl Into<String>) -> Self {
        Self {
            stored_id: id.into(),
            reverse_generation: 0,
        }
    }

    /// Construct a synthesized-reverse identity at a non-zero generation.
    pub fn synthesized_reverse(id: impl Into<String>, generation: u64) -> Option<Self> {
        (generation > 0).then(|| Self {
            stored_id: id.into(),
            reverse_generation: generation,
        })
    }

    /// Derive the next structural reversal when its generation is representable.
    pub fn reversed(&self) -> Option<Self> {
        self.reverse_generation
            .checked_add(1)
            .and_then(|generation| Self::synthesized_reverse(self.stored_id.clone(), generation))
    }

    /// Return the underlying stored Helix edge identity.
    pub fn stored_id(&self) -> &str {
        &self.stored_id
    }

    /// Zero for stored edges; positive for synthesized reversals.
    pub const fn reverse_generation(&self) -> u64 {
        self.reverse_generation
    }

    fn is_valid(&self) -> bool {
        !self.stored_id.is_empty()
    }
}

impl From<String> for EdgeId {
    fn from(id: String) -> Self {
        Self::original(id)
    }
}

impl From<&str> for EdgeId {
    fn from(id: &str) -> Self {
        Self::original(id)
    }
}

impl fmt::Display for EdgeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.reverse_generation == 0 {
            formatter.write_str(&self.stored_id)
        } else {
            write!(
                formatter,
                "reverse#{}({})",
                self.reverse_generation, self.stored_id
            )
        }
    }
}

/// Finite floating-point value strictly greater than zero.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "f64", into = "f64")]
pub struct PositiveFiniteF64(f64);

impl PositiveFiniteF64 {
    /// Validate a positive finite value.
    pub fn new(value: f64) -> Result<Self, GraphError> {
        if value.is_finite() && value > 0.0 {
            Ok(Self(value))
        } else {
            Err(GraphError::InvalidOption(
                "value must be finite and positive".to_string(),
            ))
        }
    }

    /// Return the validated primitive value.
    pub const fn get(self) -> f64 {
        self.0
    }
}

impl TryFrom<f64> for PositiveFiniteF64 {
    type Error = GraphError;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<PositiveFiniteF64> for f64 {
    fn from(value: PositiveFiniteF64) -> Self {
        value.get()
    }
}

/// Finite floating-point value greater than or equal to zero.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "f64", into = "f64")]
pub struct NonNegativeFiniteF64(f64);

impl NonNegativeFiniteF64 {
    /// Validate a non-negative finite value.
    pub fn new(value: f64) -> Result<Self, GraphError> {
        if value.is_finite() && value >= 0.0 {
            Ok(Self(value))
        } else {
            Err(GraphError::InvalidOption(
                "value must be finite and non-negative".to_string(),
            ))
        }
    }

    /// Return the validated primitive value.
    pub const fn get(self) -> f64 {
        self.0
    }
}

impl TryFrom<f64> for NonNegativeFiniteF64 {
    type Error = GraphError;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<NonNegativeFiniteF64> for f64 {
    fn from(value: NonNegativeFiniteF64) -> Self {
        value.get()
    }
}

/// Declared graph topology contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphKind {
    /// Undirected simple graph.
    Graph,
    /// Directed simple graph.
    DiGraph,
    /// Undirected graph permitting parallel endpoint pairs.
    MultiGraph,
    /// Directed graph permitting parallel endpoint pairs.
    MultiDiGraph,
}

impl GraphKind {
    /// Whether algorithms observe stored edge direction.
    pub const fn is_directed(self) -> bool {
        matches!(self, Self::DiGraph | Self::MultiDiGraph)
    }

    /// Whether parallel endpoint pairs are permitted.
    pub const fn is_multigraph(self) -> bool {
        matches!(self, Self::MultiGraph | Self::MultiDiGraph)
    }
}

/// Immutable node input and public node record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node {
    /// Opaque external identity.
    pub id: NodeId,
    /// Optional Helix node label.
    pub label: Option<String>,
    /// Selected immutable properties.
    pub attributes: Attributes,
}

impl Node {
    /// Construct a node with no label or selected properties.
    pub fn new(id: impl Into<NodeId>) -> Self {
        Self {
            id: id.into(),
            label: None,
            attributes: Attributes::new(),
        }
    }

    /// Attach a label.
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Attach selected properties.
    pub fn with_attributes(mut self, attributes: Attributes) -> Self {
        self.attributes = attributes;
        self
    }
}

/// Immutable edge input and public edge record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Edge {
    /// Stable unique edge identity.
    pub id: EdgeId,
    /// Optional Graphify multigraph key.
    pub graphify_key: Option<ExternalId>,
    /// Stored source node external ID.
    pub source: NodeId,
    /// Stored target node external ID.
    pub target: NodeId,
    /// Optional Helix edge label.
    pub label: Option<String>,
    /// Optional validated non-negative finite weight.
    pub weight: Option<f64>,
    /// Selected immutable properties.
    pub attributes: Attributes,
}

impl Edge {
    /// Construct an unweighted edge without a label or selected properties.
    pub fn new(
        id: impl Into<EdgeId>,
        source: impl Into<NodeId>,
        target: impl Into<NodeId>,
    ) -> Self {
        Self {
            id: id.into(),
            graphify_key: None,
            source: source.into(),
            target: target.into(),
            label: None,
            weight: None,
            attributes: Attributes::new(),
        }
    }

    /// Attach a Graphify multigraph key.
    pub fn with_graphify_key(mut self, key: impl Into<ExternalId>) -> Self {
        self.graphify_key = Some(key.into());
        self
    }

    /// Attach a label.
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Attach an algorithm weight.
    pub fn with_weight(mut self, weight: f64) -> Self {
        self.weight = Some(weight);
        self
    }

    /// Attach selected properties.
    pub fn with_attributes(mut self, attributes: Attributes) -> Self {
        self.attributes = attributes;
        self
    }
}

/// Graph construction or lookup failure.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum GraphError {
    /// An edge identifier is empty.
    #[error("edge ID must not be empty")]
    EmptyEdgeId,
    /// The input contains the same node identity more than once.
    #[error("duplicate node ID: {0}")]
    DuplicateNode(NodeId),
    /// The input contains the same edge identity more than once.
    #[error("duplicate edge ID: {0}")]
    DuplicateEdge(EdgeId),
    /// An edge endpoint is absent from the selected node set.
    #[error("edge {edge_id} references missing {endpoint} node {node_id}")]
    MissingEndpoint {
        /// Edge containing the invalid endpoint.
        edge_id: EdgeId,
        /// `source` or `target`.
        endpoint: &'static str,
        /// Missing node identity.
        node_id: NodeId,
    },
    /// The graph contains an invalid weight.
    #[error("edge {edge_id} weight must be finite and non-negative, got {weight}")]
    InvalidWeight {
        /// Edge containing the invalid weight.
        edge_id: EdgeId,
        /// Rejected weight.
        weight: f64,
    },
    /// An external identity is malformed or exceeds its resource bounds.
    #[error("invalid external identity: {0}")]
    InvalidExternalId(String),
    /// A requested node does not exist.
    #[error("unknown node ID: {0}")]
    UnknownNode(NodeId),
    /// A requested edge does not exist.
    #[error("unknown edge ID: {0}")]
    UnknownEdge(EdgeId),
    /// A relabel operation would merge distinct nodes.
    #[error("relabel target {target} is produced by both {first} and {second}")]
    RelabelCollision {
        /// Colliding output identity.
        target: NodeId,
        /// First source identity.
        first: NodeId,
        /// Second source identity.
        second: NodeId,
    },
    /// A simple graph contains parallel endpoint pairs.
    #[error("{kind:?} does not permit parallel edges between {pair_source} and {pair_target}")]
    ParallelEdge {
        /// Declared simple graph kind.
        kind: GraphKind,
        /// Canonical pair source.
        pair_source: NodeId,
        /// Canonical pair target.
        pair_target: NodeId,
    },
    /// Compose requires exactly equal graph kinds.
    #[error("cannot compose graphs with different kinds")]
    KindMismatch,
    /// Two graphs disagree about one stable edge identity.
    #[error("edge {edge_id} has conflicting endpoints across composed graphs")]
    ConflictingEdge {
        /// Conflicting stable edge identity.
        edge_id: EdgeId,
    },
    /// No further structural reverse generation can be represented.
    #[error("edge {stored_id} exhausted synthesized reverse generations")]
    EdgeIdentityExhausted {
        /// Underlying stored Helix edge identity.
        stored_id: String,
    },
    /// An algorithm option violates its typed runtime contract.
    #[error("invalid algorithm option: {0}")]
    InvalidOption(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ArcRef {
    pub(crate) neighbor: usize,
    pub(crate) edge: usize,
}

#[derive(Debug, Clone, PartialEq)]
struct Csr {
    offsets: Vec<usize>,
    arcs: Vec<ArcRef>,
}

impl Csr {
    fn from_rows(rows: Vec<Vec<ArcRef>>) -> Self {
        let mut offsets = Vec::with_capacity(rows.len() + 1);
        let mut arcs = Vec::with_capacity(rows.iter().map(Vec::len).sum());
        offsets.push(0);
        for row in rows {
            arcs.extend(row);
            offsets.push(arcs.len());
        }
        Self { offsets, arcs }
    }

    fn row(&self, node: usize) -> &[ArcRef] {
        &self.arcs[self.offsets[node]..self.offsets[node + 1]]
    }
}

/// Validated immutable graph used by every native algorithm.
#[derive(Debug, Clone, PartialEq)]
pub struct Graph {
    inner: Arc<GraphInner>,
}

/// Shared graph allocation. Its fields remain private; this type is public
/// only so [`Graph`]'s read-only dereference implementation can share storage.
#[doc(hidden)]
#[derive(Debug, PartialEq)]
pub struct GraphInner {
    kind: GraphKind,
    graph_attributes: Attributes,
    nodes: Vec<Node>,
    edges: Vec<Edge>,
    node_indexes: BTreeMap<NodeId, usize>,
    edge_indexes: BTreeMap<EdgeId, usize>,
    outgoing: Csr,
    incoming: Csr,
}

impl Deref for Graph {
    type Target = GraphInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Graph {
    /// Validate and construct a graph.
    pub fn new(
        kind: GraphKind,
        nodes: impl IntoIterator<Item = Node>,
        edges: impl IntoIterator<Item = Edge>,
    ) -> Result<Self, GraphError> {
        Self::with_attributes(kind, Attributes::new(), nodes, edges)
    }

    /// Validate and construct a graph with graph-level metadata.
    pub fn with_attributes(
        kind: GraphKind,
        graph_attributes: Attributes,
        nodes: impl IntoIterator<Item = Node>,
        edges: impl IntoIterator<Item = Edge>,
    ) -> Result<Self, GraphError> {
        let mut node_map = BTreeMap::new();
        for node in nodes {
            node.id.validate()?;
            let node_id = node.id.clone();
            if node_map.insert(node_id.clone(), node).is_some() {
                return Err(GraphError::DuplicateNode(node_id));
            }
        }
        let nodes = node_map.into_values().collect::<Vec<_>>();
        let node_indexes = nodes
            .iter()
            .enumerate()
            .map(|(index, node)| (node.id.clone(), index))
            .collect::<BTreeMap<_, _>>();

        let mut edge_map = BTreeMap::new();
        for edge in edges {
            if !edge.id.is_valid() {
                return Err(GraphError::EmptyEdgeId);
            }
            if !node_indexes.contains_key(&edge.source) {
                return Err(GraphError::MissingEndpoint {
                    edge_id: edge.id,
                    endpoint: "source",
                    node_id: edge.source,
                });
            }
            if !node_indexes.contains_key(&edge.target) {
                return Err(GraphError::MissingEndpoint {
                    edge_id: edge.id,
                    endpoint: "target",
                    node_id: edge.target,
                });
            }
            if let Some(weight) = edge.weight
                && (!weight.is_finite() || weight < 0.0)
            {
                return Err(GraphError::InvalidWeight {
                    edge_id: edge.id,
                    weight,
                });
            }
            let edge_id = edge.id.clone();
            if edge_map.insert(edge_id.clone(), edge).is_some() {
                return Err(GraphError::DuplicateEdge(edge_id));
            }
        }
        let edges = edge_map.into_values().collect::<Vec<_>>();
        if !kind.is_multigraph() {
            let mut endpoint_pairs = BTreeSet::new();
            for edge in &edges {
                let pair = if kind.is_directed() || edge.source <= edge.target {
                    (edge.source.clone(), edge.target.clone())
                } else {
                    (edge.target.clone(), edge.source.clone())
                };
                if !endpoint_pairs.insert(pair.clone()) {
                    return Err(GraphError::ParallelEdge {
                        kind,
                        pair_source: pair.0,
                        pair_target: pair.1,
                    });
                }
            }
        }
        let edge_indexes = edges
            .iter()
            .enumerate()
            .map(|(index, edge)| (edge.id.clone(), index))
            .collect::<BTreeMap<_, _>>();

        let mut outgoing_rows = vec![Vec::new(); nodes.len()];
        let mut incoming_rows = vec![Vec::new(); nodes.len()];
        for (edge_index, edge) in edges.iter().enumerate() {
            let source = node_indexes[&edge.source];
            let target = node_indexes[&edge.target];
            outgoing_rows[source].push(ArcRef {
                neighbor: target,
                edge: edge_index,
            });
            incoming_rows[target].push(ArcRef {
                neighbor: source,
                edge: edge_index,
            });
        }
        for row in outgoing_rows.iter_mut().chain(incoming_rows.iter_mut()) {
            row.sort_by(|left, right| compare_arcs(&nodes, &edges, left, right));
        }

        Ok(Self {
            inner: Arc::new(GraphInner {
                kind,
                graph_attributes,
                nodes,
                edges,
                node_indexes,
                edge_indexes,
                outgoing: Csr::from_rows(outgoing_rows),
                incoming: Csr::from_rows(incoming_rows),
            }),
        })
    }

    /// Declared graph topology contract.
    pub fn kind(&self) -> GraphKind {
        self.kind
    }

    /// Whether the graph is directed.
    pub fn is_directed(&self) -> bool {
        self.kind.is_directed()
    }

    /// Whether the graph declares support for parallel endpoint-pair edges.
    pub fn is_multigraph(&self) -> bool {
        self.kind.is_multigraph()
    }

    /// Graph-level immutable attributes.
    pub fn attributes(&self) -> &Attributes {
        &self.graph_attributes
    }

    /// Number of nodes.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Number of stored edges. Undirected adjacency does not duplicate this
    /// public edge count.
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// Nodes in deterministic external-ID order.
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    /// Edges in deterministic stable-ID order.
    pub fn edges(&self) -> &[Edge] {
        &self.edges
    }

    /// Look up a node.
    pub fn node(&self, id: impl Into<NodeId>) -> Option<&Node> {
        let id = id.into();
        self.node_indexes.get(&id).map(|index| &self.nodes[*index])
    }

    /// Look up an edge.
    pub fn edge(&self, id: impl Into<EdgeId>) -> Option<&Edge> {
        self.edge_indexes
            .get(&id.into())
            .map(|index| &self.edges[*index])
    }

    /// Whether the graph contains a node.
    pub fn contains_node(&self, id: impl Into<NodeId>) -> bool {
        self.node_indexes.contains_key(&id.into())
    }

    /// Whether the graph contains an edge.
    pub fn contains_edge(&self, id: impl Into<EdgeId>) -> bool {
        self.edge_indexes.contains_key(&id.into())
    }

    pub(crate) fn node_index(&self, id: impl Into<NodeId>) -> Result<usize, GraphError> {
        let id = id.into();
        self.node_indexes
            .get(&id)
            .copied()
            .ok_or(GraphError::UnknownNode(id))
    }

    pub(crate) fn node_id(&self, index: usize) -> &NodeId {
        &self.nodes[index].id
    }

    pub(crate) fn edge_at(&self, index: usize) -> &Edge {
        &self.edges[index]
    }

    pub(crate) fn outgoing(&self, node: usize) -> &[ArcRef] {
        self.outgoing.row(node)
    }

    pub(crate) fn incoming(&self, node: usize) -> &[ArcRef] {
        self.incoming.row(node)
    }

    pub(crate) fn arcs(&self, node: usize, direction: super::TraversalDirection) -> ArcIter<'_> {
        let direction = if self.kind.is_directed() {
            direction
        } else {
            super::TraversalDirection::Both
        };
        match direction {
            super::TraversalDirection::Out => ArcIter::One(self.outgoing(node).iter()),
            super::TraversalDirection::In => ArcIter::One(self.incoming(node).iter()),
            super::TraversalDirection::Both => ArcIter::Both {
                graph: self,
                node,
                outgoing: self.outgoing(node),
                incoming: self.incoming(node),
                outgoing_index: 0,
                incoming_index: 0,
            },
        }
    }
}

fn compare_arcs(nodes: &[Node], edges: &[Edge], left: &ArcRef, right: &ArcRef) -> Ordering {
    nodes[left.neighbor]
        .id
        .cmp(&nodes[right.neighbor].id)
        .then_with(|| {
            edges[left.edge]
                .graphify_key
                .cmp(&edges[right.edge].graphify_key)
        })
        .then_with(|| edges[left.edge].id.cmp(&edges[right.edge].id))
}

pub(crate) enum ArcIter<'a> {
    One(slice::Iter<'a, ArcRef>),
    Both {
        graph: &'a Graph,
        node: usize,
        outgoing: &'a [ArcRef],
        incoming: &'a [ArcRef],
        outgoing_index: usize,
        incoming_index: usize,
    },
}

impl Iterator for ArcIter<'_> {
    type Item = ArcRef;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::One(arcs) => arcs.next().copied(),
            Self::Both {
                graph,
                node,
                outgoing,
                incoming,
                outgoing_index,
                incoming_index,
            } => {
                while incoming
                    .get(*incoming_index)
                    .is_some_and(|arc| arc.neighbor == *node)
                {
                    *incoming_index += 1;
                }
                match (outgoing.get(*outgoing_index), incoming.get(*incoming_index)) {
                    (Some(left), Some(right))
                        if compare_arcs(&graph.nodes, &graph.edges, left, right)
                            != Ordering::Greater =>
                    {
                        *outgoing_index += 1;
                        Some(*left)
                    }
                    (Some(_), Some(right)) => {
                        *incoming_index += 1;
                        Some(*right)
                    }
                    (Some(left), None) => {
                        *outgoing_index += 1;
                        Some(*left)
                    }
                    (None, Some(right)) => {
                        *incoming_index += 1;
                        Some(*right)
                    }
                    (None, None) => None,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TraversalDirection;

    #[test]
    fn construction_sorts_records_and_builds_directional_adjacency() {
        let graph = Graph::new(
            GraphKind::DiGraph,
            [Node::new("b"), Node::new("a")],
            [Edge::new("edge", "a", "b")],
        )
        .unwrap();

        assert_eq!(
            graph
                .nodes()
                .iter()
                .map(|node| node.id.clone())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert_eq!(graph.outgoing(graph.node_index("a").unwrap()).len(), 1);
        assert_eq!(graph.incoming(graph.node_index("b").unwrap()).len(), 1);
    }

    #[test]
    fn construction_rejects_invalid_identity_endpoint_and_weight_states() {
        assert!(Graph::new(GraphKind::DiGraph, [Node::new("")], []).is_ok());
        assert!(matches!(
            Graph::new(
                GraphKind::DiGraph,
                [Node::new("a"), Node::new("a")],
                []
            ),
            Err(GraphError::DuplicateNode(id)) if id == "a"
        ));
        assert!(matches!(
            Graph::new(
                GraphKind::DiGraph,
                [Node::new("a")],
                [Edge::new("ab", "a", "b")]
            ),
            Err(GraphError::MissingEndpoint { endpoint: "target", node_id, .. }) if node_id == "b"
        ));
        assert!(matches!(
            Graph::new(
                GraphKind::DiGraph,
                [Node::new("a")],
                [Edge::new("aa", "a", "a").with_weight(f64::NAN)]
            ),
            Err(GraphError::InvalidWeight { .. })
        ));
        assert_eq!(
            Graph::new(
                GraphKind::DiGraph,
                [Node::new("a")],
                [Edge::new("", "a", "a")]
            )
            .unwrap_err(),
            GraphError::EmptyEdgeId
        );
        assert!(matches!(
            Graph::new(
                GraphKind::DiGraph,
                [Node::new("a")],
                [Edge::new("aa", "missing", "a")]
            ),
            Err(GraphError::MissingEndpoint {
                endpoint: "source",
                ..
            })
        ));
        assert!(matches!(
            Graph::new(
                GraphKind::DiGraph,
                [Node::new("a")],
                [Edge::new("aa", "a", "a"), Edge::new("aa", "a", "a")]
            ),
            Err(GraphError::DuplicateEdge(id)) if id == EdgeId::from("aa")
        ));
    }

    #[test]
    fn undirected_arc_iteration_does_not_duplicate_self_loops() {
        let graph = Graph::new(
            GraphKind::Graph,
            [Node::new("a"), Node::new("b")],
            [Edge::new("aa", "a", "a"), Edge::new("ab", "a", "b")],
        )
        .unwrap();
        let a = graph.node_index("a").unwrap();
        assert_eq!(
            graph
                .arcs(a, super::super::TraversalDirection::Both)
                .count(),
            2
        );
    }

    #[test]
    fn bidirectional_arc_iteration_merges_incoming_and_outgoing_stably() {
        let graph = Graph::new(
            GraphKind::DiGraph,
            [
                Node::new("a"),
                Node::new("b"),
                Node::new("c"),
                Node::new("d"),
            ],
            [
                Edge::new("ca", "c", "a"),
                Edge::new("ab", "a", "b"),
                Edge::new("da", "d", "a"),
            ],
        )
        .unwrap();
        let neighbors = graph
            .arcs(graph.node_index("a").unwrap(), TraversalDirection::Both)
            .map(|arc| graph.node_id(arc.neighbor).clone())
            .collect::<Vec<_>>();
        assert_eq!(neighbors, ["b", "c", "d"]);
    }

    #[test]
    fn constrained_float_types_reject_invalid_states_during_decode() {
        assert!(PositiveFiniteF64::new(1.0).is_ok());
        assert!(PositiveFiniteF64::new(0.0).is_err());
        assert!(NonNegativeFiniteF64::new(0.0).is_ok());
        assert!(NonNegativeFiniteF64::new(-1.0).is_err());
        assert!(serde_json::from_str::<PositiveFiniteF64>("null").is_err());
        assert_eq!(f64::from(PositiveFiniteF64::try_from(2.0).unwrap()), 2.0);
        assert_eq!(f64::from(NonNegativeFiniteF64::try_from(0.5).unwrap()), 0.5);
    }

    #[test]
    fn structural_edge_ids_round_trip_and_keep_user_strings_distinct() {
        let original = EdgeId::original("reverse#1(edge)");
        let reverse = EdgeId::original("edge").reversed().unwrap();
        assert_ne!(original, reverse);
        assert_eq!(reverse.to_string(), "reverse#1(edge)");
        assert_eq!(
            EdgeId::synthesized_reverse("edge", u64::MAX)
                .unwrap()
                .reversed(),
            None
        );
        assert!(EdgeId::synthesized_reverse("edge", 0).is_none());
        for edge_id in [
            original,
            reverse,
            EdgeId::synthesized_reverse("edge", 42).unwrap(),
        ] {
            let encoded = serde_json::to_vec(&edge_id).unwrap();
            assert_eq!(serde_json::from_slice::<EdgeId>(&encoded).unwrap(), edge_id);
        }
    }

    #[test]
    fn graph_kind_is_declared_and_simple_kinds_reject_parallel_edges() {
        for (kind, directed, multigraph) in [
            (GraphKind::Graph, false, false),
            (GraphKind::DiGraph, true, false),
            (GraphKind::MultiGraph, false, true),
            (GraphKind::MultiDiGraph, true, true),
        ] {
            let encoded = serde_json::to_vec(&kind).unwrap();
            assert_eq!(serde_json::from_slice::<GraphKind>(&encoded).unwrap(), kind);
            assert_eq!(kind.is_directed(), directed);
            assert_eq!(kind.is_multigraph(), multigraph);
        }
        let directed = Graph::new(
            GraphKind::DiGraph,
            [Node::new("a"), Node::new("b")],
            [Edge::new("ab", "a", "b"), Edge::new("ba", "b", "a")],
        )
        .unwrap();
        assert!(!directed.is_multigraph());
        assert!(matches!(
            Graph::new(
                GraphKind::DiGraph,
                [Node::new("a"), Node::new("b")],
                [Edge::new("ab", "a", "b"), Edge::new("ab2", "a", "b")]
            ),
            Err(GraphError::ParallelEdge { .. })
        ));
        let multigraph = Graph::new(GraphKind::MultiGraph, [Node::new("a")], []).unwrap();
        assert!(multigraph.is_multigraph());
        assert!(!multigraph.is_directed());
    }
}
