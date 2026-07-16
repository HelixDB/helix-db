//! One-query loading for the immutable native graph implementation.
//!
//! [`Client::graph`] executes one ordinary read batch. The returned
//! [`helix_graph::Graph`] is self-contained: algorithms and graph-object reads
//! never query Helix again.

use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use helix_ast::prelude::{OnEdges, OnNodes, ReadOnly, Traversal};
use helix_graph as graph_core;
use helix_graph::loader::{
    self, GraphLoadSpec, EDGE_ID, EDGE_KEY, EDGE_LABEL, EDGE_SOURCE, EDGE_TARGET, EDGE_WEIGHT,
    EXTERNAL_ID, NODE_ID, NODE_LABEL, PRIVATE_PREFIX,
};
pub use helix_graph::{
    Attributes, BetweennessMode, BetweennessOptions, Community, CommunityResult, Cycle,
    CycleOptions, CycleResult, DegreeKind, Edge, EdgeId, EdgeScore, EdgeTraversalDirection, Graph,
    HubExpansionPolicy, LayoutOptions, LouvainOptions, Node, NodeDegree, NodeId, NodePosition,
    NodeScore, NonNegativeFiniteF64, PathEdge, PathResult, PathWeight, PositiveFiniteF64,
    TraversalDirection, TraversalOptions, TraversalResult, TraversalStrategy, TraversedEdge, Visit,
};
use thiserror::Error;

use crate::{read_batch, Client, HelixError, Projection, QueryRequest};

/// Direction-only compatibility contract for the existing Rust SDK surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphDirection {
    /// Preserve stored edge direction.
    Directed,
    /// Treat stored edges as traversable in both directions.
    Undirected,
}

/// A validated property name used by graph selections.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct GraphProperty(String);

impl GraphProperty {
    /// Construct a selected property name.
    pub fn new(name: impl Into<String>) -> Result<Self, GraphSelectionError> {
        let name = name.into();
        if name.is_empty() {
            return Err(GraphSelectionError::EmptyProperty);
        }
        if name.starts_with(PRIVATE_PREFIX) {
            return Err(GraphSelectionError::ReservedProperty(name));
        }
        Ok(Self(name))
    }

    /// Borrow the property name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Whether a selection is expected to be filtered or deliberately scans all
/// nodes and edges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphScanPolicy {
    /// The supplied traversals constrain the selected graph.
    Filtered,
    /// The caller explicitly accepts a full graph scan.
    AllowFullScan,
}

/// Typed inputs used to construct the one graph-loading read batch.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphSelection {
    node_traversal: Traversal<OnNodes, ReadOnly>,
    edge_traversal: Traversal<OnEdges, ReadOnly>,
    direction: GraphDirection,
    node_properties: BTreeSet<GraphProperty>,
    edge_properties: BTreeSet<GraphProperty>,
    external_identity: Option<GraphProperty>,
    graphify_edge_key: Option<GraphProperty>,
    weight: Option<GraphProperty>,
    node_limit: Option<NonZeroUsize>,
    edge_limit: Option<NonZeroUsize>,
    scan_policy: GraphScanPolicy,
}

impl GraphSelection {
    /// Construct a filtered selection from node- and edge-producing read
    /// traversals.
    pub fn new(
        node_traversal: Traversal<OnNodes, ReadOnly>,
        edge_traversal: Traversal<OnEdges, ReadOnly>,
        direction: GraphDirection,
    ) -> Self {
        Self {
            node_traversal,
            edge_traversal,
            direction,
            node_properties: BTreeSet::new(),
            edge_properties: BTreeSet::new(),
            external_identity: None,
            graphify_edge_key: None,
            weight: None,
            node_limit: None,
            edge_limit: None,
            scan_policy: GraphScanPolicy::Filtered,
        }
    }

    /// Explicitly permit unfiltered node and edge traversals.
    #[must_use]
    pub fn allow_full_scan(mut self) -> Self {
        self.scan_policy = GraphScanPolicy::AllowFullScan;
        self
    }

    /// Select node properties retained by the graph.
    pub fn with_node_properties(
        mut self,
        properties: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, GraphSelectionError> {
        self.node_properties = properties
            .into_iter()
            .map(GraphProperty::new)
            .collect::<Result<_, _>>()?;
        Ok(self)
    }

    /// Select edge properties retained by the graph.
    pub fn with_edge_properties(
        mut self,
        properties: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, GraphSelectionError> {
        self.edge_properties = properties
            .into_iter()
            .map(GraphProperty::new)
            .collect::<Result<_, _>>()?;
        Ok(self)
    }

    /// Use a selected node property as the public graph node identity.
    pub fn with_external_identity(
        mut self,
        property: impl Into<String>,
    ) -> Result<Self, GraphSelectionError> {
        self.external_identity = Some(GraphProperty::new(property)?);
        Ok(self)
    }

    /// Preserve a Graphify multigraph key separately from the Helix edge ID.
    pub fn with_graphify_edge_key(
        mut self,
        property: impl Into<String>,
    ) -> Result<Self, GraphSelectionError> {
        self.graphify_edge_key = Some(GraphProperty::new(property)?);
        Ok(self)
    }

    /// Select the non-negative finite numeric edge weight used by weighted
    /// algorithms.
    pub fn with_weight(mut self, property: impl Into<String>) -> Result<Self, GraphSelectionError> {
        self.weight = Some(GraphProperty::new(property)?);
        Ok(self)
    }

    /// Reject selections returning more than `limit` nodes.
    #[must_use]
    pub fn with_node_limit(mut self, limit: NonZeroUsize) -> Self {
        self.node_limit = Some(limit);
        self
    }

    /// Reject selections returning more than `limit` edges.
    #[must_use]
    pub fn with_edge_limit(mut self, limit: NonZeroUsize) -> Self {
        self.edge_limit = Some(limit);
        self
    }

    /// Direction semantics of the constructed graph.
    pub const fn direction(&self) -> GraphDirection {
        self.direction
    }

    /// Full-scan policy chosen by the caller.
    pub const fn scan_policy(&self) -> GraphScanPolicy {
        self.scan_policy
    }

    fn validate_scan_policy(&self) -> Result<(), GraphSelectionError> {
        let starts_with_full_scan = |traversal: &serde_json::Value, source: &str| {
            let mut current = traversal;
            loop {
                let Some(object) = current.as_object() else {
                    return false;
                };
                if object
                    .get(source)
                    .and_then(serde_json::Value::as_object)
                    .and_then(|source| source.get("reference"))
                    .is_some_and(|reference| reference == "all")
                {
                    return true;
                }
                let Some(input) = object
                    .values()
                    .find_map(serde_json::Value::as_object)
                    .and_then(|fields| fields.get("input"))
                else {
                    return false;
                };
                current = input;
            }
        };
        let nodes = serde_json::to_value(self.node_traversal.root())
            .expect("Helix traversal AST serialization is infallible");
        let edges = serde_json::to_value(self.edge_traversal.root())
            .expect("Helix traversal AST serialization is infallible");
        if self.scan_policy == GraphScanPolicy::Filtered
            && (starts_with_full_scan(&nodes, "nodes") || starts_with_full_scan(&edges, "edges"))
        {
            return Err(GraphSelectionError::FullScanRequiresOptIn);
        }
        Ok(())
    }

    /// Build the ordinary read request used by [`Client::graph`].
    pub fn to_query_request(&self) -> QueryRequest {
        let mut node_projection = vec![
            Projection::property("$id", NODE_ID),
            Projection::property(
                self.external_identity
                    .as_ref()
                    .map_or("$id", GraphProperty::as_str),
                EXTERNAL_ID,
            ),
            Projection::property("$label", NODE_LABEL),
        ];
        node_projection.extend(
            self.node_properties
                .iter()
                .map(|property| Projection::property(property.as_str(), property.as_str())),
        );

        let mut edge_projection = vec![
            Projection::property("$id", EDGE_ID),
            Projection::from_endpoint("$id", EDGE_SOURCE),
            Projection::to_endpoint("$id", EDGE_TARGET),
            Projection::property("$label", EDGE_LABEL),
        ];
        if let Some(property) = &self.graphify_edge_key {
            edge_projection.push(Projection::property(property.as_str(), EDGE_KEY));
        }
        if let Some(property) = &self.weight {
            edge_projection.push(Projection::property(property.as_str(), EDGE_WEIGHT));
        }
        edge_projection.extend(
            self.edge_properties
                .iter()
                .map(|property| Projection::property(property.as_str(), property.as_str())),
        );

        let nodes = match self.node_limit {
            Some(limit) => self
                .node_traversal
                .clone()
                .limit(limit.get().saturating_add(1)),
            None => self.node_traversal.clone(),
        };
        let edges = match self.edge_limit {
            Some(limit) => self
                .edge_traversal
                .clone()
                .limit(limit.get().saturating_add(1)),
            None => self.edge_traversal.clone(),
        };
        QueryRequest::read(
            read_batch()
                .var_as("nodes", nodes.project(node_projection))
                .var_as("edges", edges.project(edge_projection))
                .returning(["nodes", "edges"]),
        )
    }
}

/// Invalid selection metadata rejected before a query executes.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum GraphSelectionError {
    /// Property names must not be empty.
    #[error("graph property names must not be empty")]
    EmptyProperty,
    /// The name would collide with loader-owned result metadata.
    #[error("graph property name uses reserved prefix: {0}")]
    ReservedProperty(String),
    /// An all-nodes or all-edges source requires explicit acknowledgement.
    #[error("full graph scans require GraphSelection::allow_full_scan()")]
    FullScanRequiresOptIn,
}

/// Failure to execute or validate a graph selection.
#[derive(Debug, Error)]
pub enum GraphLoadError {
    /// The graph selection is unsafe or invalid.
    #[error(transparent)]
    Selection(#[from] GraphSelectionError),
    /// The ordinary Helix query failed.
    #[error(transparent)]
    Helix(#[from] HelixError),
    /// Rust graph response validation or construction failed.
    #[error(transparent)]
    Graph(#[from] loader::GraphLoadError),
}

impl Client {
    /// Execute one ordinary read batch and construct an immutable native graph.
    ///
    /// All methods on the returned graph operate locally and perform no
    /// additional Helix reads.
    pub async fn graph(&self, selection: &GraphSelection) -> Result<Graph, GraphLoadError> {
        selection.validate_scan_policy()?;
        let response = self
            .query_raw(selection.to_query_request())
            .send_bytes()
            .await?;
        graph_from_response(selection, &response)
    }
}

/// Validate raw `/v1/query` response bytes and construct the immutable graph.
///
/// This is public so native SDK bindings can pass response bytes directly to
/// Rust without materializing a second language-level graph.
pub fn graph_from_response(
    selection: &GraphSelection,
    response: &[u8],
) -> Result<Graph, GraphLoadError> {
    loader::graph_from_response(
        GraphLoadSpec {
            kind: match selection.direction {
                GraphDirection::Directed => graph_core::GraphKind::DiGraph,
                GraphDirection::Undirected => graph_core::GraphKind::Graph,
            },
            node_identity: match &selection.external_identity {
                Some(property) => graph_core::IdentitySelection::ScalarProperty(
                    graph_core::GraphProperty::new(property.as_str())
                        .expect("SDK graph properties are validated"),
                ),
                None => graph_core::IdentitySelection::InternalId,
            },
            edge_key_identity: selection.graphify_edge_key.as_ref().map(|property| {
                graph_core::IdentitySelection::ScalarProperty(
                    graph_core::GraphProperty::new(property.as_str())
                        .expect("SDK graph properties are validated"),
                )
            }),
            node_limit: selection.node_limit.map(NonZeroUsize::get),
            edge_limit: selection.edge_limit.map(NonZeroUsize::get),
        },
        response,
    )
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use serde_json::{json, Value};

    use super::*;
    use crate::{g, EdgeRef, NodeRef, SourcePredicate};

    fn selection() -> GraphSelection {
        GraphSelection::new(
            g().n_where(SourcePredicate::has_key("$id")),
            g().e_where(SourcePredicate::has_key("$id")),
            GraphDirection::Directed,
        )
        .with_node_properties(["name"])
        .expect("valid property")
        .with_edge_properties(["relation"])
        .expect("valid property")
        .with_external_identity("external_id")
        .expect("valid property")
        .with_graphify_edge_key("key")
        .expect("valid property")
        .with_weight("weight")
        .expect("valid property")
    }

    fn response(nodes: Value, edges: Value) -> Vec<u8> {
        serde_json::to_vec(&json!({ "nodes": nodes, "edges": edges })).expect("fixture JSON")
    }

    #[test]
    fn query_uses_private_aliases_and_limit_sentinels() {
        let request = selection()
            .with_node_limit(NonZeroUsize::new(2).expect("non-zero"))
            .with_edge_limit(NonZeroUsize::new(3).expect("non-zero"))
            .to_query_request()
            .to_json_string()
            .expect("serialize request");
        assert!(request.contains(NODE_ID));
        assert!(request.contains(EXTERNAL_ID));
        assert!(request.contains(EDGE_SOURCE));
        assert!(request.contains("\"literal\":3"), "{request}");
        assert!(request.contains("\"literal\":4"), "{request}");
    }

    #[test]
    fn full_scan_requires_explicit_opt_in_even_after_a_filter_step() {
        let selection = GraphSelection::new(
            g().n(NodeRef::all()).has_label("File"),
            g().e(EdgeRef::all()).has_label("DEPENDS_ON"),
            GraphDirection::Directed,
        );
        assert_eq!(
            selection.validate_scan_policy(),
            Err(GraphSelectionError::FullScanRequiresOptIn)
        );
        assert!(selection.allow_full_scan().validate_scan_policy().is_ok());
    }

    #[test]
    fn loader_preserves_identity_topology_properties_and_weights() {
        let bytes = response(
            json!([
                { (NODE_ID): "n1", (EXTERNAL_ID): "a", (NODE_LABEL): "File", "name": "A" },
                { (NODE_ID): "n2", (EXTERNAL_ID): "b", (NODE_LABEL): "File", "name": "B" }
            ]),
            json!([{
                (EDGE_ID): "e1", (EDGE_SOURCE): "n1", (EDGE_TARGET): "n2",
                (EDGE_KEY): "imports", (EDGE_LABEL): "DEPENDS_ON", (EDGE_WEIGHT): 2.5,
                "relation": "import"
            }]),
        );
        let graph = graph_from_response(&selection(), &bytes).expect("valid graph response");
        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.edge_count(), 1);
        assert_eq!(
            graph.node("a").expect("node").label.as_deref(),
            Some("File")
        );
        let edge = graph.edge("e1").expect("edge");
        assert_eq!(edge.source, "a");
        assert_eq!(edge.target, "b");
        assert_eq!(edge.graphify_key, Some("imports".into()));
        assert_eq!(edge.weight, Some(2.5));
    }

    #[test]
    fn loader_rejects_duplicates_missing_endpoints_and_bad_rows() {
        let duplicate = response(
            json!([
                { (NODE_ID): "n1", (EXTERNAL_ID): "same", (NODE_LABEL): null },
                { (NODE_ID): "n2", (EXTERNAL_ID): "same", (NODE_LABEL): null }
            ]),
            json!([]),
        );
        assert!(matches!(
            graph_from_response(&selection(), &duplicate),
            Err(GraphLoadError::Graph(loader::GraphLoadError::DuplicateExternalIdentity(id)))
                if id == "same"
        ));

        let outside = response(
            json!([{ (NODE_ID): "n1", (EXTERNAL_ID): "a", (NODE_LABEL): null }]),
            json!([{
                (EDGE_ID): "e1", (EDGE_SOURCE): "n1", (EDGE_TARGET): "missing",
                (EDGE_LABEL): null
            }]),
        );
        assert!(matches!(
            graph_from_response(&selection(), &outside),
            Err(GraphLoadError::Graph(loader::GraphLoadError::InvalidRow {
                kind: "edge",
                ..
            }))
        ));

        assert!(matches!(
            graph_from_response(&selection(), b"[]"),
            Err(GraphLoadError::Graph(
                loader::GraphLoadError::InvalidResponse(_)
            ))
        ));
    }

    #[test]
    fn loader_never_returns_a_truncated_graph() {
        let bytes = response(
            json!([
                { (NODE_ID): "n1", (EXTERNAL_ID): "a", (NODE_LABEL): null },
                { (NODE_ID): "n2", (EXTERNAL_ID): "b", (NODE_LABEL): null }
            ]),
            json!([]),
        );
        let selection = selection().with_node_limit(NonZeroUsize::new(1).expect("non-zero"));
        assert!(matches!(
            graph_from_response(&selection, &bytes),
            Err(GraphLoadError::Graph(
                loader::GraphLoadError::IncompleteSelection {
                    kind: "node",
                    limit: 1
                }
            ))
        ));
    }

    #[test]
    fn property_names_reject_empty_and_reserved_aliases() {
        assert_eq!(
            GraphProperty::new(""),
            Err(GraphSelectionError::EmptyProperty)
        );
        assert!(matches!(
            GraphProperty::new("__helix_graph_bad"),
            Err(GraphSelectionError::ReservedProperty(_))
        ));
    }

    #[tokio::test]
    async fn graph_loads_once_algorithms_do_not_read_and_a_new_load_gets_a_new_snapshot() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let first = response(
            json!([{ (NODE_ID): "n1", (EXTERNAL_ID): "a", (NODE_LABEL): null }]),
            json!([]),
        );
        let second = response(
            json!([
                { (NODE_ID): "n1", (EXTERNAL_ID): "a", (NODE_LABEL): null },
                { (NODE_ID): "n2", (EXTERNAL_ID): "b", (NODE_LABEL): null }
            ]),
            json!([]),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for body in [first, second] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 16_384];
                let _ = socket.read(&mut request).await.unwrap();
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                socket.write_all(header.as_bytes()).await.unwrap();
                socket.write_all(&body).await.unwrap();
            }
            2_usize
        });
        let client = Client::new(Some(&base)).unwrap();
        let first = client.graph(&selection()).await.unwrap();
        assert_eq!(first.node_count(), 1);
        assert_eq!(
            first
                .betweenness_centrality(helix_graph::BetweennessOptions::default())
                .unwrap()
                .len(),
            1
        );
        let second = client.graph(&selection()).await.unwrap();
        assert_eq!(second.node_count(), 2);
        assert_eq!(server.await.unwrap(), 2);
    }
}
