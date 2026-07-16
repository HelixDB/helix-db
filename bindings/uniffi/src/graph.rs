//! FFI-safe façade over the storage-independent Rust graph crate.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

use helix_graph as core;
use thiserror::Error;

/// Errors returned by native graph loading and algorithms.
#[derive(Debug, Error, uniffi::Error)]
pub enum NativeGraphError {
    /// The normal query response did not match the graph projection contract.
    #[error("{message}")]
    InvalidResponse { message: String },
    /// A configured node or edge limit proved the selection incomplete.
    #[error("graph selection exceeded the {kind} safety limit of {limit}")]
    IncompleteSelection { kind: String, limit: u64 },
    /// External node identities were not unique.
    #[error("duplicate external node identity: {identity}")]
    DuplicateIdentity { identity: String },
    /// Graph metadata selection returned more than one row.
    #[error("graph metadata selection returned {count} rows; expected at most one")]
    MultipleGraphMetadataRows { count: u64 },
    /// A requested node or edge was not loaded.
    #[error("{message}")]
    UnknownEntity { message: String },
    /// An algorithm option was invalid at the FFI boundary.
    #[error("{message}")]
    InvalidOption { message: String },
    /// The underlying normal Helix query failed.
    #[error("{message}")]
    Query { message: String },
    /// Graph topology validation failed.
    #[error("{message}")]
    InvalidGraph { message: String },
}

impl NativeGraphError {
    fn invalid(message: impl Into<String>) -> Self {
        Self::InvalidOption {
            message: message.into(),
        }
    }
}

impl From<core::GraphError> for NativeGraphError {
    fn from(error: core::GraphError) -> Self {
        let message = error.to_string();
        match error {
            core::GraphError::UnknownNode(_) | core::GraphError::UnknownEdge(_) => {
                Self::UnknownEntity { message }
            }
            core::GraphError::InvalidOption(_) => Self::InvalidOption { message },
            core::GraphError::EmptyEdgeId
            | core::GraphError::DuplicateNode(_)
            | core::GraphError::DuplicateEdge(_)
            | core::GraphError::MissingEndpoint { .. }
            | core::GraphError::InvalidWeight { .. }
            | core::GraphError::InvalidExternalId(_)
            | core::GraphError::ParallelEdge { .. }
            | core::GraphError::RelabelCollision { .. }
            | core::GraphError::KindMismatch
            | core::GraphError::ConflictingEdge { .. }
            | core::GraphError::EdgeIdentityExhausted { .. } => Self::InvalidGraph { message },
        }
    }
}

impl From<core::loader::GraphLoadError> for NativeGraphError {
    fn from(error: core::loader::GraphLoadError) -> Self {
        match error {
            core::loader::GraphLoadError::InvalidResponse(message) => {
                Self::InvalidResponse { message }
            }
            invalid @ core::loader::GraphLoadError::InvalidRow { .. } => Self::InvalidResponse {
                message: invalid.to_string(),
            },
            core::loader::GraphLoadError::IncompleteSelection { kind, limit } => {
                Self::IncompleteSelection {
                    kind: kind.to_string(),
                    limit: u64::try_from(limit).expect("usize fits in u64"),
                }
            }
            core::loader::GraphLoadError::DuplicateExternalIdentity(identity) => {
                Self::DuplicateIdentity {
                    identity: identity.to_string(),
                }
            }
            core::loader::GraphLoadError::MultipleGraphMetadataRows { count } => {
                Self::MultipleGraphMetadataRows {
                    count: u64::try_from(count).expect("usize fits in u64"),
                }
            }
            core::loader::GraphLoadError::Graph(error) => error.into(),
        }
    }
}

/// Declared graph topology contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum NativeGraphKind {
    Graph,
    DiGraph,
    MultiGraph,
    MultiDiGraph,
}

impl From<NativeGraphKind> for core::GraphKind {
    fn from(kind: NativeGraphKind) -> Self {
        match kind {
            NativeGraphKind::Graph => Self::Graph,
            NativeGraphKind::DiGraph => Self::DiGraph,
            NativeGraphKind::MultiGraph => Self::MultiGraph,
            NativeGraphKind::MultiDiGraph => Self::MultiDiGraph,
        }
    }
}

/// JSON representation used for a projected external identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum NativeIdentityEncoding {
    Scalar,
    Tagged,
}

/// Canonically tagged external identity bytes.
#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct NativeExternalId {
    pub encoded: Vec<u8>,
}

/// Canonically encoded structural edge identity.
#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct NativeEdgeId {
    pub encoded: Vec<u8>,
}

/// Response-validation metadata supplied by an SDK graph selection.
#[derive(Debug, Clone, uniffi::Record)]
pub struct NativeGraphLoadSpec {
    /// Declared graph topology contract.
    pub kind: NativeGraphKind,
    /// Projected node identity representation.
    pub node_identity: NativeIdentityEncoding,
    /// Optional projected Graphify key representation.
    pub edge_key_identity: Option<NativeIdentityEncoding>,
    /// Maximum complete node result size.
    pub node_limit: Option<u64>,
    /// Maximum complete edge result size.
    pub edge_limit: Option<u64>,
}

/// Immutable node record. Attributes remain lazy JSON bytes.
#[derive(Debug, Clone, uniffi::Record)]
pub struct NativeGraphNode {
    /// External node identity.
    pub id: NativeExternalId,
    /// Optional Helix label.
    pub label: Option<String>,
    /// Selected properties encoded as one JSON object.
    pub attributes_json: Vec<u8>,
}

/// Immutable edge record. Attributes remain lazy JSON bytes.
#[derive(Debug, Clone, uniffi::Record)]
pub struct NativeGraphEdge {
    /// Stable Helix edge identity.
    pub id: NativeEdgeId,
    /// Optional Graphify multigraph key.
    pub graphify_key: Option<NativeExternalId>,
    /// Stored source external node identity.
    pub source: NativeExternalId,
    /// Stored target external node identity.
    pub target: NativeExternalId,
    /// Optional Helix label.
    pub label: Option<String>,
    /// Optional algorithm weight.
    pub weight: Option<f64>,
    /// Selected properties encoded as one JSON object.
    pub attributes_json: Vec<u8>,
}

/// Exact, sampled, or Graphify-compatible automatic Brandes mode.
#[derive(Debug, Clone, uniffi::Enum)]
pub enum NativeBetweennessMode {
    /// Use every node as a source.
    Exact,
    /// Use a deterministic unique source sample.
    Sampled { sample_count: u64, seed: u64 },
    /// Select exact/sample mode from graph size.
    Auto {
        exact_through: u64,
        sample_count: u64,
        seed: u64,
    },
}

/// Brandes algorithm options.
#[derive(Debug, Clone, uniffi::Record)]
pub struct NativeBetweennessOptions {
    /// Source-selection mode.
    pub mode: NativeBetweennessMode,
    /// Apply NetworkX-compatible normalization.
    pub normalized: bool,
    /// Include endpoints for node centrality.
    pub endpoints: bool,
    /// Use the selected edge weight rather than unit costs.
    pub weighted: bool,
}

/// One node centrality score.
#[derive(Debug, Clone, uniffi::Record)]
pub struct NativeNodeScore {
    pub node_id: NativeExternalId,
    pub score: f64,
}

/// One stable edge centrality score.
#[derive(Debug, Clone, uniffi::Record)]
pub struct NativeEdgeScore {
    pub edge_id: NativeEdgeId,
    pub graphify_key: Option<NativeExternalId>,
    pub source: NativeExternalId,
    pub target: NativeExternalId,
    pub score: f64,
}

/// Bounded simple cycle.
#[derive(Debug, Clone, uniffi::Record)]
pub struct NativeCycle {
    pub node_ids: Vec<NativeExternalId>,
    pub edge_ids: Vec<NativeEdgeId>,
}

/// Cycle output including output-cap state.
#[derive(Debug, Clone, uniffi::Record)]
pub struct NativeCycleResult {
    pub cycles: Vec<NativeCycle>,
    pub truncated: bool,
}

/// Local traversal direction.
#[derive(Debug, Clone, Copy, uniffi::Enum)]
pub enum NativeTraversalDirection {
    Out,
    In,
    Both,
}

impl From<NativeTraversalDirection> for core::TraversalDirection {
    fn from(direction: NativeTraversalDirection) -> Self {
        match direction {
            NativeTraversalDirection::Out => Self::Out,
            NativeTraversalDirection::In => Self::In,
            NativeTraversalDirection::Both => Self::Both,
        }
    }
}

/// Breadth-first or depth-first traversal.
#[derive(Debug, Clone, Copy, uniffi::Enum)]
pub enum NativeTraversalStrategy {
    BreadthFirst,
    DepthFirst,
}

/// Hub expansion behavior.
#[derive(Debug, Clone, uniffi::Enum)]
pub enum NativeHubExpansionPolicy {
    ExpandAll,
    StopNonSeedAtOrAbove { degree: u64 },
}

/// BFS/DFS options.
#[derive(Debug, Clone, uniffi::Record)]
pub struct NativeTraversalOptions {
    pub strategy: NativeTraversalStrategy,
    pub seeds: Vec<NativeExternalId>,
    pub max_depth: u64,
    pub direction: NativeTraversalDirection,
    pub allowed_labels: Vec<String>,
    pub hub_policy: NativeHubExpansionPolicy,
}

/// One traversal visit.
#[derive(Debug, Clone, uniffi::Record)]
pub struct NativeVisit {
    pub node_id: NativeExternalId,
    pub depth: u64,
    pub discovery_order: u64,
}

/// Orientation used to traverse an edge.
#[derive(Debug, Clone, Copy, uniffi::Enum)]
pub enum NativeEdgeTraversalDirection {
    Forward,
    Reverse,
}

/// Edge responsible for discovering a traversal node.
#[derive(Debug, Clone, uniffi::Record)]
pub struct NativeTraversedEdge {
    pub edge_id: NativeEdgeId,
    pub graphify_key: Option<NativeExternalId>,
    pub source: NativeExternalId,
    pub target: NativeExternalId,
    pub traversal_direction: NativeEdgeTraversalDirection,
    pub label: Option<String>,
}

/// Stable traversal output.
#[derive(Debug, Clone, uniffi::Record)]
pub struct NativeTraversalResult {
    pub visits: Vec<NativeVisit>,
    pub discovery_edges: Vec<NativeTraversedEdge>,
}

/// Degree flavor.
#[derive(Debug, Clone, Copy, uniffi::Enum)]
pub enum NativeDegreeKind {
    In,
    Out,
    Total,
}

/// One degree record.
#[derive(Debug, Clone, uniffi::Record)]
pub struct NativeNodeDegree {
    pub node_id: NativeExternalId,
    pub degree: u64,
    pub weighted_degree: f64,
}

/// One edge of a found shortest path.
#[derive(Debug, Clone, uniffi::Record)]
pub struct NativePathEdge {
    pub edge_id: NativeEdgeId,
    pub graphify_key: Option<NativeExternalId>,
    pub source: NativeExternalId,
    pub target: NativeExternalId,
    pub traversal_direction: NativeEdgeTraversalDirection,
    pub label: Option<String>,
    pub attributes_json: Vec<u8>,
}

/// Exhaustive shortest-path result state.
#[derive(Debug, Clone, uniffi::Enum)]
pub enum NativePathResult {
    MissingSource,
    MissingTarget,
    NoPath,
    Found {
        node_ids: Vec<NativeExternalId>,
        edges: Vec<NativePathEdge>,
    },
}

/// One canonical Louvain community.
#[derive(Debug, Clone, uniffi::Record)]
pub struct NativeCommunity {
    pub id: NativeExternalId,
    pub node_ids: Vec<NativeExternalId>,
}

/// Louvain output and diagnostics.
#[derive(Debug, Clone, uniffi::Record)]
pub struct NativeCommunityResult {
    pub communities: Vec<NativeCommunity>,
    pub modularity: f64,
    pub levels: u64,
}

/// Weighted Leiden output and winning-trial diagnostics.
#[derive(Debug, Clone, uniffi::Record)]
pub struct NativeLeidenResult {
    pub communities: Vec<NativeCommunity>,
    pub modularity: f64,
    pub levels: u64,
    pub winning_trial: u64,
}

/// One deterministic spring-layout position.
#[derive(Debug, Clone, uniffi::Record)]
pub struct NativeNodePosition {
    pub node_id: NativeExternalId,
    pub x: f64,
    pub y: f64,
}

/// One external-ID relabel operation.
#[derive(Debug, Clone, uniffi::Record)]
pub struct NativeRelabel {
    pub from: NativeExternalId,
    pub to: NativeExternalId,
}

/// Immutable native graph object shared by Python, Node, and Go.
#[derive(uniffi::Object)]
pub struct NativeGraph {
    graph: core::Graph,
}

impl NativeGraph {
    fn new(graph: core::Graph) -> Arc<Self> {
        Arc::new(Self { graph })
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl NativeGraph {
    /// Node centrality on Tokio's blocking pool for event-loop SDKs.
    pub async fn betweenness_centrality_async(
        &self,
        options: NativeBetweennessOptions,
    ) -> Result<Vec<NativeNodeScore>, NativeGraphError> {
        let graph = self.graph.clone();
        run_blocking(move || NativeGraph { graph }.betweenness_centrality(options)).await
    }

    /// Edge centrality on Tokio's blocking pool for event-loop SDKs.
    pub async fn edge_betweenness_centrality_async(
        &self,
        options: NativeBetweennessOptions,
    ) -> Result<Vec<NativeEdgeScore>, NativeGraphError> {
        let graph = self.graph.clone();
        run_blocking(move || NativeGraph { graph }.edge_betweenness_centrality(options)).await
    }

    /// Bounded cycles on Tokio's blocking pool for event-loop SDKs.
    pub async fn simple_cycles_async(
        &self,
        length_bound: u64,
        max_cycles: Option<u64>,
    ) -> Result<NativeCycleResult, NativeGraphError> {
        let graph = self.graph.clone();
        run_blocking(move || NativeGraph { graph }.simple_cycles(length_bound, max_cycles)).await
    }

    /// BFS/DFS on Tokio's blocking pool for event-loop SDKs.
    pub async fn traverse_async(
        &self,
        options: NativeTraversalOptions,
    ) -> Result<NativeTraversalResult, NativeGraphError> {
        let graph = self.graph.clone();
        run_blocking(move || NativeGraph { graph }.traverse(options)).await
    }

    /// Local shortest path on Tokio's blocking pool for event-loop SDKs.
    pub async fn shortest_path_async(
        &self,
        source: NativeExternalId,
        target: NativeExternalId,
        direction: NativeTraversalDirection,
        allowed_labels: Vec<String>,
        max_depth: Option<u64>,
    ) -> Result<NativePathResult, NativeGraphError> {
        let graph = self.graph.clone();
        run_blocking(move || {
            NativeGraph { graph }.shortest_path(
                source,
                target,
                direction,
                allowed_labels,
                max_depth,
            )
        })
        .await
    }

    /// Louvain on Tokio's blocking pool for event-loop SDKs.
    pub async fn louvain_communities_async(
        &self,
        resolution: f64,
        threshold: f64,
        seed: u64,
        max_levels: u64,
    ) -> Result<NativeCommunityResult, NativeGraphError> {
        let graph = self.graph.clone();
        run_blocking(move || {
            NativeGraph { graph }.louvain_communities(resolution, threshold, seed, max_levels)
        })
        .await
    }

    /// Spring layout on Tokio's blocking pool for event-loop SDKs.
    pub async fn spring_layout_async(
        &self,
        k: Option<f64>,
        iterations: u64,
        seed: u64,
        weighted: bool,
        initial_positions: Vec<NativeNodePosition>,
    ) -> Result<Vec<NativeNodePosition>, NativeGraphError> {
        let graph = self.graph.clone();
        run_blocking(move || {
            NativeGraph { graph }.spring_layout(k, iterations, seed, weighted, initial_positions)
        })
        .await
    }
}

async fn run_blocking<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T, NativeGraphError> + Send + 'static,
) -> Result<T, NativeGraphError> {
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| NativeGraphError::InvalidGraph {
            message: format!("native graph worker failed: {error}"),
        })?
}

fn native_external_id(value: &core::ExternalId) -> Result<NativeExternalId, NativeGraphError> {
    Ok(NativeExternalId {
        encoded: value.to_json_bytes()?,
    })
}

fn core_external_id(value: NativeExternalId) -> Result<core::ExternalId, NativeGraphError> {
    let parsed =
        serde_json::from_slice(&value.encoded).map_err(|error| NativeGraphError::InvalidGraph {
            message: format!("invalid external identity JSON: {error}"),
        })?;
    let identity = core::ExternalId::from_tagged_value(parsed)?;
    if identity.to_json_bytes()? != value.encoded {
        return Err(NativeGraphError::InvalidGraph {
            message: "external identity is not canonically encoded".to_string(),
        });
    }
    Ok(identity)
}

fn native_edge_id(value: &core::EdgeId) -> Result<NativeEdgeId, NativeGraphError> {
    Ok(NativeEdgeId {
        encoded: serde_json::to_vec(value).map_err(|error| NativeGraphError::InvalidGraph {
            message: error.to_string(),
        })?,
    })
}

fn core_edge_id(value: NativeEdgeId) -> Result<core::EdgeId, NativeGraphError> {
    let decoded: core::EdgeId =
        serde_json::from_slice(&value.encoded).map_err(|error| NativeGraphError::InvalidGraph {
            message: format!("invalid structural edge identity: {error}"),
        })?;
    if native_edge_id(&decoded)?.encoded != value.encoded || decoded.stored_id().is_empty() {
        return Err(NativeGraphError::InvalidGraph {
            message: "edge identity is not canonically encoded".to_string(),
        });
    }
    Ok(decoded)
}

fn identity_selection(
    encoding: NativeIdentityEncoding,
) -> Result<core::IdentitySelection, NativeGraphError> {
    let property = core::GraphProperty::new(core::loader::EXTERNAL_ID)?;
    Ok(match encoding {
        NativeIdentityEncoding::Scalar => core::IdentitySelection::ScalarProperty(property),
        NativeIdentityEncoding::Tagged => core::IdentitySelection::TaggedProperty(property),
    })
}

/// Construct a native graph directly from ordinary query response bytes.
#[uniffi::export]
pub fn graph_from_query_response(
    spec: NativeGraphLoadSpec,
    response: Vec<u8>,
) -> Result<Arc<NativeGraph>, NativeGraphError> {
    core::loader::graph_from_response(
        core::loader::GraphLoadSpec {
            kind: spec.kind.into(),
            node_identity: identity_selection(spec.node_identity)?,
            edge_key_identity: spec.edge_key_identity.map(identity_selection).transpose()?,
            node_limit: optional_usize(spec.node_limit, "node_limit")?,
            edge_limit: optional_usize(spec.edge_limit, "edge_limit")?,
        },
        &response,
    )
    .map(NativeGraph::new)
    .map_err(Into::into)
}

#[uniffi::export]
impl NativeGraph {
    pub fn node_count(&self) -> u64 {
        self.graph.node_count() as u64
    }

    pub fn edge_count(&self) -> u64 {
        self.graph.edge_count() as u64
    }

    pub fn is_directed(&self) -> bool {
        self.graph.is_directed()
    }

    pub fn is_multigraph(&self) -> bool {
        self.graph.is_multigraph()
    }

    pub fn graph_attributes_json(&self) -> Result<Vec<u8>, NativeGraphError> {
        serde_json::to_vec(self.graph.attributes())
            .map_err(|error| NativeGraphError::invalid(error.to_string()))
    }

    pub fn contains_node(&self, node_id: NativeExternalId) -> Result<bool, NativeGraphError> {
        Ok(self.graph.contains_node(core_external_id(node_id)?))
    }

    pub fn contains_edge(&self, edge_id: NativeEdgeId) -> Result<bool, NativeGraphError> {
        Ok(self.graph.contains_edge(core_edge_id(edge_id)?))
    }

    pub fn nodes(&self) -> Result<Vec<NativeGraphNode>, NativeGraphError> {
        self.graph.nodes().iter().map(graph_node).collect()
    }

    pub fn edges(&self) -> Result<Vec<NativeGraphEdge>, NativeGraphError> {
        self.graph.edges().iter().map(graph_edge).collect()
    }

    pub fn node(
        &self,
        node_id: NativeExternalId,
    ) -> Result<Option<NativeGraphNode>, NativeGraphError> {
        self.graph
            .node(core_external_id(node_id)?)
            .map(graph_node)
            .transpose()
    }

    pub fn edge(&self, edge_id: NativeEdgeId) -> Result<Option<NativeGraphEdge>, NativeGraphError> {
        self.graph
            .edge(core_edge_id(edge_id)?)
            .map(graph_edge)
            .transpose()
    }

    pub fn neighbors(
        &self,
        node_id: NativeExternalId,
        direction: NativeTraversalDirection,
    ) -> Result<Vec<NativeExternalId>, NativeGraphError> {
        self.graph
            .neighbors(core_external_id(node_id)?, direction.into())?
            .iter()
            .map(native_external_id)
            .collect()
    }

    pub fn successors(
        &self,
        node_id: NativeExternalId,
    ) -> Result<Vec<NativeExternalId>, NativeGraphError> {
        self.graph
            .successors(core_external_id(node_id)?)?
            .iter()
            .map(native_external_id)
            .collect()
    }

    pub fn predecessors(
        &self,
        node_id: NativeExternalId,
    ) -> Result<Vec<NativeExternalId>, NativeGraphError> {
        self.graph
            .predecessors(core_external_id(node_id)?)?
            .iter()
            .map(native_external_id)
            .collect()
    }

    pub fn out_edge_ids(
        &self,
        node_id: NativeExternalId,
    ) -> Result<Vec<NativeEdgeId>, NativeGraphError> {
        self.graph
            .out_edge_ids(core_external_id(node_id)?)?
            .iter()
            .map(native_edge_id)
            .collect()
    }

    pub fn in_edge_ids(
        &self,
        node_id: NativeExternalId,
    ) -> Result<Vec<NativeEdgeId>, NativeGraphError> {
        self.graph
            .in_edge_ids(core_external_id(node_id)?)?
            .iter()
            .map(native_edge_id)
            .collect()
    }

    pub fn incident_edge_ids(
        &self,
        node_id: NativeExternalId,
    ) -> Result<Vec<NativeEdgeId>, NativeGraphError> {
        self.graph
            .incident_edge_ids(core_external_id(node_id)?)?
            .iter()
            .map(native_edge_id)
            .collect()
    }

    pub fn edges_between(
        &self,
        source: NativeExternalId,
        target: NativeExternalId,
        direction: NativeTraversalDirection,
    ) -> Result<Vec<NativeEdgeId>, NativeGraphError> {
        self.graph
            .edges_between(
                core_external_id(source)?,
                core_external_id(target)?,
                direction.into(),
            )?
            .iter()
            .map(native_edge_id)
            .collect()
    }

    pub fn has_edge_between(
        &self,
        source: NativeExternalId,
        target: NativeExternalId,
        direction: NativeTraversalDirection,
    ) -> Result<bool, NativeGraphError> {
        self.graph
            .has_edge_between(
                core_external_id(source)?,
                core_external_id(target)?,
                direction.into(),
            )
            .map_err(Into::into)
    }

    pub fn degree(
        &self,
        node_id: NativeExternalId,
        kind: NativeDegreeKind,
    ) -> Result<NativeNodeDegree, NativeGraphError> {
        node_degree(
            self.graph
                .degree(core_external_id(node_id)?, degree_kind(kind))?,
        )
    }

    pub fn degrees(
        &self,
        kind: NativeDegreeKind,
    ) -> Result<Vec<NativeNodeDegree>, NativeGraphError> {
        self.graph
            .degrees(degree_kind(kind))
            .into_iter()
            .map(node_degree)
            .collect()
    }

    pub fn betweenness_centrality(
        &self,
        options: NativeBetweennessOptions,
    ) -> Result<Vec<NativeNodeScore>, NativeGraphError> {
        self.graph
            .betweenness_centrality(betweenness_options(options)?)?
            .into_iter()
            .map(|score| {
                Ok(NativeNodeScore {
                    node_id: native_external_id(&score.node_id)?,
                    score: score.score,
                })
            })
            .collect()
    }

    pub fn edge_betweenness_centrality(
        &self,
        options: NativeBetweennessOptions,
    ) -> Result<Vec<NativeEdgeScore>, NativeGraphError> {
        self.graph
            .edge_betweenness_centrality(betweenness_options(options)?)?
            .into_iter()
            .map(|score| {
                Ok(NativeEdgeScore {
                    edge_id: native_edge_id(&score.edge_id)?,
                    graphify_key: score
                        .graphify_key
                        .as_ref()
                        .map(native_external_id)
                        .transpose()?,
                    source: native_external_id(&score.source)?,
                    target: native_external_id(&score.target)?,
                    score: score.score,
                })
            })
            .collect()
    }

    pub fn simple_cycles(
        &self,
        length_bound: u64,
        max_cycles: Option<u64>,
    ) -> Result<NativeCycleResult, NativeGraphError> {
        let result = self.graph.simple_cycles(core::CycleOptions {
            length_bound: non_zero(length_bound, "length_bound")?,
            max_cycles: max_cycles
                .map(|value| non_zero(value, "max_cycles"))
                .transpose()?,
        });
        let cycles = result
            .cycles
            .into_iter()
            .map(|cycle| {
                Ok(NativeCycle {
                    node_ids: cycle
                        .node_ids
                        .iter()
                        .map(native_external_id)
                        .collect::<Result<_, NativeGraphError>>()?,
                    edge_ids: cycle
                        .edge_ids
                        .iter()
                        .map(native_edge_id)
                        .collect::<Result<_, NativeGraphError>>()?,
                })
            })
            .collect::<Result<_, NativeGraphError>>()?;
        Ok(NativeCycleResult {
            cycles,
            truncated: result.truncated,
        })
    }

    pub fn traverse(
        &self,
        options: NativeTraversalOptions,
    ) -> Result<NativeTraversalResult, NativeGraphError> {
        let result = self.graph.traverse(&core::TraversalOptions {
            strategy: match options.strategy {
                NativeTraversalStrategy::BreadthFirst => core::TraversalStrategy::BreadthFirst,
                NativeTraversalStrategy::DepthFirst => core::TraversalStrategy::DepthFirst,
            },
            seeds: options
                .seeds
                .into_iter()
                .map(core_external_id)
                .collect::<Result<_, _>>()?,
            max_depth: required_usize(options.max_depth, "max_depth")?,
            direction: options.direction.into(),
            allowed_labels: options.allowed_labels.into_iter().collect(),
            hub_policy: match options.hub_policy {
                NativeHubExpansionPolicy::ExpandAll => core::HubExpansionPolicy::ExpandAll,
                NativeHubExpansionPolicy::StopNonSeedAtOrAbove { degree } => {
                    core::HubExpansionPolicy::StopNonSeedAtOrAbove {
                        degree: required_usize(degree, "hub degree")?,
                    }
                }
            },
        })?;
        Ok(NativeTraversalResult {
            visits: result
                .visits
                .into_iter()
                .map(|visit| {
                    Ok(NativeVisit {
                        node_id: native_external_id(&visit.node_id)?,
                        depth: visit.depth as u64,
                        discovery_order: visit.discovery_order as u64,
                    })
                })
                .collect::<Result<_, NativeGraphError>>()?,
            discovery_edges: result
                .discovery_edges
                .into_iter()
                .map(traversed_edge)
                .collect::<Result<_, NativeGraphError>>()?,
        })
    }

    pub fn shortest_path(
        &self,
        source: NativeExternalId,
        target: NativeExternalId,
        direction: NativeTraversalDirection,
        allowed_labels: Vec<String>,
        max_depth: Option<u64>,
    ) -> Result<NativePathResult, NativeGraphError> {
        let result = self.graph.shortest_path(
            core_external_id(source)?,
            core_external_id(target)?,
            direction.into(),
            &allowed_labels.into_iter().collect::<BTreeSet<_>>(),
            optional_usize(max_depth, "max_depth")?,
        );
        path_result(result)
    }

    pub fn louvain_communities(
        &self,
        resolution: f64,
        threshold: f64,
        seed: u64,
        max_levels: u64,
    ) -> Result<NativeCommunityResult, NativeGraphError> {
        let result = self.graph.louvain_communities(core::LouvainOptions {
            resolution: core::PositiveFiniteF64::new(resolution)?,
            threshold: core::NonNegativeFiniteF64::new(threshold)?,
            seed,
            max_levels: non_zero(max_levels, "max_levels")?,
        })?;
        Ok(NativeCommunityResult {
            communities: result
                .communities
                .into_iter()
                .map(|community| {
                    Ok(NativeCommunity {
                        id: native_external_id(&community.id)?,
                        node_ids: community
                            .node_ids
                            .iter()
                            .map(native_external_id)
                            .collect::<Result<_, NativeGraphError>>()?,
                    })
                })
                .collect::<Result<_, NativeGraphError>>()?,
            modularity: result.modularity,
            levels: result.levels as u64,
        })
    }

    pub fn leiden(
        &self,
        resolution: f64,
        randomness: f64,
        seed: u64,
        trials: u64,
        max_iterations: u64,
        max_levels: u64,
    ) -> Result<NativeLeidenResult, NativeGraphError> {
        let result = self.graph.leiden(core::LeidenOptions {
            resolution: core::PositiveFiniteF64::new(resolution)?,
            randomness: core::PositiveFiniteF64::new(randomness)?,
            seed,
            trials: non_zero(trials, "trials")?,
            max_iterations: non_zero(max_iterations, "max_iterations")?,
            max_levels: non_zero(max_levels, "max_levels")?,
        })?;
        Ok(NativeLeidenResult {
            communities: result
                .communities
                .into_iter()
                .map(|community| {
                    Ok(NativeCommunity {
                        id: native_external_id(&community.id)?,
                        node_ids: community
                            .node_ids
                            .iter()
                            .map(native_external_id)
                            .collect::<Result<_, NativeGraphError>>()?,
                    })
                })
                .collect::<Result<_, NativeGraphError>>()?,
            modularity: result.modularity,
            levels: u64::try_from(result.levels).expect("usize fits in u64"),
            winning_trial: u64::try_from(result.winning_trial).expect("usize fits in u64"),
        })
    }

    pub fn spring_layout(
        &self,
        k: Option<f64>,
        iterations: u64,
        seed: u64,
        weighted: bool,
        initial_positions: Vec<NativeNodePosition>,
    ) -> Result<Vec<NativeNodePosition>, NativeGraphError> {
        let positions = self.graph.spring_layout(core::LayoutOptions {
            k: k.map(core::PositiveFiniteF64::new).transpose()?,
            iterations: non_zero(iterations, "iterations")?,
            seed,
            weighted,
            initial_positions: initial_positions
                .into_iter()
                .map(|position| {
                    Ok(core::NodePosition {
                        node_id: core_external_id(position.node_id)?,
                        x: position.x,
                        y: position.y,
                    })
                })
                .collect::<Result<_, NativeGraphError>>()?,
        })?;
        positions
            .into_iter()
            .map(|position| {
                Ok(NativeNodePosition {
                    node_id: native_external_id(&position.node_id)?,
                    x: position.x,
                    y: position.y,
                })
            })
            .collect()
    }

    pub fn induced_subgraph(
        &self,
        node_ids: Vec<NativeExternalId>,
    ) -> Result<Arc<NativeGraph>, NativeGraphError> {
        self.graph
            .induced_subgraph(
                node_ids
                    .into_iter()
                    .map(core_external_id)
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .map(NativeGraph::new)
            .map_err(Into::into)
    }

    pub fn to_directed(&self) -> Result<Arc<NativeGraph>, NativeGraphError> {
        self.graph
            .to_directed()
            .map(NativeGraph::new)
            .map_err(Into::into)
    }

    pub fn to_undirected(&self) -> Result<Arc<NativeGraph>, NativeGraphError> {
        self.graph
            .to_undirected()
            .map(NativeGraph::new)
            .map_err(Into::into)
    }

    pub fn copy(&self) -> Arc<NativeGraph> {
        NativeGraph::new(self.graph.clone())
    }

    pub fn compose(&self, right: Arc<NativeGraph>) -> Result<Arc<NativeGraph>, NativeGraphError> {
        self.graph
            .compose(&right.graph)
            .map(NativeGraph::new)
            .map_err(Into::into)
    }

    pub fn relabel(
        &self,
        mapping: Vec<NativeRelabel>,
    ) -> Result<Arc<NativeGraph>, NativeGraphError> {
        let mut validated = BTreeMap::new();
        for entry in mapping {
            let from = core_external_id(entry.from)?;
            let to = core_external_id(entry.to)?;
            if validated.insert(from.clone(), to).is_some() {
                return Err(NativeGraphError::invalid(format!(
                    "duplicate relabel source {}",
                    from
                )));
            }
        }
        self.graph
            .relabel(&validated)
            .map(NativeGraph::new)
            .map_err(Into::into)
    }
}

fn graph_node(node: &core::Node) -> Result<NativeGraphNode, NativeGraphError> {
    Ok(NativeGraphNode {
        id: native_external_id(&node.id)?,
        label: node.label.clone(),
        attributes_json: serde_json::to_vec(&node.attributes)
            .map_err(|error| NativeGraphError::invalid(error.to_string()))?,
    })
}

fn graph_edge(edge: &core::Edge) -> Result<NativeGraphEdge, NativeGraphError> {
    Ok(NativeGraphEdge {
        id: native_edge_id(&edge.id)?,
        graphify_key: edge
            .graphify_key
            .as_ref()
            .map(native_external_id)
            .transpose()?,
        source: native_external_id(&edge.source)?,
        target: native_external_id(&edge.target)?,
        label: edge.label.clone(),
        weight: edge.weight,
        attributes_json: serde_json::to_vec(&edge.attributes)
            .map_err(|error| NativeGraphError::invalid(error.to_string()))?,
    })
}

fn betweenness_options(
    options: NativeBetweennessOptions,
) -> Result<core::BetweennessOptions, NativeGraphError> {
    let mode = match options.mode {
        NativeBetweennessMode::Exact => core::BetweennessMode::Exact,
        NativeBetweennessMode::Sampled { sample_count, seed } => core::BetweennessMode::Sampled {
            sample_count: non_zero(sample_count, "sample_count")?,
            seed,
        },
        NativeBetweennessMode::Auto {
            exact_through,
            sample_count,
            seed,
        } => core::BetweennessMode::Auto {
            exact_through: required_usize(exact_through, "exact_through")?,
            sample_count: non_zero(sample_count, "sample_count")?,
            seed,
        },
    };
    Ok(core::BetweennessOptions {
        mode,
        normalized: options.normalized,
        endpoints: options.endpoints,
        weight: if options.weighted {
            core::PathWeight::Weighted
        } else {
            core::PathWeight::Unweighted
        },
    })
}

fn degree_kind(kind: NativeDegreeKind) -> core::DegreeKind {
    match kind {
        NativeDegreeKind::In => core::DegreeKind::In,
        NativeDegreeKind::Out => core::DegreeKind::Out,
        NativeDegreeKind::Total => core::DegreeKind::Total,
    }
}

fn node_degree(degree: core::NodeDegree) -> Result<NativeNodeDegree, NativeGraphError> {
    Ok(NativeNodeDegree {
        node_id: native_external_id(&degree.node_id)?,
        degree: degree.degree as u64,
        weighted_degree: degree.weighted_degree,
    })
}

fn edge_direction(direction: core::EdgeTraversalDirection) -> NativeEdgeTraversalDirection {
    match direction {
        core::EdgeTraversalDirection::Forward => NativeEdgeTraversalDirection::Forward,
        core::EdgeTraversalDirection::Reverse => NativeEdgeTraversalDirection::Reverse,
    }
}

fn traversed_edge(edge: core::TraversedEdge) -> Result<NativeTraversedEdge, NativeGraphError> {
    Ok(NativeTraversedEdge {
        edge_id: native_edge_id(&edge.edge_id)?,
        graphify_key: edge
            .graphify_key
            .as_ref()
            .map(native_external_id)
            .transpose()?,
        source: native_external_id(&edge.source)?,
        target: native_external_id(&edge.target)?,
        traversal_direction: edge_direction(edge.traversal_direction),
        label: edge.label,
    })
}

fn path_result(result: core::PathResult) -> Result<NativePathResult, NativeGraphError> {
    Ok(match result {
        core::PathResult::MissingSource => NativePathResult::MissingSource,
        core::PathResult::MissingTarget => NativePathResult::MissingTarget,
        core::PathResult::NoPath => NativePathResult::NoPath,
        core::PathResult::Found { node_ids, edges } => NativePathResult::Found {
            node_ids: node_ids
                .iter()
                .map(native_external_id)
                .collect::<Result<_, NativeGraphError>>()?,
            edges: edges
                .into_iter()
                .map(|edge| {
                    Ok(NativePathEdge {
                        edge_id: native_edge_id(&edge.edge_id)?,
                        graphify_key: edge
                            .graphify_key
                            .as_ref()
                            .map(native_external_id)
                            .transpose()?,
                        source: native_external_id(&edge.source)?,
                        target: native_external_id(&edge.target)?,
                        traversal_direction: edge_direction(edge.traversal_direction),
                        label: edge.label,
                        attributes_json: serde_json::to_vec(&edge.attributes)
                            .map_err(|error| NativeGraphError::invalid(error.to_string()))?,
                    })
                })
                .collect::<Result<_, NativeGraphError>>()?,
        },
    })
}

fn non_zero(value: u64, name: &str) -> Result<NonZeroUsize, NativeGraphError> {
    let value = required_usize(value, name)?;
    NonZeroUsize::new(value)
        .ok_or_else(|| NativeGraphError::invalid(format!("{name} must be non-zero")))
}

fn required_usize(value: u64, name: &str) -> Result<usize, NativeGraphError> {
    usize::try_from(value)
        .map_err(|_| NativeGraphError::invalid(format!("{name} exceeds platform size")))
}

fn optional_usize(value: Option<u64>, name: &str) -> Result<Option<usize>, NativeGraphError> {
    value.map(|value| required_usize(value, name)).transpose()
}

#[cfg(test)]
mod tests {
    use helix_graph::loader::{
        EDGE_ID, EDGE_LABEL, EDGE_SOURCE, EDGE_TARGET, EXTERNAL_ID, NODE_ID, NODE_LABEL,
    };
    use serde_json::json;

    use super::*;

    fn id(value: &str) -> NativeExternalId {
        native_external_id(&core::ExternalId::from(value)).unwrap()
    }

    fn graph() -> Arc<NativeGraph> {
        let bytes = serde_json::to_vec(&json!({
            "nodes": [
                { (NODE_ID): "n1", (EXTERNAL_ID): "a", (NODE_LABEL): "File" },
                { (NODE_ID): "n2", (EXTERNAL_ID): "b", (NODE_LABEL): "File" }
            ],
            "edges": [{
                (EDGE_ID): "e1", (EDGE_SOURCE): "n1", (EDGE_TARGET): "n2",
                (EDGE_LABEL): "DEPENDS_ON"
            }]
        }))
        .expect("fixture JSON");
        graph_from_query_response(
            NativeGraphLoadSpec {
                kind: NativeGraphKind::DiGraph,
                node_identity: NativeIdentityEncoding::Scalar,
                edge_key_identity: None,
                node_limit: None,
                edge_limit: None,
            },
            bytes,
        )
        .expect("native graph")
    }

    #[test]
    fn binding_exposes_graph_access_algorithms_and_transforms() {
        let graph = graph();
        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.edge_count(), 1);
        assert_eq!(
            graph
                .neighbors(id("a"), NativeTraversalDirection::Out)
                .unwrap(),
            [id("b")]
        );
        assert!(matches!(
            graph
                .shortest_path(
                    id("a"),
                    id("b"),
                    NativeTraversalDirection::Out,
                    vec![],
                    None
                )
                .unwrap(),
            NativePathResult::Found { .. }
        ));
        let undirected = graph.to_undirected().unwrap();
        assert!(!undirected.is_directed());
        let directed = undirected.to_directed().unwrap();
        assert_eq!(directed.edge_count(), 2);
        assert!(directed
            .contains_edge(native_edge_id(&core::EdgeId::from("e1")).unwrap())
            .unwrap());
        assert!(directed
            .contains_edge(native_edge_id(&core::EdgeId::from("e1").reversed().unwrap()).unwrap())
            .unwrap());
        let leiden = undirected.leiden(1.0, 0.001, 42, 1, 100, 10).unwrap();
        assert_eq!(leiden.communities.len(), 1);
        assert_eq!(leiden.winning_trial, 0);
    }

    #[test]
    fn binding_rejects_zero_non_zero_options() {
        assert!(graph().simple_cycles(0, None).is_err());
        assert!(graph()
            .spring_layout(None, 0, 42, true, Vec::new())
            .is_err());
    }

    #[test]
    fn binding_maps_graph_kinds_and_new_typed_errors() {
        for (native, expected) in [
            (NativeGraphKind::Graph, core::GraphKind::Graph),
            (NativeGraphKind::DiGraph, core::GraphKind::DiGraph),
            (NativeGraphKind::MultiGraph, core::GraphKind::MultiGraph),
            (NativeGraphKind::MultiDiGraph, core::GraphKind::MultiDiGraph),
        ] {
            assert_eq!(core::GraphKind::from(native), expected);
        }

        for error in [
            core::GraphError::InvalidExternalId("invalid".to_string()),
            core::GraphError::ParallelEdge {
                kind: core::GraphKind::Graph,
                pair_source: core::ExternalId::from("a"),
                pair_target: core::ExternalId::from("b"),
            },
            core::GraphError::EdgeIdentityExhausted {
                stored_id: "edge".to_string(),
            },
        ] {
            assert!(matches!(
                NativeGraphError::from(error),
                NativeGraphError::InvalidGraph { .. }
            ));
        }

        assert!(matches!(
            NativeGraphError::from(core::loader::GraphLoadError::MultipleGraphMetadataRows {
                count: 2,
            }),
            NativeGraphError::MultipleGraphMetadataRows { count: 2 }
        ));
    }

    #[tokio::test]
    async fn async_binding_runs_cpu_work_on_blocking_pool() {
        let scores = graph()
            .betweenness_centrality_async(NativeBetweennessOptions {
                mode: NativeBetweennessMode::Exact,
                normalized: false,
                endpoints: false,
                weighted: false,
            })
            .await
            .unwrap();
        assert_eq!(scores.len(), 2);
    }
}
