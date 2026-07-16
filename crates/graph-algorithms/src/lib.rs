//! Storage-independent graph representation and native graph algorithms.
//!
//! The crate deliberately has no dependency on Helix storage, query planning,
//! async runtimes, or SDK bindings. SDKs query Helix once, construct a
//! validated [`Graph`], and reuse that immutable graph for any number of local
//! algorithms.
//!
//! ```
//! use helix_graph::{Edge, Graph, GraphKind, Node};
//!
//! let graph = Graph::new(
//!     GraphKind::DiGraph,
//!     [Node::new("a"), Node::new("b")],
//!     [Edge::new("ab", "a", "b")],
//! )?;
//! assert_eq!(graph.node_count(), 2);
//! assert_eq!(graph.edge_count(), 1);
//! # Ok::<(), helix_graph::GraphError>(())
//! ```

mod algorithms;
mod identity;
pub mod loader;
mod model;
mod transform;

pub use algorithms::{
    BetweennessMode, BetweennessOptions, Community, CommunityResult, Cycle, CycleOptions,
    CycleResult, DegreeKind, EdgeScore, EdgeTraversalDirection, HubExpansionPolicy, LayoutOptions,
    LeidenOptions, LeidenResult, LouvainOptions, NodeDegree, NodePosition, NodeScore, PathEdge,
    PathResult, PathWeight, TraversalDirection, TraversalOptions, TraversalResult,
    TraversalStrategy, TraversedEdge, Visit,
};
pub use identity::{ExternalId, GraphProperty, IdentitySelection};
pub use model::{
    Attributes, Edge, EdgeId, Graph, GraphError, GraphKind, Node, NodeId, NonNegativeFiniteF64,
    PositiveFiniteF64,
};
