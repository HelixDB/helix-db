//! Residual-free access path contracts.

use serde::{Deserialize, Serialize};

use crate::{ir, properties};

mod source_kind;

pub use source_kind::AccessSourceKind;

/// Residual-free node or edge access candidate.
///
/// The node and edge variants wrap source-only access plans, so a residual
/// filter cannot be hidden inside an access-path implementation rule.
///
/// ```
/// use helix_planner::ir::{NodeAccessPlan, NodeAccessSourcePlan};
/// use helix_planner::logical::{AccessPath, NodeAccessPath};
/// use helix_planner::properties::ElementKind;
///
/// let source = NodeAccessSourcePlan::new(NodeAccessPlan::AllScan).unwrap();
/// let access = AccessPath::Node(NodeAccessPath::new(source));
///
/// assert_eq!(access.element(), ElementKind::Node);
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessPath {
    /// Node-producing access path.
    Node(NodeAccessPath),
    /// Edge-producing access path.
    Edge(EdgeAccessPath),
}

impl AccessPath {
    /// Element kind produced by this access path.
    pub const fn element(&self) -> properties::ElementKind {
        match self {
            Self::Node(_) => properties::ElementKind::Node,
            Self::Edge(_) => properties::ElementKind::Edge,
        }
    }

    /// Top-level source family used by optimizer rule scheduling.
    pub fn source_kind(&self) -> AccessSourceKind {
        match self {
            Self::Node(path) => source_kind::classify_node(path.source().as_ref()),
            Self::Edge(path) => source_kind::classify_edge(path.source().as_ref()),
        }
    }

    /// Whether this path is exactly a typed empty node/edge source.
    ///
    /// This does not inspect set children. Set simplification rules own
    /// recursive empty-set normalization.
    ///
    /// ```
    /// use helix_planner::ir::{NodeAccessPlan, NodeAccessSourcePlan};
    /// use helix_planner::logical::{AccessPath, NodeAccessPath};
    ///
    /// let source = NodeAccessSourcePlan::new(NodeAccessPlan::Empty).unwrap();
    /// let access = AccessPath::Node(NodeAccessPath::new(source));
    ///
    /// assert!(access.is_direct_empty());
    /// ```
    pub fn is_direct_empty(&self) -> bool {
        self.source_kind() == AccessSourceKind::Empty
    }

    /// Whether access-set canonicalization can rewrite this path.
    ///
    /// ```
    /// use helix_planner::ir::{AtLeast, NodeAccessPlan, NodeAccessSourcePlan};
    /// use helix_planner::logical::{AccessPath, NodeAccessPath};
    ///
    /// let empty = NodeAccessSourcePlan::new(NodeAccessPlan::Empty).unwrap();
    /// let scan = NodeAccessSourcePlan::new(NodeAccessPlan::AllScan).unwrap();
    /// let access = AccessPath::Node(NodeAccessPath::new(
    ///     NodeAccessSourcePlan::new(NodeAccessPlan::Union(
    ///         AtLeast::<_, 2>::from_pair(empty, scan),
    ///     )).unwrap(),
    /// ));
    ///
    /// assert!(access.has_set_canonicalization_candidate());
    /// ```
    pub fn has_set_canonicalization_candidate(&self) -> bool {
        match self {
            Self::Node(path) => path.source().has_set_canonicalization_candidate(),
            Self::Edge(path) => path.source().has_set_canonicalization_candidate(),
        }
    }

    /// Whether access-source subsumption can remove a redundant set child.
    ///
    /// ```
    /// use helix_planner::ir::{AtLeast, NodeAccessPlan, NodeAccessSourcePlan, NonEmptyString};
    /// use helix_planner::logical::{AccessPath, NodeAccessPath};
    ///
    /// let all = NodeAccessSourcePlan::new(NodeAccessPlan::AllScan).unwrap();
    /// let users = NodeAccessSourcePlan::new(NodeAccessPlan::LabelScan {
    ///     label: NonEmptyString::new("User").unwrap(),
    /// }).unwrap();
    /// let access = AccessPath::Node(NodeAccessPath::new(
    ///     NodeAccessSourcePlan::new(NodeAccessPlan::Union(
    ///         AtLeast::<_, 2>::from_pair(all, users),
    ///     )).unwrap(),
    /// ));
    ///
    /// assert!(access.has_set_subsumption_candidate());
    /// ```
    pub fn has_set_subsumption_candidate(&self) -> bool {
        match self {
            Self::Node(path) => path.source().has_set_subsumption_candidate(),
            Self::Edge(path) => path.source().has_set_subsumption_candidate(),
        }
    }

    /// Build a typed empty access path with the same element family.
    ///
    /// ```
    /// use helix_planner::ir::{EdgeAccessPlan, EdgeAccessSourcePlan};
    /// use helix_planner::logical::{AccessPath, EdgeAccessPath};
    /// use helix_planner::properties::ElementKind;
    ///
    /// let source = EdgeAccessSourcePlan::new(EdgeAccessPlan::AllScan).unwrap();
    /// let access = AccessPath::Edge(EdgeAccessPath::new(source));
    /// let empty = access.empty_like();
    ///
    /// assert_eq!(empty.element(), ElementKind::Edge);
    /// assert!(empty.is_direct_empty());
    /// ```
    pub fn empty_like(&self) -> Self {
        match self {
            Self::Node(_) => Self::Node(NodeAccessPath::new(
                ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::Empty),
            )),
            Self::Edge(_) => Self::Edge(EdgeAccessPath::new(
                ir::EdgeAccessSourcePlan::from_unfiltered(ir::EdgeAccessPlan::Empty),
            )),
        }
    }

    /// Return a proven hard upper cardinality bound for this access path.
    ///
    /// ```
    /// use helix_planner::ir::{AtLeast, ElementIds, NodeAccessPlan, NodeAccessSourcePlan};
    /// use helix_planner::logical::{AccessPath, NodeAccessPath};
    ///
    /// let ids = ElementIds::new(AtLeast::<_, 1>::from_one_and_rest(7, vec![9])).unwrap();
    /// let source = NodeAccessSourcePlan::new(NodeAccessPlan::PointIds { ids }).unwrap();
    /// let access = AccessPath::Node(NodeAccessPath::new(source));
    ///
    /// assert_eq!(access.hard_cardinality_upper_bound(), Some(2));
    /// ```
    pub fn hard_cardinality_upper_bound(&self) -> Option<usize> {
        match self {
            Self::Node(path) => path.source().hard_cardinality_upper_bound(),
            Self::Edge(path) => path.source().hard_cardinality_upper_bound(),
        }
    }

    /// Return the label common to every residual-free branch of this access path.
    ///
    /// ```
    /// use helix_planner::ir::{NodeAccessPlan, NodeAccessSourcePlan, NonEmptyString};
    /// use helix_planner::logical::{AccessPath, NodeAccessPath};
    ///
    /// let label = NonEmptyString::new("User").unwrap();
    /// let source = NodeAccessSourcePlan::new(NodeAccessPlan::LabelScan {
    ///     label: label.clone(),
    /// }).unwrap();
    /// let access = AccessPath::Node(NodeAccessPath::new(source));
    ///
    /// assert_eq!(access.common_label(), Some(&label));
    /// ```
    pub fn common_label(&self) -> Option<&ir::NonEmptyString> {
        match self {
            Self::Node(path) => path.common_label(),
            Self::Edge(path) => path.common_label(),
        }
    }
}

/// Residual-free node access path.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeAccessPath {
    source: ir::NodeAccessSourcePlan,
}

impl NodeAccessPath {
    /// Build a residual-free node access path.
    pub fn new(source: ir::NodeAccessSourcePlan) -> Self {
        Self { source }
    }

    /// Source access plan.
    pub fn source(&self) -> &ir::NodeAccessSourcePlan {
        &self.source
    }

    /// Return the label common to every residual-free branch of this node path.
    pub fn common_label(&self) -> Option<&ir::NonEmptyString> {
        self.source.common_label()
    }
}

/// Residual-free edge access path.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeAccessPath {
    source: ir::EdgeAccessSourcePlan,
}

impl EdgeAccessPath {
    /// Build a residual-free edge access path.
    pub fn new(source: ir::EdgeAccessSourcePlan) -> Self {
        Self { source }
    }

    /// Source access plan.
    pub fn source(&self) -> &ir::EdgeAccessSourcePlan {
        &self.source
    }

    /// Return the label common to every residual-free branch of this edge path.
    pub fn common_label(&self) -> Option<&ir::NonEmptyString> {
        self.source.common_label()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node_source(plan: ir::NodeAccessPlan) -> AccessPath {
        AccessPath::Node(NodeAccessPath::new(
            ir::NodeAccessSourcePlan::from_unfiltered(plan),
        ))
    }

    fn edge_source(plan: ir::EdgeAccessPlan) -> AccessPath {
        AccessPath::Edge(EdgeAccessPath::new(
            ir::EdgeAccessSourcePlan::from_unfiltered(plan),
        ))
    }

    #[test]
    fn access_path_hard_cardinality_upper_bound_delegates_to_element_source() {
        let point_ids =
            ir::ElementIds::new(ir::AtLeast::<_, 1>::from_one_and_rest(7, vec![9])).unwrap();
        let node = node_source(ir::NodeAccessPlan::PointIds { ids: point_ids });
        let edge = edge_source(ir::EdgeAccessPlan::AllScan);

        assert_eq!(node.hard_cardinality_upper_bound(), Some(2));
        assert_eq!(edge.hard_cardinality_upper_bound(), None);
    }

    #[test]
    fn access_path_direct_empty_and_empty_like_preserve_element_family() {
        let node = node_source(ir::NodeAccessPlan::AllScan);
        let empty_node = node.empty_like();
        let edge = edge_source(ir::EdgeAccessPlan::Empty);
        let empty_edge = edge.empty_like();

        assert!(!node.is_direct_empty());
        assert!(empty_node.is_direct_empty());
        assert_eq!(empty_node.element(), properties::ElementKind::Node);
        assert!(edge.is_direct_empty());
        assert!(empty_edge.is_direct_empty());
        assert_eq!(empty_edge.element(), properties::ElementKind::Edge);
    }
}
