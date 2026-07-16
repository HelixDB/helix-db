//! Unary access wrappers over residual-free access paths.

use serde::{Deserialize, Serialize};

use super::{AccessPath, AccessWindowRange};
use crate::ir;

mod candidates;

/// A static access-window over a residual-free access path.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccessWindow {
    access: AccessPath,
    window: AccessWindowRange,
}

impl AccessWindow {
    /// Build an access-window candidate.
    pub fn new(access: AccessPath, window: AccessWindowRange) -> Self {
        Self { access, window }
    }

    /// Access path being windowed.
    pub const fn access(&self) -> &AccessPath {
        &self.access
    }

    /// Static window.
    pub const fn window(&self) -> AccessWindowRange {
        self.window
    }

    /// Whether an access-window exploration rule can rewrite this expression.
    ///
    /// The predicate is conservative: it may return true for cheap false
    /// positives, but every currently supported fold or search-prefix
    /// tightening must return true here so the compiled optimizer schedule can
    /// skip impossible rewrite probes.
    ///
    /// ```
    /// use helix_planner::ir::{AtLeast, ElementIds, NodeAccessPlan, NodeAccessSourcePlan};
    /// use helix_planner::logical::{
    ///     AccessPath, AccessWindow, AccessWindowRange, NodeAccessPath,
    /// };
    ///
    /// let ids = ElementIds::new(AtLeast::<_, 1>::from_one_and_rest(7, vec![9])).unwrap();
    /// let source = NodeAccessSourcePlan::new(NodeAccessPlan::PointIds { ids }).unwrap();
    /// let access = AccessPath::Node(NodeAccessPath::new(source));
    /// let window = AccessWindow::new(access, AccessWindowRange::new(1, Some(2)).unwrap());
    ///
    /// assert!(window.has_rewrite_candidate());
    /// ```
    pub fn has_rewrite_candidate(&self) -> bool {
        candidates::window_has_rewrite_candidate(self)
    }
}

/// A statically known access-order rewrite candidate.
///
/// The ordering is represented by [`ir::OrderKeys`], so the candidate cannot
/// encode `Any` or an empty order request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccessOrder {
    access: AccessPath,
    ordering: ir::OrderKeys,
}

impl AccessOrder {
    /// Build an access-order candidate.
    pub fn new(access: AccessPath, ordering: ir::OrderKeys) -> Self {
        Self { access, ordering }
    }

    /// Access path being ordered.
    pub const fn access(&self) -> &AccessPath {
        &self.access
    }

    /// Required non-empty ordering.
    pub const fn ordering(&self) -> &ir::OrderKeys {
        &self.ordering
    }

    /// Whether access-order exploration can elide this explicit ordering.
    ///
    /// The predicate may include cheap false positives, but it must include
    /// every case where the access path already proves the requested order or
    /// the result cardinality makes ordering irrelevant.
    ///
    /// ```
    /// use helix_ast::{index::RangeIndexDirection, traversal::Order};
    /// use helix_planner::{catalog, ir, logical};
    ///
    /// let source = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::RangeIndex {
    ///     index: catalog::NodeRangeIndexMeta::try_new("node_range").unwrap(),
    ///     key: catalog::ScopedPropertyDirectionKey::try_new(
    ///         "User",
    ///         "age",
    ///         RangeIndexDirection::Asc,
    ///     ).unwrap(),
    ///     range: ir::IndexRange::All,
    /// }).unwrap();
    /// let access = logical::AccessPath::Node(logical::NodeAccessPath::new(source));
    /// let ordering = ir::OrderKeys::from(ir::OrderKey {
    ///     property: ir::NonEmptyString::new("age").unwrap(),
    ///     order: Order::Asc,
    /// });
    /// let order = logical::AccessOrder::new(access, ordering);
    ///
    /// assert!(order.has_order_elision_candidate());
    /// ```
    pub fn has_order_elision_candidate(&self) -> bool {
        candidates::order_has_order_elision_candidate(self)
    }

    /// Whether range-direction exploration can switch this order to another
    /// catalog-backed range index.
    ///
    /// This is catalog-independent: it proves only that the logical source and
    /// requested order are shaped such that an opposite-direction index lookup
    /// might be useful.
    ///
    /// ```
    /// use helix_ast::{index::RangeIndexDirection, traversal::Order};
    /// use helix_planner::{catalog, ir, logical};
    ///
    /// let source = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::RangeIndex {
    ///     index: catalog::NodeRangeIndexMeta::try_new("node_range").unwrap(),
    ///     key: catalog::ScopedPropertyDirectionKey::try_new(
    ///         "User",
    ///         "age",
    ///         RangeIndexDirection::Asc,
    ///     ).unwrap(),
    ///     range: ir::IndexRange::All,
    /// }).unwrap();
    /// let access = logical::AccessPath::Node(logical::NodeAccessPath::new(source));
    /// let ordering = ir::OrderKeys::from(ir::OrderKey {
    ///     property: ir::NonEmptyString::new("age").unwrap(),
    ///     order: Order::Desc,
    /// });
    /// let order = logical::AccessOrder::new(access, ordering);
    ///
    /// assert!(order.has_range_direction_candidate());
    /// ```
    pub fn has_range_direction_candidate(&self) -> bool {
        candidates::order_has_range_direction_candidate(self)
    }
}

/// A statically known access-distinct rewrite candidate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccessDistinct {
    access: AccessPath,
}

impl AccessDistinct {
    /// Build an access-distinct candidate.
    pub fn new(access: AccessPath) -> Self {
        Self { access }
    }

    /// Access path being deduplicated.
    pub const fn access(&self) -> &AccessPath {
        &self.access
    }

    /// Whether access-distinct exploration can prove this distinct is a no-op.
    ///
    /// ```
    /// use helix_planner::ir::{AtLeast, ElementIds, NodeAccessPlan, NodeAccessSourcePlan};
    /// use helix_planner::logical::{AccessDistinct, AccessPath, NodeAccessPath};
    ///
    /// let ids = ElementIds::new(AtLeast::<_, 1>::from_one_and_rest(7, vec![9])).unwrap();
    /// let source = NodeAccessSourcePlan::new(NodeAccessPlan::PointIds { ids }).unwrap();
    /// let access = AccessPath::Node(NodeAccessPath::new(source));
    /// let distinct = AccessDistinct::new(access);
    ///
    /// assert!(distinct.has_noop_candidate());
    /// ```
    pub fn has_noop_candidate(&self) -> bool {
        candidates::distinct_has_noop_candidate(self)
    }
}

/// A statically known residual filter over a residual-free access path.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccessFilter {
    access: AccessPath,
    predicate: ir::PredicatePlan,
}

impl AccessFilter {
    /// Build an access-filter candidate.
    pub fn new(access: AccessPath, predicate: ir::PredicatePlan) -> Self {
        Self { access, predicate }
    }

    /// Access path being filtered.
    pub const fn access(&self) -> &AccessPath {
        &self.access
    }

    /// Residual predicate applied to the access path.
    pub const fn predicate(&self) -> &ir::PredicatePlan {
        &self.predicate
    }
}
