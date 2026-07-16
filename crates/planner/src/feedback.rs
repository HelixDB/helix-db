//! Immutable runtime feedback snapshots for planner experiments.
//!
//! The planner remains pure: callers may attach a feedback snapshot to
//! [`crate::context::PlannerContext`], but optimization rules only see the
//! resulting immutable [`crate::context::StatsSnapshot`]. No planner pass reads
//! storage or runtime counters directly.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{catalog, context, ir};

/// Observed row cardinality from a previous runtime execution.
///
/// Zero is valid: a runtime observation can prove that a label or index lookup
/// currently returns no rows.
///
/// ```
/// use helix_planner::feedback::ObservedRows;
///
/// assert_eq!(ObservedRows::rows(0).as_rows(), 0);
/// assert_eq!(ObservedRows::from(42).as_rows(), 42);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ObservedRows(u64);

impl ObservedRows {
    /// Build an observed row cardinality.
    pub const fn rows(value: u64) -> Self {
        Self(value)
    }

    /// Return the observed row count.
    pub const fn as_rows(self) -> u64 {
        self.0
    }
}

impl From<u64> for ObservedRows {
    fn from(value: u64) -> Self {
        Self::rows(value)
    }
}

/// Immutable runtime cardinality feedback supplied by the caller.
///
/// ```
/// use helix_planner::feedback::{ObservedRows, RuntimeFeedbackSnapshot};
/// use helix_planner::ir::NonEmptyString;
///
/// let feedback = RuntimeFeedbackSnapshot::default()
///     .with_node_label_cardinality(
///         NonEmptyString::new("User").unwrap(),
///         ObservedRows::rows(12),
///     );
///
/// assert!(!feedback.is_empty());
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeFeedbackSnapshot {
    /// Observed node rows by label.
    pub node_label_cardinality: HashMap<ir::NonEmptyString, ObservedRows>,
    /// Observed edge rows by label.
    pub edge_label_cardinality: HashMap<ir::NonEmptyString, ObservedRows>,
    /// Observed rows produced by node equality indexes.
    pub node_eq_cardinality: HashMap<catalog::ScopedPropertyKey, ObservedRows>,
    /// Observed rows produced by node range indexes.
    pub node_range_cardinality: HashMap<catalog::ScopedPropertyDirectionKey, ObservedRows>,
    /// Observed rows produced by edge equality indexes.
    pub edge_eq_cardinality: HashMap<catalog::ScopedPropertyKey, ObservedRows>,
    /// Observed rows produced by edge range indexes.
    pub edge_range_cardinality: HashMap<catalog::ScopedPropertyDirectionKey, ObservedRows>,
}

impl RuntimeFeedbackSnapshot {
    /// Return whether this snapshot carries no feedback.
    pub fn is_empty(&self) -> bool {
        self.node_label_cardinality.is_empty()
            && self.edge_label_cardinality.is_empty()
            && self.node_eq_cardinality.is_empty()
            && self.node_range_cardinality.is_empty()
            && self.edge_eq_cardinality.is_empty()
            && self.edge_range_cardinality.is_empty()
    }

    /// Set observed node label cardinality.
    pub fn with_node_label_cardinality(
        mut self,
        label: ir::NonEmptyString,
        rows: ObservedRows,
    ) -> Self {
        self.node_label_cardinality.insert(label, rows);
        self
    }

    /// Set observed edge label cardinality.
    pub fn with_edge_label_cardinality(
        mut self,
        label: ir::NonEmptyString,
        rows: ObservedRows,
    ) -> Self {
        self.edge_label_cardinality.insert(label, rows);
        self
    }

    /// Set observed node equality-index cardinality.
    pub fn with_node_eq_cardinality(
        mut self,
        key: catalog::ScopedPropertyKey,
        rows: ObservedRows,
    ) -> Self {
        self.node_eq_cardinality.insert(key, rows);
        self
    }

    /// Set observed node range-index cardinality.
    pub fn with_node_range_cardinality(
        mut self,
        key: catalog::ScopedPropertyDirectionKey,
        rows: ObservedRows,
    ) -> Self {
        self.node_range_cardinality.insert(key, rows);
        self
    }

    /// Set observed edge equality-index cardinality.
    pub fn with_edge_eq_cardinality(
        mut self,
        key: catalog::ScopedPropertyKey,
        rows: ObservedRows,
    ) -> Self {
        self.edge_eq_cardinality.insert(key, rows);
        self
    }

    /// Set observed edge range-index cardinality.
    pub fn with_edge_range_cardinality(
        mut self,
        key: catalog::ScopedPropertyDirectionKey,
        rows: ObservedRows,
    ) -> Self {
        self.edge_range_cardinality.insert(key, rows);
        self
    }

    /// Apply feedback to a base immutable stats snapshot.
    pub fn apply_to(&self, mut stats: context::StatsSnapshot) -> context::StatsSnapshot {
        stats.node_label_cardinality.extend(
            self.node_label_cardinality
                .iter()
                .map(|(key, rows)| (key.clone(), rows.as_rows())),
        );
        stats.edge_label_cardinality.extend(
            self.edge_label_cardinality
                .iter()
                .map(|(key, rows)| (key.clone(), rows.as_rows())),
        );
        stats.node_eq_cardinality.extend(
            self.node_eq_cardinality
                .iter()
                .map(|(key, rows)| (key.clone(), rows.as_rows())),
        );
        stats.node_range_cardinality.extend(
            self.node_range_cardinality
                .iter()
                .map(|(key, rows)| (key.clone(), rows.as_rows())),
        );
        stats.edge_eq_cardinality.extend(
            self.edge_eq_cardinality
                .iter()
                .map(|(key, rows)| (key.clone(), rows.as_rows())),
        );
        stats.edge_range_cardinality.extend(
            self.edge_range_cardinality
                .iter()
                .map(|(key, rows)| (key.clone(), rows.as_rows())),
        );
        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_ast::index::RangeIndexDirection;

    #[test]
    fn runtime_feedback_overrides_base_stats() {
        let label = ir::NonEmptyString::new("User").unwrap();
        let edge_label = ir::NonEmptyString::new("FOLLOWS").unwrap();
        let node_eq = catalog::ScopedPropertyKey::try_new("User", "email").unwrap();
        let node_range =
            catalog::ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                .unwrap();
        let edge_eq = catalog::ScopedPropertyKey::try_new("FOLLOWS", "kind").unwrap();
        let edge_range = catalog::ScopedPropertyDirectionKey::try_new(
            "FOLLOWS",
            "weight",
            RangeIndexDirection::Desc,
        )
        .unwrap();

        let base = context::StatsSnapshot::default()
            .with_node_label_cardinality(label.clone(), 100)
            .with_edge_label_cardinality(edge_label.clone(), 100)
            .with_node_eq_cardinality(node_eq.clone(), 100)
            .with_node_range_cardinality(node_range.clone(), 100)
            .with_edge_eq_cardinality(edge_eq.clone(), 100)
            .with_edge_range_cardinality(edge_range.clone(), 100);
        let feedback = RuntimeFeedbackSnapshot::default()
            .with_node_label_cardinality(label.clone(), ObservedRows::rows(7))
            .with_edge_label_cardinality(edge_label.clone(), ObservedRows::rows(8))
            .with_node_eq_cardinality(node_eq.clone(), ObservedRows::rows(1))
            .with_node_range_cardinality(node_range.clone(), ObservedRows::rows(9))
            .with_edge_eq_cardinality(edge_eq.clone(), ObservedRows::rows(2))
            .with_edge_range_cardinality(edge_range.clone(), ObservedRows::rows(10));

        let stats = feedback.apply_to(base);

        assert_eq!(stats.node_label_cardinality[&label], 7);
        assert_eq!(stats.edge_label_cardinality[&edge_label], 8);
        assert_eq!(stats.node_eq_cardinality[&node_eq], 1);
        assert_eq!(stats.node_range_cardinality[&node_range], 9);
        assert_eq!(stats.edge_eq_cardinality[&edge_eq], 2);
        assert_eq!(stats.edge_range_cardinality[&edge_range], 10);
    }

    #[test]
    fn zero_row_feedback_is_valid() {
        let label = ir::NonEmptyString::new("User").unwrap();
        let stats = RuntimeFeedbackSnapshot::default()
            .with_node_label_cardinality(label.clone(), ObservedRows::rows(0))
            .apply_to(context::StatsSnapshot::default());

        assert_eq!(stats.node_label_cardinality[&label], 0);
    }
}
