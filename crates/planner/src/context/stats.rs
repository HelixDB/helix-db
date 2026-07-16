use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{catalog, ir};

/// Optional planner statistics.
///
/// Label and index cardinality builders accept validated labels or catalog
/// keys, preserving the same non-empty-name contract as index metadata.
///
/// # Examples
///
/// ```
/// use helix_planner::catalog::ScopedPropertyKey;
/// use helix_planner::context::StatsSnapshot;
/// use helix_planner::ir::NonEmptyString;
///
/// let label = NonEmptyString::new("User").unwrap();
/// let key = ScopedPropertyKey::try_new("User", "email").unwrap();
/// let stats = StatsSnapshot::default()
///     .with_node_label_cardinality(label.clone(), 100)
///     .with_node_eq_cardinality(key.clone(), 1);
///
/// assert_eq!(stats.node_label_cardinality[&label], 100);
/// assert_eq!(stats.node_eq_cardinality[&key], 1);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatsSnapshot {
    /// Estimated node rows by label.
    pub node_label_cardinality: HashMap<ir::NonEmptyString, u64>,
    /// Estimated edge rows by label.
    pub edge_label_cardinality: HashMap<ir::NonEmptyString, u64>,
    /// Estimated rows produced by node equality indexes.
    pub node_eq_cardinality: HashMap<catalog::ScopedPropertyKey, u64>,
    /// Estimated rows produced by node range indexes.
    pub node_range_cardinality: HashMap<catalog::ScopedPropertyDirectionKey, u64>,
    /// Estimated rows produced by edge equality indexes.
    pub edge_eq_cardinality: HashMap<catalog::ScopedPropertyKey, u64>,
    /// Estimated rows produced by edge range indexes.
    pub edge_range_cardinality: HashMap<catalog::ScopedPropertyDirectionKey, u64>,
}

impl StatsSnapshot {
    /// Set the estimated cardinality for a node label scan.
    pub fn with_node_label_cardinality(
        mut self,
        label: ir::NonEmptyString,
        cardinality: u64,
    ) -> Self {
        self.node_label_cardinality.insert(label, cardinality);
        self
    }

    /// Set the estimated cardinality for an edge label scan.
    pub fn with_edge_label_cardinality(
        mut self,
        label: ir::NonEmptyString,
        cardinality: u64,
    ) -> Self {
        self.edge_label_cardinality.insert(label, cardinality);
        self
    }

    /// Set the estimated cardinality for a node equality index lookup.
    pub fn with_node_eq_cardinality(
        mut self,
        key: catalog::ScopedPropertyKey,
        cardinality: u64,
    ) -> Self {
        self.node_eq_cardinality.insert(key, cardinality);
        self
    }

    /// Set the estimated cardinality for a node range index scan.
    pub fn with_node_range_cardinality(
        mut self,
        key: catalog::ScopedPropertyDirectionKey,
        cardinality: u64,
    ) -> Self {
        self.node_range_cardinality.insert(key, cardinality);
        self
    }

    /// Set the estimated cardinality for an edge equality index lookup.
    pub fn with_edge_eq_cardinality(
        mut self,
        key: catalog::ScopedPropertyKey,
        cardinality: u64,
    ) -> Self {
        self.edge_eq_cardinality.insert(key, cardinality);
        self
    }

    /// Set the estimated cardinality for an edge range index scan.
    pub fn with_edge_range_cardinality(
        mut self,
        key: catalog::ScopedPropertyDirectionKey,
        cardinality: u64,
    ) -> Self {
        self.edge_range_cardinality.insert(key, cardinality);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_ast::index::RangeIndexDirection;

    #[test]
    fn builders_populate_independent_cardinality_maps() {
        let node_label = ir::NonEmptyString::new("User").unwrap();
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

        let stats = StatsSnapshot::default()
            .with_node_label_cardinality(node_label.clone(), 10)
            .with_edge_label_cardinality(edge_label.clone(), 20)
            .with_node_eq_cardinality(node_eq.clone(), 1)
            .with_node_range_cardinality(node_range.clone(), 6)
            .with_edge_eq_cardinality(edge_eq.clone(), 2)
            .with_edge_range_cardinality(edge_range.clone(), 7);

        assert_eq!(stats.node_label_cardinality[&node_label], 10);
        assert_eq!(stats.edge_label_cardinality[&edge_label], 20);
        assert_eq!(stats.node_eq_cardinality[&node_eq], 1);
        assert_eq!(stats.node_range_cardinality[&node_range], 6);
        assert_eq!(stats.edge_eq_cardinality[&edge_eq], 2);
        assert_eq!(stats.edge_range_cardinality[&edge_range], 7);
    }
}
