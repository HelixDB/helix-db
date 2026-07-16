//! Node access-plan contracts.

mod analysis;
mod source;

use serde::{Deserialize, Serialize};

use crate::catalog;

use crate::ir;

pub use source::NodeAccessSourcePlan;

/// Node access plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeAccessPlan {
    /// Known empty node source.
    Empty,
    /// Point get concrete IDs.
    PointIds {
        /// Non-empty concrete IDs.
        ids: ir::ElementIds,
    },
    /// Runtime parameter IDs.
    FromParam {
        /// Parameter name.
        param: ir::NonEmptyString,
    },
    /// Variable node set.
    FromVar {
        /// Variable name.
        variable: ir::NonEmptyString,
    },
    /// Full node scan.
    AllScan,
    /// Label scan.
    LabelScan {
        /// Node label.
        label: ir::NonEmptyString,
    },
    /// Equality index lookup.
    EqualityIndex {
        /// Index metadata.
        index: catalog::NodeEqualityIndexMeta,
        /// Key.
        key: catalog::ScopedPropertyKey,
        /// Lookup value.
        value: ir::IndexValue,
    },
    /// Range index scan.
    RangeIndex {
        /// Index metadata.
        index: catalog::NodeRangeIndexMeta,
        /// Key.
        key: catalog::ScopedPropertyDirectionKey,
        /// Range bounds.
        range: ir::IndexRange,
    },
    /// Vector search.
    VectorSearch {
        /// Key.
        key: catalog::NodeSearchIndexKey,
        /// Search index execution plan.
        index: ir::SearchIndexPlan,
        /// Query vector.
        query_vector: ir::VectorQueryInputPlan,
        /// Result count.
        k: ir::SearchLimitPlan,
    },
    /// Text search.
    TextSearch {
        /// Key.
        key: catalog::NodeSearchIndexKey,
        /// Search index execution plan.
        index: ir::SearchIndexPlan,
        /// Query text.
        query_text: ir::TextQueryInputPlan,
        /// Result count.
        k: ir::SearchLimitPlan,
    },
    /// Set intersection. This is an unordered access contract; callers that
    /// need a stable property order must still request an explicit order plan.
    Intersect(ir::AtLeast<NodeAccessSourcePlan, 2>),
    /// Set union.
    Union(ir::AtLeast<NodeAccessSourcePlan, 2>),
    /// Residual-free candidate source with residual filtering.
    ScanThenFilter {
        /// Candidate source. This cannot itself be a filtered access plan.
        source: NodeAccessSourcePlan,
        /// Residual predicate.
        residual: ir::PredicatePlan,
    },
}

impl AsRef<NodeAccessPlan> for NodeAccessPlan {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl NodeAccessPlan {
    /// Return the direct label proven by this single node access operator.
    ///
    /// Set plans use [`NodeAccessSourcePlan::common_label`] because their
    /// common label must be derived from all child sources.
    ///
    /// ```
    /// use helix_planner::ir::{NodeAccessPlan, NonEmptyString};
    ///
    /// let label = NonEmptyString::new("User").unwrap();
    /// let scan = NodeAccessPlan::LabelScan {
    ///     label: label.clone(),
    /// };
    ///
    /// assert_eq!(scan.direct_label(), Some(&label));
    /// assert!(NodeAccessPlan::AllScan.direct_label().is_none());
    /// ```
    pub fn direct_label(&self) -> Option<&ir::NonEmptyString> {
        analysis::direct_label(self)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use helix_ast::value::{PropertyInput, PropertyValue};

    use super::*;

    fn source(plan: NodeAccessPlan) -> NodeAccessSourcePlan {
        NodeAccessSourcePlan::from_unfiltered(plan)
    }

    fn ids(values: Vec<u64>) -> ir::ElementIds {
        ir::ElementIds::new(ir::AtLeast::<_, 1>::try_from_vec(values).unwrap()).unwrap()
    }

    fn equality_value() -> ir::IndexValue {
        ir::IndexValue::Literal(
            ir::SecondaryIndexLiteral::new(PropertyValue::from("user@example.test")).unwrap(),
        )
    }

    fn literal_limit(value: usize) -> ir::SearchLimitPlan {
        ir::SearchLimitPlan::Literal(NonZeroUsize::new(value).unwrap())
    }

    fn vector_search(k: ir::SearchLimitPlan) -> NodeAccessPlan {
        NodeAccessPlan::VectorSearch {
            key: catalog::NodeSearchIndexKey::try_new("Doc", "embedding").unwrap(),
            index: ir::SearchIndexPlan {
                index_id: ir::NonEmptyString::new("doc_embedding").unwrap(),
                tenant: ir::SearchTenantPlan::Unscoped,
            },
            query_vector: ir::VectorQueryInputPlan::new(PropertyInput::from(
                PropertyValue::F32Array(vec![0.1]),
            ))
            .unwrap(),
            k,
        }
    }

    fn label_scan(label: &str) -> NodeAccessSourcePlan {
        source(NodeAccessPlan::LabelScan {
            label: ir::NonEmptyString::new(label).unwrap(),
        })
    }

    #[test]
    fn node_access_direct_label_covers_label_index_and_search_sources() {
        let user = ir::NonEmptyString::new("User").unwrap();

        for plan in [
            NodeAccessPlan::LabelScan {
                label: user.clone(),
            },
            NodeAccessPlan::EqualityIndex {
                index: catalog::NodeEqualityIndexMeta::try_new("user_email").unwrap(),
                key: catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
                value: equality_value(),
            },
            NodeAccessPlan::RangeIndex {
                index: catalog::NodeRangeIndexMeta::try_new("user_age").unwrap(),
                key: catalog::ScopedPropertyDirectionKey::try_new(
                    "User",
                    "age",
                    helix_ast::index::RangeIndexDirection::Asc,
                )
                .unwrap(),
                range: ir::IndexRange::All,
            },
            NodeAccessPlan::VectorSearch {
                key: catalog::NodeSearchIndexKey::try_new("User", "embedding").unwrap(),
                index: ir::SearchIndexPlan {
                    index_id: ir::NonEmptyString::new("user_embedding").unwrap(),
                    tenant: ir::SearchTenantPlan::Unscoped,
                },
                query_vector: ir::VectorQueryInputPlan::new(PropertyInput::from(
                    PropertyValue::F32Array(vec![0.1]),
                ))
                .unwrap(),
                k: literal_limit(3),
            },
            NodeAccessPlan::TextSearch {
                key: catalog::NodeSearchIndexKey::try_new("User", "bio").unwrap(),
                index: ir::SearchIndexPlan {
                    index_id: ir::NonEmptyString::new("user_bio").unwrap(),
                    tenant: ir::SearchTenantPlan::Unscoped,
                },
                query_text: ir::TextQueryInputPlan::new(PropertyInput::from("hello")).unwrap(),
                k: literal_limit(3),
            },
        ] {
            assert_eq!(plan.direct_label(), Some(&user));
        }

        assert!(NodeAccessPlan::AllScan.direct_label().is_none());
    }

    #[test]
    fn node_source_common_label_requires_every_set_branch_to_match() {
        let user = ir::NonEmptyString::new("User").unwrap();
        let homogeneous = source(NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            source(NodeAccessPlan::LabelScan {
                label: user.clone(),
            }),
            source(NodeAccessPlan::EqualityIndex {
                index: catalog::NodeEqualityIndexMeta::try_new("user_email").unwrap(),
                key: catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
                value: equality_value(),
            }),
        )));
        let mixed = source(NodeAccessPlan::Intersect(ir::AtLeast::<_, 2>::from_pair(
            source(NodeAccessPlan::LabelScan {
                label: user.clone(),
            }),
            source(NodeAccessPlan::LabelScan {
                label: ir::NonEmptyString::new("Account").unwrap(),
            }),
        )));
        let unscoped = source(NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            source(NodeAccessPlan::LabelScan {
                label: user.clone(),
            }),
            source(NodeAccessPlan::AllScan),
        )));

        assert_eq!(homogeneous.common_label(), Some(&user));
        assert!(mixed.common_label().is_none());
        assert!(unscoped.common_label().is_none());
    }

    #[test]
    fn node_source_hard_cardinality_upper_bound_covers_static_sources() {
        assert_eq!(
            source(NodeAccessPlan::Empty).hard_cardinality_upper_bound(),
            Some(0)
        );
        assert_eq!(
            source(NodeAccessPlan::PointIds {
                ids: ids(vec![1, 2, 3])
            })
            .hard_cardinality_upper_bound(),
            Some(3)
        );
        assert_eq!(
            source(NodeAccessPlan::EqualityIndex {
                index: catalog::NodeEqualityIndexMeta::try_new("user_email")
                    .unwrap()
                    .with_uniqueness(catalog::IndexUniqueness::Unique),
                key: catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
                value: equality_value(),
            })
            .hard_cardinality_upper_bound(),
            Some(1)
        );
        assert_eq!(
            source(vector_search(literal_limit(4))).hard_cardinality_upper_bound(),
            Some(4)
        );
    }

    #[test]
    fn node_source_hard_cardinality_upper_bound_preserves_unknown_sources() {
        let dynamic_limit = ir::SearchLimitPlan::Expr(
            ir::SearchLimitExprPlan::new(helix_ast::expr::Expr::param("k")).unwrap(),
        );

        assert_eq!(
            source(NodeAccessPlan::EqualityIndex {
                index: catalog::NodeEqualityIndexMeta::try_new("user_email").unwrap(),
                key: catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
                value: equality_value(),
            })
            .hard_cardinality_upper_bound(),
            None
        );
        assert_eq!(
            source(vector_search(dynamic_limit)).hard_cardinality_upper_bound(),
            None
        );
        assert_eq!(
            source(NodeAccessPlan::LabelScan {
                label: ir::NonEmptyString::new("User").unwrap()
            })
            .hard_cardinality_upper_bound(),
            None
        );
    }

    #[test]
    fn node_source_hard_cardinality_upper_bound_composes_sets() {
        let two = source(NodeAccessPlan::PointIds {
            ids: ids(vec![1, 2]),
        });
        let one = source(vector_search(literal_limit(1)));
        let scan = source(NodeAccessPlan::AllScan);

        assert_eq!(
            source(NodeAccessPlan::Intersect(ir::AtLeast::<_, 2>::from_pair(
                two.clone(),
                scan.clone(),
            )))
            .hard_cardinality_upper_bound(),
            Some(2)
        );
        assert_eq!(
            source(NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
                two.clone(),
                one,
            )))
            .hard_cardinality_upper_bound(),
            Some(3)
        );
        assert_eq!(
            source(NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
                two, scan,
            )))
            .hard_cardinality_upper_bound(),
            None
        );
    }

    #[test]
    fn node_source_set_canonicalization_candidates_track_rewrite_shapes() {
        let user = label_scan("User");
        let duplicate = source(NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            user.clone(),
            user.clone(),
        )));
        let nested = source(NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            user.clone(),
            source(NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
                label_scan("Account"),
                label_scan("Org"),
            ))),
        )));
        let empty_intersection = source(NodeAccessPlan::Intersect(ir::AtLeast::<_, 2>::from_pair(
            source(NodeAccessPlan::Empty),
            user.clone(),
        )));
        let ordinary = source(NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            user,
            label_scan("Account"),
        )));

        assert!(duplicate.has_set_canonicalization_candidate());
        assert!(nested.has_set_canonicalization_candidate());
        assert!(empty_intersection.has_set_canonicalization_candidate());
        assert!(!ordinary.has_set_canonicalization_candidate());
        assert!(!source(NodeAccessPlan::AllScan).has_set_canonicalization_candidate());
    }

    #[test]
    fn node_source_subsumption_candidates_use_source_partial_order() {
        let all = source(NodeAccessPlan::AllScan);
        let label = label_scan("User");
        let equality = source(NodeAccessPlan::EqualityIndex {
            index: catalog::NodeEqualityIndexMeta::try_new("user_email").unwrap(),
            key: catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
            value: equality_value(),
        });
        let union = source(NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            all.clone(),
            label.clone(),
        )));
        let intersection = source(NodeAccessPlan::Intersect(ir::AtLeast::<_, 2>::from_pair(
            label.clone(),
            equality.clone(),
        )));
        let ordinary = source(NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            label_scan("User"),
            label_scan("Account"),
        )));

        assert!(all.subsumes(&label));
        assert!(label.subsumes(&equality));
        assert!(union.has_set_subsumption_candidate());
        assert!(intersection.has_set_subsumption_candidate());
        assert!(!ordinary.has_set_subsumption_candidate());
    }
}
