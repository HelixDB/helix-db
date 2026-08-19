//! Edge access-plan contracts.

mod analysis;
mod source;

use serde::{Deserialize, Serialize};

use crate::catalog;

use crate::ir;

pub use source::EdgeAccessSourcePlan;

/// Edge access plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeAccessPlan {
    /// Known empty edge source.
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
    /// Variable edge set.
    FromVar {
        /// Variable name.
        variable: ir::NonEmptyString,
    },
    /// Full edge scan.
    AllScan,
    /// Label scan.
    LabelScan {
        /// Edge label.
        label: ir::NonEmptyString,
    },
    /// Equality index lookup.
    EqualityIndex {
        /// Index metadata.
        index: catalog::EdgeEqualityIndexMeta,
        /// Key.
        key: catalog::ScopedPropertyKey,
        /// Lookup value.
        value: ir::IndexValue,
    },
    /// Range index scan.
    RangeIndex {
        /// Index metadata.
        index: catalog::EdgeRangeIndexMeta,
        /// Key.
        key: catalog::ScopedPropertyDirectionKey,
        /// Range bounds.
        range: ir::IndexRange,
    },
    /// Vector search.
    VectorSearch {
        /// Key.
        key: catalog::EdgeSearchIndexKey,
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
        key: catalog::EdgeSearchIndexKey,
        /// Search index execution plan.
        index: ir::SearchIndexPlan,
        /// Query text.
        query_text: ir::TextQueryInputPlan,
        /// Result count.
        k: ir::SearchLimitPlan,
    },
    /// Set intersection. A pure-secondary intersection is ordered only when a
    /// direct range child is selected as its executable driver; otherwise it
    /// is unordered.
    Intersect(ir::AtLeast<EdgeAccessSourcePlan, 2>),
    /// Set union.
    Union(ir::AtLeast<EdgeAccessSourcePlan, 2>),
    /// Residual-free candidate source with residual filtering.
    ScanThenFilter {
        /// Candidate source. This cannot itself be a filtered access plan.
        source: EdgeAccessSourcePlan,
        /// Residual predicate.
        residual: ir::PredicatePlan,
    },
}

impl AsRef<EdgeAccessPlan> for EdgeAccessPlan {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl EdgeAccessPlan {
    /// Whether every leaf can participate in one executable secondary ID set.
    pub(crate) fn is_secondary_set_eligible(&self) -> bool {
        analysis::secondary_set_eligible(self)
    }

    /// Return the direct label proven by this single edge access operator.
    ///
    /// Set plans use [`EdgeAccessSourcePlan::common_label`] because their
    /// common label must be derived from all child sources.
    ///
    /// ```
    /// use helix_planner::ir::{EdgeAccessPlan, NonEmptyString};
    ///
    /// let label = NonEmptyString::new("LIKES").unwrap();
    /// let scan = EdgeAccessPlan::LabelScan {
    ///     label: label.clone(),
    /// };
    ///
    /// assert_eq!(scan.direct_label(), Some(&label));
    /// assert!(EdgeAccessPlan::AllScan.direct_label().is_none());
    /// ```
    pub fn direct_label(&self) -> Option<&ir::NonEmptyString> {
        analysis::direct_label(self)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use helix_ast::value::PropertyInput;

    use super::*;

    fn source(plan: EdgeAccessPlan) -> EdgeAccessSourcePlan {
        EdgeAccessSourcePlan::from_unfiltered(plan)
    }

    fn ids(values: Vec<u64>) -> ir::ElementIds {
        ir::ElementIds::new(ir::AtLeast::<_, 1>::try_from_vec(values).unwrap()).unwrap()
    }

    fn literal_limit(value: usize) -> ir::SearchLimitPlan {
        ir::SearchLimitPlan::Literal(NonZeroUsize::new(value).unwrap())
    }

    fn text_search(k: ir::SearchLimitPlan) -> EdgeAccessPlan {
        EdgeAccessPlan::TextSearch {
            key: catalog::EdgeSearchIndexKey::try_new("MENTIONS", "body").unwrap(),
            index: ir::SearchIndexPlan {
                index_id: ir::NonEmptyString::new("mentions_body").unwrap(),
                tenant: ir::SearchTenantPlan::Unscoped,
            },
            query_text: ir::TextQueryInputPlan::new(PropertyInput::from("needle")).unwrap(),
            k,
        }
    }

    fn label_scan(label: &str) -> EdgeAccessSourcePlan {
        source(EdgeAccessPlan::LabelScan {
            label: ir::NonEmptyString::new(label).unwrap(),
        })
    }

    #[test]
    fn secondary_set_eligibility_covers_nested_and_mixed_edge_trees() {
        let range = EdgeAccessPlan::RangeIndex {
            index: catalog::EdgeRangeIndexMeta::try_new("likes_weight").unwrap(),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "LIKES",
                "weight",
                helix_ast::index::RangeIndexDirection::Desc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        };
        let nested = EdgeAccessPlan::Intersect(ir::AtLeast::from_pair(
            source(EdgeAccessPlan::Empty),
            source(EdgeAccessPlan::Union(ir::AtLeast::from_pair(
                source(EdgeAccessPlan::Empty),
                source(range.clone()),
            ))),
        ));
        let mixed = EdgeAccessPlan::Intersect(ir::AtLeast::from_pair(
            source(range),
            source(EdgeAccessPlan::AllScan),
        ));

        assert!(nested.is_secondary_set_eligible());
        assert!(!mixed.is_secondary_set_eligible());
    }

    #[test]
    fn edge_access_direct_label_covers_label_index_and_search_sources() {
        let likes = ir::NonEmptyString::new("LIKES").unwrap();

        for plan in [
            EdgeAccessPlan::LabelScan {
                label: likes.clone(),
            },
            EdgeAccessPlan::EqualityIndex {
                index: catalog::EdgeEqualityIndexMeta::try_new("likes_weight").unwrap(),
                key: catalog::ScopedPropertyKey::try_new("LIKES", "weight").unwrap(),
                value: ir::IndexValue::Literal(
                    ir::SecondaryIndexLiteral::new(helix_ast::value::PropertyValue::from(1))
                        .unwrap(),
                ),
            },
            EdgeAccessPlan::RangeIndex {
                index: catalog::EdgeRangeIndexMeta::try_new("likes_created").unwrap(),
                key: catalog::ScopedPropertyDirectionKey::try_new(
                    "LIKES",
                    "created",
                    helix_ast::index::RangeIndexDirection::Desc,
                )
                .unwrap(),
                range: ir::IndexRange::All,
            },
            EdgeAccessPlan::VectorSearch {
                key: catalog::EdgeSearchIndexKey::try_new("LIKES", "embedding").unwrap(),
                index: ir::SearchIndexPlan {
                    index_id: ir::NonEmptyString::new("likes_embedding").unwrap(),
                    tenant: ir::SearchTenantPlan::Unscoped,
                },
                query_vector: ir::VectorQueryInputPlan::new(helix_ast::value::PropertyInput::from(
                    helix_ast::value::PropertyValue::F32Array(vec![0.1]),
                ))
                .unwrap(),
                k: literal_limit(3),
            },
            EdgeAccessPlan::TextSearch {
                key: catalog::EdgeSearchIndexKey::try_new("LIKES", "comment").unwrap(),
                index: ir::SearchIndexPlan {
                    index_id: ir::NonEmptyString::new("likes_comment").unwrap(),
                    tenant: ir::SearchTenantPlan::Unscoped,
                },
                query_text: ir::TextQueryInputPlan::new(helix_ast::value::PropertyInput::from(
                    "hello",
                ))
                .unwrap(),
                k: literal_limit(3),
            },
        ] {
            assert_eq!(plan.direct_label(), Some(&likes));
        }

        assert!(EdgeAccessPlan::AllScan.direct_label().is_none());
    }

    #[test]
    fn edge_source_common_label_requires_every_set_branch_to_match() {
        let likes = ir::NonEmptyString::new("LIKES").unwrap();
        let homogeneous = source(EdgeAccessPlan::Intersect(ir::AtLeast::<_, 2>::from_pair(
            source(EdgeAccessPlan::LabelScan {
                label: likes.clone(),
            }),
            source(EdgeAccessPlan::RangeIndex {
                index: catalog::EdgeRangeIndexMeta::try_new("likes_created").unwrap(),
                key: catalog::ScopedPropertyDirectionKey::try_new(
                    "LIKES",
                    "created",
                    helix_ast::index::RangeIndexDirection::Asc,
                )
                .unwrap(),
                range: ir::IndexRange::All,
            }),
        )));
        let mixed = source(EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            source(EdgeAccessPlan::LabelScan {
                label: likes.clone(),
            }),
            source(EdgeAccessPlan::LabelScan {
                label: ir::NonEmptyString::new("KNOWS").unwrap(),
            }),
        )));
        let unscoped = source(EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            source(EdgeAccessPlan::LabelScan {
                label: likes.clone(),
            }),
            source(EdgeAccessPlan::FromVar {
                variable: ir::NonEmptyString::new("saved_edges").unwrap(),
            }),
        )));

        assert_eq!(homogeneous.common_label(), Some(&likes));
        assert!(mixed.common_label().is_none());
        assert!(unscoped.common_label().is_none());
    }

    #[test]
    fn edge_source_hard_cardinality_upper_bound_covers_static_sources() {
        assert_eq!(
            source(EdgeAccessPlan::Empty).hard_cardinality_upper_bound(),
            Some(0)
        );
        assert_eq!(
            source(EdgeAccessPlan::PointIds {
                ids: ids(vec![1, 2, 3])
            })
            .hard_cardinality_upper_bound(),
            Some(3)
        );
        assert_eq!(
            source(text_search(literal_limit(4))).hard_cardinality_upper_bound(),
            Some(4)
        );
    }

    #[test]
    fn edge_source_hard_cardinality_upper_bound_preserves_unknown_sources() {
        let dynamic_limit = ir::SearchLimitPlan::Expr(
            ir::SearchLimitExprPlan::new(helix_ast::expr::Expr::param("k")).unwrap(),
        );

        assert_eq!(
            source(EdgeAccessPlan::EqualityIndex {
                index: catalog::EdgeEqualityIndexMeta::try_new("edge_status").unwrap(),
                key: catalog::ScopedPropertyKey::try_new("LIKES", "status").unwrap(),
                value: ir::IndexValue::Literal(
                    ir::SecondaryIndexLiteral::new(helix_ast::value::PropertyValue::from("hot"))
                        .unwrap(),
                ),
            })
            .hard_cardinality_upper_bound(),
            None
        );
        assert_eq!(
            source(text_search(dynamic_limit)).hard_cardinality_upper_bound(),
            None
        );
        assert_eq!(
            source(EdgeAccessPlan::AllScan).hard_cardinality_upper_bound(),
            None
        );
    }

    #[test]
    fn edge_source_hard_cardinality_upper_bound_composes_sets() {
        let two = source(EdgeAccessPlan::PointIds {
            ids: ids(vec![1, 2]),
        });
        let one = source(text_search(literal_limit(1)));
        let scan = source(EdgeAccessPlan::AllScan);

        assert_eq!(
            source(EdgeAccessPlan::Intersect(ir::AtLeast::<_, 2>::from_pair(
                two.clone(),
                scan.clone(),
            )))
            .hard_cardinality_upper_bound(),
            Some(2)
        );
        assert_eq!(
            source(EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
                two.clone(),
                one,
            )))
            .hard_cardinality_upper_bound(),
            Some(3)
        );
        assert_eq!(
            source(EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
                two, scan,
            )))
            .hard_cardinality_upper_bound(),
            None
        );
    }

    #[test]
    fn edge_source_set_canonicalization_candidates_track_rewrite_shapes() {
        let likes = label_scan("LIKES");
        let duplicate = source(EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            likes.clone(),
            likes.clone(),
        )));
        let nested = source(EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            likes.clone(),
            source(EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
                label_scan("KNOWS"),
                label_scan("FOLLOWS"),
            ))),
        )));
        let empty_intersection = source(EdgeAccessPlan::Intersect(ir::AtLeast::<_, 2>::from_pair(
            source(EdgeAccessPlan::Empty),
            likes.clone(),
        )));
        let ordinary = source(EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            likes,
            label_scan("KNOWS"),
        )));

        assert!(duplicate.has_set_canonicalization_candidate());
        assert!(nested.has_set_canonicalization_candidate());
        assert!(empty_intersection.has_set_canonicalization_candidate());
        assert!(!ordinary.has_set_canonicalization_candidate());
        assert!(!source(EdgeAccessPlan::AllScan).has_set_canonicalization_candidate());
    }

    #[test]
    fn edge_source_subsumption_candidates_use_source_partial_order() {
        let all = source(EdgeAccessPlan::AllScan);
        let label = label_scan("LIKES");
        let equality = source(EdgeAccessPlan::EqualityIndex {
            index: catalog::EdgeEqualityIndexMeta::try_new("likes_status").unwrap(),
            key: catalog::ScopedPropertyKey::try_new("LIKES", "status").unwrap(),
            value: ir::IndexValue::Literal(
                ir::SecondaryIndexLiteral::new(helix_ast::value::PropertyValue::from("hot"))
                    .unwrap(),
            ),
        });
        let union = source(EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            all.clone(),
            label.clone(),
        )));
        let intersection = source(EdgeAccessPlan::Intersect(ir::AtLeast::<_, 2>::from_pair(
            label.clone(),
            equality.clone(),
        )));
        let ordinary = source(EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            label_scan("LIKES"),
            label_scan("KNOWS"),
        )));

        assert!(all.subsumes(&label));
        assert!(label.subsumes(&equality));
        assert!(union.has_set_subsumption_candidate());
        assert!(intersection.has_set_subsumption_candidate());
        assert!(!ordinary.has_set_subsumption_candidate());
    }
}
