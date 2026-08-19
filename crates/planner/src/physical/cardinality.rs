//! Payload-carrying physical cardinality programs.

use serde::{Deserialize, Serialize};

use crate::exec;

/// Costing and diagnostics family for an exact cardinality program.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhysicalCardinality {
    /// One non-unique equality bitmap read.
    BitmapPoint,
    /// Same-index bitmap multi-get and union.
    BitmapBatchUnion,
    /// Explicit bitmap union.
    BitmapUnion,
    /// Explicit bitmap intersection.
    BitmapIntersection,
    /// Unique-owner lookup plus authoritative verification.
    UniqueVerified,
    /// Streaming range scan plus authoritative verification.
    VerifiedRange,
    /// Authoritative graph scan.
    AuthoritativeScan,
    /// Compile-time constant.
    Constant,
    /// Verified point reads.
    VerifiedPointReads,
    /// Runtime parameter or variable source.
    RuntimeInput,
    /// Full authoritative element scan.
    FullScan,
    /// Label bitmap cardinality.
    LabelBitmap,
    /// Unrestricted vector search.
    VectorSearch,
    /// Unrestricted text search.
    TextSearch,
    /// Explicit materialized set union.
    SetUnion,
    /// Explicit materialized set intersection.
    SetIntersection,
    /// Authoritative predicate filter cursor.
    FilterStream,
    /// Expansion cursor.
    ExpandStream,
    /// Explicit distinct cursor.
    DistinctStream,
    /// Restricted vector search cursor.
    RestrictedVectorStream,
    /// Restricted text search cursor.
    RestrictedTextStream,
    /// Variable cursor.
    VariableStream,
    /// Required ordered cursor.
    OrderedStream,
    /// Rows supplied by an executable dependency.
    InputRows,
    /// Scalar items supplied by an executable dependency.
    InputScalars,
    /// Explicit late-bound equality dispatch exception.
    DynamicEquality,
}

/// Complete physical count payload selected by the optimizer.
///
/// The family is derived from the executable ADT, so a diagnostic algorithm
/// tag cannot disagree with the program selected for lowering.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PhysicalCountPlan(exec::ExecCountPlan);

impl PhysicalCountPlan {
    /// Build a physical plan from its complete executable payload.
    pub const fn new(plan: exec::ExecCountPlan) -> Self {
        Self(plan)
    }

    /// Exact executable payload.
    pub const fn executable(&self) -> &exec::ExecCountPlan {
        &self.0
    }

    /// Consume this physical wrapper.
    pub fn into_executable(self) -> exec::ExecCountPlan {
        self.0
    }

    /// Root physical algorithm family.
    pub fn family(&self) -> PhysicalCardinality {
        family(&self.0)
    }
}

fn family(plan: &exec::ExecCountPlan) -> PhysicalCardinality {
    match plan {
        exec::ExecCountPlan::Constant(_) => PhysicalCardinality::Constant,
        exec::ExecCountPlan::NodeBitmap(plan) => node_bitmap_family(&plan.bitmap),
        exec::ExecCountPlan::EdgeBitmap(plan) => edge_bitmap_family(&plan.bitmap),
        exec::ExecCountPlan::NodeUnique(_) => PhysicalCardinality::UniqueVerified,
        exec::ExecCountPlan::NodeRange(_) | exec::ExecCountPlan::EdgeRange(_) => {
            PhysicalCardinality::VerifiedRange
        }
        exec::ExecCountPlan::NodeAuthoritativeScan(_)
        | exec::ExecCountPlan::EdgeAuthoritativeScan(_) => PhysicalCardinality::AuthoritativeScan,
        exec::ExecCountPlan::NodePointReads { .. } | exec::ExecCountPlan::EdgePointReads { .. } => {
            PhysicalCardinality::VerifiedPointReads
        }
        exec::ExecCountPlan::NodeRuntimeInput { .. }
        | exec::ExecCountPlan::EdgeRuntimeInput { .. }
        | exec::ExecCountPlan::RuntimeInput { .. } => PhysicalCardinality::RuntimeInput,
        exec::ExecCountPlan::NodeFullScan { .. } | exec::ExecCountPlan::EdgeFullScan { .. } => {
            PhysicalCardinality::FullScan
        }
        exec::ExecCountPlan::NodeLabelBitmap { .. }
        | exec::ExecCountPlan::EdgeLabelBitmap { .. } => PhysicalCardinality::LabelBitmap,
        exec::ExecCountPlan::NodeVectorSearch(_) | exec::ExecCountPlan::EdgeVectorSearch(_) => {
            PhysicalCardinality::VectorSearch
        }
        exec::ExecCountPlan::NodeTextSearch(_) | exec::ExecCountPlan::EdgeTextSearch(_) => {
            PhysicalCardinality::TextSearch
        }
        exec::ExecCountPlan::NodeDynamicEquality(_)
        | exec::ExecCountPlan::EdgeDynamicEquality(_) => PhysicalCardinality::DynamicEquality,
        exec::ExecCountPlan::Stream(plan) => cursor_family(&plan.cursor),
        exec::ExecCountPlan::InputRows { .. } => PhysicalCardinality::InputRows,
        exec::ExecCountPlan::InputScalars { .. } => PhysicalCardinality::InputScalars,
    }
}

fn node_bitmap_family(bitmap: &exec::ExecNodeBitmapExpr) -> PhysicalCardinality {
    match bitmap {
        exec::ExecNodeBitmapExpr::PointRead { .. } => PhysicalCardinality::BitmapPoint,
        exec::ExecNodeBitmapExpr::BatchedUnionRead { .. } => PhysicalCardinality::BitmapBatchUnion,
        exec::ExecNodeBitmapExpr::Union { .. } => PhysicalCardinality::BitmapUnion,
        exec::ExecNodeBitmapExpr::Intersect { .. } => PhysicalCardinality::BitmapIntersection,
    }
}

fn edge_bitmap_family(bitmap: &exec::ExecEdgeBitmapExpr) -> PhysicalCardinality {
    match bitmap {
        exec::ExecEdgeBitmapExpr::PointRead { .. } => PhysicalCardinality::BitmapPoint,
        exec::ExecEdgeBitmapExpr::BatchedUnionRead { .. } => PhysicalCardinality::BitmapBatchUnion,
        exec::ExecEdgeBitmapExpr::Union { .. } => PhysicalCardinality::BitmapUnion,
        exec::ExecEdgeBitmapExpr::Intersect { .. } => PhysicalCardinality::BitmapIntersection,
    }
}

fn cursor_family(cursor: &exec::ExecCountCursorPlan) -> PhysicalCardinality {
    match cursor {
        exec::ExecCountCursorPlan::EmptyRows => PhysicalCardinality::Constant,
        exec::ExecCountCursorPlan::InputRows => PhysicalCardinality::InputRows,
        exec::ExecCountCursorPlan::NodeBitmap(bitmap) => node_bitmap_family(bitmap),
        exec::ExecCountCursorPlan::EdgeBitmap(bitmap) => edge_bitmap_family(bitmap),
        exec::ExecCountCursorPlan::NodeUnique { .. } => PhysicalCardinality::UniqueVerified,
        exec::ExecCountCursorPlan::NodeRange(_) | exec::ExecCountCursorPlan::EdgeRange(_) => {
            PhysicalCardinality::VerifiedRange
        }
        exec::ExecCountCursorPlan::NodeAuthoritativeScan(_)
        | exec::ExecCountCursorPlan::EdgeAuthoritativeScan(_) => {
            PhysicalCardinality::AuthoritativeScan
        }
        exec::ExecCountCursorPlan::NodePointReads(_)
        | exec::ExecCountCursorPlan::EdgePointReads(_) => PhysicalCardinality::VerifiedPointReads,
        exec::ExecCountCursorPlan::NodeRuntimeInput(_)
        | exec::ExecCountCursorPlan::EdgeRuntimeInput(_)
        | exec::ExecCountCursorPlan::RuntimeInput(_) => PhysicalCardinality::RuntimeInput,
        exec::ExecCountCursorPlan::NodeFullScan | exec::ExecCountCursorPlan::EdgeFullScan => {
            PhysicalCardinality::FullScan
        }
        exec::ExecCountCursorPlan::NodeLabelBitmap(_)
        | exec::ExecCountCursorPlan::EdgeLabelBitmap(_) => PhysicalCardinality::LabelBitmap,
        exec::ExecCountCursorPlan::NodeVectorSearch { .. }
        | exec::ExecCountCursorPlan::EdgeVectorSearch { .. } => PhysicalCardinality::VectorSearch,
        exec::ExecCountCursorPlan::NodeTextSearch { .. }
        | exec::ExecCountCursorPlan::EdgeTextSearch { .. } => PhysicalCardinality::TextSearch,
        exec::ExecCountCursorPlan::NodeDynamicEquality { .. }
        | exec::ExecCountCursorPlan::EdgeDynamicEquality { .. } => {
            PhysicalCardinality::DynamicEquality
        }
        exec::ExecCountCursorPlan::Union { .. } => PhysicalCardinality::SetUnion,
        exec::ExecCountCursorPlan::Intersect { .. } => PhysicalCardinality::SetIntersection,
        exec::ExecCountCursorPlan::Filter { .. } => PhysicalCardinality::FilterStream,
        exec::ExecCountCursorPlan::Window { input, .. } => cursor_family(input),
        exec::ExecCountCursorPlan::Order { .. } => PhysicalCardinality::OrderedStream,
        exec::ExecCountCursorPlan::Expand { .. } => PhysicalCardinality::ExpandStream,
        exec::ExecCountCursorPlan::VectorSearch { .. } => {
            PhysicalCardinality::RestrictedVectorStream
        }
        exec::ExecCountCursorPlan::TextSearch { .. } => PhysicalCardinality::RestrictedTextStream,
        exec::ExecCountCursorPlan::Variable { .. } => PhysicalCardinality::VariableStream,
        exec::ExecCountCursorPlan::Distinct { .. } => PhysicalCardinality::DistinctStream,
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use helix_ast::{
        expr::Predicate,
        index::RangeIndexDirection,
        traversal::Order,
        value::{PropertyInput, PropertyValue},
    };

    use super::*;
    use crate::{catalog, ir, logical};

    fn name(value: &str) -> ir::NonEmptyString {
        ir::NonEmptyString::new(value).unwrap()
    }

    fn indexed(value: &str) -> exec::ExecIndexedEqualityValue {
        ir::SecondaryIndexLiteral::new(PropertyValue::from(value))
            .unwrap()
            .try_into()
            .unwrap()
    }

    fn node_point(value: &str) -> exec::ExecNodeBitmapExpr {
        exec::ExecNodeBitmapExpr::PointRead {
            index: catalog::NodeEqualityIndexMeta::new(name("node-equality"))
                .try_into()
                .unwrap(),
            key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
            value: indexed(value),
        }
    }

    fn edge_point(value: &str) -> exec::ExecEdgeBitmapExpr {
        exec::ExecEdgeBitmapExpr::PointRead {
            index: exec::ExecEdgeNonUniqueEqualityIndex::new(catalog::EdgeEqualityIndexMeta::new(
                name("edge-equality"),
            )),
            key: catalog::ScopedPropertyKey::try_new("LIKES", "status").unwrap(),
            value: indexed(value),
        }
    }

    fn node_range() -> exec::ExecNodeVerifiedRangeScanPlan {
        exec::ExecNodeVerifiedRangeScanPlan {
            index: catalog::NodeRangeIndexMeta::try_new("node-range").unwrap(),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "age",
                RangeIndexDirection::Asc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        }
    }

    fn edge_range() -> exec::ExecEdgeVerifiedRangeScanPlan {
        exec::ExecEdgeVerifiedRangeScanPlan {
            index: catalog::EdgeRangeIndexMeta::try_new("edge-range").unwrap(),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "LIKES",
                "age",
                RangeIndexDirection::Desc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        }
    }

    fn unique_parts() -> (
        exec::ExecNodeUniqueOwnerReadPlan,
        exec::ExecNodeAuthoritativeVerificationPlan,
    ) {
        let key = catalog::ScopedPropertyKey::try_new("User", "email").unwrap();
        let value = indexed("alice@example.test");
        (
            exec::ExecNodeUniqueOwnerReadPlan {
                index: catalog::NodeEqualityIndexMeta::new(name("node-unique"))
                    .with_uniqueness(catalog::IndexUniqueness::Unique)
                    .try_into()
                    .unwrap(),
                key: key.clone(),
                value: value.clone(),
            },
            exec::ExecNodeAuthoritativeVerificationPlan { key, value },
        )
    }

    fn predicate() -> ir::PredicatePlan {
        ir::PredicatePlan::new(Predicate::has_key("status")).unwrap()
    }

    fn ids() -> ir::ElementIds {
        ir::ElementIds::new(ir::AtLeast::from_one(1)).unwrap()
    }

    fn search_index() -> ir::SearchIndexPlan {
        ir::SearchIndexPlan {
            index_id: name("search-index"),
            tenant: ir::SearchTenantPlan::Unscoped,
        }
    }

    fn vector_input() -> ir::VectorQueryInputPlan {
        ir::VectorQueryInputPlan::new(PropertyInput::from(vec![1.0_f32])).unwrap()
    }

    fn text_input() -> ir::TextQueryInputPlan {
        ir::TextQueryInputPlan::new(PropertyInput::from("needle")).unwrap()
    }

    fn limit() -> ir::SearchLimitPlan {
        ir::SearchLimitPlan::Literal(NonZeroUsize::MIN)
    }

    fn assert_cursor_family(cursor: exec::ExecCountCursorPlan, expected: PhysicalCardinality) {
        let plan = PhysicalCountPlan::new(exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
            cursor,
            window: exec::ExecCountWindowPlan::identity(),
        }));
        assert_eq!(plan.family(), expected);
    }

    #[test]
    fn family_is_derived_from_payload() {
        let input = PhysicalCountPlan::new(exec::ExecCountPlan::InputRows {
            window: exec::ExecCountWindowPlan::identity(),
        });
        let constant = PhysicalCountPlan::new(exec::ExecCountPlan::Constant(0));

        assert_eq!(input.family(), PhysicalCardinality::InputRows);
        assert_eq!(constant.family(), PhysicalCardinality::Constant);
    }

    #[test]
    fn every_direct_family_is_derived_from_its_exact_payload() {
        let window = exec::ExecCountWindowPlan::identity();
        let (lookup, verification) = unique_parts();
        let plans = [
            (
                exec::ExecCountPlan::NodeBitmap(exec::ExecNodeBitmapCountPlan {
                    bitmap: node_point("active"),
                    window: window.clone(),
                }),
                PhysicalCardinality::BitmapPoint,
            ),
            (
                exec::ExecCountPlan::NodeBitmap(exec::ExecNodeBitmapCountPlan {
                    bitmap: exec::ExecNodeBitmapExpr::BatchedUnionRead {
                        index: catalog::NodeEqualityIndexMeta::new(name("node-equality"))
                            .try_into()
                            .unwrap(),
                        key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                        values: ir::AtLeast::from_pair(indexed("active"), indexed("inactive")),
                    },
                    window: window.clone(),
                }),
                PhysicalCardinality::BitmapBatchUnion,
            ),
            (
                exec::ExecCountPlan::NodeBitmap(exec::ExecNodeBitmapCountPlan {
                    bitmap: exec::ExecNodeBitmapExpr::Union {
                        driver: Box::new(node_point("active")),
                        rest: ir::AtLeast::from_one(node_point("inactive")),
                    },
                    window: window.clone(),
                }),
                PhysicalCardinality::BitmapUnion,
            ),
            (
                exec::ExecCountPlan::NodeBitmap(exec::ExecNodeBitmapCountPlan {
                    bitmap: exec::ExecNodeBitmapExpr::Intersect {
                        driver: Box::new(node_point("active")),
                        rest: ir::AtLeast::from_one(node_point("inactive")),
                    },
                    window: window.clone(),
                }),
                PhysicalCardinality::BitmapIntersection,
            ),
            (
                exec::ExecCountPlan::EdgeBitmap(exec::ExecEdgeBitmapCountPlan {
                    bitmap: edge_point("active"),
                    window: window.clone(),
                }),
                PhysicalCardinality::BitmapPoint,
            ),
            (
                exec::ExecCountPlan::NodeUnique(exec::ExecNodeUniqueCountPlan {
                    lookup,
                    verification,
                    window: window.clone(),
                }),
                PhysicalCardinality::UniqueVerified,
            ),
            (
                exec::ExecCountPlan::NodeRange(exec::ExecNodeRangeCountPlan {
                    driver: node_range(),
                    membership: exec::ExecNodeRangeMembershipPlan::All,
                    window: window.clone(),
                }),
                PhysicalCardinality::VerifiedRange,
            ),
            (
                exec::ExecCountPlan::EdgeRange(exec::ExecEdgeRangeCountPlan {
                    driver: edge_range(),
                    membership: exec::ExecEdgeRangeMembershipPlan::All,
                    window: window.clone(),
                }),
                PhysicalCardinality::VerifiedRange,
            ),
            (
                exec::ExecCountPlan::NodeAuthoritativeScan(exec::ExecNodeScanCountPlan {
                    predicate: exec::ExecNodeAuthoritativeScanPredicate::Predicate(predicate()),
                    window: window.clone(),
                }),
                PhysicalCardinality::AuthoritativeScan,
            ),
            (
                exec::ExecCountPlan::EdgeAuthoritativeScan(exec::ExecEdgeScanCountPlan {
                    predicate: exec::ExecEdgeAuthoritativeScanPredicate::Predicate(predicate()),
                    window: window.clone(),
                }),
                PhysicalCardinality::AuthoritativeScan,
            ),
            (
                exec::ExecCountPlan::NodePointReads {
                    ids: ids(),
                    window: window.clone(),
                },
                PhysicalCardinality::VerifiedPointReads,
            ),
            (
                exec::ExecCountPlan::EdgePointReads {
                    ids: ids(),
                    window: window.clone(),
                },
                PhysicalCardinality::VerifiedPointReads,
            ),
            (
                exec::ExecCountPlan::NodeRuntimeInput {
                    input: exec::ExecRuntimeInputPlan::Param(name("nodes")),
                    window: window.clone(),
                },
                PhysicalCardinality::RuntimeInput,
            ),
            (
                exec::ExecCountPlan::EdgeRuntimeInput {
                    input: exec::ExecRuntimeInputPlan::Param(name("edges")),
                    window: window.clone(),
                },
                PhysicalCardinality::RuntimeInput,
            ),
            (
                exec::ExecCountPlan::RuntimeInput {
                    input: exec::ExecRuntimeInputPlan::Variable(name("rows")),
                    window: window.clone(),
                },
                PhysicalCardinality::RuntimeInput,
            ),
            (
                exec::ExecCountPlan::NodeFullScan {
                    window: window.clone(),
                },
                PhysicalCardinality::FullScan,
            ),
            (
                exec::ExecCountPlan::EdgeFullScan {
                    window: window.clone(),
                },
                PhysicalCardinality::FullScan,
            ),
            (
                exec::ExecCountPlan::NodeLabelBitmap {
                    label: name("User"),
                    window: window.clone(),
                },
                PhysicalCardinality::LabelBitmap,
            ),
            (
                exec::ExecCountPlan::EdgeLabelBitmap {
                    label: name("LIKES"),
                    window: window.clone(),
                },
                PhysicalCardinality::LabelBitmap,
            ),
            (
                exec::ExecCountPlan::NodeVectorSearch(exec::ExecNodeVectorSearchCountPlan {
                    key: catalog::NodeSearchIndexKey::try_new("User", "embedding").unwrap(),
                    index: search_index(),
                    query_vector: vector_input(),
                    k: limit(),
                    window: window.clone(),
                }),
                PhysicalCardinality::VectorSearch,
            ),
            (
                exec::ExecCountPlan::EdgeVectorSearch(exec::ExecEdgeVectorSearchCountPlan {
                    key: catalog::EdgeSearchIndexKey::try_new("LIKES", "embedding").unwrap(),
                    index: search_index(),
                    query_vector: vector_input(),
                    k: limit(),
                    window: window.clone(),
                }),
                PhysicalCardinality::VectorSearch,
            ),
            (
                exec::ExecCountPlan::NodeTextSearch(exec::ExecNodeTextSearchCountPlan {
                    key: catalog::NodeSearchIndexKey::try_new("User", "body").unwrap(),
                    index: search_index(),
                    query_text: text_input(),
                    k: limit(),
                    window: window.clone(),
                }),
                PhysicalCardinality::TextSearch,
            ),
            (
                exec::ExecCountPlan::EdgeTextSearch(exec::ExecEdgeTextSearchCountPlan {
                    key: catalog::EdgeSearchIndexKey::try_new("LIKES", "body").unwrap(),
                    index: search_index(),
                    query_text: text_input(),
                    k: limit(),
                    window: window.clone(),
                }),
                PhysicalCardinality::TextSearch,
            ),
            (
                exec::ExecCountPlan::NodeDynamicEquality(exec::ExecNodeDynamicEqualityCountPlan {
                    index: catalog::NodeEqualityIndexMeta::new(name("node-equality")),
                    key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                    param: name("status"),
                    window: window.clone(),
                }),
                PhysicalCardinality::DynamicEquality,
            ),
            (
                exec::ExecCountPlan::EdgeDynamicEquality(exec::ExecEdgeDynamicEqualityCountPlan {
                    index: catalog::EdgeEqualityIndexMeta::new(name("edge-equality")),
                    key: catalog::ScopedPropertyKey::try_new("LIKES", "status").unwrap(),
                    param: name("status"),
                    window: window.clone(),
                }),
                PhysicalCardinality::DynamicEquality,
            ),
            (
                exec::ExecCountPlan::InputScalars {
                    window: window.clone(),
                },
                PhysicalCardinality::InputScalars,
            ),
        ];

        for (plan, expected) in plans {
            let physical = PhysicalCountPlan::new(plan.clone());
            assert_eq!(physical.executable(), &plan);
            assert_eq!(physical.family(), expected);
            assert_eq!(physical.into_executable(), plan);
        }
    }

    #[test]
    fn every_recursive_cursor_family_is_derived_from_its_root_primitive() {
        let (lookup, verification) = unique_parts();
        let cases = vec![
            (
                exec::ExecCountCursorPlan::EmptyRows,
                PhysicalCardinality::Constant,
            ),
            (
                exec::ExecCountCursorPlan::InputRows,
                PhysicalCardinality::InputRows,
            ),
            (
                exec::ExecCountCursorPlan::NodeBitmap(node_point("active")),
                PhysicalCardinality::BitmapPoint,
            ),
            (
                exec::ExecCountCursorPlan::EdgeBitmap(exec::ExecEdgeBitmapExpr::BatchedUnionRead {
                    index: exec::ExecEdgeNonUniqueEqualityIndex::new(
                        catalog::EdgeEqualityIndexMeta::new(name("edge-equality")),
                    ),
                    key: catalog::ScopedPropertyKey::try_new("LIKES", "status").unwrap(),
                    values: ir::AtLeast::from_pair(indexed("active"), indexed("inactive")),
                }),
                PhysicalCardinality::BitmapBatchUnion,
            ),
            (
                exec::ExecCountCursorPlan::EdgeBitmap(exec::ExecEdgeBitmapExpr::Union {
                    driver: Box::new(edge_point("active")),
                    rest: ir::AtLeast::from_one(edge_point("inactive")),
                }),
                PhysicalCardinality::BitmapUnion,
            ),
            (
                exec::ExecCountCursorPlan::EdgeBitmap(exec::ExecEdgeBitmapExpr::Intersect {
                    driver: Box::new(edge_point("active")),
                    rest: ir::AtLeast::from_one(edge_point("inactive")),
                }),
                PhysicalCardinality::BitmapIntersection,
            ),
            (
                exec::ExecCountCursorPlan::NodeUnique {
                    lookup,
                    verification,
                },
                PhysicalCardinality::UniqueVerified,
            ),
            (
                exec::ExecCountCursorPlan::NodeRange(node_range()),
                PhysicalCardinality::VerifiedRange,
            ),
            (
                exec::ExecCountCursorPlan::EdgeRange(edge_range()),
                PhysicalCardinality::VerifiedRange,
            ),
            (
                exec::ExecCountCursorPlan::NodeAuthoritativeScan(
                    exec::ExecNodeAuthoritativeScanPredicate::Predicate(predicate()),
                ),
                PhysicalCardinality::AuthoritativeScan,
            ),
            (
                exec::ExecCountCursorPlan::EdgeAuthoritativeScan(
                    exec::ExecEdgeAuthoritativeScanPredicate::Predicate(predicate()),
                ),
                PhysicalCardinality::AuthoritativeScan,
            ),
            (
                exec::ExecCountCursorPlan::NodePointReads(ids()),
                PhysicalCardinality::VerifiedPointReads,
            ),
            (
                exec::ExecCountCursorPlan::EdgePointReads(ids()),
                PhysicalCardinality::VerifiedPointReads,
            ),
            (
                exec::ExecCountCursorPlan::NodeRuntimeInput(exec::ExecRuntimeInputPlan::Param(
                    name("nodes"),
                )),
                PhysicalCardinality::RuntimeInput,
            ),
            (
                exec::ExecCountCursorPlan::EdgeRuntimeInput(exec::ExecRuntimeInputPlan::Param(
                    name("edges"),
                )),
                PhysicalCardinality::RuntimeInput,
            ),
            (
                exec::ExecCountCursorPlan::RuntimeInput(exec::ExecRuntimeInputPlan::Variable(
                    name("rows"),
                )),
                PhysicalCardinality::RuntimeInput,
            ),
            (
                exec::ExecCountCursorPlan::NodeFullScan,
                PhysicalCardinality::FullScan,
            ),
            (
                exec::ExecCountCursorPlan::EdgeFullScan,
                PhysicalCardinality::FullScan,
            ),
            (
                exec::ExecCountCursorPlan::NodeLabelBitmap(name("User")),
                PhysicalCardinality::LabelBitmap,
            ),
            (
                exec::ExecCountCursorPlan::EdgeLabelBitmap(name("LIKES")),
                PhysicalCardinality::LabelBitmap,
            ),
            (
                exec::ExecCountCursorPlan::NodeVectorSearch {
                    key: catalog::NodeSearchIndexKey::try_new("User", "embedding").unwrap(),
                    index: search_index(),
                    query_vector: vector_input(),
                    k: limit(),
                },
                PhysicalCardinality::VectorSearch,
            ),
            (
                exec::ExecCountCursorPlan::EdgeVectorSearch {
                    key: catalog::EdgeSearchIndexKey::try_new("LIKES", "embedding").unwrap(),
                    index: search_index(),
                    query_vector: vector_input(),
                    k: limit(),
                },
                PhysicalCardinality::VectorSearch,
            ),
            (
                exec::ExecCountCursorPlan::NodeTextSearch {
                    key: catalog::NodeSearchIndexKey::try_new("User", "body").unwrap(),
                    index: search_index(),
                    query_text: text_input(),
                    k: limit(),
                },
                PhysicalCardinality::TextSearch,
            ),
            (
                exec::ExecCountCursorPlan::EdgeTextSearch {
                    key: catalog::EdgeSearchIndexKey::try_new("LIKES", "body").unwrap(),
                    index: search_index(),
                    query_text: text_input(),
                    k: limit(),
                },
                PhysicalCardinality::TextSearch,
            ),
            (
                exec::ExecCountCursorPlan::NodeDynamicEquality {
                    index: catalog::NodeEqualityIndexMeta::new(name("node-equality")),
                    key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                    param: name("status"),
                },
                PhysicalCardinality::DynamicEquality,
            ),
            (
                exec::ExecCountCursorPlan::EdgeDynamicEquality {
                    index: catalog::EdgeEqualityIndexMeta::new(name("edge-equality")),
                    key: catalog::ScopedPropertyKey::try_new("LIKES", "status").unwrap(),
                    param: name("status"),
                },
                PhysicalCardinality::DynamicEquality,
            ),
        ];
        for (cursor, expected) in cases {
            assert_cursor_family(cursor, expected);
        }

        let direct = || exec::ExecCountCursorPlan::NodeFullScan;
        for (cursor, expected) in [
            (
                exec::ExecCountCursorPlan::Union {
                    driver: Box::new(direct()),
                    rest: ir::AtLeast::from_one(exec::ExecCountCursorPlan::EdgeFullScan),
                },
                PhysicalCardinality::SetUnion,
            ),
            (
                exec::ExecCountCursorPlan::Intersect {
                    driver: Box::new(direct()),
                    rest: ir::AtLeast::from_one(exec::ExecCountCursorPlan::EdgeFullScan),
                },
                PhysicalCardinality::SetIntersection,
            ),
            (
                exec::ExecCountCursorPlan::Filter {
                    input: Box::new(direct()),
                    predicate: predicate(),
                },
                PhysicalCardinality::FilterStream,
            ),
            (
                exec::ExecCountCursorPlan::Window {
                    input: Box::new(exec::ExecCountCursorPlan::NodeLabelBitmap(name("User"))),
                    window: exec::ExecCountWindowPlan::identity(),
                },
                PhysicalCardinality::LabelBitmap,
            ),
            (
                exec::ExecCountCursorPlan::Order {
                    input: Box::new(direct()),
                    plan: ir::OrderPlan::ExplicitSort(ir::OrderKeys::from(ir::OrderKey {
                        property: name("age"),
                        order: Order::Asc,
                    })),
                },
                PhysicalCardinality::OrderedStream,
            ),
            (
                exec::ExecCountCursorPlan::Expand {
                    input: Box::new(direct()),
                    plan: ir::ExpandPlan {
                        direction: ir::ExpandDirection::Out,
                        output: ir::ExpandOutput::Nodes,
                        label: ir::ExpandLabelPlan::Any,
                    },
                },
                PhysicalCardinality::ExpandStream,
            ),
            (
                exec::ExecCountCursorPlan::VectorSearch {
                    input: Box::new(direct()),
                    plan: Box::new(ir::RestrictedVectorSearchPlan::Nodes {
                        key: catalog::NodeSearchIndexKey::try_new("User", "embedding").unwrap(),
                        index: search_index(),
                        query_vector: vector_input(),
                        k: limit(),
                    }),
                },
                PhysicalCardinality::RestrictedVectorStream,
            ),
            (
                exec::ExecCountCursorPlan::TextSearch {
                    input: Box::new(direct()),
                    plan: Box::new(ir::RestrictedTextSearchPlan::Nodes {
                        key: catalog::NodeSearchIndexKey::try_new("User", "body").unwrap(),
                        index: search_index(),
                        query_text: text_input(),
                        k: limit(),
                    }),
                },
                PhysicalCardinality::RestrictedTextStream,
            ),
            (
                exec::ExecCountCursorPlan::Variable {
                    input: Box::new(direct()),
                    op: logical::PureStreamVariableOp::Select(name("saved")),
                },
                PhysicalCardinality::VariableStream,
            ),
            (
                exec::ExecCountCursorPlan::Distinct {
                    input: Box::new(direct()),
                    plan: exec::ExecCountDistinctPlan::HashRows,
                },
                PhysicalCardinality::DistinctStream,
            ),
        ] {
            assert_cursor_family(cursor, expected);
        }
    }
}
