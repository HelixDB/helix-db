use serde::{Deserialize, Serialize};

use crate::{catalog, exec, ir};

/// Native executable edge access.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecEdgeAccessPlan {
    /// Known empty edge stream.
    Empty,
    /// Runtime parameter edge IDs.
    FromParam { param: ir::NonEmptyString },
    /// Variable edge set.
    FromVar { variable: ir::NonEmptyString },
    /// Full edge scan.
    AllScan,
    /// Label scan.
    LabelScan { label: ir::NonEmptyString },
    /// Exact non-unique equality bitmap program.
    Bitmap {
        /// Planner-selected bitmap primitive tree.
        bitmap: exec::ExecEdgeBitmapExpr,
    },
    /// Exact authoritative equality scan.
    AuthoritativeScan {
        /// Predicate applied to authoritative graph rows.
        predicate: exec::ExecEdgeAuthoritativeScanPredicate,
    },
    /// Explicit runtime equality classifier exception.
    DynamicEquality {
        /// Logical index metadata used to validate the runtime branch.
        index: catalog::EdgeEqualityIndexMeta,
        /// Indexed property key.
        key: catalog::ScopedPropertyKey,
        /// Genuinely late-bound parameter.
        param: ir::NonEmptyString,
    },
    /// Edge range-index scan.
    RangeIndex {
        /// Index metadata.
        index: catalog::EdgeRangeIndexMeta,
        /// Indexed property key and direction.
        key: catalog::ScopedPropertyDirectionKey,
        /// Range bounds.
        range: ir::IndexRange,
    },
    /// V2-aware secondary-ID set evaluated before row materialization.
    SecondarySet {
        /// Logical secondary-index set contract.
        set: ExecEdgeSecondarySetPlan,
    },
    /// Edge vector search.
    VectorSearch {
        /// Search key.
        key: catalog::EdgeSearchIndexKey,
        /// Search index execution plan.
        index: ir::SearchIndexPlan,
        /// Query vector.
        query_vector: ir::VectorQueryInputPlan,
        /// Result count.
        k: ir::SearchLimitPlan,
    },
    /// Edge text search.
    TextSearch {
        /// Search key.
        key: catalog::EdgeSearchIndexKey,
        /// Search index execution plan.
        index: ir::SearchIndexPlan,
        /// Query text.
        query_text: ir::TextQueryInputPlan,
        /// Result count.
        k: ir::SearchLimitPlan,
    },
}

impl ExecEdgeAccessPlan {
    /// Classify one logical equality into an exact executable row-access primitive.
    pub fn exact_equality(
        index: catalog::EdgeEqualityIndexMeta,
        key: catalog::ScopedPropertyKey,
        value: ir::IndexValue,
    ) -> Self {
        exact_edge_equality(index, key, value).into()
    }
}

pub(in crate::exec) enum ExecEdgeEqualityAccessPlan {
    Empty,
    Bitmap(exec::ExecEdgeBitmapExpr),
    AuthoritativeScan(exec::ExecEdgeAuthoritativeScanPredicate),
    DynamicEquality {
        index: catalog::EdgeEqualityIndexMeta,
        key: catalog::ScopedPropertyKey,
        param: ir::NonEmptyString,
    },
}

pub(in crate::exec) fn exact_edge_equality(
    index: catalog::EdgeEqualityIndexMeta,
    key: catalog::ScopedPropertyKey,
    value: ir::IndexValue,
) -> ExecEdgeEqualityAccessPlan {
    match value {
        ir::IndexValue::Literal(value) => match value.semantics() {
            ir::LiteralEqualityIndexValueSemantics::Indexed => {
                ExecEdgeEqualityAccessPlan::Bitmap(exec::ExecEdgeBitmapExpr::PointRead {
                    index: exec::ExecEdgeNonUniqueEqualityIndex::new(index),
                    key,
                    value: exec::ExecIndexedEqualityValue::try_from(value)
                        .expect("indexed equality semantics produce an executable value"),
                })
            }
            ir::LiteralEqualityIndexValueSemantics::AuthoritativeNull => {
                ExecEdgeEqualityAccessPlan::AuthoritativeScan(
                    exec::ExecEdgeAuthoritativeScanPredicate::NullEquality { key },
                )
            }
            ir::LiteralEqualityIndexValueSemantics::NonReflexive => {
                ExecEdgeEqualityAccessPlan::Empty
            }
        },
        ir::IndexValue::Param(param) => {
            ExecEdgeEqualityAccessPlan::DynamicEquality { index, key, param }
        }
    }
}

impl From<ExecEdgeEqualityAccessPlan> for ExecEdgeAccessPlan {
    fn from(plan: ExecEdgeEqualityAccessPlan) -> Self {
        match plan {
            ExecEdgeEqualityAccessPlan::Empty => Self::Empty,
            ExecEdgeEqualityAccessPlan::Bitmap(bitmap) => Self::Bitmap { bitmap },
            ExecEdgeEqualityAccessPlan::AuthoritativeScan(predicate) => {
                Self::AuthoritativeScan { predicate }
            }
            ExecEdgeEqualityAccessPlan::DynamicEquality { index, key, param } => {
                Self::DynamicEquality { index, key, param }
            }
        }
    }
}

/// Executable edge range leaf used directly or as an ordered intersection driver.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecEdgeSecondaryRangePlan {
    /// Logical index metadata.
    pub index: catalog::EdgeRangeIndexMeta,
    /// Indexed property and physical direction capability.
    pub key: catalog::ScopedPropertyDirectionKey,
    /// Logical range bounds.
    pub range: ir::IndexRange,
}

/// V2-aware edge secondary-ID set.
///
/// The database resolves physical index ownership and generation from the
/// request-authorized Active catalog at execution time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecEdgeSecondarySetPlan {
    /// Statically empty set, including non-reflexive NaN equality.
    Empty,
    /// Exact non-unique equality bitmap program.
    Bitmap(exec::ExecEdgeBitmapExpr),
    /// Exact authoritative scan.
    AuthoritativeScan(exec::ExecEdgeAuthoritativeScanPredicate),
    /// Explicit runtime equality classifier exception.
    DynamicEquality {
        /// Logical index metadata.
        index: catalog::EdgeEqualityIndexMeta,
        /// Indexed property.
        key: catalog::ScopedPropertyKey,
        /// Genuinely late-bound parameter.
        param: ir::NonEmptyString,
    },
    /// Generation-qualified range scan resolved by the database.
    Range(ExecEdgeSecondaryRangePlan),
    /// Set intersection in planner-selected evaluation order.
    Intersect {
        /// First child executed.
        driver: Box<ExecEdgeSecondarySetPlan>,
        /// Remaining children in exact order.
        rest: ir::AtLeast<ExecEdgeSecondarySetPlan, 1>,
    },
    /// Set union in planner-selected evaluation order.
    Union {
        /// First child executed.
        driver: Box<ExecEdgeSecondarySetPlan>,
        /// Remaining children in exact order.
        rest: ir::AtLeast<ExecEdgeSecondarySetPlan, 1>,
    },
    /// Range-ordered intersection with post-filter limit semantics.
    OrderedIntersect {
        /// Range scan that defines result order.
        driver: ExecEdgeSecondaryRangePlan,
        /// Non-empty secondary filters applied to driver IDs.
        filters: ir::AtLeast<ExecEdgeSecondarySetPlan, 1>,
    },
}

impl ExecEdgeSecondarySetPlan {
    /// Classify one or more same-index logical equalities into exact set primitives.
    pub fn exact_equalities(
        index: catalog::EdgeEqualityIndexMeta,
        key: catalog::ScopedPropertyKey,
        values: ir::AtLeast<ir::IndexValue, 1>,
    ) -> Self {
        let mut children = values
            .into_iter()
            .map(
                |value| match exact_edge_equality(index.clone(), key.clone(), value) {
                    ExecEdgeEqualityAccessPlan::Empty => Self::Empty,
                    ExecEdgeEqualityAccessPlan::Bitmap(bitmap) => Self::Bitmap(bitmap),
                    ExecEdgeEqualityAccessPlan::AuthoritativeScan(predicate) => {
                        Self::AuthoritativeScan(predicate)
                    }
                    ExecEdgeEqualityAccessPlan::DynamicEquality { index, key, param } => {
                        Self::DynamicEquality { index, key, param }
                    }
                },
            )
            .collect::<Vec<_>>();
        let batch = children
            .iter()
            .map(|child| match child {
                Self::Bitmap(exec::ExecEdgeBitmapExpr::PointRead { index, key, value }) => {
                    Some((index.clone(), key.clone(), value.clone()))
                }
                _ => None,
            })
            .collect::<Option<Vec<_>>>();
        match batch {
            Some(batch) if batch.len() >= 2 => {
                let (index, key, _) = batch[0].clone();
                let values = batch.into_iter().map(|(_, _, value)| value).collect();
                return Self::Bitmap(exec::ExecEdgeBitmapExpr::BatchedUnionRead {
                    index,
                    key,
                    values: ir::AtLeast::try_from_vec(values)
                        .expect("same-index batch has at least two values"),
                });
            }
            Some(_) | None => {}
        }
        let driver = children.remove(0);
        let Some(rest) = ir::AtLeast::try_from_vec(children) else {
            return driver;
        };
        Self::Union {
            driver: Box::new(driver),
            rest,
        }
    }
}
