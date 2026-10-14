use serde::{Deserialize, Serialize};

use crate::{catalog, exec, ir};

/// Closed capability for node access with an owned row cursor. Borrowed fields
/// keep catalog metadata attached to the selected executable plan.
#[derive(Debug, Clone, Copy)]
pub enum ExecNodeCursor<'a> {
    Empty,
    AllScan,
    LabelScan {
        label: &'a ir::NonEmptyString,
    },
    Bitmap {
        bitmap: &'a exec::ExecNodeBitmapExpr,
    },
    Unique {
        lookup: &'a exec::ExecNodeUniqueOwnerReadPlan,
        verification: &'a exec::ExecNodeAuthoritativeVerificationPlan,
    },
    /// Verified secondary-index ID set, such as an equality-literal union.
    SecondarySet {
        set: &'a exec::ExecNodeSecondarySetPlan,
    },
    /// Verified node IDs within one range-index range.
    Range {
        index: &'a catalog::NodeRangeIndexMeta,
        key: &'a catalog::ScopedPropertyDirectionKey,
        range: &'a ir::IndexRange,
        iteration: ir::RangeScanIteration,
    },
}

impl exec::ExecOp {
    /// Node primitives with an owned, resumable row cursor. This is a physical
    /// capability proof only: opening the cursor still requires the request's
    /// snapshot and deferred-mutation visibility barrier.
    ///
    /// ```
    /// use helix_planner::exec;
    /// let operation = exec::ExecOp::Access {
    ///     plan: Box::new(exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::AllScan)),
    /// };
    /// assert!(matches!(operation.node_cursor_access(), Some(exec::ExecNodeCursor::AllScan)));
    /// assert!(exec::ExecOp::Noop.node_cursor_access().is_none());
    /// ```
    pub fn node_cursor_access(&self) -> Option<ExecNodeCursor<'_>> {
        let source = match self {
            Self::KvRead(exec::KvReadPlan::RangeScan {
                keyspace: exec::ElementKeyspace::NodeProperty,
                start: exec::KvKeyBound::Unbounded,
                end: exec::KvKeyBound::Unbounded,
                limit: None,
            }) => return Some(ExecNodeCursor::AllScan),
            Self::Access { plan } => {
                let exec::ExecAccessPlan::Node(source) = plan.as_ref() else {
                    return None;
                };
                source
            }
            _ => return None,
        };
        Some(match source {
            ExecNodeAccessPlan::Empty => ExecNodeCursor::Empty,
            ExecNodeAccessPlan::AllScan => ExecNodeCursor::AllScan,
            ExecNodeAccessPlan::LabelScan { label } => ExecNodeCursor::LabelScan { label },
            ExecNodeAccessPlan::Bitmap { bitmap } => ExecNodeCursor::Bitmap { bitmap },
            ExecNodeAccessPlan::Unique {
                lookup,
                verification,
            } => ExecNodeCursor::Unique {
                lookup,
                verification,
            },
            ExecNodeAccessPlan::SecondarySet { set } => ExecNodeCursor::SecondarySet { set },
            ExecNodeAccessPlan::RangeIndex {
                index,
                key,
                range,
                iteration,
            } => ExecNodeCursor::Range {
                index,
                key,
                range,
                iteration: *iteration,
            },
            _ => return None,
        })
    }
}

/// Native executable node access.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecNodeAccessPlan {
    /// Known empty node stream.
    Empty,
    /// Runtime parameter node IDs.
    FromParam { param: ir::NonEmptyString },
    /// Variable node set.
    FromVar { variable: ir::NonEmptyString },
    /// Full node scan.
    AllScan,
    /// Label scan.
    LabelScan { label: ir::NonEmptyString },
    /// Exact non-unique equality bitmap program.
    Bitmap {
        /// Planner-selected bitmap primitive tree.
        bitmap: exec::ExecNodeBitmapExpr,
    },
    /// Exact unique-owner lookup followed by authoritative verification.
    Unique {
        /// Planner-selected unique owner point read.
        lookup: exec::ExecNodeUniqueOwnerReadPlan,
        /// Required authoritative verification.
        verification: exec::ExecNodeAuthoritativeVerificationPlan,
    },
    /// Exact authoritative equality scan.
    AuthoritativeScan {
        /// Predicate applied to authoritative graph rows.
        predicate: exec::ExecNodeAuthoritativeScanPredicate,
    },
    /// Explicit runtime equality classifier exception.
    DynamicEquality {
        /// Logical index metadata used to validate the runtime branch.
        index: catalog::NodeEqualityIndexMeta,
        /// Indexed property key.
        key: catalog::ScopedPropertyKey,
        /// Genuinely late-bound parameter.
        param: ir::NonEmptyString,
    },
    /// Bounded runtime equality-domain lookup with authoritative fallback.
    DynamicMembership {
        /// Logical index metadata used to validate indexed runtime branches.
        index: catalog::NodeEqualityIndexMeta,
        /// Indexed property key.
        key: catalog::ScopedPropertyKey,
        /// Genuinely late-bound equality domain and its index-union bound.
        values: ir::RuntimeEqualitySet,
    },
    /// Node range-index scan.
    RangeIndex {
        /// Index metadata.
        index: catalog::NodeRangeIndexMeta,
        /// Indexed property key and direction.
        key: catalog::ScopedPropertyDirectionKey,
        /// Range bounds.
        range: ir::IndexRange,
        /// Traversal within the physical lane.
        #[serde(default)]
        iteration: ir::RangeScanIteration,
    },
    /// V2-aware secondary-ID set evaluated before row materialization.
    SecondarySet {
        /// Logical secondary-index set contract.
        set: ExecNodeSecondarySetPlan,
    },
    /// Node vector search.
    VectorSearch {
        /// Search key.
        key: catalog::NodeSearchIndexKey,
        /// Search index execution plan.
        index: ir::SearchIndexPlan,
        /// Query vector.
        query_vector: ir::VectorQueryInputPlan,
        /// Result count.
        k: ir::SearchLimitPlan,
    },
    /// Node text search.
    TextSearch {
        /// Search key.
        key: catalog::NodeSearchIndexKey,
        /// Search index execution plan.
        index: ir::SearchIndexPlan,
        /// Query text.
        query_text: ir::TextQueryInputPlan,
        /// Result count.
        k: ir::SearchLimitPlan,
    },
}

impl ExecNodeAccessPlan {
    /// Classify one logical equality into an exact executable row-access primitive.
    pub fn exact_equality(
        index: catalog::NodeEqualityIndexMeta,
        key: catalog::ScopedPropertyKey,
        value: ir::IndexValue,
    ) -> Self {
        exact_node_equality(index, key, value).into()
    }
}

pub(in crate::exec) enum ExecNodeEqualityAccessPlan {
    Empty,
    Bitmap(exec::ExecNodeBitmapExpr),
    Unique {
        lookup: exec::ExecNodeUniqueOwnerReadPlan,
        verification: exec::ExecNodeAuthoritativeVerificationPlan,
    },
    AuthoritativeScan(exec::ExecNodeAuthoritativeScanPredicate),
    DynamicEquality {
        index: catalog::NodeEqualityIndexMeta,
        key: catalog::ScopedPropertyKey,
        param: ir::NonEmptyString,
    },
    DynamicMembership {
        index: catalog::NodeEqualityIndexMeta,
        key: catalog::ScopedPropertyKey,
        values: ir::RuntimeEqualitySet,
    },
    /// A literal set: batched indexed members, and label rows for null.
    Set(ExecNodeSecondarySetPlan),
}

pub(in crate::exec) fn exact_node_equality(
    index: catalog::NodeEqualityIndexMeta,
    key: catalog::ScopedPropertyKey,
    value: ir::IndexValue,
) -> ExecNodeEqualityAccessPlan {
    match value {
        ir::IndexValue::Literal(value) => match value.semantics() {
            ir::LiteralEqualityIndexValueSemantics::Indexed => {
                let value = exec::ExecIndexedEqualityValue::try_from(value)
                    .expect("indexed equality semantics produce an executable value");
                match index.uniqueness {
                    catalog::IndexUniqueness::Unique => ExecNodeEqualityAccessPlan::Unique {
                        lookup: exec::ExecNodeUniqueOwnerReadPlan {
                            index: exec::ExecNodeUniqueEqualityIndex::try_from(index)
                                .expect("unique metadata produces a unique executable index"),
                            key: key.clone(),
                            value: value.clone(),
                        },
                        verification: exec::ExecNodeAuthoritativeVerificationPlan { key, value },
                    },
                    catalog::IndexUniqueness::NonUnique => {
                        ExecNodeEqualityAccessPlan::Bitmap(exec::ExecNodeBitmapExpr::PointRead {
                            index: exec::ExecNodeNonUniqueEqualityIndex::try_from(index).expect(
                                "non-unique metadata produces a non-unique executable index",
                            ),
                            key,
                            value,
                        })
                    }
                }
            }
            ir::LiteralEqualityIndexValueSemantics::AuthoritativeNull => {
                ExecNodeEqualityAccessPlan::AuthoritativeScan(
                    exec::ExecNodeAuthoritativeScanPredicate::NullEquality { key },
                )
            }
            ir::LiteralEqualityIndexValueSemantics::NonReflexive => {
                ExecNodeEqualityAccessPlan::Empty
            }
        },
        ir::IndexValue::Param(param) => {
            ExecNodeEqualityAccessPlan::DynamicEquality { index, key, param }
        }
        ir::IndexValue::ParamSet(values) => {
            ExecNodeEqualityAccessPlan::DynamicMembership { index, key, values }
        }
        ir::IndexValue::LiteralSet(values) => {
            ExecNodeEqualityAccessPlan::Set(ExecNodeSecondarySetPlan::exact_equalities(
                index,
                key,
                ir::AtLeast::try_from_vec(
                    values.into_iter().map(ir::IndexValue::Literal).collect(),
                )
                .expect("a literal set holds at least two values"),
            ))
        }
    }
}

impl From<ExecNodeEqualityAccessPlan> for ExecNodeSecondarySetPlan {
    fn from(plan: ExecNodeEqualityAccessPlan) -> Self {
        match plan {
            ExecNodeEqualityAccessPlan::Empty => Self::Empty,
            ExecNodeEqualityAccessPlan::Bitmap(bitmap) => Self::Bitmap(bitmap),
            ExecNodeEqualityAccessPlan::Unique {
                lookup,
                verification,
            } => Self::Unique {
                lookup,
                verification,
            },
            ExecNodeEqualityAccessPlan::AuthoritativeScan(predicate) => {
                Self::AuthoritativeScan(predicate)
            }
            ExecNodeEqualityAccessPlan::DynamicEquality { index, key, param } => {
                Self::DynamicEquality { index, key, param }
            }
            ExecNodeEqualityAccessPlan::DynamicMembership { index, key, values } => {
                Self::DynamicMembership { index, key, values }
            }
            ExecNodeEqualityAccessPlan::Set(set) => set,
        }
    }
}

impl From<ExecNodeEqualityAccessPlan> for ExecNodeAccessPlan {
    fn from(plan: ExecNodeEqualityAccessPlan) -> Self {
        match plan {
            ExecNodeEqualityAccessPlan::Empty => Self::Empty,
            ExecNodeEqualityAccessPlan::Bitmap(bitmap) => Self::Bitmap { bitmap },
            ExecNodeEqualityAccessPlan::Unique {
                lookup,
                verification,
            } => Self::Unique {
                lookup,
                verification,
            },
            ExecNodeEqualityAccessPlan::AuthoritativeScan(predicate) => {
                Self::AuthoritativeScan { predicate }
            }
            ExecNodeEqualityAccessPlan::DynamicEquality { index, key, param } => {
                Self::DynamicEquality { index, key, param }
            }
            ExecNodeEqualityAccessPlan::DynamicMembership { index, key, values } => {
                Self::DynamicMembership { index, key, values }
            }
            ExecNodeEqualityAccessPlan::Set(set) => Self::SecondarySet { set },
        }
    }
}

/// Executable node range leaf used directly or as an ordered intersection driver.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecNodeSecondaryRangePlan {
    /// Logical index metadata.
    pub index: catalog::NodeRangeIndexMeta,
    /// Indexed property and physical direction capability.
    pub key: catalog::ScopedPropertyDirectionKey,
    /// Logical range bounds.
    pub range: ir::IndexRange,
    /// Traversal within the physical lane.
    #[serde(default)]
    pub iteration: ir::RangeScanIteration,
}

/// V2-aware node secondary-ID set.
///
/// Raw physical keys, index IDs, generations, and tenant scope are deliberately
/// absent. The database resolves those details from the request-authorized
/// Active index catalog.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecNodeSecondarySetPlan {
    /// Statically empty set, including non-reflexive NaN equality.
    Empty,
    /// Exact non-unique equality bitmap program.
    Bitmap(exec::ExecNodeBitmapExpr),
    /// Same-index unique owner multi-get with authoritative verification in
    /// the request snapshot. Null and non-reflexive values cannot enter it.
    UniqueUnion {
        /// The required unique index lane.
        index: exec::ExecNodeUniqueEqualityIndex,
        /// One label-scoped property shared by every value.
        key: catalog::ScopedPropertyKey,
        /// Finite indexed literals; duplicates have set semantics.
        values: ir::AtLeast<exec::ExecIndexedEqualityValue, 2>,
    },
    /// Exact unique-owner lookup and verification.
    Unique {
        /// Planner-selected owner read.
        lookup: exec::ExecNodeUniqueOwnerReadPlan,
        /// Required authoritative verification.
        verification: exec::ExecNodeAuthoritativeVerificationPlan,
    },
    /// Exact authoritative scan.
    AuthoritativeScan(exec::ExecNodeAuthoritativeScanPredicate),
    /// Explicit runtime equality classifier exception.
    DynamicEquality {
        /// Logical index metadata.
        index: catalog::NodeEqualityIndexMeta,
        /// Indexed property.
        key: catalog::ScopedPropertyKey,
        /// Genuinely late-bound parameter.
        param: ir::NonEmptyString,
    },
    /// Bounded runtime equality domain with authoritative fallback.
    DynamicMembership {
        /// Logical index metadata.
        index: catalog::NodeEqualityIndexMeta,
        /// Indexed property.
        key: catalog::ScopedPropertyKey,
        /// Runtime domain and index-union bound.
        values: ir::RuntimeEqualitySet,
    },
    /// Generation-qualified range scan resolved by the database.
    Range(ExecNodeSecondaryRangePlan),
    /// Set intersection in planner-selected evaluation order.
    Intersect {
        /// First child executed.
        driver: Box<ExecNodeSecondarySetPlan>,
        /// Remaining children in exact order.
        rest: ir::AtLeast<ExecNodeSecondarySetPlan, 1>,
    },
    /// Set union in planner-selected evaluation order.
    Union {
        /// First child executed.
        driver: Box<ExecNodeSecondarySetPlan>,
        /// Remaining children in exact order.
        rest: ir::AtLeast<ExecNodeSecondarySetPlan, 1>,
    },
    /// Range-ordered intersection. Filters are fully applied before a limit may
    /// consume the ordered result.
    OrderedIntersect {
        /// Range scan that defines result order.
        driver: ExecNodeSecondaryRangePlan,
        /// Non-empty secondary filters applied to driver IDs.
        filters: ir::AtLeast<ExecNodeSecondarySetPlan, 1>,
    },
}

impl ExecNodeSecondarySetPlan {
    /// Classify one or more same-index logical equalities into exact set primitives.
    ///
    /// Indexed literals share one batched read: non-unique point reads become
    /// one `BatchedUnionRead` and unique owner reads one `UniqueUnion`, each
    /// in the place of its first member. Other members (null, runtime
    /// parameters) stay separate union children, and non-reflexive members
    /// are dropped.
    pub fn exact_equalities(
        index: catalog::NodeEqualityIndexMeta,
        key: catalog::ScopedPropertyKey,
        values: ir::AtLeast<ir::IndexValue, 1>,
    ) -> Self {
        let children = values
            .into_iter()
            .map(|value| Self::from(exact_node_equality(index.clone(), key.clone(), value)))
            .filter(|child| !matches!(child, Self::Empty))
            .collect::<Vec<_>>();
        let points = children
            .iter()
            .filter_map(|child| match child {
                Self::Bitmap(exec::ExecNodeBitmapExpr::PointRead { index, value, .. }) => {
                    Some((index.clone(), value.clone()))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let owners = children
            .iter()
            .filter_map(|child| match child {
                Self::Unique { lookup, .. } => Some((lookup.index.clone(), lookup.value.clone())),
                _ => None,
            })
            .collect::<Vec<_>>();
        let batched_points = points.len() >= 2;
        let batched_owners = owners.len() >= 2;
        let mut batch = batched_points.then(|| {
            Self::Bitmap(exec::ExecNodeBitmapExpr::BatchedUnionRead {
                index: points[0].0.clone(),
                key: key.clone(),
                values: ir::AtLeast::try_from_vec(
                    points.into_iter().map(|(_, value)| value).collect(),
                )
                .expect("a batch holds at least two point reads"),
            })
        });
        let mut owner_batch = batched_owners.then(|| Self::UniqueUnion {
            index: owners[0].0.clone(),
            key: key.clone(),
            values: ir::AtLeast::try_from_vec(owners.into_iter().map(|(_, value)| value).collect())
                .expect("a batch holds at least two owner reads"),
        });
        let mut children = children
            .into_iter()
            .filter_map(|child| match child {
                Self::Bitmap(exec::ExecNodeBitmapExpr::PointRead { .. }) if batched_points => {
                    batch.take()
                }
                Self::Unique { .. } if batched_owners => owner_batch.take(),
                child => Some(child),
            })
            .collect::<Vec<_>>();
        if children.is_empty() {
            return Self::Empty;
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
