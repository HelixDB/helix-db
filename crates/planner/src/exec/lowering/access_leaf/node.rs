use crate::exec::{ExecNodeAccessPlan, ExecPlanError};
use crate::{catalog, ir, properties};

#[derive(Debug)]
pub(in crate::exec) enum SimpleNodeAccessLeaf<'a> {
    Empty,
    FromParam {
        param: &'a ir::NonEmptyString,
    },
    FromVar {
        variable: &'a ir::NonEmptyString,
    },
    AllScan,
    LabelScan {
        label: &'a ir::NonEmptyString,
    },
    EqualityIndex {
        index: &'a catalog::NodeEqualityIndexMeta,
        key: &'a catalog::ScopedPropertyKey,
        value: &'a ir::IndexValue,
    },
    RangeIndex {
        index: &'a catalog::NodeRangeIndexMeta,
        key: &'a catalog::ScopedPropertyDirectionKey,
        range: &'a ir::IndexRange,
        iteration: &'a ir::RangeScanIteration,
    },
    VectorSearch {
        key: &'a catalog::NodeSearchIndexKey,
        index: &'a ir::SearchIndexPlan,
        query_vector: &'a ir::VectorQueryInputPlan,
        k: &'a ir::SearchLimitPlan,
    },
    TextSearch {
        key: &'a catalog::NodeSearchIndexKey,
        index: &'a ir::SearchIndexPlan,
        query_text: &'a ir::TextQueryInputPlan,
        k: &'a ir::SearchLimitPlan,
        /// Maximum edit distance for keyword matching. Zero is exact.
        fuzzy_distance: u8,
    },
}

impl<'a> TryFrom<&'a ir::NodeAccessPlan> for SimpleNodeAccessLeaf<'a> {
    type Error = ExecPlanError;

    fn try_from(plan: &'a ir::NodeAccessPlan) -> Result<Self, Self::Error> {
        match plan {
            ir::NodeAccessPlan::Empty => Ok(Self::Empty),
            ir::NodeAccessPlan::FromParam { param } => Ok(Self::FromParam { param }),
            ir::NodeAccessPlan::FromVar { variable } => Ok(Self::FromVar { variable }),
            ir::NodeAccessPlan::AllScan => Ok(Self::AllScan),
            ir::NodeAccessPlan::LabelScan { label } => Ok(Self::LabelScan { label }),
            ir::NodeAccessPlan::EqualityIndex { index, key, value } => {
                Ok(Self::EqualityIndex { index, key, value })
            }
            ir::NodeAccessPlan::RangeIndex {
                index,
                key,
                range,
                iteration,
            } => Ok(Self::RangeIndex {
                index,
                key,
                range,
                iteration,
            }),
            ir::NodeAccessPlan::VectorSearch {
                key,
                index,
                query_vector,
                k,
            } => Ok(Self::VectorSearch {
                key,
                index,
                query_vector,
                k,
            }),
            ir::NodeAccessPlan::TextSearch {
                key,
                index,
                query_text,
                k,
                fuzzy_distance,
            } => Ok(Self::TextSearch {
                key,
                index,
                query_text,
                k,
                fuzzy_distance: *fuzzy_distance,
            }),
            ir::NodeAccessPlan::PointIds { .. }
            | ir::NodeAccessPlan::Intersect(_)
            | ir::NodeAccessPlan::Union(_)
            | ir::NodeAccessPlan::ScanThenFilter { .. } => {
                Err(ExecPlanError::UnsupportedSimpleAccessLeaf {
                    element: properties::ElementKind::Node,
                })
            }
        }
    }
}

pub(in crate::exec) fn node_exec_access(plan: SimpleNodeAccessLeaf<'_>) -> ExecNodeAccessPlan {
    match plan {
        SimpleNodeAccessLeaf::Empty => ExecNodeAccessPlan::Empty,
        SimpleNodeAccessLeaf::FromParam { param } => ExecNodeAccessPlan::FromParam {
            param: param.clone(),
        },
        SimpleNodeAccessLeaf::FromVar { variable } => ExecNodeAccessPlan::FromVar {
            variable: variable.clone(),
        },
        SimpleNodeAccessLeaf::AllScan => ExecNodeAccessPlan::AllScan,
        SimpleNodeAccessLeaf::LabelScan { label } => ExecNodeAccessPlan::LabelScan {
            label: label.clone(),
        },
        SimpleNodeAccessLeaf::EqualityIndex { index, key, value } => {
            ExecNodeAccessPlan::exact_equality(index.clone(), key.clone(), value.clone())
        }
        SimpleNodeAccessLeaf::RangeIndex {
            index,
            key,
            range,
            iteration,
        } => ExecNodeAccessPlan::RangeIndex {
            iteration: *iteration,
            index: index.clone(),
            key: key.clone(),
            range: range.clone(),
        },
        SimpleNodeAccessLeaf::VectorSearch {
            key,
            index,
            query_vector,
            k,
        } => ExecNodeAccessPlan::VectorSearch {
            key: key.clone(),
            index: index.clone(),
            query_vector: query_vector.clone(),
            k: k.clone(),
        },
        SimpleNodeAccessLeaf::TextSearch {
            key,
            index,
            query_text,
            k,
            fuzzy_distance,
        } => ExecNodeAccessPlan::TextSearch {
            key: key.clone(),
            index: index.clone(),
            query_text: query_text.clone(),
            k: k.clone(),
            fuzzy_distance,
        },
    }
}
