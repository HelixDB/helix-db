use crate::exec::{ExecEdgeAccessPlan, ExecPlanError};
use crate::{catalog, ir, properties};

#[derive(Debug)]
pub(in crate::exec) enum SimpleEdgeAccessLeaf<'a> {
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
        index: &'a catalog::EdgeEqualityIndexMeta,
        key: &'a catalog::ScopedPropertyKey,
        value: &'a ir::IndexValue,
    },
    RangeIndex {
        index: &'a catalog::EdgeRangeIndexMeta,
        key: &'a catalog::ScopedPropertyDirectionKey,
        range: &'a ir::IndexRange,
        iteration: &'a ir::RangeScanIteration,
    },
    VectorSearch {
        key: &'a catalog::EdgeSearchIndexKey,
        index: &'a ir::SearchIndexPlan,
        query_vector: &'a ir::VectorQueryInputPlan,
        k: &'a ir::SearchLimitPlan,
    },
    TextSearch {
        key: &'a catalog::EdgeSearchIndexKey,
        index: &'a ir::SearchIndexPlan,
        query_text: &'a ir::TextQueryInputPlan,
        k: &'a ir::SearchLimitPlan,
        /// Maximum edit distance for keyword matching. Zero is exact.
        fuzzy_distance: u8,
    },
}

impl<'a> TryFrom<&'a ir::EdgeAccessPlan> for SimpleEdgeAccessLeaf<'a> {
    type Error = ExecPlanError;

    fn try_from(plan: &'a ir::EdgeAccessPlan) -> Result<Self, Self::Error> {
        match plan {
            ir::EdgeAccessPlan::Empty => Ok(Self::Empty),
            ir::EdgeAccessPlan::FromParam { param } => Ok(Self::FromParam { param }),
            ir::EdgeAccessPlan::FromVar { variable } => Ok(Self::FromVar { variable }),
            ir::EdgeAccessPlan::AllScan => Ok(Self::AllScan),
            ir::EdgeAccessPlan::LabelScan { label } => Ok(Self::LabelScan { label }),
            ir::EdgeAccessPlan::EqualityIndex { index, key, value } => {
                Ok(Self::EqualityIndex { index, key, value })
            }
            ir::EdgeAccessPlan::RangeIndex {
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
            ir::EdgeAccessPlan::VectorSearch {
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
            ir::EdgeAccessPlan::TextSearch {
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
            ir::EdgeAccessPlan::PointIds { .. }
            | ir::EdgeAccessPlan::Intersect(_)
            | ir::EdgeAccessPlan::Union(_)
            | ir::EdgeAccessPlan::ScanThenFilter { .. } => {
                Err(ExecPlanError::UnsupportedSimpleAccessLeaf {
                    element: properties::ElementKind::Edge,
                })
            }
        }
    }
}

pub(in crate::exec) fn edge_exec_access(plan: SimpleEdgeAccessLeaf<'_>) -> ExecEdgeAccessPlan {
    match plan {
        SimpleEdgeAccessLeaf::Empty => ExecEdgeAccessPlan::Empty,
        SimpleEdgeAccessLeaf::FromParam { param } => ExecEdgeAccessPlan::FromParam {
            param: param.clone(),
        },
        SimpleEdgeAccessLeaf::FromVar { variable } => ExecEdgeAccessPlan::FromVar {
            variable: variable.clone(),
        },
        SimpleEdgeAccessLeaf::AllScan => ExecEdgeAccessPlan::AllScan,
        SimpleEdgeAccessLeaf::LabelScan { label } => ExecEdgeAccessPlan::LabelScan {
            label: label.clone(),
        },
        SimpleEdgeAccessLeaf::EqualityIndex { index, key, value } => {
            ExecEdgeAccessPlan::exact_equality(index.clone(), key.clone(), value.clone())
        }
        SimpleEdgeAccessLeaf::RangeIndex {
            index,
            key,
            range,
            iteration,
        } => ExecEdgeAccessPlan::RangeIndex {
            iteration: *iteration,
            index: index.clone(),
            key: key.clone(),
            range: range.clone(),
        },
        SimpleEdgeAccessLeaf::VectorSearch {
            key,
            index,
            query_vector,
            k,
        } => ExecEdgeAccessPlan::VectorSearch {
            key: key.clone(),
            index: index.clone(),
            query_vector: query_vector.clone(),
            k: k.clone(),
        },
        SimpleEdgeAccessLeaf::TextSearch {
            key,
            index,
            query_text,
            k,
            fuzzy_distance,
        } => ExecEdgeAccessPlan::TextSearch {
            key: key.clone(),
            index: index.clone(),
            query_text: query_text.clone(),
            k: k.clone(),
            fuzzy_distance,
        },
    }
}
