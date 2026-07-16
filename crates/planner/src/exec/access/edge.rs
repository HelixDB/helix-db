use serde::{Deserialize, Serialize};

use crate::{catalog, ir};

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
    /// Edge equality-index lookup.
    EqualityIndex {
        /// Index metadata.
        index: catalog::EdgeEqualityIndexMeta,
        /// Indexed property key.
        key: catalog::ScopedPropertyKey,
        /// Lookup value.
        value: ir::IndexValue,
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
