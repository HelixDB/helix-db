use serde::{Deserialize, Serialize};

use crate::{catalog, ir};

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
    /// Node equality-index lookup.
    EqualityIndex {
        /// Index metadata.
        index: catalog::NodeEqualityIndexMeta,
        /// Indexed property key.
        key: catalog::ScopedPropertyKey,
        /// Lookup value.
        value: ir::IndexValue,
    },
    /// Node range-index scan.
    RangeIndex {
        /// Index metadata.
        index: catalog::NodeRangeIndexMeta,
        /// Indexed property key and direction.
        key: catalog::ScopedPropertyDirectionKey,
        /// Range bounds.
        range: ir::IndexRange,
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
