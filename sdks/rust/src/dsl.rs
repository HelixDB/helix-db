//! Rust SDK query DSL.
//!
//! The DSL surface is implemented by `helix-ast` and re-exported here so
//! applications can continue to import `helix_db::dsl::prelude::*`. Builders
//! now construct a nested AST directly; no public `Step` array is emitted.

use std::collections::HashMap;

pub use helix_ast::prelude::{
    g, read_batch, sub, write_batch, AggregateFunction, AstNode, BatchCondition, BatchEntry,
    BatchQuery, BindingProjection, BindingTarget, BindingValueRef, CompareOp, DateTime, EdgeId,
    EdgeRef, EmitBehavior, Empty, Expr, ExprProjection, IndexSpec, NamedQuery, NodeId, NodeRef,
    OnEdges, OnNodes, Order, ParamObject, ParamValue, Predicate, Projection, PropertyInput,
    PropertyProjection, PropertyValue, QueryError, QueryParamType, QueryRequest, QueryRequestType,
    QueryValue, RangeIndexDirection, ReadBatch, ReadOnly, RepeatConfig, ShortestPathDirection,
    SourcePredicate, StreamBound, SubTraversal, Terminal, Traversal, TraversalState,
    VectorDistanceMetric, WhenThen, WriteBatch, WriteEnabled,
};
pub use helix_dsl_macros::query;

/// Private helpers used by the `#[query]` macro.
#[doc(hidden)]
pub mod __private {
    use std::collections::BTreeMap;

    pub fn query_value_from_property_value(
        value: crate::PropertyValue,
        path: impl Into<String>,
    ) -> Result<crate::QueryValue, crate::QueryError> {
        fn convert(
            value: crate::PropertyValue,
            path: String,
        ) -> Result<crate::QueryValue, crate::QueryError> {
            Ok(match value {
                crate::PropertyValue::Null => crate::QueryValue::Null,
                crate::PropertyValue::Bool(value) => crate::QueryValue::Bool(value),
                crate::PropertyValue::I64(value) => crate::QueryValue::I64(value),
                crate::PropertyValue::DateTime(value) => crate::QueryValue::String(
                    crate::DateTime::from_millis(value)
                        .to_rfc3339()
                        .ok_or_else(|| crate::QueryError::invalid_datetime(path, value))?,
                ),
                crate::PropertyValue::F64(value) => crate::QueryValue::F64(value),
                crate::PropertyValue::F32(value) => crate::QueryValue::F32(value),
                crate::PropertyValue::String(value) => crate::QueryValue::String(value),
                crate::PropertyValue::Bytes(_) => {
                    return Err(crate::QueryError::unsupported_bytes(path));
                }
                crate::PropertyValue::I64Array(values) => crate::QueryValue::Array(
                    values.into_iter().map(crate::QueryValue::I64).collect(),
                ),
                crate::PropertyValue::F64Array(values) => crate::QueryValue::Array(
                    values.into_iter().map(crate::QueryValue::F64).collect(),
                ),
                crate::PropertyValue::F32Array(values) => crate::QueryValue::Array(
                    values.into_iter().map(crate::QueryValue::F32).collect(),
                ),
                crate::PropertyValue::StringArray(values) => crate::QueryValue::Array(
                    values.into_iter().map(crate::QueryValue::String).collect(),
                ),
                crate::PropertyValue::Array(values) => crate::QueryValue::Array(
                    values
                        .into_iter()
                        .enumerate()
                        .map(|(index, value)| convert(value, format!("{}[{}]", path, index)))
                        .collect::<Result<Vec<_>, _>>()?,
                ),
                crate::PropertyValue::Object(values) => crate::QueryValue::Object(
                    values
                        .into_iter()
                        .map(|(key, value)| {
                            let entry_path = format!("{}.{}", path, key);
                            Ok((key, convert(value, entry_path)?))
                        })
                        .collect::<Result<BTreeMap<_, _>, crate::QueryError>>()?,
                ),
            })
        }

        convert(value, path.into())
    }
}

/// Common query-builder imports.
#[allow(missing_docs)]
pub mod prelude {
    pub use crate::lifecycle::{
        IndexDdlReceipt, IndexErrorCode, IndexFamily, IndexOperationBlockerCode,
        IndexOperationKind, IndexOperationProgress, IndexOperationStage, IndexOperationStatus,
        IndexOperationStatusCommon,
    };
    pub use crate::{
        g, query, read_batch, sub, write_batch, AggregateFunction, AstNode, BatchCondition,
        BatchEntry, BatchQuery, BindingProjection, BindingTarget, BindingValueRef, CompareOp,
        DateTime, EdgeId, EdgeRef, EmitBehavior, Empty, Expr, ExprProjection, IndexSpec,
        NamedQuery, NodeId, NodeRef, OnEdges, OnNodes, Order, ParamObject, ParamValue, Predicate,
        Projection, PropertyInput, PropertyProjection, PropertyValue, QueryError, QueryParamType,
        QueryRequest, QueryRequestType, QueryValue, RangeIndexDirection, ReadBatch, ReadOnly,
        RepeatConfig, ShortestPathDirection, SourcePredicate, StreamBound, SubTraversal, Terminal,
        Traversal, TraversalState, VectorDistanceMetric, WhenThen, WriteBatch, WriteEnabled,
    };
}

/// Helper type alias for property maps.
#[doc(hidden)]
pub type PropertyMap = HashMap<String, PropertyValue>;
