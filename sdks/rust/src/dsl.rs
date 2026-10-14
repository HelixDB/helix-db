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
    QueryValue, RangeIndexDirection, ReadBatch, ReadOnly, RepeatConfig, SearchConsistency,
    ShortestPathDirection, SourcePredicate, StreamBound, SubTraversal, Terminal, Traversal,
    TraversalState, VectorDistanceMetric, WhenThen, WriteBatch, WriteEnabled,
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
        RepeatConfig, SearchConsistency, ShortestPathDirection, SourcePredicate, StreamBound,
        SubTraversal, Terminal, Traversal, TraversalState, VectorDistanceMetric, WhenThen,
        WriteBatch, WriteEnabled,
    };
}

/// Helper type alias for property maps.
#[doc(hidden)]
pub type PropertyMap = HashMap<String, PropertyValue>;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn property_values_convert_recursively_without_losing_shape() {
        let value = PropertyValue::Object(BTreeMap::from([
            ("null".to_string(), PropertyValue::Null),
            ("bool".to_string(), PropertyValue::Bool(true)),
            ("integer".to_string(), PropertyValue::I64(7)),
            ("datetime".to_string(), PropertyValue::DateTime(0)),
            ("f64".to_string(), PropertyValue::F64(1.5)),
            ("f32".to_string(), PropertyValue::F32(2.5)),
            (
                "string".to_string(),
                PropertyValue::String("value".to_string()),
            ),
            (
                "arrays".to_string(),
                PropertyValue::Array(vec![
                    PropertyValue::I64Array(vec![1, 2]),
                    PropertyValue::F64Array(vec![1.0, 2.0]),
                    PropertyValue::F32Array(vec![3.0, 4.0]),
                    PropertyValue::StringArray(vec!["a".to_string(), "b".to_string()]),
                ]),
            ),
        ]));

        assert_eq!(
            __private::query_value_from_property_value(value, "root").unwrap(),
            QueryValue::Object(BTreeMap::from([
                ("null".to_string(), QueryValue::Null),
                ("bool".to_string(), QueryValue::Bool(true)),
                ("integer".to_string(), QueryValue::I64(7)),
                (
                    "datetime".to_string(),
                    QueryValue::String("1970-01-01T00:00:00.000Z".to_string()),
                ),
                ("f64".to_string(), QueryValue::F64(1.5)),
                ("f32".to_string(), QueryValue::F32(2.5)),
                (
                    "string".to_string(),
                    QueryValue::String("value".to_string()),
                ),
                (
                    "arrays".to_string(),
                    QueryValue::Array(vec![
                        QueryValue::Array(vec![QueryValue::I64(1), QueryValue::I64(2)]),
                        QueryValue::Array(vec![QueryValue::F64(1.0), QueryValue::F64(2.0)]),
                        QueryValue::Array(vec![QueryValue::F32(3.0), QueryValue::F32(4.0)]),
                        QueryValue::Array(vec![
                            QueryValue::String("a".to_string()),
                            QueryValue::String("b".to_string()),
                        ]),
                    ]),
                ),
            ]))
        );
    }

    #[test]
    fn property_conversion_reports_the_exact_nested_bytes_and_datetime_path() {
        let bytes = __private::query_value_from_property_value(
            PropertyValue::Array(vec![PropertyValue::Object(BTreeMap::from([(
                "payload".to_string(),
                PropertyValue::Bytes(vec![1, 2, 3]),
            )]))]),
            "parameter",
        )
        .unwrap_err();
        assert!(matches!(
            bytes,
            QueryError::UnsupportedBytesParameter(path)
                if path == "parameter[0].payload"
        ));

        let datetime = __private::query_value_from_property_value(
            PropertyValue::Object(BTreeMap::from([(
                "created_at".to_string(),
                PropertyValue::DateTime(i64::MAX),
            )])),
            "parameter",
        )
        .unwrap_err();
        assert!(matches!(
            datetime,
            QueryError::InvalidDateTimeParameter { path, millis }
                if path == "parameter.created_at" && millis == i64::MAX
        ));
    }
}
