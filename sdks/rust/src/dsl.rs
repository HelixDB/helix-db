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
    ObjectFields, OnEdges, OnNodes, Order, ParamObject, ParamValue, Predicate, Projection,
    PropertyInput, PropertyProjection, PropertyValue, QueryError, QueryParamType, QueryRequest,
    QueryRequestType, QueryValue, RangeIndexDirection, ReadBatch, ReadOnly, RepeatConfig,
    SearchConsistency, ShortestPathDirection, SourcePredicate, StreamBound, SubTraversal, Terminal,
    Traversal, TraversalState, VectorDistanceMetric, WhenThen, WriteBatch, WriteEnabled,
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
                // Kept a datetime: `declared_type` declares it, so it stays one
                // on the server rather than arriving as RFC3339 text.
                crate::PropertyValue::DateTime(value) => {
                    crate::DateTime::from_millis(value)
                        .to_rfc3339()
                        .ok_or_else(|| crate::QueryError::invalid_datetime(path, value))?;
                    crate::QueryValue::DateTime(value)
                }
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

    /// The type `#[query]` declares for a parameter converted under its
    /// static `declared` type.
    ///
    /// JSON carries a datetime as RFC3339 text, which the server keeps as text
    /// unless its position is declared `date_time`. So a value holding
    /// datetimes inside an untyped `value` or `object` declaration is declared
    /// with the narrowest type that keeps each one a datetime: objects with
    /// their fields (`value` for fields without datetimes) and arrays with
    /// one element type that fits every element. An empty array fits any
    /// element type. A value holding no datetime keeps `declared`.
    ///
    /// Rows may differ by omitted keys: an object field that is `null` or
    /// missing in some elements is declared `{"nullable": ...}`, which the
    /// server accepts as `null` or omitted and keeps omitted, so each row is
    /// stored as it was given.
    ///
    /// # Errors
    ///
    /// [`crate::QueryError::ParameterTypeMismatch`] at the first array element
    /// no single element type fits together with the elements before it,
    /// such as a datetime in one row and text in another under the same key,
    /// or an object holding a datetime where another row holds an array.
    pub fn declared_type(
        declared: crate::QueryParamType,
        value: &crate::QueryValue,
        name: &str,
    ) -> Result<crate::QueryParamType, crate::QueryError> {
        /// What a value needs from its declaration, joined across the
        /// elements of an array.
        #[derive(Debug, PartialEq)]
        enum Shape<'v> {
            /// The elements of an empty array, which fit any declaration.
            Unconstrained,
            /// `null`, which fits `value` and any nullable type.
            Null,
            /// A value without datetimes, which `value` keeps as sent.
            Plain,
            DateTime,
            Array(Box<Shape<'v>>),
            Object(BTreeMap<&'v str, Shape<'v>>),
            /// `null` in some elements and this shape in the others.
            Nullable(Box<Shape<'v>>),
        }
        #[derive(Clone, Copy)]
        enum Segment<'v> {
            Index(usize),
            Field(&'v str),
        }
        /// The declaration a shape needs, or `None` when it holds no datetime.
        fn lower(shape: &Shape<'_>) -> Option<crate::QueryParamType> {
            match shape {
                Shape::Unconstrained | Shape::Null | Shape::Plain => None,
                Shape::DateTime => Some(crate::QueryParamType::DateTime),
                Shape::Array(element) => {
                    lower(element).map(|ty| crate::QueryParamType::Array(Box::new(ty)))
                }
                Shape::Nullable(inner) => {
                    lower(inner).map(|ty| crate::QueryParamType::Nullable(Box::new(ty)))
                }
                Shape::Object(fields) => {
                    let types = fields
                        .iter()
                        .map(|(field, shape)| (*field, shape, lower(shape)))
                        .collect::<Vec<_>>();
                    if types.iter().all(|(_, _, ty)| ty.is_none()) {
                        return None;
                    }
                    // A field `null` or omitted in some rows is declared
                    // nullable, so the server accepts it omitted too.
                    let fields = crate::ObjectFields::new(types.into_iter().map(
                        |(field, shape, ty)| match shape {
                            Shape::Null | Shape::Nullable(_) => (
                                field,
                                ty.unwrap_or(crate::QueryParamType::Nullable(Box::new(
                                    crate::QueryParamType::Value,
                                ))),
                            ),
                            _ => (field, ty.unwrap_or(crate::QueryParamType::Value)),
                        },
                    ))
                    .expect("an object's field names are distinct and at least one is typed");
                    Some(crate::QueryParamType::Record(fields))
                }
            }
        }
        /// Where two shapes conflict: the fields leading to it inside the
        /// element, outermost first, the declaration the earlier elements
        /// need there, and what the new element holds.
        struct Conflict<'v> {
            fields: Vec<&'v str>,
            expected: crate::QueryParamType,
            actual: &'static str,
        }
        /// The shape both `joined`, from earlier elements, and `next` fit.
        fn join<'v>(joined: Shape<'v>, next: Shape<'v>) -> Result<Shape<'v>, Conflict<'v>> {
            match (joined, next) {
                (Shape::Unconstrained, other) | (other, Shape::Unconstrained) => Ok(other),
                (joined, next) if joined == next => Ok(joined),
                (Shape::Null, Shape::Nullable(inner)) | (Shape::Nullable(inner), Shape::Null) => {
                    Ok(Shape::Nullable(inner))
                }
                (Shape::Null, other) | (other, Shape::Null) => {
                    Ok(Shape::Nullable(Box::new(other)))
                }
                (Shape::Nullable(joined), Shape::Nullable(next)) => {
                    join(*joined, *next).map(|inner| Shape::Nullable(Box::new(inner)))
                }
                (Shape::Nullable(joined), next) => {
                    join(*joined, next).map(|inner| Shape::Nullable(Box::new(inner)))
                }
                (joined, Shape::Nullable(next)) => {
                    join(joined, *next).map(|inner| Shape::Nullable(Box::new(inner)))
                }
                (Shape::Array(joined), Shape::Array(next)) => {
                    join(*joined, *next).map(|element| Shape::Array(Box::new(element)))
                }
                // A key one object omits is absent there, which joins like
                // `null`: the field is declared nullable.
                (Shape::Object(mut joined), Shape::Object(mut next)) => joined
                    .keys()
                    .chain(next.keys())
                    .copied()
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .map(|field| {
                        join(
                            joined.remove(field).unwrap_or(Shape::Null),
                            next.remove(field).unwrap_or(Shape::Null),
                        )
                        .map(|shape| (field, shape))
                        .map_err(|mut conflict| {
                            conflict.fields.insert(0, field);
                            conflict
                        })
                    })
                    .collect::<Result<BTreeMap<_, _>, _>>()
                    .map(Shape::Object),
                // Differing shapes fit `value` together unless one needs a
                // datetime declared.
                (joined, next) => match (lower(&joined), lower(&next)) {
                    (None, None) => Ok(Shape::Plain),
                    (Some(expected), _) => Err(Conflict {
                        fields: Vec::new(),
                        expected,
                        actual: match next {
                            Shape::DateTime => "date_time",
                            Shape::Array(_) => "array",
                            Shape::Object(_) => "object",
                            Shape::Unconstrained
                            | Shape::Null
                            | Shape::Plain
                            | Shape::Nullable(_) => "a value without datetimes",
                        },
                    }),
                    (None, Some(_)) => Err(Conflict {
                        fields: Vec::new(),
                        expected: crate::QueryParamType::Value,
                        actual: "a datetime where earlier elements hold none",
                    }),
                },
            }
        }
        fn shape<'v>(
            value: &'v crate::QueryValue,
            name: &str,
            segments: &mut Vec<Segment<'v>>,
        ) -> Result<Shape<'v>, crate::QueryError> {
            match value {
                crate::QueryValue::DateTime(_) => Ok(Shape::DateTime),
                crate::QueryValue::Null => Ok(Shape::Null),
                crate::QueryValue::Bool(_)
                | crate::QueryValue::I64(_)
                | crate::QueryValue::F64(_)
                | crate::QueryValue::F32(_)
                | crate::QueryValue::String(_) => Ok(Shape::Plain),
                crate::QueryValue::Object(fields) => fields
                    .iter()
                    .map(|(field, value)| {
                        segments.push(Segment::Field(field));
                        let field_shape = shape(value, name, segments);
                        segments.pop();
                        field_shape.map(|field_shape| (field.as_str(), field_shape))
                    })
                    .collect::<Result<BTreeMap<_, _>, _>>()
                    .map(Shape::Object),
                crate::QueryValue::Array(values) => values
                    .iter()
                    .enumerate()
                    .try_fold(Shape::Unconstrained, |joined, (index, value)| {
                        segments.push(Segment::Index(index));
                        let element = shape(value, name, segments)?;
                        let joined = join(joined, element).map_err(|conflict| {
                            let path = segments
                                .iter()
                                .copied()
                                .chain(conflict.fields.into_iter().map(Segment::Field))
                                .fold(name.to_owned(), |mut path, segment| {
                                    match segment {
                                        Segment::Index(index) => {
                                            path.push_str(&format!("[{index}]"))
                                        }
                                        Segment::Field(field) => {
                                            path.push('.');
                                            path.push_str(field);
                                        }
                                    }
                                    path
                                });
                            crate::QueryError::ParameterTypeMismatch {
                                path,
                                expected: conflict.expected,
                                actual: conflict.actual,
                            }
                        })?;
                        segments.pop();
                        Ok(joined)
                    })
                    .map(|element| Shape::Array(Box::new(element))),
            }
        }
        Ok(lower(&shape(value, name, &mut Vec::new())?).unwrap_or(declared))
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
        NamedQuery, NodeId, NodeRef, ObjectFields, OnEdges, OnNodes, Order, ParamObject,
        ParamValue, Predicate, Projection, PropertyInput, PropertyProjection, PropertyValue,
        QueryError, QueryParamType, QueryRequest, QueryRequestType, QueryValue,
        RangeIndexDirection, ReadBatch, ReadOnly, RepeatConfig, SearchConsistency,
        ShortestPathDirection, SourcePredicate, StreamBound, SubTraversal, Terminal, Traversal,
        TraversalState, VectorDistanceMetric, WhenThen, WriteBatch, WriteEnabled,
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
                ("datetime".to_string(), QueryValue::DateTime(0)),
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
