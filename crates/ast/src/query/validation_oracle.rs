//! The parameter validation `query.rs` used before paths became lazy, kept as
//! the oracle its replacement must match exactly: same verdicts, same first
//! error in document order, same paths.

use super::{query_value_kind, QueryError, QueryParamType, QueryValue, MAX_REQUEST_JSON_DEPTH};
use std::collections::BTreeMap;

/// Validate a parameter value with an explicit stack, so neither its size nor
/// its nesting reaches the call stack; nesting is bounded like a request.
pub(super) fn validate_json_value(value: &QueryValue, path: &str) -> Result<(), QueryError> {
    let mut pending = vec![(value, path.to_owned(), 1_usize)];
    while let Some((value, path, depth)) = pending.pop() {
        if depth > MAX_REQUEST_JSON_DEPTH {
            return Err(QueryError::NestingTooDeep {
                path,
                maximum: MAX_REQUEST_JSON_DEPTH,
            });
        }
        match value {
            QueryValue::F64(value) if !value.is_finite() => {
                return Err(QueryError::ParameterTypeMismatch {
                    path,
                    expected: QueryParamType::Value,
                    actual: "non-finite f64",
                });
            }
            QueryValue::F32(value) if !value.is_finite() => {
                return Err(QueryError::ParameterTypeMismatch {
                    path,
                    expected: QueryParamType::Value,
                    actual: "non-finite f32",
                });
            }
            // Reverse pushes keep the first invalid element in document order.
            QueryValue::Array(values) => pending.extend(
                values
                    .iter()
                    .enumerate()
                    .rev()
                    .map(|(index, value)| (value, format!("{path}[{index}]"), depth + 1)),
            ),
            QueryValue::Object(values) => pending.extend(
                values
                    .iter()
                    .rev()
                    .map(|(name, value)| (value, format!("{path}.{name}"), depth + 1)),
            ),
            QueryValue::Null
            | QueryValue::Bool(_)
            | QueryValue::I64(_)
            | QueryValue::F64(_)
            | QueryValue::F32(_)
            | QueryValue::String(_) => {}
        }
    }
    Ok(())
}

pub(super) fn normalize_typed_value(
    ty: &QueryParamType,
    value: QueryValue,
    path: &str,
) -> Result<QueryValue, QueryError> {
    // Normalization recurses once per array level of the declared type.
    let type_depth = std::iter::successors(Some(ty), |ty| match ty {
        QueryParamType::Array(inner) => Some(inner.as_ref()),
        QueryParamType::Bool
        | QueryParamType::I64
        | QueryParamType::F64
        | QueryParamType::F32
        | QueryParamType::String
        | QueryParamType::DateTime
        | QueryParamType::Bytes
        | QueryParamType::Value
        | QueryParamType::Object => None,
    })
    .count();
    if type_depth > MAX_REQUEST_JSON_DEPTH {
        return Err(QueryError::NestingTooDeep {
            path: path.to_owned(),
            maximum: MAX_REQUEST_JSON_DEPTH,
        });
    }
    let actual = query_value_kind(&value);
    match (ty, value) {
        (QueryParamType::Bool, value @ QueryValue::Bool(_))
        | (QueryParamType::I64, value @ QueryValue::I64(_))
        | (QueryParamType::String, value @ QueryValue::String(_)) => Ok(value),
        (QueryParamType::F64, QueryValue::F64(value)) if value.is_finite() => {
            Ok(QueryValue::F64(value))
        }
        (QueryParamType::F64, QueryValue::F32(value)) if value.is_finite() => {
            Ok(QueryValue::F64(value.into()))
        }
        (QueryParamType::F64, QueryValue::I64(value)) => Ok(QueryValue::F64(value as f64)),
        (QueryParamType::F32, QueryValue::F32(value)) if value.is_finite() => {
            Ok(QueryValue::F32(value))
        }
        (QueryParamType::F32, QueryValue::F64(value))
            if value.is_finite()
                && value >= f64::from(f32::MIN)
                && value <= f64::from(f32::MAX) =>
        {
            Ok(QueryValue::F32(value as f32))
        }
        (QueryParamType::F32, QueryValue::I64(value)) => Ok(QueryValue::F32(value as f32)),
        (QueryParamType::DateTime, QueryValue::String(datetime))
            if chrono::DateTime::parse_from_rfc3339(&datetime).is_ok() =>
        {
            Ok(QueryValue::String(datetime))
        }
        (QueryParamType::Value, value) => {
            validate_json_value(&value, path)?;
            Ok(value)
        }
        (QueryParamType::Object, value @ QueryValue::Object(_)) => {
            validate_json_value(&value, path)?;
            Ok(value)
        }
        (QueryParamType::Array(inner), QueryValue::Array(values)) => values
            .into_iter()
            .enumerate()
            .map(|(index, value)| normalize_typed_value(inner, value, &format!("{path}[{index}]")))
            .collect::<Result<Vec<_>, _>>()
            .map(QueryValue::Array),
        (QueryParamType::Bytes, _) => Err(QueryError::unsupported_bytes(path)),
        (expected, _) => Err(QueryError::ParameterTypeMismatch {
            path: path.to_owned(),
            expected: expected.clone(),
            actual,
        }),
    }
}

/// Values with every scalar kind, non-finite floats at the first, middle and
/// last position of arrays and objects, and nesting around the depth limit.
fn values() -> Vec<QueryValue> {
    let nest = |depth: usize, leaf: QueryValue, object: bool| {
        (1..depth).fold(leaf, |inner, level| match object && level % 2 == 0 {
            true => QueryValue::Object(BTreeMap::from([(format!("k{level}"), inner)])),
            false => QueryValue::Array(vec![QueryValue::I64(1), inner]),
        })
    };
    let scalars = [
        QueryValue::Null,
        QueryValue::Bool(true),
        QueryValue::I64(-7),
        QueryValue::F64(1.5),
        QueryValue::F32(2.5),
        QueryValue::String("s".to_owned()),
    ];
    let bad = [
        QueryValue::F64(f64::NAN),
        QueryValue::F64(f64::INFINITY),
        QueryValue::F32(f32::NEG_INFINITY),
    ];
    let mut values = scalars.to_vec();
    values.extend(bad.iter().cloned());
    for bad in &bad {
        for position in 0..3 {
            let mut items = vec![QueryValue::I64(0), QueryValue::I64(1), QueryValue::I64(2)];
            items[position] = bad.clone();
            values.push(QueryValue::Array(items.clone()));
            values.push(QueryValue::Object(
                items
                    .into_iter()
                    .enumerate()
                    .map(|(index, value)| (format!("field{index}"), value))
                    .collect(),
            ));
        }
        // The first invalid value in document order wins over later ones and
        // over deeper ones that come later.
        values.push(QueryValue::Array(vec![
            QueryValue::Object(BTreeMap::from([
                ("a".to_owned(), QueryValue::I64(1)),
                (
                    "b".to_owned(),
                    QueryValue::Array(vec![QueryValue::Null, bad.clone()]),
                ),
            ])),
            QueryValue::F64(f64::NAN),
        ]));
    }
    for depth in [
        MAX_REQUEST_JSON_DEPTH - 1,
        MAX_REQUEST_JSON_DEPTH,
        MAX_REQUEST_JSON_DEPTH + 1,
        MAX_REQUEST_JSON_DEPTH + 5,
    ] {
        for object in [false, true] {
            values.push(nest(depth, QueryValue::Null, object));
            values.push(nest(depth, QueryValue::Array(Vec::new()), object));
            values.push(nest(depth, QueryValue::F64(f64::NAN), object));
        }
    }
    values
}

#[test]
fn lazy_value_validation_matches_the_eager_oracle() {
    for value in values() {
        assert_eq!(
            format!("{:?}", super::validate_json_value(&value, "param", 0)),
            format!("{:?}", validate_json_value(&value, "param")),
            "{value:?}"
        );
    }
}

#[test]
fn lazy_typed_normalization_matches_the_eager_oracle() {
    let array = |inner: QueryParamType| QueryParamType::Array(Box::new(inner));
    let mut types = vec![
        QueryParamType::Bool,
        QueryParamType::I64,
        QueryParamType::F64,
        QueryParamType::F32,
        QueryParamType::String,
        QueryParamType::DateTime,
        QueryParamType::Bytes,
        QueryParamType::Value,
        QueryParamType::Object,
        array(QueryParamType::F32),
        array(QueryParamType::Object),
        array(array(QueryParamType::Value)),
        array(array(QueryParamType::I64)),
    ];
    types.extend(
        [
            MAX_REQUEST_JSON_DEPTH - 1,
            MAX_REQUEST_JSON_DEPTH,
            MAX_REQUEST_JSON_DEPTH + 1,
        ]
        .map(|depth| (1..depth).fold(QueryParamType::I64, |inner, _| array(inner))),
    );
    let mut values = values();
    values.extend([
        QueryValue::String("2026-10-07T10:00:00Z".to_owned()),
        QueryValue::F64(f64::MAX),
        QueryValue::Array(vec![QueryValue::F64(1.0), QueryValue::F64(f64::MAX)]),
        QueryValue::Array(vec![
            QueryValue::Object(BTreeMap::from([("x".to_owned(), QueryValue::I64(1))])),
            QueryValue::Object(BTreeMap::from([(
                "x".to_owned(),
                QueryValue::F64(f64::NAN),
            )])),
            QueryValue::I64(3),
        ]),
        QueryValue::Array(vec![
            QueryValue::Array(vec![QueryValue::I64(1), QueryValue::I64(2)]),
            QueryValue::Array(vec![QueryValue::I64(3), QueryValue::String("x".to_owned())]),
        ]),
    ]);
    for ty in &types {
        for value in &values {
            assert_eq!(
                format!(
                    "{:?}",
                    super::normalize_typed_value(ty, value.clone(), "param")
                ),
                format!("{:?}", normalize_typed_value(ty, value.clone(), "param")),
                "{ty:?} with {value:?}"
            );
        }
    }
}
