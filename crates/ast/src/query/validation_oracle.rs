//! The parameter validation `query.rs` used before paths became lazy, kept as
//! the oracle its replacement must match exactly: same verdicts, same first
//! error in document order, same paths.

use super::{
    query_value_kind, QueryError, QueryParamType, QueryValue, MAX_REQUEST_JSON_DEPTH,
    PARAMETER_JSON_DEPTH,
};
use std::collections::BTreeMap;

/// Validate a parameter value with an explicit stack, so neither its size nor
/// its nesting reaches the call stack; nesting is bounded in JSON levels of
/// the request body.
pub(super) fn validate_json_value(value: &QueryValue, path: &str) -> Result<(), QueryError> {
    validate_below(value, path, 0)
}

/// [`validate_json_value`] for a value under `levels_above` arrays and
/// objects of a typed parameter.
fn validate_below(value: &QueryValue, path: &str, levels_above: usize) -> Result<(), QueryError> {
    // `depth` is the JSON level the value opens if it is an array or object.
    let mut pending = vec![(value, path.to_owned(), PARAMETER_JSON_DEPTH + levels_above + 1)];
    while let Some((value, path, depth)) = pending.pop() {
        if matches!(value, QueryValue::Array(_) | QueryValue::Object(_))
            && depth > MAX_REQUEST_JSON_DEPTH
        {
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
            | QueryValue::DateTime(_)
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
    // The JSON levels the declared type opens: one for an array or
    // nullable, two for an object with fields.
    fn type_depth(ty: &QueryParamType) -> usize {
        match ty {
            QueryParamType::Array(inner) | QueryParamType::Nullable(inner) => 1 + type_depth(inner),
            QueryParamType::Record(fields) => {
                2 + fields
                    .iter()
                    .map(|(_, ty)| type_depth(ty))
                    .max()
                    .unwrap_or(0)
            }
            QueryParamType::Bool
            | QueryParamType::I64
            | QueryParamType::F64
            | QueryParamType::F32
            | QueryParamType::String
            | QueryParamType::DateTime
            | QueryParamType::Bytes
            | QueryParamType::Value
            | QueryParamType::Object => 0,
        }
    }
    if PARAMETER_JSON_DEPTH + type_depth(ty) > MAX_REQUEST_JSON_DEPTH {
        return Err(QueryError::NestingTooDeep {
            path: path.to_owned(),
            maximum: MAX_REQUEST_JSON_DEPTH,
        });
    }
    normalize_nested(ty, value, path, 0)
}

/// [`normalize_typed_value`] for a value under `level` declared levels.
fn normalize_nested(
    ty: &QueryParamType,
    value: QueryValue,
    path: &str,
    level: usize,
) -> Result<QueryValue, QueryError> {
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
            Ok(QueryValue::DateTime(
                chrono::DateTime::parse_from_rfc3339(&datetime)
                    .expect("guard parsed the datetime")
                    .timestamp_millis(),
            ))
        }
        (QueryParamType::DateTime, value @ QueryValue::DateTime(_)) => Ok(value),
        (QueryParamType::Nullable(_), QueryValue::Null) => Ok(QueryValue::Null),
        (QueryParamType::Nullable(inner), value) => normalize_nested(inner, value, path, level),
        (QueryParamType::Value, value) => {
            validate_below(&value, path, level)?;
            Ok(value)
        }
        (QueryParamType::Object, value @ QueryValue::Object(_)) => {
            validate_below(&value, path, level)?;
            Ok(value)
        }
        (QueryParamType::Record(fields), QueryValue::Object(values)) => {
            let present = values.keys().cloned().collect::<Vec<_>>();
            let missing_fields = fields
                .iter()
                .filter(|(field, ty)| {
                    !matches!(ty, QueryParamType::Nullable(_))
                        && !present.contains(&(*field).to_owned())
                })
                .map(|(field, _)| field.to_owned())
                .collect::<Vec<_>>();
            let extra_fields = present
                .iter()
                .filter(|field| fields.get(field).is_none())
                .cloned()
                .collect::<Vec<_>>();
            if !missing_fields.is_empty() || !extra_fields.is_empty() {
                return Err(QueryError::ObjectFieldMismatch {
                    path: path.to_owned(),
                    missing_fields,
                    extra_fields,
                });
            }
            values
                .into_iter()
                .map(|(field, value)| {
                    let ty = fields.get(&field).expect("declared field");
                    normalize_nested(ty, value, &format!("{path}.{field}"), level + 1)
                        .map(|value| (field, value))
                })
                .collect::<Result<BTreeMap<_, _>, _>>()
                .map(QueryValue::Object)
        }
        (QueryParamType::Array(inner), QueryValue::Array(values)) => values
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                normalize_nested(inner, value, &format!("{path}[{index}]"), level + 1)
            })
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
        MAX_REQUEST_JSON_DEPTH - PARAMETER_JSON_DEPTH - 1,
        MAX_REQUEST_JSON_DEPTH - PARAMETER_JSON_DEPTH,
        MAX_REQUEST_JSON_DEPTH - PARAMETER_JSON_DEPTH + 1,
        MAX_REQUEST_JSON_DEPTH - PARAMETER_JSON_DEPTH + 2,
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
    let nullable = |inner: QueryParamType| QueryParamType::Nullable(Box::new(inner));
    let record = |fields: Vec<(&str, QueryParamType)>| {
        QueryParamType::Record(super::ObjectFields::new(fields).unwrap())
    };
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
        record(vec![("x", QueryParamType::I64)]),
        record(vec![("x", QueryParamType::Value)]),
        // Omittable fields before, after and between required ones.
        record(vec![
            ("x", nullable(QueryParamType::DateTime)),
            ("y", QueryParamType::I64),
        ]),
        record(vec![
            ("w", nullable(QueryParamType::I64)),
            ("x", QueryParamType::I64),
            ("y", nullable(QueryParamType::Value)),
        ]),
        array(record(vec![
            ("w", nullable(QueryParamType::I64)),
            ("x", nullable(QueryParamType::I64)),
        ])),
        nullable(QueryParamType::DateTime),
        nullable(QueryParamType::Value),
        nullable(nullable(QueryParamType::I64)),
        nullable(array(nullable(QueryParamType::F32))),
        array(record(vec![("x", nullable(QueryParamType::DateTime))])),
        record(vec![(
            "x",
            nullable(record(vec![("y", QueryParamType::I64)])),
        )]),
        array(record(vec![("x", QueryParamType::I64)])),
        array(record(vec![("x", QueryParamType::Value)])),
        record(vec![
            ("a", QueryParamType::DateTime),
            ("b", array(record(vec![("c", QueryParamType::F32)]))),
        ]),
        record(vec![
            ("field0", QueryParamType::I64),
            ("field1", QueryParamType::F64),
            ("field2", QueryParamType::Value),
        ]),
    ];
    // Around the limit for array, nullable and object levels; an object
    // level is two JSON levels, so its declarations nest half as deep.
    for depth in [
        MAX_REQUEST_JSON_DEPTH - PARAMETER_JSON_DEPTH - 1,
        MAX_REQUEST_JSON_DEPTH - PARAMETER_JSON_DEPTH,
        MAX_REQUEST_JSON_DEPTH - PARAMETER_JSON_DEPTH + 1,
        MAX_REQUEST_JSON_DEPTH - PARAMETER_JSON_DEPTH + 2,
        (MAX_REQUEST_JSON_DEPTH - PARAMETER_JSON_DEPTH) / 2,
        (MAX_REQUEST_JSON_DEPTH - PARAMETER_JSON_DEPTH) / 2 + 1,
        (MAX_REQUEST_JSON_DEPTH - PARAMETER_JSON_DEPTH) / 2 + 2,
    ] {
        types.push((1..depth).fold(QueryParamType::I64, |inner, _| array(inner)));
        // A nullable level counts toward the declared type's depth.
        types.push(nullable(
            (1..depth).fold(QueryParamType::I64, |inner, _| array(inner)),
        ));
        // Object levels count like array levels, in either branch.
        types.push(
            (1..depth).fold(QueryParamType::Value, |inner, level| match level % 2 == 0 {
                true => record(vec![(format!("k{level}").as_str(), inner)]),
                false => record(vec![("a", QueryParamType::I64), ("z", inner)]),
            }),
        );
    }
    let object = |fields: Vec<(&str, QueryValue)>| {
        QueryValue::Object(
            fields
                .into_iter()
                .map(|(name, value)| (name.to_owned(), value))
                .collect(),
        )
    };
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
        object(vec![("x", QueryValue::I64(1))]),
        object(vec![("x", QueryValue::Null)]),
        object(vec![(
            "x",
            QueryValue::String("2026-10-07T10:00:00+02:00".to_owned()),
        )]),
        object(vec![("x", object(vec![("y", QueryValue::I64(1))]))]),
        object(vec![("x", QueryValue::I64(1)), ("y", QueryValue::I64(2))]),
        object(vec![("y", QueryValue::I64(2))]),
        object(vec![]),
        QueryValue::Array(vec![
            object(vec![("x", QueryValue::I64(1))]),
            object(vec![("x", QueryValue::String("1".to_owned()))]),
            object(vec![("w", QueryValue::I64(1))]),
        ]),
        object(vec![
            (
                "a",
                QueryValue::String("2026-10-07T10:00:00+02:00".to_owned()),
            ),
            (
                "b",
                QueryValue::Array(vec![
                    object(vec![("c", QueryValue::F64(0.5))]),
                    object(vec![("c", QueryValue::F64(f64::MAX))]),
                ]),
            ),
        ]),
        object(vec![
            ("a", QueryValue::String("2026-10-07".to_owned())),
            ("b", QueryValue::Array(Vec::new())),
        ]),
        object(vec![
            ("a", QueryValue::String("2026-10-07T10:00:00Z".to_owned())),
            ("b", QueryValue::Array(vec![object(vec![])])),
        ]),
    ]);
    // Values nested to match the deep object declarations, around the limit.
    for depth in [
        MAX_REQUEST_JSON_DEPTH - PARAMETER_JSON_DEPTH - 1,
        MAX_REQUEST_JSON_DEPTH - PARAMETER_JSON_DEPTH,
        MAX_REQUEST_JSON_DEPTH - PARAMETER_JSON_DEPTH + 1,
        (MAX_REQUEST_JSON_DEPTH - PARAMETER_JSON_DEPTH) / 2,
        (MAX_REQUEST_JSON_DEPTH - PARAMETER_JSON_DEPTH) / 2 + 1,
        MAX_REQUEST_JSON_DEPTH,
        MAX_REQUEST_JSON_DEPTH + 1,
    ] {
        values.push(
            (1..depth).fold(QueryValue::Null, |inner, level| match level % 2 == 0 {
                true => object(vec![(format!("k{level}").as_str(), inner)]),
                false => object(vec![("a", QueryValue::I64(1)), ("z", inner)]),
            }),
        );
    }
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
