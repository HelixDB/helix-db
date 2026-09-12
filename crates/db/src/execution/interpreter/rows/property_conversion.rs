//! Preflight the owned Cypher representation before allocating its containers.
//! Unsupported stored types produce one dormant error, without constructing a
//! partial list/map or copying unobserved payloads along the failing branch.
use crate::encoding::v2::values::property::property_value::PropertyValue as P;
use helix_planner::relational as r;
use std::collections::BTreeMap;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy)]
enum Rejection {
    StoredType,
    Nesting,
}
impl Rejection {
    fn description(self) -> (&'static str, &'static str, &'static str) {
        match self {
            Self::StoredType => (
                "UnsupportedFeature",
                "StoredValueType",
                "temporal and binary stored values are outside the MVP profile",
            ),
            Self::Nesting => (
                "ResourceLimit",
                "StoredValueNestingLimit",
                "stored property value exceeds the query value nesting limit",
            ),
        }
    }
}

pub(super) struct Conversion {
    value: P,
    bytes: Result<usize, Rejection>,
}
impl Conversion {
    pub(super) fn new(value: P) -> Self {
        let bytes = owned_bytes(&value, 0);
        Self { value, bytes }
    }
    pub(super) fn owned_bytes(&self) -> usize {
        self.bytes.unwrap_or_else(|error| {
            let (category, detail, message) = error.description();
            category.len() + detail.len() + message.len()
        })
    }
    pub(super) fn finish(self) -> r::Result<r::Value> {
        self.bytes.map_err(|error| {
            let (category, detail, message) = error.description();
            r::QueryError::runtime(category, detail, message)
        })?;
        Ok(convert(self.value))
    }
}

fn owned_bytes(value: &P, depth: usize) -> Result<usize, Rejection> {
    // Stored properties enter the same row-value domain as frontend literals.
    // Check before descending or allocating, while retaining dormant errors so
    // keys() and unobserved CASE branches do not force the property's value.
    if depth >= r::MAX_EXPRESSION_DEPTH {
        return Err(Rejection::Nesting);
    }
    let array_bytes = |len: usize| {
        if len > 0 && depth + 1 >= r::MAX_EXPRESSION_DEPTH {
            Err(Rejection::Nesting)
        } else {
            Ok(len.saturating_mul(size_of::<r::Value>()))
        }
    };
    Ok(match value {
        P::Null | P::Bool(_) | P::I64(_) | P::F64(_) | P::F32(_) => 0,
        P::String(value) => value.capacity(),
        P::I64Array(values) => array_bytes(values.len())?,
        P::F64Array(values) => array_bytes(values.len())?,
        P::F32Array(values) => array_bytes(values.len())?,
        P::StringArray(values) => values
            .iter()
            .fold(array_bytes(values.len())?, |bytes, value| {
                bytes.saturating_add(value.capacity())
            }),
        P::Array(values) => values.iter().try_fold(
            values.len().saturating_mul(size_of::<r::Value>()),
            |bytes, value| Ok(bytes.saturating_add(owned_bytes(value, depth + 1)?)),
        )?,
        P::Object(values) => values.iter().try_fold(
            r::allocation::btree_bytes::<String, r::Value>(values.len()),
            |bytes, (key, value)| {
                Ok(bytes
                    .saturating_add(key.capacity())
                    .saturating_add(owned_bytes(value, depth + 1)?))
            },
        )?,
        P::DateTime(_) | P::Bytes(_) => return Err(Rejection::StoredType),
    })
}

fn convert(value: P) -> r::Value {
    match value {
        P::Null => r::Value::Null,
        P::Bool(value) => r::Value::Boolean(value),
        P::I64(value) => r::Value::Integer(value),
        P::F64(value) | P::F32(value) => r::Value::Float(value),
        P::String(value) => r::Value::String(value),
        P::Array(values) => r::Value::List(values.into_iter().map(convert).collect()),
        // Direct insertion avoids BTreeMap::from_iter's temporary sorted vector.
        // The preflight bounds insertion-grown map nodes and moved payloads.
        P::Object(values) => r::Value::Map(values.into_iter().fold(
            BTreeMap::new(),
            |mut output, (key, value)| {
                output.insert(key, convert(value));
                output
            },
        )),
        P::I64Array(values) => r::Value::List(values.into_iter().map(r::Value::Integer).collect()),
        P::F64Array(values) => r::Value::List(values.into_iter().map(r::Value::Float).collect()),
        P::F32Array(values) => r::Value::List(
            values
                .into_iter()
                .map(|value| r::Value::Float(f64::from(value)))
                .collect(),
        ),
        P::StringArray(values) => {
            r::Value::List(values.into_iter().map(r::Value::String).collect())
        }
        P::DateTime(_) | P::Bytes(_) => {
            unreachable!("conversion preflight rejects unsupported subtrees")
        }
    }
}
