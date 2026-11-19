//! AST and query value conversion contracts.
//!
//! A nonempty list whose members are all datetimes becomes
//! [`DbPropertyValue::DateTimeArray`] at any depth, the one form every write
//! path stores for such a list, so equality indexes accept it and it equals
//! the same list written through Cypher or SQL. Any other list keeps the
//! generic array form.

use helix_ast::query::QueryValue;
use helix_ast::value::PropertyValue as AstPropertyValue;

use super::*;

pub(in crate::execution::interpreter) fn ast_to_db_value(
    value: AstPropertyValue,
) -> DbPropertyValue {
    match value {
        AstPropertyValue::Null => DbPropertyValue::Null,
        AstPropertyValue::Bool(value) => DbPropertyValue::Bool(value),
        AstPropertyValue::I64(value) => DbPropertyValue::I64(value),
        AstPropertyValue::DateTime(value) => DbPropertyValue::DateTime(value),
        AstPropertyValue::F64(value) => DbPropertyValue::F64(value),
        AstPropertyValue::F32(value) => DbPropertyValue::F32(value.into()),
        AstPropertyValue::String(value) => DbPropertyValue::String(value),
        AstPropertyValue::Bytes(value) => DbPropertyValue::Bytes(value),
        AstPropertyValue::I64Array(value) => DbPropertyValue::I64Array(value),
        AstPropertyValue::F64Array(value) => DbPropertyValue::F64Array(value),
        AstPropertyValue::F32Array(value) => DbPropertyValue::F32Array(value),
        AstPropertyValue::StringArray(value) => DbPropertyValue::StringArray(value),
        AstPropertyValue::Array(values) => {
            if !values.is_empty()
                && values
                    .iter()
                    .all(|value| matches!(value, AstPropertyValue::DateTime(_)))
            {
                // Exactly sized: count literals admit this conversion's bytes.
                let mut millis = Vec::with_capacity(values.len());
                millis.extend(
                    values
                        .iter()
                        .filter_map(AstPropertyValue::as_datetime_millis),
                );
                return DbPropertyValue::DateTimeArray(millis);
            }
            DbPropertyValue::Array(values.into_iter().map(ast_to_db_value).collect())
        }
        AstPropertyValue::Object(value) => DbPropertyValue::Object(
            value
                .into_iter()
                .map(|(name, value)| (name, ast_to_db_value(value)))
                .collect(),
        ),
    }
}

pub(super) fn query_value_to_db_value(value: QueryValue) -> DbPropertyValue {
    match value {
        QueryValue::Null => DbPropertyValue::Null,
        QueryValue::Bool(value) => DbPropertyValue::Bool(value),
        QueryValue::I64(value) => DbPropertyValue::I64(value),
        QueryValue::F64(value) => DbPropertyValue::F64(value),
        QueryValue::F32(value) => DbPropertyValue::F32(value.into()),
        QueryValue::DateTime(value) => DbPropertyValue::DateTime(value),
        QueryValue::String(value) => DbPropertyValue::String(value),
        QueryValue::Array(values) => {
            if !values.is_empty()
                && values
                    .iter()
                    .all(|value| matches!(value, QueryValue::DateTime(_)))
            {
                let mut millis = Vec::with_capacity(values.len());
                millis.extend(values.iter().filter_map(|value| {
                    let QueryValue::DateTime(millis) = value else {
                        return None;
                    };
                    Some(*millis)
                }));
                return DbPropertyValue::DateTimeArray(millis);
            }
            DbPropertyValue::Array(values.into_iter().map(query_value_to_db_value).collect())
        }
        QueryValue::Object(values) => DbPropertyValue::Object(
            values
                .into_iter()
                .map(|(name, value)| (name, query_value_to_db_value(value)))
                .collect(),
        ),
    }
}
