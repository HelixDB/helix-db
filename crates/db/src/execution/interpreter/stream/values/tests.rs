mod conversion;
mod params;
mod scalars;

use std::collections::BTreeMap;

use helix_ast::query::QueryValue;
use helix_ast::value::PropertyValue as AstPropertyValue;

use super::super::super::{ExecutionScalar, ExecutionValue};
use super::*;
use super::{conversion as value_conversion, params as value_params, scalars as value_scalars};

fn name(value: &str) -> ir::NonEmptyString {
    ir::NonEmptyString::new(value).expect("valid test name")
}
