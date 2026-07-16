//! Create-index DDL payload validation facade.
//!
//! Secondary and search indexes have different create-time invariants:
//! equality/range indexes own uniqueness/direction while vector/text indexes
//! own dimensions, metrics, and tenant scope. This facade keeps the AST dispatch
//! narrow and leaves those contracts in independently tested modules.

mod search;
mod secondary;

use helix_ast::index::IndexSpec;

use crate::{error, ir};

pub(in crate::planning) fn index_ddl_create_spec(
    spec: &IndexSpec,
) -> Result<ir::IndexDdlCreateSpec, error::PlannerError> {
    match spec {
        IndexSpec::NodeEquality {
            label,
            property,
            unique,
        } => secondary::node_equality(label, property, *unique),
        IndexSpec::NodeRange {
            label,
            property,
            direction,
        } => secondary::node_range(label, property, *direction),
        IndexSpec::EdgeEquality { label, property } => secondary::edge_equality(label, property),
        IndexSpec::EdgeRange {
            label,
            property,
            direction,
        } => secondary::edge_range(label, property, *direction),
        IndexSpec::NodeVector {
            label,
            property,
            dimension,
            metric,
            tenant_property,
        } => search::node_vector(label, property, *dimension, *metric, tenant_property),
        IndexSpec::NodeText {
            label,
            property,
            tenant_property,
        } => search::node_text(label, property, tenant_property),
        IndexSpec::EdgeVector {
            label,
            property,
            dimension,
            metric,
            tenant_property,
        } => search::edge_vector(label, property, *dimension, *metric, tenant_property),
        IndexSpec::EdgeText {
            label,
            property,
            tenant_property,
        } => search::edge_text(label, property, tenant_property),
    }
}

#[cfg(test)]
mod tests;
