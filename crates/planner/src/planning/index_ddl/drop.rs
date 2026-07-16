//! Drop-index DDL payload validation facade.
//!
//! Drop specs keep only index identity fields. Secondary and search index
//! families are split so create-only attributes cannot bleed into drop payloads.

mod search;
mod secondary;

use helix_ast::index::IndexSpec;

use crate::{error, ir};

pub(in crate::planning) fn index_ddl_drop_spec(
    spec: &IndexSpec,
) -> Result<ir::IndexDdlDropSpec, error::PlannerError> {
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
            label, property, ..
        } => search::node_vector(label, property),
        IndexSpec::NodeText {
            label, property, ..
        } => search::node_text(label, property),
        IndexSpec::EdgeVector {
            label, property, ..
        } => search::edge_vector(label, property),
        IndexSpec::EdgeText {
            label, property, ..
        } => search::edge_text(label, property),
    }
}

#[cfg(test)]
mod tests;
