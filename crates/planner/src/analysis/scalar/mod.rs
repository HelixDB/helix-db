//! Scalar predicate-analysis facade.
//!
//! The public analysis contract stays small while the implementation is split
//! by proof responsibility:
//!
//! - `truth`: constant predicate evaluation.
//! - `values`: property-value equality, ordering, and literal collections.
//! - `extract`: predicate-to-literal extraction ADTs.
//! - `constraints`: conjunction contradiction accumulation.

mod constraints;
mod extract;
mod truth;
mod values;

use helix_ast::expr::Predicate;
use helix_ast::value::PropertyValue;

pub(crate) fn scalar_property_conjunction_is_impossible(predicate: &Predicate) -> bool {
    constraints::predicate_is_statically_impossible(predicate)
}

pub(crate) fn predicate_is_statically_tautological(predicate: &Predicate) -> bool {
    truth::static_predicate_value(predicate) == Some(true)
}

pub(crate) fn literal_in_values(predicate: &Predicate) -> Option<(String, Vec<PropertyValue>)> {
    extract::literal_in_values(predicate)
}

pub(super) fn predicate_is_statically_impossible(predicate: &Predicate) -> bool {
    constraints::predicate_is_statically_impossible(predicate)
}

#[cfg(test)]
pub(super) fn static_predicate_value(predicate: &Predicate) -> Option<bool> {
    truth::static_predicate_value(predicate)
}
