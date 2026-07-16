//! Scalar conjunction contradiction accumulation.

mod bounds;
mod collect;
mod property;

use helix_ast::expr::Predicate;

pub(super) fn predicate_is_statically_impossible(predicate: &Predicate) -> bool {
    collect::predicate_is_statically_impossible(predicate)
}
