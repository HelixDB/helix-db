//! Native reserved terminal payload validation.

use helix_ast::value::PropertyValue;

use super::names;
use crate::{error, ir};

pub(super) fn fold() -> ir::ReservedOp {
    ir::ReservedOp::Fold
}

pub(super) fn unfold() -> ir::ReservedOp {
    ir::ReservedOp::Unfold
}

pub(super) fn path() -> ir::ReservedOp {
    ir::ReservedOp::Path
}

pub(super) fn simple_path() -> ir::ReservedOp {
    ir::ReservedOp::SimplePath
}

pub(super) fn with_sack(initial: &PropertyValue) -> ir::ReservedOp {
    ir::ReservedOp::WithSack(initial.clone())
}

pub(super) fn sack_set(property: &str) -> Result<ir::ReservedOp, error::PlannerError> {
    names::non_empty(property, ir::NameField::Property).map(ir::ReservedOp::SackSet)
}

pub(super) fn sack_add(property: &str) -> Result<ir::ReservedOp, error::PlannerError> {
    names::non_empty(property, ir::NameField::Property).map(ir::ReservedOp::SackAdd)
}

pub(super) fn sack_get() -> ir::ReservedOp {
    ir::ReservedOp::SackGet
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_payloads_preserve_variants() {
        assert!(matches!(fold(), ir::ReservedOp::Fold));
        assert!(matches!(unfold(), ir::ReservedOp::Unfold));
        assert!(matches!(path(), ir::ReservedOp::Path));
        assert!(matches!(simple_path(), ir::ReservedOp::SimplePath));
        assert!(matches!(
            with_sack(&PropertyValue::from(7)),
            ir::ReservedOp::WithSack(value) if value == PropertyValue::from(7)
        ));
        assert!(matches!(sack_get(), ir::ReservedOp::SackGet));
    }

    #[test]
    fn reserved_payloads_validate_sack_properties() {
        assert!(matches!(
            sack_set("score").unwrap(),
            ir::ReservedOp::SackSet(property) if property.as_ref() == "score"
        ));
        assert!(matches!(
            sack_add("score").unwrap(),
            ir::ReservedOp::SackAdd(property) if property.as_ref() == "score"
        ));
        assert!(matches!(
            sack_set(""),
            Err(error::PlannerError::InvalidEmptyName {
                field: ir::NameField::Property
            })
        ));
        assert!(matches!(
            sack_add(""),
            Err(error::PlannerError::InvalidEmptyName {
                field: ir::NameField::Property
            })
        ));
    }
}
