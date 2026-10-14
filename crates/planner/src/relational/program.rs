//! Common dependency contract for expressions attached to row programs.
use super::{Expression, Result, Slot};
use std::collections::BTreeSet;

/// Resolved expression dependency boundary. Implementations validate their
/// own operation domain before recursive dependency traversal.
pub trait ExpressionInput {
    fn validate(&self) -> Result<()>;
    fn references(&self) -> BTreeSet<Slot>;
}

impl ExpressionInput for Expression {
    fn validate(&self) -> Result<()> {
        self.validate_shape()
    }
    fn references(&self) -> BTreeSet<Slot> {
        self.slots()
    }
}
