//! Core logical expression ADTs.
//!
//! This module owns the top-level expression enum and coarse effect families.
//! Detailed payload contracts live in sibling modules and are re-exported by
//! `logical` so rules can keep using the public contract surface.

mod barrier;
mod expr;
mod pure;

pub use self::barrier::BarrierLogicalOp;
pub use self::expr::{LogicalExpr, LogicalExprKind};
pub use self::pure::{PureLogicalOp, PureLogicalOpKind};
