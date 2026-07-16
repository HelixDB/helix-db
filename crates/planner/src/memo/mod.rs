//! Cascades memo contract facade.
//!
//! The memo surface is split by invariant family:
//!
//! - `ids`: non-zero memo and physical-alternative IDs.
//! - `children`: ordered child-group lineage.
//! - `expression`: arity-checked logical expression plus child groups.
//! - `records`: memo expression/group records and mutation errors.
//! - `identity`: stable expression digests.
//! - `index`: private dense-ID lookup indexes rebuilt from validated records.
//! - `store`: the mutable memo container.

mod children;
mod expression;
mod identity;
mod ids;
mod index;
mod records;
mod store;

#[cfg(test)]
mod tests;

pub use children::MemoChildGroups;
pub use expression::{MemoExpression, MemoExpressionArityError};
pub use identity::expression_digest;
pub use ids::{MemoExprId, MemoGroupId, PhysicalAlternativeId};
pub use records::{BestPlan, InsertedMemoExpr, MemoError, MemoExpr, MemoGroup};
pub use store::Memo;
