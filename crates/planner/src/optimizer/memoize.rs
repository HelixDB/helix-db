//! Memo insertion helpers for child-aware Cascades roots.
//!
//! The optimizer evaluates rules against full logical payloads, while memo
//! child groups represent only recursive roots that must be selected and costed
//! independently. Parent-local access and variable prefixes remain embedded in
//! the parent expression, avoiding lineage-only child groups that selected
//! reconstruction would ignore.

mod contracts;
mod session;

pub(super) use contracts::QueuedMemoExpr;
pub(super) use session::MemoExpressionMemoizer;

#[cfg(test)]
mod tests;
