//! Selected executable IR reconstruction.
//!
//! This module owns the contract from a chosen physical alternative plus its
//! source logical expression into selected executable IR. Child-bearing selected
//! roots must resolve children through memo-child provenance from the optimizer
//! result that selected the parent; selected lowering does not start fallback
//! optimizer searches for missing children.

mod case;
mod dispatch;
mod flow;
mod memo_children;
mod mutation;
mod root;
mod stream;
mod terminal;

#[cfg(test)]
mod tests;
