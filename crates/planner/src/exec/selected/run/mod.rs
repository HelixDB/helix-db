//! Selected run-root contracts.
//!
//! This module is the recursive root of selected executable IR. Each variant
//! carries only selected payloads, making unselected compatibility children
//! unrepresentable once a plan has crossed the selected boundary.

mod alternative;
mod root;
#[cfg(test)]
mod tests;

pub use self::alternative::SelectedExecutableAlternativeRoot;
pub use self::root::SelectedExecutableRunRoot;
