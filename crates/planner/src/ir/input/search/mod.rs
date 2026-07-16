//! Vector/text search input contracts.
//!
//! Search inputs separate the shared expected-shape/error vocabulary from
//! runtime expression payloads and the two literal contracts. This keeps vector
//! and text query validation independently testable while preserving the
//! stable `ir::*` facade.

mod expr;
mod query;
mod text;
mod vector;

pub use expr::{SearchQueryExprPlan, SearchQueryExprPlanError};
pub use query::{SearchQueryInputExpected, SearchQueryInputPlanError};
pub use text::TextQueryInputPlan;
pub use vector::{SearchVector, SearchVectorComponent, SearchVectorError, VectorQueryInputPlan};
