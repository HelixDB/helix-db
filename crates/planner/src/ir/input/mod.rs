//! Validated runtime-input contracts.
//!
//! Input contracts sit at the boundary where AST payloads become executable IR.
//! They normalize static constants into literal arms, keep runtime expressions
//! in expression-only arms, and reject payloads whose shape cannot be valid for
//! the target operation.
//!
//! - `property` owns generic mutation/search property inputs.
//! - `search` owns vector/text query payload contracts.

mod property;
mod search;

pub use property::{PropertyInputExprPlan, PropertyInputExprPlanError, PropertyInputPlan};
pub use search::{
    SearchQueryExprPlan, SearchQueryExprPlanError, SearchQueryInputExpected,
    SearchQueryInputPlanError, SearchVector, SearchVectorComponent, SearchVectorError,
    TextQueryInputPlan, VectorQueryInputPlan,
};

#[cfg(test)]
mod tests;
