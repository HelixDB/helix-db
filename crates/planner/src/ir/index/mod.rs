//! Index and range contract ADTs.
//!
//! These modules encode search-index metadata, secondary-index lookup
//! literals, range-index literals, and static range proofs used by access
//! planning rules.

mod equality;
mod range;
mod search;

pub use equality::{IndexValue, SecondaryIndexLiteral, SecondaryIndexLiteralError};
pub use range::{
    BoundInclusivity, IndexBetweenRange, IndexBound, IndexRange, RangeIndexF32, RangeIndexF64,
    RangeIndexLiteral, RangeIndexValue,
};
pub use search::{
    SearchIndexPlan, SearchTenantPlan, SearchTenantValuePlan, SearchTenantValuePlanError,
};
