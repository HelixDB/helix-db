//! Stream-bound and stream-range contract facade.
//!
//! - `bound`: non-negative literal/runtime stream bounds.
//! - `range`: statically ordered literal ranges and dynamic range bounds.

mod bound;
mod range;

pub use bound::{
    StreamBoundExpected, StreamBoundExprPlan, StreamBoundExprPlanError, StreamBoundPlan,
    StreamBoundPlanError,
};
pub use range::{StreamDynamicRange, StreamLiteralRange, StreamRangePlan, StreamRangePlanError};
