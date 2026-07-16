//! Stream-window and search-limit bound contracts.
//!
//! This facade keeps the public `ir::*` API stable while the invariant-heavy
//! bound families live in focused modules:
//!
//! - `stream`: non-negative stream bounds and validated stream ranges.
//! - `search`: positive result-count limits for vector/text search.

mod search;
mod stream;

pub use search::{
    SearchLimitExpected, SearchLimitExprPlan, SearchLimitExprPlanError, SearchLimitPlan,
    SearchLimitPlanError,
};
pub use stream::{
    StreamBoundExpected, StreamBoundExprPlan, StreamBoundExprPlanError, StreamBoundPlan,
    StreamBoundPlanError, StreamDynamicRange, StreamLiteralRange, StreamRangePlan,
    StreamRangePlanError,
};
