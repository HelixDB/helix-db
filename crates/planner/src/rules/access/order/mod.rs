//! Access ordering and distinctness rewrites.

mod direction;
mod distinct;
mod rules;
mod satisfaction;

pub use rules::{
    AccessDistinctImplementationRule, AccessDistinctRule, AccessOrderImplementationRule,
    AccessOrderRangeDirectionRule, AccessOrderRule,
};

pub(in crate::rules::access) use direction::{
    rewrite_access_order_range_direction, AccessOrderRangeDirectionRewrite,
};
pub(in crate::rules::access) use distinct::access_distinct_is_noop;
pub(in crate::rules::access) use satisfaction::{
    access_order_satisfaction, AccessOrderSatisfaction,
};
