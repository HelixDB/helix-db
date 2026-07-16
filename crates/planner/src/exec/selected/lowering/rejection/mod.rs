//! Stable unsupported-shape reasons at the selected executable-lowering boundary.

mod planner;
mod reason;

#[cfg(test)]
mod tests;

pub(in crate::exec::selected::lowering) use planner::unsupported;
pub(in crate::exec) use reason::Reason;
