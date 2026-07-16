//! Stable unsupported-shape reasons at the native selected-planning boundary.

mod planner;
mod reason;

pub(in crate::planning::selected::native) use planner::unsupported;
pub(in crate::planning::selected::native) use reason::NativeUnsupportedReason;

#[cfg(test)]
mod tests;
