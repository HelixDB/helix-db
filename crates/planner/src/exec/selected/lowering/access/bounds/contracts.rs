//! Window-limit pushdown outcome contracts.

use crate::exec::ExecAccessReadLimit;
use crate::{logical, physical, properties};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct AccessReadUpperBound(properties::PositiveUsize);

impl AccessReadUpperBound {
    pub(super) fn from_window(window: logical::AccessWindowRange) -> WindowReadBound {
        match window.end().and_then(properties::PositiveUsize::new) {
            Some(limit) => WindowReadBound::Bounded(Self(limit)),
            None => WindowReadBound::Unbounded,
        }
    }

    pub(super) const fn as_limit(self) -> properties::PositiveUsize {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WindowReadBound {
    Unbounded,
    Bounded(AccessReadUpperBound),
}

impl WindowReadBound {
    pub(super) const fn into_exec_read_limit(self) -> ExecAccessReadLimit {
        match self {
            Self::Unbounded => ExecAccessReadLimit::Unbounded,
            Self::Bounded(upper) => ExecAccessReadLimit::bounded(upper.as_limit()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(in crate::exec::selected::lowering) enum WindowLimitPushdown {
    Applied(physical::PhysicalAccess),
    Skipped(WindowLimitPushdownSkip),
}

impl WindowLimitPushdown {
    pub(in crate::exec::selected::lowering) fn into_access_or_original(
        self,
        original: &physical::PhysicalAccess,
    ) -> physical::PhysicalAccess {
        match self {
            Self::Applied(access) => access,
            Self::Skipped(_) => original.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec::selected::lowering) enum WindowLimitPushdownSkip {
    NoBoundedWindow,
    NonKvAccess,
    UnsupportedKvRead,
}
