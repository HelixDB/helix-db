//! Selected access-window executable read-limit derivation.

use super::super::contracts;
use super::pushdown::physical_access_with_window_limit;
use crate::exec::ExecAccessReadLimit;
use crate::{logical, physical};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::exec::selected::lowering) struct WindowAccessReadPlan {
    access: physical::PhysicalAccess,
    read_limit: ExecAccessReadLimit,
    suffix: WindowSuffix,
}

impl WindowAccessReadPlan {
    pub(in crate::exec::selected::lowering) fn for_window(
        access: &physical::PhysicalAccess,
        window: logical::AccessWindowRange,
    ) -> Self {
        let read_limit =
            contracts::AccessReadUpperBound::from_window(window).into_exec_read_limit();
        let access =
            physical_access_with_window_limit(access, window).into_access_or_original(access);
        let suffix = WindowSuffix::for_window(window);
        Self {
            access,
            read_limit,
            suffix,
        }
    }

    pub(in crate::exec::selected::lowering) const fn read_limit(&self) -> ExecAccessReadLimit {
        self.read_limit
    }

    pub(in crate::exec::selected::lowering) const fn suffix(&self) -> WindowSuffix {
        self.suffix
    }

    pub(in crate::exec::selected::lowering) const fn access(&self) -> &physical::PhysicalAccess {
        &self.access
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec::selected::lowering) enum WindowSuffix {
    ElidedByReadLimit,
    Retained,
}

impl WindowSuffix {
    const fn for_window(window: logical::AccessWindowRange) -> Self {
        if window.start() == 0 && matches!(window.end(), Some(end) if end > 0) {
            Self::ElidedByReadLimit
        } else {
            Self::Retained
        }
    }
}
