//! Selected access-window physical access limit pushdown.

use super::super::{contracts, kv};
use crate::{logical, physical};

pub(in crate::exec::selected::lowering) fn physical_access_with_window_limit(
    access: &physical::PhysicalAccess,
    window: logical::AccessWindowRange,
) -> contracts::WindowLimitPushdown {
    let contracts::WindowReadBound::Bounded(upper) =
        contracts::AccessReadUpperBound::from_window(window)
    else {
        return contracts::WindowLimitPushdown::Skipped(
            contracts::WindowLimitPushdownSkip::NoBoundedWindow,
        );
    };
    let physical::PhysicalAccess::Kv(read) = access else {
        return contracts::WindowLimitPushdown::Skipped(
            contracts::WindowLimitPushdownSkip::NonKvAccess,
        );
    };
    match kv::kv_read_with_upper_bound(read, upper) {
        kv::KvReadLimitPushdown::Applied(read) => {
            contracts::WindowLimitPushdown::Applied(physical::PhysicalAccess::Kv(read))
        }
        kv::KvReadLimitPushdown::Unsupported => contracts::WindowLimitPushdown::Skipped(
            contracts::WindowLimitPushdownSkip::UnsupportedKvRead,
        ),
    }
}
