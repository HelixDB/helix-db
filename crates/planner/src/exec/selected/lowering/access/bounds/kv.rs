//! KV read limit tightening for selected access windows.

use super::contracts;
use crate::exec::KvReadPlan;
use crate::properties;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum KvReadLimitPushdown {
    Applied(KvReadPlan),
    Unsupported,
}

pub(super) fn kv_read_with_upper_bound(
    read: &KvReadPlan,
    upper: contracts::AccessReadUpperBound,
) -> KvReadLimitPushdown {
    match read {
        KvReadPlan::RangeScan {
            keyspace,
            start,
            end,
            limit,
        } => KvReadLimitPushdown::Applied(KvReadPlan::RangeScan {
            keyspace: *keyspace,
            start: start.clone(),
            end: end.clone(),
            limit: Some(tightest_limit(*limit, upper.as_limit())),
        }),
        KvReadPlan::PrefixScan {
            keyspace,
            prefix,
            limit,
        } => KvReadLimitPushdown::Applied(KvReadPlan::PrefixScan {
            keyspace: *keyspace,
            prefix: prefix.clone(),
            limit: Some(tightest_limit(*limit, upper.as_limit())),
        }),
        KvReadPlan::MultiGet(batch) => batch
            .prefix_by_original_position(upper.as_limit())
            .map(KvReadPlan::MultiGet)
            .map(KvReadLimitPushdown::Applied)
            .unwrap_or(KvReadLimitPushdown::Unsupported),
        KvReadPlan::Get { .. } => KvReadLimitPushdown::Unsupported,
    }
}

fn tightest_limit(
    existing: Option<properties::PositiveUsize>,
    upper: properties::PositiveUsize,
) -> properties::PositiveUsize {
    existing
        .filter(|existing| existing <= &upper)
        .unwrap_or(upper)
}
