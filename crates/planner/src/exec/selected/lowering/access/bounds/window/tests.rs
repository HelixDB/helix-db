use super::super::contracts;
use super::pushdown::physical_access_with_window_limit;
use super::read_limit::{WindowAccessReadPlan, WindowSuffix};
use crate::exec::{ElementKeyspace, ExecAccessReadLimit, KvKeyBound, KvMultiGetPlan, KvReadPlan};
use crate::{ir, logical, physical, properties};

fn range_scan(limit: Option<properties::PositiveUsize>) -> physical::PhysicalAccess {
    physical::PhysicalAccess::Kv(KvReadPlan::RangeScan {
        keyspace: ElementKeyspace::NodeProperty,
        start: KvKeyBound::Unbounded,
        end: KvKeyBound::Unbounded,
        limit,
    })
}

fn prefix_scan() -> physical::PhysicalAccess {
    physical::PhysicalAccess::Kv(KvReadPlan::PrefixScan {
        keyspace: ElementKeyspace::NodeProperty,
        prefix: ir::AtLeast::<_, 1>::try_from_vec(vec![1]).unwrap(),
        limit: None,
    })
}

fn get() -> physical::PhysicalAccess {
    physical::PhysicalAccess::Kv(KvReadPlan::Get {
        key: ElementKeyspace::NodeProperty.point_key(7),
    })
}

fn multi_get(ids: &[u64]) -> physical::PhysicalAccess {
    physical::PhysicalAccess::Kv(KvReadPlan::MultiGet(
        KvMultiGetPlan::new(
            ids.iter()
                .map(|id| ElementKeyspace::NodeProperty.point_key(*id))
                .collect(),
            properties::KeyLocality::Close,
            properties::PositiveUsize::new(ids.len()).unwrap(),
        )
        .unwrap(),
    ))
}

fn applied_access(pushdown: contracts::WindowLimitPushdown) -> physical::PhysicalAccess {
    match pushdown {
        contracts::WindowLimitPushdown::Applied(access) => access,
        contracts::WindowLimitPushdown::Skipped(reason) => {
            panic!("expected applied pushdown, got {reason:?}")
        }
    }
}

fn skipped_reason(pushdown: contracts::WindowLimitPushdown) -> contracts::WindowLimitPushdownSkip {
    match pushdown {
        contracts::WindowLimitPushdown::Applied(access) => {
            panic!("expected skipped pushdown, got {access:?}")
        }
        contracts::WindowLimitPushdown::Skipped(reason) => reason,
    }
}

#[test]
fn window_limit_pushdown_uses_tightest_positive_bound() {
    let window = logical::AccessWindowRange::new(2, Some(8)).expect("window is valid");
    let bounded = applied_access(physical_access_with_window_limit(&range_scan(None), window));

    assert!(matches!(
        bounded,
        physical::PhysicalAccess::Kv(KvReadPlan::RangeScan {
            limit: Some(limit),
            ..
        }) if limit.get() == 8
    ));

    let existing = range_scan(Some(properties::PositiveUsize::new(3).unwrap()));
    let bounded = applied_access(physical_access_with_window_limit(&existing, window));

    assert!(matches!(
        bounded,
        physical::PhysicalAccess::Kv(KvReadPlan::RangeScan {
            limit: Some(limit),
            ..
        }) if limit.get() == 3
    ));
}

#[test]
fn window_limit_pushdown_declines_zero_or_unbounded_windows() {
    let zero = logical::AccessWindowRange::new(0, Some(0)).expect("empty window is valid");
    assert_eq!(
        skipped_reason(physical_access_with_window_limit(&range_scan(None), zero)),
        contracts::WindowLimitPushdownSkip::NoBoundedWindow
    );

    let unbounded = logical::AccessWindowRange::new(2, None).expect("skip window is valid");
    assert_eq!(
        skipped_reason(physical_access_with_window_limit(
            &range_scan(None),
            unbounded
        )),
        contracts::WindowLimitPushdownSkip::NoBoundedWindow
    );
}

#[test]
fn window_limit_pushdown_supports_prefix_scans() {
    let window = logical::AccessWindowRange::new(0, Some(6)).expect("window is valid");
    let bounded = applied_access(physical_access_with_window_limit(&prefix_scan(), window));

    assert!(matches!(
        bounded,
        physical::PhysicalAccess::Kv(KvReadPlan::PrefixScan {
            limit: Some(limit),
            ..
        }) if limit.get() == 6
    ));
}

#[test]
fn window_limit_pushdown_supports_multi_get_logical_prefixes() {
    let window = logical::AccessWindowRange::new(0, Some(2)).expect("window is valid");
    let bounded = applied_access(physical_access_with_window_limit(
        &multi_get(&[30, 10, 20]),
        window,
    ));

    assert!(matches!(
        bounded,
        physical::PhysicalAccess::Kv(KvReadPlan::MultiGet(batch))
            if batch.keys().iter().map(crate::exec::KvKey::id).collect::<Vec<_>>() == vec![10, 30]
                && batch.original_positions() == [1, 0]
    ));
}

#[test]
fn window_limit_pushdown_reports_non_kv_access() {
    let window = logical::AccessWindowRange::new(0, Some(6)).expect("window is valid");

    assert_eq!(
        skipped_reason(physical_access_with_window_limit(
            &physical::PhysicalAccess::LabelScan,
            window,
        )),
        contracts::WindowLimitPushdownSkip::NonKvAccess
    );
}

#[test]
fn window_limit_pushdown_reports_unsupported_kv_reads() {
    let window = logical::AccessWindowRange::new(0, Some(6)).expect("window is valid");

    assert_eq!(
        skipped_reason(physical_access_with_window_limit(&get(), window)),
        contracts::WindowLimitPushdownSkip::UnsupportedKvRead
    );
}

#[test]
fn window_access_read_plan_encodes_limit_access_and_suffix_contract() {
    let unbounded = logical::AccessWindowRange::new(2, None).expect("window is valid");
    let plan = WindowAccessReadPlan::for_window(&range_scan(None), unbounded);
    assert_eq!(plan.read_limit(), ExecAccessReadLimit::Unbounded);
    assert_eq!(plan.suffix(), WindowSuffix::Retained);

    let skip_then_limit = logical::AccessWindowRange::new(2, Some(5)).expect("window is valid");
    let plan = WindowAccessReadPlan::for_window(&range_scan(None), skip_then_limit);
    assert!(matches!(
        plan.read_limit(),
        ExecAccessReadLimit::Bounded(limit) if limit.get() == 5
    ));
    assert_eq!(plan.suffix(), WindowSuffix::Retained);
    assert!(matches!(
        plan.access(),
        physical::PhysicalAccess::Kv(KvReadPlan::RangeScan {
            limit: Some(limit),
            ..
        }) if limit.get() == 5
    ));

    let prefix_limit = logical::AccessWindowRange::new(0, Some(5)).expect("window is valid");
    let plan = WindowAccessReadPlan::for_window(&range_scan(None), prefix_limit);
    assert!(matches!(
        plan.read_limit(),
        ExecAccessReadLimit::Bounded(limit) if limit.get() == 5
    ));
    assert_eq!(plan.suffix(), WindowSuffix::ElidedByReadLimit);

    let empty = logical::AccessWindowRange::new(0, Some(0)).expect("window is valid");
    let plan = WindowAccessReadPlan::for_window(&range_scan(None), empty);
    assert_eq!(plan.read_limit(), ExecAccessReadLimit::Unbounded);
    assert_eq!(plan.suffix(), WindowSuffix::Retained);
}
