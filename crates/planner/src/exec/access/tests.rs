use super::*;
use crate::properties;

#[test]
fn limited_access_flattens_nested_limits_to_tightest_bound() {
    let access = ExecAccessPlan::Node(ExecNodeAccessPlan::AllScan)
        .limited(properties::PositiveUsize::new(10).unwrap())
        .limited(properties::PositiveUsize::new(3).unwrap());

    let ExecAccessPlan::Limited(limited) = access else {
        panic!("expected limited access wrapper");
    };
    assert_eq!(limited.limit().get(), 3);
    assert!(matches!(
        limited.source(),
        ExecAccessPlan::Node(ExecNodeAccessPlan::AllScan)
    ));
}

#[test]
fn access_read_limit_applies_only_when_bounded() {
    let unbounded =
        ExecAccessReadLimit::Unbounded.apply_to(ExecAccessPlan::Node(ExecNodeAccessPlan::AllScan));
    assert!(matches!(
        unbounded,
        ExecAccessPlan::Node(ExecNodeAccessPlan::AllScan)
    ));

    let bounded = ExecAccessReadLimit::bounded(properties::PositiveUsize::new(4).unwrap())
        .apply_to(ExecAccessPlan::Node(ExecNodeAccessPlan::AllScan));
    assert!(matches!(
        bounded,
        ExecAccessPlan::Limited(limited) if limited.limit().get() == 4
    ));
}

#[test]
fn access_read_limit_elides_when_access_hard_upper_is_tighter() {
    let limit = ExecAccessReadLimit::bounded(properties::PositiveUsize::new(4).unwrap());

    assert_eq!(limit.elide_if_covered_by_hard_upper(None), limit);
    assert_eq!(limit.elide_if_covered_by_hard_upper(Some(8)), limit);
    assert_eq!(
        limit.elide_if_covered_by_hard_upper(Some(4)),
        ExecAccessReadLimit::Unbounded
    );
    assert_eq!(
        limit.elide_if_covered_by_hard_upper(Some(1)),
        ExecAccessReadLimit::Unbounded
    );
    assert_eq!(
        limit.elide_if_covered_by_hard_upper(Some(0)),
        ExecAccessReadLimit::Unbounded
    );
    assert_eq!(
        ExecAccessReadLimit::Unbounded.elide_if_covered_by_hard_upper(Some(1)),
        ExecAccessReadLimit::Unbounded
    );
}
