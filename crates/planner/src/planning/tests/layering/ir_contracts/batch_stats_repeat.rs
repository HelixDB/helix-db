use super::*;

#[test]
fn index_union_branch_limit_encodes_disabled_and_limited_modes() {
    let disabled = IndexUnionBranchLimit::from_usize(0);
    assert_eq!(disabled, IndexUnionBranchLimit::Disabled);
    assert!(NonZeroUsize::new(0).is_none());
    assert_eq!(serde_json::to_string(&disabled).unwrap(), "0");
    assert_eq!(
        serde_json::from_str::<IndexUnionBranchLimit>("0").unwrap(),
        disabled
    );

    let limited = IndexUnionBranchLimit::limited(2).unwrap();
    assert_eq!(limited, IndexUnionBranchLimit::from_usize(2));
    assert_eq!(
        limited,
        IndexUnionBranchLimit::Limited(NonZeroUsize::new(2).unwrap())
    );
    assert_eq!(serde_json::to_string(&limited).unwrap(), "2");
    assert_eq!(
        serde_json::from_str::<IndexUnionBranchLimit>("2").unwrap(),
        limited
    );
    assert!(serde_json::from_str::<IndexUnionBranchLimit>(r#""two""#).is_err());
    assert!(IndexUnionBranchLimit::limited(0).is_none());
}

#[test]
fn return_plan_separates_empty_and_non_empty_returns() {
    let variables = ReturnVariables::new(AtLeast::<_, 1>::from_one_and_rest(
        NonEmptyString::new("users").unwrap(),
        vec![NonEmptyString::new("posts").unwrap()],
    ))
    .unwrap();
    assert_eq!(
        variables.as_ref(),
        &[
            NonEmptyString::new("users").unwrap(),
            NonEmptyString::new("posts").unwrap()
        ]
    );
    let returns = ReturnPlan::Variables(variables);

    assert_eq!(
        serde_json::to_string(&ReturnPlan::None).unwrap(),
        r#""none""#
    );
    assert_eq!(
        serde_json::to_string(&returns).unwrap(),
        r#"{"variables":["users","posts"]}"#
    );
    assert_eq!(
        serde_json::from_str::<ReturnPlan>(&serde_json::to_string(&returns).unwrap()).unwrap(),
        returns
    );
    assert!(serde_json::from_str::<ReturnPlan>(r#"{"variables":[]}"#).is_err());
    assert!(serde_json::from_str::<ReturnPlan>(r#"{"variables":[""]}"#).is_err());
    assert!(serde_json::from_str::<ReturnPlan>(r#"{"variables":["users","users"]}"#).is_err());
    let duplicate = ReturnVariables::new(AtLeast::<_, 1>::from_one_and_rest(
        NonEmptyString::new("users").unwrap(),
        vec![NonEmptyString::new("users").unwrap()],
    ))
    .unwrap_err();
    assert_eq!(duplicate.to_string(), "duplicate return variable `users`");
}

#[test]
fn stats_snapshot_keeps_node_and_edge_label_cardinality_separate() {
    let stats = StatsSnapshot::default()
        .with_node_label_cardinality(NonEmptyString::new("Account").unwrap(), 10)
        .with_edge_label_cardinality(NonEmptyString::new("Account").unwrap(), 200);
    let label = NonEmptyString::new("Account").unwrap();

    assert_eq!(stats.node_label_cardinality.get(&label), Some(&10));
    assert_eq!(stats.edge_label_cardinality.get(&label), Some(&200));

    let serialized = serde_json::to_string(&stats).unwrap();
    let parsed: StatsSnapshot = serde_json::from_str(&serialized).unwrap();
    assert_eq!(parsed, stats);
}

#[test]
fn repeat_plan_uses_positive_depth_and_iteration_counts() {
    let stop = RepeatStopPlan::Times {
        count: NonZeroUsize::new(2).unwrap(),
    };
    assert_eq!(
        serde_json::to_string(&stop).unwrap(),
        r#"{"times":{"count":2}}"#
    );
    assert!(serde_json::from_str::<RepeatStopPlan>(r#"{"times":{"count":0}}"#).is_err());

    let plan = RepeatPlan {
        body: Box::new(PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)),
        stop: RepeatStopPlan::MaxDepthOnly,
        emit: RepeatEmitPlan::None,
        max_depth: NonZeroUsize::new(2).unwrap(),
    };
    let mut serialized = serde_json::to_value(&plan).unwrap();
    serialized["max_depth"] = serde_json::json!(0);
    assert!(serde_json::from_value::<RepeatPlan>(serialized).is_err());
}
