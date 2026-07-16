use super::*;
use crate::ir::NonEmptyString;

#[test]
fn trace_event_rejects_empty_paths() {
    assert!(TraceEvent::try_new(
        TracePass::AccessPath,
        "",
        TraceDecision::NodeAllScan,
        TraceReason::NodeRefAll,
    )
    .is_none());

    let event = TraceEvent::try_new(
        TracePass::SelectedHandoff,
        "entry[0]",
        TraceDecision::SelectedRunRoot,
        TraceReason::SelectedRootFamily(NonEmptyString::new("alternative").unwrap()),
    )
    .unwrap();

    assert_eq!(event.path.as_ref(), "entry[0]");
}

#[test]
fn trace_reason_round_trips_static_and_parameterized_reasons() {
    let reasons = [
        TraceReason::ConcreteIds,
        TraceReason::NativeAstRoot(NonEmptyString::new("nodes").unwrap()),
        TraceReason::SelectedOptimizerRule(NonEmptyString::new("seed_access_path").unwrap()),
        TraceReason::SelectedMemoExpression(NonEmptyString::new("g=1 e=2 a=3").unwrap()),
        TraceReason::SelectedMemoChild(NonEmptyString::new("index=0 group=2").unwrap()),
        TraceReason::IndexId(NonEmptyString::new("node_eq:User:email").unwrap()),
    ];

    for reason in reasons {
        let encoded = serde_json::to_string(&reason).unwrap();
        assert_eq!(
            serde_json::from_str::<TraceReason>(&encoded).unwrap(),
            reason
        );
    }
}

#[test]
fn trace_pass_and_decision_display_are_stable_contracts() {
    assert_eq!(TracePass::NativeHandoff.to_string(), "native_handoff");
    assert_eq!(
        TraceDecision::SelectedMemoChild.to_string(),
        "selected_memo_child"
    );
}
