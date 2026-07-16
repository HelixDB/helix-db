use super::*;

#[test]
fn expand_label_plan_encodes_any_and_named_label_scope() {
    assert_eq!(
        serde_json::to_string(&ExpandLabelPlan::Any).unwrap(),
        r#""any""#
    );

    let labeled = ExpandLabelPlan::Label(NonEmptyString::new("LIKES").unwrap());
    assert_eq!(
        serde_json::to_string(&labeled).unwrap(),
        r#"{"label":"LIKES"}"#
    );

    let plan = ExpandPlan {
        direction: ExpandDirection::Out,
        output: ExpandOutput::Nodes,
        label: labeled.clone(),
    };
    assert_eq!(
        serde_json::to_value(&plan).unwrap()["label"],
        serde_json::json!({"label": "LIKES"})
    );

    let parsed: ExpandLabelPlan = serde_json::from_str(r#"{"label":"LIKES"}"#).unwrap();
    assert_eq!(parsed, labeled);
    assert!(serde_json::from_str::<ExpandLabelPlan>(r#"{"label":""}"#).is_err());
}

#[test]
fn access_source_plans_reject_filtered_sources() {
    let node_source_plan = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::AllScan);
    assert_eq!(node_source_plan.as_ref(), &NodeAccessPlan::AllScan);
    assert_eq!(
        NodeAccessPlan::from(node_source_plan.clone()),
        NodeAccessPlan::AllScan
    );
    assert_eq!(
        serde_json::to_value(&node_source_plan).unwrap(),
        serde_json::json!("all_scan")
    );

    let edge_source_plan = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::AllScan);
    assert_eq!(edge_source_plan.as_ref(), &EdgeAccessPlan::AllScan);
    assert_eq!(
        EdgeAccessPlan::from(edge_source_plan.clone()),
        EdgeAccessPlan::AllScan
    );
    assert_eq!(
        serde_json::to_value(&edge_source_plan).unwrap(),
        serde_json::json!("all_scan")
    );

    let node_filtered = NodeAccessPlan::ScanThenFilter {
        source: NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::AllScan),
        residual: PredicatePlan::new(Predicate::eq("active", true)).unwrap(),
    };
    assert!(NodeAccessSourcePlan::new(node_filtered.clone()).is_none());
    assert!(serde_json::from_value::<NodeAccessSourcePlan>(
        serde_json::to_value(&node_filtered).unwrap()
    )
    .is_err());
    assert!(serde_json::from_str::<NodeAccessSourcePlan>(
        &serde_json::to_string(&node_filtered).unwrap()
    )
    .is_err());
    assert!(serde_json::from_str::<NodeAccessSourcePlan>("{}").is_err());

    let edge_filtered = EdgeAccessPlan::ScanThenFilter {
        source: EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::AllScan),
        residual: PredicatePlan::new(Predicate::eq("active", true)).unwrap(),
    };
    assert!(EdgeAccessSourcePlan::new(edge_filtered.clone()).is_none());
    assert!(serde_json::from_value::<EdgeAccessSourcePlan>(
        serde_json::to_value(&edge_filtered).unwrap()
    )
    .is_err());
    assert!(serde_json::from_str::<EdgeAccessSourcePlan>(
        &serde_json::to_string(&edge_filtered).unwrap()
    )
    .is_err());
    assert!(serde_json::from_str::<EdgeAccessSourcePlan>("{}").is_err());

    let mut node_union = serde_json::to_value(NodeAccessPlan::Union(AtLeast::<_, 2>::from_pair(
        NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::AllScan),
        NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::LabelScan {
            label: NonEmptyString::new("User").unwrap(),
        }),
    )))
    .unwrap();
    node_union["union"][0] = serde_json::to_value(node_filtered).unwrap();
    assert!(serde_json::from_value::<NodeAccessPlan>(node_union).is_err());

    let mut edge_intersect =
        serde_json::to_value(EdgeAccessPlan::Intersect(AtLeast::<_, 2>::from_pair(
            EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::AllScan),
            EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::LabelScan {
                label: NonEmptyString::new("FOLLOWS").unwrap(),
            }),
        )))
        .unwrap();
    edge_intersect["intersect"][0] = serde_json::to_value(edge_filtered).unwrap();
    assert!(serde_json::from_value::<EdgeAccessPlan>(edge_intersect).is_err());
}

#[test]
fn batch_condition_plan_uses_positive_min_size() {
    let output = BatchOutputPlan::Bind(NonEmptyString::new("users").unwrap());
    let run_condition = RunConditionPlan::If(BatchVariableConditionPlan::VarNotEmpty(
        NonEmptyString::new("users").unwrap(),
    ));
    let variable_condition = BatchVariableConditionPlan::VarMinSize(
        NonEmptyString::new("users").unwrap(),
        NonZeroUsize::new(2).unwrap(),
    );
    let condition = BatchConditionPlan::VarMinSize(
        NonEmptyString::new("users").unwrap(),
        NonZeroUsize::new(2).unwrap(),
    );

    assert_eq!(
        serde_json::to_string(&variable_condition).unwrap(),
        r#"{"var_min_size":["users",2]}"#
    );
    assert_eq!(
        serde_json::to_string(&condition).unwrap(),
        r#"{"var_min_size":["users",2]}"#
    );
    assert_eq!(
        serde_json::to_string(&output).unwrap(),
        r#"{"bind":"users"}"#
    );
    assert_eq!(
        serde_json::to_string(&run_condition).unwrap(),
        r#"{"if":{"var_not_empty":"users"}}"#
    );
    assert!(serde_json::from_str::<BatchOutputPlan>(r#"{"bind":""}"#).is_err());
    assert!(
        serde_json::from_str::<RunConditionPlan<BatchVariableConditionPlan>>(
            r#"{"if":"prev_not_empty"}"#
        )
        .is_err()
    );
    assert!(
        serde_json::from_str::<BatchVariableConditionPlan>(r#"{"var_min_size":["users",0]}"#)
            .is_err()
    );
    assert!(serde_json::from_str::<BatchConditionPlan>(r#"{"var_min_size":["users",0]}"#).is_err());
    assert_eq!(
        BatchConditionPlan::from(BatchVariableConditionPlan::VarNotEmpty(
            NonEmptyString::new("users").unwrap()
        )),
        BatchConditionPlan::VarNotEmpty(NonEmptyString::new("users").unwrap())
    );
    assert_eq!(
        BatchConditionPlan::from(BatchVariableConditionPlan::VarEmpty(
            NonEmptyString::new("users").unwrap()
        )),
        BatchConditionPlan::VarEmpty(NonEmptyString::new("users").unwrap())
    );
    assert_eq!(
        BatchConditionPlan::from(variable_condition),
        BatchConditionPlan::VarMinSize(
            NonEmptyString::new("users").unwrap(),
            NonZeroUsize::new(2).unwrap()
        )
    );
}

#[test]
fn trace_events_reject_empty_contract_fields() {
    let event = TraceEvent::try_new(
        TracePass::AccessPath,
        "entry[0].root",
        TraceDecision::NodeAllScan,
        TraceReason::NodeRefAll,
    )
    .unwrap();
    assert_eq!(event.pass, TracePass::AccessPath);
    assert_eq!(event.path.as_ref(), "entry[0].root");
    assert_eq!(event.decision, TraceDecision::NodeAllScan);
    assert_eq!(event.reason, TraceReason::NodeRefAll);
    assert_eq!(
        [
            TracePass::AccessPath,
            TracePass::BoundPushdown,
            TracePass::CardinalityOrder,
            TracePass::PredicateIndex,
            TracePass::OrderPushdown,
            TracePass::ReservedNoop,
            TracePass::NativeHandoff,
            TracePass::SelectedHandoff,
            TracePass::SubTraversal,
            TracePass::Variable,
        ]
        .map(|pass| pass.to_string()),
        [
            "access_path",
            "bound_pushdown",
            "cardinality_order",
            "predicate_index",
            "order_pushdown",
            "reserved_noop",
            "native_handoff",
            "selected_handoff",
            "sub_traversal",
            "variable",
        ]
    );
    assert_eq!(
        [
            TraceDecision::Context,
            TraceDecision::EdgeAllScan,
            TraceDecision::EdgeEmptyIds,
            TraceDecision::EdgeEmptyLabelScope,
            TraceDecision::EdgeEmptyPredicate,
            TraceDecision::EdgeEqualityIndex,
            TraceDecision::EdgeFullScan,
            TraceDecision::EdgeIntersect,
            TraceDecision::EdgePointGet,
            TraceDecision::EdgeRangeIndex,
            TraceDecision::EdgeScanOr,
            TraceDecision::EdgeUnion,
            TraceDecision::ExplicitSort,
            TraceDecision::Limit,
            TraceDecision::NodeAllScan,
            TraceDecision::NodeEmptyIds,
            TraceDecision::NodeEmptyLabelScope,
            TraceDecision::NodeEmptyPredicate,
            TraceDecision::NodeEqualityIndex,
            TraceDecision::NodeFullScan,
            TraceDecision::NodeIntersect,
            TraceDecision::NodePointGet,
            TraceDecision::NodeRangeIndex,
            TraceDecision::NodeScanOr,
            TraceDecision::NodeUnion,
            TraceDecision::RangeIndexOrder,
            TraceDecision::ResidualFilter,
            TraceDecision::ReservedOperation,
            TraceDecision::NativeQueryRoot,
            TraceDecision::NativeForEach,
            TraceDecision::SelectedRunRoot,
            TraceDecision::SelectedOptimizerRule,
            TraceDecision::SelectedMemoExpression,
            TraceDecision::SelectedMemoChild,
            TraceDecision::SelectedForEach,
            TraceDecision::TextIndex,
            TraceDecision::VariableFilter,
            TraceDecision::VariableOp,
            TraceDecision::VectorIndex,
        ]
        .map(|decision| decision.to_string()),
        [
            "context",
            "edge_all_scan",
            "edge_empty_ids",
            "edge_empty_label_scope",
            "edge_empty_predicate",
            "edge_equality_index",
            "edge_full_scan",
            "edge_intersect",
            "edge_point_get",
            "edge_range_index",
            "edge_scan_or",
            "edge_union",
            "explicit_sort",
            "limit",
            "node_all_scan",
            "node_empty_ids",
            "node_empty_label_scope",
            "node_empty_predicate",
            "node_equality_index",
            "node_full_scan",
            "node_intersect",
            "node_point_get",
            "node_range_index",
            "node_scan_or",
            "node_union",
            "range_index_order",
            "residual_filter",
            "reserved_operation",
            "native_query_root",
            "native_foreach",
            "selected_run_root",
            "selected_optimizer_rule",
            "selected_memo_expression",
            "selected_memo_child",
            "selected_foreach",
            "text_index",
            "variable_filter",
            "variable_op",
            "vector_index",
        ]
    );
    assert_eq!(
        [
            TraceReason::BranchStartsFromParentStream,
            TraceReason::NodeRefAll,
            TraceReason::EdgeRefAll,
            TraceReason::ConcreteIds,
            TraceReason::EmptyIdSet,
            TraceReason::HasLiteralFilter,
            TraceReason::LabelFilter,
            TraceReason::HasKeyFilter,
            TraceReason::WhereResidualIndexCandidate,
            TraceReason::WithinVariableFilter,
            TraceReason::WithoutVariableFilter,
            TraceReason::EdgeHasFilter,
            TraceReason::EdgeLabelFilter,
            TraceReason::ExplicitPhysicalLimit,
            TraceReason::StoreStreamAsVariable,
            TraceReason::SelectVariableStream,
            TraceReason::CaptureRowLocalBinding,
            TraceReason::InjectVariableStream,
            TraceReason::RangeBackedOrderRequiresAccessPathIntegration,
            TraceReason::PreservedForExecutorSemantics,
            TraceReason::ContradictoryLabelConstraints,
            TraceReason::ContradictoryScalarConstraints,
            TraceReason::LowestEstimatedCardinalityFirst,
            TraceReason::AndIndexedAtoms,
            TraceReason::NoScopedIndexableAtom,
            TraceReason::OrHasResidualBranches,
            TraceReason::NoLabelScope,
            TraceReason::NestedAndIndexedAtoms,
            TraceReason::OrIndexedAtoms,
            TraceReason::NativeAstRoot(NonEmptyString::new("nodes").unwrap()),
            TraceReason::NativeForEachBody,
            TraceReason::SelectedRootFamily(NonEmptyString::new("terminal").unwrap()),
            TraceReason::SelectedOptimizerRule(NonEmptyString::new("seed_access_path").unwrap()),
            TraceReason::SelectedMemoExpression(
                NonEmptyString::new("group=1 expr=1 alternative=1 children=[]").unwrap(),
            ),
            TraceReason::SelectedMemoChild(NonEmptyString::new("index=0 group=2").unwrap()),
            TraceReason::SelectedForEachBody,
            TraceReason::IndexId(NonEmptyString::new("node_eq:User:email").unwrap()),
        ]
        .map(|reason| reason.to_string()),
        [
            "branch starts from parent stream",
            "NodeRef::All",
            "EdgeRef::All",
            "concrete ids",
            "empty id set",
            "has literal filter",
            "label filter",
            "has_key filter",
            "where residual/index candidate",
            "within variable filter",
            "without variable filter",
            "edge_has filter",
            "edge label filter",
            "explicit physical limit",
            "store stream as variable",
            "select variable stream",
            "capture row-local binding",
            "inject variable stream",
            "range-backed order requires access-path integration",
            "preserved for executor semantics",
            "contradictory label constraints",
            "contradictory scalar property constraints",
            "lowest estimated cardinality first",
            "AND indexed atoms",
            "no scoped indexable atom",
            "OR has residual branches",
            "no label scope",
            "nested AND indexed atoms",
            "OR indexed atoms",
            "native AST root: nodes",
            "native foreach body",
            "selected root: terminal",
            "selected optimizer rule: seed_access_path",
            "selected memo: group=1 expr=1 alternative=1 children=[]",
            "selected memo child: index=0 group=2",
            "selected foreach body",
            "node_eq:User:email",
        ]
    );

    assert!(TraceEvent::try_new(
        TracePass::AccessPath,
        "",
        TraceDecision::NodeAllScan,
        TraceReason::NodeRefAll
    )
    .is_none());

    let serialized = serde_json::to_string(&event).unwrap();
    let parsed: TraceEvent = serde_json::from_str(&serialized).unwrap();
    assert_eq!(parsed, event);
    assert_eq!(
        serde_json::from_str::<TraceReason>(
            r#""range-backed order requires access-path integration""#
        )
        .unwrap(),
        TraceReason::RangeBackedOrderRequiresAccessPathIntegration
    );
    for (reason, encoded) in [
        (
            TraceReason::BranchStartsFromParentStream,
            "branch starts from parent stream",
        ),
        (TraceReason::NodeRefAll, "NodeRef::All"),
        (TraceReason::ConcreteIds, "concrete ids"),
        (TraceReason::EmptyIdSet, "empty id set"),
        (TraceReason::HasLiteralFilter, "has literal filter"),
        (TraceReason::LabelFilter, "label filter"),
        (TraceReason::HasKeyFilter, "has_key filter"),
        (
            TraceReason::WhereResidualIndexCandidate,
            "where residual/index candidate",
        ),
        (TraceReason::WithinVariableFilter, "within variable filter"),
        (
            TraceReason::WithoutVariableFilter,
            "without variable filter",
        ),
        (TraceReason::EdgeHasFilter, "edge_has filter"),
        (TraceReason::EdgeLabelFilter, "edge label filter"),
        (
            TraceReason::ExplicitPhysicalLimit,
            "explicit physical limit",
        ),
        (
            TraceReason::StoreStreamAsVariable,
            "store stream as variable",
        ),
        (TraceReason::SelectVariableStream, "select variable stream"),
        (
            TraceReason::CaptureRowLocalBinding,
            "capture row-local binding",
        ),
        (TraceReason::InjectVariableStream, "inject variable stream"),
        (
            TraceReason::RangeBackedOrderRequiresAccessPathIntegration,
            "range-backed order requires access-path integration",
        ),
        (
            TraceReason::PreservedForExecutorSemantics,
            "preserved for executor semantics",
        ),
        (
            TraceReason::ContradictoryLabelConstraints,
            "contradictory label constraints",
        ),
        (
            TraceReason::ContradictoryScalarConstraints,
            "contradictory scalar property constraints",
        ),
        (
            TraceReason::LowestEstimatedCardinalityFirst,
            "lowest estimated cardinality first",
        ),
        (TraceReason::AndIndexedAtoms, "AND indexed atoms"),
        (
            TraceReason::NoScopedIndexableAtom,
            "no scoped indexable atom",
        ),
        (
            TraceReason::OrHasResidualBranches,
            "OR has residual branches",
        ),
        (TraceReason::NoLabelScope, "no label scope"),
        (
            TraceReason::NestedAndIndexedAtoms,
            "nested AND indexed atoms",
        ),
        (TraceReason::OrIndexedAtoms, "OR indexed atoms"),
        (
            TraceReason::NativeAstRoot(NonEmptyString::new("nodes").unwrap()),
            "native AST root: nodes",
        ),
        (TraceReason::NativeForEachBody, "native foreach body"),
        (
            TraceReason::SelectedRootFamily(NonEmptyString::new("terminal").unwrap()),
            "selected root: terminal",
        ),
        (
            TraceReason::SelectedOptimizerRule(NonEmptyString::new("seed_access_path").unwrap()),
            "selected optimizer rule: seed_access_path",
        ),
        (
            TraceReason::SelectedMemoExpression(
                NonEmptyString::new("group=1 expr=1 alternative=1 children=[]").unwrap(),
            ),
            "selected memo: group=1 expr=1 alternative=1 children=[]",
        ),
        (
            TraceReason::SelectedMemoChild(NonEmptyString::new("index=0 group=2").unwrap()),
            "selected memo child: index=0 group=2",
        ),
        (TraceReason::SelectedForEachBody, "selected foreach body"),
    ] {
        assert_eq!(reason.to_string(), encoded);
        let encoded_json = serde_json::to_string(encoded).unwrap();
        assert_eq!(serde_json::to_string(&reason).unwrap(), encoded_json);
        assert_eq!(
            serde_json::from_str::<TraceReason>(&encoded_json).unwrap(),
            reason
        );
    }

    assert!(serde_json::from_str::<TraceReason>("[]").is_err());
    assert!(serde_json::from_str::<TraceEvent>(
        r#"{"pass":"access_path","path":"","decision":"node_all_scan","reason":"NodeRef::All"}"#
    )
    .is_err());
    assert!(serde_json::from_str::<TraceEvent>(
        r#"{"pass":"unknown","path":"entry[0].root","decision":"node_all_scan","reason":"NodeRef::All"}"#
    )
    .is_err());
    assert!(serde_json::from_str::<TraceEvent>(
        r#"{"pass":"access_path","path":"entry[0].root","decision":"node_scan","reason":"NodeRef::All"}"#
    )
    .is_err());
    assert!(serde_json::from_str::<TraceEvent>(
        r#"{"pass":"access_path","path":"entry[0].root","decision":"node_all_scan","reason":""}"#
    )
    .is_err());
    assert_eq!(
        serde_json::from_str::<TraceEvent>(
            r#"{"pass":"access_path","path":"entry[0].root","decision":"node_all_scan","reason":"node_eq:User:email"}"#
        )
        .unwrap()
        .reason,
        TraceReason::IndexId(NonEmptyString::new("node_eq:User:email").unwrap())
    );
}
