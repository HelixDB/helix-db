use super::*;

#[test]
fn aggregate_terminals_preserve_aggregate_payloads() {
    assert_eq!(
        aggregate_of(AstNode::Group {
            input: boxed(nodes_root()),
            property: "tenant_id".to_string(),
        }),
        AggregatePlan::Group(NonEmptyString::new("tenant_id").unwrap())
    );
    assert_eq!(
        aggregate_of(AstNode::GroupCount {
            input: boxed(nodes_root()),
            property: "status".to_string(),
        }),
        AggregatePlan::GroupCount(NonEmptyString::new("status").unwrap())
    );
    assert_eq!(
        aggregate_of(AstNode::AggregateBy {
            input: boxed(nodes_root()),
            function: AggregateFunction::Mean,
            property: "score".to_string(),
        }),
        AggregatePlan::AggregateBy {
            function: AggregateFunction::Mean,
            property: NonEmptyString::new("score").unwrap(),
        }
    );
}

#[test]
fn reserved_operations_are_all_preserved() {
    let cases = [
        (
            AstNode::Unfold {
                input: boxed(nodes_root()),
            },
            ReservedOp::Unfold,
        ),
        (
            AstNode::Path {
                input: boxed(nodes_root()),
            },
            ReservedOp::Path,
        ),
        (
            AstNode::SimplePath {
                input: boxed(nodes_root()),
            },
            ReservedOp::SimplePath,
        ),
        (
            AstNode::WithSack {
                input: boxed(nodes_root()),
                initial: PropertyValue::from(0),
            },
            ReservedOp::WithSack(PropertyValue::from(0)),
        ),
        (
            AstNode::SackSet {
                input: boxed(nodes_root()),
                property: "score".to_string(),
            },
            ReservedOp::SackSet(NonEmptyString::new("score").unwrap()),
        ),
        (
            AstNode::SackAdd {
                input: boxed(nodes_root()),
                property: "score".to_string(),
            },
            ReservedOp::SackAdd(NonEmptyString::new("score").unwrap()),
        ),
        (
            AstNode::SackGet {
                input: boxed(nodes_root()),
            },
            ReservedOp::SackGet,
        ),
    ];

    for (root, expected) in cases {
        let executable = executable_ast(root, PlannerContext::default());
        assert!(matches!(
            first_exec_op(&executable, |op| matches!(op, ExecOp::Reserved { .. })),
            ExecOp::Reserved { op } if *op == expected
        ));
    }
}

#[test]
fn parameter_bindings_builder_records_property_values() {
    let params = ParamBindings::default()
        .with_value(NonEmptyString::new("limit").unwrap(), 10)
        .with_value(NonEmptyString::new("tenant").unwrap(), "acme")
        .with_query_value(
            NonEmptyString::new("payload").unwrap(),
            QueryValue::String("raw".to_string()),
        );

    assert_eq!(params.values.get("limit"), Some(&PropertyValue::from(10)));
    assert_eq!(
        params.values.get("tenant"),
        Some(&PropertyValue::from("acme"))
    );
    assert_eq!(
        params.query_values.get("payload"),
        Some(&QueryValue::String("raw".to_string()))
    );
    assert_eq!(
        serde_json::to_string(&params.values).unwrap(),
        r#"{"limit":{"i64":10},"tenant":{"string":"acme"}}"#
    );
    assert!(serde_json::from_str::<ParamBindings>(
        r#"{"values":{"":{"i64":1}},"query_values":{}}"#
    )
    .is_err());
    assert!(
        serde_json::from_str::<ParamBindings>(r#"{"values":{},"query_values":{"":"raw"}}"#)
            .is_err()
    );
    assert!(NonEmptyString::new("").is_none());
}
