use super::*;

#[test]
fn order_plan_separates_explicit_sorts_from_range_index_ordering() {
    let keys = OrderKeys::new(AtLeast::<_, 1>::from_one_and_rest(
        OrderKey {
            property: NonEmptyString::new("age").unwrap(),
            order: Order::Asc,
        },
        vec![OrderKey {
            property: NonEmptyString::new("name").unwrap(),
            order: Order::Desc,
        }],
    ))
    .unwrap();
    assert_eq!(keys.as_ref().len(), 2);
    assert_eq!(keys.as_ref()[0].property.as_ref(), "age");
    assert_eq!(
        serde_json::from_str::<OrderKeys>(&serde_json::to_string(&keys).unwrap()).unwrap(),
        keys
    );
    let duplicate = OrderKeys::new(AtLeast::<_, 1>::from_one_and_rest(
        OrderKey {
            property: NonEmptyString::new("age").unwrap(),
            order: Order::Asc,
        },
        vec![OrderKey {
            property: NonEmptyString::new("age").unwrap(),
            order: Order::Desc,
        }],
    ))
    .unwrap_err();
    assert_eq!(
        duplicate,
        OrderKeysError::DuplicateProperty {
            property: NonEmptyString::new("age").unwrap(),
        }
    );
    assert!(serde_json::from_str::<OrderKeys>(
        r#"[{"property":"age","order":"asc"},{"property":"age","order":"desc"}]"#
    )
    .is_err());

    let explicit_sort = OrderPlan::ExplicitSort(
        OrderKeys::new(AtLeast::<_, 1>::from_one(OrderKey {
            property: NonEmptyString::new("age").unwrap(),
            order: Order::Asc,
        }))
        .unwrap(),
    );
    let range_index = OrderPlan::RangeIndex {
        key: OrderKey {
            property: NonEmptyString::new("age").unwrap(),
            order: Order::Asc,
        },
        index_id: NonEmptyString::new("node_range:User:age").unwrap(),
    };

    assert_eq!(
        serde_json::to_string(&explicit_sort).unwrap(),
        r#"{"explicit_sort":[{"property":"age","order":"asc"}]}"#
    );
    assert_eq!(
        serde_json::to_string(&range_index).unwrap(),
        r#"{"range_index":{"key":{"property":"age","order":"asc"},"index_id":"node_range:User:age"}}"#
    );
    assert!(serde_json::from_str::<OrderPlan>(r#"{"explicit_sort":[]}"#).is_err());
    assert!(serde_json::from_str::<OrderPlan>(
        r#"{"explicit_sort":[{"property":"age","order":"asc"},{"property":"age","order":"desc"}]}"#
    )
    .is_err());
    assert!(serde_json::from_str::<OrderPlan>(
        r#"{"range_index":{"key":{"property":"age","order":"asc"},"index_id":""}}"#
    )
    .is_err());
}

#[test]
fn stream_inject_keeps_input_while_source_inject_has_no_input_shape() {
    let stream_inject = executable_ast(
        AstNode::Inject {
            input: Some(Box::new(AstNode::Nodes {
                reference: NodeRef::all(),
            })),
            variable: "users".to_string(),
        },
        PlannerContext::default(),
    );
    let source_inject = executable_traversal(g().inject("users"), PlannerContext::default());

    assert!(matches!(
        first_kv_read(&stream_inject),
        KvReadPlan::RangeScan { keyspace, .. }
            if *keyspace == ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        first_exec_op(&stream_inject, |op| matches!(op, ExecOp::Variable { .. })),
        ExecOp::Variable {
            op: ExecVariableOp::Stream(StreamVariableOp::Inject(variable))
        } if variable.as_ref() == "users"
    ));
    assert_eq!(source_inject.steps().len(), 1);
    assert!(matches!(
        &source_inject.steps()[0].op,
        ExecOp::Variable {
            op: ExecVariableOp::SourceInject { variable },
        } if variable.as_ref() == "users"
    ));
}
