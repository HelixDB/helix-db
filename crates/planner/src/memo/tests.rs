use super::*;
use crate::{ir, logical, properties};

fn memo_expr(expr: logical::LogicalExpr, children: MemoChildGroups) -> MemoExpression {
    MemoExpression::new(expr, children).expect("test memo expression has valid child arity")
}

fn leaf_expr(expr: logical::LogicalExpr) -> MemoExpression {
    memo_expr(expr, MemoChildGroups::empty())
}

fn insert_group(memo: &mut Memo, expression: MemoExpression) -> MemoGroupId {
    memo.insert_group(expression)
        .expect("test memo group allocation should fit")
}

#[test]
fn memo_ids_are_positive_and_stable() {
    assert!(MemoGroupId::new(0).is_none());
    assert!(MemoExprId::new(0).is_none());
    assert!(PhysicalAlternativeId::new(0).is_none());
    assert_eq!(MemoGroupId::first().get(), 1);
    assert_eq!(MemoGroupId::first().next().unwrap().get(), 2);
    assert_eq!(MemoExprId::first().get(), 1);
    assert_eq!(MemoExprId::first().next().unwrap().get(), 2);
    let mut memo = Memo::default();
    let group = insert_group(
        &mut memo,
        leaf_expr(logical::LogicalExpr::Pure(logical::PureLogicalOp::Source {
            element: properties::ElementKind::Node,
        })),
    );

    assert_eq!(group.get(), 1);
    assert!(memo.contains_group(group));
    assert!(!memo.contains_group(MemoGroupId::new(99).unwrap()));
    assert_eq!(PhysicalAlternativeId::new(3).unwrap().get(), 3);
    assert_eq!(memo.groups()[0].expressions[0].id.get(), 1);
    assert_eq!(
        memo.groups()[0].digest,
        memo.groups()[0].expressions[0].digest
    );
    assert_eq!(
        PhysicalAlternativeId::sequential()
            .take(3)
            .map(PhysicalAlternativeId::get)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}

#[test]
fn memo_insert_expr_validates_group_tracks_counts_and_digests() {
    let mut memo = Memo::default();
    let source = logical::LogicalExpr::Pure(logical::PureLogicalOp::Source {
        element: properties::ElementKind::Node,
    });
    let limit = logical::LogicalExpr::Pure(logical::PureLogicalOp::Limit {
        count: crate::ir::StreamBoundPlan::Literal(1),
    });
    let group = insert_group(&mut memo, leaf_expr(source.clone()));

    let inserted = memo.insert_expr(group, leaf_expr(limit.clone())).unwrap();

    assert_eq!(inserted.group, group);
    assert_eq!(inserted.expr.get(), 2);
    assert_eq!(memo.group_count(), 1);
    assert_eq!(memo.expression_count(), 2);
    assert_eq!(memo.expression(inserted.expr).unwrap().expr, limit);
    assert!(memo.expression(MemoExprId::new(99).unwrap()).is_none());
    assert!(memo
        .contains_expr(group, &leaf_expr(source.clone()))
        .unwrap());
    assert!(memo
        .contains_expr(group, &leaf_expr(limit.clone()))
        .unwrap());
    assert!(!memo
        .contains_expr(
            group,
            &leaf_expr(logical::LogicalExpr::Pure(logical::PureLogicalOp::Source {
                element: properties::ElementKind::Edge,
            })),
        )
        .unwrap());
    assert_eq!(
        memo.insert_expr(
            MemoGroupId::new(9).unwrap(),
            leaf_expr(logical::LogicalExpr::Barrier(
                logical::BarrierLogicalOp::Mutation
            )),
        ),
        Err(MemoError::MissingGroup {
            group: MemoGroupId::new(9).unwrap()
        })
    );
    assert_eq!(
        memo.contains_expr(MemoGroupId::new(9).unwrap(), &leaf_expr(source.clone())),
        Err(MemoError::MissingGroup {
            group: MemoGroupId::new(9).unwrap()
        })
    );
    assert_eq!(
        MemoError::MissingGroup { group }.to_string(),
        "missing memo group 1"
    );
}

#[test]
fn memo_insertions_report_id_space_exhaustion_without_panicking() {
    let source = logical::LogicalExpr::Pure(logical::PureLogicalOp::Source {
        element: properties::ElementKind::Node,
    });
    let expression = leaf_expr(source.clone());

    let mut no_group_ids = Memo::with_next_ids(None, Some(MemoExprId::first()));
    assert_eq!(
        no_group_ids.insert_group(expression.clone()),
        Err(MemoError::GroupIdSpaceExhausted)
    );
    assert_eq!(
        MemoError::GroupIdSpaceExhausted.to_string(),
        "memo group ID space exhausted"
    );

    let mut no_expr_ids = Memo::with_next_ids(Some(MemoGroupId::first()), None);
    assert_eq!(
        no_expr_ids.insert_group(expression.clone()),
        Err(MemoError::ExprIdSpaceExhausted)
    );
    assert_eq!(no_expr_ids.group_count(), 0);
    assert_eq!(
        MemoError::ExprIdSpaceExhausted.to_string(),
        "memo expression ID space exhausted"
    );

    let mut memo = Memo::default();
    let group = insert_group(&mut memo, expression);
    memo.set_next_expr_id(None);
    assert_eq!(
        memo.insert_expr(group, leaf_expr(source)),
        Err(MemoError::ExprIdSpaceExhausted)
    );
}

#[test]
fn memo_serialization_recomputes_typed_id_cursors_and_rejects_invalid_records() {
    let source = logical::LogicalExpr::Pure(logical::PureLogicalOp::Source {
        element: properties::ElementKind::Node,
    });
    let limit = logical::LogicalExpr::Pure(logical::PureLogicalOp::Limit {
        count: crate::ir::StreamBoundPlan::Literal(1),
    });
    let edge_source = logical::LogicalExpr::Pure(logical::PureLogicalOp::Source {
        element: properties::ElementKind::Edge,
    });
    let mut memo = Memo::default();
    let first_group = insert_group(&mut memo, leaf_expr(source.clone()));
    memo.insert_expr(first_group, leaf_expr(limit.clone()))
        .unwrap();
    insert_group(&mut memo, leaf_expr(edge_source.clone()));

    let value = serde_json::to_value(&memo).unwrap();
    assert!(value.get("next_group_id").is_none());
    assert!(value.get("next_expr_id").is_none());
    assert!(value.get("indexes").is_none());

    let mut decoded: Memo = serde_json::from_value(value.clone()).unwrap();
    let inserted = decoded
        .insert_group_with_expr_id(leaf_expr(source))
        .expect("test memo expression allocation should fit");
    assert_eq!(inserted.group.get(), 3);
    assert_eq!(inserted.expr.get(), 4);

    let mut empty_group = value.clone();
    empty_group["groups"][0]["expressions"] = serde_json::json!([]);
    assert!(serde_json::from_value::<Memo>(empty_group).is_err());

    let mut mismatched_group = value.clone();
    mismatched_group["groups"][0]["expressions"][0]["group"] = serde_json::json!(2);
    assert!(serde_json::from_value::<Memo>(mismatched_group).is_err());

    let mut duplicate_expr = value;
    duplicate_expr["groups"][1]["expressions"][0]["id"] = serde_json::json!(1);
    assert!(serde_json::from_value::<Memo>(duplicate_expr).is_err());
}

#[test]
fn memo_deserialization_rebuilds_indexes_for_non_flattened_expression_order() {
    let source = logical::LogicalExpr::Pure(logical::PureLogicalOp::Source {
        element: properties::ElementKind::Node,
    });
    let edge_source = logical::LogicalExpr::Pure(logical::PureLogicalOp::Source {
        element: properties::ElementKind::Edge,
    });
    let limit = logical::LogicalExpr::Pure(logical::PureLogicalOp::Limit {
        count: crate::ir::StreamBoundPlan::Literal(1),
    });
    let mut memo = Memo::default();
    let first_group = insert_group(&mut memo, leaf_expr(source));
    let second_group = insert_group(&mut memo, leaf_expr(edge_source.clone()));
    let late_first_group_expr = memo
        .insert_expr(first_group, leaf_expr(limit.clone()))
        .unwrap();
    assert_eq!(late_first_group_expr.expr.get(), 3);

    let value = serde_json::to_value(&memo).unwrap();
    let mut decoded: Memo = serde_json::from_value(value).unwrap();

    assert_eq!(
        decoded
            .expression(MemoExprId::new(2).unwrap())
            .unwrap()
            .expr,
        edge_source
    );
    assert_eq!(
        decoded.expression(late_first_group_expr.expr).unwrap().expr,
        limit
    );
    assert_eq!(
        decoded
            .insert_expr(
                second_group,
                leaf_expr(logical::LogicalExpr::Barrier(
                    logical::BarrierLogicalOp::Mutation
                ))
            )
            .unwrap()
            .expr
            .get(),
        4
    );
}

#[test]
fn memo_deserialization_rejects_sparse_expression_ids() {
    let source = logical::LogicalExpr::Pure(logical::PureLogicalOp::Source {
        element: properties::ElementKind::Node,
    });
    let edge_source = logical::LogicalExpr::Pure(logical::PureLogicalOp::Source {
        element: properties::ElementKind::Edge,
    });
    let mut memo = Memo::default();
    insert_group(&mut memo, leaf_expr(source));
    insert_group(&mut memo, leaf_expr(edge_source));
    let mut value = serde_json::to_value(&memo).unwrap();
    value["groups"][1]["expressions"][0]["id"] = serde_json::json!(3);

    let error = serde_json::from_value::<Memo>(value)
        .expect_err("sparse expression IDs must be rejected")
        .to_string();

    assert!(
        error.contains("memo expression IDs must be dense"),
        "{error}"
    );
}

#[test]
fn memo_digests_are_stable_for_equivalent_expression_contracts() {
    let mut left = Memo::default();
    let mut right = Memo::default();
    let left_group = insert_group(
        &mut left,
        leaf_expr(logical::LogicalExpr::Pure(logical::PureLogicalOp::Source {
            element: properties::ElementKind::Node,
        })),
    );
    let right_group = insert_group(
        &mut right,
        leaf_expr(logical::LogicalExpr::Pure(logical::PureLogicalOp::Source {
            element: properties::ElementKind::Node,
        })),
    );

    assert_eq!(
        left.groups()[left_group.get() - 1].digest,
        right.groups()[right_group.get() - 1].digest
    );
}

#[test]
fn memo_expression_identity_includes_child_groups() {
    let mut memo = Memo::default();
    let seed = nested_pipeline_expr();
    let child_one = insert_group(&mut memo, leaf_expr(variable_pipeline_expr("seed")));
    let child_two = insert_group(&mut memo, leaf_expr(variable_pipeline_expr("seed")));
    let parent = insert_group(
        &mut memo,
        memo_expr(seed.clone(), MemoChildGroups::new(vec![child_one])),
    );

    assert!(memo
        .contains_expr(
            parent,
            &memo_expr(seed.clone(), MemoChildGroups::new(vec![child_one]))
        )
        .unwrap());
    assert!(!memo
        .contains_expr(
            parent,
            &memo_expr(seed.clone(), MemoChildGroups::new(vec![child_two]))
        )
        .unwrap());

    let inserted = memo
        .insert_expr(
            parent,
            memo_expr(seed.clone(), MemoChildGroups::new(vec![child_two])),
        )
        .unwrap();
    assert_eq!(
        memo.expression(inserted.expr).unwrap().children.as_slice(),
        &[child_two]
    );
}

#[test]
fn memo_expression_accepts_matching_recursive_child_arity() {
    let child = MemoGroupId::new(7).unwrap();
    let seed = nested_pipeline_expr();
    let expression = memo_expr(seed.clone(), MemoChildGroups::new(vec![child]));

    assert_eq!(expression.expr(), &seed);
    assert_eq!(expression.children().as_slice(), &[child]);

    let (expr, children) = expression.into_parts();
    assert_eq!(expr, seed);
    assert_eq!(children.as_slice(), &[child]);
}

#[test]
fn memo_expression_derives_child_groups_from_logical_children() {
    let child = MemoGroupId::new(7).unwrap();
    let mut child_count = 0;
    let expression = MemoExpression::with_derived_children(nested_pipeline_expr(), |logical| {
        child_count += 1;
        assert_eq!(logical, variable_pipeline_expr("seed"));
        child
    });

    assert_eq!(child_count, 1);
    assert_eq!(expression.children().as_slice(), &[child]);
}

#[test]
fn memo_expression_rejects_child_arity_mismatch() {
    let child = MemoGroupId::new(1).unwrap();

    let error = MemoExpression::new(
        logical::LogicalExpr::Pure(logical::PureLogicalOp::Source {
            element: properties::ElementKind::Node,
        }),
        MemoChildGroups::new(vec![child]),
    )
    .unwrap_err();

    assert_eq!(error.expected(), 0);
    assert_eq!(error.actual(), 1);
    assert_eq!(
        error.to_string(),
        "memo child-group arity mismatch: expected 0, got 1"
    );
}

fn variable_pipeline_expr(variable: &str) -> logical::LogicalExpr {
    logical::LogicalExpr::RootPipeline(variable_pipeline(variable, 1))
}

fn variable_pipeline(variable: &str, count: usize) -> logical::RootPipeline {
    logical::RootPipeline::new(
        logical::RootStream::VariableSource(logical::VariableSource::new(
            ir::NonEmptyString::new(variable).unwrap(),
        )),
        ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Limit {
            count: ir::StreamBoundPlan::Literal(count),
        }),
    )
    .unwrap()
}

fn nested_pipeline_expr() -> logical::LogicalExpr {
    logical::LogicalExpr::RootPipeline(
        logical::RootPipeline::new(
            logical::RootStream::Pipeline(Box::new(variable_pipeline("seed", 1))),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Limit {
                count: ir::StreamBoundPlan::Literal(2),
            }),
        )
        .unwrap(),
    )
}
