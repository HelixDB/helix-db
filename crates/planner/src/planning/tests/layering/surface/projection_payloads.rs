use super::*;

#[test]
fn projection_terminals_preserve_projection_payloads() {
    let count = executable_ast(
        AstNode::Count {
            input: boxed(nodes_root()),
        },
        PlannerContext::default(),
    );
    assert!(matches!(
        &count.steps()[0].op,
        ExecOp::Count { plan }
            if matches!(plan.as_ref(), ExecCountPlan::NodeFullScan { .. })
    ));
    assert_eq!(
        projection_of(AstNode::Exists {
            input: boxed(nodes_root())
        }),
        ProjectionPlan::Exists
    );
    assert_eq!(
        projection_of(AstNode::Id {
            input: boxed(nodes_root())
        }),
        ProjectionPlan::Id
    );
    assert_eq!(
        projection_of(AstNode::Label {
            input: boxed(nodes_root())
        }),
        ProjectionPlan::Label
    );
    assert_eq!(
        projection_of(AstNode::Values {
            input: boxed(nodes_root()),
            properties: vec!["name".to_string(), "email".to_string()],
        }),
        ProjectionPlan::Values(
            PropertyNames::new(AtLeast::<_, 1>::from_one_and_rest(
                NonEmptyString::new("name").unwrap(),
                vec![NonEmptyString::new("email").unwrap()]
            ))
            .unwrap()
        )
    );
    assert_eq!(
        projection_of(AstNode::ValueMap {
            input: boxed(nodes_root()),
            properties: None,
        }),
        ProjectionPlan::ValueMap(PropertySelection::All)
    );
    assert_eq!(
        projection_of(AstNode::ValueMap {
            input: boxed(nodes_root()),
            properties: Some(Vec::new()),
        }),
        ProjectionPlan::ValueMap(PropertySelection::All)
    );
    assert_eq!(
        projection_of(AstNode::ValueMap {
            input: boxed(nodes_root()),
            properties: Some(vec!["name".to_string()]),
        }),
        ProjectionPlan::ValueMap(PropertySelection::Selected(
            PropertyNames::new(AtLeast::<_, 1>::from_one(
                NonEmptyString::new("name").unwrap()
            ))
            .unwrap()
        ))
    );

    assert_eq!(
        projection_of(AstNode::Project {
            input: boxed(nodes_root()),
            projections: vec![
                Projection::property("name", "username"),
                Projection::expr("is_adult", Expr::prop("age").add(Expr::val(1))),
            ],
        }),
        ProjectionPlan::Project(
            ProjectionItems::new(AtLeast::<_, 1>::from_one_and_rest(
                ProjectionItem::Property {
                    source: NonEmptyString::new("name").unwrap(),
                    alias: NonEmptyString::new("username").unwrap(),
                },
                vec![ProjectionItem::Expr {
                    alias: NonEmptyString::new("is_adult").unwrap(),
                    expr: ExprPlan::new(Expr::prop("age").add(Expr::val(1))).unwrap(),
                }],
            ))
            .unwrap()
        )
    );

    assert_eq!(
        projection_of(AstNode::ProjectBindings {
            input: boxed(nodes_root()),
            projections: vec![
                BindingProjection::property(
                    BindingTarget::binding("author"),
                    "name",
                    "author_name",
                ),
                BindingProjection::coalesce(
                    vec![
                        BindingValueRef::current("$id"),
                        BindingValueRef::binding("fallback", "$id"),
                    ],
                    "entity_id",
                ),
            ],
            distinct: true,
        }),
        ProjectionPlan::ProjectBindings {
            projections: BindingProjectionItems::new(AtLeast::<_, 1>::from_one_and_rest(
                BindingProjectionPlan::Property {
                    target: BindingTargetPlan::Binding(NonEmptyString::new("author").unwrap()),
                    source: NonEmptyString::new("name").unwrap(),
                    alias: NonEmptyString::new("author_name").unwrap(),
                },
                vec![BindingProjectionPlan::Coalesce {
                    refs: AtLeast::<_, 1>::from_one_and_rest(
                        BindingValueRefPlan {
                            target: BindingTargetPlan::Current,
                            source: NonEmptyString::new("$id").unwrap(),
                        },
                        vec![BindingValueRefPlan {
                            target: BindingTargetPlan::Binding(
                                NonEmptyString::new("fallback").unwrap()
                            ),
                            source: NonEmptyString::new("$id").unwrap(),
                        }],
                    ),
                    alias: NonEmptyString::new("entity_id").unwrap(),
                }],
            ))
            .unwrap(),
            dedup: ProjectionDedupMode::Distinct,
        }
    );

    assert_eq!(
        projection_of(AstNode::ProjectBindings {
            input: boxed(nodes_root()),
            projections: vec![BindingProjection::current("name", "name")],
            distinct: false,
        }),
        ProjectionPlan::ProjectBindings {
            projections: BindingProjectionItems::new(AtLeast::<_, 1>::from_one(
                BindingProjectionPlan::Property {
                    target: BindingTargetPlan::Current,
                    source: NonEmptyString::new("name").unwrap(),
                    alias: NonEmptyString::new("name").unwrap(),
                },
            ))
            .unwrap(),
            dedup: ProjectionDedupMode::All,
        }
    );

    assert_eq!(
        projection_of(AstNode::EdgeProperties {
            input: boxed(edges_root())
        }),
        ProjectionPlan::EdgeProperties
    );
}
