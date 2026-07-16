use super::*;

#[test]
fn property_assignments_preserve_empty_and_reject_duplicate_names() {
    let empty = PropertyAssignments::try_from_vec(Vec::new()).unwrap();
    assert!(empty.as_ref().is_empty());
    assert_eq!(serde_json::to_string(&empty).unwrap(), "[]");

    let assignments = PropertyAssignments::try_from_vec(vec![
        (
            NonEmptyString::new("name").unwrap(),
            PropertyInputPlan::new(PropertyInput::from("alice")).unwrap(),
        ),
        (
            NonEmptyString::new("email").unwrap(),
            PropertyInputPlan::new(PropertyInput::from("alice@example.com")).unwrap(),
        ),
    ])
    .unwrap();
    assert_eq!(assignments.as_ref().len(), 2);
    assert_eq!(assignments.as_ref()[0].0.as_ref(), "name");
    assert_eq!(assignments.as_ref().len(), 2);
    assert_eq!(
        assignments
            .as_ref()
            .iter()
            .map(|(property, _value)| property.as_ref())
            .collect::<Vec<_>>(),
        vec!["name", "email"]
    );
    assert_eq!((&assignments).into_iter().count(), 2);
    assert_eq!(assignments.clone().into_iter().count(), 2);

    let serialized = serde_json::to_string(&assignments).unwrap();
    let parsed: PropertyAssignments = serde_json::from_str(&serialized).unwrap();
    assert_eq!(parsed, assignments);

    let duplicate = PropertyAssignments::try_from_vec(vec![
        (
            NonEmptyString::new("name").unwrap(),
            PropertyInputPlan::new(PropertyInput::from("alice")).unwrap(),
        ),
        (
            NonEmptyString::new("name").unwrap(),
            PropertyInputPlan::new(PropertyInput::from("bob")).unwrap(),
        ),
    ])
    .unwrap_err();
    assert_eq!(
        duplicate,
        PropertyAssignmentsError::DuplicateProperty {
            property: NonEmptyString::new("name").unwrap(),
        }
    );

    assert!(serde_json::from_str::<PropertyAssignments>(
        r#"[["name",{"value":{"string":"alice"}}],["name",{"value":{"string":"bob"}}]]"#
    )
    .is_err());
    assert!(serde_json::from_str::<PropertyAssignments>("{}").is_err());
}

#[test]
fn projection_lists_reject_duplicate_output_aliases() {
    let properties = PropertyNames::new(AtLeast::<_, 1>::from_one_and_rest(
        NonEmptyString::new("name").unwrap(),
        vec![NonEmptyString::new("email").unwrap()],
    ))
    .unwrap();
    assert_eq!(
        properties.as_ref(),
        &[
            NonEmptyString::new("name").unwrap(),
            NonEmptyString::new("email").unwrap()
        ]
    );
    assert_eq!(
        serde_json::from_str::<PropertyNames>(&serde_json::to_string(&properties).unwrap())
            .unwrap(),
        properties
    );
    let duplicate_properties = PropertyNames::new(AtLeast::<_, 1>::from_one_and_rest(
        NonEmptyString::new("name").unwrap(),
        vec![NonEmptyString::new("name").unwrap()],
    ))
    .unwrap_err();
    assert_eq!(
        duplicate_properties,
        PropertyNamesError::DuplicateName {
            name: NonEmptyString::new("name").unwrap(),
        }
    );
    assert!(serde_json::from_str::<PropertyNames>(r#"["name","name"]"#).is_err());
    assert!(serde_json::from_str::<PropertyNames>("{}").is_err());

    let projections = ProjectionItems::new(AtLeast::<_, 1>::from_one_and_rest(
        ProjectionItem::Property {
            source: NonEmptyString::new("name").unwrap(),
            alias: NonEmptyString::new("display").unwrap(),
        },
        vec![ProjectionItem::Expr {
            alias: NonEmptyString::new("computed").unwrap(),
            expr: ExprPlan::new(Expr::val(1)).unwrap(),
        }],
    ))
    .unwrap();
    assert_eq!(projections.as_ref().len(), 2);
    assert_eq!(
        serde_json::from_str::<ProjectionItems>(&serde_json::to_string(&projections).unwrap())
            .unwrap(),
        projections
    );

    let duplicate = ProjectionItems::new(AtLeast::<_, 1>::from_one_and_rest(
        ProjectionItem::Property {
            source: NonEmptyString::new("name").unwrap(),
            alias: NonEmptyString::new("display").unwrap(),
        },
        vec![ProjectionItem::Expr {
            alias: NonEmptyString::new("display").unwrap(),
            expr: ExprPlan::new(Expr::val(1)).unwrap(),
        }],
    ))
    .unwrap_err();
    assert_eq!(
        duplicate,
        ProjectionItemsError::DuplicateAlias {
            alias: NonEmptyString::new("display").unwrap(),
        }
    );
    assert!(serde_json::from_str::<ProjectionItems>(
        r#"[{"property":{"source":"name","alias":"display"}},{"expr":{"alias":"display","expr":{"constant":{"i64":1}}}}]"#
    )
    .is_err());
    assert!(serde_json::from_str::<ProjectionItems>("{}").is_err());

    let binding_projections = BindingProjectionItems::new(AtLeast::<_, 1>::from_one_and_rest(
        BindingProjectionPlan::Property {
            target: BindingTargetPlan::Current,
            source: NonEmptyString::new("name").unwrap(),
            alias: NonEmptyString::new("display").unwrap(),
        },
        vec![BindingProjectionPlan::Coalesce {
            refs: AtLeast::<_, 1>::from_one(BindingValueRefPlan {
                target: BindingTargetPlan::Current,
                source: NonEmptyString::new("$id").unwrap(),
            }),
            alias: NonEmptyString::new("entity_id").unwrap(),
        }],
    ))
    .unwrap();
    assert_eq!(binding_projections.as_ref().len(), 2);
    assert_eq!(
        serde_json::from_str::<BindingProjectionItems>(
            &serde_json::to_string(&binding_projections).unwrap()
        )
        .unwrap(),
        binding_projections
    );

    let duplicate = BindingProjectionItems::new(AtLeast::<_, 1>::from_one_and_rest(
        BindingProjectionPlan::Property {
            target: BindingTargetPlan::Current,
            source: NonEmptyString::new("name").unwrap(),
            alias: NonEmptyString::new("display").unwrap(),
        },
        vec![BindingProjectionPlan::Coalesce {
            refs: AtLeast::<_, 1>::from_one(BindingValueRefPlan {
                target: BindingTargetPlan::Current,
                source: NonEmptyString::new("$id").unwrap(),
            }),
            alias: NonEmptyString::new("display").unwrap(),
        }],
    ))
    .unwrap_err();
    assert_eq!(
        duplicate,
        ProjectionItemsError::DuplicateAlias {
            alias: NonEmptyString::new("display").unwrap(),
        }
    );
    assert!(serde_json::from_str::<BindingProjectionItems>(
        r#"[{"property":{"target":"current","source":"name","alias":"display"}},{"coalesce":{"refs":[{"target":"current","source":"$id"}],"alias":"display"}}]"#
    )
    .is_err());
    assert!(serde_json::from_str::<BindingProjectionItems>("{}").is_err());
}

#[test]
fn predicate_plan_validates_serializes_and_compares_against_ast_predicates() {
    let predicate = Predicate::eq("active", true);
    let plan = PredicatePlan::new(predicate.clone()).unwrap();
    assert_eq!(plan.predicate(), &predicate);
    assert_eq!(plan, predicate);

    let tenant = PredicatePlan::new(Predicate::eq("tenant", "acme")).unwrap();
    let conjunction = PredicatePlan::conjunction(&AtLeast::<_, 2>::from_pair(plan.clone(), tenant));
    assert!(matches!(
        conjunction.predicate(),
        Predicate::And { predicates } if predicates.len() == 2
    ));

    let serialized = serde_json::to_string(&plan).unwrap();
    let parsed: PredicatePlan = serde_json::from_str(&serialized).unwrap();
    assert_eq!(parsed, predicate);

    let filter = FilterPlan::Residual {
        predicate: plan.clone(),
    };
    assert_eq!(
        serde_json::to_string(&filter).unwrap(),
        r#"{"residual":{"predicate":{"eq":{"left":{"property":"active"},"right":{"constant":{"bool":true}}}}}}"#
    );
    let parsed_filter: FilterPlan =
        serde_json::from_str(&serde_json::to_string(&filter).unwrap()).unwrap();
    assert_eq!(parsed_filter, filter);

    let err = PredicatePlan::new(Predicate::has_key(String::new())).unwrap_err();
    assert_eq!(
        err,
        ExprPlanError::EmptyName {
            field: NameField::Property
        }
    );
    assert!(serde_json::from_str::<PredicatePlan>(r#"{"has_key":{"property":""}}"#).is_err());
    assert!(serde_json::from_str::<PredicatePlan>("[]").is_err());
    assert!(serde_json::from_str::<FilterPlan>(
        r#"{"residual":{"predicate":{"has_key":{"property":""}}}}"#
    )
    .is_err());
    assert_eq!(
        [PredicateSetOp::And, PredicateSetOp::Or].map(|op| op.to_string()),
        ["and", "or"]
    );
    assert_eq!(
        PredicatePlan::new(Predicate::and(Vec::new())).unwrap_err(),
        ExprPlanError::EmptyPredicateSet {
            op: PredicateSetOp::And
        }
    );
    assert_eq!(
        PredicatePlan::new(Predicate::or(Vec::new())).unwrap_err(),
        ExprPlanError::EmptyPredicateSet {
            op: PredicateSetOp::Or
        }
    );
    assert!(serde_json::from_str::<PredicatePlan>(r#"{"and":{"predicates":[]}}"#).is_err());
    assert!(serde_json::from_str::<PredicatePlan>(r#"{"or":{"predicates":[]}}"#).is_err());
}
