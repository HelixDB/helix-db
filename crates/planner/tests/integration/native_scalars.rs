use helix_ast::{expr, value};
use helix_planner::{ir, relational as r};
use std::collections::BTreeSet;

#[test]
fn native_scalar_contracts_share_binding_visitors_and_immutable_plans() {
    let syntax = expr::Expr::prop("age").add_expr(expr::Expr::param("bonus"));
    let plan = ir::ExprPlan::new(syntax.clone()).unwrap();
    let cloned = plan.clone();
    assert!(std::ptr::eq(plan.resolved(), cloned.resolved()));
    assert!(std::ptr::eq(plan.expr(), cloned.expr()));
    assert_eq!(
        plan.resolved().slots(),
        BTreeSet::from([ir::native::CURRENT])
    );
    let replaced = plan
        .resolved()
        .rewrite(&mut |expression| {
            let ir::native::Expression::Parameter(name) = expression else {
                return Ok(None);
            };
            Ok((name == "bonus").then_some(ir::native::Expression::Literal(
                value::PropertyValue::I64(5),
            )))
        })
        .unwrap();
    let mut parameters = 0;
    replaced.visit(&mut |expression| {
        parameters += usize::from(matches!(expression, ir::native::Expression::Parameter(_)))
    });
    assert_eq!(parameters, 0);
    assert_eq!(
        serde_json::to_value(&plan).unwrap(),
        serde_json::to_value(syntax).unwrap()
    );
    let predicate = ir::PredicatePlan::new(expr::Predicate::has_key("name")).unwrap();
    assert_eq!(
        predicate.resolved().slots(),
        BTreeSet::from([ir::native::CURRENT])
    );
    assert_eq!(
        ir::ExprPlan::new(expr::Expr::Id)
            .unwrap()
            .resolved()
            .slots(),
        BTreeSet::from([ir::native::CURRENT])
    );
    assert!(std::ptr::eq(
        predicate.resolved(),
        predicate.clone().resolved()
    ));
    assert!(std::ptr::eq(
        predicate.predicate(),
        predicate.clone().predicate()
    ));
    let cypher = r::Expression::Property(Box::new(r::Expression::Slot(r::Slot(0))), "age".into());
    assert_eq!(plan.resolved().slots(), cypher.slots());
}

#[test]
fn native_projection_lowers_to_the_common_program_without_changing_serialization() {
    let name = ir::NonEmptyString::new("name").unwrap();
    let items = ir::ProjectionItems::new(ir::AtLeast::from_one_and_rest(
        ir::ProjectionItem::Property {
            source: name.clone(),
            alias: name,
        },
        vec![ir::ProjectionItem::Expr {
            alias: ir::NonEmptyString::new("identifier").unwrap(),
            expr: ir::ExprPlan::new(expr::Expr::Id).unwrap(),
        }],
    ))
    .unwrap();
    assert_eq!(
        items.program().references(),
        &BTreeSet::from([ir::native::CURRENT])
    );
    assert_eq!(
        items.program().outputs(),
        &BTreeSet::from([r::Slot(0), r::Slot(1)])
    );
    assert!(items.program().validate_input(&BTreeSet::new()).is_err());
    let encoded = serde_json::to_value(&items).unwrap();
    assert_eq!(encoded, serde_json::to_value(items.as_ref()).unwrap());
    let decoded: ir::ProjectionItems = serde_json::from_value(encoded).unwrap();
    assert_eq!(decoded.program(), items.program());
}
