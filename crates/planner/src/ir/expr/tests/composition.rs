use super::*;
use helix_ast::{expr, value};

// Independent compatibility oracle: walk the old AST in source order, then
// validate and lower it through the unchanged native frontend adapter.
fn legacy_flatten(inputs: &[Predicate]) -> PredicatePlan {
    let mut pending: Vec<_> = inputs.iter().rev().collect();
    let mut leaves = Vec::new();
    while let Some(predicate) = pending.pop() {
        match predicate {
            Predicate::And { predicates } => pending.extend(predicates.iter().rev()),
            predicate => leaves.push(predicate.clone()),
        }
    }
    PredicatePlan::new(Predicate::and(leaves)).unwrap()
}

#[test]
fn composition_matches_legacy_lowering_and_serialization_without_changing_dependencies() {
    let leaves = [
        Predicate::eq("$label", "N"),
        Predicate::eq_param("κλειδί", "parameter"),
        Predicate::Eq {
            left: expr::Expr::case(
                vec![(
                    Predicate::and(vec![Predicate::eq("active", true)]),
                    expr::Expr::val(1),
                )],
                Some(expr::Expr::param("fallback")),
            ),
            right: expr::Expr::val(1),
        },
        Predicate::IsIn {
            value: expr::Expr::val(1),
            values: expr::Expr::val(value::PropertyValue::I64Array(vec![1, 2])),
        },
        Predicate::IsIn {
            value: expr::Expr::val(1),
            values: expr::Expr::val(1),
        },
        Predicate::Between {
            value: expr::Expr::param("value"),
            min: expr::Expr::val(i64::MIN),
            max: expr::Expr::val(i64::MAX),
        },
        Predicate::IsNull {
            property: "missing".into(),
        },
        Predicate::Eq {
            left: expr::Expr::val(value::PropertyValue::Null),
            right: expr::Expr::val(value::PropertyValue::Null),
        },
        Predicate::Compare {
            left: expr::Expr::val(value::PropertyValue::F32(1.0)),
            op: expr::CompareOp::Eq,
            right: expr::Expr::val(value::PropertyValue::F64(1.0)),
        },
    ];
    for (index, leaf) in leaves.iter().enumerate() {
        let mut nested = Predicate::and(vec![leaf.clone()]);
        for depth in 0..6 {
            let sibling = &leaves[(index + depth + 1) % leaves.len()];
            let boundary = Predicate::Or {
                predicates: vec![
                    Predicate::and(vec![sibling.clone(), leaf.clone()]),
                    Predicate::Not {
                        predicate: Box::new(nested.clone()),
                    },
                ],
            };
            let inputs = vec![nested.clone(), boundary, sibling.clone()];
            let plans: Vec<_> = inputs
                .iter()
                .cloned()
                .map(|p| PredicatePlan::new(p).unwrap())
                .collect();
            let combined =
                PredicatePlan::conjunction(&AtLeast::<_, 2>::try_from_vec(plans.clone()).unwrap());
            let old = PredicatePlan::new(Predicate::and(inputs.clone())).unwrap();
            let flattened = PredicatePlan::flattened_conjunction(&plans[0], &plans[1..]);
            let native::Expression::Function(native::Function::All, arguments) =
                flattened.resolved()
            else {
                panic!("flat conjunction")
            };
            assert_eq!(
                arguments.capacity(),
                arguments.len(),
                "composed execution arguments retain no spare slots"
            );
            let old_flattened = legacy_flatten(&inputs);
            let single = PredicatePlan::flattened_conjunction(&plans[0], &[]);
            let old_single = legacy_flatten(&inputs[..1]);
            for (actual, expected) in [
                (combined, old),
                (flattened, old_flattened),
                (single, old_single),
            ] {
                assert_eq!(actual.resolved(), expected.resolved());
                assert_eq!(
                    actual.program().references(),
                    expected.program().references()
                );
                assert_eq!(actual.predicate(), expected.predicate());
                let wire = serde_json::to_string(&actual).unwrap();
                assert_eq!(wire, serde_json::to_string(&expected).unwrap());
                let decoded: PredicatePlan = serde_json::from_str(&wire).unwrap();
                assert_eq!(decoded, actual);
            }
            nested = Predicate::and(vec![sibling.clone(), nested, leaf.clone()]);
        }
    }
}

#[test]
fn composed_deserialization_rejects_empty_sets_and_invalid_child_names() {
    for predicate in [
        Predicate::and(vec![]),
        Predicate::and(vec![Predicate::eq("valid", 1), Predicate::has_key("")]),
        Predicate::and(vec![
            Predicate::eq("valid", 1),
            Predicate::eq_param("key", ""),
        ]),
        Predicate::and(vec![
            Predicate::eq("valid", 1),
            Predicate::Or { predicates: vec![] },
        ]),
    ] {
        let wire = serde_json::to_string(&predicate).unwrap();
        assert!(PredicatePlan::new(predicate).is_err());
        assert!(serde_json::from_str::<PredicatePlan>(&wire).is_err());
    }
}
