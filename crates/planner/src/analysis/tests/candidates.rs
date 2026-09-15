use helix_ast::{expr, value::PropertyValue};

use super::allocations;

#[test]
fn candidate_classification_borrows_large_literals_without_allocating() {
    let wide = PropertyValue::StringArray(vec!["x".repeat(1024); 4096]);
    let large = "x".repeat(1024 * 1024);
    let cases = [
        (
            "membership",
            expr::Predicate::is_in("key", wide.clone()),
            true,
        ),
        ("equality", expr::Predicate::eq("key", large.clone()), true),
        ("range", expr::Predicate::gte("key", large.clone()), true),
        (
            "between",
            expr::Predicate::between("key", large.clone(), large.clone()),
            true,
        ),
        (
            "inverted",
            expr::Predicate::between("key", large, "a"),
            false,
        ),
        (
            "deferred error",
            expr::Predicate::IsIn {
                value: expr::Expr::Property("key".into()),
                values: expr::Expr::Neg {
                    expr: Box::new(expr::Expr::Constant(wide)),
                },
            },
            true,
        ),
    ];
    let mut observations = Vec::new();
    for (name, predicate, expected) in cases {
        let (candidate, observed) = allocations::observe(|| {
            crate::analysis::predicate_has_index_atom_candidate(&predicate)
        });
        assert_eq!(candidate, expected, "{name}");
        eprintln!(
            "candidate {name}: {} allocations, {} requested bytes",
            observed.allocations, observed.bytes
        );
        observations.push((name, observed));
    }
    assert!(
        observations.iter().all(|(_, count)| count.allocations == 0),
        "candidate classification allocated: {observations:?}"
    );
}

// Keep the previous owned-atom decision order as an independent oracle. The
// scalar collection shape check below deliberately does not use the new view.
fn legacy_candidate(predicate: &expr::Predicate) -> bool {
    fn reflexive(value: &PropertyValue) -> bool {
        match value {
            PropertyValue::F64(value) => !value.is_nan(),
            PropertyValue::F32(value) => !value.is_nan(),
            PropertyValue::Array(values) => values.iter().all(reflexive),
            PropertyValue::Object(values) => values.values().all(reflexive),
            _ => true,
        }
    }
    if let expr::Predicate::IsIn {
        value: expr::Expr::Property(_),
        values: expr::Expr::Constant(value),
    } = predicate
    {
        let scalar_proof = match value {
            PropertyValue::I64Array(_) | PropertyValue::StringArray(_) => true,
            PropertyValue::F64Array(values) => values.iter().all(|value| !value.is_nan()),
            PropertyValue::F32Array(values) => values.iter().all(|value| !value.is_nan()),
            PropertyValue::Array(values) => values.iter().all(reflexive),
            _ => false,
        };
        if scalar_proof {
            return true;
        }
    }
    match predicate {
        expr::Predicate::And { predicates } | expr::Predicate::Or { predicates } => {
            predicates.iter().any(legacy_candidate)
        }
        predicate => {
            matches!(
                crate::analysis::equality_atom(predicate),
                Ok(crate::analysis::EqualityIndexAtom::Atom { .. }) | Err(_)
            ) || matches!(
                crate::analysis::range_atom(predicate),
                Ok(crate::analysis::RangeIndexAtom::Atom { .. }) | Err(_)
            )
        }
    }
}

fn verify_candidate(predicate: &expr::Predicate) {
    let expected = legacy_candidate(predicate);
    let (actual, count) =
        allocations::observe(|| crate::analysis::predicate_has_index_atom_candidate(predicate));
    assert_eq!(actual, expected, "{predicate:?}");
    assert_eq!(count.allocations, 0, "{predicate:?}: {count:?}");
}

#[test]
fn borrowed_candidates_match_owned_atoms_across_predicate_and_operand_families() {
    use expr::{CompareOp, Expr, Predicate};
    let mut operands: Vec<_> = vec![
        PropertyValue::Null,
        PropertyValue::Bool(false),
        PropertyValue::Bool(true),
        PropertyValue::I64(i64::MIN),
        PropertyValue::I64(i64::MAX),
        PropertyValue::I64(0),
        PropertyValue::I64(1),
        PropertyValue::I64(9_007_199_254_740_993),
        PropertyValue::F64(-0.0),
        PropertyValue::F64(1.0),
        PropertyValue::F32(1.0),
        PropertyValue::F64(9_007_199_254_740_992.0),
        PropertyValue::F64(f64::INFINITY),
        PropertyValue::F32(f32::NEG_INFINITY),
        PropertyValue::F64(f64::NAN),
        PropertyValue::F32(f32::NAN),
        PropertyValue::DateTime(0),
        PropertyValue::DateTime(1),
        PropertyValue::String(String::new()),
        PropertyValue::String("κλειδί".into()),
        PropertyValue::Bytes(vec![0, 1]),
        PropertyValue::I64Array(vec![]),
        PropertyValue::I64Array(vec![0, 1]),
        PropertyValue::F64Array(vec![-0.0, 1.0]),
        PropertyValue::F64Array(vec![f64::NAN]),
        PropertyValue::F32Array(vec![1.0]),
        PropertyValue::F32Array(vec![f32::NAN]),
        PropertyValue::StringArray(vec!["a".into(), "z".into()]),
        PropertyValue::Array(vec![]),
        PropertyValue::array([PropertyValue::I64(1)]),
        PropertyValue::array([PropertyValue::F64(f64::NAN)]),
        PropertyValue::array([PropertyValue::F64Array(vec![f64::NAN])]),
        PropertyValue::object([("key", PropertyValue::I64(1))]),
        PropertyValue::array([PropertyValue::object([("key", PropertyValue::I64(1))])]),
        PropertyValue::array([PropertyValue::object([(
            "key",
            PropertyValue::F64(f64::NAN),
        )])]),
    ]
    .into_iter()
    .map(Expr::Constant)
    .collect();
    let property = || Expr::Property("key".into());
    let number = || Expr::Constant(PropertyValue::I64(1));
    operands.extend([
        property(),
        Expr::Property(String::new()),
        Expr::Param("p".into()),
        Expr::Param(String::new()),
        Expr::Id,
        Expr::Timestamp,
        Expr::DateTimeNow,
        Expr::Add {
            left: Box::new(property()),
            right: Box::new(number()),
        },
        Expr::Sub {
            left: Box::new(property()),
            right: Box::new(number()),
        },
        Expr::Mul {
            left: Box::new(property()),
            right: Box::new(number()),
        },
        Expr::Div {
            left: Box::new(property()),
            right: Box::new(number()),
        },
        Expr::Mod {
            left: Box::new(property()),
            right: Box::new(number()),
        },
        Expr::Neg {
            expr: Box::new(property()),
        },
        Expr::Case {
            when_then: vec![],
            else_expr: Some(Box::new(number())),
        },
    ]);
    for left in &operands {
        for right in &operands {
            for op in [
                CompareOp::Eq,
                CompareOp::Neq,
                CompareOp::Gt,
                CompareOp::Gte,
                CompareOp::Lt,
                CompareOp::Lte,
            ] {
                verify_candidate(&Predicate::Compare {
                    left: left.clone(),
                    op,
                    right: right.clone(),
                });
            }
            for predicate in [
                Predicate::Eq {
                    left: left.clone(),
                    right: right.clone(),
                },
                Predicate::Neq {
                    left: left.clone(),
                    right: right.clone(),
                },
                Predicate::Gt {
                    left: left.clone(),
                    right: right.clone(),
                },
                Predicate::Gte {
                    left: left.clone(),
                    right: right.clone(),
                },
                Predicate::Lt {
                    left: left.clone(),
                    right: right.clone(),
                },
                Predicate::Lte {
                    left: left.clone(),
                    right: right.clone(),
                },
                Predicate::IsIn {
                    value: left.clone(),
                    values: right.clone(),
                },
                Predicate::StartsWith {
                    value: left.clone(),
                    prefix: right.clone(),
                },
                Predicate::EndsWith {
                    value: left.clone(),
                    suffix: right.clone(),
                },
                Predicate::Contains {
                    value: left.clone(),
                    substring: right.clone(),
                },
                Predicate::Between {
                    value: property(),
                    min: left.clone(),
                    max: right.clone(),
                },
                Predicate::Between {
                    value: Expr::Id,
                    min: left.clone(),
                    max: right.clone(),
                },
            ] {
                verify_candidate(&predicate);
            }
        }
        let leaf = Predicate::IsIn {
            value: property(),
            values: left.clone(),
        };
        for predicate in [
            Predicate::And {
                predicates: vec![Predicate::contains("key", "a"), leaf.clone()],
            },
            Predicate::Or {
                predicates: vec![leaf.clone(), Predicate::contains("key", "a")],
            },
            Predicate::Not {
                predicate: Box::new(leaf.clone()),
            },
            Predicate::And {
                predicates: vec![
                    Predicate::Not {
                        predicate: Box::new(leaf.clone()),
                    },
                    Predicate::Or {
                        predicates: vec![leaf],
                    },
                ],
            },
        ] {
            verify_candidate(&predicate);
        }
    }
    for predicate in [
        Predicate::HasKey {
            property: "key".into(),
        },
        Predicate::IsNull {
            property: "key".into(),
        },
        Predicate::IsNotNull {
            property: "key".into(),
        },
        Predicate::And { predicates: vec![] },
        Predicate::Or { predicates: vec![] },
    ] {
        verify_candidate(&predicate);
    }
}

#[test]
fn between_candidate_keeps_lower_first_error_and_rejection_precedence() {
    use expr::{Expr, Predicate};
    let rejected = Expr::Constant(PropertyValue::Bool(false));
    let error = Expr::Param(String::new());
    for (min, max, expected) in [
        (rejected.clone(), error.clone(), false),
        (error.clone(), rejected.clone(), true),
        (Expr::Param("valid".into()), rejected.clone(), false),
        (Expr::Param("valid".into()), error, true),
    ] {
        let predicate = Predicate::Between {
            value: Expr::Property("key".into()),
            min,
            max,
        };
        assert_eq!(
            crate::analysis::predicate_has_index_atom_candidate(&predicate),
            expected
        );
        verify_candidate(&predicate);
    }
}
