//! Logical schema snapshots preserve column facts while sharing immutable maps.
use helix_planner::relational as r;
use std::{collections::BTreeSet, ptr};

#[path = "../src/analysis/tests/allocations.rs"]
mod allocations;

fn transition_query() -> r::Query {
    let bindings = [
        ("a", r::BindingType::Node, false, r::ValueType::Node),
        ("b", r::BindingType::Node, false, r::ValueType::Node),
        (
            "ab",
            r::BindingType::Relationship,
            false,
            r::ValueType::Relationship,
        ),
        ("b_alias", r::BindingType::Node, true, r::ValueType::Node),
        ("a_alias", r::BindingType::Node, false, r::ValueType::Node),
        ("created", r::BindingType::Node, false, r::ValueType::Node),
        (
            "ac",
            r::BindingType::Relationship,
            false,
            r::ValueType::Relationship,
        ),
    ]
    .into_iter()
    .map(|(name, kind, nullable, value_type)| r::Binding {
        name: name.into(),
        kind,
        nullable,
        value_type,
    })
    .collect();
    let node = |slot, label: Option<&str>| r::NodePattern {
        slot: r::Slot(slot),
        label: label.map(str::to_owned),
        properties: vec![],
    };
    let relationship = |slot, from, to| r::RelationshipPattern {
        slot: r::Slot(slot),
        from: r::Slot(from),
        to: r::Slot(to),
        direction: r::Direction::Outgoing,
        types: vec!["R".into()],
        properties: vec![],
    };
    let mut operators = vec![
        r::Operator::Match {
            pattern: r::Pattern {
                nodes: vec![node(0, Some("N"))],
                relationships: vec![],
                paths: vec![],
            },
            optional: false,
            predicate: None,
        },
        r::Operator::Match {
            pattern: r::Pattern {
                nodes: vec![node(0, None), node(1, Some("N"))],
                relationships: vec![relationship(2, 0, 1)],
                paths: vec![],
            },
            optional: true,
            predicate: None,
        },
        r::Operator::Filter(
            r::SelectionProgram::new(r::Expression::Literal(r::Value::Boolean(true))).unwrap(),
        ),
        r::Operator::Match {
            pattern: r::Pattern {
                nodes: vec![node(1, None)],
                relationships: vec![],
                paths: vec![],
            },
            optional: false,
            predicate: None,
        },
        r::Operator::Update(vec![r::PropertyMutation::Set {
            entity: r::Slot(0),
            key: "seen".into(),
            value: r::Expression::Literal(r::Value::Boolean(true)),
        }]),
        r::Operator::Delete {
            entities: vec![r::Expression::Slot(r::Slot(2))],
            detach: false,
        },
        r::Operator::Create(r::Pattern {
            nodes: vec![node(0, None), node(5, Some("N"))],
            relationships: vec![relationship(6, 0, 5)],
            paths: vec![],
        }),
    ];
    for (sources, distinct, ordering, skip, limit) in [
        (
            [1, 0],
            true,
            vec![r::Ordering {
                expression: r::Expression::Slot(r::Slot(3)),
                descending: false,
            }],
            Some(r::Expression::Literal(r::Value::Integer(0))),
            Some(r::Expression::Literal(r::Value::Integer(10))),
        ),
        ([3, 4], false, vec![], None, None),
    ] {
        operators.push(r::Operator::Project {
            items: r::ProjectionProgram::new(
                sources
                    .into_iter()
                    .zip([3, 4])
                    .map(|(from, to)| r::Projection {
                        slot: r::Slot(to),
                        expression: r::Expression::Slot(r::Slot(from)),
                    })
                    .collect(),
            )
            .unwrap(),
            distinct,
            ordering,
            predicate: None,
            skip,
            limit,
        });
    }
    r::Query::new(
        bindings,
        operators,
        vec![("b".into(), r::Slot(3)), ("a".into(), r::Slot(4))],
    )
    .unwrap()
}

#[test]
fn optional_and_mutating_scopes_preserve_column_facts_and_boundaries() {
    let query = transition_query();
    let contracts = query.contracts();
    let schemas = [
        serde_json::json!({"0": {"value_type":"Node", "nullable":false}}),
        serde_json::json!({"0": {"value_type":"Node", "nullable":false}, "1": {"value_type":"Node", "nullable":true}, "2": {"value_type":"Relationship", "nullable":true}}),
        serde_json::json!({"0": {"value_type":"Node", "nullable":false}, "1": {"value_type":"Node", "nullable":true}, "2": {"value_type":"Relationship", "nullable":true}, "5": {"value_type":"Node", "nullable":false}, "6": {"value_type":"Relationship", "nullable":false}}),
        serde_json::json!({"3": {"value_type":"Node", "nullable":true}, "4": {"value_type":"Node", "nullable":false}}),
    ];
    let expected_outputs = [0, 1, 1, 1, 1, 1, 2, 3, 3];
    assert_eq!(contracts.len(), expected_outputs.len());
    for (index, (contract, schema)) in contracts.iter().zip(expected_outputs).enumerate() {
        assert_eq!(
            serde_json::to_value(contract.output()).unwrap(),
            schemas[schema]
        );
        assert_eq!(
            serde_json::to_value(contract.input()).unwrap(),
            if index == 0 {
                serde_json::json!({})
            } else {
                schemas[expected_outputs[index - 1]].clone()
            }
        );
        assert_eq!(
            contract.effect(),
            if (4..=6).contains(&index) {
                r::Effect::Write
            } else {
                r::Effect::Read
            }
        );
        assert_eq!(contract.is_total_projection(), index == 8);
        assert_eq!(
            contract.multiplicity(),
            match index {
                0 | 1 | 3 => r::Multiplicity::MayMultiplyRows,
                2 | 7 => r::Multiplicity::MayReduceRows,
                _ => r::Multiplicity::PreservesRows,
            }
        );
        let expected_boundaries: &[r::Boundary] = match index {
            1 => &[r::Boundary::OptionalMatch],
            4..=6 => &[r::Boundary::Mutation],
            7 => &[
                r::Boundary::Distinct,
                r::Boundary::Ordering,
                r::Boundary::Window,
            ],
            _ => &[],
        };
        assert_eq!(contract.boundaries(), expected_boundaries);
        let correlation = serde_json::to_value(contract.correlation()).unwrap();
        assert_eq!(
            correlation,
            match index {
                1 => serde_json::json!({"Bound":[0]}),
                3 => serde_json::json!({"Bound":[1]}),
                _ => serde_json::json!("Independent"),
            }
        );
    }
    let references: [&[u32]; 9] = [&[], &[0], &[], &[1], &[0], &[2], &[0], &[0, 1, 3], &[3, 4]];
    for (contract, expected) in contracts.iter().zip(references) {
        assert_eq!(
            contract.references(),
            &expected
                .iter()
                .copied()
                .map(r::Slot)
                .collect::<BTreeSet<_>>()
        );
    }
    for adjacent in contracts.windows(2) {
        assert!(ptr::eq(
            adjacent[0].output().columns(),
            adjacent[1].input().columns()
        ));
    }
    for (index, contract) in contracts.iter().enumerate() {
        assert_eq!(
            ptr::eq(contract.input().columns(), contract.output().columns()),
            matches!(index, 2..=5 | 8)
        );
    }
}

#[test]
fn serialization_rebuilds_validated_shared_snapshots_without_changing_wire_values() {
    let query = transition_query();
    let encoded = serde_json::to_value(&query).unwrap();
    let decoded: r::Query = serde_json::from_value(encoded.clone()).unwrap();
    assert_eq!(decoded, query);
    assert_eq!(serde_json::to_value(&decoded).unwrap(), encoded);
    for adjacent in decoded.contracts().windows(2) {
        assert!(ptr::eq(
            adjacent[0].output().columns(),
            adjacent[1].input().columns()
        ));
    }
    let copied = query.clone();
    for (original, copy) in query.contracts().iter().zip(copied.contracts()) {
        assert!(ptr::eq(original.input().columns(), copy.input().columns()));
        assert!(ptr::eq(
            original.output().columns(),
            copy.output().columns()
        ));
    }
    drop(query);
    assert_eq!(copied, decoded);
}

#[test]
fn schema_clones_allocate_nothing_and_outlive_all_query_owners() {
    let query = transition_query();
    for index in [0, 1, 6, 7] {
        let schema = query.contracts()[index].output();
        let (owned, count) = allocations::observe(|| schema.clone());
        assert_eq!((count.allocations, count.bytes), (0, 0));
        assert!(ptr::eq(schema.columns(), owned.columns()));
    }
    let (empty, count) = allocations::observe(|| query.contracts()[0].input().clone());
    assert_eq!((count.allocations, count.bytes), (0, 0));
    let owned = query.contracts()[7].output().clone();
    drop(query);
    assert!(empty.columns().is_empty());
    assert_eq!(owned.columns().len(), 2);
    assert_eq!(
        owned.columns()[&r::Slot(3)],
        r::ColumnType {
            value_type: r::ValueType::Node,
            nullable: true
        }
    );
    assert_eq!(
        owned.columns()[&r::Slot(4)],
        r::ColumnType {
            value_type: r::ValueType::Node,
            nullable: false
        }
    );
}

#[test]
fn wide_scopes_share_unchanged_maps_while_shadowed_slots_remain_distinct() {
    for count in [0_u32, 1, 16, 64, 512] {
        for shadowing in [false, true] {
            let bindings = (0..count)
                .map(|_| r::Binding {
                    name: "value".into(),
                    kind: r::BindingType::Scalar,
                    nullable: false,
                    value_type: r::ValueType::Integer,
                })
                .collect();
            let mut operators = (0..count)
                .map(|index| {
                    if shadowing {
                        r::Operator::Project {
                            items: r::ProjectionProgram::new(vec![r::Projection {
                                slot: r::Slot(index),
                                expression: if index == 0 {
                                    r::Expression::Literal(r::Value::Integer(7))
                                } else {
                                    r::Expression::Slot(r::Slot(index - 1))
                                },
                            }])
                            .unwrap(),
                            distinct: false,
                            ordering: vec![],
                            predicate: None,
                            skip: None,
                            limit: None,
                        }
                    } else {
                        r::Operator::Unwind {
                            expression: r::Expression::Literal(r::Value::List(vec![
                                r::Value::Integer(7),
                            ])),
                            slot: r::Slot(index),
                        }
                    }
                })
                .collect::<Vec<_>>();
            operators.extend((0..16).map(|_| {
                r::Operator::Filter(
                    r::SelectionProgram::new(r::Expression::Literal(r::Value::Boolean(true)))
                        .unwrap(),
                )
            }));
            let query = r::Query::new(bindings, operators, vec![]).unwrap();
            for (index, contract) in query.contracts().iter().take(count as usize).enumerate() {
                let expected = if shadowing {
                    BTreeSet::from([r::Slot(index as u32)])
                } else {
                    (0..=index as u32).map(r::Slot).collect()
                };
                assert_eq!(contract.output().slots(), expected);
                assert!(!ptr::eq(
                    contract.input().columns(),
                    contract.output().columns()
                ));
                assert!(contract
                    .output()
                    .columns()
                    .values()
                    .all(|column| column.value_type == r::ValueType::Integer && !column.nullable));
            }
            let schema = query.contracts()[count as usize].input();
            for contract in &query.contracts()[count as usize..] {
                assert!(ptr::eq(schema.columns(), contract.input().columns()));
                assert!(ptr::eq(schema.columns(), contract.output().columns()));
            }
            let (cloned, observed) = allocations::observe(|| schema.clone());
            assert_eq!((observed.allocations, observed.bytes), (0, 0));
            drop(query);
            assert_eq!(
                cloned.columns().len(),
                if shadowing {
                    usize::from(count != 0)
                } else {
                    count as usize
                }
            );
        }
    }
}
