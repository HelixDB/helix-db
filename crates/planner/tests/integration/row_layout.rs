use helix_planner::relational as r;
use std::collections::BTreeSet;
use std::sync::Arc;

fn projection_chain(bindings: usize) -> r::Query {
    let catalog = (0..bindings)
        .map(|index| r::Binding {
            name: format!("value_{index}"),
            kind: r::BindingType::Scalar,
            nullable: false,
            value_type: r::ValueType::Integer,
        })
        .collect();
    let operators = (0..bindings)
        .map(|index| r::Operator::Project {
            items: r::ProjectionProgram::new(vec![r::Projection {
                slot: r::Slot(index as u32),
                expression: if index == 0 {
                    r::Expression::Literal(r::Value::Integer(7))
                } else {
                    r::Expression::Slot(r::Slot(index as u32 - 1))
                },
            }])
            .unwrap(),
            distinct: false,
            ordering: if index == 0 {
                vec![]
            } else {
                vec![r::Ordering {
                    expression: r::Expression::Slot(r::Slot(index as u32 - 1)),
                    descending: false,
                }]
            },
            predicate: None,
            skip: None,
            limit: None,
        })
        .collect();
    let returns = bindings
        .checked_sub(1)
        .map(|last| vec![("value".into(), r::Slot(last as u32))])
        .unwrap_or_default();
    r::Query::new(catalog, operators, returns).unwrap()
}

#[test]
fn physical_layout_reuses_dead_scopes_without_aliasing_simultaneous_values() {
    for bindings in [0, 1, 2, 3, 64, 4096] {
        let query = projection_chain(bindings);
        assert_eq!(query.bindings().len(), bindings);
        assert_eq!(query.layout().width(), bindings.min(2));
        for contract in query.contracts() {
            let live = contract
                .input()
                .columns()
                .keys()
                .chain(contract.output().columns().keys())
                .chain(contract.references())
                .copied()
                .collect::<BTreeSet<_>>();
            let cells = live
                .iter()
                .map(|slot| query.layout().cell(*slot).unwrap())
                .collect::<BTreeSet<_>>();
            assert_eq!(
                cells.len(),
                live.len(),
                "every simultaneous logical value has its own cell"
            );
            assert!(cells
                .iter()
                .all(|cell| cell.index() < query.layout().width()));
        }
        for slot in 0..bindings {
            assert_eq!(
                query.layout().cell(r::Slot(slot as u32)).unwrap().index(),
                slot % 2
            );
        }
        assert_eq!(query.layout().cell(r::Slot(u32::MAX)), None);
    }
}

#[test]
fn physical_layout_is_derived_without_changing_serialized_logical_ids() {
    let query = projection_chain(16);
    let json = serde_json::to_value(&query).unwrap();
    assert!(json.get("layout").is_none());
    assert_eq!(json["returns"][0][1], 15);
    let decoded: r::Query = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, query);
    assert_eq!(decoded.layout().width(), 2);

    let unused = r::Query::new(query.bindings().to_vec(), vec![], vec![]).unwrap();
    assert_eq!(unused.layout().width(), 0);
    assert_eq!(unused.layout().cell(r::Slot(0)), None);
}

#[test]
fn physical_program_preserves_logical_diagnostics_and_shares_native_access_programs() {
    let bindings = (0..3)
        .map(|index| r::Binding {
            name: format!("node_{index}"),
            kind: r::BindingType::Node,
            nullable: false,
            value_type: r::ValueType::Node,
        })
        .collect();
    let mut operators = vec![r::Operator::Match {
        pattern: r::Pattern {
            nodes: vec![r::NodePattern {
                slot: r::Slot(0),
                label: Some("N".into()),
                properties: vec![],
            }],
            relationships: vec![],
            paths: vec![],
        },
        optional: false,
        predicate: None,
    }];
    operators.extend((1..3).map(|index| {
        r::Operator::Project {
            items: r::ProjectionProgram::new(vec![r::Projection {
                slot: r::Slot(index),
                expression: r::Expression::Slot(r::Slot(index - 1)),
            }])
            .unwrap(),
            distinct: false,
            ordering: vec![],
            predicate: None,
            skip: None,
            limit: None,
        }
    }));
    let query = r::Query::new(bindings, operators, vec![("node".into(), r::Slot(2))]).unwrap();
    let selected = r::plan(
        query.clone(),
        &helix_planner::context::PlannerContext::default(),
    )
    .unwrap();
    assert_eq!(selected.query(), &query);
    assert_eq!(selected.program().query().width(), 2);
    assert_eq!(selected.query().returns()[0].1, r::Slot(2));
    assert_eq!(selected.program().query().returns()[0].1, r::Slot(0));
    assert!(Arc::ptr_eq(
        &selected.matches()[&0].sources[0].access,
        &selected.program().matches()[&0].sources[0].access
    ));
    let serialized = serde_json::to_value(&selected).unwrap();
    assert!(serialized.get("program").is_none());
    let identity = selected.clone().with_layout(r::RowLayoutMode::Identity);
    assert_eq!(identity.program().query().width(), 3);
    assert_eq!(serde_json::to_value(&identity).unwrap(), serialized);
    assert_eq!(
        identity
            .with_execution(r::RowExecution::Materialized)
            .program()
            .layout_mode(),
        r::RowLayoutMode::Identity
    );
    assert_eq!(
        r::RowPlan::reference(query)
            .unwrap()
            .program()
            .layout_mode(),
        r::RowLayoutMode::Identity
    );
}

proptest::proptest! {
    #[test]
    fn validated_scope_layouts_keep_live_values_distinct(
        destinations in proptest::collection::vec(0_u32..16, 0..96),
    ) {
        let bindings = (0..16).map(|index| r::Binding {
            name: format!("value_{index}"), kind: r::BindingType::Scalar,
            nullable: false, value_type: r::ValueType::Integer,
        }).collect();
        let operators = destinations.iter().copied().map(|slot| r::Operator::Project {
            items: r::ProjectionProgram::new(vec![r::Projection {
                slot: r::Slot(slot), expression: r::Expression::Literal(r::Value::Integer(1)),
            }]).unwrap(),
            distinct: false, ordering: vec![], predicate: None, skip: None, limit: None,
        }).collect();
        let query = r::Query::new(bindings, operators, vec![]).unwrap();
        proptest::prop_assert!(query.layout().width() <= 16);
        for contract in query.contracts() {
            let live = contract.input().columns().keys().chain(contract.output().columns().keys())
                .copied().collect::<BTreeSet<_>>();
            let cells = live.iter().map(|slot| query.layout().cell(*slot).unwrap())
                .collect::<BTreeSet<_>>();
            proptest::prop_assert_eq!(cells.len(), live.len());
        }
        let decoded: r::Query = serde_json::from_slice(&serde_json::to_vec(&query).unwrap()).unwrap();
        proptest::prop_assert_eq!(decoded.layout(), query.layout());
    }
}
