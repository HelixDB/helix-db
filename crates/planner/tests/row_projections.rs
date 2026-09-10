use helix_planner::relational as r;
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    task::{Context, Poll, Waker},
};

struct NoGraph;
impl r::GraphValues for NoGraph {
    fn properties(&self, _: r::Entity) -> r::Result<&r::GraphProperties> {
        panic!("scalar projection must not load graph properties")
    }
    fn label(&self, _: r::Entity) -> r::Result<Option<&str>> {
        panic!("scalar projection must not load graph labels")
    }
}

fn evaluate(
    program: &r::ProjectionProgram,
    row: &[r::Value],
    budget: usize,
) -> r::Result<Vec<r::Value>> {
    let mut evaluator = r::Evaluation {
        row,
        parameters: &BTreeMap::new(),
        graph: &NoGraph,
        group: None,
        max_collection_items: 1000,
        max_value_bytes: budget,
    };
    let future = program.evaluate(&mut evaluator);
    let Poll::Ready(result) = std::pin::pin!(future)
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("scalar adapter does not suspend");
    };
    result
}

#[test]
fn simultaneous_destinations_preserve_input_order_and_nulls() {
    let program = r::ProjectionProgram::new(vec![
        r::Projection {
            slot: r::Slot(0),
            expression: r::Expression::Slot(r::Slot(1)),
        },
        r::Projection {
            slot: r::Slot(1),
            expression: r::Expression::Slot(r::Slot(0)),
        },
        r::Projection {
            slot: r::Slot(2),
            expression: r::Expression::Literal(r::Value::Null),
        },
    ])
    .unwrap();
    let row = vec![r::Value::Integer(10), r::Value::Integer(20)];
    program
        .validate_input(&BTreeSet::from([r::Slot(0), r::Slot(1)]))
        .unwrap();
    assert_eq!(
        evaluate(&program, &row, 4096).unwrap(),
        vec![r::Value::Integer(20), r::Value::Integer(10), r::Value::Null]
    );
    assert_eq!(row, vec![r::Value::Integer(10), r::Value::Integer(20)]);
    assert_eq!(
        program.references(),
        &BTreeSet::from([r::Slot(0), r::Slot(1)])
    );
    assert_eq!(
        program.outputs(),
        &BTreeSet::from([r::Slot(0), r::Slot(1), r::Slot(2)])
    );
    assert_eq!(
        program
            .into_iter()
            .map(|item| item.slot)
            .collect::<Vec<_>>(),
        vec![r::Slot(0), r::Slot(1), r::Slot(2)]
    );
    assert_eq!(
        serde_json::from_str::<r::ProjectionProgram>(&serde_json::to_string(&program).unwrap())
            .unwrap(),
        program
    );
    let empty = r::ProjectionProgram::new(Vec::new()).unwrap();
    assert!(evaluate(&empty, &[], 4096).unwrap().is_empty());
}

#[test]
fn projection_validation_rejects_duplicate_dangling_and_malformed_expressions() {
    let item = r::Projection {
        slot: r::Slot(1),
        expression: r::Expression::Slot(r::Slot(0)),
    };
    assert_eq!(
        r::ProjectionProgram::new(vec![item.clone(), item.clone()])
            .unwrap_err()
            .detail,
        "InvalidSchema"
    );
    let encoded = serde_json::to_string(&vec![item.clone(), item.clone()]).unwrap();
    assert!(serde_json::from_str::<r::ProjectionProgram>(&encoded).is_err());
    let program = r::ProjectionProgram::new(vec![item]).unwrap();
    assert_eq!(
        program
            .validate_input(&BTreeSet::from([r::Slot(1)]))
            .unwrap_err()
            .detail,
        "UnboundSlot"
    );
    assert_eq!(
        r::ProjectionProgram::new(vec![r::Projection {
            slot: r::Slot(0),
            expression: r::Expression::Function(r::Function::Id, vec![]),
        }])
        .unwrap_err()
        .detail,
        "FunctionArity"
    );
}

#[test]
fn earlier_values_consume_the_budget_and_expression_failures_keep_their_order() {
    let row = vec![r::Value::String("x".repeat(512))];
    let make = |count| {
        r::ProjectionProgram::new(
            (0..count)
                .map(|index| r::Projection {
                    slot: r::Slot(index),
                    expression: r::Expression::Slot(r::Slot(0)),
                })
                .collect(),
        )
        .unwrap()
    };
    assert!(evaluate(&make(1), &row, 1000).is_ok());
    assert_eq!(
        evaluate(&make(2), &row, 1000).unwrap_err().detail,
        "MemoryLimit"
    );
    assert_eq!(
        evaluate(&make(1), &row, 1).unwrap_err().detail,
        "MemoryLimit"
    );
    let program = r::ProjectionProgram::new(vec![
        r::Projection {
            slot: r::Slot(0),
            expression: r::Expression::Parameter("first".into()),
        },
        r::Projection {
            slot: r::Slot(1),
            expression: r::Expression::Slot(r::Slot(9)),
        },
    ])
    .unwrap();
    assert_eq!(
        evaluate(&program, &row, 4096).unwrap_err().detail,
        "MissingParameter"
    );
    assert_eq!(row, vec![r::Value::String("x".repeat(512))]);
}
