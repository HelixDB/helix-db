use helix_planner::{ir, relational as r};
use std::{
    collections::BTreeMap,
    ops::ControlFlow,
    sync::atomic::{AtomicUsize, Ordering},
};

struct Graph {
    reads: AtomicUsize,
    properties: r::GraphProperties,
}
impl r::GraphValues for Graph {
    fn properties(&self, _: r::Entity) -> r::Result<&r::GraphProperties> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        Ok(&self.properties)
    }
    fn label(&self, _: r::Entity) -> r::Result<Option<&str>> {
        Ok(Some("N"))
    }
}

#[test]
fn operand_is_evaluated_once_even_when_every_alternative_misses() {
    let graph = Graph {
        reads: AtomicUsize::new(0),
        properties: BTreeMap::from([("value".into(), Ok(r::Value::Integer(100)))]),
    };
    let parameters = BTreeMap::new();
    let evaluation = r::Evaluation {
        row: &[],
        parameters: &parameters,
        graph: &graph,
        group: None,
        max_collection_items: usize::MAX,
        memory: r::EvaluationMemory::new(usize::MAX),
    };
    for count in [1, 4, 16, 64] {
        let expression = r::Expression::SimpleCase(Box::new(r::SimpleCase {
            operand: r::Expression::Property(
                Box::new(r::Expression::Literal(r::Value::Entity(r::Entity::Node(1)))),
                "value".into(),
            ),
            branches: ir::AtLeast::try_from_vec(
                (0..count)
                    .map(|n| {
                        (
                            r::Expression::Literal(r::Value::Integer(n)),
                            r::Expression::Parameter("unselected".into()),
                        )
                    })
                    .collect(),
            )
            .unwrap(),
            otherwise: r::Expression::Literal(r::Value::Integer(7)),
        }));
        assert_eq!(evaluation.eval(&expression).unwrap(), r::Value::Integer(7));
        assert_eq!(graph.reads.swap(0, Ordering::Relaxed), 1);
    }
}

#[test]
fn live_operand_is_charged_during_comparison_and_released_before_results() {
    let graph = Graph {
        reads: AtomicUsize::new(0),
        properties: BTreeMap::new(),
    };
    let parameters = BTreeMap::new();
    let literal = |text: String| r::Expression::Literal(r::Value::String(text));
    for matched in [false, true] {
        let expression = r::Expression::SimpleCase(Box::new(r::SimpleCase {
            operand: literal("x".repeat(800)),
            branches: ir::AtLeast::from_one((
                literal(if matched { "x" } else { "y" }.repeat(800)),
                literal("z".repeat(1500)),
            )),
            otherwise: literal("z".repeat(1500)),
        }));
        let evaluation = r::Evaluation {
            row: &[],
            parameters: &parameters,
            graph: &graph,
            group: None,
            max_collection_items: usize::MAX,
            memory: r::EvaluationMemory::new(2000),
        };
        assert_eq!(
            evaluation.eval(&expression).unwrap(),
            r::Value::String("z".repeat(1500))
        );
        let constrained = r::Evaluation {
            memory: r::EvaluationMemory::new(1000),
            ..evaluation
        };
        let error = constrained.eval(&expression).unwrap_err();
        assert_eq!(error.category, "ResourceLimit");
        assert_eq!(error.detail, "MemoryLimit");
    }
}

#[test]
fn all_case_slots_are_visited_rewritten_and_checked() {
    let expression = r::Expression::SimpleCase(Box::new(r::SimpleCase {
        operand: r::Expression::Slot(r::Slot(0)),
        branches: ir::AtLeast::from_one((
            r::Expression::HasLabel(r::Slot(1), "N".into()),
            r::Expression::Slot(r::Slot(2)),
        )),
        otherwise: r::Expression::Slot(r::Slot(3)),
    }));
    let mut seen = Vec::new();
    expression.visit(&mut |node| {
        let (r::Expression::Slot(slot) | r::Expression::HasLabel(slot, _)) = node else {
            return;
        };
        seen.push(slot.0);
    });
    assert_eq!(seen, [0, 1, 2, 3]);
    let map = |node: &r::Expression| match node {
        r::Expression::Slot(slot) => Some(r::Expression::Slot(r::Slot(slot.0 + 4))),
        r::Expression::HasLabel(slot, label) => {
            Some(r::Expression::HasLabel(r::Slot(slot.0 + 4), label.clone()))
        }
        _ => None,
    };
    let borrowed = expression.rewrite(&mut |node| Ok(map(node))).unwrap();
    let owned = expression
        .clone()
        .rewrite_owned(&mut |node| {
            Ok::<_, r::QueryError>(match map(&node) {
                Some(replacement) => ControlFlow::Break(replacement),
                None => ControlFlow::Continue(node),
            })
        })
        .unwrap();
    assert_eq!(borrowed, owned);
    assert_eq!(owned.slots(), (4..8).map(r::Slot).collect());
    let mut json = serde_json::to_value(&expression).unwrap();
    assert_eq!(
        serde_json::from_value::<r::Expression>(json.clone()).unwrap(),
        expression
    );
    json["SimpleCase"]["branches"] = serde_json::json!([]);
    assert!(serde_json::from_value::<r::Expression>(json).is_err());
    let mut invalid = expression;
    for _ in 0..r::MAX_EXPRESSION_DEPTH {
        invalid = r::Expression::Unary(r::Unary::Positive, Box::new(invalid));
    }
    assert_eq!(
        invalid.validate_shape().unwrap_err().category,
        "ResourceLimit"
    );
}

struct NonClone(u8, std::rc::Rc<std::cell::Cell<usize>>);
impl Drop for NonClone {
    fn drop(&mut self) {
        self.1.set(self.1.get() + 1);
    }
}
#[test]
fn owned_case_rewrites_drop_each_payload_once_and_stop_in_preorder() {
    type E = r::ScalarExpression<NonClone, (), (), ()>;
    for failure in [None, Some(1), Some(2), Some(3), Some(4)] {
        let drops = std::rc::Rc::new(std::cell::Cell::new(0));
        let literal = |id| E::Literal(NonClone(id, drops.clone()));
        let expression = E::SimpleCase(Box::new(r::SimpleCase {
            operand: literal(1),
            branches: ir::AtLeast::from_one((literal(2), literal(3))),
            otherwise: literal(4),
        }));
        let mut seen = Vec::new();
        let result = expression.rewrite_owned(&mut |node| {
            let E::Literal(value) = &node else {
                return Ok(ControlFlow::Continue(node));
            };
            seen.push(value.0);
            if failure == Some(value.0) {
                Err(value.0)
            } else {
                Ok(ControlFlow::Continue(node))
            }
        });
        assert_eq!(result.as_ref().err().copied(), failure);
        assert_eq!(seen, (1..=failure.unwrap_or(4)).collect::<Vec<_>>());
        drop(result);
        assert_eq!(drops.get(), 4);
    }
    // A boxed CASE adds no payload width to the existing generic enum layout.
    assert_eq!(
        size_of::<r::ScalarExpression<u8, (), (), ()>>(),
        5 * size_of::<usize>()
    );
}
