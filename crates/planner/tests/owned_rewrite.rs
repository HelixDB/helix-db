use helix_planner::relational as r;
#[path = "../src/analysis/tests/allocations.rs"]
mod allocations;
use std::ops::ControlFlow;

struct NonClone(u8, std::rc::Rc<std::cell::Cell<usize>>);
impl Drop for NonClone {
    fn drop(&mut self) {
        self.1.set(self.1.get() + 1);
    }
}
type Expr = r::ScalarExpression<NonClone, NonClone, NonClone, NonClone>;

#[test]
fn owned_rewrites_move_nonclone_payloads_and_stop_at_the_first_error() {
    for fail in [None, Some(1), Some(2), Some(3)] {
        let drops = std::rc::Rc::new(std::cell::Cell::new(0));
        let literal = |id| Expr::Literal(NonClone(id, drops.clone()));
        let expression = Expr::Function(
            NonClone(10, drops.clone()),
            vec![
                Expr::Binary(
                    NonClone(11, drops.clone()),
                    Box::new(literal(1)),
                    Box::new(Expr::Unary(
                        NonClone(12, drops.clone()),
                        Box::new(literal(2)),
                    )),
                ),
                literal(3),
            ],
        );
        let mut seen = Vec::new();
        let result = expression.rewrite_owned(&mut |node| {
            let Expr::Literal(value) = &node else {
                return Ok(ControlFlow::Continue(node));
            };
            seen.push(value.0);
            if fail == Some(value.0) {
                Err(value.0)
            } else {
                Ok(ControlFlow::Continue(node))
            }
        });
        assert_eq!(seen, (1..=fail.unwrap_or(3)).collect::<Vec<_>>());
        assert_eq!(result.as_ref().err().copied(), fail);
        drop(result);
        assert_eq!(drops.get(), 6);
    }
}

#[test]
fn replacement_prunes_children_and_drops_replaced_owners_once() {
    let drops = std::rc::Rc::new(std::cell::Cell::new(0));
    let expression = Expr::List(
        (1..=3)
            .map(|id| Expr::Literal(NonClone(id, drops.clone())))
            .collect(),
    );
    let mut seen = 0;
    let result = expression
        .rewrite_owned(&mut |_| {
            seen += 1;
            Ok::<_, ()>(ControlFlow::Break(Expr::Slot(r::Slot(9))))
        })
        .unwrap();
    assert!(matches!(result, Expr::Slot(r::Slot(9))));
    assert_eq!(seen, 1);
    assert_eq!(drops.get(), 3);
}

#[test]
fn large_payloads_are_transferred_without_copying() {
    let payload = "x".repeat(1024 * 1024);
    let pointer = payload.as_ptr();
    let expression = r::Expression::Binary(
        r::Binary::Add,
        Box::new(r::Expression::Literal(r::Value::String(payload))),
        Box::new(r::Expression::Aggregate {
            function: r::Aggregate::Count,
            argument: None,
            distinct: false,
        }),
    );
    let ((result, aggregate), count) = allocations::observe(|| {
        let mut aggregate = None;
        let result = expression
            .rewrite_owned(&mut |node| {
                if matches!(node, r::Expression::Aggregate { .. }) {
                    aggregate = Some(node);
                    return Ok::<_, ()>(ControlFlow::Break(r::Expression::Slot(r::Slot(7))));
                }
                Ok(ControlFlow::Continue(node))
            })
            .unwrap();
        (result, aggregate)
    });
    assert!(count.bytes < 4096, "{count:?}");
    let r::Expression::Binary(_, left, right) = result else {
        panic!("binary result")
    };
    let r::Expression::Literal(r::Value::String(value)) = *left else {
        panic!("owned literal")
    };
    assert_eq!(value.as_ptr(), pointer);
    assert_eq!(value.len(), 1024 * 1024);
    assert!(matches!(*right, r::Expression::Slot(r::Slot(7))));
    assert!(matches!(
        aggregate,
        Some(r::Expression::Aggregate {
            function: r::Aggregate::Count,
            ..
        })
    ));
}

#[test]
fn every_child_shape_matches_existing_rewrite_order_and_pruning() {
    type E = r::ScalarExpression<u8, (), (), ()>;
    let leaf = E::Literal;
    let tree = E::List(vec![
        E::Property(Box::new(leaf(0)), "key".into()),
        E::Index(Box::new(leaf(1)), Box::new(leaf(2))),
        E::Slice {
            value: Box::new(leaf(3)),
            start: Some(Box::new(leaf(4))),
            end: Some(Box::new(leaf(5))),
        },
        E::Unary((), Box::new(leaf(6))),
        E::Binary((), Box::new(leaf(7)), Box::new(leaf(8))),
        E::Function((), vec![leaf(9)]),
        E::Aggregate {
            function: r::Aggregate::Count,
            argument: Some(Box::new(leaf(10))),
            distinct: false,
        },
        E::Map(vec![("entry".into(), leaf(11))]),
        E::Case {
            branches: vec![(leaf(12), leaf(13))],
            otherwise: Box::new(leaf(14)),
        },
        E::SimpleCase(Box::new(r::SimpleCase {
            operand: leaf(15),
            branches: helix_planner::ir::AtLeast::from_one((leaf(16), leaf(17))),
            otherwise: leaf(18),
        })),
        E::HasLabel(r::Slot(0), "N".into()),
        E::Parameter("p".into()),
        E::Slot(r::Slot(1)),
        E::Slice {
            value: Box::new(E::List(vec![])),
            start: None,
            end: None,
        },
        E::Aggregate {
            function: r::Aggregate::Count,
            argument: None,
            distinct: false,
        },
    ]);
    for prune in [false, true] {
        for failure in [
            None,
            Some(0),
            Some(7),
            Some(14),
            Some(15),
            Some(16),
            Some(17),
            Some(18),
        ] {
            let mut old_seen = Vec::new();
            let mut new_seen = Vec::new();
            let control = |node: &E, seen: &mut Vec<u8>| -> r::Result<bool> {
                if prune
                    && matches!(
                        node,
                        E::Property(..) | E::Function(..) | E::Aggregate { .. } | E::SimpleCase(_)
                    )
                {
                    return Ok(true);
                }
                let E::Literal(value) = node else {
                    return Ok(false);
                };
                seen.push(*value);
                if failure == Some(*value) {
                    return Err(r::QueryError::compile("TestError", "Stop", "first error"));
                }
                Ok(false)
            };
            let expected =
                tree.rewrite(&mut |node| Ok(control(node, &mut old_seen)?.then(|| node.clone())));
            let actual = tree.clone().rewrite_owned(&mut |node| {
                Ok::<_, r::QueryError>(if control(&node, &mut new_seen)? {
                    ControlFlow::Break(node)
                } else {
                    ControlFlow::Continue(node)
                })
            });
            assert_eq!(actual, expected);
            assert_eq!(new_seen, old_seen);
        }
    }
}
