use helix_planner::relational as r;
#[path = "../src/analysis/tests/allocations.rs"]
mod allocation;
use r::TraversalControl;

struct NonClone(u8);
type Expr = r::ScalarExpression<NonClone, (), (), ()>;
fn tree() -> Expr {
    let literal = |id| Expr::Literal(NonClone(id));
    Expr::List(vec![
        Expr::Property(Box::new(Expr::Slot(r::Slot(0))), "key".into()),
        Expr::Index(Box::new(literal(0)), Box::new(literal(1))),
        Expr::Slice {
            value: Box::new(Expr::List(vec![literal(2)])),
            start: Some(Box::new(literal(3))),
            end: Some(Box::new(literal(4))),
        },
        Expr::Unary((), Box::new(literal(5))),
        Expr::Binary((), Box::new(literal(6)), Box::new(literal(7))),
        Expr::Function((), vec![literal(8)]),
        Expr::Aggregate {
            function: r::Aggregate::Count,
            argument: Some(Box::new(literal(9))),
            distinct: false,
        },
        Expr::Map(vec![("entry".into(), literal(10))]),
        Expr::Case {
            branches: vec![(literal(11), literal(12))],
            otherwise: Box::new(literal(13)),
        },
        Expr::HasLabel(r::Slot(1), "N".into()),
        Expr::Parameter("p".into()),
    ])
}

#[test]
fn traversal_borrows_nonclone_leaves_in_stable_child_order() {
    let expression = tree();
    let mut ids = Vec::new();
    let mut slots = Vec::new();
    let mut labels = Vec::new();
    let mut parameters = Vec::new();
    expression
        .try_visit_pruned(&mut |node| {
            match node {
                Expr::Literal(NonClone(id)) => ids.push(*id),
                Expr::Slot(slot) => slots.push(*slot),
                Expr::HasLabel(slot, label) => labels.push((*slot, label.as_str())),
                Expr::Parameter(name) => parameters.push(name.as_str()),
                Expr::Property(..)
                | Expr::Index(..)
                | Expr::Slice { .. }
                | Expr::Unary(..)
                | Expr::Binary(..)
                | Expr::Function(..)
                | Expr::Aggregate { .. }
                | Expr::List(_)
                | Expr::Map(_)
                | Expr::Case { .. } => {}
            }
            Ok::<_, ()>(TraversalControl::Descend)
        })
        .unwrap();
    assert_eq!(ids, (0..14).collect::<Vec<_>>());
    assert_eq!(slots, [r::Slot(0)]);
    assert_eq!(labels, [(r::Slot(1), "N")]);
    assert_eq!(parameters, ["p"]);
}

#[test]
fn pruning_visits_the_root_and_skips_only_that_subtree() {
    let expression = tree();
    let mut ids = Vec::new();
    expression
        .try_visit_pruned(&mut |node| {
            if matches!(
                node,
                Expr::Slice { .. }
                    | Expr::Function(..)
                    | Expr::Aggregate { .. }
                    | Expr::Property(..)
            ) {
                return Ok::<_, ()>(TraversalControl::Prune);
            }
            let Expr::Literal(NonClone(id)) = node else {
                return Ok(TraversalControl::Descend);
            };
            ids.push(*id);
            Ok(TraversalControl::Descend)
        })
        .unwrap();
    assert_eq!(ids, [0, 1, 5, 6, 7, 10, 11, 12, 13]);
    let mut roots = 0;
    expression
        .try_visit_pruned(&mut |_| {
            roots += 1;
            Ok::<_, ()>(TraversalControl::Prune)
        })
        .unwrap();
    assert_eq!(roots, 1);
}

#[test]
fn the_first_visitor_error_stops_before_later_siblings() {
    let expression = tree();
    let mut ids = Vec::new();
    let result = expression.try_visit_pruned(&mut |node| {
        let Expr::Literal(NonClone(id)) = node else {
            return Ok(TraversalControl::Descend);
        };
        ids.push(*id);
        if *id == 6 {
            Err(*id)
        } else {
            Ok(TraversalControl::Descend)
        }
    });
    assert_eq!(result, Err(6));
    assert_eq!(ids, [0, 1, 2, 3, 4, 5, 6]);
}

#[test]
fn empty_optional_children_are_valid_and_wide_deep_walks_allocate_nothing() {
    let empty = Expr::List(vec![
        Expr::Aggregate {
            function: r::Aggregate::Count,
            argument: None,
            distinct: false,
        },
        Expr::Slice {
            value: Box::new(Expr::Literal(NonClone(0))),
            start: None,
            end: None,
        },
        Expr::Map(vec![]),
        Expr::Function((), vec![]),
    ]);
    let wide = Expr::List((0..10000).map(|_| Expr::Literal(NonClone(0))).collect());
    let deep = (0..47).fold(Expr::Literal(NonClone(0)), |expression, _| {
        Expr::Unary((), Box::new(expression))
    });
    for (expression, expected) in [(empty, 6), (wide, 10001), (deep, 48)] {
        let ((result, visited), count) = allocation::observe(|| {
            let mut visited = 0;
            let result = expression.try_visit_pruned(&mut |_| {
                visited += 1;
                Ok::<_, ()>(TraversalControl::Descend)
            });
            (result, visited)
        });
        assert_eq!(result, Ok(()));
        assert_eq!(visited, expected);
        assert_eq!((count.allocations, count.bytes), (0, 0));
    }
}
