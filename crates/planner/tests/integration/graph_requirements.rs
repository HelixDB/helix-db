use helix_planner::relational as r;
use std::collections::{BTreeMap, BTreeSet};

fn field(slot: u32, key: &str) -> r::Expression {
    r::Expression::Property(Box::new(r::Expression::Slot(r::Slot(slot))), key.into())
}

#[test]
fn borrowed_requirements_preserve_selected_metadata_and_dynamic_demands() {
    use r::Expression as E;
    let expressions = [
        field(0, "a"),
        field(0, "a"),
        field(0, "b"),
        E::HasLabel(r::Slot(1), "N".into()),
        E::Function(r::Function::Labels, vec![E::Slot(r::Slot(2))]),
        E::Function(r::Function::Type, vec![E::Slot(r::Slot(3))]),
        E::Function(r::Function::Properties, vec![E::Slot(r::Slot(4))]),
        E::Function(r::Function::Keys, vec![E::Slot(r::Slot(5))]),
        E::Index(
            Box::new(E::Slot(r::Slot(6))),
            Box::new(E::Parameter("key".into())),
        ),
        E::Index(
            Box::new(E::Slot(r::Slot(7))),
            Box::new(E::Literal(r::Value::Integer(0))),
        ),
        E::Slot(r::Slot(8)),
        E::Function(r::Function::Id, vec![E::Slot(r::Slot(9))]),
        field(4, "already_all"),
        E::Function(r::Function::Keys, vec![E::Slot(r::Slot(0))]),
        field(10, "selected"),
    ];
    let mut actual = BTreeMap::new();
    for expression in &expressions {
        expression.graph_requirements(&mut actual);
    }
    let empty = || r::PropertyDemand::Keys(BTreeSet::new());
    assert_eq!(
        actual,
        BTreeMap::from([
            (r::Slot(0), r::PropertyDemand::All),
            (r::Slot(1), empty()),
            (r::Slot(2), empty()),
            (r::Slot(3), empty()),
            (r::Slot(4), r::PropertyDemand::All),
            (r::Slot(5), r::PropertyDemand::All),
            (r::Slot(6), r::PropertyDemand::All),
            (
                r::Slot(10),
                r::PropertyDemand::Keys(BTreeSet::from(["selected".into()]))
            ),
        ])
    );
}

#[test]
fn borrowed_requirement_errors_stop_before_later_fields_or_branches() {
    let expression = r::Expression::List(vec![field(0, "first"), field(1, "later")]);
    let mut seen = Vec::new();
    let result = expression.try_graph_requirements(|slot, requirement| {
        seen.push((slot, requirement));
        Err("denied")
    });
    assert_eq!(result, Err("denied"));
    assert_eq!(seen, [(r::Slot(0), r::PropertyRequirement::Key("first"))]);
    let mut visits = 0;
    let result = expression.try_visit(&mut |_| {
        visits += 1;
        if visits == 2 {
            Err("stop")
        } else {
            Ok(())
        }
    });
    assert_eq!(result, Err("stop"));
    assert_eq!(visits, 2);
}

#[test]
fn fallible_visitors_preserve_child_order_across_every_container() {
    use r::Expression as E;
    let expression = E::List(vec![
        E::Unary(r::Unary::Not, Box::new(field(0, "p"))),
        E::Binary(
            r::Binary::Add,
            Box::new(field(1, "p")),
            Box::new(field(2, "p")),
        ),
        E::Slice {
            value: Box::new(field(3, "p")),
            start: Some(Box::new(field(4, "p"))),
            end: Some(Box::new(field(5, "p"))),
        },
        E::Function(r::Function::Coalesce, vec![field(6, "p"), field(7, "p")]),
        E::Aggregate {
            function: r::Aggregate::Count,
            argument: Some(Box::new(field(8, "p"))),
            distinct: false,
        },
        E::Map(vec![("k".into(), field(9, "p"))]),
        E::Case {
            branches: vec![(field(10, "p"), field(11, "p"))],
            otherwise: Box::new(field(12, "p")),
        },
        E::Index(
            Box::new(field(13, "p")),
            Box::new(E::Literal(r::Value::Integer(0))),
        ),
        field(14, "p"),
        E::SimpleCase(Box::new(r::SimpleCase {
            operand: field(15, "p"),
            branches: helix_planner::ir::AtLeast::from_one((field(16, "p"), field(17, "p"))),
            otherwise: field(18, "p"),
        })),
        E::Parameter("unused".into()),
    ]);
    expression.validate_shape().unwrap();
    let mut demands = Vec::new();
    expression
        .try_graph_requirements(|slot, demand| {
            demands.push((slot, demand));
            Ok::<_, ()>(())
        })
        .unwrap();
    assert_eq!(
        demands,
        (0..19)
            .map(|i| (r::Slot(i), r::PropertyRequirement::Key("p")))
            .collect::<Vec<_>>()
    );
    for stop in 0..19 {
        let mut slots = Vec::new();
        let result = expression.try_visit(&mut |expression| {
            let E::Slot(slot) = expression else {
                return Ok(());
            };
            slots.push(*slot);
            if slot.0 == stop {
                Err(stop)
            } else {
                Ok(())
            }
        });
        assert_eq!(result, Err(stop));
        assert_eq!(slots, (0..=stop).map(r::Slot).collect::<Vec<_>>());
    }
}
