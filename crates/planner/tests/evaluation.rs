use helix_planner::relational as r;
use std::collections::BTreeMap;

struct NoGraph;
impl r::GraphValues for NoGraph {
    fn properties(&self, _: r::Entity) -> r::Result<&r::GraphProperties> {
        panic!("scalar expression attempted graph access")
    }
    fn label(&self, _: r::Entity) -> r::Result<Option<&str>> {
        panic!("scalar expression attempted graph access")
    }
}

#[test]
fn owned_expression_temporaries_preserve_inputs_and_function_arguments() {
    use r::{Expression as E, Function as F, Value as V};
    let nested = V::Map(BTreeMap::from([("key".into(), V::String("value".into()))]));
    let list = V::List(vec![V::Integer(1), nested.clone(), V::Integer(3)]);
    let row = vec![list.clone(), nested.clone()];
    let parameters = BTreeMap::from([("list".into(), list.clone())]);
    let evaluation = r::Evaluation {
        row: &row,
        parameters: &parameters,
        graph: &NoGraph,
        group: None,
        max_collection_items: 100,
        max_value_bytes: 16_384,
    };
    let integer = |n| E::Literal(V::Integer(n));
    for (expression, expected) in [
        (
            E::Index(Box::new(E::Slot(r::Slot(0))), Box::new(integer(-2))),
            nested.clone(),
        ),
        (
            E::Property(Box::new(E::Slot(r::Slot(1))), "key".into()),
            V::String("value".into()),
        ),
        (
            E::Index(
                Box::new(E::Slot(r::Slot(1))),
                Box::new(E::Literal(V::String("key".into()))),
            ),
            V::String("value".into()),
        ),
        (
            E::Slice {
                value: Box::new(E::Parameter("list".into())),
                start: Some(Box::new(integer(1))),
                end: Some(Box::new(integer(-1))),
            },
            V::List(vec![nested.clone()]),
        ),
        (
            E::Function(F::Head, vec![E::Slot(r::Slot(0))]),
            V::Integer(1),
        ),
        (
            E::Function(F::Last, vec![E::Slot(r::Slot(0))]),
            V::Integer(3),
        ),
        (
            E::Function(F::Properties, vec![E::Slot(r::Slot(1))]),
            nested.clone(),
        ),
        (
            E::Function(F::Range, vec![integer(3), integer(1), integer(-1)]),
            V::List(vec![V::Integer(3), V::Integer(2), V::Integer(1)]),
        ),
        (
            E::Function(F::Range, vec![E::Literal(V::Null), integer(1)]),
            V::Null,
        ),
        (
            E::Function(
                F::Substring,
                vec![
                    E::Literal(V::String("aλ猫z".into())),
                    integer(1),
                    integer(2),
                ],
            ),
            V::String("λ猫".into()),
        ),
    ] {
        assert_eq!(evaluation.eval(&expression).unwrap(), expected);
        assert_eq!(row, vec![list.clone(), nested.clone()]);
        assert_eq!(parameters["list"], list);
    }
}

#[test]
fn unwind_range_admits_live_arguments_and_keeps_argument_errors_visible() {
    use r::{Expression as E, Function as F, Value as V};
    let parameters = BTreeMap::from([("large".into(), V::String("x".repeat(700)))]);
    let evaluation = r::Evaluation {
        row: &[],
        parameters: &parameters,
        graph: &NoGraph,
        group: None,
        max_collection_items: 100,
        max_value_bytes: 1200,
    };
    let range = E::Function(
        F::Range,
        vec![E::Parameter("large".into()), E::Parameter("large".into())],
    );
    assert!(evaluation.eval(&E::Parameter("large".into())).is_ok());
    assert!(matches!(evaluation.unwind(&range), Err(error) if error.detail == "MemoryLimit"));
    let null_with_missing = E::Function(
        F::Range,
        vec![E::Literal(V::Null), E::Parameter("missing".into())],
    );
    assert!(
        matches!(evaluation.unwind(&null_with_missing), Err(error) if error.detail == "MissingParameter")
    );
    let too_small = r::Evaluation {
        max_value_bytes: 1,
        ..evaluation
    };
    assert!(matches!(too_small.unwind(&range), Err(error) if error.detail == "MemoryLimit"));
}

#[test]
fn overlapping_index_and_slice_operands_are_admitted_together() {
    use r::{Expression as E, Function as F, Value as V};
    let large = V::String("x".repeat(1024));
    let list = V::List(vec![large.clone()]);
    let row = vec![list.clone()];
    let parameters = BTreeMap::from([("large".into(), large.clone())]);
    let evaluation = r::Evaluation {
        row: &row,
        parameters: &parameters,
        graph: &NoGraph,
        group: None,
        max_collection_items: 100,
        max_value_bytes: 1600,
    };
    let index = E::Function(F::Size, vec![E::Parameter("large".into())]);
    // Either operand fits alone. The index expression still needs its large
    // argument while the list operand is live, even though its result is small.
    assert_eq!(evaluation.eval(&E::Slot(r::Slot(0))).unwrap(), list);
    assert_eq!(evaluation.eval(&index).unwrap(), V::Integer(1024));
    for expression in [
        E::Index(Box::new(E::Slot(r::Slot(0))), Box::new(index.clone())),
        E::Slice {
            value: Box::new(E::Slot(r::Slot(0))),
            start: Some(Box::new(index.clone())),
            end: None,
        },
        E::Slice {
            value: Box::new(E::Slot(r::Slot(0))),
            start: None,
            end: Some(Box::new(index)),
        },
    ] {
        let error = evaluation.eval(&expression).unwrap_err();
        assert_eq!(
            (&*error.category, &*error.detail),
            ("ResourceLimit", "MemoryLimit")
        );
    }
    let small = r::Evaluation {
        max_value_bytes: 512,
        ..evaluation
    };
    for expression in [E::Slot(r::Slot(0)), E::Parameter("large".into())] {
        assert_eq!(small.eval(&expression).unwrap_err().detail, "MemoryLimit");
    }
}

#[test]
fn graph_outputs_and_path_expansion_are_admitted_before_materialization() {
    use r::{Entity, Expression as E, Function as F, Value as V};
    struct Graph {
        properties: r::GraphProperties,
        label: String,
    }
    impl r::GraphValues for Graph {
        fn properties(&self, _: Entity) -> r::Result<&r::GraphProperties> {
            Ok(&self.properties)
        }
        fn label(&self, _: Entity) -> r::Result<Option<&str>> {
            Ok(Some(&self.label))
        }
    }
    let graph = Graph {
        properties: BTreeMap::from([
            ("large".into(), Ok(V::String("x".repeat(2048)))),
            ("small".into(), Ok(V::Integer(7))),
            ("k".repeat(2048), Ok(V::Null)),
        ]),
        label: "λ".repeat(1024),
    };
    let parameters = BTreeMap::new();
    let evaluation = r::Evaluation {
        row: &[],
        parameters: &parameters,
        graph: &graph,
        group: None,
        max_collection_items: 100,
        max_value_bytes: 1024,
    };
    let node = E::Literal(V::Entity(Entity::Node(1)));
    assert_eq!(
        evaluation
            .eval(&E::Property(Box::new(node.clone()), "small".into()))
            .unwrap(),
        V::Integer(7),
    );
    for expression in [
        E::Property(Box::new(node.clone()), "large".into()),
        E::Index(
            Box::new(node.clone()),
            Box::new(E::Literal(V::String("large".into()))),
        ),
        E::Function(F::Properties, vec![node.clone()]),
        E::Function(F::Keys, vec![node.clone()]),
        E::Function(F::Labels, vec![node]),
        E::Function(
            F::Type,
            vec![E::Literal(V::Entity(Entity::Relationship(2)))],
        ),
    ] {
        assert_eq!(
            evaluation.eval(&expression).unwrap_err().detail,
            "MemoryLimit"
        );
    }
    let path = E::Literal(V::Path(
        r::Path::new((0..100).collect(), (100..199).collect()).unwrap(),
    ));
    let evaluation = r::Evaluation {
        max_value_bytes: 5000,
        ..evaluation
    };
    assert!(evaluation.eval(&path).is_ok());
    for function in [F::Nodes, F::Relationships] {
        assert_eq!(
            evaluation
                .eval(&E::Function(function, vec![path.clone()]))
                .unwrap_err()
                .detail,
            "MemoryLimit"
        );
    }
}
