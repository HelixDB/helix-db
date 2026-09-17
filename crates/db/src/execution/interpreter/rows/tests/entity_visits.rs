use super::super::memory;
use super::*;
use crate::execution::interpreter::test_support;
use helix_planner::context;
use r::GraphValues;

#[test]
fn graph_reference_visits_do_not_allocate_for_nested_or_repeated_values() {
    let mut counts = BTreeMap::from([
        (r::Entity::Node(1), 0),
        (r::Entity::Node(2), 0),
        (r::Entity::Node(7), 0),
        (r::Entity::Relationship(3), 0),
        (r::Entity::Relationship(4), 0),
        (r::Entity::Relationship(9), 0),
    ]);
    let value = r::Value::List(vec![
        r::Value::Entity(r::Entity::Node(7)),
        r::Value::Map(BTreeMap::from([(
            "path".into(),
            r::Value::Path(r::Path::new(vec![1, 2, 1], vec![3, 4]).unwrap()),
        )])),
        r::Value::Entity(r::Entity::Relationship(9)),
        r::Value::Null,
        r::Value::Boolean(true),
        r::Value::Integer(7),
        r::Value::Float(f64::NAN),
        r::Value::String("not a graph reference".into()),
        r::Value::List(Vec::new()),
        r::Value::List(vec![r::Value::Entity(r::Entity::Node(7)); 100_000]),
        r::Value::Map(BTreeMap::new()),
    ]);
    let (result, allocated) = crate::allocation_testing::observe(|| {
        visit_entities(&value, &mut |entity| {
            *counts.get_mut(&entity).expect("known fixture entity") += 1;
            Ok(())
        })
    });
    result.unwrap();
    assert_eq!((allocated.allocations, allocated.bytes), (0, 0));
    assert_eq!(
        counts,
        BTreeMap::from([
            (r::Entity::Node(1), 2),
            (r::Entity::Node(2), 1),
            (r::Entity::Node(7), 100_001),
            (r::Entity::Relationship(3), 1),
            (r::Entity::Relationship(4), 1),
            (r::Entity::Relationship(9), 1),
        ])
    );
}

#[test]
fn graph_reference_visits_stop_at_the_first_callback_error() {
    let value = r::Value::Map(BTreeMap::from([(
        "nested".into(),
        r::Value::List(vec![
            r::Value::Path(
                r::Path::new(vec![1, 2], vec![3]).unwrap()
            );
            100
        ]),
    )]));
    let mut visits = 0;
    let mut failure = Some(Error::Query(r::QueryError::runtime(
        "ResourceLimit",
        "ExpectedFailure",
        "visitor callback failure",
    )));
    let error = visit_entities(&value, &mut |_| {
        visits += 1;
        if visits == 5 {
            return Err(failure.take().unwrap());
        }
        Ok(())
    })
    .unwrap_err();
    assert!(matches!(error, Error::Query(error) if error.detail == "ExpectedFailure"));
    assert_eq!(visits, 5);
}

#[tokio::test]
async fn repeated_entity_demand_allocations_depend_on_unique_references() {
    let db = test_support::open_db("repeated-entity-demand").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.enable_request_read_view().await.unwrap();
    ctx.row_memory = Some(memory::Budget::new(64 * 1024));
    let demand = BTreeMap::from([(
        r::Slot(0),
        r::PropertyDemand::Keys(["first".into(), "second".into()].into()),
    )]);
    let mut observations = Vec::new();
    for references in [1, 17, 512, 100_000] {
        let rows = vec![vec![r::Value::Entity(r::Entity::Node(7))]; references];
        let before = ctx.row_budget().reads().multi_get_keys;
        let mut allocations = 0_usize;
        let mut bytes = 0_usize;
        let result = {
            let mut hydration = std::pin::pin!(ctx.graph_batch_required(&rows, &demand));
            futures::future::poll_fn(|cx| {
                let (poll, observed) =
                    crate::allocation_testing::observe(|| hydration.as_mut().poll(cx));
                allocations = allocations.saturating_add(observed.allocations);
                bytes = bytes.saturating_add(observed.bytes);
                poll
            })
            .await
        }
        .unwrap();
        assert!(result.entities.is_empty());
        assert_eq!(ctx.row_budget().reads().multi_get_keys - before, 1);
        drop(result);
        assert_eq!(ctx.row_budget().available(), 64 * 1024);
        observations.push((references, allocations, bytes));
    }
    // Storage and polling can have fixed setup allocations. Repeated graph IDs
    // must not allocate a temporary set or clone demand keys per input row.
    let (_, smallest_allocations, smallest_bytes) = observations[0];
    for &(references, allocations, bytes) in &observations[1..] {
        assert!(
            allocations <= smallest_allocations + 32 && bytes <= smallest_bytes + 4096,
            "demand allocation grew with {references} references: {observations:?}"
        );
    }
    ctx.close_request_read_view().unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn shared_graph_references_union_keys_and_upgrade_to_all_properties() {
    let db = test_support::open_db("entity-demand-union").await;
    let response = db
        .cypher(crate::cypher::Request::new(
            "CREATE (a:N {first:1,second:2,third:3,hidden:4})-[e:R {first:5,second:6,third:7,hidden:8}]->(b:N {first:9}) RETURN a,e,b",
        ))
        .await
        .unwrap();
    let id = |column: usize| {
        response.rows[0][column]["id"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    let (a, e, b) = (id(0), id(1), id(2));
    let rows = [vec![
        r::Value::Entity(r::Entity::Node(a)),
        r::Value::Map(BTreeMap::from([(
            "path".into(),
            r::Value::Path(r::Path::new(vec![a, b], vec![e]).unwrap()),
        )])),
        r::Value::Entity(r::Entity::Node(a)),
        r::Value::Entity(r::Entity::Relationship(e)),
    ]];
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.enable_request_read_view().await.unwrap();
    ctx.row_memory = Some(memory::Budget::new(128 * 1024));
    for all_slot in [None, Some(0), Some(1), Some(2), Some(3)] {
        let demand = ["first", "second", "third", "first"]
            .into_iter()
            .enumerate()
            .map(|(slot, key)| {
                (
                    r::Slot(slot as u32),
                    if all_slot == Some(slot) {
                        r::PropertyDemand::All
                    } else {
                        r::PropertyDemand::Keys([key.into()].into())
                    },
                )
            })
            .collect();
        let before = ctx.row_budget().reads().multi_get_keys;
        let graph = ctx.graph_batch_required(&rows, &demand).await.unwrap();
        assert_eq!(graph.entities.len(), 3);
        assert_eq!(ctx.row_budget().reads().multi_get_keys - before, 4);
        for (entity, all, values, selected) in [
            (
                r::Entity::Node(a),
                matches!(all_slot, Some(0..=2)),
                [1, 2, 3, 4],
                [true, true, true, false],
            ),
            (
                r::Entity::Relationship(e),
                matches!(all_slot, Some(1 | 3)),
                [5, 6, 7, 8],
                [true, true, false, false],
            ),
        ] {
            let properties = graph.properties(entity).unwrap();
            assert_eq!(
                properties.len(),
                if all {
                    4
                } else {
                    selected.into_iter().filter(|selected| *selected).count()
                }
            );
            for (index, key) in ["first", "second", "third", "hidden"]
                .into_iter()
                .enumerate()
            {
                if all || selected[index] {
                    assert_eq!(
                        graph.property(entity, key).unwrap(),
                        &r::Value::Integer(values[index])
                    );
                } else {
                    assert!(!properties.contains_key(key));
                }
            }
        }
        let properties = graph.properties(r::Entity::Node(b)).unwrap();
        assert_eq!(properties.len(), usize::from(all_slot == Some(1)));
        if all_slot == Some(1) {
            assert_eq!(
                graph.property(r::Entity::Node(b), "first").unwrap(),
                &r::Value::Integer(9)
            );
        }
        drop(graph);
        assert_eq!(ctx.row_budget().available(), 128 * 1024);
    }
    ctx.close_request_read_view().unwrap();
    db.close().await.unwrap();
}
