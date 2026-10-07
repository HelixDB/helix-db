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

/// A stored `$label` that is not a nonempty string hydrates as an unlabeled
/// node with its properties intact. A relationship whose record vanished
/// without a type recorded by this request stays unavailable instead of
/// failing the batch.
#[tokio::test]
async fn hydration_leaves_unreadable_labels_unset_and_skips_vanished_relationships() {
    let db = test_support::open_db("unreadable-entity-labels").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    for (id, label) in [
        (7, P::I64(3)),
        (8, P::String(String::new())),
        (9, P::String("N".into())),
    ] {
        db.inner_db()
            .put(
                ctx.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
                    id,
                ))),
                property::encode_properties(&[
                    Property::new("$label", label),
                    Property::i64("k", id as i64),
                ]),
            )
            .await
            .unwrap();
    }
    ctx.row_memory = Some(memory::Budget::new(64 * 1024));
    ctx.enable_request_read_view().await.unwrap();
    let rows = [vec![
        r::Value::Entity(r::Entity::Node(7)),
        r::Value::Entity(r::Entity::Node(8)),
        r::Value::Entity(r::Entity::Node(9)),
        r::Value::Entity(r::Entity::Relationship(11)),
    ]];
    for demand in [
        r::PropertyDemand::All,
        r::PropertyDemand::Keys(["k".into()].into()),
    ] {
        let demand = (0..4).map(|slot| (r::Slot(slot), demand.clone())).collect();
        let graph = ctx.graph_batch_required(&rows, &demand).await.unwrap();
        for (id, label) in [(7, None), (8, None), (9, Some("N"))] {
            assert_eq!(graph.label(r::Entity::Node(id)).unwrap(), label);
            assert_eq!(
                graph.property(r::Entity::Node(id), "k").unwrap(),
                &r::Value::Integer(id as i64)
            );
        }
        assert!(!graph.entities.contains_key(&r::Entity::Relationship(11)));
        assert!(matches!(
            graph.label(r::Entity::Relationship(11)),
            Err(error) if error.detail == "DeletedEntityAccess"
        ));
        drop(graph);
        assert_eq!(ctx.row_budget().available(), 64 * 1024);
    }
    ctx.close_request_read_view().unwrap();
    db.close().await.unwrap();
}

/// Output admission reads every property it will encode. A stored value with
/// no Cypher representation fails the query with its own error, whether the
/// entity is returned directly or inside a list; selected readable properties
/// of the same entity remain available.
#[tokio::test]
async fn returned_entities_with_unreadable_properties_fail_with_the_stored_value_error() {
    let db = test_support::open_db("unreadable-returned-entities").await;
    let ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    db.inner_db()
        .put(
            ctx.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
                7,
            ))),
            property::encode_properties(&[
                Property::string("$label", "N"),
                Property::i64("k", 1),
                Property::new("when", P::DateTime(1)),
            ]),
        )
        .await
        .unwrap();
    drop(ctx);
    let response = db
        .cypher(crate::cypher::Request::new("MATCH (n) RETURN n.k"))
        .await
        .unwrap();
    assert_eq!(response.rows, vec![vec![serde_json::json!(1)]]);
    for text in ["MATCH (n) RETURN n", "MATCH (n) RETURN [n]"] {
        let error = db
            .cypher(crate::cypher::Request::new(text))
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::Query(ref error) if error.detail == "StoredValueType"),
            "{text}: {error}"
        );
    }
    db.close().await.unwrap();
}

/// Wire encoding never relabels an entity: hydrated data of the other kind
/// for its identity is an internal error, not a node or relationship value.
#[test]
fn wire_encoding_rejects_hydrated_data_of_the_other_entity_kind() {
    let mut graph = GraphBatch::default();
    graph.entities.insert(
        r::Entity::Relationship(3),
        EntityData {
            kind: EntityKind::Node {
                label: Some("N".into()),
            },
            properties: BTreeMap::new(),
        },
    );
    graph.entities.insert(
        r::Entity::Node(4),
        EntityData {
            kind: EntityKind::Relationship {
                label: "R".into(),
                endpoints: (1, 2),
            },
            properties: BTreeMap::new(),
        },
    );
    for entity in [r::Entity::Relationship(3), r::Entity::Node(4)] {
        assert!(matches!(
            graph.wire(&r::Value::Entity(entity)),
            Err(Error::Query(error)) if error.detail == "EntityKindMismatch"
        ));
    }
}
