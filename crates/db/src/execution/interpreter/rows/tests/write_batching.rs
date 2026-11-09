//! CREATE and SET cost one read per batch and one change per entity, and store
//! exactly what applying each update alone stores.
use super::super::*;
use crate::encoding::v2::values::property;
use crate::execution::interpreter::test_support;
use bytes::Bytes;
use helix_planner::context;
use rand::{rngs::StdRng, RngExt, SeedableRng};
use serde_json::json;

fn node_rows(ids: &[u64], budget: &memory::Budget) -> memory::Rows {
    memory::Rows::new(
        ids.iter()
            .map(|id| vec![r::Value::Entity(r::Entity::Node(*id))])
            .collect(),
        budget,
    )
    .unwrap()
}

fn set(key: &str, value: r::Expression) -> r::PropertyMutation {
    r::PropertyMutation::Set {
        entity: r::Slot(0),
        key: key.into(),
        value,
    }
}

fn read_key(key: &str) -> r::Expression {
    r::Expression::Property(Box::new(r::Expression::Slot(r::Slot(0))), key.into())
}

/// Every stored row except index catalog records, which carry a random
/// operation ID per database.
async fn all_rows(db: &crate::HelixDB) -> BTreeMap<Bytes, Bytes> {
    let mut scan = db.inner_db().scan(..).await.unwrap();
    let mut rows = BTreeMap::new();
    while let Some(row) = scan.next().await.unwrap() {
        if !row.key.starts_with(&[6, 1]) && !row.key.starts_with(&[6, 2]) {
            rows.insert(row.key, row.value);
        }
    }
    rows
}

#[tokio::test]
async fn set_costs_two_reads_and_one_change_per_entity_for_any_key_or_row_count() {
    for rows in [1_usize, 64] {
        let db = test_support::open_db("set-one-change-per-entity").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.row_memory = Some(memory::Budget::new(16 * 1024 * 1024));
        let mut ids = Vec::new();
        for k in 0..rows {
            let properties = vec![property::Property::i64("k", k as i64)];
            ids.push(ctx.row_create_node("N", properties).await.unwrap());
        }
        ctx.enable_request_write_scope().await.unwrap();
        let updates = [
            set("a", r::Expression::Literal(r::Value::Integer(1))),
            set("b", read_key("k")),
            set("c", r::Expression::Literal(r::Value::Integer(3))),
            r::PropertyMutation::Remove {
                entity: r::Slot(0),
                key: "k".into(),
            },
        ];
        let input = node_rows(&ids, ctx.row_budget());
        let before = ctx.row_budget().reads();
        let output = ctx
            .update_rows(input, &updates, &BTreeMap::new(), Limits::default())
            .await
            .unwrap();
        let after = ctx.row_budget().reads();
        assert_eq!(after.multi_get_batches - before.multi_get_batches, 2);
        assert_eq!(after.multi_get_keys - before.multi_get_keys, 2 * rows);
        assert_eq!(after.point_gets, before.point_gets);
        let active = ctx.active_write_tx().unwrap();
        assert_eq!(active.index_context.transitions, rows);
        drop(output);
        ctx.commit_request_write_scope().await.unwrap();
        drop(ctx);
        let response = db
            .cypher(crate::cypher::Request::new(
                "MATCH (n:N) RETURN collect(properties(n))[0]",
            ))
            .await
            .unwrap();
        assert_eq!(response.rows, vec![vec![json!({"a":1,"b":0,"c":3})]]);
        db.close().await.unwrap();
    }
}

/// Each batch reads once, and a later batch sees what earlier ones wrote.
#[tokio::test]
async fn set_reads_once_per_batch_and_rows_past_a_batch_see_earlier_batches() {
    let db = test_support::open_db("set-batch-boundaries").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(16 * 1024 * 1024));
    let id = ctx
        .row_create_node("N", vec![property::Property::i64("v", 0)])
        .await
        .unwrap();
    ctx.enable_request_write_scope().await.unwrap();
    let increment = set(
        "v",
        r::Expression::Binary(
            r::Binary::Add,
            Box::new(read_key("v")),
            Box::new(r::Expression::Literal(r::Value::Integer(1))),
        ),
    );
    let input = node_rows(&[id; 5], ctx.row_budget());
    let limits = Limits {
        batch_rows: 2,
        ..Limits::default()
    };
    ctx.update_rows(input, &[increment], &BTreeMap::new(), limits)
        .await
        .unwrap();
    ctx.commit_request_write_scope().await.unwrap();
    drop(ctx);
    let response = db
        .cypher(crate::cypher::Request::new("MATCH (n:N) RETURN n.v"))
        .await
        .unwrap();
    assert_eq!(response.rows, vec![vec![json!(5)]]);
    db.close().await.unwrap();
}

/// Each statement reads a value an earlier update of the same clause wrote.
#[tokio::test]
async fn set_expressions_observe_every_earlier_update() {
    for (setup, statement, expected) in [
        (
            "CREATE (:N {a:0})",
            "MATCH (n:N) SET n.a=1, n.b=n.a+1 RETURN properties(n)",
            json!({"a":1,"b":2}),
        ),
        (
            "CREATE (:N {v:0})",
            "UNWIND [1,2,3] AS i MATCH (n:N) SET n.v=n.v+i WITH n RETURN DISTINCT properties(n)",
            json!({"v":6}),
        ),
        (
            "CREATE (:N {v:1})",
            "MATCH (a:N),(b:N) SET a.v=a.v+1, b.w=a.v+1 RETURN properties(a)",
            json!({"v":2,"w":3}),
        ),
        (
            "CREATE (:N {x:1})",
            "MATCH (n:N) SET n.x=9, n=n, n.y=n.x RETURN properties(n)",
            json!({"x":9,"y":9}),
        ),
        (
            "CREATE (:N {x:1,y:2})",
            "MATCH (n:N) SET n={z:3}, n.w=n.z+1 RETURN properties(n)",
            json!({"z":3,"w":4}),
        ),
        (
            "CREATE (:N {x:1,y:2})",
            "MATCH (n:N) SET n+={y:null}, n.y=n.x+1 RETURN properties(n)",
            json!({"x":1,"y":2}),
        ),
    ] {
        let db = test_support::open_db("set-sequential-visibility").await;
        db.cypher(crate::cypher::Request::new(setup)).await.unwrap();
        let response = db
            .cypher(crate::cypher::Request::new(statement))
            .await
            .unwrap();
        assert_eq!(response.rows, vec![vec![expected]], "{statement}");
        db.close().await.unwrap();
    }
}

/// A unique index claims values in transition order, so each statement here
/// only succeeds if its releases stay ahead of its claims.
#[tokio::test]
async fn set_keeps_statement_order_for_unique_claims() {
    for (statement, succeeds) in [
        (
            "MATCH (n:U) WITH n ORDER BY n.code DESC SET n.code=n.code+1",
            true,
        ),
        (
            "MATCH (n:U) WITH n ORDER BY n.code ASC SET n.code=n.code+1",
            false,
        ),
        (
            "MATCH (a:U {code:1}),(b:U {code:2}) SET a.other=1, b.code=9, a.code=2",
            true,
        ),
        (
            "MATCH (a:U {code:1}),(b:U {code:2}) SET b.code=9, a.code=2, b.code=1",
            true,
        ),
    ] {
        let config = test_support::in_memory_config("set-unique-order")
            .with_unique_equality_index("U", "code");
        let db = test_support::open_db_with_config(config).await;
        db.cypher(crate::cypher::Request::new(
            "CREATE (:U {code:1}), (:U {code:2})",
        ))
        .await
        .unwrap();
        let result = db.cypher(crate::cypher::Request::new(statement)).await;
        assert_eq!(result.is_ok(), succeeds, "{statement}: {result:?}");
        db.close().await.unwrap();
    }
}

/// Pending edits hold twice each value, so a statement that fits as stored
/// rows must apply them rather than run out of memory holding them.
#[tokio::test]
async fn pending_edits_yield_to_the_memory_budget() {
    let db = test_support::open_db("set-pending-memory").await;
    db.cypher(crate::cypher::Request::new(
        "UNWIND range(1,16) AS k CREATE (:N {k:k})",
    ))
    .await
    .unwrap();
    let mut request = crate::cypher::Request::new("MATCH (n:N) SET n.v=$payload");
    request.parameters.insert(
        "payload".into(),
        helix_ast::query::QueryValue::String("x".repeat(96 * 1024)),
    );
    crate::cypher::execute(
        &db,
        request,
        crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
        crate::query_service::QueryMode::Execute,
        crate::execution_control::ExecutionControl::unlimited(),
        Limits {
            memory_bytes: 3 * 1024 * 1024,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    db.close().await.unwrap();
}

/// Updates on large rows hold one chunk of old rows at a time, so they fit
/// the budget applying each row alone needs. 64 rows of 256 KiB are 16 MiB.
#[tokio::test]
async fn large_row_updates_fit_the_budget_one_row_at_a_time_needs() {
    for statement in [
        "MATCH (n:Doc) REMOVE n.payload",
        "MATCH (n:Doc) SET n.flag=1",
    ] {
        let db = test_support::open_db("set-large-rows").await;
        let mut seed = ExecutionContext::new(&db, context::ParamBindings::default());
        seed.row_memory = Some(memory::Budget::new(1 << 32));
        seed.enable_request_write_scope().await.unwrap();
        for k in 0..64 {
            let properties = vec![
                property::Property::bytes("payload", vec![7; 256 * 1024]),
                property::Property::i64("k", k),
            ];
            seed.row_create_node("Doc", properties).await.unwrap();
        }
        seed.commit_request_write_scope().await.unwrap();
        drop(seed);
        crate::cypher::execute(
            &db,
            crate::cypher::Request::new(statement),
            crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
            crate::query_service::QueryMode::Execute,
            crate::execution_control::ExecutionControl::unlimited(),
            Limits {
                memory_bytes: 24 * 1024 * 1024,
                ..Limits::default()
            },
        )
        .await
        .unwrap_or_else(|error| panic!("{statement}: {error:?}"));
        db.close().await.unwrap();
    }
}

/// A folded edit is checked once, against its final row. With a tenant
/// partitioned text index, text set alone is still refused, while text set
/// with its tenant in one clause is accepted in either order.
#[tokio::test]
async fn folded_edits_check_indexes_against_the_final_row() {
    for (statement, succeeds) in [
        ("MATCH (n:Doc) SET n.body='hello'", false),
        ("MATCH (n:Doc) SET n.body='hello', n.tenant='acme'", true),
        ("MATCH (n:Doc) SET n.tenant='acme', n.body='hello'", true),
    ] {
        let config = test_support::in_memory_config("set-tenant-text")
            .with_tenant_text_index("Doc", "body", "tenant");
        let db = test_support::open_db_with_config(config).await;
        db.cypher(crate::cypher::Request::new("CREATE (:Doc {k:1})"))
            .await
            .unwrap();
        let result = db.cypher(crate::cypher::Request::new(statement)).await;
        assert_eq!(result.is_ok(), succeeds, "{statement}: {result:?}");
        db.close().await.unwrap();
    }
}

/// `a` and `b` swap through the row, so each read must see the prior write.
#[tokio::test]
async fn set_map_source_applies_pending_edits_to_that_source_first() {
    let db = test_support::open_db("set-map-source-barrier").await;
    db.cypher(crate::cypher::Request::new(
        "CREATE (:A {x:1}), (:T {old:1})",
    ))
    .await
    .unwrap();
    let response = db
        .cypher(crate::cypher::Request::new(
            "MATCH (a:A),(t:T) SET a.x=7, t=a RETURN properties(t)",
        ))
        .await
        .unwrap();
    assert_eq!(response.rows, vec![vec![json!({"x":7})]]);
    db.close().await.unwrap();
}

/// A removal may hit an entity a prior clause deleted, while a value needs a
/// row. Folding a removal with a value keeps the stricter contract.
#[tokio::test]
async fn folded_edits_on_deleted_entities_keep_their_contract() {
    for (statement, succeeds) in [
        ("MATCH (n:N) DELETE n REMOVE n.a", true),
        ("MATCH (n:N) DELETE n SET n.a=null", true),
        ("MATCH (n:N) DELETE n REMOVE n.a SET n.b=1", false),
        ("MATCH (n:N) DELETE n SET n.b=1 REMOVE n.a", false),
        ("MATCH (n:N) DELETE n SET n+={a:null}", false),
        ("MATCH (n:N) DELETE n SET n={}", false),
    ] {
        let db = test_support::open_db("set-deleted-entities").await;
        db.cypher(crate::cypher::Request::new("CREATE (:N {a:1})"))
            .await
            .unwrap();
        let result = db.cypher(crate::cypher::Request::new(statement)).await;
        assert_eq!(result.is_ok(), succeeds, "{statement}: {result:?}");
        let remaining = db
            .cypher(crate::cypher::Request::new("MATCH (n:N) RETURN count(n)"))
            .await
            .unwrap();
        assert_eq!(
            remaining.rows,
            vec![vec![json!(i64::from(!succeeds))]],
            "{statement}"
        );
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn set_rejects_internal_and_empty_names_before_writing() {
    for (key, detail) in [
        ("$label", "reserved_property_name"),
        ("", "empty_property_name"),
    ] {
        let db = test_support::open_db("set-invalid-names").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.row_memory = Some(memory::Budget::new(1024 * 1024));
        let id = ctx.row_create_node("N", vec![]).await.unwrap();
        let before = all_rows(&db).await;
        let input = node_rows(&[id], ctx.row_budget());
        let updates = [set(key, r::Expression::Literal(r::Value::Integer(1)))];
        let error = match ctx
            .update_rows(input, &updates, &BTreeMap::new(), Limits::default())
            .await
        {
            Err(Error::Query(error)) => error,
            Err(error) => panic!("unexpected failure: {error:?}"),
            Ok(_) => panic!("the name was accepted"),
        };
        assert_eq!(error.detail, detail);
        assert_eq!(all_rows(&db).await, before);
        drop(ctx);
        db.close().await.unwrap();
    }
}

/// One update from a closed set of shapes, over the three row slots.
fn random_update(rng: &mut StdRng) -> r::PropertyMutation {
    let entity = r::Slot(rng.random_range(0..3));
    let key = |rng: &mut StdRng| ["a", "b", "c", "d"][rng.random_range(0..4)].to_owned();
    let value = |rng: &mut StdRng| match rng.random_range(0..7) {
        0 => r::Expression::Literal(r::Value::Null),
        1 => r::Expression::Property(
            Box::new(r::Expression::Slot(r::Slot(rng.random_range(0..3)))),
            key(rng),
        ),
        // An equality index cannot hold a list, and storage has no mixed one.
        2 => r::Expression::Literal(r::Value::List(vec![r::Value::Integer(1)])),
        3 => r::Expression::Literal(r::Value::List(vec![
            r::Value::Integer(1),
            r::Value::Boolean(true),
        ])),
        _ => r::Expression::Literal(r::Value::Integer(rng.random_range(0..4))),
    };
    let map = |rng: &mut StdRng| {
        r::Expression::Map(
            (0..rng.random_range(0..3))
                .map(|_| (key(rng), value(rng)))
                .collect(),
        )
    };
    match rng.random_range(0..6) {
        0 => r::PropertyMutation::Remove {
            entity,
            key: key(rng),
        },
        1 => r::PropertyMutation::Replace {
            entity,
            properties: map(rng),
        },
        2 => r::PropertyMutation::Extend {
            entity,
            properties: map(rng),
        },
        3 => r::PropertyMutation::Extend {
            entity,
            properties: r::Expression::Slot(r::Slot(rng.random_range(0..3))),
        },
        _ => r::PropertyMutation::Set {
            entity,
            key: key(rng),
            value: value(rng),
        },
    }
}

/// Opens one identical graph: two nodes and a relationship joining them, with
/// equality and unique indexes, and returns the entities.
async fn differential_graph(name: &str) -> (crate::HelixDB, [r::Entity; 3]) {
    let config = test_support::in_memory_config(name)
        .with_equality_index("N", "a")
        .with_unique_equality_index("N", "b")
        .with_edge_equality_index("R", "a");
    let db = test_support::open_db_with_config(config).await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let first = ctx
        .row_create_node(
            "N",
            vec![
                property::Property::i64("a", 1),
                property::Property::i64("b", 2),
            ],
        )
        .await
        .unwrap();
    let second = ctx
        .row_create_node("N", vec![property::Property::i64("c", 3)])
        .await
        .unwrap();
    let edge = ctx
        .row_create_edge(first, second, "R", vec![property::Property::i64("d", 4)])
        .await
        .unwrap();
    drop(ctx);
    (
        db,
        [
            r::Entity::Node(first),
            r::Entity::Node(second),
            r::Entity::Relationship(edge),
        ],
    )
}

const DIFFERENTIAL_SEEDS: u64 = 32;

/// Applying every update alone is the sequential contract. Folding must match
/// it in outcome and in every stored byte, so no row format question arises.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn folded_set_stores_the_same_bytes_as_applying_each_update_alone() {
    let seeds = (0..DIFFERENTIAL_SEEDS)
        .map(|seed| tokio::spawn(set_differential(seed)))
        .collect::<Vec<_>>();
    for seed in seeds {
        seed.await.unwrap();
    }
}

async fn set_differential(seed: u64) {
    let mut rng = StdRng::seed_from_u64(seed);
    let updates = (0..rng.random_range(1..7))
        .map(|_| random_update(&mut rng))
        .collect::<Vec<_>>();
    let (folded_db, entities) = differential_graph(&format!("set-folded-{seed}")).await;
    let (alone_db, _) = differential_graph(&format!("set-alone-{seed}")).await;
    let rows = (0..rng.random_range(1..4))
        .map(|_| {
            (0..3)
                .map(|_| r::Value::Entity(entities[rng.random_range(0..3)]))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut folded_ctx = ExecutionContext::new(&folded_db, context::ParamBindings::default());
    folded_ctx.row_memory = Some(memory::Budget::new(16 * 1024 * 1024));
    folded_ctx.enable_request_write_scope().await.unwrap();
    let folded = folded_ctx
        .update_rows(
            memory::Rows::new(rows.clone(), folded_ctx.row_budget()).unwrap(),
            &updates,
            &BTreeMap::new(),
            Limits::default(),
        )
        .await
        .map(drop);
    let mut alone_ctx = ExecutionContext::new(&alone_db, context::ParamBindings::default());
    alone_ctx.row_memory = Some(memory::Budget::new(16 * 1024 * 1024));
    alone_ctx.enable_request_write_scope().await.unwrap();
    let mut alone = Ok(());
    'updates: for row in &rows {
        for update in &updates {
            let input = memory::Rows::new(vec![row.clone()], alone_ctx.row_budget()).unwrap();
            alone = alone_ctx
                .update_rows(
                    input,
                    std::slice::from_ref(update),
                    &BTreeMap::new(),
                    Limits::default(),
                )
                .await
                .map(drop);
            if alone.is_err() {
                break 'updates;
            }
        }
    }
    let folded = match folded {
        Ok(()) => folded_ctx.commit_request_write_scope().await.map_err(drop),
        Err(_) => {
            folded_ctx.abort_request_write_scope();
            Err(())
        }
    };
    let alone = match alone {
        Ok(()) => alone_ctx.commit_request_write_scope().await.map_err(drop),
        Err(_) => {
            alone_ctx.abort_request_write_scope();
            Err(())
        }
    };
    assert_eq!(folded, alone, "seed {seed}: {updates:?}");
    drop(folded_ctx);
    drop(alone_ctx);
    let (folded_rows, alone_rows) = (all_rows(&folded_db).await, all_rows(&alone_db).await);
    let differing = folded_rows
        .keys()
        .chain(alone_rows.keys())
        .filter(|key| folded_rows.get(*key) != alone_rows.get(*key))
        .collect::<std::collections::BTreeSet<_>>();
    assert!(
        differing.is_empty(),
        "seed {seed}: {updates:?}\n{:#?}",
        differing
            .iter()
            .map(|key| (key, folded_rows.get(*key), alone_rows.get(*key)))
            .collect::<Vec<_>>()
    );
    folded_db.close().await.unwrap();
    alone_db.close().await.unwrap();
}

#[tokio::test]
async fn create_checks_bound_endpoints_with_one_read_for_any_row_count() {
    for rows in [1_usize, 64] {
        let db = test_support::open_db("create-one-endpoint-read").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.row_memory = Some(memory::Budget::new(16 * 1024 * 1024));
        let source = ctx.row_create_node("N", vec![]).await.unwrap();
        ctx.enable_request_write_scope().await.unwrap();
        let pattern = r::Pattern {
            nodes: vec![r::NodePattern {
                slot: r::Slot(1),
                label: Some("Created".into()),
                properties: vec![],
            }],
            relationships: vec![
                r::RelationshipPattern {
                    slot: r::Slot(2),
                    from: r::Slot(0),
                    to: r::Slot(1),
                    direction: r::Direction::Outgoing,
                    types: vec!["R".into()],
                    properties: vec![],
                },
                r::RelationshipPattern {
                    slot: r::Slot(3),
                    from: r::Slot(1),
                    to: r::Slot(0),
                    direction: r::Direction::Outgoing,
                    types: vec!["S".into()],
                    properties: vec![],
                },
            ],
            paths: vec![],
        };
        let input = memory::Rows::new(
            vec![
                vec![
                    r::Value::Entity(r::Entity::Node(source)),
                    r::Value::Null,
                    r::Value::Null,
                    r::Value::Null,
                ];
                rows
            ],
            ctx.row_budget(),
        )
        .unwrap();
        let before = ctx.row_budget().reads();
        drop(
            ctx.create_rows(input, &pattern, &BTreeMap::new(), Limits::default())
                .await
                .unwrap(),
        );
        let after = ctx.row_budget().reads();
        assert_eq!(after.multi_get_batches - before.multi_get_batches, 1);
        assert_eq!(after.multi_get_keys - before.multi_get_keys, 1);
        ctx.commit_request_write_scope().await.unwrap();
        drop(ctx);
        let response = db
            .cypher(crate::cypher::Request::new(
                "MATCH (:N)-[r]->() RETURN count(r)",
            ))
            .await
            .unwrap();
        assert_eq!(response.rows, vec![vec![json!(rows as i64)]]);
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn create_checks_endpoints_once_per_batch() {
    let db = test_support::open_db("create-endpoint-batches").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(16 * 1024 * 1024));
    let source = ctx.row_create_node("N", vec![]).await.unwrap();
    ctx.enable_request_write_scope().await.unwrap();
    let pattern = r::Pattern {
        nodes: vec![],
        relationships: vec![r::RelationshipPattern {
            slot: r::Slot(1),
            from: r::Slot(0),
            to: r::Slot(0),
            direction: r::Direction::Outgoing,
            types: vec!["R".into()],
            properties: vec![],
        }],
        paths: vec![],
    };
    let input = memory::Rows::new(
        vec![vec![r::Value::Entity(r::Entity::Node(source)), r::Value::Null]; 5],
        ctx.row_budget(),
    )
    .unwrap();
    let before = ctx.row_budget().reads();
    let limits = Limits {
        batch_rows: 2,
        ..Limits::default()
    };
    drop(
        ctx.create_rows(input, &pattern, &BTreeMap::new(), limits)
            .await
            .unwrap(),
    );
    let after = ctx.row_budget().reads();
    assert_eq!(after.multi_get_batches - before.multi_get_batches, 3);
    ctx.abort_request_write_scope();
    drop(ctx);
    db.close().await.unwrap();
}

/// A missing endpoint in any row fails the clause and commits none of it.
#[tokio::test]
async fn create_rejects_a_missing_endpoint_in_any_row_and_commits_nothing() {
    for missing_row in [0_usize, 1, 2] {
        let db = test_support::open_db("create-missing-endpoint").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.row_memory = Some(memory::Budget::new(16 * 1024 * 1024));
        let present = ctx.row_create_node("N", vec![]).await.unwrap();
        ctx.enable_request_write_scope().await.unwrap();
        let pattern = r::Pattern {
            nodes: vec![],
            relationships: vec![r::RelationshipPattern {
                slot: r::Slot(2),
                from: r::Slot(0),
                to: r::Slot(1),
                direction: r::Direction::Outgoing,
                types: vec!["R".into()],
                properties: vec![],
            }],
            paths: vec![],
        };
        let rows = (0..3)
            .map(|row| {
                let target = if row == missing_row {
                    u64::MAX
                } else {
                    present
                };
                vec![
                    r::Value::Entity(r::Entity::Node(present)),
                    r::Value::Entity(r::Entity::Node(target)),
                    r::Value::Null,
                ]
            })
            .collect::<Vec<_>>();
        let input = memory::Rows::new(rows, ctx.row_budget()).unwrap();
        let error = ctx
            .create_rows(input, &pattern, &BTreeMap::new(), Limits::default())
            .await
            .err()
            .expect("the endpoint is missing");
        assert!(
            matches!(
                error,
                Error::Storage(crate::HelixDbError::NodeNotFound(u64::MAX))
            ),
            "{error:?}"
        );
        ctx.abort_request_write_scope();
        drop(ctx);
        let response = db
            .cypher(crate::cypher::Request::new(
                "MATCH ()-[r]->() RETURN count(r)",
            ))
            .await
            .unwrap();
        assert_eq!(response.rows, vec![vec![json!(0)]]);
        db.close().await.unwrap();
    }
}

/// An endpoint deleted earlier in the statement is absent from the
/// transaction's view, though the row still names it.
#[tokio::test]
async fn create_rejects_an_endpoint_the_statement_deleted() {
    let db = test_support::open_db("create-deleted-endpoint").await;
    db.cypher(crate::cypher::Request::new("CREATE (:N)"))
        .await
        .unwrap();
    let error = db
        .cypher(crate::cypher::Request::new(
            "MATCH (n:N) DELETE n CREATE (n)-[:R]->(:M)",
        ))
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::Storage(crate::HelixDbError::NodeNotFound(_))),
        "{error:?}"
    );
    let response = db
        .cypher(crate::cypher::Request::new("MATCH (n) RETURN count(n)"))
        .await
        .unwrap();
    assert_eq!(response.rows, vec![vec![json!(1)]]);
    db.close().await.unwrap();
}

/// Rows stage in order, so a later expression reads an earlier row's entities.
#[tokio::test]
async fn create_expressions_observe_entities_earlier_rows_staged() {
    let db = test_support::open_db("create-staged-order").await;
    let response = db
        .cypher(crate::cypher::Request::new(
            "UNWIND [1,2] AS i CREATE (a:N {k:i})-[r:R {k:i+1}]->(b:N {k:a.k+1}), (b)-[:S {k:r.k+b.k}]->(a) RETURN a.k,b.k ORDER BY a.k",
        ))
        .await
        .unwrap();
    assert_eq!(
        response.rows,
        vec![vec![json!(1), json!(2)], vec![json!(2), json!(3)]]
    );
    let response = db
        .cypher(crate::cypher::Request::new(
            "MATCH ()-[s:S]->() RETURN s.k ORDER BY s.k",
        ))
        .await
        .unwrap();
    assert_eq!(response.rows, vec![vec![json!(4)], vec![json!(6)]]);
    db.close().await.unwrap();
}
