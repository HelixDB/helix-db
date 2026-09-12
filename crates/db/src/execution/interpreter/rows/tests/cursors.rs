use super::super::*;
use crate::encoding::v2::{
    keys,
    values::{edge_endpoints::EdgeEndpointsValue, property},
};
use futures::TryStreamExt;
use helix_planner::{context, exec, ir};

#[tokio::test]
async fn expansion_batches_combine_parents_and_retain_partial_cursor_ownership() {
    let db = crate::execution::interpreter::test_support::open_db("expansion-parent-batches").await;
    let fixture = db.cypher(crate::cypher::Request::new(
        "UNWIND range(0,31) AS i CREATE (a:Parent {key:i})-[r:R]->(b:Child {key:i}) RETURN a,r,b",
    )).await.unwrap();
    let decode = |rows: Vec<Vec<serde_json::Value>>| {
        rows.into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|entity| entity["id"].as_str().unwrap().parse::<u64>().unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    };
    let mut graph = decode(fixture.rows);
    let source_ids = graph.iter().map(|triple| triple[0]).collect::<Vec<_>>();
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
    let step = r::MatchStep::Expand {
        relationship: 0,
        from: r::Slot(0),
        to: r::Slot(1),
        reverse: false,
    };
    let limits = Limits {
        batch_rows: 8,
        memory_bytes: 64 * 1024,
        ..Default::default()
    };
    for stop_early in [false, true] {
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.row_memory = Some(memory::Budget::new(limits.memory_bytes));
        ctx.enable_request_read_view().await.unwrap();
        let data = source_ids
            .iter()
            .enumerate()
            .map(|(index, id)| {
                vec![
                    r::Value::Entity(r::Entity::Node(*id)),
                    r::Value::Null,
                    r::Value::Null,
                    r::Value::Integer(index as i64),
                ]
            })
            .collect();
        let rows = memory::Rows::new(data, ctx.row_budget()).unwrap();
        let mut batches = Box::pin(ctx.expansion_batches(rows, &pattern, &step, limits));
        let mut count = 0;
        while let Some(batch) = batches.try_next().await.unwrap() {
            count += batch.len();
            for row in &batch {
                let r::Value::Integer(index) = row[3] else {
                    panic!("parent ordinal")
                };
                assert_eq!(
                    row[0],
                    r::Value::Entity(r::Entity::Node(graph[index as usize][0]))
                );
                assert_eq!(
                    row[1],
                    r::Value::Entity(r::Entity::Node(graph[index as usize][2]))
                );
                assert_eq!(
                    row[2],
                    r::Value::Entity(r::Entity::Relationship(graph[index as usize][1]))
                );
            }
            if stop_early {
                break;
            }
        }
        assert_eq!(count, if stop_early { 8 } else { 32 });
        let reads = ctx.row_budget().reads();
        // Each parent needs a neighbor and pair lookup. Property/endpoint
        // hydration is one pair of calls per eight parents, not per parent.
        assert_eq!(reads.point_gets, count);
        assert_eq!(reads.multi_get_batches, count + 2 * count / 8);
        assert_eq!(reads.multi_get_keys, 3 * count);
        drop(batches);
        assert_eq!(ctx.row_budget().reads(), reads);
        assert_eq!(ctx.row_budget().available(), limits.memory_bytes);
        ctx.close_request_read_view().unwrap();
    }
    // A suspended level needs only its active parent after yielding a full
    // batch. Large unrelated values expose accidental retention of completed
    // parents independently of the output row count and topology oracle.
    {
        let payload_bytes = 16 * 1024;
        let memory_bytes = 512 * 1024;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.row_memory = Some(memory::Budget::new(memory_bytes));
        ctx.enable_request_read_view().await.unwrap();
        let data = source_ids[..8]
            .iter()
            .map(|id| {
                vec![
                    r::Value::Entity(r::Entity::Node(*id)),
                    r::Value::Null,
                    r::Value::Null,
                    r::Value::String("p".repeat(payload_bytes)),
                ]
            })
            .collect();
        let rows = memory::Rows::new(data, ctx.row_budget()).unwrap();
        let mut batches = Box::pin(ctx.expansion_batches(
            rows,
            &pattern,
            &step,
            Limits {
                batch_rows: 8,
                memory_bytes,
                ..Default::default()
            },
        ));
        let batch = batches.try_next().await.unwrap().unwrap();
        assert_eq!(batch.len(), 8);
        for (row, triple) in batch.iter().zip(&graph) {
            assert_eq!(row[0], r::Value::Entity(r::Entity::Node(triple[0])));
            assert_eq!(row[1], r::Value::Entity(r::Entity::Node(triple[2])));
            assert_eq!(row[2], r::Value::Entity(r::Entity::Relationship(triple[1])));
            let r::Value::String(payload) = &row[3] else {
                panic!("preserved parent payload")
            };
            assert_eq!(payload.len(), payload_bytes);
            assert!(payload.bytes().all(|byte| byte == b'p'));
        }
        drop(batch);
        assert!(
            memory_bytes - ctx.row_budget().available() < 2 * payload_bytes,
            "completed parents must release their payload before the next poll"
        );
        assert!(batches.try_next().await.unwrap().is_none());
        drop(batches);
        assert_eq!(ctx.row_budget().available(), memory_bytes);
        ctx.close_request_read_view().unwrap();
    }
    graph.extend(decode(db.cypher(crate::cypher::Request::new(
        "MATCH (a:Parent {key:1}) UNWIND range(1,11) AS i CREATE (a)-[r:R]->(b:Extra {key:i}) RETURN a,r,b",
    )).await.unwrap().rows));
    let parents = [
        Some(source_ids[0]),
        Some(source_ids[1]),
        None,
        Some(source_ids[1]),
        Some(source_ids[2]),
    ];
    let mut expected = parents
        .iter()
        .enumerate()
        .flat_map(|(index, parent)| {
            graph
                .iter()
                .filter(move |triple| Some(triple[0]) == *parent)
                .map(move |triple| (index as i64, triple[0], triple[1], triple[2]))
        })
        .collect::<Vec<_>>();
    expected.sort_unstable();
    for batch_rows in [1, 3, 8, 17] {
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.row_memory = Some(memory::Budget::new(limits.memory_bytes));
        ctx.enable_request_read_view().await.unwrap();
        let data = parents
            .iter()
            .enumerate()
            .map(|(index, parent)| {
                vec![
                    parent.map_or(r::Value::Null, |id| r::Value::Entity(r::Entity::Node(id))),
                    r::Value::Null,
                    r::Value::Null,
                    r::Value::Integer(index as i64),
                ]
            })
            .collect();
        let rows = memory::Rows::new(data, ctx.row_budget()).unwrap();
        let mut batches = Box::pin(ctx.expansion_batches(
            rows,
            &pattern,
            &step,
            Limits {
                batch_rows,
                ..limits
            },
        ));
        let mut actual = Vec::new();
        while let Some(batch) = batches.try_next().await.unwrap() {
            assert!(batch.len() <= batch_rows);
            for row in &batch {
                let [r::Value::Entity(r::Entity::Node(from)), r::Value::Entity(r::Entity::Node(to)), r::Value::Entity(r::Entity::Relationship(edge)), r::Value::Integer(index)] =
                    row.as_slice()
                else {
                    panic!("expanded row")
                };
                actual.push((*index, *from, *edge, *to));
            }
        }
        actual.sort_unstable();
        assert_eq!(actual, expected);
        drop(batches);
        assert_eq!(ctx.row_budget().available(), limits.memory_bytes);
        ctx.close_request_read_view().unwrap();
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn exact_pair_candidates_must_match_stored_endpoints_and_direction() {
    use crate::execution::interpreter::test_support;
    let db = test_support::open_db("cypher-stale-pair-candidates").await;
    let a = test_support::add_user(&db, "a").await;
    let b = test_support::add_user(&db, "b").await;
    let c = test_support::add_user(&db, "c").await;
    let unrelated = test_support::add_edge(&db, b, c, "R").await;
    let ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    // A stale candidate must not turn the real b->c relationship into a->b
    // or reverse its direction. The graph oracle contains only b->c.
    for (from, to) in [(a, b), (c, b)] {
        db.inner_db()
            .put(
                ctx.storage_key(keys::DataKeyKind::EdgePairIndex(
                    keys::EdgePairIndexKey::new(from, to),
                )),
                crate::encoding::v2::values::indexes::equality::SecondaryEqualityValue::encode_ids(
                    &[unrelated].into_iter().collect(),
                ),
            )
            .await
            .unwrap();
    }
    drop(ctx);
    for (left, right, pattern) in [
        ("a", "b", "(a)-[r:R]->(b)"),
        ("a", "b", "(a)-[r:R]-(b)"),
        ("c", "b", "(a)-[r:R]->(b)"),
    ] {
        let query = format!("MATCH (a:User {{name:'{left}'}}) MATCH (b:User {{name:'{right}'}}) MATCH {pattern} RETURN r");
        let result = db
            .cypher(crate::cypher::Request::new(&query))
            .await
            .unwrap();
        assert!(result.rows.is_empty(), "{query}: {:?}", result.rows);
    }
    let result = db.cypher(crate::cypher::Request::new("MATCH (a:User {name:'b'}) MATCH (b:User {name:'c'}) MATCH (a)-[r:R]->(b) RETURN count(*)")).await.unwrap();
    assert_eq!(result.rows, vec![vec![serde_json::json!(1)]]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn typed_expansion_reads_local_indexes_and_checks_parallel_relationship_types_in_batches() {
    use crate::execution::interpreter::test_support;
    let db = test_support::open_db("cypher-local-typed-expansion").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let follows = test_support::add_edge(&db, alice, bob, "FOLLOWS").await;
    let other = test_support::add_edge(&db, alice, bob, "OTHER").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(1024 * 1024));
    // Fault injection makes any attempt to decode the database-wide bitmap
    // visible, independent of caches or elapsed-time benchmark noise.
    let key = keys::DataKey::Data {
        scope: ctx.tenant_scope,
        kind: keys::DataKeyKind::PropertyIndex(
            crate::encoding::indexes::PropertyIndexKey::EdgeLabel(
                crate::encoding::indexes::label::EdgeLabelKey::new(
                    crate::encoding::indexes::hash_property_value("FOLLOWS"),
                ),
            ),
        ),
    }
    .to_bytes();
    db.inner_db()
        .put(
            key,
            bytes::Bytes::from_static(b"global bitmap must not be read"),
        )
        .await
        .unwrap();
    ctx.enable_request_read_view().await.unwrap();
    let candidates = ctx
        .expand_edge_candidate_ids(
            alice,
            ir::ExpandDirection::Out,
            &ir::ExpandLabelPlan::Label(ir::NonEmptyString::new("FOLLOWS").unwrap()),
            None,
            512,
        )
        .await
        .unwrap();
    assert_eq!(candidates.iter().collect::<Vec<_>>(), vec![follows, other]);
    assert_eq!(ctx.row_budget().reads().point_gets, 1); // neighborhood
    assert_eq!(ctx.row_budget().reads().multi_get_batches, 1); // edge pair
    let _batch = ctx.row_budget().reserve(2 * 128).unwrap();
    assert_eq!(
        ctx.relationship_types_batch(&[follows, other], &["FOLLOWS".into()])
            .await
            .unwrap(),
        vec![true, false]
    );
    assert_eq!(ctx.row_budget().reads().multi_get_batches, 2);
    assert_eq!(ctx.row_budget().reads().multi_get_keys, 3);
    assert_eq!(
        ctx.relationship_types_batch(&[follows, other], &[])
            .await
            .unwrap(),
        vec![true, true]
    );
    assert_eq!(
        ctx.relationship_types_batch(&[u64::MAX], &["FOLLOWS".into()])
            .await
            .unwrap(),
        vec![false]
    );
    drop(_batch);
    drop(candidates);
    assert_eq!(ctx.row_budget().available(), 1024 * 1024);
    ctx.close_request_read_view().unwrap();
    let result = db
        .cypher(crate::cypher::Request::new(
            "MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN id(r) AS id",
        ))
        .await
        .unwrap();
    assert_eq!(result.rows, vec![vec![serde_json::json!(follows)]]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn node_cursors_skip_missing_legacy_owners_and_release_budget_on_exhaustion() {
    let db =
        crate::execution::interpreter::test_support::open_db("cypher-stale-cursor-owners").await;
    let id = 7;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(1024 * 1024));
    db.inner_db()
        .put(
            ctx.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
                id,
            ))),
            property::encode_properties(&[property::Property::string("$label", "N")]),
        )
        .await
        .unwrap();
    ctx.enable_request_read_view().await.unwrap();
    for batch_rows in [1, 2, 8] {
        let owners = roaring::RoaringTreemap::from_iter([0, 1, id, u64::MAX]);
        let owners =
            crate::query_resources::bitmap::Bitmap::retain_legacy(owners, Some(ctx.row_budget()))
                .unwrap();
        let cursor = scan::NodeCursor::Indexed {
            ids: owners.into_iter(),
            verify_existence: true,
        };
        let batches = ctx
            .node_id_batches(
                cursor,
                1,
                r::Slot(0),
                Limits {
                    batch_rows,
                    ..Default::default()
                },
            )
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            batches
                .iter()
                .flat_map(|batch| batch.iter())
                .cloned()
                .collect::<Vec<_>>(),
            vec![vec![r::Value::Entity(r::Entity::Node(id))]]
        );
        drop(batches);
        assert_eq!(ctx.row_budget().available(), 1024 * 1024);
    }
    ctx.fail_deadline_after(0);
    let result = ctx
        .node_cursor(&exec::ExecOp::Access {
            plan: Box::new(exec::ExecAccessPlan::Node(
                exec::ExecNodeAccessPlan::AllScan,
            )),
        })
        .await;
    assert!(matches!(
        result,
        Err(Error::Storage(crate::HelixDbError::QueryDeadlineExceeded))
    ));
    ctx.close_request_read_view().unwrap();
    drop(ctx);
    db.close().await.unwrap();
}

#[tokio::test]
async fn node_cursors_reject_foreign_tenants_and_unadmitted_raw_rows() {
    use keys::scope::{DataScope, TenantId};
    let db =
        crate::execution::interpreter::test_support::open_db("cypher-cursor-tenant-budget").await;
    let scope = DataScope::Tenant(TenantId::from_u128(1));
    let foreign = DataScope::Tenant(TenantId::from_u128(2));
    let mut ctx = ExecutionContext::new_scoped(&db, context::ParamBindings::default(), scope);
    ctx.row_memory = Some(memory::Budget::new(1024 * 1024));
    let foreign_context =
        ExecutionContext::new_scoped(&db, context::ParamBindings::default(), foreign);
    for context in [&ctx, &foreign_context] {
        db.inner_db()
            .put(
                context.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
                    7,
                ))),
                property::encode_properties(&[
                    property::Property::string("$label", "N"),
                    property::Property::string("payload", "x".repeat(16 * 1024)),
                ]),
            )
            .await
            .unwrap();
    }
    let foreign_prefix = keys::DataKey::data_prefix(
        foreign,
        bytes::Bytes::from(vec![keys::KeyPrefix::NodeProperty.as_u8()]),
    );
    // The cursor API cannot encode its originating namespace in its type; the
    // runtime guard must reject a valid physical row from another tenant.
    let cursor =
        scan::NodeCursor::Scan(db.inner_db().scan_prefix(foreign_prefix, ..).await.unwrap());
    let result = ctx
        .node_id_batches(
            cursor,
            1,
            r::Slot(0),
            Limits {
                batch_rows: 1,
                ..Default::default()
            },
        )
        .try_collect::<Vec<_>>()
        .await;
    assert!(
        matches!(result,Err(Error::Storage(crate::HelixDbError::InvariantViolation(message))) if message.contains("tenant"))
    );
    assert_eq!(ctx.row_budget().available(), 1024 * 1024);

    ctx.enable_request_read_view().await.unwrap();
    ctx.row_memory = Some(memory::Budget::new(64));
    let operation = exec::ExecOp::Access {
        plan: Box::new(exec::ExecAccessPlan::Node(
            exec::ExecNodeAccessPlan::AllScan,
        )),
    };
    let cursor = ctx.node_cursor(&operation).await.unwrap().unwrap();
    let result = ctx
        .node_id_batches(
            cursor,
            1,
            r::Slot(0),
            Limits {
                batch_rows: 1,
                ..Default::default()
            },
        )
        .try_collect::<Vec<_>>()
        .await;
    assert!(matches!(result,Err(Error::Query(error)) if error.detail=="MemoryLimit"));
    assert_eq!(ctx.row_budget().available(), 64);
    ctx.close_request_read_view().unwrap();
    drop(ctx);
    drop(foreign_context);
    db.close().await.unwrap();
}

#[tokio::test]
async fn cursor_contracts_cover_fallbacks_write_visibility_filters_and_exhaustion() {
    let db =
        crate::execution::interpreter::test_support::open_db("cypher-node-cursor-contracts").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(1024 * 1024));
    let access = |source| exec::ExecOp::Access {
        plan: Box::new(exec::ExecAccessPlan::Node(source)),
    };
    assert!(ctx
        .node_cursor(&exec::ExecOp::Noop)
        .await
        .unwrap()
        .is_none());
    assert!(ctx
        .node_cursor(&exec::ExecOp::Access {
            plan: Box::new(exec::ExecAccessPlan::Edge(
                exec::ExecEdgeAccessPlan::AllScan
            ))
        })
        .await
        .unwrap()
        .is_none());
    assert!(ctx
        .node_cursor(&access(exec::ExecNodeAccessPlan::FromParam {
            param: ir::NonEmptyString::new("ids").unwrap()
        }))
        .await
        .unwrap()
        .is_none());
    assert!(matches!(
        ctx.node_cursor(&access(exec::ExecNodeAccessPlan::AllScan))
            .await,
        Err(Error::Storage(crate::HelixDbError::InvariantViolation(_)))
    ));
    let empty = ctx
        .node_cursor(&access(exec::ExecNodeAccessPlan::Empty))
        .await
        .unwrap()
        .unwrap();
    let node = r::NodePattern {
        slot: r::Slot(0),
        label: Some("N".into()),
        properties: vec![],
    };
    let plan = r::MatchPlan {
        sources: vec![],
        steps: vec![r::MatchStep::Scan(r::Slot(0))],
        cartesian_products: 0,
        estimated_rows: 1,
        incoming: Default::default(),
    };
    let pattern = r::Pattern {
        nodes: vec![node],
        relationships: vec![],
        paths: vec![],
    };
    let operation = matches::Match {
        pattern: &pattern,
        optional: false,
        predicate: None,
        demand: usize::MAX,
    };
    let params = BTreeMap::new();
    assert!(ctx
        .graph_match_batches(empty, 1, operation, &plan, &params, Limits::default())
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .is_empty());
    ctx.enable_request_write_scope().await.unwrap();
    let id = ctx
        .row_create_node(
            "N",
            vec![property::Property::new(
                "key",
                property::property_value::PropertyValue::I64(2),
            )],
        )
        .await
        .unwrap();
    for (label, property_value, expected) in [("N", 2, 1), ("Other", 2, 0), ("N", 3, 0)] {
        let node = r::NodePattern {
            slot: r::Slot(0),
            label: Some(label.into()),
            properties: vec![(
                "key".into(),
                r::Expression::Literal(r::Value::Integer(property_value)),
            )],
        };
        let pattern = r::Pattern {
            nodes: vec![node],
            relationships: vec![],
            paths: vec![],
        };
        let operation = matches::Match {
            pattern: &pattern,
            optional: false,
            predicate: None,
            demand: usize::MAX,
        };
        let cursor = ctx
            .node_cursor(&access(exec::ExecNodeAccessPlan::AllScan))
            .await
            .unwrap()
            .unwrap();
        let batches = ctx
            .graph_match_batches(
                cursor,
                1,
                operation,
                &plan,
                &params,
                Limits {
                    batch_rows: 1,
                    ..Default::default()
                },
            )
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.len()).sum::<usize>(), expected);
        if expected == 1 {
            assert_eq!(batches[0][0][0], r::Value::Entity(r::Entity::Node(id)));
        }
    }
    ctx.row_memory = Some(memory::Budget::new(1));
    let cursor = ctx
        .node_cursor(&access(exec::ExecNodeAccessPlan::AllScan))
        .await
        .unwrap()
        .unwrap();
    let error = ctx
        .graph_match_batches(cursor, 1, operation, &plan, &params, Limits::default())
        .try_collect::<Vec<_>>()
        .await
        .err()
        .unwrap();
    assert!(matches!(error,Error::Query(error) if error.detail=="MemoryLimit"));
    ctx.abort_request_write_scope();
    assert_eq!(
        db.cypher(crate::cypher::Request::new("MATCH (n) RETURN count(*)"))
            .await
            .unwrap()
            .rows,
        vec![vec![serde_json::json!(0)]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn graph_hydration_rejects_invalid_metadata_and_corrupt_encoded_values() {
    let db =
        crate::execution::interpreter::test_support::open_db("cypher-hydration-corruption").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(1024 * 1024));
    for id in [17, 18, 19, 20] {
        let properties = if id == 19 {
            bytes::Bytes::from_static(b"corrupt")
        } else if id == 18 {
            property::encode_properties(&[])
        } else {
            property::encode_properties(&[property::Property::string("$label", "R")])
        };
        db.inner_db()
            .put(
                ctx.storage_key(keys::DataKeyKind::EdgePropertyById(
                    keys::EdgePropertyByIdKey::new(id),
                )),
                properties,
            )
            .await
            .unwrap();
        if id != 17 {
            let endpoints = if id == 20 {
                bytes::Bytes::from_static(b"short")
            } else {
                EdgeEndpointsValue::new(1, 2).encode()
            };
            db.inner_db()
                .put(
                    ctx.storage_key(keys::DataKeyKind::EdgeEndpoints(
                        keys::EdgeEndpointsKey::new(id),
                    )),
                    endpoints,
                )
                .await
                .unwrap();
        }
    }
    ctx.enable_request_read_view().await.unwrap();
    assert_eq!(
        ctx.relationship_types_batch(&[18, 17, u64::MAX], &["R".into()])
            .await
            .unwrap(),
        vec![false, true, false]
    );
    assert!(ctx
        .relationship_types_batch(&[19], &["R".into()])
        .await
        .is_err());
    for id in [17, 18, 19, 20] {
        let row = vec![r::Value::Entity(r::Entity::Relationship(id))];
        let result = ctx.graph_batch(&[row]).await;
        match id {
            17 => assert!(result.unwrap().entities.is_empty()),
            18 => assert!(
                matches!(result,Err(Error::Query(error)) if error.detail=="UntypedStoredRelationship")
            ),
            _ => assert!(matches!(result, Err(Error::Storage(_)))),
        }
    }
    ctx.close_request_read_view().unwrap();
    db.close().await.unwrap();
}
