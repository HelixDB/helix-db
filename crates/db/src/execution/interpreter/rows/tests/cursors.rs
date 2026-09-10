use super::super::*;
use crate::encoding::v2::{
    keys,
    values::{edge_endpoints::EdgeEndpointsValue, property},
};
use futures::TryStreamExt;
use helix_planner::{context, exec, ir};

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
        let reservation = ctx
            .row_budget()
            .reserve(memory::bitmap_bytes(&owners))
            .unwrap();
        let cursor = scan::NodeCursor::Indexed {
            ids: owners.into_iter(),
            verify_existence: true,
            _memory: reservation,
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
