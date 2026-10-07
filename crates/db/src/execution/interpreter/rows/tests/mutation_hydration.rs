use super::super::*;
use crate::encoding::v2::{
    keys,
    values::property::{self, property_value::PropertyValue as P, Property},
};
use crate::execution::interpreter::test_support;
use helix_planner::context;
use serde_json::json;

#[tokio::test]
async fn create_hydrates_only_its_property_expressions_before_staging() {
    for selected in [false, true] {
        let db = test_support::open_db("create-selective-hydration").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let encoded = property::encode_properties(&[
            Property::string("$label", "Source"),
            Property::i64("small", 17),
            Property::new("large", P::I64Array(vec![1; 16 * 1024])),
            Property::new("future", P::DateTime(1)),
        ]);
        db.inner_db()
            .put(
                ctx.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
                    u64::MAX,
                ))),
                encoded.clone(),
            )
            .await
            .unwrap();
        // Raw archive validation remains necessary for a selected field; the
        // unselected native array must not expand into row values alongside it.
        let limit = encoded.len() * 2 + 64 * 1024;
        ctx.row_memory = Some(memory::Budget::new(limit));
        ctx.enable_request_write_scope().await.unwrap();
        let expression = if selected {
            r::Expression::Property(Box::new(r::Expression::Slot(r::Slot(0))), "small".into())
        } else {
            r::Expression::Literal(r::Value::Integer(17))
        };
        let pattern = r::Pattern {
            nodes: vec![r::NodePattern {
                slot: r::Slot(1),
                label: Some("Created".into()),
                properties: vec![("value".into(), expression)],
            }],
            relationships: vec![],
            paths: vec![],
        };
        let input = memory::Rows::new(
            vec![vec![
                r::Value::Entity(r::Entity::Node(u64::MAX)),
                r::Value::Null,
            ]],
            ctx.row_budget(),
        )
        .unwrap();
        let rows = ctx
            .create_rows(input, &pattern, &BTreeMap::new(), Limits::default())
            .await
            .unwrap();
        let r::Value::Entity(r::Entity::Node(id)) = rows[0][1] else {
            panic!("created node")
        };
        let reads = ctx.row_budget().reads();
        assert_eq!(reads.multi_get_keys, usize::from(selected));
        assert_eq!(reads.multi_get_batches, usize::from(selected));
        drop(rows);
        ctx.commit_request_write_scope().await.unwrap();
        assert_eq!(ctx.row_budget().available(), limit);
        let bytes = db
            .inner_db()
            .get(
                ctx.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
                    id,
                ))),
            )
            .await
            .unwrap()
            .unwrap();
        let properties = property::decode_properties(&bytes).unwrap();
        assert!(properties.contains(&Property::i64("value", 17)));
        ctx.enable_request_write_scope().await.unwrap();
        let before = ctx.row_budget().reads();
        let input = memory::Rows::new(
            vec![vec![
                r::Value::Entity(r::Entity::Node(u64::MAX)),
                r::Value::Entity(r::Entity::Node(id)),
            ]],
            ctx.row_budget(),
        )
        .unwrap();
        let updates = [
            r::PropertyMutation::Set {
                entity: r::Slot(1),
                key: "copied".into(),
                value: r::Expression::Property(
                    Box::new(r::Expression::Slot(r::Slot(0))),
                    "small".into(),
                ),
            },
            r::PropertyMutation::Remove {
                entity: r::Slot(1),
                key: "value".into(),
            },
        ];
        let rows = ctx
            .update_rows(input, &updates, &BTreeMap::new(), Limits::default())
            .await
            .unwrap();
        let after = ctx.row_budget().reads();
        // One selected expression read and one canonical observation per edit.
        assert_eq!(after.multi_get_keys - before.multi_get_keys, 3);
        assert_eq!(after.multi_get_batches - before.multi_get_batches, 3);
        drop(rows);
        ctx.commit_request_write_scope().await.unwrap();
        assert_eq!(ctx.row_budget().available(), limit);
        let bytes = db
            .inner_db()
            .get(
                ctx.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
                    id,
                ))),
            )
            .await
            .unwrap()
            .unwrap();
        let properties = property::decode_properties(&bytes).unwrap();
        assert!(properties.contains(&Property::i64("copied", 17)));
        assert!(!properties.iter().any(|property| property.name == "value"));
        let source = db
            .inner_db()
            .get(
                ctx.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
                    u64::MAX,
                ))),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            source, encoded,
            "unrelated native representations stay unchanged"
        );
        drop(ctx);
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn selected_entity_maps_and_sequential_writes_observe_current_properties() {
    for choose_a in [false, true] {
        let db = test_support::open_db("map-source-hydration").await;
        db.cypher(crate::cypher::Request::new(
            "CREATE (:Target {x:99,old:1}), (:A {x:17}), (:B {x:23}), (:Endpoint)-[:R {old:2}]->(:Endpoint)",
        )).await.unwrap();
        let mut request = crate::cypher::Request::new(
            "MATCH (t:Target),(a:A),(b:B),()-[r:R]->() \
             SET t = CASE WHEN $choose THEN a ELSE b END, t.x=t.x+1 \
             SET r = coalesce(null,t), r.x=r.x+1 \
             RETURN properties(t),properties(r)",
        );
        request.parameters.insert(
            "choose".into(),
            helix_ast::query::QueryValue::Bool(choose_a),
        );
        let result = db.cypher(request).await.unwrap();
        let x = if choose_a { 18 } else { 24 };
        assert_eq!(result.rows, vec![vec![json!({"x":x}), json!({"x":x+1})]]);
        let result = db.cypher(crate::cypher::Request::new(
            "CREATE (a:Fresh {x:5}), (b:Fresh {x:a.x+1}), (a)-[r:FRESH {x:b.x+1}]->(b) RETURN a.x,b.x,r.x",
        )).await.unwrap();
        assert_eq!(result.rows, vec![vec![json!(5), json!(6), json!(7)]]);
        for (query, detail) in [
            ("MATCH (t:Target),(a:A) SET t.x=100 SET t = CASE WHEN true THEN a ELSE null END RETURN 1/0", "DivisionByZero"),
            ("MATCH (t:Target),(a:A) SET t.x=100 DELETE a SET t=coalesce(a,null)", "DeletedEntityAccess"),
        ] {
            let error = db.cypher(crate::cypher::Request::new(query)).await.unwrap_err();
            assert!(matches!(error, Error::Query(error) if error.phase == r::ErrorPhase::Runtime && error.detail == detail), "{query}");
            let result = db.cypher(crate::cypher::Request::new(
                "MATCH (t:Target),(a:A) RETURN t.x,a.x",
            )).await.unwrap();
            assert_eq!(result.rows, vec![vec![json!(x),json!(17)]]);
        }
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn unselected_map_sources_stay_unloaded_and_selected_source_limits_roll_back() {
    let db = test_support::open_db("selected-map-source-admission").await;
    db.cypher(crate::cypher::Request::new(
        "CREATE (:Target {x:99}), (:Small {x:17})",
    ))
    .await
    .unwrap();
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let large = ctx
        .row_create_node(
            "Large",
            vec![Property::new("payload", P::I64Array(vec![1; 16 * 1024]))],
        )
        .await
        .unwrap();
    let bytes = db
        .inner_db()
        .get(
            ctx.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
                large,
            ))),
        )
        .await
        .unwrap()
        .unwrap();
    let limits = Limits {
        memory_bytes: bytes.len() * 2 + 64 * 1024,
        ..Limits::default()
    };
    drop(bytes);
    drop(ctx);
    let result = crate::cypher::execute(
        &db,
        crate::cypher::Request::new("MATCH (t:Target),(s:Small),(l:Large) SET t=CASE WHEN true THEN s ELSE l END RETURN t.x"),
        keys::scope::DataScope::LegacyUnscoped,
        crate::query_service::QueryMode::Execute,
        crate::execution_control::ExecutionControl::unlimited(),
        limits,
    ).await.unwrap();
    assert_eq!(result.rows, vec![vec![json!(17)]]);
    let error = crate::cypher::execute(
        &db,
        crate::cypher::Request::new("MATCH (t:Target),(s:Small),(l:Large) SET t.marker=99 SET t=CASE WHEN false THEN s ELSE l END RETURN t.x"),
        keys::scope::DataScope::LegacyUnscoped,
        crate::query_service::QueryMode::Execute,
        crate::execution_control::ExecutionControl::unlimited(),
        limits,
    ).await.unwrap_err();
    assert!(
        matches!(error,Error::Query(error) if error.phase == r::ErrorPhase::Runtime && error.detail == "MemoryLimit")
    );
    let after = db
        .cypher(crate::cypher::Request::new(
            "MATCH (t:Target) RETURN properties(t)",
        ))
        .await
        .unwrap();
    assert_eq!(after.rows, vec![vec![json!({"x":17})]]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn cancelled_hydration_rejects_before_constructing_property_requirements() {
    use futures::FutureExt;
    let db = test_support::open_db("cancelled-property-requirements").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(0));
    let expression = r::Expression::Property(
        Box::new(r::Expression::Slot(r::Slot(0))),
        "key".repeat(64 * 1024),
    );
    ctx.fail_deadline_after(0);
    let (result, allocations) = crate::allocation_testing::observe(|| {
        ctx.expression_graph_batch(&[], [&expression])
            .now_or_never()
    });
    assert!(matches!(
        result,
        Some(Err(Error::Storage(
            crate::HelixDbError::QueryDeadlineExceeded
        )))
    ));
    assert_eq!(allocations.allocations, 0);
    assert_eq!(ctx.row_budget().available(), 0);
    drop(ctx);
    db.close().await.unwrap();
}
