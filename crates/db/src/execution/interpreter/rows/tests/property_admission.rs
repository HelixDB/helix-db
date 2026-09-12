use super::super::*;
use crate::encoding::v2::{
    keys,
    values::property::{self, property_value::PropertyValue as P, Property},
};
use crate::execution::interpreter::{test_support, ElementRef, ExecutionRow};
use helix_planner::context;
use r::GraphValues;

#[tokio::test]
async fn selected_native_and_graph_fields_skip_large_decoded_payloads_and_release_owners() {
    let db = test_support::open_db("selected-property-admission").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let encoded = property::encode_properties(&[
        Property::string("$label", "N"),
        Property::i64("small", 17),
        Property::new("large", P::I64Array(vec![1; 16 * 1024])),
        Property::new("null", P::Null),
        Property::new("future", P::DateTime(1)),
        Property::string("$internal", "hidden"),
    ]);
    for kind in [
        keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(7)),
        keys::DataKeyKind::EdgePropertyById(keys::EdgePropertyByIdKey::new(9)),
    ] {
        db.inner_db()
            .put(ctx.storage_key(kind), encoded.clone())
            .await
            .unwrap();
    }
    // Raw bytes and their aligned validation copy remain necessary. Selection
    // avoids decoding/converting the large list, independent of allocator RSS.
    let limit = encoded.len() * 2 + 32 * 1024;
    ctx.row_memory = Some(memory::Budget::new(limit));
    ctx.enable_request_read_view().await.unwrap();
    for element in [ElementRef::Node(7), ElementRef::Edge(9)] {
        let row = ExecutionRow::current(element);
        assert!(ctx
            .row_properties_match(&row, &["$label", "small"], |properties| {
                assert!(ctx.row_budget().available() < limit - encoded.len());
                properties == [Property::string("$label", "N"), Property::i64("small", 17)]
            })
            .await
            .unwrap());
        assert_eq!(ctx.row_budget().available(), limit);
        assert!(matches!(
            ctx.row_properties_match(&row, &["large"], |_| panic!("decode must be denied"))
                .await,
            Err(crate::HelixDbError::QueryMemoryLimitExceeded)
        ));
        assert_eq!(ctx.row_budget().available(), limit);
    }
    for row in [
        ExecutionRow::empty(),
        ExecutionRow::current(ElementRef::Node(u64::MAX)),
    ] {
        assert!(ctx
            .row_properties_match(&row, &["small"], <[Property]>::is_empty)
            .await
            .unwrap());
        assert_eq!(ctx.row_budget().available(), limit);
    }
    assert_eq!(
        ctx.relationship_types_batch(&[9, u64::MAX], &["N".into()])
            .await
            .unwrap(),
        vec![true, false]
    );
    assert_eq!(ctx.row_budget().available(), limit);
    let rows = [vec![r::Value::Entity(r::Entity::Node(7))]];
    let wanted = BTreeMap::from([(
        r::Slot(0),
        r::PropertyDemand::Keys(
            ["small", "null", "future", "$internal"]
                .into_iter()
                .map(String::from)
                .collect(),
        ),
    )]);
    let graph = ctx.graph_batch_required(&rows, &wanted).await.unwrap();
    let properties = graph.properties(r::Entity::Node(7)).unwrap();
    assert_eq!(properties.len(), 2);
    assert_eq!(properties["small"], Ok(r::Value::Integer(17)));
    assert!(matches!(&properties["future"], Err(error) if error.detail == "StoredValueType"));
    assert_eq!(graph.label(r::Entity::Node(7)).unwrap(), Some("N"));
    assert!(
        ctx.row_budget().available() < limit,
        "hydrated values retain admission"
    );
    drop(graph);
    assert_eq!(ctx.row_budget().available(), limit);
    let all = BTreeMap::from([(r::Slot(0), r::PropertyDemand::All)]);
    assert!(
        matches!(ctx.graph_batch_required(&rows, &all).await, Err(Error::Query(error)) if error.detail == "MemoryLimit")
    );
    assert_eq!(ctx.row_budget().available(), limit);
    ctx.close_request_read_view().unwrap();
    // The native frontend without a query budget keeps its legacy decoder.
    ctx.row_memory = None;
    assert!(ctx
        .row_properties_match(
            &ExecutionRow::current(ElementRef::Node(7)),
            &["large"],
            |properties| {
                matches!(&properties[0].value, P::I64Array(values) if values.len() == 16 * 1024)
            }
        )
        .await
        .unwrap());
    db.close().await.unwrap();
}

#[tokio::test]
async fn conversion_admission_rejects_expanded_lists_before_hydration_and_releases_on_cancel() {
    let db = test_support::open_db("property-conversion-admission").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let encoded = property::encode_properties(&[
        Property::string("$label", "N"),
        Property::new("values", P::I64Array(vec![1; 4096])),
    ]);
    db.inner_db()
        .put(
            ctx.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
                7,
            ))),
            encoded.clone(),
        )
        .await
        .unwrap();
    let selected = property::prepared::Archive::new(&encoded);
    let decoded = selected
        .prepare(property::prepared::Selection::All)
        .unwrap()
        .owned_bytes();
    // There is room for raw, alignment and decoded owners but insufficient room
    // for the expanded row-value array alongside the decoded source.
    let limit = encoded.len() * 2 + decoded + 16 * 1024;
    ctx.row_memory = Some(memory::Budget::new(limit));
    ctx.enable_request_read_view().await.unwrap();
    let rows = [vec![r::Value::Entity(r::Entity::Node(7))]];
    let all = BTreeMap::from([(r::Slot(0), r::PropertyDemand::All)]);
    assert!(
        matches!(ctx.graph_batch_required(&rows, &all).await, Err(Error::Query(error)) if error.detail == "MemoryLimit")
    );
    assert_eq!(ctx.row_budget().available(), limit);
    let unpolled = ctx.graph_batch_required(&rows, &all);
    drop(unpolled);
    assert_eq!(ctx.row_budget().available(), limit);
    ctx.fail_deadline_after(0);
    assert!(matches!(
        ctx.graph_batch_required(&rows, &all).await,
        Err(Error::Storage(crate::HelixDbError::QueryDeadlineExceeded))
    ));
    assert_eq!(ctx.row_budget().available(), limit);
    ctx.close_request_read_view().unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn stored_archive_nesting_has_a_specific_resource_error_and_releases_all_admission() {
    let db = test_support::open_db("stored-archive-nesting").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let nested = (0..513).fold(P::I64(1), |value, _| P::Array(vec![value]));
    let encoded = property::encode_properties(&[Property::new("nested", nested)]);
    db.inner_db()
        .put(
            ctx.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
                7,
            ))),
            encoded,
        )
        .await
        .unwrap();
    let limit = 1024 * 1024;
    ctx.row_memory = Some(memory::Budget::new(limit));
    ctx.enable_request_read_view().await.unwrap();
    let rows = [vec![r::Value::Entity(r::Entity::Node(7))]];
    let all = BTreeMap::from([(r::Slot(0), r::PropertyDemand::All)]);
    assert!(
        matches!(ctx.graph_batch_required(&rows, &all).await, Err(Error::Query(error))
        if error.category == "ResourceLimit" && error.detail == "StoredValueNestingLimit" && error.phase == r::ErrorPhase::Runtime)
    );
    assert_eq!(ctx.row_budget().available(), limit);
    ctx.close_request_read_view().unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn stored_map_output_obeys_the_common_value_depth_limit_without_forcing_unused_values() {
    let db = test_support::open_db("stored-map-output-depth").await;
    for depth in [
        0,
        r::MAX_EXPRESSION_DEPTH - 1,
        r::MAX_EXPRESSION_DEPTH,
        96,
        254,
    ] {
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let nested = (0..depth).fold(P::I64(i64::MAX), |value, _| {
            P::Object(BTreeMap::from([("child".into(), value)]))
        });
        let encoded = property::encode_properties(&[
            Property::string("$label", "N"),
            Property::new("nested", nested),
        ]);
        db.inner_db()
            .put(
                ctx.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
                    7,
                ))),
                encoded,
            )
            .await
            .unwrap();
        ctx.row_memory = Some(memory::Budget::new(Limits::default().memory_bytes));
        ctx.enable_request_read_view().await.unwrap();
        let rows = [vec![r::Value::Entity(r::Entity::Node(7))]];
        let all = BTreeMap::from([(r::Slot(0), r::PropertyDemand::All)]);
        let graph = ctx.graph_batch_required(&rows, &all).await.unwrap();
        let value = graph.properties(r::Entity::Node(7)).unwrap()["nested"].as_ref();
        if depth < r::MAX_EXPRESSION_DEPTH {
            let value = value.unwrap();
            let output_memory = ctx
                .row_budget()
                .reserve(value.allocated_bytes() + graph.wire_memory(value).unwrap())
                .unwrap();
            let value = value.clone();
            let wire = graph.wire(&value).unwrap();
            let mut leaf = &wire;
            for _ in 0..depth {
                leaf = &leaf["child"];
            }
            assert_eq!(
                leaf,
                &serde_json::json!({"$type":"integer", "value": i64::MAX.to_string()})
            );
            assert!(!serde_json::to_vec(&wire).unwrap().is_empty());
            drop(wire);
            drop(value);
            drop(output_memory);
        } else {
            let error = value.unwrap_err();
            assert_eq!(error.category, "ResourceLimit");
            assert_eq!(error.detail, "StoredValueNestingLimit");
            assert_eq!(error.phase, r::ErrorPhase::Runtime);
            assert!(
                matches!(graph.wire(&rows[0][0]), Err(Error::Query(error)) if error.detail == "StoredValueNestingLimit")
            );
        }
        drop(graph);
        assert_eq!(ctx.row_budget().available(), Limits::default().memory_bytes);
        ctx.close_request_read_view().unwrap();
        // These are production-service reads over the same native storage row.
        // Both operations can inspect the entity without forcing a deep value.
        assert_eq!(
            db.cypher(crate::cypher::Request::new(
                "MATCH (n) RETURN CASE WHEN false THEN n.nested ELSE 17 END AS value"
            ))
            .await
            .unwrap()
            .rows,
            vec![vec![serde_json::json!(17)]]
        );
        assert_eq!(
            db.cypher(crate::cypher::Request::new(
                "MATCH (n) RETURN keys(n) AS names"
            ))
            .await
            .unwrap()
            .rows,
            vec![vec![serde_json::json!(["nested"])]]
        );
        if depth >= r::MAX_EXPRESSION_DEPTH {
            assert!(
                matches!(db.cypher(crate::cypher::Request::new("MATCH (n) RETURN n.nested")).await, Err(Error::Query(error)) if error.category == "ResourceLimit" && error.detail == "StoredValueNestingLimit")
            );
            assert!(
                matches!(db.cypher(crate::cypher::Request::new("CREATE (:RolledBack) WITH 1 AS x MATCH (n) WHERE n.nested IS NOT NULL RETURN n.nested")).await,
                Err(Error::Query(error)) if error.detail == "StoredValueNestingLimit")
            );
            assert_eq!(
                db.cypher(crate::cypher::Request::new(
                    "MATCH (n:RolledBack) RETURN count(*)"
                ))
                .await
                .unwrap()
                .rows,
                vec![vec![serde_json::json!(0)]]
            );
        }
    }
    db.close().await.unwrap();
}
