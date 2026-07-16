use super::support::*;

#[tokio::test]
async fn limited_native_node_access_truncates_before_projection() {
    let db = test_support::open_db("access-limited-native-node").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let carol = test_support::add_user(&db, "carol").await;
    let ids = test_support::name("ids");

    let value = run_limited_node_access_with_params(
        &db,
        exec::ExecNodeAccessPlan::FromParam { param: ids.clone() },
        2,
        context::ParamBindings::default().with_value(
            ids,
            PropertyValue::I64Array(vec![alice as i64, bob as i64, carol as i64]),
        ),
    )
    .await;

    assert_eq!(
        value,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::NodeId(alice),
            ExecutionScalar::NodeId(bob),
        ])
    );
}

#[tokio::test]
async fn element_range_scan_honors_inclusive_and_exclusive_node_bounds() {
    let db = test_support::open_db("access-node-kv-range-bounds").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let carol = test_support::add_user(&db, "carol").await;
    let keyspace = exec::ElementKeyspace::NodeProperty;

    let lower_excluded = db
        .execute(
            &element_range_scan_ids_plan(
                keyspace,
                exec::KvKeyBound::excluded_id(alice),
                exec::KvKeyBound::included_id(carol),
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("range scan executes")
        .last
        .expect("project step returns a value");
    assert_eq!(
        lower_excluded,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::NodeId(bob),
            ExecutionScalar::NodeId(carol)
        ])
    );

    let upper_excluded = db
        .execute(
            &element_range_scan_ids_plan(
                keyspace,
                exec::KvKeyBound::included_id(alice),
                exec::KvKeyBound::excluded_id(carol),
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("range scan executes")
        .last
        .expect("project step returns a value");
    assert_eq!(
        upper_excluded,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::NodeId(alice),
            ExecutionScalar::NodeId(bob)
        ])
    );
}

#[tokio::test]
async fn element_range_scan_honors_inclusive_and_exclusive_edge_bounds() {
    let db = test_support::open_db("access-edge-kv-range-bounds").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let carol = test_support::add_user(&db, "carol").await;
    let follows = test_support::add_edge(&db, alice, bob, "FOLLOWS").await;
    let knows = test_support::add_edge(&db, bob, carol, "KNOWS").await;
    let likes = test_support::add_edge(&db, carol, alice, "LIKES").await;
    let keyspace = exec::ElementKeyspace::EdgeEndpoints;

    let value = db
        .execute(
            &element_range_scan_ids_plan(
                keyspace,
                exec::KvKeyBound::excluded_id(follows),
                exec::KvKeyBound::excluded_id(likes),
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("range scan executes")
        .last
        .expect("project step returns a value");

    assert_eq!(
        value,
        ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(knows)])
    );
}

#[tokio::test]
async fn single_get_reads_typed_node_point_key() {
    let db = test_support::open_db("access-kv-get-node").await;
    let alice = test_support::add_user(&db, "alice").await;

    let value = db
        .execute(
            &kv_read_ids_plan(exec::KvReadPlan::Get {
                key: exec::ElementKeyspace::NodeProperty.point_key(alice),
            }),
            context::ParamBindings::default(),
        )
        .await
        .expect("typed point get executes")
        .last
        .expect("project step returns a value");

    assert_eq!(
        value,
        ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(alice)])
    );
}

#[tokio::test]
async fn multi_get_restores_original_node_order_after_sorted_physical_reads() {
    let db = test_support::open_db("access-multi-get-node-order").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let carol = test_support::add_user(&db, "carol").await;
    let missing = carol + 10_000;
    let batch = exec::KvMultiGetPlan::new(
        vec![carol, missing, alice, bob]
            .into_iter()
            .map(|id| exec::ElementKeyspace::NodeProperty.point_key(id))
            .collect(),
        properties::KeyLocality::Close,
        properties::PositiveUsize::new(4).expect("positive batch size"),
    )
    .expect("valid node multi-get");

    let value = db
        .execute(
            &kv_read_ids_plan(exec::KvReadPlan::MultiGet(batch)),
            context::ParamBindings::default(),
        )
        .await
        .expect("multi-get executes")
        .last
        .expect("project step returns a value");

    assert_eq!(
        value,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::NodeId(carol),
            ExecutionScalar::NodeId(alice),
            ExecutionScalar::NodeId(bob),
        ])
    );
}

#[tokio::test]
async fn multi_get_restores_original_edge_order_after_sorted_physical_reads() {
    let db = test_support::open_db("access-multi-get-edge-order").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let carol = test_support::add_user(&db, "carol").await;
    let follows = test_support::add_edge(&db, alice, bob, "FOLLOWS").await;
    let knows = test_support::add_edge(&db, bob, carol, "KNOWS").await;
    let likes = test_support::add_edge(&db, carol, alice, "LIKES").await;
    let batch = exec::KvMultiGetPlan::new(
        vec![likes, follows, knows]
            .into_iter()
            .map(|id| exec::ElementKeyspace::EdgeEndpoints.point_key(id))
            .collect(),
        properties::KeyLocality::Close,
        properties::PositiveUsize::new(3).expect("positive batch size"),
    )
    .expect("valid edge multi-get");

    let value = db
        .execute(
            &kv_read_ids_plan(exec::KvReadPlan::MultiGet(batch)),
            context::ParamBindings::default(),
        )
        .await
        .expect("multi-get executes")
        .last
        .expect("project step returns a value");

    assert_eq!(
        value,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::EdgeId(likes),
            ExecutionScalar::EdgeId(follows),
            ExecutionScalar::EdgeId(knows),
        ])
    );
}

#[test]
fn limited_index_helpers_preserve_prefix_order_and_tighten_search_k() {
    let mut ids = roaring::RoaringTreemap::new();
    ids.insert(3);
    ids.insert(1);
    ids.insert(2);

    assert_eq!(
        limited_index_ids(ids.clone(), properties::PositiveUsize::new(2)),
        vec![1, 2]
    );
    assert_eq!(limited_index_ids(ids, None), vec![1, 2, 3]);
    assert_eq!(limited_search_k(10, properties::PositiveUsize::new(4)), 4);
    assert_eq!(limited_search_k(3, properties::PositiveUsize::new(4)), 3);
    assert_eq!(limited_search_k(7, None), 7);
}
