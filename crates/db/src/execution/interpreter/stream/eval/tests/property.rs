use super::*;

#[tokio::test]
async fn row_property_reads_id_stored_properties_and_missing_values() {
    let db = test_support::open_db("stream-eval-row-property").await;
    let id = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::String("ada".to_string())),
            ("age", PropertyValue::I64(37)),
            (
                "metadata",
                PropertyValue::object([
                    ("externalID", PropertyValue::from("ada-ext")),
                    ("score", PropertyValue::I64(9)),
                ]),
            ),
            ("metadata.externalID", PropertyValue::from("exact-ext")),
        ],
    )
    .await;
    let ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let row = current_node(id);

    assert_eq!(
        ctx.row_property(&row, &name("$id")).await.unwrap(),
        Some(DbPropertyValue::I64(id as i64))
    );
    assert_eq!(
        ctx.row_property(&row, &name("name")).await.unwrap(),
        Some(DbPropertyValue::String("ada".to_string()))
    );
    assert_eq!(
        ctx.row_property(&row, &name("metadata.score"))
            .await
            .unwrap(),
        Some(DbPropertyValue::I64(9))
    );
    assert_eq!(
        ctx.row_property(&row, &name("metadata.externalID"))
            .await
            .unwrap(),
        Some(DbPropertyValue::String("exact-ext".to_string()))
    );
    assert_eq!(
        ctx.row_property(&row, &name("metadata.")).await.unwrap(),
        None
    );
    assert_eq!(ctx.row_property(&row, &name(".score")).await.unwrap(), None);
    assert_eq!(
        ctx.row_property(&row, &name("age.value")).await.unwrap(),
        None
    );
    assert_eq!(
        ctx.row_property(&row, &name("missing")).await.unwrap(),
        None
    );
    assert_eq!(
        ctx.row_properties(&ExecutionRow::empty()).await.unwrap(),
        Vec::new()
    );
}

#[tokio::test]
async fn row_property_reads_edge_properties_and_empty_current_id() {
    let db = test_support::open_db("stream-eval-row-edge-property").await;
    let from = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::String("Alice".to_string())),
            ("kind", PropertyValue::String("source".to_string())),
            (
                "metadata",
                PropertyValue::object([("score", PropertyValue::I64(9))]),
            ),
            ("metadata.externalID", PropertyValue::from("exact-source")),
            ("$id", PropertyValue::from("stored-id-must-not-win")),
        ],
    )
    .await;
    let to = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::String("Bob".to_string())),
            ("kind", PropertyValue::String("target".to_string())),
        ],
    )
    .await;
    let edge = test_support::add_edge_with_properties(
        &db,
        from,
        to,
        "Follows",
        vec![("since", PropertyValue::I64(2024))],
    )
    .await;
    let ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    assert_eq!(
        ctx.row_property(&current_edge(edge), &name("since"))
            .await
            .unwrap(),
        Some(DbPropertyValue::I64(2024))
    );
    assert_eq!(
        ctx.row_property(&current_edge(edge), &name("$from"))
            .await
            .unwrap(),
        Some(DbPropertyValue::I64(from as i64))
    );
    assert_eq!(
        ctx.row_property(&current_edge(edge), &name("$to"))
            .await
            .unwrap(),
        Some(DbPropertyValue::I64(to as i64))
    );
    assert_eq!(
        ctx.row_property(&current_edge(edge), &name("$from.$id"))
            .await
            .unwrap(),
        Some(DbPropertyValue::I64(from as i64))
    );
    assert_eq!(
        ctx.row_property(&current_edge(edge), &name("$to.$id"))
            .await
            .unwrap(),
        Some(DbPropertyValue::I64(to as i64))
    );
    assert_eq!(
        ctx.row_property(&current_edge(edge), &name("$from.name"))
            .await
            .unwrap(),
        Some(DbPropertyValue::String("Alice".to_string()))
    );
    assert_eq!(
        ctx.row_property(&current_edge(edge), &name("$to.kind"))
            .await
            .unwrap(),
        Some(DbPropertyValue::String("target".to_string()))
    );
    assert_eq!(
        ctx.row_property(&current_edge(edge), &name("$from.metadata.score"))
            .await
            .unwrap(),
        Some(DbPropertyValue::I64(9))
    );
    assert_eq!(
        ctx.row_property(&current_edge(edge), &name("$from.metadata.externalID"))
            .await
            .unwrap(),
        Some(DbPropertyValue::String("exact-source".to_string()))
    );
    assert_eq!(
        ctx.row_property(&current_edge(edge), &name("$to.missing"))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        ctx.row_property(&current_edge(edge), &name("$from."))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        ctx.row_property(&current_node(from), &name("$from"))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        ctx.row_property(&ExecutionRow::empty(), &name("$id"))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        ctx.row_property(&current_edge(u64::MAX), &name("$from"))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        ctx.row_property(&current_edge(u64::MAX), &name("$from.name"))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        ctx.row_property(&current_edge(u64::MAX), &name("$to.$id"))
            .await
            .unwrap(),
        None
    );
}
