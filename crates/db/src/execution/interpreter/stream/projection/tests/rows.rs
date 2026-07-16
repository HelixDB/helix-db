use super::*;

#[tokio::test]
async fn stream_projection_terminal_and_property_shapes_are_explicit() {
    let db = test_support::open_db("projection-stream-terminal-shapes").await;
    let ada = test_support::add_node_with_properties(
        &db,
        "User",
        vec![("name", PropertyValue::from("ada"))],
    )
    .await;
    let bob = test_support::add_user(&db, "bob").await;
    let edge = test_support::add_edge(&db, ada, bob, "KNOWS").await;
    let rows = || {
        vec![
            ExecutionRow::current(ElementRef::Node(ada)),
            ExecutionRow::empty(),
            ExecutionRow::current(ElementRef::Edge(edge)),
        ]
    };
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    assert_eq!(
        ctx.project(ExecutionValue::Stream(rows()), &ir::ProjectionPlan::Count)
            .await
            .expect("count projection succeeds"),
        ExecutionValue::Count(3)
    );
    assert_eq!(
        ctx.project(
            ExecutionValue::Stream(Vec::new()),
            &ir::ProjectionPlan::Exists,
        )
        .await
        .expect("exists projection succeeds"),
        ExecutionValue::Bool(false)
    );
    assert_eq!(
        ctx.project(ExecutionValue::Stream(rows()), &ir::ProjectionPlan::Id)
            .await
            .expect("id projection succeeds"),
        ExecutionValue::Scalars(vec![
            ExecutionScalar::NodeId(ada),
            ExecutionScalar::EdgeId(edge),
        ])
    );
    assert_eq!(
        ctx.project(
            ExecutionValue::Stream(rows()),
            &ir::ProjectionPlan::Values(property_names(vec!["name"])),
        )
        .await
        .expect("values projection succeeds"),
        ExecutionValue::Scalars(vec![ExecutionScalar::Object(BTreeMap::from([(
            "name".to_string(),
            DbPropertyValue::String("ada".to_string()),
        )]))])
    );
    assert_eq!(
        ctx.project(ExecutionValue::Stream(rows()), &ir::ProjectionPlan::Label)
            .await
            .expect("label projection succeeds"),
        ExecutionValue::Scalars(vec![
            ExecutionScalar::Value(DbPropertyValue::String("User".to_string())),
            ExecutionScalar::Value(DbPropertyValue::String("KNOWS".to_string())),
        ])
    );
}

#[tokio::test]
async fn value_map_all_reads_stored_node_properties() {
    let db = test_support::open_db("projection-value-map-all").await;
    let ada = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::from("ada")),
            ("score", PropertyValue::I64(7)),
        ],
    )
    .await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    let ExecutionValue::Scalars(values) = ctx
        .project(
            ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Node(ada))]),
            &ir::ProjectionPlan::ValueMap(ir::PropertySelection::All),
        )
        .await
        .expect("value-map all projection succeeds")
    else {
        panic!("value-map projection returns scalars");
    };

    assert_eq!(values.len(), 1);
    let object = object(values.into_iter().next().expect("one object"));
    assert_eq!(
        object.get("name"),
        Some(&DbPropertyValue::String("ada".to_string()))
    );
    assert_eq!(object.get("score"), Some(&DbPropertyValue::I64(7)));
    assert_eq!(object.get("$id"), Some(&DbPropertyValue::I64(ada as i64)));
    assert_eq!(
        object.get("$label"),
        Some(&DbPropertyValue::String("User".to_string()))
    );
}

#[tokio::test]
async fn value_map_selected_keeps_requested_stored_properties_only() {
    let db = test_support::open_db("projection-value-map-selected").await;
    let ada = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::from("ada")),
            ("score", PropertyValue::I64(7)),
        ],
    )
    .await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    let ExecutionValue::Scalars(values) = ctx
        .project(
            ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Node(ada))]),
            &ir::ProjectionPlan::ValueMap(ir::PropertySelection::Selected(property_names(vec![
                "score", "missing",
            ]))),
        )
        .await
        .expect("value-map selected projection succeeds")
    else {
        panic!("value-map projection returns scalars");
    };

    assert_eq!(
        values.into_iter().map(object).collect::<Vec<_>>(),
        vec![BTreeMap::from([(
            "score".to_string(),
            DbPropertyValue::I64(7)
        )])]
    );
}

#[tokio::test]
async fn values_and_selected_value_map_use_nested_property_paths() {
    let db = test_support::open_db("projection-nested-property-paths").await;
    let ada = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::from("ada")),
            (
                "metadata",
                PropertyValue::object([
                    ("externalID", PropertyValue::from("ada-ext")),
                    ("score", PropertyValue::I64(7)),
                ]),
            ),
        ],
    )
    .await;
    let row = ExecutionRow::current(ElementRef::Node(ada));
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    let values = ctx
        .project(
            ExecutionValue::Stream(vec![row.clone()]),
            &ir::ProjectionPlan::Values(property_names(vec![
                "$id",
                "metadata.externalID",
                "metadata.missing",
            ])),
        )
        .await
        .expect("nested values projection succeeds");
    assert_eq!(
        values,
        ExecutionValue::Scalars(vec![ExecutionScalar::Object(BTreeMap::from([
            ("$id".to_string(), DbPropertyValue::I64(ada as i64)),
            (
                "metadata.externalID".to_string(),
                DbPropertyValue::String("ada-ext".to_string())
            ),
        ]))])
    );

    let ExecutionValue::Scalars(map_values) = ctx
        .project(
            ExecutionValue::Stream(vec![row]),
            &ir::ProjectionPlan::ValueMap(ir::PropertySelection::Selected(property_names(vec![
                "$id",
                "metadata.score",
            ]))),
        )
        .await
        .expect("nested selected value-map projection succeeds")
    else {
        panic!("value-map projection returns scalars");
    };

    assert_eq!(
        map_values.into_iter().map(object).collect::<Vec<_>>(),
        vec![BTreeMap::from([
            ("$id".to_string(), DbPropertyValue::I64(ada as i64)),
            ("metadata.score".to_string(), DbPropertyValue::I64(7)),
        ])]
    );
}

#[tokio::test]
async fn general_projection_mixes_stored_properties_and_expressions() {
    let db = test_support::open_db("projection-general-items").await;
    let ada = test_support::add_node_with_properties(
        &db,
        "User",
        vec![("name", PropertyValue::from("ada"))],
    )
    .await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let projection = ir::ProjectionPlan::Project(projection_items(vec![
        ir::ProjectionItem::Property {
            source: name("name"),
            alias: name("display"),
        },
        ir::ProjectionItem::Property {
            source: name("missing"),
            alias: name("omitted"),
        },
        ir::ProjectionItem::Expr {
            alias: name("constant"),
            expr: ir::ExprPlan::new(Expr::val(42)).expect("valid constant expression"),
        },
    ]));

    let ExecutionValue::Scalars(values) = ctx
        .project(
            ExecutionValue::Stream(vec![
                ExecutionRow::current(ElementRef::Node(ada)),
                ExecutionRow::empty(),
            ]),
            &projection,
        )
        .await
        .expect("general projection succeeds")
    else {
        panic!("general projection returns scalars");
    };

    assert_eq!(
        values
            .into_iter()
            .map(object)
            .collect::<Vec<BTreeMap<_, _>>>(),
        vec![
            BTreeMap::from([
                (
                    "display".to_string(),
                    DbPropertyValue::String("ada".to_string()),
                ),
                ("constant".to_string(), DbPropertyValue::I64(42)),
            ]),
            BTreeMap::from([("constant".to_string(), DbPropertyValue::I64(42))]),
        ]
    );
}

#[tokio::test]
async fn binding_projection_dedup_is_stream_local() {
    let db = test_support::open_db("projection-binding-dedup").await;
    let ada = test_support::add_node_with_properties(
        &db,
        "User",
        vec![("name", PropertyValue::from("ada"))],
    )
    .await;
    let binding = name("person");
    let mut first = ExecutionRow::empty();
    first
        .bindings
        .insert(binding.clone(), ElementRef::Node(ada));
    let duplicate = first.clone();
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    let result = ctx
        .project(
            ExecutionValue::Stream(vec![first, duplicate]),
            &ir::ProjectionPlan::ProjectBindings {
                projections: binding_projection_items(vec![ir::BindingProjectionPlan::Property {
                    target: ir::BindingTargetPlan::Binding(binding),
                    source: name("name"),
                    alias: name("display"),
                }]),
                dedup: ir::ProjectionDedupMode::Distinct,
            },
        )
        .await
        .expect("binding projection succeeds");

    assert_eq!(
        result,
        ExecutionValue::Scalars(vec![ExecutionScalar::Object(BTreeMap::from([(
            "display".to_string(),
            DbPropertyValue::String("ada".to_string()),
        )]))])
    );
}

#[tokio::test]
async fn edge_properties_projection_skips_non_edge_rows() {
    let db = test_support::open_db("projection-edge-properties-filter").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let edge = test_support::add_edge_with_properties(
        &db,
        alice,
        bob,
        "KNOWS",
        vec![("since", PropertyValue::I64(2026))],
    )
    .await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    let ExecutionValue::Scalars(values) = ctx
        .project(
            ExecutionValue::Stream(vec![
                ExecutionRow::current(ElementRef::Node(alice)),
                ExecutionRow::empty(),
                ExecutionRow::current(ElementRef::Edge(edge)),
            ]),
            &ir::ProjectionPlan::EdgeProperties,
        )
        .await
        .expect("edge-properties projection succeeds")
    else {
        panic!("edge-properties projection returns scalars");
    };

    assert_eq!(
        values.into_iter().map(object).collect::<Vec<_>>(),
        vec![BTreeMap::from([
            (
                "$label".to_string(),
                DbPropertyValue::String("KNOWS".to_string()),
            ),
            ("since".to_string(), DbPropertyValue::I64(2026)),
        ])]
    );
}

#[tokio::test]
async fn project_rejects_folded_stream_inputs() {
    let db = test_support::open_db("projection-folded-rejection").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    assert!(ctx
        .project(
            ExecutionValue::FoldedStream(FoldedStream::new(vec![ExecutionRow::current(
                ElementRef::Node(1)
            )])),
            &ir::ProjectionPlan::Id,
        )
        .await
        .expect_err("folded stream projection is rejected")
        .to_string()
        .contains("project expected stream input, got folded stream"));
}
