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
    let mut label_rows = rows();
    label_rows.push(ExecutionRow::current_with_virtual_properties(
        ElementRef::Node(u64::MAX),
        RowVirtualProperties::from_one(
            test_support::name("$label"),
            DbPropertyValue::String("Virtual".to_string()),
        ),
    ));
    assert_eq!(
        ctx.project(
            ExecutionValue::Stream(label_rows),
            &ir::ProjectionPlan::Label,
        )
        .await
        .expect("label projection succeeds"),
        ExecutionValue::Scalars(vec![
            ExecutionScalar::Value(DbPropertyValue::String("User".to_string())),
            ExecutionScalar::Value(DbPropertyValue::String("KNOWS".to_string())),
            ExecutionScalar::Value(DbPropertyValue::String("Virtual".to_string())),
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
    let stored = object(values.into_iter().next().expect("one object"));
    assert_eq!(
        stored.get("name"),
        Some(&DbPropertyValue::String("ada".to_string()))
    );
    assert_eq!(stored.get("score"), Some(&DbPropertyValue::I64(7)));
    assert_eq!(stored.get("$id"), Some(&DbPropertyValue::I64(ada as i64)));
    assert_eq!(
        stored.get("$label"),
        Some(&DbPropertyValue::String("User".to_string()))
    );

    let ExecutionValue::Scalars(values) = ctx
        .project(
            ExecutionValue::Stream(vec![
                ExecutionRow::current(ElementRef::Node(u64::MAX)),
                ExecutionRow::empty(),
            ]),
            &ir::ProjectionPlan::ValueMap(ir::PropertySelection::All),
        )
        .await
        .expect("oversized ID value-map projection succeeds")
    else {
        panic!("value-map projection returns scalars");
    };
    assert_eq!(
        values.into_iter().map(object).collect::<Vec<_>>(),
        vec![
            BTreeMap::from([("$id".to_string(), DbPropertyValue::I64(i64::MAX),)]),
            BTreeMap::new()
        ]
    );
    assert_projection_reads(&ctx, 2, 1, 0);
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
    assert_projection_reads(&ctx, 2, 2, 0);
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
async fn selected_projections_materialize_each_row_once() {
    let db = test_support::open_db("projection-read-baseline-selected").await;
    let ada = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::from("ada")),
            ("score", PropertyValue::I64(7)),
        ],
    )
    .await;
    let row = ExecutionRow::current(ElementRef::Node(ada));

    let mut values_ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    values_ctx
        .project(
            ExecutionValue::Stream(vec![row.clone()]),
            &ir::ProjectionPlan::Values(property_names(vec![
                "$id", "$label", "name", "score", "missing",
            ])),
        )
        .await
        .expect("selected values projection succeeds");
    assert_projection_reads(&values_ctx, 1, 1, 0);

    let mut value_map_ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    value_map_ctx
        .project(
            ExecutionValue::Stream(vec![row.clone()]),
            &ir::ProjectionPlan::ValueMap(ir::PropertySelection::Selected(property_names(vec![
                "score", "missing",
            ]))),
        )
        .await
        .expect("selected value-map projection succeeds");
    assert_projection_reads(&value_map_ctx, 1, 1, 0);

    let mut project_ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let projection = ir::ProjectionPlan::Project(projection_items(vec![
        ir::ProjectionItem::Property {
            source: name("name"),
            alias: name("direct_name"),
        },
        ir::ProjectionItem::Expr {
            alias: name("score_expr"),
            expr: ir::ExprPlan::new(Expr::prop("score")).expect("valid property expression"),
        },
        ir::ProjectionItem::Expr {
            alias: name("case_expr"),
            expr: ir::ExprPlan::new(Expr::case(
                vec![(
                    helix_ast::expr::Predicate::HasKey {
                        property: "missing".to_string(),
                    },
                    Expr::val("unexpected"),
                )],
                Some(Expr::prop("$label")),
            ))
            .expect("valid case expression"),
        },
    ]));
    project_ctx
        .project(ExecutionValue::Stream(vec![row]), &projection)
        .await
        .expect("general projection succeeds");
    assert_projection_reads(&project_ctx, 1, 1, 0);
}

#[tokio::test]
async fn edge_projection_materializes_edge_endpoints_and_endpoint_nodes_once() {
    let db = test_support::open_db("projection-read-baseline-edge").await;
    let from = test_support::add_user(&db, "alice").await;
    let to = test_support::add_user(&db, "bob").await;
    let edge = test_support::add_edge_with_properties(
        &db,
        from,
        to,
        "KNOWS",
        vec![("since", PropertyValue::I64(2026))],
    )
    .await;
    let projection = ir::ProjectionPlan::Project(projection_items(
        vec![
            ("$from", "from"),
            ("$to", "to"),
            ("$from.$id", "from_id"),
            ("$to.$id", "to_id"),
            ("$from.name", "from_name"),
            ("$from.$label", "from_label"),
            ("$to.name", "to_name"),
            ("$to.$label", "to_label"),
            ("since", "since"),
            ("$label", "edge_label"),
        ]
        .into_iter()
        .map(|(source, alias)| ir::ProjectionItem::Property {
            source: name(source),
            alias: name(alias),
        })
        .collect(),
    ));
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    ctx.project(
        ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Edge(edge))]),
        &projection,
    )
    .await
    .expect("edge projection succeeds");

    assert_projection_reads(&ctx, 3, 3, 1);
}

#[tokio::test]
async fn inline_and_virtual_projection_fields_do_not_load_stored_properties() {
    let db = test_support::open_db("projection-inline-virtual-no-reads").await;
    let distance = name("$distance");
    let row = ExecutionRow::current_with_virtual_properties(
        ElementRef::Node(42),
        RowVirtualProperties::from_one(distance.clone(), DbPropertyValue::F64(0.25)),
    );
    let projection = ir::ProjectionPlan::Project(projection_items(vec![
        ir::ProjectionItem::Property {
            source: name("$id"),
            alias: name("id"),
        },
        ir::ProjectionItem::Property {
            source: distance,
            alias: name("distance"),
        },
        ir::ProjectionItem::Expr {
            alias: name("constant"),
            expr: ir::ExprPlan::new(Expr::val(7)).expect("valid constant expression"),
        },
    ]));
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    let result = ctx
        .project(ExecutionValue::Stream(vec![row]), &projection)
        .await
        .expect("inline projection succeeds");

    assert_eq!(
        result,
        ExecutionValue::Scalars(vec![ExecutionScalar::Object(BTreeMap::from([
            ("constant".to_string(), DbPropertyValue::I64(7)),
            ("distance".to_string(), DbPropertyValue::F64(0.25)),
            ("id".to_string(), DbPropertyValue::I64(42)),
        ]))])
    );
    assert_projection_reads(&ctx, 0, 0, 0);
}

#[tokio::test]
async fn missing_property_blobs_are_cached_only_for_the_current_row() {
    let db = test_support::open_db("projection-missing-blob-row-scope").await;
    let projection = ir::ProjectionPlan::Project(projection_items(vec![
        ir::ProjectionItem::Property {
            source: name("first"),
            alias: name("first"),
        },
        ir::ProjectionItem::Property {
            source: name("second"),
            alias: name("second"),
        },
    ]));
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    let result = ctx
        .project(
            ExecutionValue::Stream(vec![
                ExecutionRow::current(ElementRef::Node(u64::MAX)),
                ExecutionRow::current(ElementRef::Node(u64::MAX)),
            ]),
            &projection,
        )
        .await
        .expect("missing properties remain a valid projection");

    assert_eq!(
        result,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::Object(BTreeMap::new()),
            ExecutionScalar::Object(BTreeMap::new()),
        ])
    );
    assert_projection_reads(&ctx, 2, 0, 0);
}

#[tokio::test]
async fn missing_edge_endpoints_are_negative_cached_for_the_current_row() {
    let db = test_support::open_db("projection-missing-endpoint-row-scope").await;
    let projection = ir::ProjectionPlan::Project(projection_items(
        [
            ("$from", "from"),
            ("$from.$id", "from_id"),
            ("$from.name", "from_name"),
            ("$to", "to"),
            ("$to.$id", "to_id"),
            ("$to.name", "to_name"),
        ]
        .into_iter()
        .map(|(source, alias)| ir::ProjectionItem::Property {
            source: name(source),
            alias: name(alias),
        })
        .collect(),
    ));
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    let result = ctx
        .project(
            ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Edge(u64::MAX))]),
            &projection,
        )
        .await
        .expect("missing endpoints remain a valid projection");

    assert_eq!(
        result,
        ExecutionValue::Scalars(vec![ExecutionScalar::Object(BTreeMap::new())])
    );
    assert_projection_reads(&ctx, 0, 0, 1);
}

#[tokio::test]
async fn binding_projection_shares_stored_blobs_without_sharing_virtual_values() {
    let db = test_support::open_db("projection-binding-row-cache").await;
    let current = test_support::add_user(&db, "current").await;
    let other = test_support::add_user(&db, "other").await;
    let same_binding = name("same");
    let other_binding = name("other");
    let distance = name("$distance");
    let mut same_row = ExecutionRow::current_with_virtual_properties(
        ElementRef::Node(current),
        RowVirtualProperties::from_one(distance.clone(), DbPropertyValue::F64(0.25)),
    );
    same_row
        .bindings
        .insert(same_binding.clone(), ElementRef::Node(current));
    same_row.binding_virtual_properties.insert(
        same_binding.clone(),
        RowVirtualProperties::from_one(distance.clone(), DbPropertyValue::F64(0.75)),
    );
    let same_projection = ir::ProjectionPlan::ProjectBindings {
        projections: binding_projection_items(vec![
            ir::BindingProjectionPlan::Property {
                target: ir::BindingTargetPlan::Current,
                source: name("name"),
                alias: name("current_name"),
            },
            ir::BindingProjectionPlan::Property {
                target: ir::BindingTargetPlan::Binding(same_binding.clone()),
                source: name("name"),
                alias: name("bound_name"),
            },
            ir::BindingProjectionPlan::Property {
                target: ir::BindingTargetPlan::Current,
                source: distance.clone(),
                alias: name("current_distance"),
            },
            ir::BindingProjectionPlan::Property {
                target: ir::BindingTargetPlan::Binding(same_binding),
                source: distance.clone(),
                alias: name("bound_distance"),
            },
        ]),
        dedup: ir::ProjectionDedupMode::All,
    };
    let mut same_ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    let same_result = same_ctx
        .project(ExecutionValue::Stream(vec![same_row]), &same_projection)
        .await
        .expect("same-element binding projection succeeds");

    assert_eq!(
        same_result,
        ExecutionValue::Scalars(vec![ExecutionScalar::Object(BTreeMap::from([
            ("bound_distance".to_string(), DbPropertyValue::F64(0.75)),
            (
                "bound_name".to_string(),
                DbPropertyValue::String("current".to_string()),
            ),
            ("current_distance".to_string(), DbPropertyValue::F64(0.25),),
            (
                "current_name".to_string(),
                DbPropertyValue::String("current".to_string()),
            ),
        ]))])
    );
    assert_projection_reads(&same_ctx, 1, 1, 0);

    let mut distinct_row = ExecutionRow::current(ElementRef::Node(current));
    distinct_row
        .bindings
        .insert(other_binding.clone(), ElementRef::Node(other));
    let distinct_projection = ir::ProjectionPlan::ProjectBindings {
        projections: binding_projection_items(vec![
            ir::BindingProjectionPlan::Property {
                target: ir::BindingTargetPlan::Current,
                source: name("name"),
                alias: name("current_name"),
            },
            ir::BindingProjectionPlan::Property {
                target: ir::BindingTargetPlan::Binding(other_binding.clone()),
                source: name("name"),
                alias: name("other_name"),
            },
        ]),
        dedup: ir::ProjectionDedupMode::All,
    };
    let mut distinct_ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    distinct_ctx
        .project(
            ExecutionValue::Stream(vec![distinct_row]),
            &distinct_projection,
        )
        .await
        .expect("distinct-element binding projection succeeds");
    assert_projection_reads(&distinct_ctx, 2, 2, 0);

    let mut coalesce_row = ExecutionRow::current_with_virtual_properties(
        ElementRef::Node(current),
        RowVirtualProperties::from_one(distance.clone(), DbPropertyValue::F64(0.5)),
    );
    coalesce_row
        .bindings
        .insert(other_binding.clone(), ElementRef::Node(u64::MAX));
    let coalesce_projection = ir::ProjectionPlan::ProjectBindings {
        projections: binding_projection_items(vec![ir::BindingProjectionPlan::Coalesce {
            refs: binding_refs(vec![
                ir::BindingValueRefPlan {
                    target: ir::BindingTargetPlan::Current,
                    source: distance,
                },
                ir::BindingValueRefPlan {
                    target: ir::BindingTargetPlan::Binding(other_binding),
                    source: name("name"),
                },
            ]),
            alias: name("selected"),
        }]),
        dedup: ir::ProjectionDedupMode::All,
    };
    let mut coalesce_ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    coalesce_ctx
        .project(
            ExecutionValue::Stream(vec![coalesce_row]),
            &coalesce_projection,
        )
        .await
        .expect("coalesce projection succeeds");
    assert_projection_reads(&coalesce_ctx, 0, 0, 0);
}

#[tokio::test]
async fn row_cache_stays_typed_and_snapshot_local_during_concurrent_mutation() {
    let db = test_support::open_db("projection-cache-snapshot-local").await;
    let id = test_support::add_user(&db, "before").await;
    let projection = ir::ProjectionPlan::Project(projection_items(vec![
        ir::ProjectionItem::Property {
            source: name("name"),
            alias: name("first"),
        },
        ir::ProjectionItem::Property {
            source: name("name"),
            alias: name("second"),
        },
    ]));
    let mut snapshot_ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    snapshot_ctx
        .enable_request_read_view()
        .await
        .expect("projection request snapshot opens");

    let key = crate::encoding::keys::DataKey::Data {
        scope: crate::encoding::keys::scope::DataScope::LegacyUnscoped,
        kind: crate::encoding::keys::DataKeyKind::NodeProperty(
            crate::encoding::keys::NodePropertyKey::new(id),
        ),
    }
    .to_bytes();
    db.inner_db()
        .put(
            key,
            crate::encoding::property::encode_properties(&[
                crate::encoding::property::Property::string("name", "after"),
            ]),
        )
        .await
        .expect("concurrent property mutation commits");

    let snapshotted = snapshot_ctx
        .project(
            ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Node(id))]),
            &projection,
        )
        .await
        .expect("snapshotted projection succeeds");
    assert_eq!(
        snapshotted,
        ExecutionValue::Scalars(vec![ExecutionScalar::Object(BTreeMap::from([
            (
                "first".to_string(),
                DbPropertyValue::String("before".to_string()),
            ),
            (
                "second".to_string(),
                DbPropertyValue::String("before".to_string()),
            ),
        ]))])
    );
    assert_projection_reads(&snapshot_ctx, 1, 1, 0);
    snapshot_ctx
        .close_request_read_view()
        .expect("projection request snapshot closes");

    let mut following_ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let following = following_ctx
        .project(
            ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Node(id))]),
            &projection,
        )
        .await
        .expect("following projection succeeds");
    assert_eq!(
        following,
        ExecutionValue::Scalars(vec![ExecutionScalar::Object(BTreeMap::from([
            (
                "first".to_string(),
                DbPropertyValue::String("after".to_string()),
            ),
            (
                "second".to_string(),
                DbPropertyValue::String("after".to_string()),
            ),
        ]))])
    );
    assert_projection_reads(&following_ctx, 1, 1, 0);
}

#[tokio::test]
async fn typed_element_cache_keys_keep_equal_node_and_edge_ids_distinct() {
    let db = test_support::open_db("projection-typed-element-cache-key").await;
    let id = 7;
    let scope = crate::encoding::keys::scope::DataScope::LegacyUnscoped;
    let node_key = crate::encoding::keys::DataKey::Data {
        scope,
        kind: crate::encoding::keys::DataKeyKind::NodeProperty(
            crate::encoding::keys::NodePropertyKey::new(id),
        ),
    }
    .to_bytes();
    let edge_key = crate::encoding::keys::DataKey::Data {
        scope,
        kind: crate::encoding::keys::DataKeyKind::EdgePropertyById(
            crate::encoding::keys::EdgePropertyByIdKey::new(id),
        ),
    }
    .to_bytes();
    let raw = db.inner_db();
    raw.put(
        node_key,
        crate::encoding::property::encode_properties(&[
            crate::encoding::property::Property::string("kind", "node"),
        ]),
    )
    .await
    .expect("node property blob writes");
    raw.put(
        edge_key,
        crate::encoding::property::encode_properties(&[
            crate::encoding::property::Property::string("kind", "edge"),
        ]),
    )
    .await
    .expect("edge property blob writes");

    let edge_binding = name("edge");
    let mut row = ExecutionRow::current(ElementRef::Node(id));
    row.bindings
        .insert(edge_binding.clone(), ElementRef::Edge(id));
    let projection = ir::ProjectionPlan::ProjectBindings {
        projections: binding_projection_items(vec![
            ir::BindingProjectionPlan::Property {
                target: ir::BindingTargetPlan::Current,
                source: name("kind"),
                alias: name("node_kind"),
            },
            ir::BindingProjectionPlan::Property {
                target: ir::BindingTargetPlan::Binding(edge_binding),
                source: name("kind"),
                alias: name("edge_kind"),
            },
        ]),
        dedup: ir::ProjectionDedupMode::All,
    };
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    let result = ctx
        .project(ExecutionValue::Stream(vec![row]), &projection)
        .await
        .expect("typed-element projection succeeds");

    assert_eq!(
        result,
        ExecutionValue::Scalars(vec![ExecutionScalar::Object(BTreeMap::from([
            (
                "edge_kind".to_string(),
                DbPropertyValue::String("edge".to_string()),
            ),
            (
                "node_kind".to_string(),
                DbPropertyValue::String("node".to_string()),
            ),
        ]))])
    );
    assert_projection_reads(&ctx, 2, 2, 0);
}

#[tokio::test]
async fn corrupt_property_blob_errors_propagate_through_materializers() {
    let db = test_support::open_db("projection-corrupt-property-blob").await;
    let id = 9;
    let key = crate::encoding::keys::DataKey::Data {
        scope: crate::encoding::keys::scope::DataScope::LegacyUnscoped,
        kind: crate::encoding::keys::DataKeyKind::NodeProperty(
            crate::encoding::keys::NodePropertyKey::new(id),
        ),
    }
    .to_bytes();
    db.inner_db()
        .put(key, bytes::Bytes::from_static(b"corrupt"))
        .await
        .expect("corrupt property blob writes");
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let row = || ExecutionRow::current(ElementRef::Node(id));

    for projection in [
        ir::ProjectionPlan::Values(property_names(vec!["first"])),
        ir::ProjectionPlan::ValueMap(ir::PropertySelection::All),
        ir::ProjectionPlan::ValueMap(ir::PropertySelection::Selected(property_names(vec![
            "first",
        ]))),
        ir::ProjectionPlan::Project(projection_items(vec![ir::ProjectionItem::Property {
            source: name("first"),
            alias: name("first"),
        }])),
        ir::ProjectionPlan::ProjectBindings {
            projections: binding_projection_items(vec![ir::BindingProjectionPlan::Property {
                target: ir::BindingTargetPlan::Current,
                source: name("first"),
                alias: name("first"),
            }]),
            dedup: ir::ProjectionDedupMode::All,
        },
        ir::ProjectionPlan::Label,
    ] {
        assert!(matches!(
            ctx.project(ExecutionValue::Stream(vec![row()]), &projection)
                .await,
            Err(HelixDbError::Encoding(_))
        ));
    }

    assert_projection_reads(&ctx, 6, 6, 0);
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
async fn edge_properties_projection_includes_system_fields_and_skips_invalid_rows() {
    let db = test_support::open_db("projection-edge-properties-filter").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let edge = test_support::add_edge_with_properties(
        &db,
        alice,
        bob,
        "KNOWS",
        vec![
            ("since", PropertyValue::I64(2026)),
            ("$id", PropertyValue::I64(-1)),
            ("$from", PropertyValue::I64(-1)),
            ("$to", PropertyValue::I64(-1)),
        ],
    )
    .await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    let ExecutionValue::Scalars(values) = ctx
        .project(
            ExecutionValue::Stream(vec![
                ExecutionRow::current(ElementRef::Node(alice)),
                ExecutionRow::empty(),
                ExecutionRow::current(ElementRef::Edge(u64::MAX)),
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
            ("$from".to_string(), DbPropertyValue::I64(alice as i64)),
            ("$id".to_string(), DbPropertyValue::I64(edge as i64)),
            (
                "$label".to_string(),
                DbPropertyValue::String("KNOWS".to_string()),
            ),
            ("$to".to_string(), DbPropertyValue::I64(bob as i64)),
            ("since".to_string(), DbPropertyValue::I64(2026)),
        ])]
    );
    assert_projection_reads(&ctx, 1, 1, 2);
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
