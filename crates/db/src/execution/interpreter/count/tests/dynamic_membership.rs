use super::*;

#[tokio::test]
async fn dynamic_membership_counts_match_rows_for_indexed_and_authoritative_domains() {
    let db = test_support::open_db_with_config(
        test_support::in_memory_config("count-dynamic-membership")
            .with_equality_index("User", "status")
            .with_edge_equality_index("FOLLOWS", "status"),
    )
    .await;
    let a = test_support::add_node_with_properties(
        &db,
        "User",
        vec![("status", PropertyValue::from("active"))],
    )
    .await;
    let b = test_support::add_node_with_properties(
        &db,
        "User",
        vec![("status", PropertyValue::from("paused"))],
    )
    .await;
    test_support::add_node_with_properties(&db, "User", vec![]).await;
    test_support::add_node_with_properties(
        &db,
        "Other",
        vec![("status", PropertyValue::from("active"))],
    )
    .await;
    for props in [
        vec![("status", PropertyValue::from("active"))],
        vec![("status", PropertyValue::from("paused"))],
        vec![],
    ] {
        test_support::add_edge_with_properties(&db, a, b, "FOLLOWS", props).await;
    }
    test_support::add_edge_with_properties(
        &db,
        a,
        b,
        "OTHER",
        vec![("status", PropertyValue::from("active"))],
    )
    .await;
    let node_index = catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:User:status"));
    let edge_index =
        catalog::EdgeEqualityIndexMeta::new(test_support::name("edge_eq:FOLLOWS:status"));
    let node_key = catalog::ScopedPropertyKey::try_new("User", "status").unwrap();
    let edge_key = catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap();
    for (value, expected) in [
        (
            PropertyValue::StringArray(vec!["active".into(), "active".into(), "paused".into()]),
            2,
        ),
        (PropertyValue::StringArray(vec![]), 0),
        (PropertyValue::Null, 1),
        (PropertyValue::from("active"), 1),
        (PropertyValue::StringArray(vec!["absent".into()]), 0),
    ] {
        for max_values in [1, 8] {
            let values = ir::RuntimeEqualitySet::new(
                test_support::name("domain"),
                std::num::NonZeroUsize::new(max_values).unwrap(),
            );
            let params = context::ParamBindings::default()
                .with_value(test_support::name("domain"), value.clone());
            let direct = [
                exec::ExecCountPlan::NodeDynamicMembership(
                    exec::ExecNodeDynamicMembershipCountPlan {
                        index: node_index.clone(),
                        key: node_key.clone(),
                        values: values.clone(),
                        window: bounded(1, 1),
                    },
                ),
                exec::ExecCountPlan::EdgeDynamicMembership(
                    exec::ExecEdgeDynamicMembershipCountPlan {
                        index: edge_index.clone(),
                        key: edge_key.clone(),
                        values: values.clone(),
                        window: bounded(1, 1),
                    },
                ),
            ];
            for plan in direct {
                assert_eq!(
                    execute_direct_count_with_params(&db, plan, params.clone())
                        .await
                        .unwrap(),
                    ExecutionValue::Count(usize::from(expected > 1))
                );
            }
            let cursors = [
                exec::ExecCountCursorPlan::NodeDynamicMembership {
                    index: node_index.clone(),
                    key: node_key.clone(),
                    values: values.clone(),
                },
                exec::ExecCountCursorPlan::EdgeDynamicMembership {
                    index: edge_index.clone(),
                    key: edge_key.clone(),
                    values: values.clone(),
                },
            ];
            for write_view in [false, true] {
                let mut execution = ExecutionContext::new(&db, params.clone());
                if write_view {
                    execution.enable_request_write_scope().await.unwrap();
                } else {
                    execution.enable_request_read_view().await.unwrap();
                }
                // The row access and count programs must agree even when a
                // runtime domain crosses the authoritative-fallback boundary.
                for access in [
                    exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::SecondarySet {
                        set: exec::ExecNodeSecondarySetPlan::DynamicMembership {
                            index: node_index.clone(),
                            key: node_key.clone(),
                            values: values.clone(),
                        },
                    }),
                    exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::SecondarySet {
                        set: exec::ExecEdgeSecondarySetPlan::DynamicMembership {
                            index: edge_index.clone(),
                            key: edge_key.clone(),
                            values: values.clone(),
                        },
                    }),
                ] {
                    let ExecutionValue::Stream(rows) =
                        execution.execute_access(&access).await.unwrap()
                    else {
                        panic!("secondary access must return rows");
                    };
                    assert_eq!(rows.len(), expected);
                }
                for cursor in &cursors {
                    let rows = execution.count_cursor(cursor, &mut None).await.unwrap();
                    assert_eq!(rows.len(), expected);
                    let count = execution
                        .count_cursor_cardinality(
                            cursor,
                            &mut None,
                            EvaluatedCountWindow {
                                skip: 0,
                                take: None,
                            },
                        )
                        .await
                        .unwrap();
                    assert_eq!(count, expected);
                }
                if write_view {
                    execution.abort_request_write_scope();
                } else {
                    execution.close_request_read_view().unwrap();
                }
            }
        }
    }
    let values = ir::RuntimeEqualitySet::new(
        test_support::name("missing"),
        std::num::NonZeroUsize::new(2).unwrap(),
    );
    let mut execution = ExecutionContext::new(&db, context::ParamBindings::default());
    execution.enable_request_read_view().await.unwrap();
    for cursor in [
        exec::ExecCountCursorPlan::NodeDynamicMembership {
            index: node_index.clone(),
            key: node_key.clone(),
            values: values.clone(),
        },
        exec::ExecCountCursorPlan::EdgeDynamicMembership {
            index: edge_index.clone(),
            key: edge_key.clone(),
            values: values.clone(),
        },
        exec::ExecCountCursorPlan::NodeDynamicMembership {
            index: node_index,
            key: edge_key,
            values: values.clone(),
        },
        exec::ExecCountCursorPlan::EdgeDynamicMembership {
            index: edge_index,
            key: node_key,
            values,
        },
    ] {
        assert!(execution.count_cursor(&cursor, &mut None).await.is_err());
        assert!(execution
            .count_cursor_cardinality(
                &cursor,
                &mut None,
                EvaluatedCountWindow {
                    skip: 0,
                    take: None
                }
            )
            .await
            .is_err());
    }
    execution.close_request_read_view().unwrap();
    db.close().await.unwrap();
}
