use super::super::*;
use crate::encoding::v2::{keys, values};
use crate::execution::interpreter::test_support;
use helix_planner::context;
use serde_json::json;

#[tokio::test]
async fn label_completion_preserves_stale_owner_filtering_scopes_paths_and_windows() {
    let db = test_support::open_db("cypher-label-completion-model").await;
    let scopes = [
        keys::DataScope::LegacyUnscoped,
        keys::DataScope::Tenant(keys::TenantId::from_u128(1)),
    ];
    for (index, scope) in scopes.into_iter().enumerate() {
        for (id, label, key) in [(512, "N", 10), (513, "Other", 20), (514, "N", 30)] {
            db.inner_db()
                .put(
                    keys::DataKey::Data {
                        scope,
                        kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(id)),
                    }
                    .to_bytes(),
                    values::property::encode_properties(&[
                        values::property::Property::string("$label", label),
                        values::property::Property::new(
                            "key",
                            values::property::property_value::PropertyValue::I64(
                                key + index as i64 * 100,
                            ),
                        ),
                    ]),
                )
                .await
                .unwrap();
        }
        for (label, end) in [("N", 515), ("Empty", 512)] {
            let ids = (0..end).chain([u64::MAX]).collect();
            db.inner_db()
                .put(
                    keys::DataKey::Data {
                        scope,
                        kind: keys::DataKeyKind::PropertyIndex(
                            keys::indexes::PropertyIndexKey::Equality(
                                keys::indexes::equality::EqualityIndexKey::new(
                                    keys::indexes::hash_property_name("$label"),
                                    keys::indexes::hash_property_value(label),
                                ),
                            ),
                        ),
                    }
                    .to_bytes(),
                    values::indexes::SecondaryEqualityBitmapValue::new(ids).encode(),
                )
                .await
                .unwrap();
        }
    }
    for (index, scope) in scopes.into_iter().enumerate() {
        let first = 10 + index * 100;
        let last = 30 + index * 100;
        for (text, expected) in [
            (
                "MATCH (n:N) RETURN n.key ORDER BY n.key",
                vec![vec![json!(first)], vec![json!(last)]],
            ),
            (
                "MATCH (n:N) WITH n SKIP 1 LIMIT 1 RETURN n.key",
                vec![vec![json!(last)]],
            ),
            ("OPTIONAL MATCH (n:Empty) RETURN n", vec![vec![json!(null)]]),
            (
                "OPTIONAL MATCH (n:Empty) WHERE 1/0=0 RETURN n",
                vec![vec![json!(null)]],
            ),
            ("MATCH (n:Empty) WHERE 1/0=0 RETURN n", vec![]),
            (
                "OPTIONAL MATCH (n:N) WHERE n.key<0 RETURN n.key",
                vec![vec![json!(null)]],
            ),
            (
                "MATCH p=(n:N) RETURN head(nodes(p)).key,length(p) ORDER BY head(nodes(p)).key",
                vec![vec![json!(first), json!(0)], vec![json!(last), json!(0)]],
            ),
            (
                "MATCH (n:N),(n:N) RETURN n.key ORDER BY n.key",
                vec![vec![json!(first)], vec![json!(last)]],
            ),
        ] {
            let query = helix_cypher::compile(text).unwrap();
            let selected = r::plan(
                query.clone(),
                &db.planner_context(context::ParamBindings::default()),
            )
            .unwrap();
            for plan in [
                selected.clone(),
                selected.with_execution(r::RowExecution::Materialized),
                r::RowPlan::reference(query).unwrap(),
            ] {
                for batch_rows in [1, 512] {
                    let response =
                        Interpreter::new_scoped(&db, context::ParamBindings::default(), scope)
                            .execute_rows(
                                &plan,
                                &BTreeMap::new(),
                                Limits {
                                    batch_rows,
                                    ..Default::default()
                                },
                            )
                            .await
                            .unwrap_or_else(|error| {
                                panic!("{text}; scope {index}; batch {batch_rows}: {error}")
                            });
                    assert_eq!(
                        response.rows, expected,
                        "{text}; scope {index}; batch {batch_rows}"
                    );
                }
            }
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn label_completion_preserves_corruption_errors_and_mutation_rollback() {
    let db = test_support::open_db("cypher-label-completion-errors").await;
    let response = db
        .cypher(crate::cypher::Request::new("CREATE (n:N {key:1}) RETURN n"))
        .await
        .unwrap();
    let id = response.rows[0][0]["id"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    for text in [
        "MATCH (n:N) WHERE 1/0=0 RETURN n",
        "MATCH (n:N) SET n.key=2 RETURN 1/0",
        "MATCH (n:N) DELETE n RETURN n.key",
    ] {
        let expected = if text.contains("DELETE") {
            "DeletedEntityAccess"
        } else {
            "DivisionByZero"
        };
        let query = helix_cypher::compile(text).unwrap();
        let selected = r::plan(
            query.clone(),
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        for plan in [
            selected.clone(),
            selected.with_execution(r::RowExecution::Materialized),
            r::RowPlan::reference(query).unwrap(),
        ] {
            let result = Interpreter::new(&db, context::ParamBindings::default())
                .execute_rows(&plan, &BTreeMap::new(), Limits::default())
                .await;
            assert!(
                matches!(result, Err(Error::Query(error)) if error.detail == expected),
                "{text}"
            );
            assert_eq!(
                db.cypher(crate::cypher::Request::new("MATCH (n:N) RETURN n.key"))
                    .await
                    .unwrap()
                    .rows,
                vec![vec![json!(1)]]
            );
        }
    }
    db.inner_db()
        .put(
            keys::DataKey::Data {
                scope: keys::DataScope::LegacyUnscoped,
                kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(id)),
            }
            .to_bytes(),
            bytes::Bytes::from_static(b"corrupt node properties"),
        )
        .await
        .unwrap();
    let query = helix_cypher::compile("MATCH (n:N) RETURN count(*)").unwrap();
    let selected = r::plan(
        query.clone(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    for plan in [
        selected.clone(),
        selected.with_execution(r::RowExecution::Materialized),
        r::RowPlan::reference(query).unwrap(),
    ] {
        let result = Interpreter::new(&db, context::ParamBindings::default())
            .execute_rows(&plan, &BTreeMap::new(), Limits::default())
            .await;
        assert!(matches!(
            result,
            Err(Error::Storage(crate::HelixDbError::Encoding(_)))
        ));
    }
    db.inner_db()
        .put(
            keys::DataKey::Data {
                scope: keys::DataScope::LegacyUnscoped,
                kind: keys::DataKeyKind::PropertyIndex(keys::indexes::PropertyIndexKey::Equality(
                    keys::indexes::equality::EqualityIndexKey::new(
                        keys::indexes::hash_property_name("$label"),
                        keys::indexes::hash_property_value("N"),
                    ),
                )),
            }
            .to_bytes(),
            bytes::Bytes::from_static(b"corrupt label bitmap"),
        )
        .await
        .unwrap();
    for text in ["MATCH (n:N) RETURN n", "MATCH (n:N) RETURN n LIMIT 0"] {
        let result = db.cypher(crate::cypher::Request::new(text)).await;
        assert!(
            matches!(
                result,
                Err(Error::Storage(crate::HelixDbError::Encoding(_)))
            ),
            "{text}: {result:?}"
        );
    }
    db.close().await.unwrap();
}
