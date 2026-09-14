use super::super::*;
use crate::encoding::v2::{
    keys,
    values::property::{self, property_value::PropertyValue as P, Property},
};
use crate::execution::interpreter::test_support;
use helix_planner::context;
use serde_json::json;

#[derive(Clone, Copy, Debug)]
enum Subject {
    Node,
    Edge,
    Detach,
}

#[tokio::test]
async fn deletion_avoids_unneeded_native_array_conversion_and_retains_relationship_types() {
    for subject in [Subject::Node, Subject::Edge, Subject::Detach] {
        let db = test_support::open_db("deletion-selective-hydration").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let properties = if matches!(subject, Subject::Edge) {
            vec![]
        } else {
            vec![Property::new("large", P::I64Array(vec![1; 16 * 1024]))]
        };
        let node = ctx.row_create_node("Source", properties).await.unwrap();
        let (key, text, expected, remaining) = match subject {
            Subject::Node => (
                keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(node)),
                "MATCH (n:Source) DELETE n RETURN 1",
                json!(1),
                0,
            ),
            Subject::Edge | Subject::Detach => {
                let endpoint = ctx.row_create_node("Endpoint", vec![]).await.unwrap();
                let properties = if matches!(subject, Subject::Edge) {
                    vec![Property::new("large", P::I64Array(vec![1; 16 * 1024]))]
                } else {
                    vec![]
                };
                let edge = ctx
                    .row_create_edge(node, endpoint, "R", properties)
                    .await
                    .unwrap();
                match subject {
                    Subject::Edge => (
                        keys::DataKeyKind::EdgePropertyById(keys::EdgePropertyByIdKey::new(edge)),
                        "MATCH (n:Source)-[r:R]->() DELETE r RETURN type(r)",
                        json!("R"),
                        2,
                    ),
                    Subject::Detach => (
                        keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(node)),
                        "MATCH (n:Source)-[r:R]->() DETACH DELETE n RETURN type(r)",
                        json!("R"),
                        1,
                    ),
                    Subject::Node => unreachable!("node case handled above"),
                }
            }
        };
        let key = ctx.storage_key(key);
        let bytes = db.inner_db().get(&key).await.unwrap().unwrap();
        let encoded_length = bytes.len();
        assert!(property::decode_properties(&bytes)
            .unwrap()
            .iter()
            .any(|property| property.name == "large"));
        drop(bytes);
        drop(ctx);
        // Canonical archive validation and native index snapshots remain
        // necessary. Expanding an unrelated array into Cypher values does not.
        let result = crate::cypher::execute(
            &db,
            crate::cypher::Request::new(text),
            keys::scope::DataScope::LegacyUnscoped,
            crate::query_service::QueryMode::Execute,
            crate::execution_control::ExecutionControl::unlimited(),
            Limits {
                memory_bytes: encoded_length * 3 + 128 * 1024,
                ..Default::default()
            },
        )
        .await
        .unwrap_or_else(|error| panic!("{subject:?}: {error:?}"));
        assert_eq!(result.rows, vec![vec![expected]]);
        assert!(db.inner_db().get(&key).await.unwrap().is_none());
        let nodes = db
            .cypher(crate::cypher::Request::new("MATCH (n) RETURN count(*)"))
            .await
            .unwrap();
        assert_eq!(nodes.rows, vec![vec![json!(remaining)]]);
        let edges = db
            .cypher(crate::cypher::Request::new(
                "MATCH ()-[r]->() RETURN count(*)",
            ))
            .await
            .unwrap();
        assert_eq!(edges.rows, vec![vec![json!(0)]]);
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn deletion_expressions_keep_their_selected_and_dynamic_property_requirements() {
    for property in ["a.choice", "a[$key]"] {
        let db = test_support::open_db("deletion-expression-hydration").await;
        db.cypher(crate::cypher::Request::new(
            "CREATE (:A {choice:1}), (:B), (:C)",
        ))
        .await
        .unwrap();
        let mut request = crate::cypher::Request::new(format!("MATCH (a:A),(b:B),(c:C) DELETE CASE WHEN {property}=1 THEN b ELSE c END RETURN a.choice"));
        request.parameters.insert(
            "key".into(),
            helix_ast::query::QueryValue::String("choice".into()),
        );
        let result = db.cypher(request).await.unwrap();
        assert_eq!(result.rows, vec![vec![json!(1)]]);
        let after = db
            .cypher(crate::cypher::Request::new(
                "MATCH (n) RETURN labels(n) ORDER BY labels(n)",
            ))
            .await
            .unwrap();
        assert_eq!(after.rows, vec![vec![json!(["A"])], vec![json!(["C"])]]);
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn cancelled_direct_hydration_precedes_entity_demand_allocation() {
    use futures::FutureExt;
    let db = test_support::open_db("cancelled-direct-hydration").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(0));
    let rows = [vec![r::Value::Entity(r::Entity::Node(1))]];
    let demand = BTreeMap::from([(r::Slot(0), r::PropertyDemand::All)]);
    ctx.fail_deadline_after(0);
    let (result, allocated) = crate::allocation_testing::observe(|| {
        ctx.graph_batch_required(&rows, &demand).now_or_never()
    });
    assert!(matches!(
        result,
        Some(Err(Error::Storage(
            crate::HelixDbError::QueryDeadlineExceeded
        )))
    ));
    assert_eq!(allocated.allocations, 0);
    assert_eq!(ctx.row_budget().available(), 0);
    drop(ctx);
    db.close().await.unwrap();
}
