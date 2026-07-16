//! Graph element row materialization.

use helix_planner::ir;

use super::super::{
    ElementRef, ExecutionContext, ExecutionRow, ExecutionValue, RowVirtualProperties,
};
use crate::encoding::keys;
use crate::encoding::property::property_value::PropertyValue as DbPropertyValue;
use crate::error::Result;
use crate::search::vector::{DistanceOutputVersion, TypedVectorSearchResult, VectorEntityId};

impl<'db> ExecutionContext<'db> {
    pub(super) async fn node_rows(&self, ids: Vec<u64>) -> Result<ExecutionValue> {
        let mut rows = Vec::new();
        for id in ids {
            let key = keys::Key::Data {
                scope: self.tenant_scope,
                kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(id)),
            }
            .to_bytes();
            if self.get_raw(&key).await?.is_some() {
                rows.push(ExecutionRow::current(ElementRef::Node(id)));
            }
        }
        Ok(ExecutionValue::Stream(rows))
    }

    pub(super) async fn edge_rows(&self, ids: Vec<u64>) -> Result<ExecutionValue> {
        let mut rows = Vec::new();
        for id in ids {
            let key = keys::Key::Data {
                scope: self.tenant_scope,
                kind: keys::DataKeyKind::EdgeEndpoints(keys::EdgeEndpointsKey::new(id)),
            }
            .to_bytes();
            if self.get_raw(&key).await?.is_some() {
                rows.push(ExecutionRow::current(ElementRef::Edge(id)));
            }
        }
        Ok(ExecutionValue::Stream(rows))
    }

    pub(super) async fn node_search_rows(
        &self,
        results: Vec<TypedVectorSearchResult>,
    ) -> Result<ExecutionValue> {
        let mut rows = Vec::new();
        for result in results {
            let VectorEntityId::Node(entity_id) = result.entity_id() else {
                return Err(crate::error::HelixDbError::InvariantViolation(
                    "edge-bound vector result reached node row materialization".to_string(),
                ));
            };
            let key = keys::Key::Data {
                scope: self.tenant_scope,
                kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(entity_id)),
            }
            .to_bytes();
            if self.get_raw(&key).await?.is_some() {
                rows.push(search_row(ElementRef::Node(entity_id), result));
            }
        }
        Ok(ExecutionValue::Stream(rows))
    }

    pub(super) async fn edge_search_rows(
        &self,
        results: Vec<TypedVectorSearchResult>,
    ) -> Result<ExecutionValue> {
        let mut rows = Vec::new();
        for result in results {
            let VectorEntityId::Edge(entity_id) = result.entity_id() else {
                return Err(crate::error::HelixDbError::InvariantViolation(
                    "node-bound vector result reached edge row materialization".to_string(),
                ));
            };
            let key = keys::Key::Data {
                scope: self.tenant_scope,
                kind: keys::DataKeyKind::EdgeEndpoints(keys::EdgeEndpointsKey::new(entity_id)),
            }
            .to_bytes();
            if self.get_raw(&key).await?.is_some() {
                rows.push(search_row(ElementRef::Edge(entity_id), result));
            }
        }
        Ok(ExecutionValue::Stream(rows))
    }
}

fn search_row(element: ElementRef, result: TypedVectorSearchResult) -> ExecutionRow {
    let distance = result.materialize_distance(DistanceOutputVersion::CurrentScore);
    ExecutionRow::current_with_virtual_properties(
        element,
        RowVirtualProperties::from_one(
            ir::NonEmptyString::new("$distance").expect("distance virtual property is non-empty"),
            DbPropertyValue::F64(distance.value() as f64),
        ),
    )
}

#[cfg(test)]
mod tests {
    use helix_planner::context;

    use super::super::super::test_support;
    use super::*;
    use crate::encoding::v1::values::vector_generation::{ActiveScoreSemantic, VectorEntityKind};
    use crate::search::vector::{DistanceScore, SearchResult};

    fn vector_result(kind: VectorEntityKind, entity_id: u64) -> TypedVectorSearchResult {
        TypedVectorSearchResult::from_physical(
            kind,
            ActiveScoreSemantic::ManhattanF32V1,
            SearchResult::new(entity_id, DistanceScore::try_new(0.25).unwrap()),
        )
    }

    fn current_node_ids(value: ExecutionValue) -> Vec<u64> {
        let ExecutionValue::Stream(rows) = value else {
            panic!("row materialization should return a stream");
        };
        rows.into_iter()
            .map(|row| match row.current {
                Some(ElementRef::Node(id)) => id,
                Some(ElementRef::Edge(id)) => panic!("expected node row, got edge {id}"),
                None => panic!("materialized node row should expose the current element"),
            })
            .collect()
    }

    fn current_edge_ids(value: ExecutionValue) -> Vec<u64> {
        let ExecutionValue::Stream(rows) = value else {
            panic!("row materialization should return a stream");
        };
        rows.into_iter()
            .map(|row| match row.current {
                Some(ElementRef::Edge(id)) => id,
                Some(ElementRef::Node(id)) => panic!("expected edge row, got node {id}"),
                None => panic!("materialized edge row should expose the current element"),
            })
            .collect()
    }

    #[tokio::test]
    async fn node_rows_materialize_existing_ids_in_input_order() {
        let db = test_support::open_db("access-node-row-materialization").await;
        let alice = test_support::add_user(&db, "alice").await;
        let bob = test_support::add_user(&db, "bob").await;
        let ctx = ExecutionContext::new(&db, context::ParamBindings::default());

        let rows = ctx
            .node_rows(vec![bob, u64::MAX, alice])
            .await
            .expect("node rows materialize");

        assert_eq!(current_node_ids(rows), vec![bob, alice]);
    }

    #[tokio::test]
    async fn edge_rows_materialize_existing_ids_in_input_order() {
        let db = test_support::open_db("access-edge-row-materialization").await;
        let alice = test_support::add_user(&db, "alice").await;
        let bob = test_support::add_user(&db, "bob").await;
        let carol = test_support::add_user(&db, "carol").await;
        let follows = test_support::add_edge(&db, alice, bob, "FOLLOWS").await;
        let knows = test_support::add_edge(&db, bob, carol, "KNOWS").await;
        let ctx = ExecutionContext::new(&db, context::ParamBindings::default());

        let rows = ctx
            .edge_rows(vec![knows, u64::MAX, follows])
            .await
            .expect("edge rows materialize");

        assert_eq!(current_edge_ids(rows), vec![knows, follows]);
    }

    #[tokio::test]
    async fn vector_search_rows_enforce_the_bound_entity_kind() {
        let db = test_support::open_db("typed-vector-row-materialization").await;
        let alice = test_support::add_user(&db, "alice").await;
        let bob = test_support::add_user(&db, "bob").await;
        let follows = test_support::add_edge(&db, alice, bob, "FOLLOWS").await;
        let ctx = ExecutionContext::new(&db, context::ParamBindings::default());

        let nodes = ctx
            .node_search_rows(vec![vector_result(VectorEntityKind::Node, alice)])
            .await
            .unwrap();
        assert_eq!(current_node_ids(nodes), vec![alice]);

        let edges = ctx
            .edge_search_rows(vec![vector_result(VectorEntityKind::Edge, follows)])
            .await
            .unwrap();
        assert_eq!(current_edge_ids(edges), vec![follows]);

        assert!(matches!(
            ctx.node_search_rows(vec![vector_result(VectorEntityKind::Edge, follows)])
                .await,
            Err(crate::error::HelixDbError::InvariantViolation(_))
        ));
        assert!(matches!(
            ctx.edge_search_rows(vec![vector_result(VectorEntityKind::Node, alice)])
                .await,
            Err(crate::error::HelixDbError::InvariantViolation(_))
        ));
    }
}
