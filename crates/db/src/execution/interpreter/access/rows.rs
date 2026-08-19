//! Graph element row materialization.

use helix_planner::ir;

use super::super::{
    ElementRef, ExecutionContext, ExecutionRow, ExecutionValue, RowVirtualProperties,
};
use crate::encoding::keys;
use crate::encoding::property::property_value::PropertyValue as DbPropertyValue;
use crate::error::Result;
use crate::search::text::TextSearchHit;
use crate::search::vector::{DistanceOutputVersion, TypedVectorSearchResult, VectorEntityId};

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) fn verified_node_rows(
        &self,
        ids: Vec<u64>,
    ) -> Result<ExecutionValue> {
        ids.iter()
            .try_for_each(|_| self.check_execution_deadline())?;
        Ok(ExecutionValue::Stream(
            ids.into_iter()
                .map(|id| ExecutionRow::current(ElementRef::Node(id)))
                .collect(),
        ))
    }

    pub(in crate::execution::interpreter) fn verified_edge_rows(
        &self,
        ids: Vec<u64>,
    ) -> Result<ExecutionValue> {
        ids.iter()
            .try_for_each(|_| self.check_execution_deadline())?;
        Ok(ExecutionValue::Stream(
            ids.into_iter()
                .map(|id| ExecutionRow::current(ElementRef::Edge(id)))
                .collect(),
        ))
    }

    pub(in crate::execution::interpreter) async fn node_rows(
        &self,
        ids: Vec<u64>,
    ) -> Result<ExecutionValue> {
        self.node_row_vec(ids).await.map(ExecutionValue::Stream)
    }

    pub(in crate::execution::interpreter) async fn node_row_vec(
        &self,
        ids: Vec<u64>,
    ) -> Result<Vec<ExecutionRow>> {
        let mut rows = Vec::new();
        for id in ids {
            self.check_execution_deadline()?;
            let key = keys::Key::Data {
                scope: self.tenant_scope,
                kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(id)),
            }
            .to_bytes();
            if self.get_raw(&key).await?.is_some() {
                rows.push(ExecutionRow::current(ElementRef::Node(id)));
            }
        }
        Ok(rows)
    }

    pub(in crate::execution::interpreter) async fn edge_rows(
        &self,
        ids: Vec<u64>,
    ) -> Result<ExecutionValue> {
        self.edge_row_vec(ids).await.map(ExecutionValue::Stream)
    }

    pub(in crate::execution::interpreter) async fn edge_row_vec(
        &self,
        ids: Vec<u64>,
    ) -> Result<Vec<ExecutionRow>> {
        let mut rows = Vec::new();
        for id in ids {
            self.check_execution_deadline()?;
            let key = keys::Key::Data {
                scope: self.tenant_scope,
                kind: keys::DataKeyKind::EdgeEndpoints(keys::EdgeEndpointsKey::new(id)),
            }
            .to_bytes();
            if self.get_raw(&key).await?.is_some() {
                rows.push(ExecutionRow::current(ElementRef::Edge(id)));
            }
        }
        Ok(rows)
    }

    pub(in crate::execution::interpreter) async fn node_search_rows(
        &self,
        results: Vec<TypedVectorSearchResult>,
    ) -> Result<ExecutionValue> {
        self.node_search_row_vec(results)
            .await
            .map(ExecutionValue::Stream)
    }

    pub(in crate::execution::interpreter) async fn node_search_row_vec(
        &self,
        results: Vec<TypedVectorSearchResult>,
    ) -> Result<Vec<ExecutionRow>> {
        let mut rows = Vec::new();
        for result in results {
            self.check_execution_deadline()?;
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
        Ok(rows)
    }

    pub(in crate::execution::interpreter) async fn edge_search_rows(
        &self,
        results: Vec<TypedVectorSearchResult>,
    ) -> Result<ExecutionValue> {
        self.edge_search_row_vec(results)
            .await
            .map(ExecutionValue::Stream)
    }

    pub(in crate::execution::interpreter) async fn edge_search_row_vec(
        &self,
        results: Vec<TypedVectorSearchResult>,
    ) -> Result<Vec<ExecutionRow>> {
        let mut rows = Vec::new();
        for result in results {
            self.check_execution_deadline()?;
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
        Ok(rows)
    }

    pub(in crate::execution::interpreter) async fn node_text_search_rows(
        &self,
        results: Vec<TextSearchHit>,
    ) -> Result<ExecutionValue> {
        self.node_text_search_row_vec(results)
            .await
            .map(ExecutionValue::Stream)
    }

    pub(in crate::execution::interpreter) async fn node_text_search_row_vec(
        &self,
        results: Vec<TextSearchHit>,
    ) -> Result<Vec<ExecutionRow>> {
        let mut rows = Vec::new();
        for result in results {
            let key = keys::Key::Data {
                scope: self.tenant_scope,
                kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(result.entity_id)),
            }
            .to_bytes();
            if self.get_raw(&key).await?.is_some() {
                rows.push(text_search_row(ElementRef::Node(result.entity_id), result));
            }
        }
        Ok(rows)
    }

    pub(in crate::execution::interpreter) async fn edge_text_search_rows(
        &self,
        results: Vec<TextSearchHit>,
    ) -> Result<ExecutionValue> {
        self.edge_text_search_row_vec(results)
            .await
            .map(ExecutionValue::Stream)
    }

    pub(in crate::execution::interpreter) async fn edge_text_search_row_vec(
        &self,
        results: Vec<TextSearchHit>,
    ) -> Result<Vec<ExecutionRow>> {
        let mut rows = Vec::new();
        for result in results {
            let key = keys::Key::Data {
                scope: self.tenant_scope,
                kind: keys::DataKeyKind::EdgeEndpoints(keys::EdgeEndpointsKey::new(
                    result.entity_id,
                )),
            }
            .to_bytes();
            if self.get_raw(&key).await?.is_some() {
                rows.push(text_search_row(ElementRef::Edge(result.entity_id), result));
            }
        }
        Ok(rows)
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

fn text_search_row(element: ElementRef, result: TextSearchHit) -> ExecutionRow {
    ExecutionRow::current_with_virtual_properties(
        element,
        RowVirtualProperties::from_one(
            ir::NonEmptyString::new("$score").expect("score virtual property is non-empty"),
            DbPropertyValue::F64(f64::from(result.score)),
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
            .node_search_rows(vec![
                vector_result(VectorEntityKind::Node, alice),
                vector_result(VectorEntityKind::Node, u64::MAX),
            ])
            .await
            .unwrap();
        assert_eq!(current_node_ids(nodes), vec![alice]);

        let edges = ctx
            .edge_search_rows(vec![
                vector_result(VectorEntityKind::Edge, follows),
                vector_result(VectorEntityKind::Edge, u64::MAX),
            ])
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

    #[tokio::test]
    async fn text_search_rows_preserve_raw_scores_for_nodes_and_edges() {
        let db = test_support::open_db("text-score-row-materialization").await;
        let alice = test_support::add_user(&db, "alice").await;
        let bob = test_support::add_user(&db, "bob").await;
        let follows = test_support::add_edge(&db, alice, bob, "FOLLOWS").await;
        let ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let score = ir::NonEmptyString::new("$score").unwrap();
        let distance = ir::NonEmptyString::new("$distance").unwrap();

        let nodes = ctx
            .node_text_search_rows(vec![
                TextSearchHit {
                    entity_id: alice,
                    score: 0.25,
                },
                TextSearchHit {
                    entity_id: u64::MAX,
                    score: 1.0,
                },
            ])
            .await
            .unwrap();
        let ExecutionValue::Stream(node_rows) = nodes else {
            panic!("text node rows materialize as a stream");
        };
        assert_eq!(node_rows.len(), 1);
        assert_eq!(node_rows[0].current, Some(ElementRef::Node(alice)));
        assert_eq!(
            node_rows[0].virtual_properties.get(&score),
            Some(DbPropertyValue::F64(f64::from(0.25_f32)))
        );
        assert!(node_rows[0].virtual_properties.get(&distance).is_none());

        let edges = ctx
            .edge_text_search_rows(vec![TextSearchHit {
                entity_id: follows,
                score: 0.75,
            }])
            .await
            .unwrap();
        let ExecutionValue::Stream(edge_rows) = edges else {
            panic!("text edge rows materialize as a stream");
        };
        assert_eq!(edge_rows.len(), 1);
        assert_eq!(edge_rows[0].current, Some(ElementRef::Edge(follows)));
        assert_eq!(
            edge_rows[0].virtual_properties.get(&score),
            Some(DbPropertyValue::F64(f64::from(0.75_f32)))
        );
        assert!(edge_rows[0].virtual_properties.get(&distance).is_none());
    }
}
