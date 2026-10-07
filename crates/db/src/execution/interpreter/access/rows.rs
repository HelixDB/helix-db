//! Graph element row materialization.

use helix_planner::ir;

use super::super::{
    ElementRef, ExecutionContext, ExecutionRow, ExecutionValue, RowVirtualProperties,
};
use crate::encoding::keys;
use crate::encoding::property::property_value::PropertyValue as DbPropertyValue;
use crate::error::{HelixDbError, Result};
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
        Ok(self
            .retain_existing(ids, |id| {
                keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(*id))
            })
            .await?
            .into_iter()
            .map(|id| ExecutionRow::current(ElementRef::Node(id)))
            .collect())
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
        Ok(self
            .retain_existing(ids, |id| {
                keys::DataKeyKind::EdgeEndpoints(keys::EdgeEndpointsKey::new(*id))
            })
            .await?
            .into_iter()
            .map(|id| ExecutionRow::current(ElementRef::Edge(id)))
            .collect())
    }

    pub(in crate::execution::interpreter) async fn node_search_rows(
        &self,
        results: Vec<TypedVectorSearchResult>,
    ) -> Result<ExecutionValue> {
        self.node_search_row_vec(results)
            .await
            .map(ExecutionValue::Stream)
    }

    /// Keeps the node hits whose record still exists, in rank order. A hit
    /// bound to an edge generation fails the batch before any read.
    pub(in crate::execution::interpreter) async fn node_search_row_vec(
        &self,
        results: Vec<TypedVectorSearchResult>,
    ) -> Result<Vec<ExecutionRow>> {
        let hits = results
            .into_iter()
            .map(|result| match result.entity_id() {
                VectorEntityId::Node(id) => Ok((id, result)),
                VectorEntityId::Edge(_) => Err(HelixDbError::InvariantViolation(
                    "edge-bound vector result reached node row materialization".to_string(),
                )),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(self
            .retain_existing(hits, |(id, _)| {
                keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(*id))
            })
            .await?
            .into_iter()
            .map(|(id, result)| search_row(ElementRef::Node(id), result))
            .collect())
    }

    pub(in crate::execution::interpreter) async fn edge_search_rows(
        &self,
        results: Vec<TypedVectorSearchResult>,
    ) -> Result<ExecutionValue> {
        self.edge_search_row_vec(results)
            .await
            .map(ExecutionValue::Stream)
    }

    /// Keeps the edge hits whose record still exists, in rank order. A hit
    /// bound to a node generation fails the batch before any read.
    pub(in crate::execution::interpreter) async fn edge_search_row_vec(
        &self,
        results: Vec<TypedVectorSearchResult>,
    ) -> Result<Vec<ExecutionRow>> {
        let hits = results
            .into_iter()
            .map(|result| match result.entity_id() {
                VectorEntityId::Edge(id) => Ok((id, result)),
                VectorEntityId::Node(_) => Err(HelixDbError::InvariantViolation(
                    "node-bound vector result reached edge row materialization".to_string(),
                )),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(self
            .retain_existing(hits, |(id, _)| {
                keys::DataKeyKind::EdgeEndpoints(keys::EdgeEndpointsKey::new(*id))
            })
            .await?
            .into_iter()
            .map(|(id, result)| search_row(ElementRef::Edge(id), result))
            .collect())
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
        Ok(self
            .retain_existing(results, |hit| {
                keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(hit.entity_id))
            })
            .await?
            .into_iter()
            .map(|hit| text_search_row(ElementRef::Node(hit.entity_id), hit))
            .collect())
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
        Ok(self
            .retain_existing(results, |hit| {
                keys::DataKeyKind::EdgeEndpoints(keys::EdgeEndpointsKey::new(hit.entity_id))
            })
            .await?
            .into_iter()
            .map(|hit| text_search_row(ElementRef::Edge(hit.entity_id), hit))
            .collect())
    }

    /// Keeps the `items` whose stored record exists, in input order.
    ///
    /// The deadline is checked per item before any read. Records are read
    /// [`EXISTENCE_BATCH_ROWS`] at a time through one overlapped multi-get,
    /// so a cold batch waits for a few block fetches rather than one per
    /// item, and at most one batch of values is held at once.
    async fn retain_existing<T>(
        &self,
        items: Vec<T>,
        record: impl Fn(&T) -> keys::DataKeyKind<'static>,
    ) -> Result<Vec<T>> {
        let mut exists = Vec::with_capacity(items.len());
        for batch in items.chunks(EXISTENCE_BATCH_ROWS) {
            let keys = batch
                .iter()
                .map(|item| {
                    self.check_execution_deadline()?;
                    Ok(self.storage_key(record(item)))
                })
                .collect::<Result<Vec<_>>>()?;
            exists.extend(self.multi_get_raw(&keys).await?.iter().map(Option::is_some));
        }
        Ok(items
            .into_iter()
            .zip(exists)
            .filter_map(|(item, exists)| exists.then_some(item))
            .collect())
    }
}

/// Records one existence batch reads. Bounds the values held at once (a
/// record can carry an embedding) and matches the planner's record batch.
const EXISTENCE_BATCH_ROWS: usize = helix_planner::cost::RECORD_BATCH_ROWS as usize;

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
    use crate::encoding::v2::values::indexes::vector::{ActiveScoreSemantic, VectorEntityKind};
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
            .node_rows(vec![bob, u64::MAX, alice, bob])
            .await
            .expect("node rows materialize");

        assert_eq!(current_node_ids(rows), vec![bob, alice, bob]);
    }

    #[tokio::test]
    async fn existence_reads_use_bounded_batches_and_preserve_duplicate_ids() {
        let db = test_support::open_db("access-batched-existence").await;
        let alice = test_support::add_user(&db, "alice").await;
        let bob = test_support::add_user(&db, "bob").await;
        let edge = test_support::add_edge(&db, alice, bob, "R").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let budget = super::super::super::rows::memory::Budget::new(1024 * 1024);
        ctx.row_memory = Some(budget.clone());
        let ids = [alice, u64::MAX, bob]
            .into_iter()
            .cycle()
            .take(1025)
            .collect::<Vec<_>>();
        let expected = ids
            .iter()
            .copied()
            .filter(|id| *id != u64::MAX)
            .collect::<Vec<_>>();
        assert_eq!(
            current_node_ids(ctx.node_rows(ids).await.unwrap()),
            expected
        );
        assert_eq!(
            current_edge_ids(ctx.edge_rows(vec![edge; 513]).await.unwrap()),
            vec![edge; 513]
        );
        let reads = budget.reads();
        assert_eq!(reads.point_gets, 0);
        assert_eq!(
            reads.multi_get_batches,
            1025_usize.div_ceil(EXISTENCE_BATCH_ROWS) + 513_usize.div_ceil(EXISTENCE_BATCH_ROWS)
        );
        assert_eq!(reads.multi_get_keys, 1538);
        db.close().await.unwrap();
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
            .edge_rows(vec![knows, u64::MAX, follows, knows])
            .await
            .expect("edge rows materialize");

        assert_eq!(current_edge_ids(rows), vec![knows, follows, knows]);
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

    /// Existence checks read whole record batches through multi-gets rather
    /// than one get per hit, keep input order across batch boundaries, drop
    /// only missing records, and keep each hit's own score.
    #[tokio::test]
    async fn existence_checks_keep_input_order_across_record_batches() {
        let db = test_support::open_db("existence-batches").await;
        let mut users = Vec::new();
        for index in 0..EXISTENCE_BATCH_ROWS + 40 {
            users.push(test_support::add_user(&db, &format!("user-{index}")).await);
        }
        // Newest first, with a missing id after every fifth user.
        let ids = users
            .iter()
            .rev()
            .enumerate()
            .flat_map(|(index, id)| match index % 5 {
                0 => vec![*id, u64::MAX - index as u64],
                _ => vec![*id],
            })
            .collect::<Vec<_>>();
        let expected = users.iter().rev().copied().collect::<Vec<_>>();

        let ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        assert_eq!(
            current_node_ids(ctx.node_rows(ids.clone()).await.unwrap()),
            expected
        );
        let hits = ids
            .iter()
            .map(|id| vector_result(VectorEntityKind::Node, *id))
            .collect();
        assert_eq!(
            current_node_ids(ctx.node_search_rows(hits).await.unwrap()),
            expected
        );
        let score = ir::NonEmptyString::new("$score").unwrap();
        let text_hits = ids
            .iter()
            .enumerate()
            .map(|(rank, id)| TextSearchHit {
                entity_id: *id,
                score: rank as f32,
            })
            .collect();
        let ExecutionValue::Stream(text_rows) = ctx.node_text_search_rows(text_hits).await.unwrap()
        else {
            panic!("text node rows materialize as a stream");
        };
        let kept = text_rows
            .iter()
            .map(|row| {
                let Some(ElementRef::Node(id)) = row.current else {
                    panic!("text rows are node rows");
                };
                let rank = ids.iter().position(|candidate| *candidate == id).unwrap();
                assert_eq!(
                    row.virtual_properties.get(&score),
                    Some(DbPropertyValue::F64(f64::from(rank as f32)))
                );
                id
            })
            .collect::<Vec<_>>();
        assert_eq!(kept, expected);

        let work = ctx.pull_work.snapshot();
        assert_eq!(work.raw_gets, 0, "no per-hit point reads");
        assert_eq!(work.multi_get_keys, 3 * ids.len());
    }

    /// A hit of the wrong entity kind fails the whole batch before any read,
    /// wherever it sits in the batch.
    #[tokio::test]
    async fn search_rows_reject_the_wrong_kind_before_reading() {
        let db = test_support::open_db("existence-wrong-kind").await;
        let alice = test_support::add_user(&db, "alice").await;
        let bob = test_support::add_user(&db, "bob").await;
        let follows = test_support::add_edge(&db, alice, bob, "FOLLOWS").await;
        let ctx = ExecutionContext::new(&db, context::ParamBindings::default());

        assert!(matches!(
            ctx.node_search_rows(vec![
                vector_result(VectorEntityKind::Node, alice),
                vector_result(VectorEntityKind::Node, bob),
                vector_result(VectorEntityKind::Edge, follows),
            ])
            .await,
            Err(HelixDbError::InvariantViolation(_))
        ));
        assert!(matches!(
            ctx.edge_search_rows(vec![
                vector_result(VectorEntityKind::Edge, follows),
                vector_result(VectorEntityKind::Node, alice),
            ])
            .await,
            Err(HelixDbError::InvariantViolation(_))
        ));
        let work = ctx.pull_work.snapshot();
        assert_eq!((work.raw_gets, work.multi_get_keys), (0, 0));
    }

    /// Every materializer checks the deadline per item before reading, and
    /// once more as the batch read starts.
    #[tokio::test]
    async fn existence_checks_respect_the_deadline() {
        let db = test_support::open_db("existence-deadline").await;
        let alice = test_support::add_user(&db, "alice").await;
        let bob = test_support::add_user(&db, "bob").await;
        let follows = test_support::add_edge(&db, alice, bob, "FOLLOWS").await;

        for successful_checks in [0, 1, 2] {
            let ctx = ExecutionContext::new(&db, context::ParamBindings::default());
            let deadline = |result: Result<ExecutionValue>| {
                assert!(
                    matches!(result, Err(HelixDbError::QueryDeadlineExceeded)),
                    "{successful_checks}"
                );
                ctx.fail_deadline_after(successful_checks);
            };
            ctx.fail_deadline_after(successful_checks);
            deadline(ctx.node_rows(vec![alice, bob]).await);
            deadline(ctx.edge_rows(vec![follows, follows]).await);
            deadline(
                ctx.node_search_rows(vec![
                    vector_result(VectorEntityKind::Node, alice),
                    vector_result(VectorEntityKind::Node, bob),
                ])
                .await,
            );
            deadline(
                ctx.edge_search_rows(vec![
                    vector_result(VectorEntityKind::Edge, follows),
                    vector_result(VectorEntityKind::Edge, follows),
                ])
                .await,
            );
            let text = |id| {
                vec![
                    TextSearchHit {
                        entity_id: id,
                        score: 1.0,
                    },
                    TextSearchHit {
                        entity_id: id,
                        score: 0.5,
                    },
                ]
            };
            deadline(ctx.node_text_search_rows(text(alice)).await);
            deadline(ctx.edge_text_search_rows(text(follows)).await);
            assert_eq!(ctx.pull_work.snapshot().multi_get_keys, 0);
        }

        let ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        assert!(ctx.node_rows(Vec::new()).await.is_ok());
        assert_eq!(ctx.pull_work.snapshot().multi_get_keys, 0);
    }
}
