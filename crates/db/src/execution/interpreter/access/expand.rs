//! Graph expansion execution for executable access plans.
//!
//! This module owns traversal expansion from current node rows to neighboring
//! node rows or concrete edge rows. Access dispatch and index/search lookup stay
//! in sibling modules.

use std::collections::BTreeSet;

use helix_planner::ir;

use super::super::*;
use crate::encoding::keys;
#[cfg(test)]
use crate::encoding::v2::values;
use crate::query_resources::adjacency;
use crate::query_resources::bitmap;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn expand(
        &self,
        input: ExecutionValue,
        plan: &ir::ExpandPlan,
    ) -> Result<ExecutionValue> {
        let rows = self.stream_rows(input, "expand")?;
        // Keep storage-batch futures out of the recursive interpreter's stack
        // frame. One allocation per expansion operator preserves the normal
        // thread-stack contract even inside nested native control flow.
        match plan.output {
            ir::ExpandOutput::Nodes => Box::pin(self.expand_node_output(rows, plan)).await,
            ir::ExpandOutput::Edges => Box::pin(self.expand_edge_output(rows, plan)).await,
        }
    }

    async fn expand_node_output(
        &self,
        rows: Vec<ExecutionRow>,
        plan: &ir::ExpandPlan,
    ) -> Result<ExecutionValue> {
        let mut expanded = Vec::new();
        for row in rows {
            self.check_execution_deadline()?;
            let neighbor_ids = match row.current.as_ref() {
                Some(ElementRef::Node(node_id)) => match &plan.label {
                    ir::ExpandLabelPlan::Any => {
                        self.expand_any_edges(*node_id, plan.direction).await?
                    }
                    ir::ExpandLabelPlan::Label(label) => {
                        self.expand_labeled_edges(*node_id, plan.direction, label)
                            .await?
                    }
                },
                Some(ElementRef::Edge(edge_id)) => {
                    let Some((from, to)) = self.get_edge_endpoints(*edge_id).await? else {
                        continue;
                    };
                    match plan.direction {
                        ir::ExpandDirection::Out => vec![to],
                        ir::ExpandDirection::In => vec![from],
                        ir::ExpandDirection::Both => {
                            let previous_node = row.path.elements().iter().rev().find_map(
                                |element| match element {
                                    ElementRef::Node(id) => Some(*id),
                                    ElementRef::Edge(_) => None,
                                },
                            );
                            match previous_node {
                                Some(previous) if previous == from => vec![to],
                                Some(previous) if previous == to => vec![from],
                                _ if from == to => vec![from],
                                _ => vec![from, to],
                            }
                        }
                    }
                }
                None => continue,
            };
            for neighbor_id in neighbor_ids {
                self.check_execution_deadline()?;
                let mut next = row.clone();
                next.set_current(ElementRef::Node(neighbor_id));
                expanded.push(next);
            }
        }
        Ok(ExecutionValue::Stream(expanded))
    }

    async fn expand_edge_output(
        &self,
        rows: Vec<ExecutionRow>,
        plan: &ir::ExpandPlan,
    ) -> Result<ExecutionValue> {
        let Some(label) = self.edge_output_label(&plan.label).await? else {
            return Ok(ExecutionValue::Stream(Vec::new()));
        };
        let mut expanded = Vec::new();
        for row in rows {
            self.check_execution_deadline()?;
            let Some(ElementRef::Node(node_id)) = row.current.as_ref() else {
                continue;
            };
            let edge_ids = match &label {
                EdgeOutputExpansionLabel::Any => {
                    self.expand_any_edge_ids(*node_id, plan.direction, 512)
                        .await?
                }
                EdgeOutputExpansionLabel::Label { label, edge_ids } => {
                    self.expand_labeled_edge_ids(
                        *node_id,
                        plan.direction,
                        label,
                        Some(edge_ids),
                        512,
                    )
                    .await?
                }
            };
            for edge_id in edge_ids {
                self.check_execution_deadline()?;
                let mut next = row.clone();
                next.set_current(ElementRef::Edge(edge_id));
                expanded.push(next);
            }
        }
        Ok(ExecutionValue::Stream(expanded))
    }

    /// Neighborhood candidates for a graph-pattern expansion. Label-neighbor
    /// indexes prune endpoints, but parallel edges can have different types.
    /// The row operator must check candidate types before expanding another hop.
    /// This avoids reading the global relationship-label bitmap per source node.
    #[cfg(test)]
    pub(in crate::execution::interpreter) async fn expand_edge_candidate_ids(
        &self,
        node_id: u64,
        direction: ir::ExpandDirection,
        label: &ir::ExpandLabelPlan,
        target: Option<u64>,
        batch_size: usize,
    ) -> Result<bitmap::Bitmap> {
        Box::pin(super::EdgeCursor::new(
            self, node_id, direction, label, target, batch_size,
        ))
        .await?
        .collect(self, None)
        .await
    }

    async fn edge_output_label<'a>(
        &self,
        label: &'a ir::ExpandLabelPlan,
    ) -> Result<Option<EdgeOutputExpansionLabel<'a>>> {
        match label {
            ir::ExpandLabelPlan::Any => Ok(Some(EdgeOutputExpansionLabel::Any)),
            ir::ExpandLabelPlan::Label(label) => {
                let edge_ids = self.lookup_global_edge_label_index(label.as_ref()).await?;
                Ok((!edge_ids.is_empty())
                    .then_some(EdgeOutputExpansionLabel::Label { label, edge_ids }))
            }
        }
    }

    async fn expand_any_edges(
        &self,
        node_id: u64,
        direction: ir::ExpandDirection,
    ) -> Result<Vec<u64>> {
        let key = keys::DataKey::Data {
            scope: self.tenant_scope,
            kind: keys::DataKeyKind::Adjacency(keys::AdjacencyKey::new(node_id)),
        }
        .to_bytes();
        let Some(value) = self.get_raw(&key).await? else {
            return Ok(Vec::new());
        };
        let edges = adjacency::Adjacency::decode(&value, self.row_memory.as_ref())?;
        let mut out = BTreeSet::new();
        match direction {
            ir::ExpandDirection::Out | ir::ExpandDirection::Both => out.extend(edges.iter_out()),
            ir::ExpandDirection::In => {}
        }
        match direction {
            ir::ExpandDirection::In | ir::ExpandDirection::Both => out.extend(edges.iter_in()),
            ir::ExpandDirection::Out => {}
        }
        Ok(out.into_iter().collect())
    }

    async fn expand_any_edge_ids(
        &self,
        node_id: u64,
        direction: ir::ExpandDirection,
        batch_size: usize,
    ) -> Result<bitmap::Bitmap> {
        Box::pin(super::EdgeCursor::new(
            self,
            node_id,
            direction,
            &ir::ExpandLabelPlan::Any,
            None,
            batch_size,
        ))
        .await?
        .collect(self, None)
        .await
    }

    async fn expand_labeled_edges(
        &self,
        node_id: u64,
        direction: ir::ExpandDirection,
        label: &ir::NonEmptyString,
    ) -> Result<Vec<u64>> {
        let mut out = BTreeSet::new();
        match direction {
            ir::ExpandDirection::Out | ir::ExpandDirection::Both => {
                out.extend(
                    self.lookup_out_neighbors_by_label(node_id, label.as_ref())
                        .await?,
                );
            }
            ir::ExpandDirection::In => {}
        }
        match direction {
            ir::ExpandDirection::In | ir::ExpandDirection::Both => {
                out.extend(
                    self.lookup_in_neighbors_by_label(node_id, label.as_ref())
                        .await?,
                );
            }
            ir::ExpandDirection::Out => {}
        }
        Ok(out.into_iter().collect())
    }

    async fn expand_labeled_edge_ids(
        &self,
        node_id: u64,
        direction: ir::ExpandDirection,
        label: &ir::NonEmptyString,
        label_edge_ids: Option<&roaring::RoaringTreemap>,
        batch_size: usize,
    ) -> Result<bitmap::Bitmap> {
        Box::pin(super::EdgeCursor::new(
            self,
            node_id,
            direction,
            &ir::ExpandLabelPlan::Label(label.clone()),
            None,
            batch_size,
        ))
        .await?
        .collect(self, label_edge_ids)
        .await
    }
}

enum EdgeOutputExpansionLabel<'a> {
    Any,
    Label {
        label: &'a ir::NonEmptyString,
        edge_ids: crate::query_resources::bitmap::Bitmap,
    },
}

#[cfg(test)]
mod tests {
    use helix_planner::context;

    use super::super::super::test_support;
    use super::*;

    #[tokio::test]
    async fn pair_batches_are_bounded_deduplicate_members_and_release_failed_admission() {
        let db = test_support::open_db("expand-pair-batches").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.row_memory = Some(crate::query_resources::Budget::new(2 * 1024 * 1024));
        for target in 0..1025 {
            let ids = [target % 11, 100 + target % 11].into_iter().collect();
            let mut bytes = values::indexes::equality::SecondaryEqualityBitmapValue::new(ids)
                .encode()
                .to_vec();
            // Built-in pair rows retain the portable-prefix compatibility rule.
            bytes.push(255);
            db.inner_db()
                .put(
                    ctx.storage_key(keys::DataKeyKind::EdgePairIndex(
                        keys::EdgePairIndexKey::new(7, target),
                    )),
                    bytes::Bytes::from(bytes),
                )
                .await
                .unwrap();
        }
        ctx.enable_request_read_view().await.unwrap();
        let filter = (0..11).collect::<roaring::RoaringTreemap>();
        for batch_size in [512, 17] {
            let cursor = super::super::EdgeCursor::from_pairs(
                (0..1026).map(|target| (7, target)).collect(),
                batch_size,
            );
            let output = Box::pin(cursor.collect(&ctx, Some(&filter))).await.unwrap();
            assert_eq!(
                output.iter().collect::<Vec<_>>(),
                (0..11).collect::<Vec<_>>()
            );
            drop(output);
            assert_eq!(
                ctx.row_memory.as_ref().unwrap().available(),
                2 * 1024 * 1024
            );
        }
        let budget = ctx.row_memory.as_ref().unwrap();
        assert_eq!(budget.reads().point_gets, 0);
        assert_eq!(budget.reads().multi_get_batches, 64);
        assert_eq!(budget.reads().multi_get_keys, 2 * 1026);
        let empty = super::super::EdgeCursor::empty(512)
            .collect(&ctx, None)
            .await
            .unwrap();
        assert!(empty.is_empty());
        drop(empty);
        assert_eq!(budget.reads().multi_get_batches, 64);
        let cursor =
            super::super::EdgeCursor::from_pairs((0..1026).map(|target| (7, target)).collect(), 16);
        let (batch, rest) = Box::pin(cursor.next_batch(&ctx)).await.unwrap().unwrap();
        assert_eq!(batch.ids().len(), 16);
        assert_eq!(budget.reads().multi_get_keys, 2 * 1026 + 16);
        assert!(budget.available() < 2 * 1024 * 1024);
        drop(batch);
        drop(rest);
        assert_eq!(budget.available(), 2 * 1024 * 1024);
        ctx.row_memory = Some(crate::query_resources::Budget::new(32 * 1024));
        let cursor =
            super::super::EdgeCursor::from_pairs((0..512).map(|target| (7, target)).collect(), 512);
        assert!(matches!(
            Box::pin(cursor.next_batch(&ctx)).await,
            Err(HelixDbError::QueryMemoryLimitExceeded)
        ));
        assert_eq!(
            ctx.row_memory.as_ref().unwrap().reads().multi_get_batches,
            0
        );
        assert_eq!(ctx.row_memory.as_ref().unwrap().available(), 32 * 1024);
        ctx.close_request_read_view().unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn bound_endpoint_expansion_probes_exact_pairs_in_every_direction() {
        let db = test_support::open_db("expand-bound-endpoints").await;
        let a = test_support::add_user(&db, "a").await;
        let b = test_support::add_user(&db, "b").await;
        let forward = test_support::add_edge(&db, a, b, "R").await;
        let parallel = test_support::add_edge(&db, a, b, "S").await;
        let backward = test_support::add_edge(&db, b, a, "R").await;
        let self_loop = test_support::add_edge(&db, a, a, "R").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.row_memory = Some(crate::query_resources::Budget::new(1024 * 1024));
        // Unreadable adjacency proves this path does not scan the neighborhood.
        db.inner_db()
            .put(
                ctx.storage_key(keys::DataKeyKind::Adjacency(keys::AdjacencyKey::new(a))),
                bytes::Bytes::from_static(b"must not decode adjacency"),
            )
            .await
            .unwrap();
        ctx.enable_request_read_view().await.unwrap();
        for (direction, target, expected, reads) in [
            (ir::ExpandDirection::Out, b, vec![forward, parallel], 1),
            (ir::ExpandDirection::In, b, vec![backward], 1),
            (
                ir::ExpandDirection::Both,
                b,
                vec![forward, parallel, backward],
                2,
            ),
            (ir::ExpandDirection::Both, a, vec![self_loop], 1),
            (ir::ExpandDirection::Out, u64::MAX, vec![], 1),
        ] {
            let before = ctx.row_memory.as_ref().unwrap().reads();
            let ids = ctx
                .expand_edge_candidate_ids(
                    a,
                    direction,
                    &ir::ExpandLabelPlan::Any,
                    Some(target),
                    512,
                )
                .await
                .unwrap();
            assert_eq!(ids.into_iter().collect::<Vec<_>>(), expected);
            let after = ctx.row_memory.as_ref().unwrap().reads();
            assert_eq!(after.point_gets - before.point_gets, reads);
            assert_eq!(after.multi_get_batches, before.multi_get_batches);
            assert_eq!(ctx.row_memory.as_ref().unwrap().available(), 1024 * 1024);
        }
        ctx.close_request_read_view().unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn any_node_expansion_covers_all_directions_and_missing_adjacency() {
        let db = test_support::open_db("expand-any-node-directions").await;
        let alice = test_support::add_user(&db, "alice").await;
        let bob = test_support::add_user(&db, "bob").await;
        let carol = test_support::add_user(&db, "carol").await;
        test_support::add_edge(&db, alice, bob, "KNOWS").await;
        test_support::add_edge(&db, carol, alice, "FOLLOWS").await;
        let context = ExecutionContext::new(&db, context::ParamBindings::default());
        let node_ids = |value: ExecutionValue| {
            let ExecutionValue::Stream(rows) = value else {
                panic!("expansion should return a stream");
            };
            rows.into_iter()
                .map(|row| match row.current {
                    Some(ElementRef::Node(id)) => id,
                    other => panic!("expected expanded node row, got {other:?}"),
                })
                .collect::<Vec<_>>()
        };

        for (direction, expected) in [
            (ir::ExpandDirection::Out, vec![bob]),
            (ir::ExpandDirection::In, vec![carol]),
            (ir::ExpandDirection::Both, vec![bob, carol]),
        ] {
            let value = context
                .expand(
                    ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Node(alice))]),
                    &ir::ExpandPlan {
                        direction,
                        label: ir::ExpandLabelPlan::Any,
                        output: ir::ExpandOutput::Nodes,
                    },
                )
                .await
                .expect("any-node expansion succeeds");
            assert_eq!(node_ids(value), expected);
        }

        let missing = context
            .expand(
                ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Node(u64::MAX))]),
                &ir::ExpandPlan {
                    direction: ir::ExpandDirection::Out,
                    label: ir::ExpandLabelPlan::Any,
                    output: ir::ExpandOutput::Nodes,
                },
            )
            .await
            .expect("missing adjacency is empty");
        assert_eq!(missing, ExecutionValue::Stream(Vec::new()));
    }

    #[tokio::test]
    async fn edge_current_node_expansion_uses_direction_path_and_self_loop_contracts() {
        let db = test_support::open_db("expand-edge-current-directions").await;
        let alice = test_support::add_user(&db, "alice").await;
        let bob = test_support::add_user(&db, "bob").await;
        let edge = test_support::add_edge(&db, alice, bob, "KNOWS").await;
        let self_edge = test_support::add_edge(&db, alice, alice, "SELF").await;
        let context = ExecutionContext::new(&db, context::ParamBindings::default());
        let node_ids = |value: ExecutionValue| {
            let ExecutionValue::Stream(rows) = value else {
                panic!("expansion should return a stream");
            };
            rows.into_iter()
                .map(|row| match row.current {
                    Some(ElementRef::Node(id)) => id,
                    other => panic!("expected expanded node row, got {other:?}"),
                })
                .collect::<Vec<_>>()
        };
        let mut from_path = ExecutionRow::current(ElementRef::Node(alice));
        from_path.set_current(ElementRef::Edge(edge));
        let mut to_path = ExecutionRow::current(ElementRef::Node(bob));
        to_path.set_current(ElementRef::Edge(edge));

        let both = context
            .expand(
                ExecutionValue::Stream(vec![
                    ExecutionRow::current(ElementRef::Edge(edge)),
                    from_path,
                    to_path,
                    ExecutionRow::current(ElementRef::Edge(self_edge)),
                    ExecutionRow::current(ElementRef::Edge(u64::MAX)),
                    ExecutionRow::empty(),
                ]),
                &ir::ExpandPlan {
                    direction: ir::ExpandDirection::Both,
                    label: ir::ExpandLabelPlan::Any,
                    output: ir::ExpandOutput::Nodes,
                },
            )
            .await
            .expect("edge-current both expansion succeeds");
        assert_eq!(node_ids(both), vec![alice, bob, bob, alice, alice]);

        for (direction, expected) in [
            (ir::ExpandDirection::Out, vec![bob]),
            (ir::ExpandDirection::In, vec![alice]),
        ] {
            let value = context
                .expand(
                    ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Edge(edge))]),
                    &ir::ExpandPlan {
                        direction,
                        label: ir::ExpandLabelPlan::Any,
                        output: ir::ExpandOutput::Nodes,
                    },
                )
                .await
                .expect("edge-current directed expansion succeeds");
            assert_eq!(node_ids(value), expected);
        }
    }

    #[tokio::test]
    async fn edge_output_expansion_skips_absent_labels_non_nodes_and_missing_adjacency() {
        let db = test_support::open_db("expand-edge-output-empty-inputs").await;
        let alice = test_support::add_user(&db, "alice").await;
        let bob = test_support::add_user(&db, "bob").await;
        let edge = test_support::add_edge(&db, alice, bob, "KNOWS").await;
        let context = ExecutionContext::new(&db, context::ParamBindings::default());

        let absent_label = context
            .expand(
                ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Node(alice))]),
                &ir::ExpandPlan {
                    direction: ir::ExpandDirection::Out,
                    label: ir::ExpandLabelPlan::Label(test_support::name("MISSING")),
                    output: ir::ExpandOutput::Edges,
                },
            )
            .await
            .expect("absent edge label is empty");
        assert_eq!(absent_label, ExecutionValue::Stream(Vec::new()));

        let non_nodes = context
            .expand(
                ExecutionValue::Stream(vec![
                    ExecutionRow::current(ElementRef::Edge(edge)),
                    ExecutionRow::empty(),
                    ExecutionRow::current(ElementRef::Node(u64::MAX)),
                ]),
                &ir::ExpandPlan {
                    direction: ir::ExpandDirection::Both,
                    label: ir::ExpandLabelPlan::Any,
                    output: ir::ExpandOutput::Edges,
                },
            )
            .await
            .expect("non-node and missing adjacency inputs are skipped");
        assert_eq!(non_nodes, ExecutionValue::Stream(Vec::new()));
    }
}
