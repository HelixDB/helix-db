//! Graph expansion execution for executable access plans.
//!
//! This module owns traversal expansion from current node rows to neighboring
//! node rows or concrete edge rows. Access dispatch and index/search lookup stay
//! in sibling modules.

use std::collections::BTreeSet;

use helix_planner::ir;

use super::super::*;
use crate::encoding::keys;
use crate::encoding::v2::values;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn expand(
        &mut self,
        input: ExecutionValue,
        plan: &ir::ExpandPlan,
    ) -> Result<ExecutionValue> {
        let rows = self.stream_rows(input, "expand")?;
        match plan.output {
            ir::ExpandOutput::Nodes => self.expand_node_output(rows, plan).await,
            ir::ExpandOutput::Edges => self.expand_edge_output(rows, plan).await,
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
                    self.expand_any_edge_ids(*node_id, plan.direction).await?
                }
                EdgeOutputExpansionLabel::Label { label, edge_ids } => {
                    self.expand_labeled_edge_ids(*node_id, plan.direction, label, edge_ids)
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
        let edges = values::adjacency::decode_edges(&value)?;
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
    ) -> Result<BTreeSet<u64>> {
        let key = keys::DataKey::Data {
            scope: self.tenant_scope,
            kind: keys::DataKeyKind::Adjacency(keys::AdjacencyKey::new(node_id)),
        }
        .to_bytes();
        let Some(value) = self.get_raw(&key).await? else {
            return Ok(BTreeSet::new());
        };
        let edges = values::adjacency::decode_edges(&value)?;
        let mut out = BTreeSet::new();
        if matches!(
            direction,
            ir::ExpandDirection::Out | ir::ExpandDirection::Both
        ) {
            for to in edges.iter_out() {
                self.check_execution_deadline()?;
                self.extend_pair_edge_ids(&mut out, node_id, to, None)
                    .await?;
            }
        }
        if matches!(
            direction,
            ir::ExpandDirection::In | ir::ExpandDirection::Both
        ) {
            for from in edges.iter_in() {
                self.check_execution_deadline()?;
                self.extend_pair_edge_ids(&mut out, from, node_id, None)
                    .await?;
            }
        }
        Ok(out)
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
        label_edge_ids: &roaring::RoaringTreemap,
    ) -> Result<BTreeSet<u64>> {
        let mut out = BTreeSet::new();
        if matches!(
            direction,
            ir::ExpandDirection::Out | ir::ExpandDirection::Both
        ) {
            for to in self
                .lookup_out_neighbors_by_label(node_id, label.as_ref())
                .await?
            {
                self.check_execution_deadline()?;
                self.extend_pair_edge_ids(&mut out, node_id, to, Some(label_edge_ids))
                    .await?;
            }
        }
        if matches!(
            direction,
            ir::ExpandDirection::In | ir::ExpandDirection::Both
        ) {
            for from in self
                .lookup_in_neighbors_by_label(node_id, label.as_ref())
                .await?
            {
                self.check_execution_deadline()?;
                self.extend_pair_edge_ids(&mut out, from, node_id, Some(label_edge_ids))
                    .await?;
            }
        }
        Ok(out)
    }

    async fn extend_pair_edge_ids(
        &self,
        out: &mut BTreeSet<u64>,
        from: u64,
        to: u64,
        filter: Option<&roaring::RoaringTreemap>,
    ) -> Result<()> {
        let pair_ids = self.lookup_edge_pair_index(from, to).await?;
        out.extend(pair_ids.into_iter().filter(|edge_id| {
            filter
                .map(|label_edge_ids| label_edge_ids.contains(*edge_id))
                .unwrap_or(true)
        }));
        Ok(())
    }
}

enum EdgeOutputExpansionLabel<'a> {
    Any,
    Label {
        label: &'a ir::NonEmptyString,
        edge_ids: roaring::RoaringTreemap,
    },
}

#[cfg(test)]
mod tests {
    use helix_planner::context;

    use super::super::super::test_support;
    use super::*;

    #[tokio::test]
    async fn any_node_expansion_covers_all_directions_and_missing_adjacency() {
        let db = test_support::open_db("expand-any-node-directions").await;
        let alice = test_support::add_user(&db, "alice").await;
        let bob = test_support::add_user(&db, "bob").await;
        let carol = test_support::add_user(&db, "carol").await;
        test_support::add_edge(&db, alice, bob, "KNOWS").await;
        test_support::add_edge(&db, carol, alice, "FOLLOWS").await;
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
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
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
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
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());

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
