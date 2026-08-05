//! Executable mutation operation flow.
//!
//! This module owns dispatch from [`exec::ExecMutationPlan`] into row
//! collection, transaction creation, storage mutation helpers, and result
//! stream construction. Storage-specific contracts remain in the node, edge,
//! adjacency, search-index, and property helper modules.

use std::collections::BTreeSet;

use super::contracts::{reject_label_mutation, EdgeMutationTarget};
use super::*;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn execute_mutation(
        &mut self,
        input: ExecutionValue,
        plan: &exec::ExecMutationPlan,
    ) -> Result<ExecutionValue> {
        match plan {
            exec::ExecMutationPlan::AddNodeSource { label, properties } => {
                self.add_node_source(label, properties).await
            }
            exec::ExecMutationPlan::AddNodeFromInput { label, properties } => {
                self.add_node_from_input(input, label, properties).await
            }
            exec::ExecMutationPlan::AddEdge {
                label,
                to,
                properties,
            } => self.add_edges(input, label, to, properties).await,
            exec::ExecMutationPlan::SetProperty { name, value } => {
                self.set_property(input, name, value).await
            }
            exec::ExecMutationPlan::RemoveProperty { name } => {
                self.remove_property(input, name).await
            }
            exec::ExecMutationPlan::Drop => self.drop_nodes(input).await,
            exec::ExecMutationPlan::DropEdge { to } => {
                self.drop_edges_between(input, to, None).await
            }
            exec::ExecMutationPlan::DropEdgeLabeled { to, label } => {
                self.drop_edges_between(input, to, Some(label)).await
            }
            exec::ExecMutationPlan::DropEdgeByIdSource { edges } => {
                self.drop_edges_by_id(None, edges).await
            }
            exec::ExecMutationPlan::DropEdgeByIdFromInput { edges } => {
                self.drop_edges_by_id(Some(input), edges).await
            }
        }
    }

    async fn add_node_source(
        &mut self,
        label: &ir::NonEmptyString,
        assignments: &ir::PropertyAssignments,
    ) -> Result<ExecutionValue> {
        let row = ExecutionRow::empty();
        let id = self.writer()?.node_ids().allocate().await?;
        let properties = self
            .node_create_properties(&row, label, assignments)
            .await?;
        let mut scope = self.take_or_begin_write_scope().await?;
        self.store_node(&scope.txn, id, properties, &mut scope.index_context)
            .await?;
        self.finish_write_scope(scope).await?;
        Ok(ExecutionValue::Stream(vec![ExecutionRow::current(
            ElementRef::Node(id),
        )]))
    }

    async fn add_node_from_input(
        &mut self,
        input: ExecutionValue,
        label: &ir::NonEmptyString,
        assignments: &ir::PropertyAssignments,
    ) -> Result<ExecutionValue> {
        let rows = self.stream_rows(input, "add node")?;
        let ids = self
            .writer()?
            .node_ids()
            .allocate_batch(rows.len().try_into().unwrap_or(u64::MAX))
            .await?;
        let mut rows_and_properties = Vec::with_capacity(rows.len());
        for row in rows {
            self.check_execution_deadline()?;
            let properties = self
                .node_create_properties(&row, label, assignments)
                .await?;
            rows_and_properties.push((row, properties));
        }
        let mut scope = self.take_or_begin_write_scope().await?;
        let mut output = Vec::with_capacity(rows_and_properties.len());

        for ((row, properties), id) in rows_and_properties.into_iter().zip(ids) {
            self.check_execution_deadline()?;
            self.store_node(&scope.txn, id, properties, &mut scope.index_context)
                .await?;

            let mut next = row;
            next.set_current(ElementRef::Node(id));
            output.push(next);
        }

        self.finish_write_scope(scope).await?;
        Ok(ExecutionValue::Stream(output))
    }

    pub(super) async fn add_edges(
        &mut self,
        input: ExecutionValue,
        label: &ir::NonEmptyString,
        to: &ir::NodeTargetPlan,
        assignments: &ir::PropertyAssignments,
    ) -> Result<ExecutionValue> {
        let rows = self.stream_rows(input, "add edge")?;
        let targets = self.node_targets(to).await?;
        if rows.is_empty() {
            return Ok(ExecutionValue::Stream(Vec::new()));
        }

        let source_rows = rows
            .into_iter()
            .filter_map(|row| {
                let from = match row.current.as_ref() {
                    Some(ElementRef::Node(from)) => *from,
                    Some(ElementRef::Edge(_)) | None => return None,
                };
                Some((row, from))
            })
            .collect::<Vec<_>>();
        if source_rows.is_empty() {
            return Ok(ExecutionValue::Stream(Vec::new()));
        }
        if targets.is_empty() {
            return Err(HelixDbError::Query(
                "addE() requires at least one target vertex".to_string(),
            ));
        }

        let edge_count = source_rows
            .len()
            .checked_mul(targets.len())
            .ok_or_else(|| HelixDbError::Query("edge creation count overflowed".to_string()))?;
        let mut source_rows_and_properties = Vec::with_capacity(source_rows.len());
        for (row, from) in source_rows {
            self.check_execution_deadline()?;
            let properties = self
                .edge_create_properties(&row, label, assignments)
                .await?;
            source_rows_and_properties.push((
                row,
                from,
                crate::index_lifecycle::graph_mutation::CanonicalPropertyRow::new(properties),
            ));
        }
        let ids = self
            .writer()?
            .edge_ids()
            .allocate_batch(edge_count.try_into().unwrap_or(u64::MAX))
            .await?;
        let mut scope = self.take_or_begin_write_scope().await?;
        let endpoint_existence = self
            .observe_node_existence(
                &scope.txn,
                source_rows_and_properties
                    .iter()
                    .map(|(_, from, _)| *from)
                    .chain(targets.iter().copied()),
            )
            .await?;
        let mut next_edge_id = ids.start;
        let mut output = Vec::with_capacity(edge_count);

        for (row, from, properties) in source_rows_and_properties {
            self.check_execution_deadline()?;
            endpoint_existence.require(from)?;
            for to in &targets {
                self.check_execution_deadline()?;
                endpoint_existence.require(*to)?;
                let edge_id = next_edge_id;
                next_edge_id += 1;
                let edge = EdgeMutationTarget::new(edge_id, from, *to);
                self.store_edge(
                    &scope.txn,
                    edge,
                    label,
                    &properties,
                    &mut scope.index_context,
                )
                .await?;

                let mut next = row.clone();
                next.set_current(ElementRef::Edge(edge_id));
                output.push(next);
            }
        }

        self.finish_write_scope(scope).await?;
        Ok(ExecutionValue::Stream(output))
    }

    async fn set_property(
        &mut self,
        input: ExecutionValue,
        name: &ir::NonEmptyString,
        value: &ir::PropertyInputPlan,
    ) -> Result<ExecutionValue> {
        let rows = self.stream_rows(input, "set property")?;
        let mut values = Vec::with_capacity(rows.len());
        for row in &rows {
            self.check_execution_deadline()?;
            values.push(self.property_input_value(row, value).await?);
        }
        let mut scope = self.take_or_begin_write_scope().await?;
        let mut observed_nodes = self
            .observe_node_rows(
                &scope.txn,
                rows.iter().filter_map(|row| match row.current.as_ref() {
                    Some(ElementRef::Node(node_id)) => Some(*node_id),
                    Some(ElementRef::Edge(_)) | None => None,
                }),
            )
            .await?;
        let mut observed_edges = self
            .observe_edge_rows(
                &scope.txn,
                rows.iter().filter_map(|row| match row.current.as_ref() {
                    Some(ElementRef::Edge(edge_id)) => Some(*edge_id),
                    Some(ElementRef::Node(_)) | None => None,
                }),
            )
            .await?;

        for (row, value) in rows.iter().zip(values) {
            self.check_execution_deadline()?;
            let property = Property::new(name.as_ref(), value);
            match row.current.as_ref() {
                Some(ElementRef::Node(node_id)) => {
                    let final_row = self
                        .set_node_property_observed(
                            &scope.txn,
                            *node_id,
                            property,
                            observed_nodes.observed(*node_id),
                            &mut scope.index_context,
                        )
                        .await?;
                    observed_nodes.replace(*node_id, Some(final_row));
                }
                Some(ElementRef::Edge(edge_id)) => {
                    if name.as_ref() == "$label" {
                        return Err(HelixDbError::Query(
                            "edge `$label` mutations are not supported by executable mutations"
                                .to_string(),
                        ));
                    }
                    let final_row = self
                        .set_edge_property_observed(
                            &scope.txn,
                            *edge_id,
                            property,
                            observed_edges.observed(*edge_id),
                            &mut scope.index_context,
                        )
                        .await?;
                    observed_edges.replace_properties(*edge_id, Some(final_row));
                }
                None => {}
            }
        }

        self.finish_write_scope(scope).await?;
        Ok(ExecutionValue::Stream(rows))
    }

    async fn remove_property(
        &mut self,
        input: ExecutionValue,
        name: &ir::NonEmptyString,
    ) -> Result<ExecutionValue> {
        reject_label_mutation(name)?;
        let rows = self.stream_rows(input, "remove property")?;
        let mut scope = self.take_or_begin_write_scope().await?;
        let mut observed_nodes = self
            .observe_node_rows(
                &scope.txn,
                rows.iter().filter_map(|row| match row.current.as_ref() {
                    Some(ElementRef::Node(node_id)) => Some(*node_id),
                    Some(ElementRef::Edge(_)) | None => None,
                }),
            )
            .await?;
        let mut observed_edges = self
            .observe_edge_rows(
                &scope.txn,
                rows.iter().filter_map(|row| match row.current.as_ref() {
                    Some(ElementRef::Edge(edge_id)) => Some(*edge_id),
                    Some(ElementRef::Node(_)) | None => None,
                }),
            )
            .await?;

        for row in &rows {
            self.check_execution_deadline()?;
            match row.current.as_ref() {
                Some(ElementRef::Node(node_id)) => {
                    let final_row = self
                        .remove_node_property_observed(
                            &scope.txn,
                            *node_id,
                            name,
                            observed_nodes.observed(*node_id),
                            &mut scope.index_context,
                        )
                        .await?;
                    observed_nodes.replace(*node_id, final_row);
                }
                Some(ElementRef::Edge(edge_id)) => {
                    let final_row = self
                        .remove_edge_property_observed(
                            &scope.txn,
                            *edge_id,
                            name,
                            observed_edges.observed(*edge_id),
                            &mut scope.index_context,
                        )
                        .await?;
                    observed_edges.replace_properties(*edge_id, final_row);
                }
                None => {}
            }
        }

        self.finish_write_scope(scope).await?;
        Ok(ExecutionValue::Stream(rows))
    }

    async fn drop_nodes(&mut self, input: ExecutionValue) -> Result<ExecutionValue> {
        let rows = self.stream_rows(input, "drop")?;
        let node_ids = rows
            .iter()
            .filter_map(|row| match row.current.as_ref() {
                Some(ElementRef::Node(id)) => Some(*id),
                Some(ElementRef::Edge(_)) | None => None,
            })
            .collect::<BTreeSet<_>>();
        if node_ids.is_empty() {
            return Ok(ExecutionValue::Stream(Vec::new()));
        }

        let mut scope = self.take_or_begin_write_scope().await?;
        self.delete_nodes(&scope.txn, node_ids, &mut scope.index_context)
            .await?;
        self.finish_write_scope(scope).await?;
        Ok(ExecutionValue::Stream(Vec::new()))
    }

    async fn drop_edges_between(
        &mut self,
        input: ExecutionValue,
        to: &ir::NodeTargetPlan,
        label: Option<&ir::NonEmptyString>,
    ) -> Result<ExecutionValue> {
        let rows = self.stream_rows(input, "drop edge")?;
        let source_nodes = rows
            .iter()
            .filter_map(|row| match row.current.as_ref() {
                Some(ElementRef::Node(id)) => Some(*id),
                Some(ElementRef::Edge(_)) | None => None,
            })
            .collect::<BTreeSet<_>>();
        let targets = self
            .node_targets(to)
            .await?
            .into_iter()
            .collect::<BTreeSet<_>>();
        if source_nodes.is_empty() || targets.is_empty() {
            return Ok(ExecutionValue::Stream(Vec::new()));
        }

        let mut scope = self.take_or_begin_write_scope().await?;
        scope.index_context.flush_topology(&scope.txn).await?;
        let pairs = self
            .edge_pairs_between(&scope.txn, &source_nodes, &targets, &scope.index_context)
            .await?;
        let pair_values = self
            .observe_edge_pair_values(&scope.txn, pairs, &scope.index_context)
            .await?;
        let mut candidate_edge_ids = BTreeSet::new();
        for value in pair_values.values() {
            self.check_execution_deadline()?;
            let Some(value) = value.as_ref() else {
                continue;
            };
            candidate_edge_ids
                .extend(values::secondary::SecondaryEqualityValue::decode(value)?.into_ids());
        }
        let mut observed_edges = self
            .observe_edge_deletions_from_pairs(
                &scope.txn,
                candidate_edge_ids.iter().copied(),
                pair_values,
            )
            .await?;
        let edge_ids = candidate_edge_ids
            .into_iter()
            .filter(|edge_id| observed_edges.matches_label(*edge_id, label))
            .collect::<Vec<_>>();
        for edge_id in edge_ids {
            self.check_execution_deadline()?;
            self.delete_edge_observed(
                &scope.txn,
                edge_id,
                observed_edges.take(edge_id)?,
                &mut scope.index_context,
            )
            .await?;
        }
        self.finish_write_scope(scope).await?;
        Ok(ExecutionValue::Stream(Vec::new()))
    }

    async fn drop_edges_by_id(
        &mut self,
        input: Option<ExecutionValue>,
        edges: &ir::EdgeTargetPlan,
    ) -> Result<ExecutionValue> {
        if let Some(input) = input {
            let rows = self.stream_rows(input, "drop edge by id")?;
            if rows.is_empty() {
                return Ok(ExecutionValue::Stream(Vec::new()));
            }
        }
        let edge_ids = self
            .edge_targets(edges)?
            .into_iter()
            .collect::<BTreeSet<_>>();
        if edge_ids.is_empty() {
            return Ok(ExecutionValue::Stream(Vec::new()));
        }

        let mut scope = self.take_or_begin_write_scope().await?;
        scope.index_context.flush_topology(&scope.txn).await?;
        let mut observed_edges = self
            .observe_edge_deletions(&scope.txn, edge_ids.iter().copied(), &scope.index_context)
            .await?;
        for edge_id in edge_ids {
            self.check_execution_deadline()?;
            self.delete_edge_observed(
                &scope.txn,
                edge_id,
                observed_edges.take(edge_id)?,
                &mut scope.index_context,
            )
            .await?;
        }
        self.finish_write_scope(scope).await?;
        Ok(ExecutionValue::Stream(Vec::new()))
    }
}

#[cfg(test)]
mod tests {
    use helix_ast::value::PropertyValue as AstPropertyValue;
    use helix_planner::context;

    use super::super::super::test_support;
    use super::*;

    #[tokio::test]
    async fn executable_mutation_rejects_direct_edge_label_changes() {
        let db = test_support::open_db("mutation-edge-label-change").await;
        let from = test_support::add_user(&db, "alice").await;
        let to = test_support::add_user(&db, "bob").await;
        let edge_id = test_support::add_edge(&db, from, to, "FOLLOWS").await;
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());

        let error = context
            .execute_mutation(
                ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Edge(edge_id))]),
                &exec::ExecMutationPlan::SetProperty {
                    name: test_support::name("$label"),
                    value: ir::PropertyInputPlan::Value(AstPropertyValue::String(
                        "LIKES".to_string(),
                    )),
                },
            )
            .await
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("edge `$label` mutations are not supported"));
    }

    #[tokio::test]
    async fn executable_mutation_covers_input_creation_and_empty_runtime_domains() {
        let db = test_support::open_db("mutation-input-and-empty-domains").await;
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        let label = test_support::name("User");
        let properties = test_support::assignments(vec![(
            "status",
            AstPropertyValue::String("active".to_string()),
        )]);

        let created = context
            .execute_mutation(
                ExecutionValue::Stream(vec![ExecutionRow::empty(), ExecutionRow::empty()]),
                &exec::ExecMutationPlan::AddNodeFromInput {
                    label: label.clone(),
                    properties,
                },
            )
            .await
            .expect("input-driven node creation succeeds");
        let ExecutionValue::Stream(created_rows) = created else {
            panic!("input-driven node creation returns a stream");
        };
        assert_eq!(created_rows.len(), 2);
        assert!(created_rows
            .iter()
            .all(|row| matches!(row.current, Some(ElementRef::Node(_)))));

        let empty_edge_create = context
            .execute_mutation(
                ExecutionValue::Stream(Vec::new()),
                &exec::ExecMutationPlan::AddEdge {
                    label: test_support::name("FOLLOWS"),
                    to: ir::NodeTargetPlan::Empty,
                    properties: ir::PropertyAssignments::default(),
                },
            )
            .await
            .expect("empty input is a successful no-op");
        assert_eq!(empty_edge_create, ExecutionValue::Stream(Vec::new()));

        let non_node_edge_create = context
            .execute_mutation(
                ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Edge(1))]),
                &exec::ExecMutationPlan::AddEdge {
                    label: test_support::name("FOLLOWS"),
                    to: ir::NodeTargetPlan::Empty,
                    properties: ir::PropertyAssignments::default(),
                },
            )
            .await
            .expect("rows without current nodes are a successful no-op");
        assert_eq!(non_node_edge_create, ExecutionValue::Stream(Vec::new()));

        let Some(ElementRef::Node(source)) = created_rows[0].current.as_ref() else {
            panic!("created source row retains its node");
        };
        let missing_target = context
            .execute_mutation(
                ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Node(*source))]),
                &exec::ExecMutationPlan::AddEdge {
                    label: test_support::name("FOLLOWS"),
                    to: ir::NodeTargetPlan::Empty,
                    properties: ir::PropertyAssignments::default(),
                },
            )
            .await
            .unwrap_err();
        assert!(missing_target
            .to_string()
            .contains("requires at least one target vertex"));

        for plan in [
            exec::ExecMutationPlan::SetProperty {
                name: test_support::name("status"),
                value: ir::PropertyInputPlan::Value(AstPropertyValue::String(
                    "inactive".to_string(),
                )),
            },
            exec::ExecMutationPlan::RemoveProperty {
                name: test_support::name("status"),
            },
        ] {
            let result = context
                .execute_mutation(ExecutionValue::Stream(vec![ExecutionRow::empty()]), &plan)
                .await
                .expect("a row without a current element is preserved");
            assert_eq!(result, ExecutionValue::Stream(vec![ExecutionRow::empty()]));
        }

        for (input, plan) in [
            (
                ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Edge(1))]),
                exec::ExecMutationPlan::Drop,
            ),
            (
                ExecutionValue::Stream(vec![ExecutionRow::empty()]),
                exec::ExecMutationPlan::DropEdge {
                    to: ir::NodeTargetPlan::Empty,
                },
            ),
            (
                ExecutionValue::Stream(Vec::new()),
                exec::ExecMutationPlan::DropEdgeByIdSource {
                    edges: ir::EdgeTargetPlan::Empty,
                },
            ),
        ] {
            let result = context
                .execute_mutation(input, &plan)
                .await
                .expect("an empty mutation domain is a successful no-op");
            assert_eq!(result, ExecutionValue::Stream(Vec::new()));
        }
    }
}
