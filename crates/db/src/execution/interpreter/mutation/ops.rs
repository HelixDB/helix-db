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
        self.store_node(
            &scope.txn,
            id,
            &properties,
            scope.configured_indexes.runtime(),
            &mut scope.index_context,
        )
        .await?
        .merge_into(&mut scope.text_indexes);
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
            let properties = self
                .node_create_properties(&row, label, assignments)
                .await?;
            rows_and_properties.push((row, properties));
        }
        let mut scope = self.take_or_begin_write_scope().await?;
        let mut output = Vec::with_capacity(rows_and_properties.len());

        for ((row, properties), id) in rows_and_properties.into_iter().zip(ids) {
            self.store_node(
                &scope.txn,
                id,
                &properties,
                scope.configured_indexes.runtime(),
                &mut scope.index_context,
            )
            .await?
            .merge_into(&mut scope.text_indexes);

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
            let properties = self
                .edge_create_properties(&row, label, assignments)
                .await?;
            source_rows_and_properties.push((row, from, properties));
        }
        let ids = self
            .writer()?
            .edge_ids()
            .allocate_batch(edge_count.try_into().unwrap_or(u64::MAX))
            .await?;
        let mut scope = self.take_or_begin_write_scope().await?;
        let mut next_edge_id = ids.start;
        let mut output = Vec::with_capacity(edge_count);

        for (row, from, properties) in source_rows_and_properties {
            self.ensure_node_exists_in_tx(&scope.txn, from).await?;
            for to in &targets {
                self.ensure_node_exists_in_tx(&scope.txn, *to).await?;
                let edge_id = next_edge_id;
                next_edge_id += 1;
                let edge = EdgeMutationTarget::new(edge_id, from, *to);
                self.store_edge(
                    &scope.txn,
                    edge,
                    label,
                    &properties,
                    scope.configured_indexes.runtime(),
                    &mut scope.index_context,
                )
                .await?
                .merge_into(&mut scope.text_indexes);

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
            values.push(self.property_input_value(row, value).await?);
        }
        let mut scope = self.take_or_begin_write_scope().await?;

        for (row, value) in rows.iter().zip(values) {
            let property = Property::new(name.as_ref(), value);
            match row.current.as_ref() {
                Some(ElementRef::Node(node_id)) => {
                    self.set_node_property(
                        &scope.txn,
                        *node_id,
                        property,
                        scope.configured_indexes.runtime(),
                        &mut scope.index_context,
                    )
                    .await?
                    .merge_into(&mut scope.text_indexes);
                }
                Some(ElementRef::Edge(edge_id)) => {
                    if name.as_ref() == "$label" {
                        return Err(HelixDbError::Query(
                            "edge `$label` mutations are not supported by executable mutations"
                                .to_string(),
                        ));
                    }
                    self.set_edge_property(
                        &scope.txn,
                        *edge_id,
                        property,
                        scope.configured_indexes.runtime(),
                        &mut scope.index_context,
                    )
                    .await?
                    .merge_into(&mut scope.text_indexes);
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

        for row in &rows {
            match row.current.as_ref() {
                Some(ElementRef::Node(node_id)) => {
                    self.remove_node_property(
                        &scope.txn,
                        *node_id,
                        name,
                        scope.configured_indexes.runtime(),
                        &mut scope.index_context,
                    )
                    .await?
                    .merge_into(&mut scope.text_indexes);
                }
                Some(ElementRef::Edge(edge_id)) => {
                    self.remove_edge_property(
                        &scope.txn,
                        *edge_id,
                        name,
                        scope.configured_indexes.runtime(),
                        &mut scope.index_context,
                    )
                    .await?
                    .merge_into(&mut scope.text_indexes);
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
        for node_id in node_ids {
            self.delete_node(
                &scope.txn,
                node_id,
                scope.configured_indexes.runtime(),
                &mut scope.index_context,
            )
            .await?
            .merge_into(&mut scope.text_indexes);
        }
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
        let mut edge_ids = BTreeSet::new();
        for from in source_nodes {
            for target in &targets {
                let pair_ids = crate::search::lookup_edge_pair_index_scoped(
                    &scope.txn,
                    from,
                    *target,
                    self.tenant_scope,
                )
                .await?;
                for edge_id in pair_ids {
                    if self.edge_matches_label(&scope.txn, edge_id, label).await? {
                        edge_ids.insert(edge_id);
                    }
                }
            }
        }
        for edge_id in edge_ids {
            self.delete_edge(
                &scope.txn,
                edge_id,
                scope.configured_indexes.runtime(),
                &mut scope.index_context,
            )
            .await?
            .merge_into(&mut scope.text_indexes);
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
        for edge_id in edge_ids {
            self.delete_edge(
                &scope.txn,
                edge_id,
                scope.configured_indexes.runtime(),
                &mut scope.index_context,
            )
            .await?
            .merge_into(&mut scope.text_indexes);
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
}
