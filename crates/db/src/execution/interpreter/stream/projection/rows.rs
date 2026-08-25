//! Stream-row projection contracts.

use std::collections::{BTreeMap, BTreeSet};

use super::*;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter::stream::projection) async fn project_stream_rows(
        &self,
        rows: Vec<ExecutionRow>,
        projection: &ir::ProjectionPlan,
    ) -> Result<ExecutionValue> {
        match projection {
            ir::ProjectionPlan::Exists => Ok(ExecutionValue::Bool(!rows.is_empty())),
            ir::ProjectionPlan::Id => {
                let mut scalars = Vec::with_capacity(rows.len());
                for row in &rows {
                    self.check_execution_deadline()?;
                    if let Some(element) = row.current.as_ref() {
                        scalars.push(match element {
                            ElementRef::Node(id) => ExecutionScalar::NodeId(*id),
                            ElementRef::Edge(id) => ExecutionScalar::EdgeId(*id),
                        });
                    }
                }
                Ok(ExecutionValue::Scalars(scalars))
            }
            ir::ProjectionPlan::Values(names) => self.project_values(&rows, names).await,
            ir::ProjectionPlan::ValueMap(selection) => {
                self.project_value_map(&rows, selection).await
            }
            ir::ProjectionPlan::Project(items) => self.project_items(&rows, items).await,
            ir::ProjectionPlan::ProjectBindings { projections, dedup } => {
                self.project_bindings(&rows, projections, *dedup).await
            }
            ir::ProjectionPlan::Label => self.project_labels(&rows).await,
            ir::ProjectionPlan::EdgeProperties => self.project_edge_properties(&rows).await,
        }
    }

    async fn project_values(
        &self,
        rows: &[ExecutionRow],
        names: &ir::PropertyNames,
    ) -> Result<ExecutionValue> {
        let mut scalars = Vec::new();
        for row in rows {
            self.check_execution_deadline()?;
            let mut resolver = eval::RowValueResolver::new(self);
            let mut object = BTreeMap::new();
            for name in names.as_ref() {
                self.check_execution_deadline()?;
                if let Some(value) = resolver.row_property(row, name).await? {
                    object.insert(name.as_ref().to_string(), value);
                }
            }
            if !object.is_empty() {
                scalars.push(ExecutionScalar::Object(object));
            }
        }
        Ok(ExecutionValue::Scalars(scalars))
    }

    async fn project_value_map(
        &self,
        rows: &[ExecutionRow],
        selection: &ir::PropertySelection,
    ) -> Result<ExecutionValue> {
        let mut scalars = Vec::with_capacity(rows.len());
        for row in rows {
            self.check_execution_deadline()?;
            let object = match selection {
                ir::PropertySelection::All => {
                    let mut object = helpers::properties_to_object(self.row_properties(row).await?);
                    if let Some(element) = row.current.as_ref() {
                        object.insert(
                            "$id".to_string(),
                            DbPropertyValue::I64(element.id().try_into().unwrap_or(i64::MAX)),
                        );
                    }
                    object
                }
                ir::PropertySelection::Selected(names) => {
                    let mut resolver = eval::RowValueResolver::new(self);
                    let mut object = BTreeMap::new();
                    for name in names.as_ref() {
                        self.check_execution_deadline()?;
                        if let Some(value) = resolver.row_property(row, name).await? {
                            object.insert(name.as_ref().to_string(), value);
                        }
                    }
                    object
                }
            };
            scalars.push(ExecutionScalar::Object(object));
        }
        Ok(ExecutionValue::Scalars(scalars))
    }

    async fn project_items(
        &self,
        rows: &[ExecutionRow],
        items: &ir::ProjectionItems,
    ) -> Result<ExecutionValue> {
        let mut scalars = Vec::with_capacity(rows.len());
        for row in rows {
            self.check_execution_deadline()?;
            let mut resolver = eval::RowValueResolver::new(self);
            let mut object = BTreeMap::new();
            for item in items.as_ref() {
                self.check_execution_deadline()?;
                match item {
                    ir::ProjectionItem::Property { source, alias } => {
                        if let Some(value) = resolver.row_property(row, source).await? {
                            object.insert(alias.as_ref().to_string(), value);
                        }
                    }
                    ir::ProjectionItem::Expr { alias, expr } => {
                        object.insert(
                            alias.as_ref().to_string(),
                            self.eval_expr_with_resolver(row, expr.expr(), &mut resolver)
                                .await?,
                        );
                    }
                }
            }
            scalars.push(ExecutionScalar::Object(object));
        }
        Ok(ExecutionValue::Scalars(scalars))
    }

    async fn project_bindings(
        &self,
        rows: &[ExecutionRow],
        projections: &ir::BindingProjectionItems,
        dedup: ir::ProjectionDedupMode,
    ) -> Result<ExecutionValue> {
        let mut scalars = Vec::with_capacity(rows.len());
        let mut seen = BTreeSet::new();
        for row in rows {
            self.check_execution_deadline()?;
            let mut resolver = eval::RowValueResolver::new(self);
            let mut object = BTreeMap::new();
            for projection in projections.as_ref() {
                self.check_execution_deadline()?;
                if let Some((alias, value)) = self
                    .binding_projection_with_resolver(row, projection, &mut resolver)
                    .await?
                {
                    object.insert(alias, value);
                }
            }
            if matches!(dedup, ir::ProjectionDedupMode::Distinct)
                && !seen.insert(format!("{object:?}"))
            {
                continue;
            }
            scalars.push(ExecutionScalar::Object(object));
        }
        Ok(ExecutionValue::Scalars(scalars))
    }

    async fn project_labels(&self, rows: &[ExecutionRow]) -> Result<ExecutionValue> {
        let label = helpers::label_property_name();
        let mut scalars = Vec::new();
        for row in rows {
            self.check_execution_deadline()?;
            if let Some(value) = row.virtual_properties.get(&label) {
                scalars.push(ExecutionScalar::Value(value));
                continue;
            }
            if let Some(value) = self
                .row_properties(row)
                .await?
                .into_iter()
                .find(|property| property.name == label.as_ref())
                .map(|property| property.value)
            {
                scalars.push(ExecutionScalar::Value(value));
            }
        }
        Ok(ExecutionValue::Scalars(scalars))
    }

    async fn project_edge_properties(&self, rows: &[ExecutionRow]) -> Result<ExecutionValue> {
        let mut scalars = Vec::new();
        for row in rows {
            self.check_execution_deadline()?;
            let Some(ElementRef::Edge(edge_id)) = row.current.as_ref() else {
                continue;
            };
            let Some((from, to)) = self.get_edge_endpoints(*edge_id).await? else {
                continue;
            };
            let mut object = helpers::properties_to_object(self.row_properties(row).await?);
            object.insert(
                "$id".to_string(),
                DbPropertyValue::I64((*edge_id).try_into().unwrap_or(i64::MAX)),
            );
            object.insert(
                "$from".to_string(),
                DbPropertyValue::I64(from.try_into().unwrap_or(i64::MAX)),
            );
            object.insert(
                "$to".to_string(),
                DbPropertyValue::I64(to.try_into().unwrap_or(i64::MAX)),
            );
            scalars.push(ExecutionScalar::Object(object));
        }
        Ok(ExecutionValue::Scalars(scalars))
    }
}
