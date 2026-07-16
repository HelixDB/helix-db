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
            ir::ProjectionPlan::Count => Ok(ExecutionValue::Count(rows.len())),
            ir::ProjectionPlan::Exists => Ok(ExecutionValue::Bool(!rows.is_empty())),
            ir::ProjectionPlan::Id => Ok(ExecutionValue::Scalars(
                rows.iter()
                    .filter_map(|row| row.current.as_ref())
                    .map(|element| match element {
                        ElementRef::Node(id) => ExecutionScalar::NodeId(*id),
                        ElementRef::Edge(id) => ExecutionScalar::EdgeId(*id),
                    })
                    .collect(),
            )),
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
            let mut object = BTreeMap::new();
            for name in names.as_ref() {
                if let Some(value) = self.row_property(row, name).await? {
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
            let properties = self.row_properties(row).await?;
            let object = match selection {
                ir::PropertySelection::All => {
                    let mut object = helpers::properties_to_object(properties);
                    if let Some(element) = row.current.as_ref() {
                        object.insert(
                            "$id".to_string(),
                            DbPropertyValue::I64(element.id().try_into().unwrap_or(i64::MAX)),
                        );
                    }
                    object
                }
                ir::PropertySelection::Selected(names) => {
                    let mut object = BTreeMap::new();
                    for name in names.as_ref() {
                        if let Some(value) = self.row_property(row, name).await? {
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
            let mut object = BTreeMap::new();
            for item in items.as_ref() {
                match item {
                    ir::ProjectionItem::Property { source, alias } => {
                        if let Some(value) = self.row_property(row, source).await? {
                            object.insert(alias.as_ref().to_string(), value);
                        }
                    }
                    ir::ProjectionItem::Expr { alias, expr } => {
                        object.insert(
                            alias.as_ref().to_string(),
                            self.eval_expr(row, expr.expr()).await?,
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
            let mut object = BTreeMap::new();
            for projection in projections.as_ref() {
                if let Some((alias, value)) = self.binding_projection(row, projection).await? {
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
            if let Some(value) = self.row_property(row, &label).await? {
                scalars.push(ExecutionScalar::Value(value));
            }
        }
        Ok(ExecutionValue::Scalars(scalars))
    }

    async fn project_edge_properties(&self, rows: &[ExecutionRow]) -> Result<ExecutionValue> {
        let mut scalars = Vec::new();
        for row in rows {
            if !matches!(row.current.as_ref(), Some(ElementRef::Edge(_))) {
                continue;
            }
            scalars.push(ExecutionScalar::Object(helpers::properties_to_object(
                self.row_properties(row).await?,
            )));
        }
        Ok(ExecutionValue::Scalars(scalars))
    }
}
