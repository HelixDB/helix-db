//! Stream-row projection contracts.

use std::collections::{BTreeMap, BTreeSet};

use super::super::values::DistinctKey;
use super::*;
use helix_planner::relational as r;

struct NativeProjectionEvaluator<'a, 'ctx, 'db> {
    context: &'ctx ExecutionContext<'db>,
    row: &'a ExecutionRow,
    resolver: &'a mut eval::RowValueResolver<'ctx, 'db>,
}
impl r::ProjectionEvaluator<ir::ResolvedProjection> for NativeProjectionEvaluator<'_, '_, '_> {
    type Value = Option<DbPropertyValue>;
    type Error = HelixDbError;

    fn prepare(&mut self, _columns: usize) -> Result<()> {
        self.context.check_execution_deadline()
    }
    async fn evaluate(&mut self, expression: &ir::ResolvedProjection) -> Result<Self::Value> {
        self.context.check_execution_deadline()?;
        match expression.kind() {
            ir::ResolvedProjectionKind::Property { input, source } => {
                assert_eq!(*input, ir::native::CURRENT, "validated native input");
                self.resolver.row_property(self.row, source).await
            }
            ir::ResolvedProjectionKind::Expression(expression) => Box::pin(
                self.context
                    .eval_resolved(self.row, expression, self.resolver),
            )
            .await
            .map(Some),
        }
    }
}

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
        let properties = names.as_ref().iter().collect::<Vec<_>>();
        let mut scalars = Vec::new();
        for batch in rows.chunks(RECORD_BATCH_ROWS) {
            let mut resolver = eval::RowValueResolver::new(self);
            resolver.prefetch_rows(batch, &properties).await?;
            for row in batch {
                self.check_execution_deadline()?;
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
        }
        Ok(ExecutionValue::Scalars(scalars))
    }

    async fn project_value_map(
        &self,
        rows: &[ExecutionRow],
        selection: &ir::PropertySelection,
    ) -> Result<ExecutionValue> {
        let mut scalars = Vec::with_capacity(rows.len());
        for batch in rows.chunks(RECORD_BATCH_ROWS) {
            let mut resolver = eval::RowValueResolver::new(self);
            match selection {
                ir::PropertySelection::All => {
                    resolver
                        .prefetch(batch.iter().filter_map(|row| row.current.as_ref()))
                        .await?;
                }
                ir::PropertySelection::Selected(names) => {
                    resolver
                        .prefetch_rows(batch, &names.as_ref().iter().collect::<Vec<_>>())
                        .await?;
                }
            }
            for (row, last_use) in batch.iter().zip(last_uses(batch)) {
                self.check_execution_deadline()?;
                let object = match selection {
                    ir::PropertySelection::All => {
                        let mut object = helpers::properties_to_object(
                            resolver.row_properties(row, last_use).await?,
                        );
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
        }
        Ok(ExecutionValue::Scalars(scalars))
    }

    async fn project_items(
        &self,
        rows: &[ExecutionRow],
        items: &ir::ProjectionItems,
    ) -> Result<ExecutionValue> {
        // Every property column is evaluated for every row; expression columns
        // may short-circuit, so only property columns decide the prefetch.
        let properties = items
            .program()
            .iter()
            .filter_map(|projection| match projection.expression.kind() {
                ir::ResolvedProjectionKind::Property { source, .. } => Some(source),
                ir::ResolvedProjectionKind::Expression(_) => None,
            })
            .collect::<Vec<_>>();
        let mut scalars = Vec::with_capacity(rows.len());
        for batch in rows.chunks(RECORD_BATCH_ROWS) {
            let mut resolver = eval::RowValueResolver::new(self);
            resolver.prefetch_rows(batch, &properties).await?;
            for row in batch {
                self.check_execution_deadline()?;
                let mut evaluator = NativeProjectionEvaluator {
                    context: self,
                    row,
                    resolver: &mut resolver,
                };
                let values = items.program().evaluate(&mut evaluator).await?;
                let mut object = BTreeMap::new();
                for (item, value) in items.as_ref().iter().zip(values) {
                    let Some(value) = value else {
                        continue;
                    };
                    let (ir::ProjectionItem::Property { alias, .. }
                    | ir::ProjectionItem::Expr { alias, .. }) = item;
                    object.insert(alias.as_ref().to_string(), value);
                }
                scalars.push(ExecutionScalar::Object(object));
            }
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
        // DISTINCT drops duplicates as rows are projected so only the first
        // occurrence of each object is retained, never every duplicate payload.
        let mut seen = BTreeSet::new();
        for batch in rows.chunks(RECORD_BATCH_ROWS) {
            let mut records = Vec::new();
            for row in batch {
                for projection in projections.as_ref() {
                    let (target, source) = match projection {
                        ir::BindingProjectionPlan::Property { target, source, .. } => {
                            (target, source)
                        }
                        // Only the first bound reference is always read.
                        ir::BindingProjectionPlan::Coalesce { refs, .. } => {
                            let Some(value_ref) = refs.as_ref().iter().find(|value_ref| {
                                self.binding_target(row, &value_ref.target).is_some()
                            }) else {
                                continue;
                            };
                            (&value_ref.target, &value_ref.source)
                        }
                    };
                    let Some((element, virtual_properties)) = self.binding_target(row, target)
                    else {
                        continue;
                    };
                    records.extend(eval::record_read(Some(element), virtual_properties, source));
                }
            }
            let mut resolver = eval::RowValueResolver::new(self);
            resolver.prefetch(records).await?;
            for row in batch {
                self.check_execution_deadline()?;
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
                let scalar = ExecutionScalar::Object(object);
                if matches!(dedup, ir::ProjectionDedupMode::Distinct)
                    && !seen.insert(DistinctKey(scalar.clone()))
                {
                    continue;
                }
                scalars.push(scalar);
            }
        }
        Ok(ExecutionValue::Scalars(scalars))
    }

    async fn project_labels(&self, rows: &[ExecutionRow]) -> Result<ExecutionValue> {
        let label = helpers::label_property_name();
        let mut scalars = Vec::new();
        for batch in rows.chunks(RECORD_BATCH_ROWS) {
            let mut resolver = eval::RowValueResolver::new(self);
            resolver.prefetch_rows(batch, &[&label]).await?;
            for row in batch {
                self.check_execution_deadline()?;
                if let Some(value) = resolver.row_property(row, &label).await? {
                    scalars.push(ExecutionScalar::Value(value));
                }
            }
        }
        Ok(ExecutionValue::Scalars(scalars))
    }

    async fn project_edge_properties(&self, rows: &[ExecutionRow]) -> Result<ExecutionValue> {
        let mut scalars = Vec::new();
        for batch in rows.chunks(RECORD_BATCH_ROWS) {
            let mut resolver = eval::RowValueResolver::new(self);
            let edges = batch
                .iter()
                .filter_map(|row| match row.current {
                    Some(ElementRef::Edge(edge_id)) => Some(edge_id),
                    Some(ElementRef::Node(_)) | None => None,
                })
                .collect::<Vec<_>>();
            resolver.prefetch_edge_endpoints(&edges).await?;
            // Records of edges without endpoints are never projected.
            let mut present = Vec::new();
            for edge_id in edges {
                if resolver.edge_endpoints(edge_id).await?.is_some() {
                    present.push(ElementRef::Edge(edge_id));
                }
            }
            resolver.prefetch(&present).await?;
            for (row, last_use) in batch.iter().zip(last_uses(batch)) {
                self.check_execution_deadline()?;
                let Some(ElementRef::Edge(edge_id)) = row.current.as_ref() else {
                    continue;
                };
                let Some((from, to)) = resolver.edge_endpoints(*edge_id).await? else {
                    continue;
                };
                let mut object =
                    helpers::properties_to_object(resolver.row_properties(row, last_use).await?);
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
        }
        Ok(ExecutionValue::Scalars(scalars))
    }
}

/// Whether each row of `rows` is the last to use its element, so its record
/// can move out of a batch cache instead of being copied.
fn last_uses(rows: &[ExecutionRow]) -> Vec<bool> {
    let mut seen = std::collections::BTreeSet::new();
    let mut last = rows
        .iter()
        .rev()
        .map(|row| {
            row.current
                .as_ref()
                .is_some_and(|element| seen.insert(element))
        })
        .collect::<Vec<_>>();
    last.reverse();
    last
}
