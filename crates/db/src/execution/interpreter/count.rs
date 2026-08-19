//! Literal execution of planner-selected cardinality programs.
//!
//! Every match arm implements the primitive named by executable IR. This
//! module may evaluate encoded arithmetic and execute cursor nodes, but it does
//! not substitute access families, batch leaves, reorder children, or push
//! windows across cursor operators.

use std::collections::BTreeSet;

use futures::future::BoxFuture;
use futures::FutureExt;
use helix_ast::query::QueryValue;
use helix_ast::value::PropertyValue as AstPropertyValue;
use helix_planner::{exec, ir, properties};

use super::access::SearchReadLimit;
use super::*;
use crate::config::{TextElementType, VectorElementType};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EvaluatedCountWindow {
    skip: usize,
    take: Option<usize>,
}

// Keep leaf and recursive structural futures separate. Besides making a
// category mismatch unrepresentable, this bounds each recursive cursor frame
// by the structural operations instead of the much larger set of leaf reads.
enum CountCursorLeaf<'a> {
    EmptyRows,
    InputRows,
    NodeBitmap(&'a exec::ExecNodeBitmapExpr),
    EdgeBitmap(&'a exec::ExecEdgeBitmapExpr),
    NodeUnique {
        lookup: &'a exec::ExecNodeUniqueOwnerReadPlan,
        verification: &'a exec::ExecNodeAuthoritativeVerificationPlan,
    },
    NodeRange(&'a exec::ExecNodeVerifiedRangeScanPlan),
    EdgeRange(&'a exec::ExecEdgeVerifiedRangeScanPlan),
    NodeAuthoritativeScan(&'a exec::ExecNodeAuthoritativeScanPredicate),
    EdgeAuthoritativeScan(&'a exec::ExecEdgeAuthoritativeScanPredicate),
    NodePointReads(&'a ir::ElementIds),
    EdgePointReads(&'a ir::ElementIds),
    NodeRuntimeInput(&'a exec::ExecRuntimeInputPlan),
    EdgeRuntimeInput(&'a exec::ExecRuntimeInputPlan),
    RuntimeInput(&'a exec::ExecRuntimeInputPlan),
    NodeFullScan,
    EdgeFullScan,
    NodeLabelBitmap(&'a ir::NonEmptyString),
    EdgeLabelBitmap(&'a ir::NonEmptyString),
    NodeVectorSearch {
        key: &'a helix_planner::catalog::NodeSearchIndexKey,
        index: &'a ir::SearchIndexPlan,
        query_vector: &'a ir::VectorQueryInputPlan,
        k: &'a ir::SearchLimitPlan,
    },
    EdgeVectorSearch {
        key: &'a helix_planner::catalog::EdgeSearchIndexKey,
        index: &'a ir::SearchIndexPlan,
        query_vector: &'a ir::VectorQueryInputPlan,
        k: &'a ir::SearchLimitPlan,
    },
    NodeTextSearch {
        key: &'a helix_planner::catalog::NodeSearchIndexKey,
        index: &'a ir::SearchIndexPlan,
        query_text: &'a ir::TextQueryInputPlan,
        k: &'a ir::SearchLimitPlan,
    },
    EdgeTextSearch {
        key: &'a helix_planner::catalog::EdgeSearchIndexKey,
        index: &'a ir::SearchIndexPlan,
        query_text: &'a ir::TextQueryInputPlan,
        k: &'a ir::SearchLimitPlan,
    },
    NodeDynamicEquality {
        index: &'a helix_planner::catalog::NodeEqualityIndexMeta,
        key: &'a helix_planner::catalog::ScopedPropertyKey,
        param: &'a ir::NonEmptyString,
    },
    EdgeDynamicEquality {
        index: &'a helix_planner::catalog::EdgeEqualityIndexMeta,
        key: &'a helix_planner::catalog::ScopedPropertyKey,
        param: &'a ir::NonEmptyString,
    },
}

enum CountCursorStructural<'a> {
    Union {
        driver: &'a exec::ExecCountCursorPlan,
        rest: &'a ir::AtLeast<exec::ExecCountCursorPlan, 1>,
    },
    Intersect {
        driver: &'a exec::ExecCountCursorPlan,
        rest: &'a ir::AtLeast<exec::ExecCountCursorPlan, 1>,
    },
    Filter {
        input: &'a exec::ExecCountCursorPlan,
        predicate: &'a ir::PredicatePlan,
    },
    Window {
        input: &'a exec::ExecCountCursorPlan,
        window: &'a exec::ExecCountWindowPlan,
    },
    Order {
        input: &'a exec::ExecCountCursorPlan,
        plan: &'a ir::OrderPlan,
    },
    Expand {
        input: &'a exec::ExecCountCursorPlan,
        plan: &'a ir::ExpandPlan,
    },
    VectorSearch {
        input: &'a exec::ExecCountCursorPlan,
        plan: &'a ir::RestrictedVectorSearchPlan,
    },
    TextSearch {
        input: &'a exec::ExecCountCursorPlan,
        plan: &'a ir::RestrictedTextSearchPlan,
    },
    Variable {
        input: &'a exec::ExecCountCursorPlan,
        op: &'a helix_planner::logical::PureStreamVariableOp,
    },
    Distinct {
        input: &'a exec::ExecCountCursorPlan,
        plan: exec::ExecCountDistinctPlan,
    },
}

impl EvaluatedCountWindow {
    fn apply(self, cardinality: usize) -> usize {
        let cardinality = cardinality.saturating_sub(self.skip);
        self.take.map_or(cardinality, |take| cardinality.min(take))
    }

    fn apply_rows(self, rows: Vec<ExecutionRow>) -> Vec<ExecutionRow> {
        let rows = rows.into_iter().skip(self.skip);
        match self.take {
            Some(take) => rows.take(take).collect(),
            None => rows.collect(),
        }
    }

    fn threshold(self) -> Option<usize> {
        self.take.map(|take| self.skip.saturating_add(take))
    }
}

fn count_plan_window(plan: &exec::ExecCountPlan) -> Option<&exec::ExecCountWindowPlan> {
    match plan {
        exec::ExecCountPlan::Constant(_) => None,
        exec::ExecCountPlan::NodeBitmap(plan) => Some(&plan.window),
        exec::ExecCountPlan::EdgeBitmap(plan) => Some(&plan.window),
        exec::ExecCountPlan::NodeUnique(plan) => Some(&plan.window),
        exec::ExecCountPlan::NodeRange(plan) => Some(&plan.window),
        exec::ExecCountPlan::EdgeRange(plan) => Some(&plan.window),
        exec::ExecCountPlan::NodeAuthoritativeScan(plan) => Some(&plan.window),
        exec::ExecCountPlan::EdgeAuthoritativeScan(plan) => Some(&plan.window),
        exec::ExecCountPlan::NodePointReads { window, .. }
        | exec::ExecCountPlan::EdgePointReads { window, .. }
        | exec::ExecCountPlan::NodeRuntimeInput { window, .. }
        | exec::ExecCountPlan::EdgeRuntimeInput { window, .. }
        | exec::ExecCountPlan::RuntimeInput { window, .. }
        | exec::ExecCountPlan::NodeFullScan { window }
        | exec::ExecCountPlan::EdgeFullScan { window }
        | exec::ExecCountPlan::NodeLabelBitmap { window, .. }
        | exec::ExecCountPlan::EdgeLabelBitmap { window, .. }
        | exec::ExecCountPlan::InputRows { window }
        | exec::ExecCountPlan::InputScalars { window } => Some(window),
        exec::ExecCountPlan::NodeVectorSearch(plan) => Some(&plan.window),
        exec::ExecCountPlan::EdgeVectorSearch(plan) => Some(&plan.window),
        exec::ExecCountPlan::NodeTextSearch(plan) => Some(&plan.window),
        exec::ExecCountPlan::EdgeTextSearch(plan) => Some(&plan.window),
        exec::ExecCountPlan::NodeDynamicEquality(plan) => Some(&plan.window),
        exec::ExecCountPlan::EdgeDynamicEquality(plan) => Some(&plan.window),
        exec::ExecCountPlan::Stream(plan) => Some(&plan.window),
    }
}

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn execute_count(
        &mut self,
        input: ExecutionValue,
        plan: &exec::ExecCountPlan,
    ) -> Result<ExecutionValue> {
        plan.validate().map_err(|error| {
            HelixDbError::InvariantViolation(format!("invalid count program: {error:?}"))
        })?;
        let evaluated_window = count_plan_window(plan)
            .map(|window| self.count_window(window))
            .transpose()?;
        let count = match plan {
            exec::ExecCountPlan::Constant(count) => *count,
            exec::ExecCountPlan::NodeBitmap(plan) => {
                let window = evaluated_window.expect("bitmap counts carry a window");
                window.apply(self.node_bitmap(&plan.bitmap).await?.len() as usize)
            }
            exec::ExecCountPlan::EdgeBitmap(plan) => {
                let window = evaluated_window.expect("bitmap counts carry a window");
                window.apply(self.edge_bitmap(&plan.bitmap).await?.len() as usize)
            }
            exec::ExecCountPlan::NodeUnique(plan) => {
                let window = evaluated_window.expect("unique counts carry a window");
                let read = self.verified_node_unique_owner(&plan.lookup, &plan.verification);
                window.apply(usize::from(read.await?.is_some()))
            }
            exec::ExecCountPlan::NodeRange(plan) => {
                validate_range_index("node_range:", &plan.driver.index.index_id, &plan.driver.key)?;
                let window = evaluated_window.expect("range counts carry a window");
                let filters = match &plan.membership {
                    exec::ExecNodeRangeMembershipPlan::All => Vec::new(),
                    exec::ExecNodeRangeMembershipPlan::BitmapFilters(filters) => {
                        let mut bitmaps = Vec::with_capacity(filters.as_ref().len());
                        for filter in filters {
                            bitmaps.push(self.node_bitmap(filter).await?);
                        }
                        bitmaps
                    }
                };
                let read = self.node_range_index_count_with_membership(
                    &plan.driver.key,
                    &plan.driver.range,
                    &filters,
                    window.threshold(),
                );
                let accepted = read.await?;
                window.apply(accepted)
            }
            exec::ExecCountPlan::EdgeRange(plan) => {
                validate_range_index("edge_range:", &plan.driver.index.index_id, &plan.driver.key)?;
                let window = evaluated_window.expect("range counts carry a window");
                let filters = match &plan.membership {
                    exec::ExecEdgeRangeMembershipPlan::All => Vec::new(),
                    exec::ExecEdgeRangeMembershipPlan::BitmapFilters(filters) => {
                        let mut bitmaps = Vec::with_capacity(filters.as_ref().len());
                        for filter in filters {
                            bitmaps.push(self.edge_bitmap(filter).await?);
                        }
                        bitmaps
                    }
                };
                let read = self.edge_range_index_count_with_membership(
                    &plan.driver.key,
                    &plan.driver.range,
                    &filters,
                    window.threshold(),
                );
                let accepted = read.await?;
                window.apply(accepted)
            }
            exec::ExecCountPlan::NodeAuthoritativeScan(plan) => {
                let window = evaluated_window.expect("scan counts carry a window");
                let read = self.authoritative_node_rows(&plan.predicate, window.threshold());
                let rows = read.await?;
                window.apply(rows)
            }
            exec::ExecCountPlan::EdgeAuthoritativeScan(plan) => {
                let window = evaluated_window.expect("scan counts carry a window");
                let read = self.authoritative_edge_rows(&plan.predicate, window.threshold());
                let rows = read.await?;
                window.apply(rows)
            }
            exec::ExecCountPlan::NodePointReads { ids, .. } => {
                let window = evaluated_window.expect("point-read counts carry a window");
                let read = self.existing_node_count(ids.as_ref(), window.threshold());
                window.apply(read.await?)
            }
            exec::ExecCountPlan::EdgePointReads { ids, .. } => {
                let window = evaluated_window.expect("point-read counts carry a window");
                let read = self.existing_edge_count(ids.as_ref(), window.threshold());
                window.apply(read.await?)
            }
            exec::ExecCountPlan::NodeRuntimeInput { input, .. } => {
                let window = evaluated_window.expect("runtime counts carry a window");
                let ids = self.runtime_ids(input)?;
                window.apply(self.existing_node_count(&ids, window.threshold()).await?)
            }
            exec::ExecCountPlan::EdgeRuntimeInput { input, .. } => {
                let window = evaluated_window.expect("runtime counts carry a window");
                let ids = self.runtime_ids(input)?;
                window.apply(self.existing_edge_count(&ids, window.threshold()).await?)
            }
            exec::ExecCountPlan::RuntimeInput { input, .. } => {
                let window = evaluated_window.expect("runtime counts carry a window");
                window.apply(self.runtime_row_count(input)?)
            }
            exec::ExecCountPlan::NodeFullScan { .. } => {
                let window = evaluated_window.expect("full-scan counts carry a window");
                let limit = positive_limit(window.threshold());
                let read = self.scan_element_ids(exec::ElementKeyspace::NodeProperty, limit);
                let ids = read.await?;
                window.apply(ids.len())
            }
            exec::ExecCountPlan::EdgeFullScan { .. } => {
                let window = evaluated_window.expect("full-scan counts carry a window");
                let limit = positive_limit(window.threshold());
                let read = self.scan_element_ids(exec::ElementKeyspace::EdgeEndpoints, limit);
                let ids = read.await?;
                window.apply(ids.len())
            }
            exec::ExecCountPlan::NodeLabelBitmap { label, .. } => {
                let window = evaluated_window.expect("label counts carry a window");
                let value = DbPropertyValue::String(label.as_ref().to_string());
                let read = self.lookup_equality_index_set("$label", &value);
                let bitmap = read.await?;
                window.apply(bitmap.len() as usize)
            }
            exec::ExecCountPlan::EdgeLabelBitmap { label, .. } => {
                let window = evaluated_window.expect("label counts carry a window");
                let bitmap = self.lookup_global_edge_label_index(label.as_ref()).await?;
                window.apply(bitmap.len() as usize)
            }
            exec::ExecCountPlan::NodeVectorSearch(plan) => {
                let window = evaluated_window.expect("search counts carry a window");
                let read = self.vector_search_results(
                    VectorElementType::Node,
                    &plan.key.label,
                    &plan.key.property,
                    &plan.index,
                    &plan.query_vector,
                    SearchReadLimit::new(&plan.k, None),
                );
                let results = read.await?;
                let ids = results
                    .into_iter()
                    .map(|result| result.entity_id().local_id())
                    .collect::<Vec<_>>();
                return self
                    .existing_node_count(&ids, window.threshold())
                    .await
                    .map(|count| ExecutionValue::Count(window.apply(count)));
            }
            exec::ExecCountPlan::EdgeVectorSearch(plan) => {
                let window = evaluated_window.expect("search counts carry a window");
                let read = self.vector_search_results(
                    VectorElementType::Edge,
                    &plan.key.label,
                    &plan.key.property,
                    &plan.index,
                    &plan.query_vector,
                    SearchReadLimit::new(&plan.k, None),
                );
                let results = read.await?;
                let ids = results
                    .into_iter()
                    .map(|result| result.entity_id().local_id())
                    .collect::<Vec<_>>();
                return self
                    .existing_edge_count(&ids, window.threshold())
                    .await
                    .map(|count| ExecutionValue::Count(window.apply(count)));
            }
            exec::ExecCountPlan::NodeTextSearch(plan) => {
                let window = evaluated_window.expect("search counts carry a window");
                let read = self.text_search_hits(
                    TextElementType::Node,
                    &plan.key.label,
                    &plan.key.property,
                    &plan.index,
                    &plan.query_text,
                    SearchReadLimit::new(&plan.k, None),
                );
                let results = read.await?;
                let ids = results
                    .into_iter()
                    .map(|result| result.entity_id)
                    .collect::<Vec<_>>();
                return self
                    .existing_node_count(&ids, window.threshold())
                    .await
                    .map(|count| ExecutionValue::Count(window.apply(count)));
            }
            exec::ExecCountPlan::EdgeTextSearch(plan) => {
                let window = evaluated_window.expect("search counts carry a window");
                let read = self.text_search_hits(
                    TextElementType::Edge,
                    &plan.key.label,
                    &plan.key.property,
                    &plan.index,
                    &plan.query_text,
                    SearchReadLimit::new(&plan.k, None),
                );
                let results = read.await?;
                let ids = results
                    .into_iter()
                    .map(|result| result.entity_id)
                    .collect::<Vec<_>>();
                return self
                    .existing_edge_count(&ids, window.threshold())
                    .await
                    .map(|count| ExecutionValue::Count(window.apply(count)));
            }
            exec::ExecCountPlan::NodeDynamicEquality(plan) => {
                validate_node_equality_index(&plan.index.index_id, &plan.key)?;
                let window = evaluated_window.expect("dynamic counts carry a window");
                let value = self.param_value(&plan.param)?;
                let read = self.lookup_managed_equality_union(
                    crate::index_lifecycle::IndexElementKind::Node,
                    &plan.key,
                    core::slice::from_ref(&value),
                );
                let ids = read.await?;
                window.apply(ids.len() as usize)
            }
            exec::ExecCountPlan::EdgeDynamicEquality(plan) => {
                validate_edge_equality_index(&plan.index.index_id, &plan.key)?;
                let window = evaluated_window.expect("dynamic counts carry a window");
                let value = self.param_value(&plan.param)?;
                let read = self.lookup_managed_equality_union(
                    crate::index_lifecycle::IndexElementKind::Edge,
                    &plan.key,
                    core::slice::from_ref(&value),
                );
                let ids = read.await?;
                window.apply(ids.len() as usize)
            }
            exec::ExecCountPlan::Stream(plan) => {
                let mut dependency = Some(input);
                self.count_cursor_cardinality(
                    &plan.cursor,
                    &mut dependency,
                    evaluated_window.expect("stream counts carry a window"),
                )
                .await?
            }
            exec::ExecCountPlan::InputRows { .. } => {
                let rows = match input {
                    ExecutionValue::Stream(rows) => rows.len(),
                    other @ (ExecutionValue::FoldedStream(_)
                    | ExecutionValue::Count(_)
                    | ExecutionValue::Bool(_)
                    | ExecutionValue::Scalars(_)
                    | ExecutionValue::IndexDdlReceipt(_)
                    | ExecutionValue::IndexOperationStatus(_)) => {
                        return Err(count_shape_error("rows", &other));
                    }
                };
                evaluated_window
                    .expect("row-input counts carry a window")
                    .apply(rows)
            }
            exec::ExecCountPlan::InputScalars { .. } => {
                let scalars = match input {
                    ExecutionValue::Count(_) | ExecutionValue::Bool(_) => 1,
                    ExecutionValue::Scalars(values) => values.len(),
                    other @ (ExecutionValue::Stream(_)
                    | ExecutionValue::FoldedStream(_)
                    | ExecutionValue::IndexDdlReceipt(_)
                    | ExecutionValue::IndexOperationStatus(_)) => {
                        return Err(count_shape_error("scalar items", &other));
                    }
                };
                evaluated_window
                    .expect("scalar-input counts carry a window")
                    .apply(scalars)
            }
        };
        Ok(ExecutionValue::Count(count))
    }

    fn count_window(&self, plan: &exec::ExecCountWindowPlan) -> Result<EvaluatedCountWindow> {
        let mut resolve = |name: &ir::NonEmptyString| self.count_bound_param(name);
        let skip = plan.skip.evaluate(&mut resolve)?;
        let take = match &plan.take {
            exec::ExecCountTake::All => None,
            exec::ExecCountTake::AtMost(take) => Some(take.evaluate(&mut resolve)?),
        };
        Ok(EvaluatedCountWindow { skip, take })
    }

    fn count_bound_param(&self, name: &ir::NonEmptyString) -> Result<usize> {
        if let Some(value) = self.params.values.get(name) {
            let AstPropertyValue::I64(value) = value else {
                return Err(HelixDbError::Query(format!(
                    "count window parameter `{name}` is not a non-negative integer"
                )));
            };
            return usize::try_from(*value).map_err(|_| {
                HelixDbError::Query(format!(
                    "count window parameter `{name}` is not a non-negative integer"
                ))
            });
        }
        let Some(value) = self.params.query_values.get(name) else {
            return Err(HelixDbError::Query(format!(
                "count window parameter `{name}` is not bound"
            )));
        };
        let QueryValue::I64(value) = value else {
            return Err(HelixDbError::Query(format!(
                "count window parameter `{name}` is not a non-negative integer"
            )));
        };
        usize::try_from(*value).map_err(|_| {
            HelixDbError::Query(format!(
                "count window parameter `{name}` is not a non-negative integer"
            ))
        })
    }

    fn runtime_ids(&self, input: &exec::ExecRuntimeInputPlan) -> Result<Vec<u64>> {
        match input {
            exec::ExecRuntimeInputPlan::Param(param) => self.param_ids(param),
            exec::ExecRuntimeInputPlan::Variable(variable) => {
                let value = self.variable_value(variable)?;
                Ok(match value {
                    ExecutionValue::Stream(rows) => rows
                        .iter()
                        .filter_map(|row| row.current.as_ref().map(ElementRef::id))
                        .collect::<Vec<_>>(),
                    other @ (ExecutionValue::FoldedStream(_)
                    | ExecutionValue::Count(_)
                    | ExecutionValue::Bool(_)
                    | ExecutionValue::Scalars(_)
                    | ExecutionValue::IndexDdlReceipt(_)
                    | ExecutionValue::IndexOperationStatus(_)) => {
                        return Err(count_shape_error("element rows", other));
                    }
                })
            }
        }
    }

    fn runtime_row_count(&self, input: &exec::ExecRuntimeInputPlan) -> Result<usize> {
        match input {
            exec::ExecRuntimeInputPlan::Param(param) => self.param_ids(param).map(|ids| ids.len()),
            exec::ExecRuntimeInputPlan::Variable(variable) => {
                match self.variable_value(variable)? {
                    ExecutionValue::Stream(rows) => Ok(rows.len()),
                    other @ (ExecutionValue::FoldedStream(_)
                    | ExecutionValue::Count(_)
                    | ExecutionValue::Bool(_)
                    | ExecutionValue::Scalars(_)
                    | ExecutionValue::IndexDdlReceipt(_)
                    | ExecutionValue::IndexOperationStatus(_)) => {
                        Err(count_shape_error("rows", other))
                    }
                }
            }
        }
    }

    async fn existing_node_count(&self, ids: &[u64], threshold: Option<usize>) -> Result<usize> {
        let mut count = 0usize;
        for id in ids {
            if threshold.is_some_and(|threshold| count >= threshold) {
                break;
            }
            let key = keys::Key::Data {
                scope: self.tenant_scope,
                kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(*id)),
            }
            .to_bytes();
            if self.get_raw(&key).await?.is_some() {
                count = count.saturating_add(1);
            }
        }
        Ok(count)
    }

    async fn existing_edge_count(&self, ids: &[u64], threshold: Option<usize>) -> Result<usize> {
        let mut count = 0usize;
        for id in ids {
            if threshold.is_some_and(|threshold| count >= threshold) {
                break;
            }
            let key = keys::Key::Data {
                scope: self.tenant_scope,
                kind: keys::DataKeyKind::EdgeEndpoints(keys::EdgeEndpointsKey::new(*id)),
            }
            .to_bytes();
            if self.get_raw(&key).await?.is_some() {
                count = count.saturating_add(1);
            }
        }
        Ok(count)
    }

    async fn authoritative_node_rows(
        &self,
        predicate: &exec::ExecNodeAuthoritativeScanPredicate,
        threshold: Option<usize>,
    ) -> Result<usize> {
        let read = self.scan_element_ids(exec::ElementKeyspace::NodeProperty, None);
        let ids = read.await?;
        let mut accepted = 0usize;
        for id in ids {
            if threshold.is_some_and(|threshold| accepted >= threshold) {
                break;
            }
            let row = ExecutionRow::current(ElementRef::Node(id));
            let matches = match predicate {
                exec::ExecNodeAuthoritativeScanPredicate::NullEquality { key } => {
                    let read = self.scoped_null_matches(&row, key);
                    read.await?
                }
                exec::ExecNodeAuthoritativeScanPredicate::Predicate(predicate) => {
                    let read = self.eval_predicate(&row, predicate.predicate());
                    read.await?
                }
            };
            if matches {
                accepted = accepted.saturating_add(1);
            }
        }
        Ok(accepted)
    }

    async fn authoritative_edge_rows(
        &self,
        predicate: &exec::ExecEdgeAuthoritativeScanPredicate,
        threshold: Option<usize>,
    ) -> Result<usize> {
        let read = self.scan_element_ids(exec::ElementKeyspace::EdgeEndpoints, None);
        let ids = read.await?;
        let mut accepted = 0usize;
        for id in ids {
            if threshold.is_some_and(|threshold| accepted >= threshold) {
                break;
            }
            let row = ExecutionRow::current(ElementRef::Edge(id));
            let matches = match predicate {
                exec::ExecEdgeAuthoritativeScanPredicate::NullEquality { key } => {
                    let read = self.scoped_null_matches(&row, key);
                    read.await?
                }
                exec::ExecEdgeAuthoritativeScanPredicate::Predicate(predicate) => {
                    let read = self.eval_predicate(&row, predicate.predicate());
                    read.await?
                }
            };
            if matches {
                accepted = accepted.saturating_add(1);
            }
        }
        Ok(accepted)
    }

    pub(in crate::execution::interpreter) async fn scoped_null_matches(
        &self,
        row: &ExecutionRow,
        key: &helix_planner::catalog::ScopedPropertyKey,
    ) -> Result<bool> {
        let read = self.row_properties(row);
        let properties = read.await?;
        if properties
            .iter()
            .find(|property| property.name == "$label")
            .and_then(|property| property.value.as_str())
            != Some(key.label.as_ref())
        {
            return Ok(false);
        }
        Ok(properties
            .iter()
            .find(|property| property.name == key.property.as_ref())
            .is_none_or(|property| matches!(property.value, DbPropertyValue::Null)))
    }

    /// Execute the exact unique-owner primitive and verify the authoritative row.
    ///
    /// An absent owner is a legitimate miss. A present owner whose label or
    /// indexed value disagrees with the plan is catalog corruption, never a
    /// reason to substitute an authoritative scan.
    pub(in crate::execution::interpreter) async fn verified_node_unique_owner(
        &self,
        lookup: &exec::ExecNodeUniqueOwnerReadPlan,
        verification: &exec::ExecNodeAuthoritativeVerificationPlan,
    ) -> Result<Option<u64>> {
        validate_node_equality_index(&lookup.index.metadata().index_id, &lookup.key)?;
        if lookup.key != verification.key || lookup.value != verification.value {
            return Err(HelixDbError::InvariantViolation(
                "unique verification does not match its exact owner lookup".to_string(),
            ));
        }
        let lookup_value = indexed_value(&lookup.value);
        let read = self.lookup_managed_equality_point_exact(
            crate::index_lifecycle::IndexElementKind::Node,
            &lookup.key,
            &lookup_value,
            true,
        );
        let ids = read.await?;
        let mut ids = ids.into_iter();
        let Some(id) = ids.next() else {
            return Ok(None);
        };
        let row = ExecutionRow::current(ElementRef::Node(id));
        let expected = indexed_value(&verification.value);
        crate::index_lifecycle::secondary::record_equality_graph_read();
        let read = self.row_properties(&row);
        let properties = read.await?;
        let label_matches = properties
            .iter()
            .find(|property| property.name == "$label")
            .and_then(|property| property.value.as_str())
            == Some(verification.key.label.as_ref());
        let value_matches = properties
            .iter()
            .find(|property| property.name == verification.key.property.as_ref())
            .is_some_and(|property| property.value.eq_value(&expected));
        if !label_matches || !value_matches {
            return Err(HelixDbError::IndexCatalogCorruption(
                "unique equality owner disagrees with its authoritative node".to_string(),
            ));
        }
        Ok(Some(id))
    }

    pub(in crate::execution::interpreter) fn node_bitmap<'a>(
        &'a self,
        expression: &'a exec::ExecNodeBitmapExpr,
    ) -> BoxFuture<'a, Result<roaring::RoaringTreemap>> {
        async move {
            self.check_execution_deadline()?;
            match expression {
                exec::ExecNodeBitmapExpr::PointRead { index, key, value } => {
                    validate_node_equality_index(&index.metadata().index_id, key)?;
                    self.lookup_managed_equality_point_exact(
                        crate::index_lifecycle::IndexElementKind::Node,
                        key,
                        &indexed_value(value),
                        false,
                    )
                    .await
                }
                exec::ExecNodeBitmapExpr::BatchedUnionRead { index, key, values } => {
                    validate_node_equality_index(&index.metadata().index_id, key)?;
                    let values = values.iter().map(indexed_value).collect::<Vec<_>>();
                    self.lookup_managed_equality_literal_batch(
                        crate::index_lifecycle::IndexElementKind::Node,
                        key,
                        &values,
                    )
                    .await
                }
                exec::ExecNodeBitmapExpr::Union { driver, rest } => {
                    let mut result = self.node_bitmap(driver).await?;
                    for child in rest {
                        let read = self.node_bitmap(child);
                        let child = match read.await {
                            Ok(child) => child,
                            Err(error) => return Err(error),
                        };
                        result |= child;
                    }
                    Ok(result)
                }
                exec::ExecNodeBitmapExpr::Intersect { driver, rest } => {
                    let mut result = self.node_bitmap(driver).await?;
                    for child in rest {
                        let read = self.node_bitmap(child);
                        let child = match read.await {
                            Ok(child) => child,
                            Err(error) => return Err(error),
                        };
                        result &= child;
                    }
                    Ok(result)
                }
            }
        }
        .boxed()
    }

    pub(in crate::execution::interpreter) fn edge_bitmap<'a>(
        &'a self,
        expression: &'a exec::ExecEdgeBitmapExpr,
    ) -> BoxFuture<'a, Result<roaring::RoaringTreemap>> {
        async move {
            self.check_execution_deadline()?;
            match expression {
                exec::ExecEdgeBitmapExpr::PointRead { index, key, value } => {
                    validate_edge_equality_index(&index.metadata().index_id, key)?;
                    self.lookup_managed_equality_point_exact(
                        crate::index_lifecycle::IndexElementKind::Edge,
                        key,
                        &indexed_value(value),
                        false,
                    )
                    .await
                }
                exec::ExecEdgeBitmapExpr::BatchedUnionRead { index, key, values } => {
                    validate_edge_equality_index(&index.metadata().index_id, key)?;
                    let values = values.iter().map(indexed_value).collect::<Vec<_>>();
                    self.lookup_managed_equality_literal_batch(
                        crate::index_lifecycle::IndexElementKind::Edge,
                        key,
                        &values,
                    )
                    .await
                }
                exec::ExecEdgeBitmapExpr::Union { driver, rest } => {
                    let mut result = self.edge_bitmap(driver).await?;
                    for child in rest {
                        let read = self.edge_bitmap(child);
                        let child = match read.await {
                            Ok(child) => child,
                            Err(error) => return Err(error),
                        };
                        result |= child;
                    }
                    Ok(result)
                }
                exec::ExecEdgeBitmapExpr::Intersect { driver, rest } => {
                    let mut result = self.edge_bitmap(driver).await?;
                    for child in rest {
                        let read = self.edge_bitmap(child);
                        let child = match read.await {
                            Ok(child) => child,
                            Err(error) => return Err(error),
                        };
                        result &= child;
                    }
                    Ok(result)
                }
            }
        }
        .boxed()
    }

    /// Count an exact recursive cursor without constructing its terminal row collection.
    ///
    /// Identity-sensitive cursor nodes still call `count_cursor`, which materializes only
    /// because their selected primitive requires complete rows. Source cursors and the
    /// terminal filter/window nodes execute directly to a scalar and honor the encoded
    /// accepted-row threshold.
    fn count_cursor_cardinality<'a>(
        &'a mut self,
        cursor: &'a exec::ExecCountCursorPlan,
        dependency: &'a mut Option<ExecutionValue>,
        window: EvaluatedCountWindow,
    ) -> BoxFuture<'a, Result<usize>> {
        async move {
            self.check_execution_deadline()?;
            let cardinality = match cursor {
                exec::ExecCountCursorPlan::EmptyRows => 0,
                exec::ExecCountCursorPlan::InputRows => match dependency.take() {
                    Some(ExecutionValue::Stream(rows)) => rows.len(),
                    Some(other) => return Err(count_shape_error("rows", &other)),
                    None => {
                        return Err(HelixDbError::InvariantViolation(
                            "count cursor consumed its row dependency more than once".to_string(),
                        ));
                    }
                },
                exec::ExecCountCursorPlan::NodeBitmap(bitmap) => {
                    self.node_bitmap(bitmap).await?.len() as usize
                }
                exec::ExecCountCursorPlan::EdgeBitmap(bitmap) => {
                    self.edge_bitmap(bitmap).await?.len() as usize
                }
                exec::ExecCountCursorPlan::NodeUnique {
                    lookup,
                    verification,
                } => usize::from(
                    self.verified_node_unique_owner(lookup, verification)
                        .await?
                        .is_some(),
                ),
                exec::ExecCountCursorPlan::NodeRange(plan) => {
                    validate_range_index("node_range:", &plan.index.index_id, &plan.key)?;
                    self.node_range_index_count_with_membership(
                        &plan.key,
                        &plan.range,
                        &[],
                        window.threshold(),
                    )
                    .await?
                }
                exec::ExecCountCursorPlan::EdgeRange(plan) => {
                    validate_range_index("edge_range:", &plan.index.index_id, &plan.key)?;
                    self.edge_range_index_count_with_membership(
                        &plan.key,
                        &plan.range,
                        &[],
                        window.threshold(),
                    )
                    .await?
                }
                exec::ExecCountCursorPlan::NodeAuthoritativeScan(predicate) => {
                    self.authoritative_node_rows(predicate, window.threshold())
                        .await?
                }
                exec::ExecCountCursorPlan::EdgeAuthoritativeScan(predicate) => {
                    self.authoritative_edge_rows(predicate, window.threshold())
                        .await?
                }
                exec::ExecCountCursorPlan::NodePointReads(ids) => {
                    self.existing_node_count(ids.as_ref(), window.threshold())
                        .await?
                }
                exec::ExecCountCursorPlan::EdgePointReads(ids) => {
                    self.existing_edge_count(ids.as_ref(), window.threshold())
                        .await?
                }
                exec::ExecCountCursorPlan::NodeRuntimeInput(input) => {
                    let ids = self.runtime_ids(input)?;
                    self.existing_node_count(&ids, window.threshold()).await?
                }
                exec::ExecCountCursorPlan::EdgeRuntimeInput(input) => {
                    let ids = self.runtime_ids(input)?;
                    self.existing_edge_count(&ids, window.threshold()).await?
                }
                exec::ExecCountCursorPlan::RuntimeInput(input) => self.runtime_row_count(input)?,
                exec::ExecCountCursorPlan::NodeFullScan => {
                    let limit = positive_limit(window.threshold());
                    self.scan_element_ids(exec::ElementKeyspace::NodeProperty, limit)
                        .await?
                        .len()
                }
                exec::ExecCountCursorPlan::EdgeFullScan => {
                    let limit = positive_limit(window.threshold());
                    self.scan_element_ids(exec::ElementKeyspace::EdgeEndpoints, limit)
                        .await?
                        .len()
                }
                exec::ExecCountCursorPlan::NodeLabelBitmap(label) => {
                    let value = DbPropertyValue::String(label.as_ref().to_string());
                    self.lookup_equality_index_set("$label", &value)
                        .await?
                        .len() as usize
                }
                exec::ExecCountCursorPlan::EdgeLabelBitmap(label) => {
                    self.lookup_global_edge_label_index(label.as_ref())
                        .await?
                        .len() as usize
                }
                exec::ExecCountCursorPlan::NodeVectorSearch {
                    key,
                    index,
                    query_vector,
                    k,
                } => {
                    let results = self
                        .vector_search_results(
                            VectorElementType::Node,
                            &key.label,
                            &key.property,
                            index,
                            query_vector,
                            SearchReadLimit::new(k, None),
                        )
                        .await?;
                    let ids = results
                        .into_iter()
                        .map(|result| result.entity_id().local_id())
                        .collect::<Vec<_>>();
                    return self
                        .existing_node_count(&ids, window.threshold())
                        .await
                        .map(|cardinality| window.apply(cardinality));
                }
                exec::ExecCountCursorPlan::EdgeVectorSearch {
                    key,
                    index,
                    query_vector,
                    k,
                } => {
                    let results = self
                        .vector_search_results(
                            VectorElementType::Edge,
                            &key.label,
                            &key.property,
                            index,
                            query_vector,
                            SearchReadLimit::new(k, None),
                        )
                        .await?;
                    let ids = results
                        .into_iter()
                        .map(|result| result.entity_id().local_id())
                        .collect::<Vec<_>>();
                    return self
                        .existing_edge_count(&ids, window.threshold())
                        .await
                        .map(|cardinality| window.apply(cardinality));
                }
                exec::ExecCountCursorPlan::NodeTextSearch {
                    key,
                    index,
                    query_text,
                    k,
                } => {
                    let hits = self
                        .text_search_hits(
                            TextElementType::Node,
                            &key.label,
                            &key.property,
                            index,
                            query_text,
                            SearchReadLimit::new(k, None),
                        )
                        .await?;
                    let ids = hits
                        .into_iter()
                        .map(|result| result.entity_id)
                        .collect::<Vec<_>>();
                    return self
                        .existing_node_count(&ids, window.threshold())
                        .await
                        .map(|cardinality| window.apply(cardinality));
                }
                exec::ExecCountCursorPlan::EdgeTextSearch {
                    key,
                    index,
                    query_text,
                    k,
                } => {
                    let hits = self
                        .text_search_hits(
                            TextElementType::Edge,
                            &key.label,
                            &key.property,
                            index,
                            query_text,
                            SearchReadLimit::new(k, None),
                        )
                        .await?;
                    let ids = hits
                        .into_iter()
                        .map(|result| result.entity_id)
                        .collect::<Vec<_>>();
                    return self
                        .existing_edge_count(&ids, window.threshold())
                        .await
                        .map(|cardinality| window.apply(cardinality));
                }
                exec::ExecCountCursorPlan::NodeDynamicEquality { index, key, param } => {
                    validate_node_equality_index(&index.index_id, key)?;
                    let value = self.param_value(param)?;
                    self.lookup_managed_equality_union(
                        crate::index_lifecycle::IndexElementKind::Node,
                        key,
                        core::slice::from_ref(&value),
                    )
                    .await?
                    .len() as usize
                }
                exec::ExecCountCursorPlan::EdgeDynamicEquality { index, key, param } => {
                    validate_edge_equality_index(&index.index_id, key)?;
                    let value = self.param_value(param)?;
                    self.lookup_managed_equality_union(
                        crate::index_lifecycle::IndexElementKind::Edge,
                        key,
                        core::slice::from_ref(&value),
                    )
                    .await?
                    .len() as usize
                }
                exec::ExecCountCursorPlan::Filter { input, predicate } => {
                    let rows = self.count_cursor(input, dependency).await?;
                    let mut accepted = 0usize;
                    for row in rows {
                        if window
                            .threshold()
                            .is_some_and(|threshold| accepted >= threshold)
                        {
                            break;
                        }
                        self.check_execution_deadline()?;
                        if self.eval_predicate(&row, predicate.predicate()).await? {
                            accepted = accepted.saturating_add(1);
                        }
                    }
                    accepted
                }
                exec::ExecCountCursorPlan::Window {
                    input,
                    window: positioned,
                } => {
                    let positioned = self.count_window(positioned)?;
                    let positioned = self
                        .count_cursor_cardinality(input, dependency, positioned)
                        .await?;
                    return Ok(window.apply(positioned));
                }
                exec::ExecCountCursorPlan::Union { .. }
                | exec::ExecCountCursorPlan::Intersect { .. }
                | exec::ExecCountCursorPlan::Order { .. }
                | exec::ExecCountCursorPlan::Expand { .. }
                | exec::ExecCountCursorPlan::VectorSearch { .. }
                | exec::ExecCountCursorPlan::TextSearch { .. }
                | exec::ExecCountCursorPlan::Variable { .. }
                | exec::ExecCountCursorPlan::Distinct { .. } => {
                    self.count_cursor(cursor, dependency).await?.len()
                }
            };
            Ok(window.apply(cardinality))
        }
        .boxed()
    }

    fn count_cursor<'a>(
        &'a mut self,
        cursor: &'a exec::ExecCountCursorPlan,
        dependency: &'a mut Option<ExecutionValue>,
    ) -> BoxFuture<'a, Result<Vec<ExecutionRow>>> {
        match cursor {
            exec::ExecCountCursorPlan::EmptyRows => {
                self.count_cursor_leaf(CountCursorLeaf::EmptyRows, dependency)
            }
            exec::ExecCountCursorPlan::InputRows => {
                self.count_cursor_leaf(CountCursorLeaf::InputRows, dependency)
            }
            exec::ExecCountCursorPlan::NodeBitmap(bitmap) => {
                self.count_cursor_leaf(CountCursorLeaf::NodeBitmap(bitmap), dependency)
            }
            exec::ExecCountCursorPlan::EdgeBitmap(bitmap) => {
                self.count_cursor_leaf(CountCursorLeaf::EdgeBitmap(bitmap), dependency)
            }
            exec::ExecCountCursorPlan::NodeUnique {
                lookup,
                verification,
            } => self.count_cursor_leaf(
                CountCursorLeaf::NodeUnique {
                    lookup,
                    verification,
                },
                dependency,
            ),
            exec::ExecCountCursorPlan::NodeRange(plan) => {
                self.count_cursor_leaf(CountCursorLeaf::NodeRange(plan), dependency)
            }
            exec::ExecCountCursorPlan::EdgeRange(plan) => {
                self.count_cursor_leaf(CountCursorLeaf::EdgeRange(plan), dependency)
            }
            exec::ExecCountCursorPlan::NodeAuthoritativeScan(predicate) => self.count_cursor_leaf(
                CountCursorLeaf::NodeAuthoritativeScan(predicate),
                dependency,
            ),
            exec::ExecCountCursorPlan::EdgeAuthoritativeScan(predicate) => self.count_cursor_leaf(
                CountCursorLeaf::EdgeAuthoritativeScan(predicate),
                dependency,
            ),
            exec::ExecCountCursorPlan::NodePointReads(ids) => {
                self.count_cursor_leaf(CountCursorLeaf::NodePointReads(ids), dependency)
            }
            exec::ExecCountCursorPlan::EdgePointReads(ids) => {
                self.count_cursor_leaf(CountCursorLeaf::EdgePointReads(ids), dependency)
            }
            exec::ExecCountCursorPlan::NodeRuntimeInput(input) => {
                self.count_cursor_leaf(CountCursorLeaf::NodeRuntimeInput(input), dependency)
            }
            exec::ExecCountCursorPlan::EdgeRuntimeInput(input) => {
                self.count_cursor_leaf(CountCursorLeaf::EdgeRuntimeInput(input), dependency)
            }
            exec::ExecCountCursorPlan::RuntimeInput(input) => {
                self.count_cursor_leaf(CountCursorLeaf::RuntimeInput(input), dependency)
            }
            exec::ExecCountCursorPlan::NodeFullScan => {
                self.count_cursor_leaf(CountCursorLeaf::NodeFullScan, dependency)
            }
            exec::ExecCountCursorPlan::EdgeFullScan => {
                self.count_cursor_leaf(CountCursorLeaf::EdgeFullScan, dependency)
            }
            exec::ExecCountCursorPlan::NodeLabelBitmap(label) => {
                self.count_cursor_leaf(CountCursorLeaf::NodeLabelBitmap(label), dependency)
            }
            exec::ExecCountCursorPlan::EdgeLabelBitmap(label) => {
                self.count_cursor_leaf(CountCursorLeaf::EdgeLabelBitmap(label), dependency)
            }
            exec::ExecCountCursorPlan::NodeVectorSearch {
                key,
                index,
                query_vector,
                k,
            } => self.count_cursor_leaf(
                CountCursorLeaf::NodeVectorSearch {
                    key,
                    index,
                    query_vector,
                    k,
                },
                dependency,
            ),
            exec::ExecCountCursorPlan::EdgeVectorSearch {
                key,
                index,
                query_vector,
                k,
            } => self.count_cursor_leaf(
                CountCursorLeaf::EdgeVectorSearch {
                    key,
                    index,
                    query_vector,
                    k,
                },
                dependency,
            ),
            exec::ExecCountCursorPlan::NodeTextSearch {
                key,
                index,
                query_text,
                k,
            } => self.count_cursor_leaf(
                CountCursorLeaf::NodeTextSearch {
                    key,
                    index,
                    query_text,
                    k,
                },
                dependency,
            ),
            exec::ExecCountCursorPlan::EdgeTextSearch {
                key,
                index,
                query_text,
                k,
            } => self.count_cursor_leaf(
                CountCursorLeaf::EdgeTextSearch {
                    key,
                    index,
                    query_text,
                    k,
                },
                dependency,
            ),
            exec::ExecCountCursorPlan::NodeDynamicEquality { index, key, param } => self
                .count_cursor_leaf(
                    CountCursorLeaf::NodeDynamicEquality { index, key, param },
                    dependency,
                ),
            exec::ExecCountCursorPlan::EdgeDynamicEquality { index, key, param } => self
                .count_cursor_leaf(
                    CountCursorLeaf::EdgeDynamicEquality { index, key, param },
                    dependency,
                ),
            exec::ExecCountCursorPlan::Union { driver, rest } => self
                .count_cursor_structural(CountCursorStructural::Union { driver, rest }, dependency),
            exec::ExecCountCursorPlan::Intersect { driver, rest } => self.count_cursor_structural(
                CountCursorStructural::Intersect { driver, rest },
                dependency,
            ),
            exec::ExecCountCursorPlan::Filter { input, predicate } => self.count_cursor_structural(
                CountCursorStructural::Filter { input, predicate },
                dependency,
            ),
            exec::ExecCountCursorPlan::Window { input, window } => self.count_cursor_structural(
                CountCursorStructural::Window { input, window },
                dependency,
            ),
            exec::ExecCountCursorPlan::Order { input, plan } => self
                .count_cursor_structural(CountCursorStructural::Order { input, plan }, dependency),
            exec::ExecCountCursorPlan::Expand { input, plan } => self
                .count_cursor_structural(CountCursorStructural::Expand { input, plan }, dependency),
            exec::ExecCountCursorPlan::VectorSearch { input, plan } => self
                .count_cursor_structural(
                    CountCursorStructural::VectorSearch { input, plan },
                    dependency,
                ),
            exec::ExecCountCursorPlan::TextSearch { input, plan } => self.count_cursor_structural(
                CountCursorStructural::TextSearch { input, plan },
                dependency,
            ),
            exec::ExecCountCursorPlan::Variable { input, op } => self
                .count_cursor_structural(CountCursorStructural::Variable { input, op }, dependency),
            exec::ExecCountCursorPlan::Distinct { input, plan } => self.count_cursor_structural(
                CountCursorStructural::Distinct { input, plan: *plan },
                dependency,
            ),
        }
    }

    fn count_cursor_leaf<'a>(
        &'a mut self,
        cursor: CountCursorLeaf<'a>,
        dependency: &'a mut Option<ExecutionValue>,
    ) -> BoxFuture<'a, Result<Vec<ExecutionRow>>> {
        async move {
            self.check_execution_deadline()?;
            match cursor {
                CountCursorLeaf::EmptyRows => Ok(Vec::new()),
                CountCursorLeaf::InputRows => match dependency.take() {
                    Some(ExecutionValue::Stream(rows)) => Ok(rows),
                    Some(other) => Err(count_shape_error("rows", &other)),
                    None => Err(HelixDbError::InvariantViolation(
                        "count cursor consumed its row dependency more than once".to_string(),
                    )),
                },
                CountCursorLeaf::NodeBitmap(bitmap) => {
                    let read = self.node_bitmap(bitmap);
                    let ids = read.await?;
                    Ok(ids
                        .into_iter()
                        .map(|id| ExecutionRow::current(ElementRef::Node(id)))
                        .collect())
                }
                CountCursorLeaf::EdgeBitmap(bitmap) => {
                    let read = self.edge_bitmap(bitmap);
                    let ids = read.await?;
                    Ok(ids
                        .into_iter()
                        .map(|id| ExecutionRow::current(ElementRef::Edge(id)))
                        .collect())
                }
                CountCursorLeaf::NodeUnique {
                    lookup,
                    verification,
                } => {
                    let read = self.verified_node_unique_owner(lookup, verification);
                    let id = read.await?;
                    Ok(id
                        .into_iter()
                        .map(|id| ExecutionRow::current(ElementRef::Node(id)))
                        .collect())
                }
                CountCursorLeaf::NodeRange(plan) => {
                    let read = self.validated_node_range_ids(plan);
                    let ids = read.await?;
                    Ok(ids
                        .into_iter()
                        .map(|id| ExecutionRow::current(ElementRef::Node(id)))
                        .collect())
                }
                CountCursorLeaf::EdgeRange(plan) => {
                    let read = self.validated_edge_range_ids(plan);
                    let ids = read.await?;
                    Ok(ids
                        .into_iter()
                        .map(|id| ExecutionRow::current(ElementRef::Edge(id)))
                        .collect())
                }
                CountCursorLeaf::NodeAuthoritativeScan(predicate) => {
                    let read = self.scan_element_ids(exec::ElementKeyspace::NodeProperty, None);
                    let ids = read.await?;
                    let mut rows = Vec::new();
                    for id in ids {
                        let row = ExecutionRow::current(ElementRef::Node(id));
                        let matches = match predicate {
                            exec::ExecNodeAuthoritativeScanPredicate::NullEquality { key } => {
                                let read = self.scoped_null_matches(&row, key);
                                read.await?
                            }
                            exec::ExecNodeAuthoritativeScanPredicate::Predicate(predicate) => {
                                let read = self.eval_predicate(&row, predicate.predicate());
                                read.await?
                            }
                        };
                        if matches {
                            rows.push(row);
                        }
                    }
                    Ok(rows)
                }
                CountCursorLeaf::EdgeAuthoritativeScan(predicate) => {
                    let read = self.scan_element_ids(exec::ElementKeyspace::EdgeEndpoints, None);
                    let ids = read.await?;
                    let mut rows = Vec::new();
                    for id in ids {
                        let row = ExecutionRow::current(ElementRef::Edge(id));
                        let matches = match predicate {
                            exec::ExecEdgeAuthoritativeScanPredicate::NullEquality { key } => {
                                let read = self.scoped_null_matches(&row, key);
                                read.await?
                            }
                            exec::ExecEdgeAuthoritativeScanPredicate::Predicate(predicate) => {
                                let read = self.eval_predicate(&row, predicate.predicate());
                                read.await?
                            }
                        };
                        if matches {
                            rows.push(row);
                        }
                    }
                    Ok(rows)
                }
                CountCursorLeaf::NodePointReads(ids) => {
                    self.node_row_vec(ids.as_ref().to_vec()).await
                }
                CountCursorLeaf::EdgePointReads(ids) => {
                    self.edge_row_vec(ids.as_ref().to_vec()).await
                }
                CountCursorLeaf::NodeRuntimeInput(input) => {
                    let ids = self.runtime_ids(input)?;
                    self.node_row_vec(ids).await
                }
                CountCursorLeaf::EdgeRuntimeInput(input) => {
                    let ids = self.runtime_ids(input)?;
                    self.edge_row_vec(ids).await
                }
                CountCursorLeaf::RuntimeInput(input) => match input {
                    exec::ExecRuntimeInputPlan::Variable(variable) => {
                        match self.variable_value(variable)?.clone() {
                            ExecutionValue::Stream(rows) => Ok(rows),
                            other @ (ExecutionValue::FoldedStream(_)
                            | ExecutionValue::Count(_)
                            | ExecutionValue::Bool(_)
                            | ExecutionValue::Scalars(_)
                            | ExecutionValue::IndexDdlReceipt(_)
                            | ExecutionValue::IndexOperationStatus(_)) => {
                                Err(count_shape_error("rows", &other))
                            }
                        }
                    }
                    exec::ExecRuntimeInputPlan::Param(param) => {
                        let ids = self.param_ids(param)?;
                        Ok(ids
                            .into_iter()
                            .map(|id| ExecutionRow::current(ElementRef::Node(id)))
                            .collect())
                    }
                },
                CountCursorLeaf::NodeFullScan => {
                    let read = self.scan_element_ids(exec::ElementKeyspace::NodeProperty, None);
                    let ids = read.await?;
                    Ok(ids
                        .into_iter()
                        .map(|id| ExecutionRow::current(ElementRef::Node(id)))
                        .collect())
                }
                CountCursorLeaf::EdgeFullScan => {
                    let read = self.scan_element_ids(exec::ElementKeyspace::EdgeEndpoints, None);
                    let ids = read.await?;
                    Ok(ids
                        .into_iter()
                        .map(|id| ExecutionRow::current(ElementRef::Edge(id)))
                        .collect())
                }
                CountCursorLeaf::NodeLabelBitmap(label) => {
                    let value = DbPropertyValue::String(label.as_ref().to_string());
                    let read = self.lookup_equality_index_set("$label", &value);
                    let ids = read.await?;
                    Ok(ids
                        .into_iter()
                        .map(|id| ExecutionRow::current(ElementRef::Node(id)))
                        .collect())
                }
                CountCursorLeaf::EdgeLabelBitmap(label) => {
                    let read = self.lookup_global_edge_label_index(label.as_ref());
                    let ids = read.await?;
                    Ok(ids
                        .into_iter()
                        .map(|id| ExecutionRow::current(ElementRef::Edge(id)))
                        .collect())
                }
                CountCursorLeaf::NodeVectorSearch {
                    key,
                    index,
                    query_vector,
                    k,
                } => {
                    let read = self.vector_search_results(
                        VectorElementType::Node,
                        &key.label,
                        &key.property,
                        index,
                        query_vector,
                        SearchReadLimit::new(k, None),
                    );
                    let results = read.await?;
                    self.node_search_row_vec(results).await
                }
                CountCursorLeaf::EdgeVectorSearch {
                    key,
                    index,
                    query_vector,
                    k,
                } => {
                    let read = self.vector_search_results(
                        VectorElementType::Edge,
                        &key.label,
                        &key.property,
                        index,
                        query_vector,
                        SearchReadLimit::new(k, None),
                    );
                    let results = read.await?;
                    self.edge_search_row_vec(results).await
                }
                CountCursorLeaf::NodeTextSearch {
                    key,
                    index,
                    query_text,
                    k,
                } => {
                    let read = self.text_search_hits(
                        TextElementType::Node,
                        &key.label,
                        &key.property,
                        index,
                        query_text,
                        SearchReadLimit::new(k, None),
                    );
                    let hits = read.await?;
                    self.node_text_search_row_vec(hits).await
                }
                CountCursorLeaf::EdgeTextSearch {
                    key,
                    index,
                    query_text,
                    k,
                } => {
                    let read = self.text_search_hits(
                        TextElementType::Edge,
                        &key.label,
                        &key.property,
                        index,
                        query_text,
                        SearchReadLimit::new(k, None),
                    );
                    let hits = read.await?;
                    self.edge_text_search_row_vec(hits).await
                }
                CountCursorLeaf::NodeDynamicEquality { index, key, param } => {
                    validate_node_equality_index(&index.index_id, key)?;
                    let value = self.param_value(param)?;
                    let read = self.lookup_managed_equality_union(
                        crate::index_lifecycle::IndexElementKind::Node,
                        key,
                        core::slice::from_ref(&value),
                    );
                    let ids = read.await?;
                    Ok(ids
                        .into_iter()
                        .map(|id| ExecutionRow::current(ElementRef::Node(id)))
                        .collect())
                }
                CountCursorLeaf::EdgeDynamicEquality { index, key, param } => {
                    validate_edge_equality_index(&index.index_id, key)?;
                    let value = self.param_value(param)?;
                    let read = self.lookup_managed_equality_union(
                        crate::index_lifecycle::IndexElementKind::Edge,
                        key,
                        core::slice::from_ref(&value),
                    );
                    let ids = read.await?;
                    Ok(ids
                        .into_iter()
                        .map(|id| ExecutionRow::current(ElementRef::Edge(id)))
                        .collect())
                }
            }
        }
        .boxed()
    }

    fn count_cursor_structural<'a>(
        &'a mut self,
        cursor: CountCursorStructural<'a>,
        dependency: &'a mut Option<ExecutionValue>,
    ) -> BoxFuture<'a, Result<Vec<ExecutionRow>>> {
        async move {
            self.check_execution_deadline()?;
            match cursor {
                CountCursorStructural::Union { driver, rest } => {
                    let mut rows = self.count_cursor(driver, dependency).await?;
                    let mut seen = rows.iter().cloned().collect::<BTreeSet<_>>();
                    for child in rest {
                        for row in self.count_cursor(child, dependency).await? {
                            if seen.insert(row.clone()) {
                                rows.push(row);
                            }
                        }
                    }
                    Ok(rows)
                }
                CountCursorStructural::Intersect { driver, rest } => {
                    let mut rows = self.count_cursor(driver, dependency).await?;
                    for child in rest {
                        let read = self.count_cursor(child, dependency);
                        let allowed = read.await?.into_iter().collect::<BTreeSet<_>>();
                        rows.retain(|row| allowed.contains(row));
                    }
                    Ok(rows)
                }
                CountCursorStructural::Filter { input, predicate } => {
                    let input = ExecutionValue::Stream(self.count_cursor(input, dependency).await?);
                    let output = self.filter(input, predicate).await?;
                    self.stream_rows(output, "exact count filter")
                }
                CountCursorStructural::Window { input, window } => {
                    let rows = self.count_cursor(input, dependency).await?;
                    Ok(self.count_window(window)?.apply_rows(rows))
                }
                CountCursorStructural::Order { input, plan } => {
                    let input = ExecutionValue::Stream(self.count_cursor(input, dependency).await?);
                    let output = self.order(input, plan).await?;
                    self.stream_rows(output, "exact count order")
                }
                CountCursorStructural::Expand { input, plan } => {
                    let input = ExecutionValue::Stream(self.count_cursor(input, dependency).await?);
                    let output = self.expand(input, plan).await?;
                    self.stream_rows(output, "exact count expansion")
                }
                CountCursorStructural::VectorSearch { input, plan } => {
                    let input = ExecutionValue::Stream(self.count_cursor(input, dependency).await?);
                    let output = self.restricted_vector_search(input, plan).await?;
                    self.stream_rows(output, "exact count restricted vector search")
                }
                CountCursorStructural::TextSearch { input, plan } => {
                    let input = ExecutionValue::Stream(self.count_cursor(input, dependency).await?);
                    let output = self.restricted_text_search(input, plan).await?;
                    self.stream_rows(output, "exact count restricted text search")
                }
                CountCursorStructural::Variable { input, op } => {
                    let input = ExecutionValue::Stream(self.count_cursor(input, dependency).await?);
                    let executable = exec::ExecVariableOp::Stream(op.to_stream_op());
                    let output = self.variable(input, &executable)?;
                    self.stream_rows(output, "exact count variable")
                }
                CountCursorStructural::Distinct { input, plan } => {
                    let mut rows = self.count_cursor(input, dependency).await?;
                    match plan {
                        exec::ExecCountDistinctPlan::HashRows => {
                            let output = self
                                .distinct(ExecutionValue::Stream(rows))
                                .expect("typed count distinct always receives rows");
                            self.stream_rows(output, "exact count distinct")
                        }
                        exec::ExecCountDistinctPlan::OrderedRows => {
                            rows.dedup();
                            Ok(rows)
                        }
                    }
                }
            }
        }
        .boxed()
    }

    async fn validated_node_range_ids(
        &self,
        plan: &exec::ExecNodeVerifiedRangeScanPlan,
    ) -> Result<Vec<u64>> {
        validate_range_index("node_range:", &plan.index.index_id, &plan.key)?;
        self.node_range_index_ids(&plan.key, &plan.range, None)
            .await
    }

    async fn validated_edge_range_ids(
        &self,
        plan: &exec::ExecEdgeVerifiedRangeScanPlan,
    ) -> Result<Vec<u64>> {
        validate_range_index("edge_range:", &plan.index.index_id, &plan.key)?;
        self.edge_range_index_ids(&plan.key, &plan.range, None)
            .await
    }
}

pub(in crate::execution::interpreter) fn indexed_value(
    value: &exec::ExecIndexedEqualityValue,
) -> DbPropertyValue {
    stream::ast_to_db_value(value.literal().as_property_value().clone())
}

fn positive_limit(limit: Option<usize>) -> Option<properties::PositiveUsize> {
    limit.and_then(properties::PositiveUsize::new)
}

fn count_shape_error(expected: &str, actual: &ExecutionValue) -> HelixDbError {
    if matches!(
        actual,
        ExecutionValue::IndexDdlReceipt(_) | ExecutionValue::IndexOperationStatus(_)
    ) {
        return HelixDbError::Query("count cannot consume an index lifecycle value".to_string());
    }
    HelixDbError::InvariantViolation(format!("count plan expected {expected}, got {actual:?}"))
}

pub(in crate::execution::interpreter) fn validate_node_equality_index(
    index_id: &ir::NonEmptyString,
    key: &helix_planner::catalog::ScopedPropertyKey,
) -> Result<()> {
    validate_index_id("node_eq:", index_id, key)
}

pub(in crate::execution::interpreter) fn validate_edge_equality_index(
    index_id: &ir::NonEmptyString,
    key: &helix_planner::catalog::ScopedPropertyKey,
) -> Result<()> {
    validate_index_id("edge_eq:", index_id, key)
}

fn validate_range_index(
    prefix: &'static str,
    index_id: &ir::NonEmptyString,
    key: &helix_planner::catalog::ScopedPropertyDirectionKey,
) -> Result<()> {
    validate_index_id(prefix, index_id, key)
}

fn validate_index_id(
    prefix: &'static str,
    actual: &ir::NonEmptyString,
    key: &impl std::fmt::Display,
) -> Result<()> {
    let expected = ir::NonEmptyString::from_prefixed_display(prefix, key);
    if actual != &expected {
        return Err(HelixDbError::IndexCatalogCorruption(format!(
            "planner logical index identity `{actual}` disagrees with `{expected}`"
        )));
    }
    Ok(())
}

#[cfg(all(feature = "production-coverage", not(test)))]
pub(super) async fn run_production_contracts() {
    tests::run_production_contracts().await;
}

#[cfg(any(test, feature = "production-coverage"))]
mod tests {
    use helix_ast::expr::Predicate;
    use helix_ast::index::RangeIndexDirection;
    use helix_ast::query::QueryValue;
    use helix_ast::value::PropertyValue;
    use helix_planner::{catalog, context};
    #[cfg(test)]
    use proptest::prelude::*;
    #[cfg(test)]
    use proptest::strategy::ValueTree;
    #[cfg(test)]
    use proptest::test_runner::TestRunner;

    use super::super::access::tests::support as access_support;
    use super::super::test_support;
    use super::*;

    fn indexed(value: &str) -> exec::ExecIndexedEqualityValue {
        exec::ExecIndexedEqualityValue::try_from(
            ir::SecondaryIndexLiteral::new(PropertyValue::from(value)).unwrap(),
        )
        .unwrap()
    }

    fn node_equality_index(label: &str, property: &str) -> exec::ExecNodeNonUniqueEqualityIndex {
        exec::ExecNodeNonUniqueEqualityIndex::try_from(catalog::NodeEqualityIndexMeta::new(
            test_support::name(&format!("node_eq:{label}:{property}")),
        ))
        .unwrap()
    }

    fn node_point(label: &str, property: &str, value: &str) -> exec::ExecNodeBitmapExpr {
        exec::ExecNodeBitmapExpr::PointRead {
            index: node_equality_index(label, property),
            key: catalog::ScopedPropertyKey::try_new(label, property).unwrap(),
            value: indexed(value),
        }
    }

    fn edge_equality_index(label: &str, property: &str) -> exec::ExecEdgeNonUniqueEqualityIndex {
        exec::ExecEdgeNonUniqueEqualityIndex::new(catalog::EdgeEqualityIndexMeta::new(
            test_support::name(&format!("edge_eq:{label}:{property}")),
        ))
    }

    fn edge_point(label: &str, property: &str, value: &str) -> exec::ExecEdgeBitmapExpr {
        exec::ExecEdgeBitmapExpr::PointRead {
            index: edge_equality_index(label, property),
            key: catalog::ScopedPropertyKey::try_new(label, property).unwrap(),
            value: indexed(value),
        }
    }

    async fn execute_direct_count(
        db: &HelixDB,
        plan: exec::ExecCountPlan,
    ) -> Result<ExecutionValue> {
        execute_direct_count_with_params(db, plan, context::ParamBindings::default()).await
    }

    async fn execute_direct_count_with_params(
        db: &HelixDB,
        plan: exec::ExecCountPlan,
        params: context::ParamBindings,
    ) -> Result<ExecutionValue> {
        let executable = test_support::executable(
            ir::PlanKind::Read,
            vec![test_support::step(
                1,
                Vec::new(),
                exec::ExecOp::Count {
                    plan: Box::new(plan),
                },
            )],
            1,
        );
        db.execute(&executable, params).await?.last.ok_or_else(|| {
            HelixDbError::InvariantViolation("direct count test has no result".to_string())
        })
    }

    fn bounded(skip: usize, take: usize) -> exec::ExecCountWindowPlan {
        exec::ExecCountWindowPlan::identity()
            .then_skip(exec::ExecUsizeExpr::literal(skip))
            .then_limit(exec::ExecUsizeExpr::literal(take))
    }

    #[cfg_attr(test, test)]
    fn evaluated_windows_and_index_identity_validation_cover_boundaries() {
        let all = EvaluatedCountWindow {
            skip: 2,
            take: None,
        };
        assert_eq!(all.apply(5), 3);
        assert_eq!(
            all.apply_rows(vec![
                ExecutionRow::current(ElementRef::Node(1)),
                ExecutionRow::current(ElementRef::Node(2)),
                ExecutionRow::current(ElementRef::Node(3)),
            ]),
            vec![ExecutionRow::current(ElementRef::Node(3))]
        );
        assert_eq!(all.threshold(), None);

        let bounded = EvaluatedCountWindow {
            skip: usize::MAX,
            take: Some(2),
        };
        assert_eq!(bounded.apply(3), 0);
        assert!(bounded.apply_rows(Vec::new()).is_empty());
        assert_eq!(bounded.threshold(), Some(usize::MAX));
        assert_eq!(positive_limit(None), None);
        assert_eq!(positive_limit(Some(0)), None);
        assert_eq!(
            positive_limit(Some(3)).map(properties::PositiveUsize::get),
            Some(3)
        );

        let node = catalog::ScopedPropertyKey::try_new("User", "status").unwrap();
        assert!(
            validate_node_equality_index(&test_support::name("node_eq:User:status"), &node).is_ok()
        );
        assert!(
            validate_edge_equality_index(&test_support::name("edge_eq:User:status"), &node).is_ok()
        );
        let error = validate_node_equality_index(&test_support::name("wrong"), &node).unwrap_err();
        assert!(
            matches!(error, HelixDbError::IndexCatalogCorruption(message)
            if message.contains("planner logical index identity"))
        );
    }

    #[cfg_attr(test, tokio::test)]
    async fn direct_non_search_count_families_match_materialized_sources() {
        let db = test_support::open_db_with_config(
            test_support::in_memory_config("count-direct-source-matrix")
                .with_equality_index("User", "status")
                .with_edge_equality_index("FOLLOWS", "status")
                .with_range_index("User", "rank")
                .with_edge_range_index("FOLLOWS", "rank"),
        )
        .await;
        let first = test_support::add_node_with_properties(
            &db,
            "User",
            vec![
                ("status", PropertyValue::from("active")),
                ("rank", PropertyValue::from("a")),
            ],
        )
        .await;
        let second = test_support::add_node_with_properties(
            &db,
            "User",
            vec![
                ("status", PropertyValue::from("paused")),
                ("rank", PropertyValue::from("b")),
            ],
        )
        .await;
        let absent = test_support::add_node_with_properties(&db, "User", Vec::new()).await;
        test_support::add_node_with_properties(&db, "Other", Vec::new()).await;
        let first_edge = test_support::add_edge_with_properties(
            &db,
            first,
            second,
            "FOLLOWS",
            vec![
                ("status", PropertyValue::from("active")),
                ("rank", PropertyValue::from("a")),
            ],
        )
        .await;
        let second_edge = test_support::add_edge_with_properties(
            &db,
            second,
            first,
            "FOLLOWS",
            vec![
                ("status", PropertyValue::from("paused")),
                ("rank", PropertyValue::from("b")),
            ],
        )
        .await;

        let node_union = exec::ExecNodeBitmapExpr::Union {
            driver: Box::new(node_point("User", "status", "active")),
            rest: ir::AtLeast::from_one(node_point("User", "status", "paused")),
        };
        let node_intersection = exec::ExecNodeBitmapExpr::Intersect {
            driver: Box::new(node_union.clone()),
            rest: ir::AtLeast::from_one(node_point("User", "status", "active")),
        };
        assert_eq!(
            execute_direct_count(
                &db,
                exec::ExecCountPlan::NodeBitmap(exec::ExecNodeBitmapCountPlan {
                    bitmap: node_intersection,
                    window: exec::ExecCountWindowPlan::identity(),
                }),
            )
            .await
            .unwrap(),
            ExecutionValue::Count(1)
        );

        let edge_batch = exec::ExecEdgeBitmapExpr::BatchedUnionRead {
            index: edge_equality_index("FOLLOWS", "status"),
            key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
            values: ir::AtLeast::from_pair(indexed("active"), indexed("paused")),
        };
        let edge_union = exec::ExecEdgeBitmapExpr::Union {
            driver: Box::new(edge_batch),
            rest: ir::AtLeast::from_one(edge_point("FOLLOWS", "status", "missing")),
        };
        let edge_intersection = exec::ExecEdgeBitmapExpr::Intersect {
            driver: Box::new(edge_union),
            rest: ir::AtLeast::from_one(edge_point("FOLLOWS", "status", "active")),
        };
        assert_eq!(
            execute_direct_count(
                &db,
                exec::ExecCountPlan::EdgeBitmap(exec::ExecEdgeBitmapCountPlan {
                    bitmap: edge_intersection,
                    window: exec::ExecCountWindowPlan::identity(),
                }),
            )
            .await
            .unwrap(),
            ExecutionValue::Count(1)
        );

        let node_range = exec::ExecNodeVerifiedRangeScanPlan {
            index: catalog::NodeRangeIndexMeta::new(test_support::name("node_range:User:rank:Asc")),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "rank",
                RangeIndexDirection::Asc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        };
        assert_eq!(
            execute_direct_count(
                &db,
                exec::ExecCountPlan::NodeRange(exec::ExecNodeRangeCountPlan {
                    driver: node_range.clone(),
                    membership: exec::ExecNodeRangeMembershipPlan::BitmapFilters(
                        ir::AtLeast::from_one(node_point("User", "status", "active")),
                    ),
                    window: bounded(0, 1),
                }),
            )
            .await
            .unwrap(),
            ExecutionValue::Count(1)
        );
        assert_eq!(
            execute_direct_count(
                &db,
                exec::ExecCountPlan::NodeRange(exec::ExecNodeRangeCountPlan {
                    driver: exec::ExecNodeVerifiedRangeScanPlan {
                        range: ir::IndexRange::Lower {
                            lower: ir::IndexBound::Inclusive(
                                ir::RangeIndexValue::literal(PropertyValue::from("a")).unwrap(),
                            ),
                        },
                        ..node_range.clone()
                    },
                    membership: exec::ExecNodeRangeMembershipPlan::All,
                    window: exec::ExecCountWindowPlan::identity(),
                }),
            )
            .await
            .unwrap(),
            ExecutionValue::Count(2)
        );
        assert_eq!(
            execute_direct_count(
                &db,
                exec::ExecCountPlan::NodeRange(exec::ExecNodeRangeCountPlan {
                    driver: node_range,
                    membership: exec::ExecNodeRangeMembershipPlan::All,
                    window: bounded(1, 1),
                }),
            )
            .await
            .unwrap(),
            ExecutionValue::Count(1)
        );

        let edge_range = exec::ExecEdgeVerifiedRangeScanPlan {
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:rank:Asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "rank",
                RangeIndexDirection::Asc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        };
        assert_eq!(
            execute_direct_count(
                &db,
                exec::ExecCountPlan::EdgeRange(exec::ExecEdgeRangeCountPlan {
                    driver: edge_range.clone(),
                    membership: exec::ExecEdgeRangeMembershipPlan::BitmapFilters(
                        ir::AtLeast::from_one(edge_point("FOLLOWS", "status", "paused")),
                    ),
                    window: exec::ExecCountWindowPlan::identity(),
                }),
            )
            .await
            .unwrap(),
            ExecutionValue::Count(1)
        );
        assert_eq!(
            execute_direct_count(
                &db,
                exec::ExecCountPlan::EdgeRange(exec::ExecEdgeRangeCountPlan {
                    driver: edge_range,
                    membership: exec::ExecEdgeRangeMembershipPlan::All,
                    window: bounded(1, 1),
                }),
            )
            .await
            .unwrap(),
            ExecutionValue::Count(1)
        );

        let node_null = exec::ExecNodeAuthoritativeScanPredicate::NullEquality {
            key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
        };
        assert_eq!(
            execute_direct_count(
                &db,
                exec::ExecCountPlan::NodeAuthoritativeScan(exec::ExecNodeScanCountPlan {
                    predicate: node_null.clone(),
                    window: exec::ExecCountWindowPlan::identity(),
                }),
            )
            .await
            .unwrap(),
            ExecutionValue::Count(1)
        );
        assert_eq!(
            execute_direct_count(
                &db,
                exec::ExecCountPlan::NodeAuthoritativeScan(exec::ExecNodeScanCountPlan {
                    predicate: exec::ExecNodeAuthoritativeScanPredicate::Predicate(
                        ir::PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
                    ),
                    window: exec::ExecCountWindowPlan::identity(),
                }),
            )
            .await
            .unwrap(),
            ExecutionValue::Count(1)
        );
        let edge_predicate = exec::ExecEdgeAuthoritativeScanPredicate::Predicate(
            ir::PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
        );
        assert_eq!(
            execute_direct_count(
                &db,
                exec::ExecCountPlan::EdgeAuthoritativeScan(exec::ExecEdgeScanCountPlan {
                    predicate: edge_predicate.clone(),
                    window: exec::ExecCountWindowPlan::identity(),
                }),
            )
            .await
            .unwrap(),
            ExecutionValue::Count(1)
        );
        assert_eq!(
            execute_direct_count(
                &db,
                exec::ExecCountPlan::EdgeAuthoritativeScan(exec::ExecEdgeScanCountPlan {
                    predicate: exec::ExecEdgeAuthoritativeScanPredicate::NullEquality {
                        key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
                    },
                    window: exec::ExecCountWindowPlan::identity(),
                }),
            )
            .await
            .unwrap(),
            ExecutionValue::Count(0)
        );
        assert_eq!(
            execute_direct_count(
                &db,
                exec::ExecCountPlan::EdgeAuthoritativeScan(exec::ExecEdgeScanCountPlan {
                    predicate: edge_predicate,
                    window: bounded(0, 1),
                }),
            )
            .await
            .unwrap(),
            ExecutionValue::Count(1)
        );

        for (plan, expected) in [
            (
                exec::ExecCountPlan::Constant(usize::MAX),
                ExecutionValue::Count(usize::MAX),
            ),
            (
                exec::ExecCountPlan::NodePointReads {
                    ids: test_support::ids(vec![first, 999_999, second]),
                    window: bounded(1, 1),
                },
                ExecutionValue::Count(1),
            ),
            (
                exec::ExecCountPlan::NodePointReads {
                    ids: test_support::ids(vec![first, second, 999_999]),
                    window: bounded(0, 1),
                },
                ExecutionValue::Count(1),
            ),
            (
                exec::ExecCountPlan::EdgePointReads {
                    ids: test_support::ids(vec![first_edge, 999_999, second_edge]),
                    window: bounded(1, 1),
                },
                ExecutionValue::Count(1),
            ),
            (
                exec::ExecCountPlan::EdgePointReads {
                    ids: test_support::ids(vec![first_edge, second_edge, 999_999]),
                    window: bounded(0, 1),
                },
                ExecutionValue::Count(1),
            ),
            (
                exec::ExecCountPlan::NodeFullScan {
                    window: bounded(1, 2),
                },
                ExecutionValue::Count(2),
            ),
            (
                exec::ExecCountPlan::EdgeFullScan {
                    window: bounded(1, 1),
                },
                ExecutionValue::Count(1),
            ),
            (
                exec::ExecCountPlan::NodeLabelBitmap {
                    label: test_support::name("User"),
                    window: bounded(1, 2),
                },
                ExecutionValue::Count(2),
            ),
            (
                exec::ExecCountPlan::EdgeLabelBitmap {
                    label: test_support::name("FOLLOWS"),
                    window: bounded(1, 1),
                },
                ExecutionValue::Count(1),
            ),
        ] {
            assert_eq!(execute_direct_count(&db, plan).await.unwrap(), expected);
        }

        let ids_param = test_support::name("ids");
        let node_dynamic = test_support::name("node_status");
        let edge_dynamic = test_support::name("edge_status");
        let params = context::ParamBindings::default()
            .with_value(
                ids_param.clone(),
                PropertyValue::I64Array(vec![first as i64, second as i64, 999_999]),
            )
            .with_value(node_dynamic.clone(), PropertyValue::from("active"))
            .with_value(edge_dynamic.clone(), PropertyValue::from("paused"));
        for (plan, expected) in [
            (
                exec::ExecCountPlan::NodeRuntimeInput {
                    input: exec::ExecRuntimeInputPlan::Param(ids_param.clone()),
                    window: exec::ExecCountWindowPlan::identity(),
                },
                2,
            ),
            (
                exec::ExecCountPlan::EdgeRuntimeInput {
                    input: exec::ExecRuntimeInputPlan::Param(ids_param.clone()),
                    window: exec::ExecCountWindowPlan::identity(),
                },
                2,
            ),
            (
                exec::ExecCountPlan::RuntimeInput {
                    input: exec::ExecRuntimeInputPlan::Param(ids_param),
                    window: bounded(1, 1),
                },
                1,
            ),
            (
                exec::ExecCountPlan::NodeDynamicEquality(exec::ExecNodeDynamicEqualityCountPlan {
                    index: catalog::NodeEqualityIndexMeta::new(test_support::name(
                        "node_eq:User:status",
                    )),
                    key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                    param: node_dynamic,
                    window: exec::ExecCountWindowPlan::identity(),
                }),
                1,
            ),
            (
                exec::ExecCountPlan::EdgeDynamicEquality(exec::ExecEdgeDynamicEqualityCountPlan {
                    index: catalog::EdgeEqualityIndexMeta::new(test_support::name(
                        "edge_eq:FOLLOWS:status",
                    )),
                    key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
                    param: edge_dynamic,
                    window: exec::ExecCountWindowPlan::identity(),
                }),
                1,
            ),
        ] {
            assert_eq!(
                execute_direct_count_with_params(&db, plan, params.clone())
                    .await
                    .unwrap(),
                ExecutionValue::Count(expected)
            );
        }

        let variable = test_support::name("rows");
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        context.enable_request_read_view().await.unwrap();
        context.variables.insert(
            variable.clone(),
            ExecutionValue::Stream(vec![
                ExecutionRow::current(ElementRef::Node(first)),
                ExecutionRow::current(ElementRef::Node(absent)),
            ]),
        );
        for (plan, expected) in [
            (
                exec::ExecCountPlan::NodeRuntimeInput {
                    input: exec::ExecRuntimeInputPlan::Variable(variable.clone()),
                    window: bounded(1, 1),
                },
                1,
            ),
            (
                exec::ExecCountPlan::RuntimeInput {
                    input: exec::ExecRuntimeInputPlan::Variable(variable),
                    window: exec::ExecCountWindowPlan::identity(),
                },
                2,
            ),
        ] {
            assert_eq!(
                context
                    .execute_count(ExecutionValue::Stream(Vec::new()), &plan)
                    .await
                    .unwrap(),
                ExecutionValue::Count(expected)
            );
        }
        context.close_request_read_view().unwrap();
        let node_key =
            catalog::ScopedPropertyDirectionKey::try_new("User", "rank", RangeIndexDirection::Asc)
                .unwrap();
        let edge_key = catalog::ScopedPropertyDirectionKey::try_new(
            "FOLLOWS",
            "rank",
            RangeIndexDirection::Asc,
        )
        .unwrap();
        assert!(context
            .node_range_index_count_with_membership(&node_key, &ir::IndexRange::All, &[], None)
            .await
            .is_err());
        assert!(context
            .edge_range_index_count_with_membership(&edge_key, &ir::IndexRange::All, &[], None)
            .await
            .is_err());
    }

    #[cfg_attr(test, tokio::test)]
    async fn dynamic_equality_runtime_classification_covers_every_named_case() {
        let db = test_support::open_db_with_config(
            test_support::in_memory_config("count-dynamic-equality-classification")
                .with_equality_index("User", "status")
                .with_unique_equality_index("User", "email")
                .with_edge_equality_index("FOLLOWS", "status"),
        )
        .await;
        let active = test_support::add_node_with_properties(
            &db,
            "User",
            vec![("status", PropertyValue::from("active"))],
        )
        .await;
        let null = test_support::add_node_with_properties(
            &db,
            "User",
            vec![("status", PropertyValue::Null)],
        )
        .await;
        let missing = test_support::add_node_with_properties(&db, "User", Vec::new()).await;
        let owner = test_support::add_node_with_properties(
            &db,
            "User",
            vec![("email", PropertyValue::from("owner@example.com"))],
        )
        .await;
        test_support::add_edge_with_properties(
            &db,
            active,
            null,
            "FOLLOWS",
            vec![("status", PropertyValue::from("active"))],
        )
        .await;
        assert_ne!(missing, active);

        let param = test_support::name("late");
        let node =
            exec::ExecCountPlan::NodeDynamicEquality(exec::ExecNodeDynamicEqualityCountPlan {
                index: catalog::NodeEqualityIndexMeta::new(test_support::name(
                    "node_eq:User:status",
                )),
                key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                param: param.clone(),
                window: exec::ExecCountWindowPlan::identity(),
            });
        for (value, expected) in [
            (PropertyValue::from("active"), 1),
            (PropertyValue::Null, 3),
            (PropertyValue::F64(f64::NAN), 0),
        ] {
            assert_eq!(
                execute_direct_count_with_params(
                    &db,
                    node.clone(),
                    context::ParamBindings::default().with_value(param.clone(), value),
                )
                .await
                .unwrap(),
                ExecutionValue::Count(expected)
            );
        }
        for value in [
            PropertyValue::Array(Vec::new()),
            PropertyValue::String("x".repeat(
                crate::encoding::v1::property::equality_value::MAX_EQUALITY_CANONICAL_LEN + 1,
            )),
        ] {
            assert!(execute_direct_count_with_params(
                &db,
                node.clone(),
                context::ParamBindings::default().with_value(param.clone(), value),
            )
            .await
            .is_err());
        }

        let unique =
            exec::ExecCountPlan::NodeDynamicEquality(exec::ExecNodeDynamicEqualityCountPlan {
                index: catalog::NodeEqualityIndexMeta::new(test_support::name(
                    "node_eq:User:email",
                ))
                .with_uniqueness(catalog::IndexUniqueness::Unique),
                key: catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
                param: param.clone(),
                window: exec::ExecCountWindowPlan::identity(),
            });
        for (value, expected) in [
            (PropertyValue::from("owner@example.com"), 1),
            (PropertyValue::from("missing@example.com"), 0),
            (PropertyValue::F64(f64::NAN), 0),
        ] {
            assert_eq!(
                execute_direct_count_with_params(
                    &db,
                    unique.clone(),
                    context::ParamBindings::default().with_value(param.clone(), value),
                )
                .await
                .unwrap(),
                ExecutionValue::Count(expected)
            );
        }
        db.inner_db()
            .put(
                keys::Key::Data {
                    scope: crate::encoding::v1::keys::tenant::DataScope::LegacyUnscoped,
                    kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(owner)),
                }
                .to_bytes(),
                crate::encoding::v1::property::encode_properties(&[
                    crate::encoding::v1::property::Property::string("$label", "User"),
                    crate::encoding::v1::property::Property::string(
                        "email",
                        "different@example.com",
                    ),
                ]),
            )
            .await
            .unwrap();
        assert!(execute_direct_count_with_params(
            &db,
            unique,
            context::ParamBindings::default()
                .with_value(param.clone(), PropertyValue::from("owner@example.com"),),
        )
        .await
        .is_err());

        let edge =
            exec::ExecCountPlan::EdgeDynamicEquality(exec::ExecEdgeDynamicEqualityCountPlan {
                index: catalog::EdgeEqualityIndexMeta::new(test_support::name(
                    "edge_eq:FOLLOWS:status",
                )),
                key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
                param: param.clone(),
                window: exec::ExecCountWindowPlan::identity(),
            });
        for (value, expected) in [
            (PropertyValue::from("active"), 1),
            (PropertyValue::Null, 0),
            (PropertyValue::F64(f64::NAN), 0),
        ] {
            assert_eq!(
                execute_direct_count_with_params(
                    &db,
                    edge.clone(),
                    context::ParamBindings::default().with_value(param.clone(), value),
                )
                .await
                .unwrap(),
                ExecutionValue::Count(expected)
            );
        }
    }

    #[cfg_attr(test, tokio::test)]
    async fn every_direct_storage_count_propagates_its_own_read_failure() {
        let db = test_support::open_db_with_config(
            test_support::in_memory_config("count-direct-storage-errors")
                .with_equality_index("User", "status")
                .with_unique_equality_index("User", "email")
                .with_edge_equality_index("FOLLOWS", "status")
                .with_range_index("User", "rank")
                .with_edge_range_index("FOLLOWS", "rank"),
        )
        .await;
        db.inner_db().close().await.unwrap();
        let ids = test_support::name("ids");
        let late = test_support::name("late");
        let mut execution = ExecutionContext::new(
            &db,
            context::ParamBindings::default()
                .with_value(ids.clone(), PropertyValue::I64Array(vec![1]))
                .with_value(late.clone(), PropertyValue::from("active")),
        );
        let exact_unique = exec::ExecNodeAccessPlan::exact_equality(
            catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:User:email"))
                .with_uniqueness(catalog::IndexUniqueness::Unique),
            catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
            ir::IndexValue::Literal(
                ir::SecondaryIndexLiteral::new(PropertyValue::from("a@example.com")).unwrap(),
            ),
        );
        let exec::ExecNodeAccessPlan::Unique {
            lookup,
            verification,
        } = exact_unique
        else {
            panic!("unique fixture classifies exactly")
        };
        let node_range = exec::ExecNodeVerifiedRangeScanPlan {
            index: catalog::NodeRangeIndexMeta::new(test_support::name("node_range:User:rank:Asc")),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "rank",
                RangeIndexDirection::Asc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        };
        let edge_range = exec::ExecEdgeVerifiedRangeScanPlan {
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:rank:Asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "rank",
                RangeIndexDirection::Asc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        };
        let window = exec::ExecCountWindowPlan::identity();
        let plans = vec![
            exec::ExecCountPlan::NodeBitmap(exec::ExecNodeBitmapCountPlan {
                bitmap: node_point("User", "status", "active"),
                window: window.clone(),
            }),
            exec::ExecCountPlan::EdgeBitmap(exec::ExecEdgeBitmapCountPlan {
                bitmap: edge_point("FOLLOWS", "status", "active"),
                window: window.clone(),
            }),
            exec::ExecCountPlan::NodeUnique(exec::ExecNodeUniqueCountPlan {
                lookup: lookup.clone(),
                verification: verification.clone(),
                window: window.clone(),
            }),
            exec::ExecCountPlan::NodeRange(exec::ExecNodeRangeCountPlan {
                driver: node_range.clone(),
                membership: exec::ExecNodeRangeMembershipPlan::All,
                window: window.clone(),
            }),
            exec::ExecCountPlan::EdgeRange(exec::ExecEdgeRangeCountPlan {
                driver: edge_range.clone(),
                membership: exec::ExecEdgeRangeMembershipPlan::All,
                window: window.clone(),
            }),
            exec::ExecCountPlan::NodeAuthoritativeScan(exec::ExecNodeScanCountPlan {
                predicate: exec::ExecNodeAuthoritativeScanPredicate::Predicate(
                    ir::PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
                ),
                window: window.clone(),
            }),
            exec::ExecCountPlan::EdgeAuthoritativeScan(exec::ExecEdgeScanCountPlan {
                predicate: exec::ExecEdgeAuthoritativeScanPredicate::Predicate(
                    ir::PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
                ),
                window: window.clone(),
            }),
            exec::ExecCountPlan::NodePointReads {
                ids: test_support::ids(vec![1]),
                window: window.clone(),
            },
            exec::ExecCountPlan::EdgePointReads {
                ids: test_support::ids(vec![1]),
                window: window.clone(),
            },
            exec::ExecCountPlan::NodeRuntimeInput {
                input: exec::ExecRuntimeInputPlan::Param(ids.clone()),
                window: window.clone(),
            },
            exec::ExecCountPlan::EdgeRuntimeInput {
                input: exec::ExecRuntimeInputPlan::Param(ids),
                window: window.clone(),
            },
            exec::ExecCountPlan::NodeFullScan {
                window: window.clone(),
            },
            exec::ExecCountPlan::EdgeFullScan {
                window: window.clone(),
            },
            exec::ExecCountPlan::NodeLabelBitmap {
                label: test_support::name("User"),
                window: window.clone(),
            },
            exec::ExecCountPlan::EdgeLabelBitmap {
                label: test_support::name("FOLLOWS"),
                window: window.clone(),
            },
            exec::ExecCountPlan::NodeDynamicEquality(exec::ExecNodeDynamicEqualityCountPlan {
                index: catalog::NodeEqualityIndexMeta::new(test_support::name(
                    "node_eq:User:status",
                )),
                key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                param: late.clone(),
                window: window.clone(),
            }),
            exec::ExecCountPlan::EdgeDynamicEquality(exec::ExecEdgeDynamicEqualityCountPlan {
                index: catalog::EdgeEqualityIndexMeta::new(test_support::name(
                    "edge_eq:FOLLOWS:status",
                )),
                key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
                param: late,
                window,
            }),
        ];
        for plan in plans {
            assert!(execution
                .execute_count(ExecutionValue::Stream(Vec::new()), &plan)
                .await
                .is_err());
        }
        let cursors = vec![
            exec::ExecCountCursorPlan::NodeBitmap(node_point("User", "status", "active")),
            exec::ExecCountCursorPlan::EdgeBitmap(edge_point("FOLLOWS", "status", "active")),
            exec::ExecCountCursorPlan::NodeUnique {
                lookup,
                verification,
            },
            exec::ExecCountCursorPlan::NodeRange(node_range),
            exec::ExecCountCursorPlan::EdgeRange(edge_range),
            exec::ExecCountCursorPlan::NodeAuthoritativeScan(
                exec::ExecNodeAuthoritativeScanPredicate::Predicate(
                    ir::PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
                ),
            ),
            exec::ExecCountCursorPlan::EdgeAuthoritativeScan(
                exec::ExecEdgeAuthoritativeScanPredicate::Predicate(
                    ir::PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
                ),
            ),
            exec::ExecCountCursorPlan::NodePointReads(test_support::ids(vec![1])),
            exec::ExecCountCursorPlan::EdgePointReads(test_support::ids(vec![1])),
            exec::ExecCountCursorPlan::NodeRuntimeInput(exec::ExecRuntimeInputPlan::Param(
                test_support::name("ids"),
            )),
            exec::ExecCountCursorPlan::EdgeRuntimeInput(exec::ExecRuntimeInputPlan::Param(
                test_support::name("ids"),
            )),
            exec::ExecCountCursorPlan::NodeFullScan,
            exec::ExecCountCursorPlan::EdgeFullScan,
            exec::ExecCountCursorPlan::NodeLabelBitmap(test_support::name("User")),
            exec::ExecCountCursorPlan::EdgeLabelBitmap(test_support::name("FOLLOWS")),
            exec::ExecCountCursorPlan::NodeDynamicEquality {
                index: catalog::NodeEqualityIndexMeta::new(test_support::name(
                    "node_eq:User:status",
                )),
                key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                param: test_support::name("late"),
            },
            exec::ExecCountCursorPlan::EdgeDynamicEquality {
                index: catalog::EdgeEqualityIndexMeta::new(test_support::name(
                    "edge_eq:FOLLOWS:status",
                )),
                key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
                param: test_support::name("late"),
            },
        ];
        for cursor in cursors {
            let mut dependency = Some(ExecutionValue::Stream(Vec::new()));
            assert!(execution
                .count_cursor(&cursor, &mut dependency)
                .await
                .is_err());
            let plan = exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
                cursor,
                window: exec::ExecCountWindowPlan::identity(),
            });
            assert!(execution
                .execute_count(ExecutionValue::Stream(Vec::new()), &plan)
                .await
                .is_err());
        }
    }

    #[cfg_attr(test, tokio::test)]
    async fn direct_and_cursor_authoritative_counts_propagate_each_predicate_failure() {
        let db = test_support::open_db("count-corrupt-authority-errors").await;
        let node = test_support::add_node_with_properties(
            &db,
            "User",
            vec![("status", PropertyValue::from("active"))],
        )
        .await;
        let edge = test_support::add_edge_with_properties(
            &db,
            node,
            node,
            "FOLLOWS",
            vec![("status", PropertyValue::from("active"))],
        )
        .await;
        db.inner_db()
            .put(
                keys::Key::Data {
                    scope: crate::encoding::v1::keys::tenant::DataScope::LegacyUnscoped,
                    kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(node)),
                }
                .to_bytes(),
                bytes::Bytes::from_static(b"malformed node authority"),
            )
            .await
            .unwrap();
        db.inner_db()
            .put(
                keys::Key::Data {
                    scope: crate::encoding::v1::keys::tenant::DataScope::LegacyUnscoped,
                    kind: keys::DataKeyKind::EdgePropertyById(keys::EdgePropertyByIdKey::new(edge)),
                }
                .to_bytes(),
                bytes::Bytes::from_static(b"malformed edge authority"),
            )
            .await
            .unwrap();
        let node_predicates = [
            exec::ExecNodeAuthoritativeScanPredicate::NullEquality {
                key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
            },
            exec::ExecNodeAuthoritativeScanPredicate::Predicate(
                ir::PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
            ),
        ];
        let edge_predicates = [
            exec::ExecEdgeAuthoritativeScanPredicate::NullEquality {
                key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
            },
            exec::ExecEdgeAuthoritativeScanPredicate::Predicate(
                ir::PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
            ),
        ];
        let mut execution = ExecutionContext::new(&db, context::ParamBindings::default());
        for predicate in node_predicates {
            let cursor = exec::ExecCountCursorPlan::NodeAuthoritativeScan(predicate.clone());
            let mut dependency = Some(ExecutionValue::Stream(Vec::new()));
            assert!(execution
                .count_cursor(&cursor, &mut dependency)
                .await
                .is_err());
            for plan in [
                exec::ExecCountPlan::NodeAuthoritativeScan(exec::ExecNodeScanCountPlan {
                    predicate: predicate.clone(),
                    window: exec::ExecCountWindowPlan::identity(),
                }),
                exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
                    cursor: exec::ExecCountCursorPlan::NodeAuthoritativeScan(predicate),
                    window: exec::ExecCountWindowPlan::identity(),
                }),
            ] {
                assert!(execution
                    .execute_count(ExecutionValue::Stream(Vec::new()), &plan)
                    .await
                    .is_err());
            }
        }
        for predicate in edge_predicates {
            let cursor = exec::ExecCountCursorPlan::EdgeAuthoritativeScan(predicate.clone());
            let mut dependency = Some(ExecutionValue::Stream(Vec::new()));
            assert!(execution
                .count_cursor(&cursor, &mut dependency)
                .await
                .is_err());
            for plan in [
                exec::ExecCountPlan::EdgeAuthoritativeScan(exec::ExecEdgeScanCountPlan {
                    predicate: predicate.clone(),
                    window: exec::ExecCountWindowPlan::identity(),
                }),
                exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
                    cursor: exec::ExecCountCursorPlan::EdgeAuthoritativeScan(predicate),
                    window: exec::ExecCountWindowPlan::identity(),
                }),
            ] {
                assert!(execution
                    .execute_count(ExecutionValue::Stream(Vec::new()), &plan)
                    .await
                    .is_err());
            }
        }
    }

    #[cfg_attr(test, tokio::test)]
    async fn count_window_parameters_shapes_and_runtime_variables_fail_closed() {
        let db = test_support::open_db("count-window-shape-errors").await;
        let property = test_support::name("property");
        let negative = test_support::name("negative");
        let query = test_support::name("query");
        let query_negative = test_support::name("query_negative");
        let query_string = test_support::name("query_string");
        let missing = test_support::name("missing");
        let params = context::ParamBindings::default()
            .with_value(property.clone(), PropertyValue::I64(4))
            .with_value(negative.clone(), PropertyValue::I64(-1))
            .with_query_value(query.clone(), QueryValue::I64(5))
            .with_query_value(query_negative.clone(), QueryValue::I64(-2))
            .with_query_value(query_string.clone(), QueryValue::String("five".to_string()));
        let mut execution = ExecutionContext::new(&db, params);

        assert_eq!(execution.count_bound_param(&property).unwrap(), 4);
        assert_eq!(execution.count_bound_param(&query).unwrap(), 5);
        for name in [&negative, &query_negative, &query_string, &missing] {
            assert!(
                execution
                    .count_bound_param(name)
                    .unwrap_err()
                    .to_string()
                    .contains("not a non-negative integer")
                    || execution
                        .count_bound_param(name)
                        .unwrap_err()
                        .to_string()
                        .contains("not bound")
            );
        }

        let non_integer = test_support::name("non_integer");
        execution
            .params
            .values
            .insert(non_integer.clone(), PropertyValue::Bool(true));
        assert!(execution
            .count_bound_param(&non_integer)
            .unwrap_err()
            .to_string()
            .contains("not a non-negative integer"));
        let window = exec::ExecCountWindowPlan {
            skip: exec::ExecUsizeExpr::Param(property),
            take: exec::ExecCountTake::All,
        };
        assert_eq!(
            execution.count_window(&window).unwrap(),
            EvaluatedCountWindow {
                skip: 4,
                take: None,
            }
        );

        let scalar_plan = exec::ExecCountPlan::InputScalars {
            window: exec::ExecCountWindowPlan::identity(),
        };
        for (input, expected) in [
            (ExecutionValue::Count(99), 1),
            (ExecutionValue::Bool(false), 1),
            (
                ExecutionValue::Scalars(vec![
                    ExecutionScalar::NodeId(1),
                    ExecutionScalar::String("value".to_string()),
                ]),
                2,
            ),
        ] {
            assert_eq!(
                execution.execute_count(input, &scalar_plan).await.unwrap(),
                ExecutionValue::Count(expected)
            );
        }

        let row_plan = exec::ExecCountPlan::InputRows {
            window: exec::ExecCountWindowPlan::identity(),
        };
        for input in [
            ExecutionValue::FoldedStream(FoldedStream::new(Vec::new())),
            ExecutionValue::Count(0),
            ExecutionValue::Bool(false),
            ExecutionValue::Scalars(Vec::new()),
        ] {
            assert!(execution.execute_count(input, &row_plan).await.is_err());
        }
        assert!(execution
            .execute_count(ExecutionValue::Stream(Vec::new()), &scalar_plan)
            .await
            .is_err());
        assert!(execution
            .execute_count(
                ExecutionValue::IndexDdlReceipt(
                    crate::index_lifecycle::IndexDdlReceipt::ExistingOperation {
                        operation_id: crate::index_lifecycle::IndexOperationId::from_bytes([9; 16])
                            .unwrap(),
                    },
                ),
                &scalar_plan,
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("index lifecycle value"));

        let rows = test_support::name("rows");
        execution
            .variables
            .insert(rows.clone(), ExecutionValue::Count(2));
        for input in [
            exec::ExecRuntimeInputPlan::Variable(rows.clone()),
            exec::ExecRuntimeInputPlan::Variable(test_support::name("unbound")),
        ] {
            assert!(execution.runtime_ids(&input).is_err());
            assert!(execution.runtime_row_count(&input).is_err());
        }

        let mut scalar_dependency = Some(ExecutionValue::Scalars(Vec::new()));
        assert!(execution
            .count_cursor(
                &exec::ExecCountCursorPlan::InputRows,
                &mut scalar_dependency,
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("expected rows"));
        let scalar_rows = test_support::name("scalar_rows");
        execution
            .variables
            .insert(scalar_rows.clone(), ExecutionValue::Bool(true));
        let mut no_dependency = Some(ExecutionValue::Stream(Vec::new()));
        assert!(execution
            .count_cursor(
                &exec::ExecCountCursorPlan::RuntimeInput(exec::ExecRuntimeInputPlan::Variable(
                    scalar_rows
                ),),
                &mut no_dependency,
            )
            .await
            .is_err());

        let multiple_inputs = exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
            cursor: exec::ExecCountCursorPlan::Union {
                driver: Box::new(exec::ExecCountCursorPlan::InputRows),
                rest: ir::AtLeast::from_one(exec::ExecCountCursorPlan::InputRows),
            },
            window: exec::ExecCountWindowPlan::identity(),
        });
        assert!(execution
            .execute_count(ExecutionValue::Stream(Vec::new()), &multiple_inputs)
            .await
            .unwrap_err()
            .to_string()
            .contains("invalid count program"));

        let mut oversized_expression = exec::ExecUsizeExpr::literal(0);
        for _ in 0..exec::MAX_EXEC_USIZE_EXPR_NODES {
            oversized_expression = exec::ExecUsizeExpr::SaturatingAdd(
                Box::new(oversized_expression),
                Box::new(exec::ExecUsizeExpr::literal(1)),
            );
        }
        let invalid_window = exec::ExecCountPlan::InputRows {
            window: exec::ExecCountWindowPlan {
                skip: oversized_expression,
                take: exec::ExecCountTake::All,
            },
        };
        assert!(execution
            .execute_count(ExecutionValue::Stream(Vec::new()), &invalid_window)
            .await
            .is_err());

        let unique_index = exec::ExecNodeUniqueEqualityIndex::try_from(
            catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:User:email"))
                .with_uniqueness(catalog::IndexUniqueness::Unique),
        )
        .unwrap();
        let email = catalog::ScopedPropertyKey::try_new("User", "email").unwrap();
        let lookup = exec::ExecNodeUniqueOwnerReadPlan {
            index: unique_index,
            key: email.clone(),
            value: indexed("a@example.com"),
        };
        for verification in [
            exec::ExecNodeAuthoritativeVerificationPlan {
                key: catalog::ScopedPropertyKey::try_new("Other", "email").unwrap(),
                value: indexed("a@example.com"),
            },
            exec::ExecNodeAuthoritativeVerificationPlan {
                key: email,
                value: indexed("b@example.com"),
            },
        ] {
            assert!(execution
                .verified_node_unique_owner(&lookup, &verification)
                .await
                .unwrap_err()
                .to_string()
                .contains("does not match"));
        }
    }

    #[cfg_attr(test, tokio::test)]
    async fn recursive_count_cursor_matrix_preserves_identity_and_child_order() {
        let db = test_support::open_db_with_config(
            test_support::in_memory_config("count-recursive-cursor-matrix")
                .with_equality_index("User", "status")
                .with_unique_equality_index("User", "email")
                .with_edge_equality_index("FOLLOWS", "status")
                .with_range_index("User", "rank")
                .with_edge_range_index("FOLLOWS", "rank"),
        )
        .await;
        let first = test_support::add_node_with_properties(
            &db,
            "User",
            vec![
                ("status", PropertyValue::from("active")),
                ("email", PropertyValue::from("a@example.com")),
                ("rank", PropertyValue::from("a")),
            ],
        )
        .await;
        let second = test_support::add_node_with_properties(
            &db,
            "User",
            vec![
                ("status", PropertyValue::from("paused")),
                ("email", PropertyValue::from("b@example.com")),
                ("rank", PropertyValue::from("b")),
            ],
        )
        .await;
        let null_node = test_support::add_node_with_properties(&db, "User", Vec::new()).await;
        let first_edge = test_support::add_edge_with_properties(
            &db,
            first,
            second,
            "FOLLOWS",
            vec![
                ("status", PropertyValue::from("active")),
                ("rank", PropertyValue::from("a")),
            ],
        )
        .await;
        let second_edge = test_support::add_edge_with_properties(
            &db,
            second,
            first,
            "FOLLOWS",
            vec![("rank", PropertyValue::from("b"))],
        )
        .await;
        let ids = test_support::name("ids");
        let late_node = test_support::name("late_node");
        let late_edge = test_support::name("late_edge");
        let params = context::ParamBindings::default()
            .with_value(
                ids.clone(),
                PropertyValue::I64Array(vec![first as i64, second as i64]),
            )
            .with_value(late_node.clone(), PropertyValue::from("active"))
            .with_value(late_edge.clone(), PropertyValue::from("active"));
        let mut execution = ExecutionContext::new(&db, params);
        execution.enable_request_read_view().await.unwrap();
        let node_rows_variable = test_support::name("node_rows");
        let edge_rows_variable = test_support::name("edge_rows");
        let mixed_rows_variable = test_support::name("mixed_rows");
        execution.variables.insert(
            node_rows_variable.clone(),
            ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Node(first))]),
        );
        execution.variables.insert(
            edge_rows_variable.clone(),
            ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Edge(first_edge))]),
        );
        execution.variables.insert(
            mixed_rows_variable.clone(),
            ExecutionValue::Stream(vec![
                ExecutionRow::current(ElementRef::Node(first)),
                ExecutionRow::current(ElementRef::Edge(first_edge)),
            ]),
        );

        let exact_unique = exec::ExecNodeAccessPlan::exact_equality(
            catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:User:email"))
                .with_uniqueness(catalog::IndexUniqueness::Unique),
            catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
            ir::IndexValue::Literal(
                ir::SecondaryIndexLiteral::new(PropertyValue::from("a@example.com")).unwrap(),
            ),
        );
        let exec::ExecNodeAccessPlan::Unique {
            lookup,
            verification,
        } = exact_unique
        else {
            panic!("unique fixture selects unique owner verification")
        };
        let node_range = exec::ExecNodeVerifiedRangeScanPlan {
            index: catalog::NodeRangeIndexMeta::new(test_support::name("node_range:User:rank:Asc")),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "rank",
                RangeIndexDirection::Asc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        };
        let edge_range = exec::ExecEdgeVerifiedRangeScanPlan {
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:rank:Asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "rank",
                RangeIndexDirection::Asc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        };

        let sources = vec![
            (exec::ExecCountCursorPlan::EmptyRows, 0),
            (
                exec::ExecCountCursorPlan::NodeBitmap(node_point("User", "status", "active")),
                1,
            ),
            (
                exec::ExecCountCursorPlan::EdgeBitmap(edge_point("FOLLOWS", "status", "active")),
                1,
            ),
            (
                exec::ExecCountCursorPlan::NodeUnique {
                    lookup,
                    verification,
                },
                1,
            ),
            (exec::ExecCountCursorPlan::NodeRange(node_range.clone()), 2),
            (exec::ExecCountCursorPlan::EdgeRange(edge_range.clone()), 2),
            (
                exec::ExecCountCursorPlan::NodeAuthoritativeScan(
                    exec::ExecNodeAuthoritativeScanPredicate::Predicate(
                        ir::PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
                    ),
                ),
                1,
            ),
            (
                exec::ExecCountCursorPlan::NodeAuthoritativeScan(
                    exec::ExecNodeAuthoritativeScanPredicate::NullEquality {
                        key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                    },
                ),
                1,
            ),
            (
                exec::ExecCountCursorPlan::EdgeAuthoritativeScan(
                    exec::ExecEdgeAuthoritativeScanPredicate::NullEquality {
                        key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
                    },
                ),
                1,
            ),
            (
                exec::ExecCountCursorPlan::EdgeAuthoritativeScan(
                    exec::ExecEdgeAuthoritativeScanPredicate::Predicate(
                        ir::PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
                    ),
                ),
                1,
            ),
            (
                exec::ExecCountCursorPlan::NodePointReads(test_support::ids(vec![
                    first, 999_999, second,
                ])),
                2,
            ),
            (
                exec::ExecCountCursorPlan::EdgePointReads(test_support::ids(vec![
                    first_edge,
                    999_999,
                    second_edge,
                ])),
                2,
            ),
            (
                exec::ExecCountCursorPlan::NodeRuntimeInput(exec::ExecRuntimeInputPlan::Param(
                    ids.clone(),
                )),
                2,
            ),
            (
                exec::ExecCountCursorPlan::EdgeRuntimeInput(exec::ExecRuntimeInputPlan::Param(
                    ids.clone(),
                )),
                2,
            ),
            (
                exec::ExecCountCursorPlan::RuntimeInput(exec::ExecRuntimeInputPlan::Param(
                    ids.clone(),
                )),
                2,
            ),
            (
                exec::ExecCountCursorPlan::NodeRuntimeInput(exec::ExecRuntimeInputPlan::Variable(
                    node_rows_variable,
                )),
                1,
            ),
            (
                exec::ExecCountCursorPlan::EdgeRuntimeInput(exec::ExecRuntimeInputPlan::Variable(
                    edge_rows_variable,
                )),
                1,
            ),
            (
                exec::ExecCountCursorPlan::RuntimeInput(exec::ExecRuntimeInputPlan::Variable(
                    mixed_rows_variable,
                )),
                2,
            ),
            (exec::ExecCountCursorPlan::NodeFullScan, 3),
            (exec::ExecCountCursorPlan::EdgeFullScan, 2),
            (
                exec::ExecCountCursorPlan::NodeLabelBitmap(test_support::name("User")),
                3,
            ),
            (
                exec::ExecCountCursorPlan::EdgeLabelBitmap(test_support::name("FOLLOWS")),
                2,
            ),
            (
                exec::ExecCountCursorPlan::NodeDynamicEquality {
                    index: catalog::NodeEqualityIndexMeta::new(test_support::name(
                        "node_eq:User:status",
                    )),
                    key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                    param: late_node,
                },
                1,
            ),
            (
                exec::ExecCountCursorPlan::EdgeDynamicEquality {
                    index: catalog::EdgeEqualityIndexMeta::new(test_support::name(
                        "edge_eq:FOLLOWS:status",
                    )),
                    key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
                    param: late_edge,
                },
                1,
            ),
        ];
        for (cursor, expected) in sources {
            let mut dependency = Some(ExecutionValue::Stream(Vec::new()));
            assert_eq!(
                execution
                    .count_cursor(&cursor, &mut dependency)
                    .await
                    .unwrap()
                    .len(),
                expected
            );
            let plan = exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
                cursor,
                window: exec::ExecCountWindowPlan::identity(),
            });
            assert_eq!(
                execution
                    .execute_count(ExecutionValue::Stream(Vec::new()), &plan)
                    .await
                    .unwrap(),
                ExecutionValue::Count(expected)
            );
        }

        let union = exec::ExecCountCursorPlan::Union {
            driver: Box::new(exec::ExecCountCursorPlan::NodePointReads(
                test_support::ids(vec![first]),
            )),
            rest: ir::AtLeast::from_one(exec::ExecCountCursorPlan::NodePointReads(
                test_support::ids(vec![first, second]),
            )),
        };
        let intersection = exec::ExecCountCursorPlan::Intersect {
            driver: Box::new(union.clone()),
            rest: ir::AtLeast::from_one(exec::ExecCountCursorPlan::NodePointReads(
                test_support::ids(vec![second, null_node]),
            )),
        };
        let filtered = exec::ExecCountCursorPlan::Filter {
            input: Box::new(union.clone()),
            predicate: ir::PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
        };
        let positioned = exec::ExecCountCursorPlan::Window {
            input: Box::new(union.clone()),
            window: bounded(1, 1),
        };
        let ordered = exec::ExecCountCursorPlan::Order {
            input: Box::new(union.clone()),
            plan: ir::OrderPlan::RangeIndex {
                key: ir::OrderKey {
                    property: test_support::name("rank"),
                    order: helix_ast::traversal::Order::Asc,
                },
                index_id: test_support::name("node_range:User:rank:Asc"),
            },
        };
        let bound = exec::ExecCountCursorPlan::Variable {
            input: Box::new(union.clone()),
            op: helix_planner::logical::PureStreamVariableOp::Bind(test_support::name("current")),
        };
        let expanded = exec::ExecCountCursorPlan::Expand {
            input: Box::new(exec::ExecCountCursorPlan::NodePointReads(
                test_support::ids(vec![first]),
            )),
            plan: ir::ExpandPlan {
                direction: ir::ExpandDirection::Out,
                output: ir::ExpandOutput::Nodes,
                label: ir::ExpandLabelPlan::Any,
            },
        };
        for (cursor, expected) in [
            (union, 2),
            (intersection, 1),
            (filtered, 1),
            (positioned, 1),
            (ordered, 2),
            (bound, 2),
            (expanded, 1),
        ] {
            let mut dependency = Some(ExecutionValue::Stream(Vec::new()));
            assert_eq!(
                execution
                    .count_cursor(&cursor, &mut dependency)
                    .await
                    .unwrap()
                    .len(),
                expected
            );
            let plan = exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
                cursor,
                window: bounded(0, expected),
            });
            assert_eq!(
                execution
                    .execute_count(ExecutionValue::Stream(Vec::new()), &plan)
                    .await
                    .unwrap(),
                ExecutionValue::Count(expected)
            );
        }

        for algorithm in [
            exec::ExecCountDistinctPlan::HashRows,
            exec::ExecCountDistinctPlan::OrderedRows,
        ] {
            let cursor = exec::ExecCountCursorPlan::Distinct {
                input: Box::new(exec::ExecCountCursorPlan::InputRows),
                plan: algorithm,
            };
            let rows = vec![
                ExecutionRow::current(ElementRef::Node(first)),
                ExecutionRow::current(ElementRef::Node(first)),
                ExecutionRow::current(ElementRef::Node(second)),
            ];
            let mut dependency = Some(ExecutionValue::Stream(rows.clone()));
            assert_eq!(
                execution
                    .count_cursor(&cursor, &mut dependency)
                    .await
                    .unwrap()
                    .len(),
                2
            );
            let plan = exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
                cursor,
                window: exec::ExecCountWindowPlan::identity(),
            });
            assert_eq!(
                execution
                    .execute_count(ExecutionValue::Stream(rows), &plan)
                    .await
                    .unwrap(),
                ExecutionValue::Count(2)
            );
        }

        let mut dependency = Some(ExecutionValue::Stream(Vec::new()));
        assert!(execution
            .count_cursor(&exec::ExecCountCursorPlan::InputRows, &mut dependency)
            .await
            .unwrap()
            .is_empty());
        assert!(execution
            .count_cursor(&exec::ExecCountCursorPlan::InputRows, &mut dependency)
            .await
            .unwrap_err()
            .to_string()
            .contains("more than once"));
        execution.close_request_read_view().unwrap();
    }

    #[cfg_attr(test, tokio::test)]
    async fn every_recursive_cursor_wrapper_propagates_child_failure_in_encoded_order() {
        let db = test_support::open_db("count-recursive-child-errors").await;
        let mut execution = ExecutionContext::new(&db, context::ParamBindings::default());
        let invalid = || exec::ExecCountCursorPlan::NodeDynamicEquality {
            index: catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:Other:status")),
            key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
            param: test_support::name("missing"),
        };
        let index = access_support::search_index("missing-search-index");
        let k = access_support::literal_search_limit(1);
        let wrappers = vec![
            exec::ExecCountCursorPlan::Union {
                driver: Box::new(invalid()),
                rest: ir::AtLeast::from_one(exec::ExecCountCursorPlan::EmptyRows),
            },
            exec::ExecCountCursorPlan::Union {
                driver: Box::new(exec::ExecCountCursorPlan::EmptyRows),
                rest: ir::AtLeast::from_one(invalid()),
            },
            exec::ExecCountCursorPlan::Intersect {
                driver: Box::new(invalid()),
                rest: ir::AtLeast::from_one(exec::ExecCountCursorPlan::EmptyRows),
            },
            exec::ExecCountCursorPlan::Intersect {
                driver: Box::new(exec::ExecCountCursorPlan::EmptyRows),
                rest: ir::AtLeast::from_one(invalid()),
            },
            exec::ExecCountCursorPlan::Filter {
                input: Box::new(invalid()),
                predicate: ir::PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
            },
            exec::ExecCountCursorPlan::Window {
                input: Box::new(invalid()),
                window: exec::ExecCountWindowPlan::identity(),
            },
            exec::ExecCountCursorPlan::Order {
                input: Box::new(invalid()),
                plan: ir::OrderPlan::RangeIndex {
                    key: ir::OrderKey {
                        property: test_support::name("rank"),
                        order: helix_ast::traversal::Order::Asc,
                    },
                    index_id: test_support::name("node_range:User:rank:Asc"),
                },
            },
            exec::ExecCountCursorPlan::Expand {
                input: Box::new(invalid()),
                plan: ir::ExpandPlan {
                    direction: ir::ExpandDirection::Out,
                    output: ir::ExpandOutput::Nodes,
                    label: ir::ExpandLabelPlan::Any,
                },
            },
            exec::ExecCountCursorPlan::VectorSearch {
                input: Box::new(invalid()),
                plan: Box::new(ir::RestrictedVectorSearchPlan::Nodes {
                    key: catalog::NodeSearchIndexKey::try_new("User", "embedding").unwrap(),
                    index: index.clone(),
                    query_vector: ir::VectorQueryInputPlan::Vector(
                        ir::SearchVector::new(vec![1.0]).unwrap(),
                    ),
                    k: k.clone(),
                }),
            },
            exec::ExecCountCursorPlan::TextSearch {
                input: Box::new(invalid()),
                plan: Box::new(ir::RestrictedTextSearchPlan::Nodes {
                    key: catalog::NodeSearchIndexKey::try_new("User", "body").unwrap(),
                    index,
                    query_text: ir::TextQueryInputPlan::Text(test_support::name("rust")),
                    k,
                }),
            },
            exec::ExecCountCursorPlan::Variable {
                input: Box::new(invalid()),
                op: helix_planner::logical::PureStreamVariableOp::Bind(test_support::name("bound")),
            },
            exec::ExecCountCursorPlan::Distinct {
                input: Box::new(invalid()),
                plan: exec::ExecCountDistinctPlan::HashRows,
            },
        ];
        for cursor in wrappers {
            let mut dependency = Some(ExecutionValue::Stream(Vec::new()));
            assert!(execution
                .count_cursor(&cursor, &mut dependency)
                .await
                .is_err());
            let plan = exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
                cursor,
                window: exec::ExecCountWindowPlan::identity(),
            });
            assert!(execution
                .execute_count(ExecutionValue::Stream(Vec::new()), &plan)
                .await
                .is_err());
        }
    }

    #[cfg_attr(test, tokio::test)]
    async fn every_recursive_cursor_operation_propagates_its_primitive_failure() {
        let db = test_support::open_db("count-recursive-operation-errors").await;
        let node = test_support::add_node_with_properties(
            &db,
            "User",
            vec![("status", PropertyValue::from("active"))],
        )
        .await;
        db.inner_db()
            .put(
                keys::Key::Data {
                    scope: crate::encoding::v1::keys::tenant::DataScope::LegacyUnscoped,
                    kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(node)),
                }
                .to_bytes(),
                bytes::Bytes::from_static(b"malformed operation authority"),
            )
            .await
            .unwrap();
        let mut execution = ExecutionContext::new(&db, context::ParamBindings::default());
        for cursor in [
            exec::ExecCountCursorPlan::Filter {
                input: Box::new(exec::ExecCountCursorPlan::NodePointReads(
                    test_support::ids(vec![node]),
                )),
                predicate: ir::PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
            },
            exec::ExecCountCursorPlan::Order {
                input: Box::new(exec::ExecCountCursorPlan::NodePointReads(
                    test_support::ids(vec![node]),
                )),
                plan: ir::OrderPlan::ExplicitSort(ir::OrderKeys::from(ir::OrderKey {
                    property: test_support::name("status"),
                    order: helix_ast::traversal::Order::Asc,
                })),
            },
        ] {
            let mut dependency = Some(ExecutionValue::Stream(Vec::new()));
            assert!(execution
                .count_cursor(&cursor, &mut dependency)
                .await
                .is_err());
            let plan = exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
                cursor,
                window: exec::ExecCountWindowPlan::identity(),
            });
            assert!(execution
                .execute_count(ExecutionValue::Stream(Vec::new()), &plan)
                .await
                .is_err());
        }
        for op in [
            helix_planner::logical::PureStreamVariableOp::Select(test_support::name("missing")),
            helix_planner::logical::PureStreamVariableOp::Inject(test_support::name("missing")),
            helix_planner::logical::PureStreamVariableOp::Within(test_support::name("missing")),
            helix_planner::logical::PureStreamVariableOp::Without(test_support::name("missing")),
        ] {
            let cursor = exec::ExecCountCursorPlan::Variable {
                input: Box::new(exec::ExecCountCursorPlan::EmptyRows),
                op,
            };
            let mut dependency = Some(ExecutionValue::Stream(Vec::new()));
            assert!(execution
                .count_cursor(&cursor, &mut dependency)
                .await
                .is_err());
        }

        let closed_db = test_support::open_db("count-recursive-closed-operation-errors").await;
        closed_db.inner_db().close().await.unwrap();
        let index = access_support::search_index("missing-search-index");
        let k = access_support::literal_search_limit(1);
        let mut closed = ExecutionContext::new(&closed_db, context::ParamBindings::default());
        let cursors = [
            exec::ExecCountCursorPlan::Expand {
                input: Box::new(exec::ExecCountCursorPlan::InputRows),
                plan: ir::ExpandPlan {
                    direction: ir::ExpandDirection::Out,
                    output: ir::ExpandOutput::Nodes,
                    label: ir::ExpandLabelPlan::Any,
                },
            },
            exec::ExecCountCursorPlan::VectorSearch {
                input: Box::new(exec::ExecCountCursorPlan::InputRows),
                plan: Box::new(ir::RestrictedVectorSearchPlan::Nodes {
                    key: catalog::NodeSearchIndexKey::try_new("User", "embedding").unwrap(),
                    index: index.clone(),
                    query_vector: ir::VectorQueryInputPlan::Vector(
                        ir::SearchVector::new(vec![1.0]).unwrap(),
                    ),
                    k: k.clone(),
                }),
            },
            exec::ExecCountCursorPlan::TextSearch {
                input: Box::new(exec::ExecCountCursorPlan::InputRows),
                plan: Box::new(ir::RestrictedTextSearchPlan::Nodes {
                    key: catalog::NodeSearchIndexKey::try_new("User", "body").unwrap(),
                    index,
                    query_text: ir::TextQueryInputPlan::Text(test_support::name("rust")),
                    k,
                }),
            },
        ];
        for cursor in cursors {
            let mut dependency = Some(ExecutionValue::Stream(vec![ExecutionRow::current(
                ElementRef::Node(1),
            )]));
            assert!(closed.count_cursor(&cursor, &mut dependency).await.is_err());
        }
    }

    #[cfg_attr(test, tokio::test)]
    async fn direct_and_restricted_search_count_families_use_selected_search_primitives() {
        let db = test_support::open_db_with_config(
            test_support::in_memory_config("count-search-family-matrix")
                .with_node_vector_index(
                    "Doc",
                    "embedding",
                    2,
                    crate::search::vector::VectorDistanceMetric::Cosine,
                )
                .with_edge_vector_index(
                    "MENTIONS",
                    "embedding",
                    2,
                    crate::search::vector::VectorDistanceMetric::Euclidean,
                )
                .with_node_text_index("Doc", "body")
                .with_edge_text_index("MENTIONS", "body"),
        )
        .await;
        let node = test_support::add_node_with_properties(
            &db,
            "Doc",
            vec![
                ("embedding", PropertyValue::F32Array(vec![1.0, 0.0])),
                ("body", PropertyValue::from("rust planner execution")),
            ],
        )
        .await;
        let other = test_support::add_node_with_properties(&db, "Doc", Vec::new()).await;
        let edge = test_support::add_edge_with_properties(
            &db,
            node,
            other,
            "MENTIONS",
            vec![
                ("embedding", PropertyValue::F32Array(vec![1.0, 0.0])),
                ("body", PropertyValue::from("rust graph edge")),
            ],
        )
        .await;
        let node_vector_definition = crate::config::VectorIndexDefinition::new_node(
            "Doc",
            "embedding",
            2,
            crate::search::vector::VectorDistanceMetric::Cosine,
        )
        .unwrap();
        access_support::seed_vector_index::<crate::search::vector::distance::Cosine>(
            &db,
            &node_vector_definition,
            &[(node, vec![1.0, 0.0]), (999_999, vec![0.5, 0.5])],
        )
        .await;
        let edge_vector_definition = crate::config::VectorIndexDefinition::new_edge(
            "MENTIONS",
            "embedding",
            2,
            crate::search::vector::VectorDistanceMetric::Euclidean,
        )
        .unwrap();
        access_support::seed_vector_index::<crate::search::vector::distance::Euclidean>(
            &db,
            &edge_vector_definition,
            &[(edge, vec![1.0, 0.0]), (999_998, vec![0.5, 0.5])],
        )
        .await;
        let node_text_definition =
            crate::config::TextIndexDefinition::new_node("Doc", "body").unwrap();
        access_support::seed_managed_text_index(
            &db,
            &node_text_definition,
            &[crate::search::text::TextDocumentInput::new(
                node,
                "rust planner execution",
            )],
        )
        .await;
        let edge_text_definition =
            crate::config::TextIndexDefinition::new_edge("MENTIONS", "body").unwrap();
        access_support::seed_managed_text_index(
            &db,
            &edge_text_definition,
            &[crate::search::text::TextDocumentInput::new(
                edge,
                "rust graph edge",
            )],
        )
        .await;

        let node_vector_index = access_support::search_index(&crate::search::vector_index_name(
            crate::config::VectorElementType::Node,
            "Doc",
            "embedding",
        ));
        let edge_vector_index = access_support::search_index(&crate::search::vector_index_name(
            crate::config::VectorElementType::Edge,
            "MENTIONS",
            "embedding",
        ));
        let node_text_index = access_support::search_index(&crate::search::text_index_name(
            crate::config::TextElementType::Node,
            "Doc",
            "body",
        ));
        let edge_text_index = access_support::search_index(&crate::search::text_index_name(
            crate::config::TextElementType::Edge,
            "MENTIONS",
            "body",
        ));
        let node_vector = exec::ExecNodeVectorSearchCountPlan {
            key: catalog::NodeSearchIndexKey::try_new("Doc", "embedding").unwrap(),
            index: node_vector_index.clone(),
            query_vector: ir::VectorQueryInputPlan::Vector(
                ir::SearchVector::new(vec![1.0, 0.0]).unwrap(),
            ),
            k: access_support::literal_search_limit(2),
            window: exec::ExecCountWindowPlan::identity(),
        };
        let edge_vector = exec::ExecEdgeVectorSearchCountPlan {
            key: catalog::EdgeSearchIndexKey::try_new("MENTIONS", "embedding").unwrap(),
            index: edge_vector_index.clone(),
            query_vector: ir::VectorQueryInputPlan::Vector(
                ir::SearchVector::new(vec![1.0, 0.0]).unwrap(),
            ),
            k: access_support::literal_search_limit(2),
            window: exec::ExecCountWindowPlan::identity(),
        };
        let node_text = exec::ExecNodeTextSearchCountPlan {
            key: catalog::NodeSearchIndexKey::try_new("Doc", "body").unwrap(),
            index: node_text_index.clone(),
            query_text: ir::TextQueryInputPlan::Text(test_support::name("rust")),
            k: access_support::literal_search_limit(2),
            window: exec::ExecCountWindowPlan::identity(),
        };
        let edge_text = exec::ExecEdgeTextSearchCountPlan {
            key: catalog::EdgeSearchIndexKey::try_new("MENTIONS", "body").unwrap(),
            index: edge_text_index.clone(),
            query_text: ir::TextQueryInputPlan::Text(test_support::name("rust")),
            k: access_support::literal_search_limit(2),
            window: exec::ExecCountWindowPlan::identity(),
        };
        for plan in [
            exec::ExecCountPlan::NodeVectorSearch(node_vector.clone()),
            exec::ExecCountPlan::EdgeVectorSearch(edge_vector.clone()),
            exec::ExecCountPlan::NodeTextSearch(node_text.clone()),
            exec::ExecCountPlan::EdgeTextSearch(edge_text.clone()),
        ] {
            assert_eq!(
                execute_direct_count(&db, plan).await.unwrap(),
                ExecutionValue::Count(1)
            );
        }

        let mut execution = ExecutionContext::new(&db, context::ParamBindings::default());
        execution.enable_request_read_view().await.unwrap();
        for cursor in [
            exec::ExecCountCursorPlan::NodeVectorSearch {
                key: node_vector.key.clone(),
                index: node_vector.index.clone(),
                query_vector: node_vector.query_vector.clone(),
                k: node_vector.k.clone(),
            },
            exec::ExecCountCursorPlan::EdgeVectorSearch {
                key: edge_vector.key.clone(),
                index: edge_vector.index.clone(),
                query_vector: edge_vector.query_vector.clone(),
                k: edge_vector.k.clone(),
            },
            exec::ExecCountCursorPlan::NodeTextSearch {
                key: node_text.key.clone(),
                index: node_text.index.clone(),
                query_text: node_text.query_text.clone(),
                k: node_text.k.clone(),
            },
            exec::ExecCountCursorPlan::EdgeTextSearch {
                key: edge_text.key.clone(),
                index: edge_text.index.clone(),
                query_text: edge_text.query_text.clone(),
                k: edge_text.k.clone(),
            },
            exec::ExecCountCursorPlan::VectorSearch {
                input: Box::new(exec::ExecCountCursorPlan::NodePointReads(
                    test_support::ids(vec![node]),
                )),
                plan: Box::new(ir::RestrictedVectorSearchPlan::Nodes {
                    key: node_vector.key,
                    index: node_vector_index,
                    query_vector: node_vector.query_vector,
                    k: node_vector.k,
                }),
            },
            exec::ExecCountCursorPlan::VectorSearch {
                input: Box::new(exec::ExecCountCursorPlan::EdgePointReads(
                    test_support::ids(vec![edge]),
                )),
                plan: Box::new(ir::RestrictedVectorSearchPlan::Edges {
                    key: edge_vector.key,
                    index: edge_vector_index,
                    query_vector: edge_vector.query_vector,
                    k: edge_vector.k,
                }),
            },
            exec::ExecCountCursorPlan::TextSearch {
                input: Box::new(exec::ExecCountCursorPlan::NodePointReads(
                    test_support::ids(vec![node]),
                )),
                plan: Box::new(ir::RestrictedTextSearchPlan::Nodes {
                    key: node_text.key,
                    index: node_text_index,
                    query_text: node_text.query_text,
                    k: node_text.k,
                }),
            },
            exec::ExecCountCursorPlan::TextSearch {
                input: Box::new(exec::ExecCountCursorPlan::EdgePointReads(
                    test_support::ids(vec![edge]),
                )),
                plan: Box::new(ir::RestrictedTextSearchPlan::Edges {
                    key: edge_text.key,
                    index: edge_text_index,
                    query_text: edge_text.query_text,
                    k: edge_text.k,
                }),
            },
        ] {
            let mut dependency = Some(ExecutionValue::Stream(Vec::new()));
            assert_eq!(
                execution
                    .count_cursor(&cursor, &mut dependency)
                    .await
                    .unwrap()
                    .len(),
                1
            );
            let plan = exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
                cursor,
                window: exec::ExecCountWindowPlan::identity(),
            });
            assert_eq!(
                execution
                    .execute_count(ExecutionValue::Stream(Vec::new()), &plan)
                    .await
                    .unwrap(),
                ExecutionValue::Count(1)
            );
        }
        execution.close_request_read_view().unwrap();
    }

    #[cfg_attr(test, tokio::test)]
    async fn every_direct_and_cursor_search_propagates_its_selected_read_failure() {
        let db = test_support::open_db("count-search-storage-errors").await;
        db.inner_db().close().await.unwrap();
        let index = access_support::search_index("missing-search-index");
        let k = access_support::literal_search_limit(1);
        let node_key = catalog::NodeSearchIndexKey::try_new("Doc", "body").unwrap();
        let edge_key = catalog::EdgeSearchIndexKey::try_new("MENTIONS", "body").unwrap();
        let vector = ir::VectorQueryInputPlan::Vector(ir::SearchVector::new(vec![1.0]).unwrap());
        let text = ir::TextQueryInputPlan::Text(test_support::name("rust"));
        let window = exec::ExecCountWindowPlan::identity();
        let direct = vec![
            exec::ExecCountPlan::NodeVectorSearch(exec::ExecNodeVectorSearchCountPlan {
                key: node_key.clone(),
                index: index.clone(),
                query_vector: vector.clone(),
                k: k.clone(),
                window: window.clone(),
            }),
            exec::ExecCountPlan::EdgeVectorSearch(exec::ExecEdgeVectorSearchCountPlan {
                key: edge_key.clone(),
                index: index.clone(),
                query_vector: vector.clone(),
                k: k.clone(),
                window: window.clone(),
            }),
            exec::ExecCountPlan::NodeTextSearch(exec::ExecNodeTextSearchCountPlan {
                key: node_key.clone(),
                index: index.clone(),
                query_text: text.clone(),
                k: k.clone(),
                window: window.clone(),
            }),
            exec::ExecCountPlan::EdgeTextSearch(exec::ExecEdgeTextSearchCountPlan {
                key: edge_key.clone(),
                index: index.clone(),
                query_text: text.clone(),
                k: k.clone(),
                window: window.clone(),
            }),
        ];
        let mut execution = ExecutionContext::new(&db, context::ParamBindings::default());
        for plan in direct {
            assert!(execution
                .execute_count(ExecutionValue::Stream(Vec::new()), &plan)
                .await
                .is_err());
        }
        let cursors = vec![
            exec::ExecCountCursorPlan::NodeVectorSearch {
                key: node_key.clone(),
                index: index.clone(),
                query_vector: vector.clone(),
                k: k.clone(),
            },
            exec::ExecCountCursorPlan::EdgeVectorSearch {
                key: edge_key.clone(),
                index: index.clone(),
                query_vector: vector,
                k: k.clone(),
            },
            exec::ExecCountCursorPlan::NodeTextSearch {
                key: node_key,
                index: index.clone(),
                query_text: text.clone(),
                k: k.clone(),
            },
            exec::ExecCountCursorPlan::EdgeTextSearch {
                key: edge_key,
                index,
                query_text: text,
                k,
            },
        ];
        for cursor in cursors {
            let mut dependency = Some(ExecutionValue::Stream(Vec::new()));
            assert!(execution
                .count_cursor(&cursor, &mut dependency)
                .await
                .is_err());
            let plan = exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
                cursor,
                window: window.clone(),
            });
            assert!(execution
                .execute_count(ExecutionValue::Stream(Vec::new()), &plan)
                .await
                .is_err());
        }
    }

    #[cfg_attr(test, tokio::test)]
    async fn input_shapes_and_parameterized_windows_are_literal_contracts() {
        let db = test_support::open_db("count-input-shapes-and-windows").await;
        let skip = test_support::name("skip");
        let take = test_support::name("take");
        let params = context::ParamBindings::default()
            .with_value(skip.clone(), PropertyValue::I64(1))
            .with_value(take.clone(), PropertyValue::I64(2));
        let mut context = ExecutionContext::new(&db, params);
        let rows = vec![
            ExecutionRow::current(ElementRef::Node(1)),
            ExecutionRow::current(ElementRef::Node(2)),
            ExecutionRow::current(ElementRef::Node(3)),
            ExecutionRow::current(ElementRef::Node(4)),
        ];
        let plan = exec::ExecCountPlan::InputRows {
            window: exec::ExecCountWindowPlan {
                skip: exec::ExecUsizeExpr::Param(skip),
                take: exec::ExecCountTake::AtMost(exec::ExecUsizeExpr::Param(take)),
            },
        };

        assert_eq!(
            context
                .execute_count(ExecutionValue::Stream(rows), &plan)
                .await
                .unwrap(),
            ExecutionValue::Count(2)
        );
        let error = context
            .execute_count(ExecutionValue::Scalars(Vec::new()), &plan)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("count plan expected rows"));
    }

    #[cfg_attr(test, tokio::test)]
    async fn count_window_runtime_dynamic_range_and_deadline_errors_are_exhaustive() {
        let db = test_support::open_db("count-contract-error-matrix").await;
        let missing = test_support::name("missing");
        let mut execution = ExecutionContext::new(&db, context::ParamBindings::default());
        for window in [
            exec::ExecCountWindowPlan {
                skip: exec::ExecUsizeExpr::Param(missing.clone()),
                take: exec::ExecCountTake::All,
            },
            exec::ExecCountWindowPlan {
                skip: exec::ExecUsizeExpr::literal(0),
                take: exec::ExecCountTake::AtMost(exec::ExecUsizeExpr::Param(missing.clone())),
            },
        ] {
            assert!(execution
                .execute_count(
                    ExecutionValue::Stream(Vec::new()),
                    &exec::ExecCountPlan::InputRows { window },
                )
                .await
                .is_err());
        }

        let invalid_node_range = exec::ExecNodeVerifiedRangeScanPlan {
            index: catalog::NodeRangeIndexMeta::new(test_support::name(
                "node_range:Other:rank:Asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "rank",
                RangeIndexDirection::Asc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        };
        let invalid_edge_range = exec::ExecEdgeVerifiedRangeScanPlan {
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:OTHER:rank:Asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "rank",
                RangeIndexDirection::Asc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        };
        let valid_node_range = exec::ExecNodeVerifiedRangeScanPlan {
            index: catalog::NodeRangeIndexMeta::new(test_support::name("node_range:User:rank:Asc")),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "rank",
                RangeIndexDirection::Asc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        };
        let valid_edge_range = exec::ExecEdgeVerifiedRangeScanPlan {
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:rank:Asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "rank",
                RangeIndexDirection::Asc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        };
        let invalid_node_bitmap = exec::ExecNodeBitmapExpr::PointRead {
            index: node_equality_index("Other", "status"),
            key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
            value: indexed("active"),
        };
        let invalid_edge_bitmap = exec::ExecEdgeBitmapExpr::PointRead {
            index: edge_equality_index("OTHER", "status"),
            key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
            value: indexed("active"),
        };
        let direct = vec![
            exec::ExecCountPlan::NodeRange(exec::ExecNodeRangeCountPlan {
                driver: invalid_node_range.clone(),
                membership: exec::ExecNodeRangeMembershipPlan::All,
                window: exec::ExecCountWindowPlan::identity(),
            }),
            exec::ExecCountPlan::EdgeRange(exec::ExecEdgeRangeCountPlan {
                driver: invalid_edge_range.clone(),
                membership: exec::ExecEdgeRangeMembershipPlan::All,
                window: exec::ExecCountWindowPlan::identity(),
            }),
            exec::ExecCountPlan::NodeRange(exec::ExecNodeRangeCountPlan {
                driver: valid_node_range,
                membership: exec::ExecNodeRangeMembershipPlan::BitmapFilters(
                    ir::AtLeast::from_one(invalid_node_bitmap),
                ),
                window: exec::ExecCountWindowPlan::identity(),
            }),
            exec::ExecCountPlan::EdgeRange(exec::ExecEdgeRangeCountPlan {
                driver: valid_edge_range,
                membership: exec::ExecEdgeRangeMembershipPlan::BitmapFilters(
                    ir::AtLeast::from_one(invalid_edge_bitmap),
                ),
                window: exec::ExecCountWindowPlan::identity(),
            }),
            exec::ExecCountPlan::NodeRuntimeInput {
                input: exec::ExecRuntimeInputPlan::Param(missing.clone()),
                window: exec::ExecCountWindowPlan::identity(),
            },
            exec::ExecCountPlan::EdgeRuntimeInput {
                input: exec::ExecRuntimeInputPlan::Variable(missing.clone()),
                window: exec::ExecCountWindowPlan::identity(),
            },
            exec::ExecCountPlan::RuntimeInput {
                input: exec::ExecRuntimeInputPlan::Variable(missing.clone()),
                window: exec::ExecCountWindowPlan::identity(),
            },
            exec::ExecCountPlan::NodeDynamicEquality(exec::ExecNodeDynamicEqualityCountPlan {
                index: catalog::NodeEqualityIndexMeta::new(test_support::name(
                    "node_eq:Other:status",
                )),
                key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                param: missing.clone(),
                window: exec::ExecCountWindowPlan::identity(),
            }),
            exec::ExecCountPlan::NodeDynamicEquality(exec::ExecNodeDynamicEqualityCountPlan {
                index: catalog::NodeEqualityIndexMeta::new(test_support::name(
                    "node_eq:User:status",
                )),
                key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                param: missing.clone(),
                window: exec::ExecCountWindowPlan::identity(),
            }),
            exec::ExecCountPlan::EdgeDynamicEquality(exec::ExecEdgeDynamicEqualityCountPlan {
                index: catalog::EdgeEqualityIndexMeta::new(test_support::name(
                    "edge_eq:OTHER:status",
                )),
                key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
                param: missing.clone(),
                window: exec::ExecCountWindowPlan::identity(),
            }),
            exec::ExecCountPlan::EdgeDynamicEquality(exec::ExecEdgeDynamicEqualityCountPlan {
                index: catalog::EdgeEqualityIndexMeta::new(test_support::name(
                    "edge_eq:FOLLOWS:status",
                )),
                key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
                param: missing.clone(),
                window: exec::ExecCountWindowPlan::identity(),
            }),
        ];
        for plan in direct {
            assert!(execution
                .execute_count(ExecutionValue::Stream(Vec::new()), &plan)
                .await
                .is_err());
        }

        let cursors = vec![
            exec::ExecCountCursorPlan::NodeRange(invalid_node_range),
            exec::ExecCountCursorPlan::EdgeRange(invalid_edge_range),
            exec::ExecCountCursorPlan::NodeRuntimeInput(exec::ExecRuntimeInputPlan::Param(
                missing.clone(),
            )),
            exec::ExecCountCursorPlan::EdgeRuntimeInput(exec::ExecRuntimeInputPlan::Variable(
                missing.clone(),
            )),
            exec::ExecCountCursorPlan::RuntimeInput(exec::ExecRuntimeInputPlan::Variable(
                missing.clone(),
            )),
            exec::ExecCountCursorPlan::RuntimeInput(exec::ExecRuntimeInputPlan::Param(
                missing.clone(),
            )),
            exec::ExecCountCursorPlan::NodeDynamicEquality {
                index: catalog::NodeEqualityIndexMeta::new(test_support::name(
                    "node_eq:User:status",
                )),
                key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                param: missing.clone(),
            },
            exec::ExecCountCursorPlan::EdgeDynamicEquality {
                index: catalog::EdgeEqualityIndexMeta::new(test_support::name(
                    "edge_eq:OTHER:status",
                )),
                key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
                param: missing.clone(),
            },
            exec::ExecCountCursorPlan::EdgeDynamicEquality {
                index: catalog::EdgeEqualityIndexMeta::new(test_support::name(
                    "edge_eq:FOLLOWS:status",
                )),
                key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
                param: missing.clone(),
            },
            exec::ExecCountCursorPlan::Window {
                input: Box::new(exec::ExecCountCursorPlan::EmptyRows),
                window: exec::ExecCountWindowPlan {
                    skip: exec::ExecUsizeExpr::Param(missing),
                    take: exec::ExecCountTake::All,
                },
            },
        ];
        for cursor in cursors {
            let mut dependency = Some(ExecutionValue::Stream(Vec::new()));
            assert!(execution
                .count_cursor(&cursor, &mut dependency)
                .await
                .is_err());
            let plan = exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
                cursor,
                window: exec::ExecCountWindowPlan::identity(),
            });
            assert!(execution
                .execute_count(ExecutionValue::Stream(Vec::new()), &plan)
                .await
                .is_err());
        }

        let mut expired = ExecutionContext::new_scoped_controlled(
            &db,
            context::ParamBindings::default(),
            crate::encoding::v1::keys::tenant::DataScope::LegacyUnscoped,
            crate::execution_control::ExecutionControl::from_timeout(std::time::Duration::ZERO),
        );
        assert!(expired
            .node_bitmap(&node_point("User", "status", "active"))
            .await
            .is_err());
        assert!(expired
            .edge_bitmap(&edge_point("FOLLOWS", "status", "active"))
            .await
            .is_err());
        let mut dependency = Some(ExecutionValue::Stream(Vec::new()));
        assert!(expired
            .count_cursor(&exec::ExecCountCursorPlan::EmptyRows, &mut dependency)
            .await
            .is_err());
        let mut dependency = Some(ExecutionValue::Stream(Vec::new()));
        assert!(expired
            .count_cursor_cardinality(
                &exec::ExecCountCursorPlan::EmptyRows,
                &mut dependency,
                EvaluatedCountWindow {
                    skip: 0,
                    take: None
                },
            )
            .await
            .is_err());
    }

    #[cfg_attr(test, tokio::test)]
    async fn point_and_batch_bitmap_counts_issue_only_the_encoded_primitive() {
        let db = test_support::open_db_with_config(
            test_support::in_memory_config("count-exact-bitmap-primitives")
                .with_equality_index("User", "status"),
        )
        .await;
        test_support::add_node_with_properties(
            &db,
            "User",
            vec![("status", PropertyValue::from("active"))],
        )
        .await;
        test_support::add_node_with_properties(
            &db,
            "User",
            vec![("status", PropertyValue::from("inactive"))],
        )
        .await;
        crate::index_lifecycle::secondary::reset_equality_read_metrics();
        let point = exec::ExecCountPlan::NodeBitmap(exec::ExecNodeBitmapCountPlan {
            bitmap: node_point("User", "status", "active"),
            window: exec::ExecCountWindowPlan::identity(),
        });
        assert_eq!(
            execute_direct_count(&db, point).await.unwrap(),
            ExecutionValue::Count(1)
        );
        assert_eq!(
            crate::index_lifecycle::secondary::equality_read_metrics(),
            crate::index_lifecycle::secondary::SecondaryEqualityReadMetrics {
                point_reads: 2,
                multi_get_calls: 0,
                scans: 0,
                graph_reads: 0,
            }
        );

        crate::index_lifecycle::secondary::reset_equality_read_metrics();
        let batch = exec::ExecCountPlan::NodeBitmap(exec::ExecNodeBitmapCountPlan {
            bitmap: exec::ExecNodeBitmapExpr::BatchedUnionRead {
                index: node_equality_index("User", "status"),
                key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                values: ir::AtLeast::from_pair(indexed("active"), indexed("inactive")),
            },
            window: exec::ExecCountWindowPlan::identity(),
        });
        assert_eq!(
            execute_direct_count(&db, batch).await.unwrap(),
            ExecutionValue::Count(2)
        );
        assert_eq!(
            crate::index_lifecycle::secondary::equality_read_metrics(),
            crate::index_lifecycle::secondary::SecondaryEqualityReadMetrics {
                point_reads: 3,
                multi_get_calls: 1,
                scans: 0,
                graph_reads: 0,
            }
        );
    }

    #[cfg_attr(test, tokio::test)]
    async fn bitmap_child_order_is_observed_and_never_reordered() {
        let db = test_support::open_db("count-bitmap-child-order").await;
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        let invalid_driver = exec::ExecNodeBitmapExpr::PointRead {
            index: node_equality_index("Other", "status"),
            key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
            value: indexed("active"),
        };
        let plan = exec::ExecCountPlan::NodeBitmap(exec::ExecNodeBitmapCountPlan {
            bitmap: exec::ExecNodeBitmapExpr::Union {
                driver: Box::new(invalid_driver),
                rest: ir::AtLeast::from_one(node_point("User", "status", "active")),
            },
            window: exec::ExecCountWindowPlan::identity(),
        });

        let error = context
            .execute_count(ExecutionValue::Stream(Vec::new()), &plan)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("node_eq:Other:status` disagrees with `node_eq:User:status"),
            "{error}"
        );
    }

    #[cfg_attr(test, tokio::test)]
    async fn bitmap_validation_and_recursive_error_paths_cover_every_exact_variant() {
        let db = test_support::open_db_with_config(
            test_support::in_memory_config("count-bitmap-error-matrix")
                .with_equality_index("User", "status")
                .with_edge_equality_index("FOLLOWS", "status"),
        )
        .await;
        let node = test_support::add_node_with_properties(
            &db,
            "User",
            vec![("status", PropertyValue::from("active"))],
        )
        .await;
        test_support::add_edge_with_properties(
            &db,
            node,
            node,
            "FOLLOWS",
            vec![("status", PropertyValue::from("active"))],
        )
        .await;
        let mut execution = ExecutionContext::new(&db, context::ParamBindings::default());
        execution.enable_request_read_view().await.unwrap();
        let invalid_node_point = || exec::ExecNodeBitmapExpr::PointRead {
            index: node_equality_index("Other", "status"),
            key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
            value: indexed("active"),
        };
        let invalid_node_batch = exec::ExecNodeBitmapExpr::BatchedUnionRead {
            index: node_equality_index("Other", "status"),
            key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
            values: ir::AtLeast::from_pair(indexed("active"), indexed("paused")),
        };
        let valid_node = node_point("User", "status", "active");
        assert_eq!(execution.node_bitmap(&valid_node).await.unwrap().len(), 1);
        for expression in [
            invalid_node_point(),
            invalid_node_batch,
            exec::ExecNodeBitmapExpr::Union {
                driver: Box::new(invalid_node_point()),
                rest: ir::AtLeast::from_one(valid_node.clone()),
            },
            exec::ExecNodeBitmapExpr::Union {
                driver: Box::new(valid_node.clone()),
                rest: ir::AtLeast::from_one(invalid_node_point()),
            },
            exec::ExecNodeBitmapExpr::Intersect {
                driver: Box::new(invalid_node_point()),
                rest: ir::AtLeast::from_one(valid_node.clone()),
            },
            exec::ExecNodeBitmapExpr::Intersect {
                driver: Box::new(valid_node),
                rest: ir::AtLeast::from_one(invalid_node_point()),
            },
        ] {
            assert!(execution.node_bitmap(&expression).await.is_err());
        }

        let invalid_edge_point = || exec::ExecEdgeBitmapExpr::PointRead {
            index: edge_equality_index("OTHER", "status"),
            key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
            value: indexed("active"),
        };
        let invalid_edge_batch = exec::ExecEdgeBitmapExpr::BatchedUnionRead {
            index: edge_equality_index("OTHER", "status"),
            key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
            values: ir::AtLeast::from_pair(indexed("active"), indexed("paused")),
        };
        let valid_edge = edge_point("FOLLOWS", "status", "active");
        assert_eq!(execution.edge_bitmap(&valid_edge).await.unwrap().len(), 1);
        for expression in [
            invalid_edge_point(),
            invalid_edge_batch,
            exec::ExecEdgeBitmapExpr::Union {
                driver: Box::new(invalid_edge_point()),
                rest: ir::AtLeast::from_one(valid_edge.clone()),
            },
            exec::ExecEdgeBitmapExpr::Union {
                driver: Box::new(valid_edge.clone()),
                rest: ir::AtLeast::from_one(invalid_edge_point()),
            },
            exec::ExecEdgeBitmapExpr::Intersect {
                driver: Box::new(invalid_edge_point()),
                rest: ir::AtLeast::from_one(valid_edge.clone()),
            },
            exec::ExecEdgeBitmapExpr::Intersect {
                driver: Box::new(valid_edge),
                rest: ir::AtLeast::from_one(invalid_edge_point()),
            },
        ] {
            assert!(execution.edge_bitmap(&expression).await.is_err());
        }
    }

    #[cfg_attr(test, tokio::test)]
    async fn unique_count_performs_one_owner_read_and_one_authoritative_verification() {
        let db = test_support::open_db_with_config(
            test_support::in_memory_config("count-exact-unique-owner")
                .with_unique_equality_index("User", "email"),
        )
        .await;
        let owner = test_support::add_node_with_properties(
            &db,
            "User",
            vec![("email", PropertyValue::from("alice@example.com"))],
        )
        .await;
        let exact = exec::ExecNodeAccessPlan::exact_equality(
            catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:User:email"))
                .with_uniqueness(catalog::IndexUniqueness::Unique),
            catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
            ir::IndexValue::Literal(
                ir::SecondaryIndexLiteral::new(PropertyValue::from("alice@example.com")).unwrap(),
            ),
        );
        let exec::ExecNodeAccessPlan::Unique {
            lookup,
            verification,
        } = exact
        else {
            panic!("unique metadata must select the exact unique family")
        };
        crate::index_lifecycle::secondary::reset_equality_read_metrics();
        assert_eq!(
            execute_direct_count(
                &db,
                exec::ExecCountPlan::NodeUnique(exec::ExecNodeUniqueCountPlan {
                    lookup: lookup.clone(),
                    verification: verification.clone(),
                    window: exec::ExecCountWindowPlan::identity(),
                }),
            )
            .await
            .unwrap(),
            ExecutionValue::Count(1)
        );
        assert_eq!(
            crate::index_lifecycle::secondary::equality_read_metrics(),
            crate::index_lifecycle::secondary::SecondaryEqualityReadMetrics {
                point_reads: 2,
                multi_get_calls: 0,
                scans: 0,
                graph_reads: 1,
            }
        );
        let missing_value = indexed("missing@example.com");
        let mut missing_lookup = lookup.clone();
        missing_lookup.value = missing_value.clone();
        let mut missing_verification = verification.clone();
        missing_verification.value = missing_value;
        assert_eq!(
            execute_direct_count(
                &db,
                exec::ExecCountPlan::NodeUnique(exec::ExecNodeUniqueCountPlan {
                    lookup: missing_lookup,
                    verification: missing_verification,
                    window: exec::ExecCountWindowPlan::identity(),
                }),
            )
            .await
            .unwrap(),
            ExecutionValue::Count(0)
        );
        let mut invalid_lookup = lookup.clone();
        invalid_lookup.index = exec::ExecNodeUniqueEqualityIndex::try_from(
            catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:Other:email"))
                .with_uniqueness(catalog::IndexUniqueness::Unique),
        )
        .unwrap();
        let context = ExecutionContext::new(&db, context::ParamBindings::default());
        assert!(context
            .verified_node_unique_owner(&invalid_lookup, &verification)
            .await
            .is_err());

        db.inner_db()
            .put(
                keys::Key::Data {
                    scope: crate::encoding::v1::keys::tenant::DataScope::LegacyUnscoped,
                    kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(owner)),
                }
                .to_bytes(),
                crate::encoding::v1::property::encode_properties(&[
                    crate::encoding::v1::property::Property::string("$label", "Other"),
                    crate::encoding::v1::property::Property::string("email", "alice@example.com"),
                ]),
            )
            .await
            .unwrap();
        assert!(matches!(
            execute_direct_count(
                &db,
                exec::ExecCountPlan::NodeUnique(exec::ExecNodeUniqueCountPlan {
                    lookup: lookup.clone(),
                    verification: verification.clone(),
                    window: exec::ExecCountWindowPlan::identity(),
                }),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("authoritative node")
        ));
        db.inner_db()
            .put(
                keys::Key::Data {
                    scope: crate::encoding::v1::keys::tenant::DataScope::LegacyUnscoped,
                    kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(owner)),
                }
                .to_bytes(),
                bytes::Bytes::from_static(b"malformed unique authority"),
            )
            .await
            .unwrap();
        assert!(execute_direct_count(
            &db,
            exec::ExecCountPlan::NodeUnique(exec::ExecNodeUniqueCountPlan {
                lookup,
                verification,
                window: exec::ExecCountWindowPlan::identity(),
            }),
        )
        .await
        .is_err());
    }

    #[cfg_attr(test, tokio::test)]
    async fn authoritative_null_count_applies_its_normalized_window_after_matches() {
        let db = test_support::open_db("count-authoritative-null-window").await;
        for properties in [
            vec![("status", PropertyValue::Null)],
            Vec::new(),
            vec![("status", PropertyValue::Null)],
            vec![("status", PropertyValue::from("active"))],
        ] {
            test_support::add_node_with_properties(&db, "User", properties).await;
        }
        test_support::add_node_with_properties(&db, "Other", vec![("status", PropertyValue::Null)])
            .await;
        let window = exec::ExecCountWindowPlan::identity()
            .then_skip(exec::ExecUsizeExpr::literal(1))
            .then_limit(exec::ExecUsizeExpr::literal(1));

        assert_eq!(
            execute_direct_count(
                &db,
                exec::ExecCountPlan::NodeAuthoritativeScan(exec::ExecNodeScanCountPlan {
                    predicate: exec::ExecNodeAuthoritativeScanPredicate::NullEquality {
                        key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                    },
                    window,
                }),
            )
            .await
            .unwrap(),
            ExecutionValue::Count(1)
        );
    }

    #[cfg_attr(test, tokio::test)]
    async fn terminal_cursor_counts_avoid_row_output_and_stop_at_the_encoded_threshold() {
        let db = test_support::open_db_with_config(
            test_support::in_memory_config("count-terminal-cursor-threshold")
                .with_equality_index("User", "status"),
        )
        .await;
        let first = test_support::add_node_with_properties(
            &db,
            "User",
            vec![("status", PropertyValue::from("active"))],
        )
        .await;
        let second = test_support::add_node_with_properties(
            &db,
            "User",
            vec![("status", PropertyValue::from("paused"))],
        )
        .await;
        let mut execution = ExecutionContext::new(&db, context::ParamBindings::default());
        execution.enable_request_read_view().await.unwrap();

        let bitmap = exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
            cursor: exec::ExecCountCursorPlan::NodeBitmap(node_point("User", "status", "active")),
            window: bounded(0, 1),
        });
        assert_eq!(
            execution
                .execute_count(ExecutionValue::Stream(Vec::new()), &bitmap)
                .await
                .unwrap(),
            ExecutionValue::Count(1)
        );
        #[cfg(test)]
        assert_eq!(execution.projection_read_snapshot().property_gets, 0);

        let filter = exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
            cursor: exec::ExecCountCursorPlan::Filter {
                input: Box::new(exec::ExecCountCursorPlan::NodePointReads(
                    test_support::ids(vec![first, second]),
                )),
                predicate: ir::PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
            },
            window: bounded(0, 1),
        });
        assert_eq!(
            execution
                .execute_count(ExecutionValue::Stream(Vec::new()), &filter)
                .await
                .unwrap(),
            ExecutionValue::Count(1)
        );
        #[cfg(test)]
        {
            assert_eq!(execution.projection_read_snapshot().property_gets, 1);
            assert_eq!(execution.projection_read_snapshot().property_decodes, 1);
        }

        let unbounded_filter = exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
            cursor: exec::ExecCountCursorPlan::Filter {
                input: Box::new(exec::ExecCountCursorPlan::NodePointReads(
                    test_support::ids(vec![first, second]),
                )),
                predicate: ir::PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
            },
            window: exec::ExecCountWindowPlan::identity(),
        });
        assert_eq!(
            execution
                .execute_count(ExecutionValue::Stream(Vec::new()), &unbounded_filter)
                .await
                .unwrap(),
            ExecutionValue::Count(1)
        );
        #[cfg(test)]
        {
            assert_eq!(execution.projection_read_snapshot().property_gets, 3);
            assert_eq!(execution.projection_read_snapshot().property_decodes, 3);
        }

        let input = exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
            cursor: exec::ExecCountCursorPlan::InputRows,
            window: bounded(1, 1),
        });
        assert_eq!(
            execution
                .execute_count(
                    ExecutionValue::Stream(vec![
                        ExecutionRow::current(ElementRef::Node(first)),
                        ExecutionRow::current(ElementRef::Node(second)),
                    ]),
                    &input,
                )
                .await
                .unwrap(),
            ExecutionValue::Count(1)
        );
        assert!(execution
            .execute_count(ExecutionValue::Scalars(Vec::new()), &input)
            .await
            .is_err());

        let mut consumed = Some(ExecutionValue::Stream(Vec::new()));
        assert_eq!(
            execution
                .count_cursor_cardinality(
                    &exec::ExecCountCursorPlan::InputRows,
                    &mut consumed,
                    EvaluatedCountWindow {
                        skip: 0,
                        take: None
                    },
                )
                .await
                .unwrap(),
            0
        );
        assert!(execution
            .count_cursor_cardinality(
                &exec::ExecCountCursorPlan::InputRows,
                &mut consumed,
                EvaluatedCountWindow {
                    skip: 0,
                    take: None
                },
            )
            .await
            .is_err());

        let positioned_failure = exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
            cursor: exec::ExecCountCursorPlan::Window {
                input: Box::new(exec::ExecCountCursorPlan::NodeDynamicEquality {
                    index: catalog::NodeEqualityIndexMeta::new(test_support::name(
                        "node_eq:Other:status",
                    )),
                    key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                    param: test_support::name("missing"),
                }),
                window: exec::ExecCountWindowPlan::identity(),
            },
            window: exec::ExecCountWindowPlan::identity(),
        });
        assert!(execution
            .execute_count(ExecutionValue::Stream(Vec::new()), &positioned_failure,)
            .await
            .is_err());

        #[cfg(test)]
        {
            let mut deadline = ExecutionContext::new(&db, context::ParamBindings::default());
            deadline.fail_deadline_after(2);
            let mut dependency = Some(ExecutionValue::Stream(vec![ExecutionRow::current(
                ElementRef::Node(first),
            )]));
            assert!(matches!(
                deadline
                    .count_cursor_cardinality(
                        &exec::ExecCountCursorPlan::Filter {
                            input: Box::new(exec::ExecCountCursorPlan::InputRows),
                            predicate: ir::PredicatePlan::new(Predicate::eq("status", "active"))
                                .unwrap(),
                        },
                        &mut dependency,
                        EvaluatedCountWindow {
                            skip: 0,
                            take: None,
                        },
                    )
                    .await,
                Err(HelixDbError::QueryDeadlineExceeded)
            ));

            let structural = exec::ExecCountCursorPlan::Union {
                driver: Box::new(exec::ExecCountCursorPlan::EmptyRows),
                rest: ir::AtLeast::from_one(exec::ExecCountCursorPlan::EmptyRows),
            };
            let mut dependency = None;
            let mut deadline = ExecutionContext::new(&db, context::ParamBindings::default());
            deadline.fail_deadline_after(0);
            assert!(matches!(
                deadline.count_cursor(&structural, &mut dependency).await,
                Err(HelixDbError::QueryDeadlineExceeded)
            ));
        }
        execution.close_request_read_view().unwrap();
    }

    #[cfg(test)]
    #[tokio::test]
    async fn randomized_recursive_counts_match_the_materialized_row_oracle() {
        let db = test_support::open_db("count-randomized-recursive-oracle").await;
        let mut nodes = Vec::new();
        for index in 0..12 {
            nodes.push(
                test_support::add_node_with_properties(
                    &db,
                    "User",
                    vec![
                        (
                            "status",
                            PropertyValue::from(if index % 2 == 0 { "active" } else { "paused" }),
                        ),
                        ("rank", PropertyValue::from(format!("{index:02}"))),
                    ],
                )
                .await,
            );
        }
        for (index, node) in nodes.iter().copied().enumerate() {
            test_support::add_edge_with_properties(
                &db,
                node,
                nodes[(index + 1) % nodes.len()],
                "NEXT",
                Vec::new(),
            )
            .await;
        }

        let strategy = (
            prop::collection::vec(0usize..nodes.len(), 0..24),
            prop::collection::vec((0u8..8, 0usize..20, 0usize..20), 0..10),
            0usize..24,
            prop::option::of(0usize..24),
        );
        let mut runner = TestRunner::deterministic();
        let all_ids = test_support::ids(nodes.clone());
        let mut execution = ExecutionContext::new(&db, context::ParamBindings::default());
        execution.enable_request_read_view().await.unwrap();

        for case_index in 0..96 {
            let (row_indexes, ops, skip, take) = strategy
                .new_tree(&mut runner)
                .expect("recursive count oracle strategy is valid")
                .current();
            let rows = row_indexes
                .into_iter()
                .map(|index| ExecutionRow::current(ElementRef::Node(nodes[index])))
                .collect::<Vec<_>>();
            let mut cursor = exec::ExecCountCursorPlan::InputRows;
            for (kind, first, second) in ops {
                cursor = match kind {
                    0 => exec::ExecCountCursorPlan::Window {
                        input: Box::new(cursor),
                        window: bounded(first, second),
                    },
                    1 => exec::ExecCountCursorPlan::Filter {
                        input: Box::new(cursor),
                        predicate: ir::PredicatePlan::new(Predicate::eq("status", "active"))
                            .unwrap(),
                    },
                    2 => exec::ExecCountCursorPlan::Order {
                        input: Box::new(cursor),
                        plan: ir::OrderPlan::ExplicitSort(ir::OrderKeys::from(ir::OrderKey {
                            property: test_support::name("rank"),
                            order: if first % 2 == 0 {
                                helix_ast::traversal::Order::Asc
                            } else {
                                helix_ast::traversal::Order::Desc
                            },
                        })),
                    },
                    3 => exec::ExecCountCursorPlan::Distinct {
                        input: Box::new(cursor),
                        plan: exec::ExecCountDistinctPlan::HashRows,
                    },
                    4 => exec::ExecCountCursorPlan::Distinct {
                        input: Box::new(cursor),
                        plan: exec::ExecCountDistinctPlan::OrderedRows,
                    },
                    5 => exec::ExecCountCursorPlan::Union {
                        driver: Box::new(cursor),
                        rest: ir::AtLeast::from_one(exec::ExecCountCursorPlan::EmptyRows),
                    },
                    6 => exec::ExecCountCursorPlan::Intersect {
                        driver: Box::new(cursor),
                        rest: ir::AtLeast::from_one(exec::ExecCountCursorPlan::NodePointReads(
                            all_ids.clone(),
                        )),
                    },
                    7 => exec::ExecCountCursorPlan::Expand {
                        input: Box::new(cursor),
                        plan: ir::ExpandPlan {
                            direction: if second % 2 == 0 {
                                ir::ExpandDirection::Out
                            } else {
                                ir::ExpandDirection::In
                            },
                            output: ir::ExpandOutput::Nodes,
                            label: ir::ExpandLabelPlan::Any,
                        },
                    },
                    _ => unreachable!("generated cursor kind is bounded"),
                };
            }
            let window = EvaluatedCountWindow { skip, take };
            let mut oracle_dependency = Some(ExecutionValue::Stream(rows.clone()));
            let materialized = execution
                .count_cursor(&cursor, &mut oracle_dependency)
                .await
                .unwrap_or_else(|error| panic!("oracle case {case_index} failed: {error}"));
            let expected = window.apply(materialized.len());

            let mut scalar_dependency = Some(ExecutionValue::Stream(rows));
            let actual = execution
                .count_cursor_cardinality(&cursor, &mut scalar_dependency, window)
                .await
                .unwrap_or_else(|error| panic!("scalar case {case_index} failed: {error}"));
            assert_eq!(
                actual, expected,
                "random recursive cardinality case {case_index} diverged for {cursor:?}"
            );
        }
        execution.close_request_read_view().unwrap();
    }

    #[cfg(all(feature = "production-coverage", not(test)))]
    pub(super) async fn run_production_contracts() {
        test_support::run_production_contracts().await;
        evaluated_windows_and_index_identity_validation_cover_boundaries();
        direct_non_search_count_families_match_materialized_sources().await;
        dynamic_equality_runtime_classification_covers_every_named_case().await;
        every_direct_storage_count_propagates_its_own_read_failure().await;
        direct_and_cursor_authoritative_counts_propagate_each_predicate_failure().await;
        count_window_parameters_shapes_and_runtime_variables_fail_closed().await;
        recursive_count_cursor_matrix_preserves_identity_and_child_order().await;
        every_recursive_cursor_wrapper_propagates_child_failure_in_encoded_order().await;
        every_recursive_cursor_operation_propagates_its_primitive_failure().await;
        direct_and_restricted_search_count_families_use_selected_search_primitives().await;
        every_direct_and_cursor_search_propagates_its_selected_read_failure().await;
        input_shapes_and_parameterized_windows_are_literal_contracts().await;
        count_window_runtime_dynamic_range_and_deadline_errors_are_exhaustive().await;
        point_and_batch_bitmap_counts_issue_only_the_encoded_primitive().await;
        bitmap_child_order_is_observed_and_never_reordered().await;
        bitmap_validation_and_recursive_error_paths_cover_every_exact_variant().await;
        unique_count_performs_one_owner_read_and_one_authoritative_verification().await;
        authoritative_null_count_applies_its_normalized_window_after_matches().await;
        terminal_cursor_counts_avoid_row_output_and_stop_at_the_encoded_threshold().await;
    }
}
