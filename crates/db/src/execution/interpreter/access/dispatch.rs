//! Executable graph-access dispatch.
//!
//! This module owns the element-specific interpretation of planner
//! `ExecAccessPlan` values. Storage row materialization, runtime parameter
//! extraction, index lookups, range scans, and search reads stay in sibling
//! access modules so the planner-facing dispatch remains small.

use helix_planner::{exec, properties};

use super::super::{ExecutionContext, ExecutionValue};
use super::indexes::{limited_index_ids, scoped_property_key};
use super::search::SearchReadLimit;
use crate::config::{TextElementType, VectorElementType};
use crate::error::Result;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn execute_access(
        &mut self,
        plan: &exec::ExecAccessPlan,
    ) -> Result<ExecutionValue> {
        self.execute_limited_access(plan, None).await
    }

    async fn execute_limited_access(
        &mut self,
        plan: &exec::ExecAccessPlan,
        limit: Option<properties::PositiveUsize>,
    ) -> Result<ExecutionValue> {
        let mut plan = plan;
        let mut limit = limit;
        loop {
            match plan {
                exec::ExecAccessPlan::Limited(limited) => {
                    limit = Some(tightest_access_limit(limit, limited.limit()));
                    plan = limited.source();
                }
                exec::ExecAccessPlan::Node(plan) => {
                    return self.execute_node_access(plan, limit).await;
                }
                exec::ExecAccessPlan::Edge(plan) => {
                    return self.execute_edge_access(plan, limit).await;
                }
            }
        }
    }

    async fn execute_node_access(
        &mut self,
        plan: &exec::ExecNodeAccessPlan,
        limit: Option<properties::PositiveUsize>,
    ) -> Result<ExecutionValue> {
        let mut ids = match plan {
            exec::ExecNodeAccessPlan::Empty => Vec::new(),
            exec::ExecNodeAccessPlan::FromParam { param } => self.param_ids(param)?,
            exec::ExecNodeAccessPlan::FromVar { variable } => {
                self.access_variable_nodes(variable)?
            }
            exec::ExecNodeAccessPlan::AllScan => {
                self.scan_element_ids(exec::ElementKeyspace::NodeProperty, limit)
                    .await?
            }
            exec::ExecNodeAccessPlan::LabelScan { label } => limited_index_ids(
                self.lookup_equality_index_set("$label", label.as_ref())
                    .await?,
                limit,
            ),
            exec::ExecNodeAccessPlan::EqualityIndex { key, value, .. } => {
                let value = self.index_value(value)?;
                limited_index_ids(
                    self.lookup_equality_index_set(
                        &scoped_property_key(key),
                        &crate::search::property_value_to_index_string(&value),
                    )
                    .await?,
                    limit,
                )
            }
            exec::ExecNodeAccessPlan::RangeIndex { key, range, .. } => {
                self.node_range_index_ids(key, range, limit).await?
            }
            exec::ExecNodeAccessPlan::VectorSearch {
                key,
                index,
                query_vector,
                k,
            } => {
                let mut results = self
                    .vector_search_results(
                        VectorElementType::Node,
                        &key.label,
                        &key.property,
                        index,
                        query_vector,
                        SearchReadLimit::new(k, limit),
                    )
                    .await?;
                truncate_search_results(&mut results, limit);
                return self.node_search_rows(results).await;
            }
            exec::ExecNodeAccessPlan::TextSearch {
                key,
                index,
                query_text,
                k,
            } => {
                self.text_search_ids(
                    TextElementType::Node,
                    &key.label,
                    &key.property,
                    index,
                    query_text,
                    SearchReadLimit::new(k, limit),
                )
                .await?
            }
        };
        truncate_ids(&mut ids, limit);
        self.node_rows(ids).await
    }

    async fn execute_edge_access(
        &mut self,
        plan: &exec::ExecEdgeAccessPlan,
        limit: Option<properties::PositiveUsize>,
    ) -> Result<ExecutionValue> {
        let mut ids = match plan {
            exec::ExecEdgeAccessPlan::Empty => Vec::new(),
            exec::ExecEdgeAccessPlan::FromParam { param } => self.param_ids(param)?,
            exec::ExecEdgeAccessPlan::FromVar { variable } => {
                self.access_variable_edges(variable)?
            }
            exec::ExecEdgeAccessPlan::AllScan => {
                self.scan_element_ids(exec::ElementKeyspace::EdgeEndpoints, limit)
                    .await?
            }
            exec::ExecEdgeAccessPlan::LabelScan { label } => limited_index_ids(
                self.lookup_global_edge_label_index(label.as_ref()).await?,
                limit,
            ),
            exec::ExecEdgeAccessPlan::EqualityIndex { key, value, .. } => {
                let value = self.index_value(value)?;
                limited_index_ids(
                    self.lookup_global_edge_equality_index(
                        &scoped_property_key(key),
                        &crate::search::property_value_to_index_string(&value),
                    )
                    .await?,
                    limit,
                )
            }
            exec::ExecEdgeAccessPlan::RangeIndex { key, range, .. } => {
                self.edge_range_index_ids(key, range, limit).await?
            }
            exec::ExecEdgeAccessPlan::VectorSearch {
                key,
                index,
                query_vector,
                k,
            } => {
                let mut results = self
                    .vector_search_results(
                        VectorElementType::Edge,
                        &key.label,
                        &key.property,
                        index,
                        query_vector,
                        SearchReadLimit::new(k, limit),
                    )
                    .await?;
                truncate_search_results(&mut results, limit);
                return self.edge_search_rows(results).await;
            }
            exec::ExecEdgeAccessPlan::TextSearch {
                key,
                index,
                query_text,
                k,
            } => {
                self.text_search_ids(
                    TextElementType::Edge,
                    &key.label,
                    &key.property,
                    index,
                    query_text,
                    SearchReadLimit::new(k, limit),
                )
                .await?
            }
        };
        truncate_ids(&mut ids, limit);
        self.edge_rows(ids).await
    }
}

fn truncate_ids(ids: &mut Vec<u64>, limit: Option<properties::PositiveUsize>) {
    if let Some(limit) = limit {
        ids.truncate(limit.get());
    }
}

fn truncate_search_results(
    results: &mut Vec<crate::search::vector::TypedVectorSearchResult>,
    limit: Option<properties::PositiveUsize>,
) {
    if let Some(limit) = limit {
        results.truncate(limit.get());
    }
}

fn tightest_access_limit(
    current: Option<properties::PositiveUsize>,
    next: properties::PositiveUsize,
) -> properties::PositiveUsize {
    current.filter(|current| current <= &next).unwrap_or(next)
}

#[cfg(test)]
mod tests {
    use helix_ast::value::PropertyValue;
    use helix_planner::{catalog, context, ir};

    use super::super::super::{test_support, ElementRef, ExecutionRow};
    use super::*;

    fn positive(value: usize) -> properties::PositiveUsize {
        properties::PositiveUsize::new(value).expect("positive test limit")
    }

    #[test]
    fn truncate_ids_applies_optional_positive_limit() {
        let mut ids = vec![1, 2, 3, 4];
        truncate_ids(&mut ids, Some(positive(2)));
        assert_eq!(ids, vec![1, 2]);

        truncate_ids(&mut ids, None);
        assert_eq!(ids, vec![1, 2]);

        truncate_ids(&mut ids, Some(positive(10)));
        assert_eq!(ids, vec![1, 2]);

        let mut results = vec![
            crate::search::vector::TypedVectorSearchResult::from_physical(
                crate::encoding::v1::values::vector_generation::VectorEntityKind::Node,
                crate::encoding::v1::values::vector_generation::ActiveScoreSemantic::ManhattanF32V1,
                crate::search::vector::SearchResult::new(
                    1,
                    crate::search::vector::DistanceScore::try_new(0.1).unwrap(),
                ),
            ),
            crate::search::vector::TypedVectorSearchResult::from_physical(
                crate::encoding::v1::values::vector_generation::VectorEntityKind::Node,
                crate::encoding::v1::values::vector_generation::ActiveScoreSemantic::ManhattanF32V1,
                crate::search::vector::SearchResult::new(
                    2,
                    crate::search::vector::DistanceScore::try_new(0.2).unwrap(),
                ),
            ),
        ];
        truncate_search_results(&mut results, Some(positive(1)));
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].entity_id(),
            crate::search::vector::VectorEntityId::Node(1)
        );
    }

    #[test]
    fn tightest_access_limit_keeps_the_smallest_nested_limit() {
        assert_eq!(tightest_access_limit(None, positive(5)).get(), 5);
        assert_eq!(
            tightest_access_limit(Some(positive(3)), positive(5)).get(),
            3
        );
        assert_eq!(
            tightest_access_limit(Some(positive(8)), positive(5)).get(),
            5
        );
        assert_eq!(
            tightest_access_limit(Some(positive(5)), positive(5)).get(),
            5
        );
    }

    #[tokio::test]
    async fn access_dispatch_covers_node_equality_and_edge_source_variants() {
        let config = test_support::in_memory_config("access-dispatch-variants")
            .with_equality_index("User", "email");
        let db = test_support::open_db_with_config(config).await;
        let user = test_support::add_node_with_properties(
            &db,
            "User",
            vec![("email", PropertyValue::from("alice@example.com"))],
        )
        .await;
        let other = test_support::add_node_with_properties(&db, "User", Vec::new()).await;
        let edge = test_support::add_edge(&db, user, other, "KNOWS").await;
        let mut execution = ExecutionContext::new(&db, context::ParamBindings::default());
        let edge_variable = test_support::name("edge");
        execution.variables.insert(
            edge_variable.clone(),
            ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Edge(edge))]),
        );

        let node_equality = exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::EqualityIndex {
            index: catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq")),
            key: catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
            value: ir::IndexValue::Literal(
                ir::SecondaryIndexLiteral::new(PropertyValue::from("alice@example.com")).unwrap(),
            ),
        });
        assert_eq!(
            execution.execute_access(&node_equality).await.unwrap(),
            ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Node(user))])
        );

        for (plan, expected) in [
            (exec::ExecEdgeAccessPlan::Empty, Vec::new()),
            (
                exec::ExecEdgeAccessPlan::FromVar {
                    variable: edge_variable,
                },
                vec![ExecutionRow::current(ElementRef::Edge(edge))],
            ),
            (
                exec::ExecEdgeAccessPlan::AllScan,
                vec![ExecutionRow::current(ElementRef::Edge(edge))],
            ),
            (
                exec::ExecEdgeAccessPlan::LabelScan {
                    label: test_support::name("KNOWS"),
                },
                vec![ExecutionRow::current(ElementRef::Edge(edge))],
            ),
        ] {
            assert_eq!(
                execution
                    .execute_access(&exec::ExecAccessPlan::Edge(plan))
                    .await
                    .unwrap(),
                ExecutionValue::Stream(expected)
            );
        }
    }
}
