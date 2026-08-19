//! Executable graph-access dispatch.
//!
//! This module owns the element-specific interpretation of planner
//! `ExecAccessPlan` values. Storage row materialization, runtime parameter
//! extraction, index lookups, range scans, and search reads stay in sibling
//! access modules so the planner-facing dispatch remains small.

use helix_planner::{exec, ir, properties};

use super::super::{ExecutionContext, ExecutionValue};
use super::indexes::limited_index_ids;
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
            self.check_execution_deadline()?;
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
                let read = self.scan_element_ids(exec::ElementKeyspace::NodeProperty, limit);
                read.await?
            }
            exec::ExecNodeAccessPlan::LabelScan { label } => limited_index_ids(
                self.lookup_equality_index_set(
                    "$label",
                    &crate::encoding::property::property_value::PropertyValue::String(
                        label.as_ref().to_string(),
                    ),
                )
                .await?,
                limit,
            ),
            exec::ExecNodeAccessPlan::Bitmap { bitmap } => {
                let ids = limited_index_ids(self.node_bitmap(bitmap).await?, limit);
                return self.verified_node_rows(ids);
            }
            exec::ExecNodeAccessPlan::Unique {
                lookup,
                verification,
            } => {
                let ids = self
                    .verified_node_unique_owner(lookup, verification)
                    .await?
                    .into_iter()
                    .collect();
                return self.verified_node_rows(ids);
            }
            exec::ExecNodeAccessPlan::AuthoritativeScan { predicate } => {
                let read = self.scan_element_ids(exec::ElementKeyspace::NodeProperty, None);
                let ids = read.await?;
                let mut rows = Vec::new();
                for id in ids {
                    let row =
                        super::super::ExecutionRow::current(super::super::ElementRef::Node(id));
                    let matches = match predicate {
                        exec::ExecNodeAuthoritativeScanPredicate::NullEquality { key } => {
                            self.scoped_null_matches(&row, key).await?
                        }
                        exec::ExecNodeAuthoritativeScanPredicate::Predicate(predicate) => {
                            self.eval_predicate(&row, predicate.predicate()).await?
                        }
                    };
                    if matches {
                        rows.push(row);
                        if limit.is_some_and(|limit| rows.len() >= limit.get()) {
                            break;
                        }
                    }
                }
                return Ok(ExecutionValue::Stream(rows));
            }
            exec::ExecNodeAccessPlan::DynamicEquality { index, key, param } => {
                super::super::count::validate_node_equality_index(&index.index_id, key)?;
                let value = self.index_value(&ir::IndexValue::Param(param.clone()))?;
                let read = self.lookup_managed_equality_union(
                    crate::index_lifecycle::IndexElementKind::Node,
                    key,
                    core::slice::from_ref(&value),
                );
                let ids = limited_index_ids(read.await?, limit);
                return self.verified_node_rows(ids);
            }
            exec::ExecNodeAccessPlan::RangeIndex { key, range, .. } => {
                let ids = self.node_range_index_ids(key, range, limit).await?;
                return self.verified_node_rows(ids);
            }
            exec::ExecNodeAccessPlan::SecondarySet { set } => {
                let ids = self.node_secondary_set_ids(set, limit).await?;
                return self.verified_node_rows(ids);
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
                let results = self
                    .text_search_hits(
                        TextElementType::Node,
                        &key.label,
                        &key.property,
                        index,
                        query_text,
                        SearchReadLimit::new(k, limit),
                    )
                    .await?;
                return self.node_text_search_rows(results).await;
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
                let read = self.scan_element_ids(exec::ElementKeyspace::EdgeEndpoints, limit);
                read.await?
            }
            exec::ExecEdgeAccessPlan::LabelScan { label } => limited_index_ids(
                self.lookup_global_edge_label_index(label.as_ref()).await?,
                limit,
            ),
            exec::ExecEdgeAccessPlan::Bitmap { bitmap } => {
                let ids = limited_index_ids(self.edge_bitmap(bitmap).await?, limit);
                return self.verified_edge_rows(ids);
            }
            exec::ExecEdgeAccessPlan::AuthoritativeScan { predicate } => {
                let read = self.scan_element_ids(exec::ElementKeyspace::EdgeEndpoints, None);
                let ids = read.await?;
                let mut rows = Vec::new();
                for id in ids {
                    let row =
                        super::super::ExecutionRow::current(super::super::ElementRef::Edge(id));
                    let matches = match predicate {
                        exec::ExecEdgeAuthoritativeScanPredicate::NullEquality { key } => {
                            self.scoped_null_matches(&row, key).await?
                        }
                        exec::ExecEdgeAuthoritativeScanPredicate::Predicate(predicate) => {
                            self.eval_predicate(&row, predicate.predicate()).await?
                        }
                    };
                    if matches {
                        rows.push(row);
                        if limit.is_some_and(|limit| rows.len() >= limit.get()) {
                            break;
                        }
                    }
                }
                return Ok(ExecutionValue::Stream(rows));
            }
            exec::ExecEdgeAccessPlan::DynamicEquality { index, key, param } => {
                super::super::count::validate_edge_equality_index(&index.index_id, key)?;
                let value = self.index_value(&ir::IndexValue::Param(param.clone()))?;
                let read = self.lookup_managed_equality_union(
                    crate::index_lifecycle::IndexElementKind::Edge,
                    key,
                    core::slice::from_ref(&value),
                );
                let ids = limited_index_ids(read.await?, limit);
                return self.verified_edge_rows(ids);
            }
            exec::ExecEdgeAccessPlan::RangeIndex { key, range, .. } => {
                let ids = self.edge_range_index_ids(key, range, limit).await?;
                return self.verified_edge_rows(ids);
            }
            exec::ExecEdgeAccessPlan::SecondarySet { set } => {
                let ids = self.edge_secondary_set_ids(set, limit).await?;
                return self.verified_edge_rows(ids);
            }
            exec::ExecEdgeAccessPlan::VectorSearch {
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
                    SearchReadLimit::new(k, limit),
                );
                let mut results = read.await?;
                truncate_search_results(&mut results, limit);
                return self.edge_search_rows(results).await;
            }
            exec::ExecEdgeAccessPlan::TextSearch {
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
                    SearchReadLimit::new(k, limit),
                );
                let results = read.await?;
                return self.edge_text_search_rows(results).await;
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

#[cfg(any(test, feature = "production-coverage"))]
#[cfg_attr(all(feature = "production-coverage", not(test)), allow(dead_code))]
pub(super) mod tests {
    use helix_ast::expr::Predicate;
    use helix_ast::value::PropertyValue;
    use helix_planner::{catalog, context, ir};

    use super::super::super::{test_support, ElementRef, ExecutionRow};
    use super::*;

    fn positive(value: usize) -> properties::PositiveUsize {
        properties::PositiveUsize::new(value).expect("positive test limit")
    }

    #[cfg_attr(test, test)]
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

    #[cfg_attr(test, test)]
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

    #[cfg_attr(test, tokio::test)]
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
        execution.enable_request_read_view().await.unwrap();
        let edge_variable = test_support::name("edge");
        execution.variables.insert(
            edge_variable.clone(),
            ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Edge(edge))]),
        );

        let node_equality = exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::exact_equality(
            catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:User:email")),
            catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
            ir::IndexValue::Literal(
                ir::SecondaryIndexLiteral::new(PropertyValue::from("alice@example.com")).unwrap(),
            ),
        ));
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
        execution.close_request_read_view().unwrap();
    }

    #[cfg_attr(test, tokio::test)]
    async fn exact_access_dispatch_covers_bitmap_unique_scan_and_dynamic_families() {
        let db = test_support::open_db_with_config(
            test_support::in_memory_config("access-exact-family-matrix")
                .with_equality_index("User", "status")
                .with_unique_equality_index("User", "email")
                .with_edge_equality_index("FOLLOWS", "status")
                .with_range_index("User", "rank")
                .with_edge_range_index("FOLLOWS", "rank"),
        )
        .await;
        let active = test_support::add_node_with_properties(
            &db,
            "User",
            vec![
                ("status", PropertyValue::from("active")),
                ("email", PropertyValue::from("a@example.com")),
                ("rank", PropertyValue::from("a")),
            ],
        )
        .await;
        let absent = test_support::add_node_with_properties(
            &db,
            "User",
            vec![("rank", PropertyValue::from("b"))],
        )
        .await;
        test_support::add_node_with_properties(&db, "Other", vec![("status", PropertyValue::Null)])
            .await;
        let active_edge = test_support::add_edge_with_properties(
            &db,
            active,
            absent,
            "FOLLOWS",
            vec![
                ("status", PropertyValue::from("active")),
                ("rank", PropertyValue::from("a")),
            ],
        )
        .await;
        let null_edge = test_support::add_edge_with_properties(
            &db,
            absent,
            active,
            "FOLLOWS",
            vec![("rank", PropertyValue::from("b"))],
        )
        .await;
        let node_param = test_support::name("node_status");
        let edge_param = test_support::name("edge_status");
        let mut execution = ExecutionContext::new(
            &db,
            context::ParamBindings::default()
                .with_value(node_param.clone(), PropertyValue::from("active"))
                .with_value(edge_param.clone(), PropertyValue::from("active")),
        );
        execution.enable_request_read_view().await.unwrap();

        let indexed = |value: &str| {
            exec::ExecIndexedEqualityValue::try_from(
                ir::SecondaryIndexLiteral::new(PropertyValue::from(value)).unwrap(),
            )
            .unwrap()
        };
        let node_bitmap = exec::ExecNodeBitmapExpr::PointRead {
            index: exec::ExecNodeNonUniqueEqualityIndex::try_from(
                catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:User:status")),
            )
            .unwrap(),
            key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
            value: indexed("active"),
        };
        let edge_bitmap = exec::ExecEdgeBitmapExpr::PointRead {
            index: exec::ExecEdgeNonUniqueEqualityIndex::new(catalog::EdgeEqualityIndexMeta::new(
                test_support::name("edge_eq:FOLLOWS:status"),
            )),
            key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
            value: indexed("active"),
        };
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
        } = exact_unique.clone()
        else {
            panic!("unique fixture selects exact unique access")
        };

        let plans = vec![
            (
                exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::Bitmap {
                    bitmap: node_bitmap,
                }),
                vec![ExecutionRow::current(ElementRef::Node(active))],
            ),
            (
                exec::ExecAccessPlan::Node(exact_unique),
                vec![ExecutionRow::current(ElementRef::Node(active))],
            ),
            (
                exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::DynamicEquality {
                    index: catalog::NodeEqualityIndexMeta::new(test_support::name(
                        "node_eq:User:status",
                    )),
                    key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                    param: node_param.clone(),
                }),
                vec![ExecutionRow::current(ElementRef::Node(active))],
            ),
            (
                exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::Bitmap {
                    bitmap: edge_bitmap,
                }),
                vec![ExecutionRow::current(ElementRef::Edge(active_edge))],
            ),
            (
                exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::DynamicEquality {
                    index: catalog::EdgeEqualityIndexMeta::new(test_support::name(
                        "edge_eq:FOLLOWS:status",
                    )),
                    key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
                    param: edge_param.clone(),
                }),
                vec![ExecutionRow::current(ElementRef::Edge(active_edge))],
            ),
        ];
        for (plan, expected) in plans {
            assert_eq!(
                execution.execute_access(&plan).await.unwrap(),
                ExecutionValue::Stream(expected)
            );
        }

        let node_range = exec::ExecNodeSecondaryRangePlan {
            index: catalog::NodeRangeIndexMeta::new(test_support::name("node_range:User:rank:Asc")),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "rank",
                helix_ast::index::RangeIndexDirection::Asc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        };
        let edge_range = exec::ExecEdgeSecondaryRangePlan {
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:rank:Asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "rank",
                helix_ast::index::RangeIndexDirection::Asc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        };
        for (plan, expected) in [
            (
                exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::SecondarySet {
                    set: exec::ExecNodeSecondarySetPlan::DynamicEquality {
                        index: catalog::NodeEqualityIndexMeta::new(test_support::name(
                            "node_eq:User:status",
                        )),
                        key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                        param: node_param,
                    },
                }),
                vec![ExecutionRow::current(ElementRef::Node(active))],
            ),
            (
                exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::SecondarySet {
                    set: exec::ExecNodeSecondarySetPlan::Unique {
                        lookup,
                        verification,
                    },
                }),
                vec![ExecutionRow::current(ElementRef::Node(active))],
            ),
            (
                exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::SecondarySet {
                    set: exec::ExecNodeSecondarySetPlan::AuthoritativeScan(
                        exec::ExecNodeAuthoritativeScanPredicate::Predicate(
                            ir::PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
                        ),
                    ),
                }),
                vec![ExecutionRow::current(ElementRef::Node(active))],
            ),
            (
                exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::SecondarySet {
                    set: exec::ExecNodeSecondarySetPlan::AuthoritativeScan(
                        exec::ExecNodeAuthoritativeScanPredicate::NullEquality {
                            key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                        },
                    ),
                }),
                vec![ExecutionRow::current(ElementRef::Node(absent))],
            ),
            (
                exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::SecondarySet {
                    set: exec::ExecNodeSecondarySetPlan::OrderedIntersect {
                        driver: node_range,
                        filters: ir::AtLeast::from_one(exec::ExecNodeSecondarySetPlan::Bitmap(
                            exec::ExecNodeBitmapExpr::PointRead {
                                index: exec::ExecNodeNonUniqueEqualityIndex::try_from(
                                    catalog::NodeEqualityIndexMeta::new(test_support::name(
                                        "node_eq:User:status",
                                    )),
                                )
                                .unwrap(),
                                key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                                value: indexed("active"),
                            },
                        )),
                    },
                }),
                vec![ExecutionRow::current(ElementRef::Node(active))],
            ),
            (
                exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::SecondarySet {
                    set: exec::ExecEdgeSecondarySetPlan::DynamicEquality {
                        index: catalog::EdgeEqualityIndexMeta::new(test_support::name(
                            "edge_eq:FOLLOWS:status",
                        )),
                        key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
                        param: edge_param,
                    },
                }),
                vec![ExecutionRow::current(ElementRef::Edge(active_edge))],
            ),
            (
                exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::SecondarySet {
                    set: exec::ExecEdgeSecondarySetPlan::AuthoritativeScan(
                        exec::ExecEdgeAuthoritativeScanPredicate::Predicate(
                            ir::PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
                        ),
                    ),
                }),
                vec![ExecutionRow::current(ElementRef::Edge(active_edge))],
            ),
            (
                exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::SecondarySet {
                    set: exec::ExecEdgeSecondarySetPlan::AuthoritativeScan(
                        exec::ExecEdgeAuthoritativeScanPredicate::NullEquality {
                            key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
                        },
                    ),
                }),
                vec![ExecutionRow::current(ElementRef::Edge(null_edge))],
            ),
            (
                exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::SecondarySet {
                    set: exec::ExecEdgeSecondarySetPlan::OrderedIntersect {
                        driver: edge_range,
                        filters: ir::AtLeast::from_one(exec::ExecEdgeSecondarySetPlan::Bitmap(
                            exec::ExecEdgeBitmapExpr::PointRead {
                                index: exec::ExecEdgeNonUniqueEqualityIndex::new(
                                    catalog::EdgeEqualityIndexMeta::new(test_support::name(
                                        "edge_eq:FOLLOWS:status",
                                    )),
                                ),
                                key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status")
                                    .unwrap(),
                                value: indexed("active"),
                            },
                        )),
                    },
                }),
                vec![ExecutionRow::current(ElementRef::Edge(active_edge))],
            ),
        ] {
            assert_eq!(
                execution.execute_access(&plan).await.unwrap(),
                ExecutionValue::Stream(expected)
            );
        }

        for (plan, expected) in [
            (
                exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::AuthoritativeScan {
                    predicate: exec::ExecNodeAuthoritativeScanPredicate::NullEquality {
                        key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                    },
                })
                .limited(positive(1)),
                ExecutionRow::current(ElementRef::Node(absent)),
            ),
            (
                exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::AuthoritativeScan {
                    predicate: exec::ExecNodeAuthoritativeScanPredicate::Predicate(
                        ir::PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
                    ),
                }),
                ExecutionRow::current(ElementRef::Node(active)),
            ),
            (
                exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::AuthoritativeScan {
                    predicate: exec::ExecEdgeAuthoritativeScanPredicate::NullEquality {
                        key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
                    },
                })
                .limited(positive(1)),
                ExecutionRow::current(ElementRef::Edge(null_edge)),
            ),
            (
                exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::AuthoritativeScan {
                    predicate: exec::ExecEdgeAuthoritativeScanPredicate::Predicate(
                        ir::PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
                    ),
                }),
                ExecutionRow::current(ElementRef::Edge(active_edge)),
            ),
        ] {
            assert_eq!(
                execution.execute_access(&plan).await.unwrap(),
                ExecutionValue::Stream(vec![expected])
            );
        }
        execution.close_request_read_view().unwrap();
    }

    #[cfg_attr(test, tokio::test)]
    async fn exact_access_dispatch_rejects_each_invalid_dynamic_and_set_driver_contract() {
        let db = test_support::open_db("access-dispatch-contract-errors").await;
        let mut execution = ExecutionContext::new(&db, context::ParamBindings::default());
        let missing = test_support::name("missing");
        let node_key = catalog::ScopedPropertyKey::try_new("User", "status").unwrap();
        let edge_key = catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap();
        let invalid_node = exec::ExecNodeSecondarySetPlan::DynamicEquality {
            index: catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:Other:status")),
            key: node_key.clone(),
            param: missing.clone(),
        };
        let missing_node = exec::ExecNodeSecondarySetPlan::DynamicEquality {
            index: catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:User:status")),
            key: node_key.clone(),
            param: missing.clone(),
        };
        let invalid_edge = exec::ExecEdgeSecondarySetPlan::DynamicEquality {
            index: catalog::EdgeEqualityIndexMeta::new(test_support::name("edge_eq:OTHER:status")),
            key: edge_key.clone(),
            param: missing.clone(),
        };
        let missing_edge = exec::ExecEdgeSecondarySetPlan::DynamicEquality {
            index: catalog::EdgeEqualityIndexMeta::new(test_support::name(
                "edge_eq:FOLLOWS:status",
            )),
            key: edge_key.clone(),
            param: missing.clone(),
        };

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
            mut verification,
        } = exact_unique
        else {
            panic!("unique fixture classifies exactly")
        };
        verification.key = node_key.clone();

        let node_plans = vec![
            exec::ExecNodeAccessPlan::DynamicEquality {
                index: catalog::NodeEqualityIndexMeta::new(test_support::name(
                    "node_eq:Other:status",
                )),
                key: node_key.clone(),
                param: missing.clone(),
            },
            exec::ExecNodeAccessPlan::DynamicEquality {
                index: catalog::NodeEqualityIndexMeta::new(test_support::name(
                    "node_eq:User:status",
                )),
                key: node_key.clone(),
                param: missing.clone(),
            },
            exec::ExecNodeAccessPlan::SecondarySet {
                set: exec::ExecNodeSecondarySetPlan::Unique {
                    lookup,
                    verification,
                },
            },
            exec::ExecNodeAccessPlan::SecondarySet {
                set: exec::ExecNodeSecondarySetPlan::Intersect {
                    driver: Box::new(invalid_node.clone()),
                    rest: ir::AtLeast::from_one(exec::ExecNodeSecondarySetPlan::Empty),
                },
            },
            exec::ExecNodeAccessPlan::SecondarySet {
                set: exec::ExecNodeSecondarySetPlan::Union {
                    driver: Box::new(invalid_node.clone()),
                    rest: ir::AtLeast::from_one(exec::ExecNodeSecondarySetPlan::Empty),
                },
            },
            exec::ExecNodeAccessPlan::SecondarySet {
                set: exec::ExecNodeSecondarySetPlan::OrderedIntersect {
                    driver: exec::ExecNodeSecondaryRangePlan {
                        index: catalog::NodeRangeIndexMeta::new(test_support::name(
                            "node_range:User:rank:Asc",
                        )),
                        key: catalog::ScopedPropertyDirectionKey::try_new(
                            "User",
                            "rank",
                            helix_ast::index::RangeIndexDirection::Asc,
                        )
                        .unwrap(),
                        range: ir::IndexRange::All,
                    },
                    filters: ir::AtLeast::from_one(invalid_node),
                },
            },
            exec::ExecNodeAccessPlan::SecondarySet { set: missing_node },
        ];
        for plan in node_plans {
            assert!(execution
                .execute_access(&exec::ExecAccessPlan::Node(plan))
                .await
                .is_err());
        }

        let edge_plans = vec![
            exec::ExecEdgeAccessPlan::DynamicEquality {
                index: catalog::EdgeEqualityIndexMeta::new(test_support::name(
                    "edge_eq:OTHER:status",
                )),
                key: edge_key.clone(),
                param: missing.clone(),
            },
            exec::ExecEdgeAccessPlan::DynamicEquality {
                index: catalog::EdgeEqualityIndexMeta::new(test_support::name(
                    "edge_eq:FOLLOWS:status",
                )),
                key: edge_key,
                param: missing,
            },
            exec::ExecEdgeAccessPlan::SecondarySet {
                set: exec::ExecEdgeSecondarySetPlan::Intersect {
                    driver: Box::new(invalid_edge.clone()),
                    rest: ir::AtLeast::from_one(exec::ExecEdgeSecondarySetPlan::Empty),
                },
            },
            exec::ExecEdgeAccessPlan::SecondarySet {
                set: exec::ExecEdgeSecondarySetPlan::Union {
                    driver: Box::new(invalid_edge.clone()),
                    rest: ir::AtLeast::from_one(exec::ExecEdgeSecondarySetPlan::Empty),
                },
            },
            exec::ExecEdgeAccessPlan::SecondarySet {
                set: exec::ExecEdgeSecondarySetPlan::OrderedIntersect {
                    driver: exec::ExecEdgeSecondaryRangePlan {
                        index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                            "edge_range:FOLLOWS:rank:Asc",
                        )),
                        key: catalog::ScopedPropertyDirectionKey::try_new(
                            "FOLLOWS",
                            "rank",
                            helix_ast::index::RangeIndexDirection::Asc,
                        )
                        .unwrap(),
                        range: ir::IndexRange::All,
                    },
                    filters: ir::AtLeast::from_one(invalid_edge),
                },
            },
            exec::ExecEdgeAccessPlan::SecondarySet { set: missing_edge },
        ];
        for plan in edge_plans {
            assert!(execution
                .execute_access(&exec::ExecAccessPlan::Edge(plan))
                .await
                .is_err());
        }
    }

    #[cfg_attr(test, tokio::test)]
    async fn exact_access_dispatch_propagates_scan_predicate_index_and_search_failures() {
        let corrupt_db = test_support::open_db("access-dispatch-corrupt-authority").await;
        let node = test_support::add_node_with_properties(
            &corrupt_db,
            "User",
            vec![("status", PropertyValue::from("active"))],
        )
        .await;
        let edge = test_support::add_edge_with_properties(
            &corrupt_db,
            node,
            node,
            "FOLLOWS",
            vec![("status", PropertyValue::from("active"))],
        )
        .await;
        corrupt_db
            .inner_db()
            .put(
                crate::encoding::v1::keys::Key::Data {
                    scope: crate::encoding::v1::keys::tenant::DataScope::LegacyUnscoped,
                    kind: crate::encoding::v1::keys::DataKeyKind::NodeProperty(
                        crate::encoding::v1::keys::NodePropertyKey::new(node),
                    ),
                }
                .to_bytes(),
                bytes::Bytes::from_static(b"malformed node authority"),
            )
            .await
            .unwrap();
        corrupt_db
            .inner_db()
            .put(
                crate::encoding::v1::keys::Key::Data {
                    scope: crate::encoding::v1::keys::tenant::DataScope::LegacyUnscoped,
                    kind: crate::encoding::v1::keys::DataKeyKind::EdgePropertyById(
                        crate::encoding::v1::keys::EdgePropertyByIdKey::new(edge),
                    ),
                }
                .to_bytes(),
                bytes::Bytes::from_static(b"malformed edge authority"),
            )
            .await
            .unwrap();
        let mut corrupt = ExecutionContext::new(&corrupt_db, context::ParamBindings::default());
        for predicate in [
            exec::ExecNodeAuthoritativeScanPredicate::NullEquality {
                key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
            },
            exec::ExecNodeAuthoritativeScanPredicate::Predicate(
                ir::PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
            ),
        ] {
            for plan in [
                exec::ExecNodeAccessPlan::AuthoritativeScan {
                    predicate: predicate.clone(),
                },
                exec::ExecNodeAccessPlan::SecondarySet {
                    set: exec::ExecNodeSecondarySetPlan::AuthoritativeScan(predicate),
                },
            ] {
                assert!(corrupt
                    .execute_access(&exec::ExecAccessPlan::Node(plan))
                    .await
                    .is_err());
            }
        }
        for predicate in [
            exec::ExecEdgeAuthoritativeScanPredicate::NullEquality {
                key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
            },
            exec::ExecEdgeAuthoritativeScanPredicate::Predicate(
                ir::PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
            ),
        ] {
            for plan in [
                exec::ExecEdgeAccessPlan::AuthoritativeScan {
                    predicate: predicate.clone(),
                },
                exec::ExecEdgeAccessPlan::SecondarySet {
                    set: exec::ExecEdgeSecondarySetPlan::AuthoritativeScan(predicate),
                },
            ] {
                assert!(corrupt
                    .execute_access(&exec::ExecAccessPlan::Edge(plan))
                    .await
                    .is_err());
            }
        }

        let closed_db = test_support::open_db("access-dispatch-closed-storage").await;
        closed_db.inner_db().close().await.unwrap();
        let late = test_support::name("late");
        let mut closed = ExecutionContext::new(
            &closed_db,
            context::ParamBindings::default()
                .with_value(late.clone(), PropertyValue::from("active")),
        );
        let search = super::super::tests::support::search_index("missing-search-index");
        let search_limit = super::super::tests::support::literal_search_limit(1);
        let node_plans = vec![
            exec::ExecNodeAccessPlan::AllScan,
            exec::ExecNodeAccessPlan::AuthoritativeScan {
                predicate: exec::ExecNodeAuthoritativeScanPredicate::Predicate(
                    ir::PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
                ),
            },
            exec::ExecNodeAccessPlan::DynamicEquality {
                index: catalog::NodeEqualityIndexMeta::new(test_support::name(
                    "node_eq:User:status",
                )),
                key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                param: late.clone(),
            },
            exec::ExecNodeAccessPlan::SecondarySet {
                set: exec::ExecNodeSecondarySetPlan::AuthoritativeScan(
                    exec::ExecNodeAuthoritativeScanPredicate::Predicate(
                        ir::PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
                    ),
                ),
            },
            exec::ExecNodeAccessPlan::VectorSearch {
                key: catalog::NodeSearchIndexKey::try_new("User", "embedding").unwrap(),
                index: search.clone(),
                query_vector: ir::VectorQueryInputPlan::Vector(
                    ir::SearchVector::new(vec![1.0]).unwrap(),
                ),
                k: search_limit.clone(),
            },
            exec::ExecNodeAccessPlan::TextSearch {
                key: catalog::NodeSearchIndexKey::try_new("User", "body").unwrap(),
                index: search.clone(),
                query_text: ir::TextQueryInputPlan::Text(test_support::name("rust")),
                k: search_limit.clone(),
            },
        ];
        for plan in node_plans {
            assert!(closed
                .execute_access(&exec::ExecAccessPlan::Node(plan))
                .await
                .is_err());
        }
        let edge_plans = vec![
            exec::ExecEdgeAccessPlan::AllScan,
            exec::ExecEdgeAccessPlan::AuthoritativeScan {
                predicate: exec::ExecEdgeAuthoritativeScanPredicate::Predicate(
                    ir::PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
                ),
            },
            exec::ExecEdgeAccessPlan::DynamicEquality {
                index: catalog::EdgeEqualityIndexMeta::new(test_support::name(
                    "edge_eq:FOLLOWS:status",
                )),
                key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
                param: late,
            },
            exec::ExecEdgeAccessPlan::SecondarySet {
                set: exec::ExecEdgeSecondarySetPlan::AuthoritativeScan(
                    exec::ExecEdgeAuthoritativeScanPredicate::Predicate(
                        ir::PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
                    ),
                ),
            },
            exec::ExecEdgeAccessPlan::VectorSearch {
                key: catalog::EdgeSearchIndexKey::try_new("FOLLOWS", "embedding").unwrap(),
                index: search.clone(),
                query_vector: ir::VectorQueryInputPlan::Vector(
                    ir::SearchVector::new(vec![1.0]).unwrap(),
                ),
                k: search_limit.clone(),
            },
            exec::ExecEdgeAccessPlan::TextSearch {
                key: catalog::EdgeSearchIndexKey::try_new("FOLLOWS", "body").unwrap(),
                index: search,
                query_text: ir::TextQueryInputPlan::Text(test_support::name("rust")),
                k: search_limit,
            },
        ];
        for plan in edge_plans {
            assert!(closed
                .execute_access(&exec::ExecAccessPlan::Edge(plan))
                .await
                .is_err());
        }
    }

    #[cfg(all(feature = "production-coverage", not(test)))]
    pub(in crate::execution::interpreter::access) async fn run_production_contracts() {
        truncate_ids_applies_optional_positive_limit();
        tightest_access_limit_keeps_the_smallest_nested_limit();
        access_dispatch_covers_node_equality_and_edge_source_variants().await;
        exact_access_dispatch_covers_bitmap_unique_scan_and_dynamic_families().await;
        exact_access_dispatch_rejects_each_invalid_dynamic_and_set_driver_contract().await;
        exact_access_dispatch_propagates_scan_predicate_index_and_search_failures().await;
    }
}
