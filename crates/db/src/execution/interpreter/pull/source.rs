//! Resumable graph sources. Stored bitmaps are prepared once; storage scans keep
//! their original iterator in the request's read view until demand ends.
use super::*;
use access::kv;
use bytes::Bytes;
use helix_planner::properties;

pub(super) enum Plan<'a> {
    Prepared,
    Access(&'a exec::ExecAccessPlan),
    Kv(&'a exec::KvReadPlan),
}

pub(super) struct Source<'a> {
    plan: Plan<'a>,
    state: State,
    remaining: Demand,
}

enum Ids {
    Values(std::vec::IntoIter<u64>),
    Bitmap(Box<roaring::treemap::IntoIter>),
}

impl Iterator for Ids {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Values(ids) => ids.next(),
            Self::Bitmap(ids) => ids.next(),
        }
    }
}

enum State {
    Unopened,
    Ids {
        ids: Ids,
        keyspace: exec::ElementKeyspace,
        verified: bool,
    },
    Scan {
        iter: slatedb::DbIterator,
        keyspace: exec::ElementKeyspace,
    },
    Range {
        cursor: Box<crate::index_lifecycle::secondary::OrderedRangeCursor>,
        keyspace: exec::ElementKeyspace,
    },
    Rows(Items),
    Done,
}

impl<'a> Source<'a> {
    /// Retain compressed membership and expand only requested identifiers.
    pub(super) fn bitmap(
        ids: roaring::RoaringTreemap,
        keyspace: exec::ElementKeyspace,
        verified: bool,
    ) -> Self {
        Self {
            plan: Plan::Prepared,
            state: State::Ids {
                ids: Ids::Bitmap(Box::new(ids.into_iter())),
                keyspace,
                verified,
            },
            remaining: Demand::All,
        }
    }

    pub(super) fn ids(ids: Vec<u64>, keyspace: exec::ElementKeyspace, verified: bool) -> Self {
        Self {
            plan: Plan::Prepared,
            state: State::Ids {
                ids: Ids::Values(ids.into_iter()),
                keyspace,
                verified,
            },
            remaining: Demand::All,
        }
    }

    pub(super) fn new(ctx: &ExecutionContext<'_>, mut plan: Plan<'a>) -> Result<Self> {
        let mut limit = None;
        if let Plan::Access(mut source) = plan {
            let mut bounds = Vec::new();
            while let exec::ExecAccessPlan::Limited(limited) = source {
                bounds.push(limited.limit());
                source = limited.source();
            }
            for bound in bounds.into_iter().rev() {
                let count = match bound {
                    exec::ExecAccessLimit::Zero => 0,
                    exec::ExecAccessLimit::Static(n) => n.get(),
                    exec::ExecAccessLimit::Dynamic(expr) => stream::eval_stream_bound(
                        &ir::StreamBoundPlan::Expr(expr.clone()),
                        &ctx.params,
                    )?,
                };
                limit = Some(limit.map_or(count, |n: usize| n.min(count)));
            }
            plan = Plan::Access(source);
        }
        if let Plan::Kv(
            exec::KvReadPlan::RangeScan { limit: n, .. }
            | exec::KvReadPlan::PrefixScan { limit: n, .. },
        ) = plan
        {
            limit = n.map(properties::PositiveUsize::get);
        }
        Ok(Self {
            plan,
            state: State::Unopened,
            remaining: limit.map_or(Demand::All, Demand::take),
        })
    }

    async fn open(&self, ctx: &mut ExecutionContext<'_>) -> Result<State> {
        use exec::{
            ElementKeyspace as K, ExecAccessPlan as A, ExecEdgeAccessPlan as E,
            ExecNodeAccessPlan as N,
        };
        let ids = |ids: Vec<u64>, keyspace, verified| State::Ids {
            ids: Ids::Values(ids.into_iter()),
            keyspace,
            verified,
        };
        let bitmap_ids = |ids, keyspace, verified| Self::bitmap(ids, keyspace, verified).state;
        match self.plan {
            Plan::Prepared => unreachable!("prepared IDs already have source state"),
            Plan::Access(A::Node(N::Empty)) | Plan::Access(A::Edge(E::Empty)) => Ok(State::Done),
            Plan::Access(A::Node(N::FromParam { param })) => {
                Ok(ids(ctx.param_ids(param)?, K::NodeProperty, false))
            }
            Plan::Access(A::Edge(E::FromParam { param })) => {
                Ok(ids(ctx.param_ids(param)?, K::EdgeEndpoints, false))
            }
            Plan::Access(A::Node(N::FromVar { variable })) => Ok(ids(
                ctx.access_variable_nodes(variable)?,
                K::NodeProperty,
                false,
            )),
            Plan::Access(A::Edge(E::FromVar { variable })) => Ok(ids(
                ctx.access_variable_edges(variable)?,
                K::EdgeEndpoints,
                false,
            )),
            Plan::Access(A::Node(
                N::AllScan
                | N::AuthoritativeScan { .. }
                | N::SecondarySet {
                    set: exec::ExecNodeSecondarySetPlan::AuthoritativeScan(_),
                },
            )) => Self::scan(ctx, K::NodeProperty).await,
            Plan::Access(A::Edge(
                E::AllScan
                | E::AuthoritativeScan { .. }
                | E::SecondarySet {
                    set: exec::ExecEdgeSecondarySetPlan::AuthoritativeScan(_),
                },
            )) => Self::scan(ctx, K::EdgeEndpoints).await,
            Plan::Access(A::Node(N::LabelScan { label })) => Ok(bitmap_ids(
                ctx.lookup_equality_index_set(
                    "$label",
                    &DbPropertyValue::String(label.to_string()),
                )
                .await?,
                K::NodeProperty,
                false,
            )),
            Plan::Access(A::Edge(E::LabelScan { label })) => Ok(bitmap_ids(
                ctx.lookup_global_edge_label_index(label.as_ref()).await?,
                K::EdgeEndpoints,
                false,
            )),
            Plan::Access(A::Node(N::Bitmap { bitmap })) => Ok(bitmap_ids(
                ctx.node_bitmap(bitmap).await?,
                K::NodeProperty,
                true,
            )),
            Plan::Access(A::Edge(E::Bitmap { bitmap })) => Ok(bitmap_ids(
                ctx.edge_bitmap(bitmap).await?,
                K::EdgeEndpoints,
                true,
            )),
            Plan::Access(A::Node(N::Unique {
                lookup,
                verification,
            })) => Ok(ids(
                ctx.verified_node_unique_owner(lookup, verification)
                    .await?
                    .into_iter()
                    .collect(),
                K::NodeProperty,
                true,
            )),
            Plan::Access(A::Node(N::DynamicEquality { index, key, param })) => {
                count::validate_node_equality_index(&index.index_id, key)?;
                let value = ctx.index_value(&ir::IndexValue::Param(param.clone()))?;
                Ok(bitmap_ids(
                    ctx.lookup_managed_equality_union(
                        crate::index_lifecycle::IndexElementKind::Node,
                        key,
                        &[value],
                    )
                    .await?,
                    K::NodeProperty,
                    true,
                ))
            }
            Plan::Access(A::Edge(E::DynamicEquality { index, key, param })) => {
                count::validate_edge_equality_index(&index.index_id, key)?;
                let value = ctx.index_value(&ir::IndexValue::Param(param.clone()))?;
                Ok(bitmap_ids(
                    ctx.lookup_managed_equality_union(
                        crate::index_lifecycle::IndexElementKind::Edge,
                        key,
                        &[value],
                    )
                    .await?,
                    K::EdgeEndpoints,
                    true,
                ))
            }
            Plan::Access(A::Node(N::DynamicMembership { index, key, values })) => {
                count::validate_node_equality_index(&index.index_id, key)?;
                Ok(bitmap_ids(
                    ctx.dynamic_membership_ids(
                        crate::index_lifecycle::IndexElementKind::Node,
                        key,
                        values,
                    )
                    .await?,
                    K::NodeProperty,
                    true,
                ))
            }
            Plan::Access(A::Edge(E::DynamicMembership { index, key, values })) => {
                count::validate_edge_equality_index(&index.index_id, key)?;
                Ok(bitmap_ids(
                    ctx.dynamic_membership_ids(
                        crate::index_lifecycle::IndexElementKind::Edge,
                        key,
                        values,
                    )
                    .await?,
                    K::EdgeEndpoints,
                    true,
                ))
            }
            Plan::Access(A::Node(N::SecondarySet {
                set: exec::ExecNodeSecondarySetPlan::Range(driver),
            })) => {
                self.open_range(
                    ctx,
                    K::NodeProperty,
                    &driver.key,
                    &driver.range,
                    driver.iteration,
                    Vec::new(),
                )
                .await
            }
            Plan::Access(A::Edge(E::SecondarySet {
                set: exec::ExecEdgeSecondarySetPlan::Range(driver),
            })) => {
                self.open_range(
                    ctx,
                    K::EdgeEndpoints,
                    &driver.key,
                    &driver.range,
                    driver.iteration,
                    Vec::new(),
                )
                .await
            }
            Plan::Access(A::Node(N::SecondarySet {
                set: exec::ExecNodeSecondarySetPlan::OrderedIntersect { driver, filters },
            })) => {
                let mut membership = Vec::new();
                for filter in filters {
                    membership.push(
                        ctx.node_secondary_set_ids(filter, None)
                            .await?
                            .into_iter()
                            .collect(),
                    );
                }
                self.open_range(
                    ctx,
                    K::NodeProperty,
                    &driver.key,
                    &driver.range,
                    driver.iteration,
                    membership,
                )
                .await
            }
            Plan::Access(A::Edge(E::SecondarySet {
                set: exec::ExecEdgeSecondarySetPlan::OrderedIntersect { driver, filters },
            })) => {
                let mut membership = Vec::new();
                for filter in filters {
                    membership.push(
                        ctx.edge_secondary_set_ids(filter, None)
                            .await?
                            .into_iter()
                            .collect(),
                    );
                }
                self.open_range(
                    ctx,
                    K::EdgeEndpoints,
                    &driver.key,
                    &driver.range,
                    driver.iteration,
                    membership,
                )
                .await
            }
            Plan::Access(A::Node(
                N::SecondarySet { .. } | N::VectorSearch { .. } | N::TextSearch { .. },
            ))
            | Plan::Access(A::Edge(
                E::SecondarySet { .. } | E::VectorSearch { .. } | E::TextSearch { .. },
            )) => {
                let Plan::Access(plan) = self.plan else {
                    unreachable!()
                };
                Ok(State::Rows(Items::new(
                    Box::pin(ctx.execute_prepared_access(
                        plan,
                        match self.remaining {
                            Demand::All => None,
                            Demand::Take(n) => properties::PositiveUsize::new(n.get()),
                            Demand::Done => unreachable!("zero demand does not open a source"),
                        },
                    ))
                    .await?,
                )))
            }
            Plan::Access(A::Node(N::RangeIndex {
                key,
                range,
                iteration,
                ..
            })) => {
                self.open_range(ctx, K::NodeProperty, key, range, *iteration, Vec::new())
                    .await
            }
            Plan::Access(A::Edge(E::RangeIndex {
                key,
                range,
                iteration,
                ..
            })) => {
                self.open_range(ctx, K::EdgeEndpoints, key, range, *iteration, Vec::new())
                    .await
            }
            Plan::Access(A::Limited(_)) => unreachable!("bounds resolved when source is activated"),
            Plan::Kv(exec::KvReadPlan::RangeScan {
                keyspace,
                start,
                end,
                ..
            }) => {
                let (start, end) = kv::element_range_bounds(*keyspace, start, end);
                Ok(State::Scan {
                    iter: ctx.open_raw_range(start, end).await?,
                    keyspace: *keyspace,
                })
            }
            Plan::Kv(exec::KvReadPlan::PrefixScan {
                keyspace, prefix, ..
            }) => {
                let mut bytes = kv::element_prefix(*keyspace);
                bytes.extend_from_slice(prefix.as_ref());
                Ok(State::Scan {
                    iter: ctx.open_raw_prefix(Bytes::from(bytes)).await?,
                    keyspace: *keyspace,
                })
            }
            // Keep the planner-selected point and multi-get primitives intact.
            Plan::Kv(plan @ (exec::KvReadPlan::Get { .. } | exec::KvReadPlan::MultiGet(_))) => Ok(
                State::Rows(Items::new(Box::pin(ctx.execute_kv_read(plan)).await?)),
            ),
        }
    }

    /// Known reverse bounds use the native bounded tie-group preparation.
    /// Unknown demand (for example a residual filter) keeps the resumable
    /// cursor. Both paths use the same request view and authoritative checks.
    async fn open_range(
        &self,
        ctx: &ExecutionContext<'_>,
        keyspace: exec::ElementKeyspace,
        key: &helix_planner::catalog::ScopedPropertyDirectionKey,
        range: &ir::IndexRange,
        iteration: ir::RangeScanIteration,
        membership: Vec<roaring::RoaringTreemap>,
    ) -> Result<State> {
        if membership.iter().any(roaring::RoaringTreemap::is_empty) {
            return Ok(State::Done);
        }
        let element = match keyspace {
            exec::ElementKeyspace::NodeProperty => crate::index_lifecycle::IndexElementKind::Node,
            exec::ElementKeyspace::EdgeEndpoints => crate::index_lifecycle::IndexElementKind::Edge,
        };
        if let Demand::Take(limit) = self.remaining
            && iteration == ir::RangeScanIteration::Reverse
        {
            let ids = ctx
                .range_index_ids(
                    element,
                    key,
                    range,
                    iteration,
                    &membership,
                    properties::PositiveUsize::new(limit.get()),
                )
                .await?;
            return Ok(State::Ids {
                ids: Ids::Values(ids.into_iter()),
                keyspace,
                verified: true,
            });
        }
        Ok(State::Range {
            cursor: Box::new(
                ctx.open_range_cursor(element, key, range, iteration)
                    .await?
                    .with_membership(membership),
            ),
            keyspace,
        })
    }

    async fn scan(ctx: &ExecutionContext<'_>, keyspace: exec::ElementKeyspace) -> Result<State> {
        Ok(State::Scan {
            iter: ctx
                .open_raw_range(
                    Bytes::from(kv::element_prefix(keyspace)),
                    Bytes::from(kv::element_prefix_end(keyspace)),
                )
                .await?,
            keyspace,
        })
    }

    pub(super) async fn next(
        &mut self,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<ExecutionValue>> {
        loop {
            ctx.check_execution_deadline()?;
            if matches!(self.remaining, Demand::Done) {
                return Ok(None);
            }
            let row = match &mut self.state {
                State::Unopened => {
                    self.state = Box::pin(self.open(ctx)).await?;
                    continue;
                }
                State::Done => return Ok(None),
                State::Range { cursor, keyspace } => {
                    let Some(id) = ctx.next_range_cursor(cursor).await? else {
                        self.state = State::Done;
                        continue;
                    };
                    self.remaining.consume();
                    ExecutionRow::current(kv::element_ref(*keyspace, id))
                }
                State::Rows(items) => {
                    let item = items.next();
                    self.remaining.consume();
                    return Ok(item);
                }
                State::Ids {
                    ids,
                    keyspace,
                    verified,
                } => {
                    let Some(id) = ids.next() else {
                        self.state = State::Done;
                        continue;
                    };
                    #[cfg(test)]
                    ctx.pull_work
                        .source_visits
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    // An access-local limit applies at its original ID boundary.
                    self.remaining.consume();
                    if !*verified {
                        let key = exec::KvKey::from_id(*keyspace, id);
                        if ctx
                            .get_raw(&kv::physical_element_key(ctx.tenant_scope, &key).2)
                            .await?
                            .is_none()
                        {
                            continue;
                        }
                    }
                    ExecutionRow::current(kv::element_ref(*keyspace, id))
                }
                State::Scan { iter, keyspace } => {
                    let Some(entry) = iter.next().await? else {
                        self.state = State::Done;
                        continue;
                    };
                    #[cfg(test)]
                    ctx.pull_work
                        .source_visits
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let Some(key) = ctx.tenant_scope.strip_key(&entry.key) else {
                        return Err(HelixDbError::InvariantViolation(
                            "tenant-scoped scan returned key outside tenant prefix".into(),
                        ));
                    };
                    let Some(id) = kv::parse_element_id(*keyspace, key) else {
                        continue;
                    };
                    ExecutionRow::current(kv::element_ref(*keyspace, id))
                }
            };
            let accepted = match self.plan {
                Plan::Access(exec::ExecAccessPlan::Node(
                    exec::ExecNodeAccessPlan::AuthoritativeScan { predicate }
                    | exec::ExecNodeAccessPlan::SecondarySet {
                        set: exec::ExecNodeSecondarySetPlan::AuthoritativeScan(predicate),
                    },
                )) => match predicate {
                    exec::ExecNodeAuthoritativeScanPredicate::NullEquality { key } => {
                        ctx.scoped_null_matches(&row, key).await?
                    }
                    exec::ExecNodeAuthoritativeScanPredicate::Predicate(predicate) => {
                        ctx.eval_predicate(&row, predicate.predicate()).await?
                    }
                },
                Plan::Access(exec::ExecAccessPlan::Edge(
                    exec::ExecEdgeAccessPlan::AuthoritativeScan { predicate }
                    | exec::ExecEdgeAccessPlan::SecondarySet {
                        set: exec::ExecEdgeSecondarySetPlan::AuthoritativeScan(predicate),
                    },
                )) => match predicate {
                    exec::ExecEdgeAuthoritativeScanPredicate::NullEquality { key } => {
                        ctx.scoped_null_matches(&row, key).await?
                    }
                    exec::ExecEdgeAuthoritativeScanPredicate::Predicate(predicate) => {
                        ctx.eval_predicate(&row, predicate.predicate()).await?
                    }
                },
                Plan::Prepared | Plan::Access(_) | Plan::Kv(_) => true,
            };
            if !accepted {
                continue;
            }
            if matches!(self.state, State::Scan { .. }) {
                self.remaining.consume();
            }
            return Ok(Some(ExecutionValue::Stream(vec![row])));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn bitmap_sources_keep_compressed_iterators_for_bounded_and_unknown_demand() {
        use helix_ast::value::PropertyValue;
        use helix_planner::catalog;
        let db = test_support::open_db_with_config(
            test_support::in_memory_config("pull-bitmap-retention")
                .with_equality_index("User", "kind")
                .with_edge_equality_index("LINK", "kind"),
        )
        .await;
        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        for _ in 0..128 {
            nodes.push(
                test_support::add_node_with_properties(
                    &db,
                    "User",
                    vec![("kind", PropertyValue::from("match"))],
                )
                .await,
            );
        }
        for _ in 0..128 {
            edges.push(
                test_support::add_edge_with_properties(
                    &db,
                    nodes[0],
                    nodes[1],
                    "LINK",
                    vec![("kind", PropertyValue::from("match"))],
                )
                .await,
            );
        }
        let key = catalog::ScopedPropertyKey::try_new("User", "kind").unwrap();
        let edge_key = catalog::ScopedPropertyKey::try_new("LINK", "kind").unwrap();
        let index = catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:User:kind"));
        let edge_index =
            catalog::EdgeEqualityIndexMeta::new(test_support::name("edge_eq:LINK:kind"));
        let param = test_support::name("kind");
        let values =
            ir::RuntimeEqualitySet::new(param.clone(), std::num::NonZeroUsize::new(2).unwrap());
        let value = exec::ExecIndexedEqualityValue::try_from(
            ir::SecondaryIndexLiteral::new(PropertyValue::from("match")).unwrap(),
        )
        .unwrap();
        let plans = [
            (
                exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::LabelScan {
                    label: test_support::name("User"),
                }),
                &nodes,
            ),
            (
                exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::LabelScan {
                    label: test_support::name("LINK"),
                }),
                &edges,
            ),
            (
                exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::Bitmap {
                    bitmap: exec::ExecNodeBitmapExpr::PointRead {
                        index: index.clone().try_into().unwrap(),
                        key: key.clone(),
                        value: value.clone(),
                    },
                }),
                &nodes,
            ),
            (
                exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::Bitmap {
                    bitmap: exec::ExecEdgeBitmapExpr::PointRead {
                        index: exec::ExecEdgeNonUniqueEqualityIndex::new(edge_index.clone()),
                        key: edge_key.clone(),
                        value,
                    },
                }),
                &edges,
            ),
            (
                exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::DynamicEquality {
                    index: index.clone(),
                    key: key.clone(),
                    param: param.clone(),
                }),
                &nodes,
            ),
            (
                exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::DynamicEquality {
                    index: edge_index.clone(),
                    key: edge_key.clone(),
                    param: param.clone(),
                }),
                &edges,
            ),
            (
                exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::DynamicMembership {
                    index,
                    key,
                    values: values.clone(),
                }),
                &nodes,
            ),
            (
                exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::DynamicMembership {
                    index: edge_index,
                    key: edge_key,
                    values,
                }),
                &edges,
            ),
        ];
        for (plan, expected) in plans {
            let membership = matches!(
                plan,
                exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::DynamicMembership { .. })
                    | exec::ExecAccessPlan::Edge(
                        exec::ExecEdgeAccessPlan::DynamicMembership { .. }
                    )
            );
            for take in [0, 1, 7, 256] {
                let params = context::ParamBindings::default().with_value(
                    param.clone(),
                    if membership {
                        PropertyValue::StringArray(vec!["match".into()])
                    } else {
                        PropertyValue::from("match")
                    },
                );
                let mut ctx = ExecutionContext::new(&db, params);
                ctx.enable_request_read_view().await.unwrap();
                let limit = properties::PositiveUsize::new(take)
                    .map_or(exec::ExecAccessLimit::Zero, exec::ExecAccessLimit::Static);
                let access = plan.clone().limited_by(limit);
                let mut source = Source::new(&ctx, Plan::Access(&access)).unwrap();
                let mut actual = Vec::new();
                while let Some(value) = source.next(&mut ctx).await.unwrap() {
                    assert!(matches!(
                        source.state,
                        State::Ids {
                            ids: Ids::Bitmap(_),
                            ..
                        }
                    ));
                    actual.extend(
                        ctx.stream_rows(value, "test")
                            .unwrap()
                            .into_iter()
                            .map(|row| row.current.unwrap().id()),
                    );
                }
                assert_eq!(
                    actual,
                    expected.iter().take(take).copied().collect::<Vec<_>>()
                );
                assert_eq!(
                    ctx.pull_work.snapshot().source_visits,
                    expected.len().min(take)
                );
                // Unknown downstream demand must retain the same compressed
                // representation; stopping after one item must not expand it.
                let mut source = Source::new(&ctx, Plan::Access(&plan)).unwrap();
                assert!(source.next(&mut ctx).await.unwrap().is_some());
                assert!(matches!(
                    source.state,
                    State::Ids {
                        ids: Ids::Bitmap(_),
                        ..
                    }
                ));
                let count =
                    match &plan {
                        exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::DynamicEquality {
                            index,
                            key,
                            param,
                        }) => Some(exec::ExecCountCursorPlan::NodeDynamicEquality {
                            index: index.clone(),
                            key: key.clone(),
                            param: param.clone(),
                        }),
                        exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::DynamicEquality {
                            index,
                            key,
                            param,
                        }) => Some(exec::ExecCountCursorPlan::EdgeDynamicEquality {
                            index: index.clone(),
                            key: key.clone(),
                            param: param.clone(),
                        }),
                        exec::ExecAccessPlan::Node(
                            exec::ExecNodeAccessPlan::DynamicMembership { index, key, values },
                        ) => Some(exec::ExecCountCursorPlan::NodeDynamicMembership {
                            index: index.clone(),
                            key: key.clone(),
                            values: values.clone(),
                        }),
                        exec::ExecAccessPlan::Edge(
                            exec::ExecEdgeAccessPlan::DynamicMembership { index, key, values },
                        ) => Some(exec::ExecCountCursorPlan::EdgeDynamicMembership {
                            index: index.clone(),
                            key: key.clone(),
                            values: values.clone(),
                        }),
                        exec::ExecAccessPlan::Node(_)
                        | exec::ExecAccessPlan::Edge(_)
                        | exec::ExecAccessPlan::Limited(_) => None,
                    };
                let before = ctx.pull_work.snapshot().source_visits;
                let Some(count) = count else {
                    ctx.close_request_read_view().unwrap();
                    continue;
                };
                assert_eq!(
                    ctx.pull_count_cardinality(&count, &mut None, 0, Some(take))
                        .await
                        .unwrap(),
                    expected.len().min(take)
                );
                assert_eq!(
                    ctx.pull_work.snapshot().source_visits - before,
                    expected.len().min(take)
                );
                ctx.close_request_read_view().unwrap();
            }
        }
        for plan in [
            exec::ExecCountCursorPlan::NodeLabelBitmap(test_support::name("User")),
            exec::ExecCountCursorPlan::EdgeLabelBitmap(test_support::name("LINK")),
        ] {
            let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
            ctx.enable_request_read_view().await.unwrap();
            assert_eq!(
                ctx.pull_count_cardinality(&plan, &mut None, 0, Some(1))
                    .await
                    .unwrap(),
                1
            );
            assert_eq!(ctx.pull_work.snapshot().source_visits, 1);
            ctx.close_request_read_view().unwrap();
        }
        for keyspace in [
            exec::ElementKeyspace::NodeProperty,
            exec::ElementKeyspace::EdgeEndpoints,
        ] {
            let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
            let mut source = Source::bitmap(roaring::RoaringTreemap::new(), keyspace, false);
            assert!(source.next(&mut ctx).await.unwrap().is_none());
            assert_eq!(ctx.pull_work.snapshot().source_visits, 0);
        }
        db.close().await.unwrap();
    }
    #[tokio::test]
    async fn bounded_reverse_ranges_preserve_retention_membership_and_stale_refill() {
        use crate::encoding::keys;
        use helix_ast::{index::RangeIndexDirection, value::PropertyValue};
        use helix_planner::catalog;
        for direction in [RangeIndexDirection::Asc, RangeIndexDirection::Desc] {
            let config = test_support::in_memory_config("bounded-reverse-range");
            let config = match direction {
                RangeIndexDirection::Asc => config
                    .with_range_index("User", "score")
                    .with_edge_range_index("LINK", "score"),
                RangeIndexDirection::Desc => config
                    .with_range_desc_index("User", "score")
                    .with_edge_range_desc_index("LINK", "score"),
            };
            let db = test_support::open_db_with_config(config).await;
            let mut nodes = Vec::new();
            let mut edges = Vec::new();
            for _ in 0..16 {
                nodes.push(
                    test_support::add_node_with_properties(
                        &db,
                        "User",
                        vec![("score", PropertyValue::I64(10))],
                    )
                    .await,
                );
            }
            for _ in 0..16 {
                edges.push(
                    test_support::add_edge_with_properties(
                        &db,
                        nodes[0],
                        nodes[1],
                        "LINK",
                        vec![("score", PropertyValue::I64(10))],
                    )
                    .await,
                );
            }
            for (keyspace, label, ids) in [
                (exec::ElementKeyspace::NodeProperty, "User", nodes),
                (exec::ElementKeyspace::EdgeEndpoints, "LINK", edges),
            ] {
                // Leave a stale index entry at the first ascending-ID tie.
                let key = keys::DataKey::Data {
                    scope: keys::scope::DataScope::LegacyUnscoped,
                    kind: match keyspace {
                        exec::ElementKeyspace::NodeProperty => {
                            keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(ids[0]))
                        }
                        exec::ElementKeyspace::EdgeEndpoints => {
                            keys::DataKeyKind::EdgePropertyById(keys::EdgePropertyByIdKey::new(
                                ids[0],
                            ))
                        }
                    },
                }
                .to_bytes();
                db.inner_db().delete(key).await.unwrap();
                let key = catalog::ScopedPropertyDirectionKey::try_new(label, "score", direction)
                    .unwrap();
                // Exercise node and edge secondary-set access in a request
                // snapshot, not just the shared preparation helper.
                let family = match keyspace {
                    exec::ElementKeyspace::NodeProperty => "node_range",
                    exec::ElementKeyspace::EdgeEndpoints => "edge_range",
                };
                let index_name =
                    test_support::name(&format!("{family}:{label}:score:{direction:?}"));
                let access = match keyspace {
                    exec::ElementKeyspace::NodeProperty => {
                        exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::SecondarySet {
                            set: exec::ExecNodeSecondarySetPlan::Range(
                                exec::ExecNodeSecondaryRangePlan {
                                    index: catalog::NodeRangeIndexMeta::new(index_name),
                                    key: key.clone(),
                                    range: ir::IndexRange::All,
                                    iteration: ir::RangeScanIteration::Reverse,
                                },
                            ),
                        })
                    }
                    exec::ElementKeyspace::EdgeEndpoints => {
                        exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::SecondarySet {
                            set: exec::ExecEdgeSecondarySetPlan::Range(
                                exec::ExecEdgeSecondaryRangePlan {
                                    index: catalog::EdgeRangeIndexMeta::new(index_name),
                                    key: key.clone(),
                                    range: ir::IndexRange::All,
                                    iteration: ir::RangeScanIteration::Reverse,
                                },
                            ),
                        })
                    }
                }
                .limited_by(exec::ExecAccessLimit::Static(
                    properties::PositiveUsize::new(1).unwrap(),
                ));
                let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
                ctx.enable_request_read_view().await.unwrap();
                let result = ctx.execute_access(&access).await.unwrap();
                ctx.close_request_read_view().unwrap();
                assert_eq!(
                    ctx.stream_rows(result, "test").unwrap()[0]
                        .current
                        .as_ref()
                        .unwrap()
                        .id(),
                    ids[1]
                );
                assert!(ctx.range_reads.peak.load(Ordering::Relaxed) <= 1);
                // Empty intersections must not visit or buffer even one range entry.
                for iteration in [
                    ir::RangeScanIteration::Forward,
                    ir::RangeScanIteration::Reverse,
                ] {
                    for remaining in [Demand::All, Demand::take(1)] {
                        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
                        ctx.enable_request_read_view().await.unwrap();
                        let mut source = Source {
                            plan: Plan::Prepared,
                            state: State::Done,
                            remaining,
                        };
                        source.state = source
                            .open_range(
                                &ctx,
                                keyspace,
                                &key,
                                &ir::IndexRange::All,
                                iteration,
                                vec![
                                    ids.iter().copied().collect(),
                                    roaring::RoaringTreemap::new(),
                                ],
                            )
                            .await
                            .unwrap();
                        assert!(matches!(source.state, State::Done));
                        assert!(source.next(&mut ctx).await.unwrap().is_none());
                        let element = match keyspace {
                            exec::ElementKeyspace::NodeProperty => {
                                crate::index_lifecycle::IndexElementKind::Node
                            }
                            exec::ElementKeyspace::EdgeEndpoints => {
                                crate::index_lifecycle::IndexElementKind::Edge
                            }
                        };
                        let mut cursor = ctx
                            .open_range_cursor(element, &key, &ir::IndexRange::All, iteration)
                            .await
                            .unwrap()
                            .with_membership(vec![roaring::RoaringTreemap::new()]);
                        // Also cover direct cursor users and repeated polls of an exhausted cursor.
                        for _ in 0..2 {
                            assert!(ctx.next_range_cursor(&mut cursor).await.unwrap().is_none());
                        }
                        assert_eq!(ctx.range_reads.entries.load(Ordering::Relaxed), 0);
                        assert_eq!(ctx.range_reads.reads.load(Ordering::Relaxed), 0);
                        assert_eq!(ctx.range_reads.peak.load(Ordering::Relaxed), 0);
                        ctx.close_request_read_view().unwrap();
                    }
                }
                for membership in [
                    Vec::new(),
                    vec![ids
                        .iter()
                        .step_by(2)
                        .copied()
                        .collect::<roaring::RoaringTreemap>()],
                    vec![roaring::RoaringTreemap::new()],
                ] {
                    for take in [1, 3, 20] {
                        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
                        let mut source = Source {
                            plan: Plan::Prepared,
                            state: State::Done,
                            remaining: Demand::take(take),
                        };
                        source.state = source
                            .open_range(
                                &ctx,
                                keyspace,
                                &key,
                                &ir::IndexRange::All,
                                ir::RangeScanIteration::Reverse,
                                membership.clone(),
                            )
                            .await
                            .unwrap();
                        let mut actual = Vec::new();
                        while let Some(value) = source.next(&mut ctx).await.unwrap() {
                            actual.extend(
                                ctx.stream_rows(value, "test")
                                    .unwrap()
                                    .into_iter()
                                    .map(|row| row.current.unwrap().id()),
                            );
                        }
                        let expected = ids
                            .iter()
                            .skip(1)
                            .copied()
                            .filter(|id| membership.iter().all(|set| set.contains(*id)))
                            .take(take)
                            .collect::<Vec<_>>();
                        assert_eq!(actual, expected);
                        assert!(ctx.range_reads.peak.load(Ordering::Relaxed) <= take);
                    }
                }
            }
            db.close().await.unwrap();
        }
    }
}
