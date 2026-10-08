//! Resumable graph sources. Stored bitmaps are prepared once; storage scans keep
//! their original iterator in the request's read view until demand ends.
use super::*;
use crate::encoding::v2::values::property::view;
use access::kv;
use bytes::Bytes;
use helix_planner::properties;

/// Most unverified IDs checked for existence per `multi_get`.
const RECORD_BATCH_ROWS: usize = helix_planner::cost::RECORD_BATCH_ROWS as usize;

/// Factor by which each existence check of a source with no access-local
/// limit reads more IDs than the last, from one up to [`RECORD_BATCH_ROWS`]:
/// a consumer that stops after a few rows reads a few records, and a long
/// read still reaches full batches after three checks (1, 8, 64, 256).
const EXISTENCE_BATCH_GROWTH: usize = 8;

pub(super) enum Plan<'a> {
    Prepared,
    Access(&'a exec::ExecAccessPlan),
    Kv(&'a exec::KvReadPlan),
}

pub(super) struct Source<'a> {
    plan: Plan<'a>,
    state: State,
    remaining: Demand,
    /// Aligned property-record copies a predicate scan reuses across rows.
    buffers: view::Buffers,
}

enum Ids {
    Values(std::vec::IntoIter<u64>),
    Bitmap(Box<crate::query_resources::bitmap::IntoIter>),
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
    /// IDs to emit. Unverified IDs are checked for existence in batches of
    /// at most [`RECORD_BATCH_ROWS`]; `pending` holds the rest of the last
    /// batch in order, each with whether its element exists. Without an
    /// access-local limit, `batch` IDs are checked next, a number that grows
    /// by [`EXISTENCE_BATCH_GROWTH`] with every check.
    Ids {
        ids: Ids,
        keyspace: exec::ElementKeyspace,
        verified: bool,
        pending: std::collections::VecDeque<(u64, bool)>,
        batch: usize,
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
        ids: crate::query_resources::bitmap::Bitmap,
        keyspace: exec::ElementKeyspace,
        verified: bool,
    ) -> Self {
        Self {
            plan: Plan::Prepared,
            state: State::Ids {
                ids: Ids::Bitmap(Box::new(ids.into_iter())),
                keyspace,
                verified,
                pending: std::collections::VecDeque::new(),
                batch: 1,
            },
            remaining: Demand::All,
            buffers: view::Buffers::default(),
        }
    }

    pub(super) fn ids(ids: Vec<u64>, keyspace: exec::ElementKeyspace, verified: bool) -> Self {
        Self {
            plan: Plan::Prepared,
            state: State::Ids {
                ids: Ids::Values(ids.into_iter()),
                keyspace,
                verified,
                pending: std::collections::VecDeque::new(),
                batch: 1,
            },
            remaining: Demand::All,
            buffers: view::Buffers::default(),
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
            buffers: view::Buffers::default(),
        })
    }

    async fn open(&self, ctx: &mut ExecutionContext<'_>) -> Result<State> {
        use exec::{
            ElementKeyspace as K, ExecAccessPlan as A, ExecEdgeAccessPlan as E,
            ExecNodeAccessPlan as N,
        };
        let ids = |ids: Vec<u64>, keyspace, verified| Self::ids(ids, keyspace, verified).state;
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
                N::AuthoritativeScan {
                    predicate: exec::ExecNodeAuthoritativeScanPredicate::NullEquality { key },
                }
                | N::SecondarySet {
                    set:
                        exec::ExecNodeSecondarySetPlan::AuthoritativeScan(
                            exec::ExecNodeAuthoritativeScanPredicate::NullEquality { key },
                        ),
                },
            )) => Ok(bitmap_ids(
                ctx.null_equality_rows(crate::index_lifecycle::IndexElementKind::Node, key, None)
                    .await?,
                K::NodeProperty,
                true,
            )),
            Plan::Access(A::Edge(
                E::AuthoritativeScan {
                    predicate: exec::ExecEdgeAuthoritativeScanPredicate::NullEquality { key },
                }
                | E::SecondarySet {
                    set:
                        exec::ExecEdgeSecondarySetPlan::AuthoritativeScan(
                            exec::ExecEdgeAuthoritativeScanPredicate::NullEquality { key },
                        ),
                },
            )) => Ok(bitmap_ids(
                ctx.null_equality_rows(crate::index_lifecycle::IndexElementKind::Edge, key, None)
                    .await?,
                K::EdgeEndpoints,
                true,
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
            // Deletes remove label memberships in the same transaction, so a
            // label bitmap holds only live elements, as counts and index
            // memberships already rely on.
            Plan::Access(A::Node(N::LabelScan { label })) => Ok(bitmap_ids(
                ctx.lookup_equality_index_set(
                    "$label",
                    &DbPropertyValue::String(label.to_string()),
                )
                .await?,
                K::NodeProperty,
                true,
            )),
            Plan::Access(A::Edge(E::LabelScan { label })) => Ok(bitmap_ids(
                ctx.lookup_global_edge_label_index(label.as_ref()).await?,
                K::EdgeEndpoints,
                true,
            )),
            Plan::Access(A::Node(N::Bitmap { bitmap })) => Ok(bitmap_ids(
                ctx.node_bitmap(bitmap, access::PARALLEL_INDEX_READS)
                    .await?,
                K::NodeProperty,
                true,
            )),
            Plan::Access(A::Edge(E::Bitmap { bitmap })) => Ok(bitmap_ids(
                ctx.edge_bitmap(bitmap, access::PARALLEL_INDEX_READS)
                    .await?,
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
                        access::PARALLEL_INDEX_READS,
                        None,
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
                        access::PARALLEL_INDEX_READS,
                        None,
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
                let allowed = ctx
                    .node_secondary_filter_intersection(filters, access::PARALLEL_INDEX_READS)
                    .await?;
                self.open_range(
                    ctx,
                    K::NodeProperty,
                    &driver.key,
                    &driver.range,
                    driver.iteration,
                    vec![allowed],
                )
                .await
            }
            Plan::Access(A::Edge(E::SecondarySet {
                set: exec::ExecEdgeSecondarySetPlan::OrderedIntersect { driver, filters },
            })) => {
                let allowed = ctx
                    .edge_secondary_filter_intersection(filters, access::PARALLEL_INDEX_READS)
                    .await?;
                self.open_range(
                    ctx,
                    K::EdgeEndpoints,
                    &driver.key,
                    &driver.range,
                    driver.iteration,
                    vec![allowed],
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
        membership: Vec<crate::query_resources::bitmap::Bitmap>,
    ) -> Result<State> {
        if membership.iter().any(|ids| ids.is_empty()) {
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
                    &membership
                        .iter()
                        .map(|bitmap| &**bitmap)
                        .collect::<Vec<_>>(),
                    properties::PositiveUsize::new(limit.get()),
                )
                .await?;
            return Ok(Self::ids(ids, keyspace, true).state);
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
            // A node scan also yields the element's property record.
            let (row, record) = match &mut self.state {
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
                    (ExecutionRow::current(kv::element_ref(*keyspace, id)), None)
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
                    pending,
                    batch,
                } => {
                    let (id, exists) = match (*verified, pending.pop_front()) {
                        (true, _) => {
                            let Some(id) = ids.next() else {
                                self.state = State::Done;
                                continue;
                            };
                            (id, true)
                        }
                        (false, Some(checked)) => checked,
                        (false, None) => {
                            // Check the next batch, no longer than the
                            // access-local limit still allows, or growing
                            // from one ID when the demand is unknown.
                            let wanted = match self.remaining {
                                Demand::Take(remaining) => remaining.get().min(RECORD_BATCH_ROWS),
                                Demand::All | Demand::Done => {
                                    let wanted = *batch;
                                    *batch = wanted
                                        .saturating_mul(EXISTENCE_BATCH_GROWTH)
                                        .min(RECORD_BATCH_ROWS);
                                    wanted
                                }
                            };
                            let batch = ids.by_ref().take(wanted).collect::<Vec<_>>();
                            if batch.is_empty() {
                                self.state = State::Done;
                                continue;
                            }
                            let keys = batch
                                .iter()
                                .map(|id| {
                                    kv::physical_element_key(
                                        ctx.tenant_scope,
                                        &exec::KvKey::from_id(*keyspace, *id),
                                    )
                                    .2
                                })
                                .collect::<Vec<_>>();
                            pending.extend(
                                batch.into_iter().zip(
                                    Box::pin(ctx.multi_get_raw(&keys))
                                        .await?
                                        .into_iter()
                                        .map(|record| record.is_some()),
                                ),
                            );
                            continue;
                        }
                    };
                    #[cfg(test)]
                    ctx.pull_work
                        .source_visits
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    // An access-local limit applies at its original ID boundary.
                    self.remaining.consume();
                    if !exists {
                        continue;
                    }
                    (ExecutionRow::current(kv::element_ref(*keyspace, id)), None)
                }
                State::Scan { iter, keyspace } => {
                    let Some(entry) = iter.next().await? else {
                        self.state = State::Done;
                        continue;
                    };
                    if let Some(budget) = &ctx.row_memory {
                        budget.record_reads(crate::query_resources::StorageReadUsage {
                            scan_rows: 1,
                            ..Default::default()
                        });
                    }
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
                    // Inside a write transaction a consumer may write a later
                    // row after this scan read it, so only read-only requests
                    // reuse the scanned record instead of reading it again.
                    // A predicate admits it like the read it replaces.
                    let record = (*keyspace == exec::ElementKeyspace::NodeProperty
                        && ctx.active_write_tx().is_none())
                    .then_some(entry.value);
                    (
                        ExecutionRow::current(kv::element_ref(*keyspace, id)),
                        record,
                    )
                }
            };
            // Null equality opens as verified label rows; only a predicate
            // scan evaluates each row here.
            let accepted = match self.plan {
                Plan::Access(exec::ExecAccessPlan::Node(
                    exec::ExecNodeAccessPlan::AuthoritativeScan {
                        predicate: exec::ExecNodeAuthoritativeScanPredicate::Predicate(predicate),
                    }
                    | exec::ExecNodeAccessPlan::SecondarySet {
                        set:
                            exec::ExecNodeSecondarySetPlan::AuthoritativeScan(
                                exec::ExecNodeAuthoritativeScanPredicate::Predicate(predicate),
                            ),
                    },
                ))
                | Plan::Access(exec::ExecAccessPlan::Edge(
                    exec::ExecEdgeAccessPlan::AuthoritativeScan {
                        predicate: exec::ExecEdgeAuthoritativeScanPredicate::Predicate(predicate),
                    }
                    | exec::ExecEdgeAccessPlan::SecondarySet {
                        set:
                            exec::ExecEdgeSecondarySetPlan::AuthoritativeScan(
                                exec::ExecEdgeAuthoritativeScanPredicate::Predicate(predicate),
                            ),
                    },
                )) => {
                    let record = record
                        .map(|record| storage::retain_read(record, ctx.row_memory.as_ref()))
                        .transpose()?;
                    ctx.eval_predicate_plan_on_record(&row, predicate, record, &mut self.buffers)
                        .await?
                }
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
    async fn label_scan_rows_are_not_rechecked() {
        let db = test_support::open_db("pull-label-scan-trust").await;
        let mut nodes = Vec::new();
        for _ in 0..5 {
            nodes.push(test_support::add_node_with_properties(&db, "User", Vec::new()).await);
        }
        test_support::add_node_with_properties(&db, "Other", Vec::new()).await;
        let mut edges = Vec::new();
        for _ in 0..3 {
            edges.push(
                test_support::add_edge_with_properties(&db, nodes[0], nodes[1], "LINK", Vec::new())
                    .await,
            );
        }
        for (plan, expected) in [
            (
                exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::LabelScan {
                    label: test_support::name("User"),
                }),
                nodes,
            ),
            (
                exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::LabelScan {
                    label: test_support::name("LINK"),
                }),
                edges,
            ),
        ] {
            let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
            ctx.enable_request_read_view().await.unwrap();
            let mut source = Source::new(&ctx, Plan::Access(&plan)).unwrap();
            let mut actual = Vec::new();
            while let Some(value) = source.next(&mut ctx).await.unwrap() {
                actual.extend(
                    ctx.stream_rows(value, "test")
                        .unwrap()
                        .into_iter()
                        .map(|row| row.current.unwrap().id()),
                );
            }
            assert_eq!(actual, expected);
            // The label bitmap is trusted: no record is read to emit an ID.
            let work = ctx.pull_work.snapshot();
            assert_eq!(work.raw_gets, 0);
            assert_eq!(work.multi_get_keys, 0);
        }
    }

    #[tokio::test]
    async fn unverified_ids_are_checked_in_record_batches() {
        let db = test_support::open_db("pull-unverified-id-batches").await;
        let mut existing = Vec::new();
        for _ in 0..300 {
            existing.push(test_support::add_node_with_properties(&db, "User", Vec::new()).await);
        }
        let missing = |n: u64| existing[existing.len() - 1] + 1_000 + n;
        // Every third ID was never written, as if deleted.
        let ids = existing
            .iter()
            .enumerate()
            .flat_map(|(n, id)| {
                core::iter::once(*id).chain((n % 3 == 0).then(|| missing(n as u64)))
            })
            .collect::<Vec<_>>();

        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.enable_request_read_view().await.unwrap();
        let mut source = Source::ids(ids.clone(), exec::ElementKeyspace::NodeProperty, false);
        let mut actual = Vec::new();
        while let Some(value) = source.next(&mut ctx).await.unwrap() {
            actual.extend(
                ctx.stream_rows(value, "test")
                    .unwrap()
                    .into_iter()
                    .map(|row| row.current.unwrap().id()),
            );
        }
        assert_eq!(actual, existing);
        let work = ctx.pull_work.snapshot();
        assert_eq!(work.raw_gets, 0);
        assert_eq!(work.multi_get_keys, ids.len());
        assert_eq!(work.source_visits, ids.len());

        // With unknown demand the checks grow from one ID: a consumer that
        // stops after one row reads one record, after two rows nine.
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.enable_request_read_view().await.unwrap();
        let mut source = Source::ids(ids.clone(), exec::ElementKeyspace::NodeProperty, false);
        assert!(source.next(&mut ctx).await.unwrap().is_some());
        assert_eq!(ctx.pull_work.snapshot().multi_get_keys, 1);
        // The second ID is missing; the third exists.
        assert!(source.next(&mut ctx).await.unwrap().is_some());
        assert_eq!(ctx.pull_work.snapshot().multi_get_keys, 1 + 8);

        // An access-local limit still applies at the ID boundary: the first
        // three IDs are read, one of them missing, and nothing more.
        let param = test_support::name("ids");
        let limited = exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::FromParam {
            param: param.clone(),
        })
        .limited_by(exec::ExecAccessLimit::Static(
            properties::PositiveUsize::new(3).unwrap(),
        ));
        let mut ctx = ExecutionContext::new(
            &db,
            context::ParamBindings::default().with_value(
                param,
                helix_ast::value::PropertyValue::I64Array(
                    ids.iter().map(|id| *id as i64).collect(),
                ),
            ),
        );
        ctx.enable_request_read_view().await.unwrap();
        let mut source = Source::new(&ctx, Plan::Access(&limited)).unwrap();
        let mut actual = Vec::new();
        while let Some(value) = source.next(&mut ctx).await.unwrap() {
            actual.extend(
                ctx.stream_rows(value, "test")
                    .unwrap()
                    .into_iter()
                    .map(|row| row.current.unwrap().id()),
            );
        }
        assert_eq!(actual, vec![existing[0], existing[1]]);
        assert_eq!(ctx.pull_work.snapshot().multi_get_keys, 3);
    }

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
            let mut source = Source::bitmap(
                crate::query_resources::bitmap::Bitmap::empty(None).unwrap(),
                keyspace,
                false,
            );
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
                            buffers: view::Buffers::default(),
                        };
                        source.state = source
                            .open_range(
                                &ctx,
                                keyspace,
                                &key,
                                &ir::IndexRange::All,
                                iteration,
                                vec![
                                    crate::query_resources::bitmap::Bitmap::retain_legacy(
                                        ids.iter().copied().collect(),
                                        None,
                                    )
                                    .unwrap(),
                                    crate::query_resources::bitmap::Bitmap::empty(None).unwrap(),
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
                        let mut cursor =
                            ctx.open_range_cursor(element, &key, &ir::IndexRange::All, iteration)
                                .await
                                .unwrap()
                                .with_membership(vec![
                                    crate::query_resources::bitmap::Bitmap::empty(None).unwrap(),
                                ]);
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
                            buffers: view::Buffers::default(),
                        };
                        source.state = source
                            .open_range(
                                &ctx,
                                keyspace,
                                &key,
                                &ir::IndexRange::All,
                                ir::RangeScanIteration::Reverse,
                                membership
                                    .iter()
                                    .cloned()
                                    .map(|ids| {
                                        crate::query_resources::bitmap::Bitmap::retain_legacy(
                                            ids, None,
                                        )
                                        .unwrap()
                                    })
                                    .collect(),
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

    /// Rows written straight to the node keyspace: varied value types,
    /// duplicate and dotted names, an empty row and a large payload.
    async fn scan_predicate_fixture(db: &crate::HelixDB) {
        use crate::encoding::property::{
            encode_properties, property_value::PropertyValue as V, Property,
        };
        let object = |score: i64| V::Object([("score".to_string(), V::I64(score))].into());
        let rows = [
            vec![
                Property::string("$label", "User"),
                Property::string("status", "active"),
                Property::i64("score", 5),
                Property::new("meta", object(9)),
            ],
            vec![
                Property::string("$label", "User"),
                Property::string("status", "inactive"),
                Property::f64("score", 7.5),
            ],
            vec![
                Property::string("status", "active"),
                Property::f32_array("embedding", vec![0.5; 1536]),
                Property::i64("score", 11),
                Property::string("status", "inactive"),
            ],
            vec![
                Property::string("$label", "User"),
                Property::new("score", V::Null),
            ],
            Vec::new(),
            vec![
                Property::string("status", "active"),
                Property::i64("meta.score", 1),
                Property::new("meta", object(3)),
            ],
            vec![
                Property::string("status", "inactive"),
                Property::string("status", "active"),
                Property::bytes("blob", vec![1; 4096]),
            ],
        ];
        for (id, properties) in (1_u64..).zip(rows) {
            db.inner_db()
                .put(
                    keys::DataKey::Data {
                        scope: keys::scope::DataScope::LegacyUnscoped,
                        kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(id)),
                    }
                    .to_bytes(),
                    encode_properties(&properties),
                )
                .await
                .unwrap();
        }
    }

    /// Predicates over the fixture with the node IDs they select.
    fn scan_predicate_cases() -> Vec<(helix_ast::expr::Predicate, Vec<u64>)> {
        use helix_ast::expr::Predicate as P;
        vec![
            (P::eq("status", "active"), vec![1, 3, 6]),
            (P::gt("score", 6_i64), vec![2, 3]),
            (P::eq("meta.score", 9_i64), vec![1]),
            (P::eq("meta.score", 1_i64), vec![6]),
            (P::is_null("status"), vec![4, 5]),
            (P::is_not_null("embedding"), vec![3]),
            (
                P::and(vec![P::eq("status", "active"), P::gt("score", 6_i64)]),
                vec![3],
            ),
            (
                P::or(vec![P::eq("status", "inactive"), P::is_null("score")]),
                vec![2, 4, 5, 6, 7],
            ),
            (P::eq("$id", 2_i64), vec![2]),
            (P::not(P::eq("status", "active")), vec![2, 4, 5, 7]),
        ]
    }

    fn node_scan_plan(predicate: helix_ast::expr::Predicate) -> exec::ExecAccessPlan {
        exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::AuthoritativeScan {
            predicate: exec::ExecNodeAuthoritativeScanPredicate::Predicate(
                ir::PredicatePlan::new(predicate).unwrap(),
            ),
        })
    }

    async fn scan_ids(
        ctx: &mut ExecutionContext<'_>,
        plan: &exec::ExecAccessPlan,
    ) -> Result<Vec<u64>> {
        let mut source = Source::new(ctx, Plan::Access(plan))?;
        let mut ids = Vec::new();
        while let Some(value) = source.next(ctx).await? {
            ids.extend(
                ctx.stream_rows(value, "test")?
                    .into_iter()
                    .map(|row| row.current.unwrap().id()),
            );
        }
        Ok(ids)
    }

    #[tokio::test]
    async fn authoritative_scan_predicates_select_exactly_the_stored_matches() {
        let db = test_support::open_db("pull-scan-predicate-golden").await;
        scan_predicate_fixture(&db).await;
        for (predicate, expected) in scan_predicate_cases() {
            let plan = node_scan_plan(predicate.clone());
            let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
            ctx.enable_request_read_view().await.unwrap();
            assert_eq!(
                scan_ids(&mut ctx, &plan).await.unwrap(),
                expected,
                "{predicate:?}"
            );
            assert_eq!(ctx.pull_work.snapshot().source_visits, 7);
        }
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn authoritative_scan_reports_a_corrupt_row_only_when_a_predicate_reads_it() {
        let db = test_support::open_db("pull-scan-predicate-corruption").await;
        scan_predicate_fixture(&db).await;
        db.inner_db()
            .put(
                keys::DataKey::Data {
                    scope: keys::scope::DataScope::LegacyUnscoped,
                    kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(4)),
                }
                .to_bytes(),
                Bytes::from_static(b"corrupt node row"),
            )
            .await
            .unwrap();
        let reads = node_scan_plan(helix_ast::expr::Predicate::eq("status", "active"));
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.enable_request_read_view().await.unwrap();
        let mut source = Source::new(&ctx, Plan::Access(&reads)).unwrap();
        let first = source.next(&mut ctx).await.unwrap().unwrap();
        assert_eq!(
            ctx.stream_rows(first, "test").unwrap()[0].current,
            Some(ElementRef::Node(1))
        );
        let mut error = None;
        while error.is_none() {
            match source.next(&mut ctx).await {
                Ok(Some(_)) => {}
                Ok(None) => panic!("the corrupt row must fail the scan"),
                Err(failure) => error = Some(failure),
            }
        }
        assert!(matches!(
            error,
            Some(HelixDbError::Encoding(
                crate::encoding::error::EncodingError::Rkyv(_)
            ))
        ));
        let id_only = node_scan_plan(helix_ast::expr::Predicate::eq("$id", 4_i64));
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.enable_request_read_view().await.unwrap();
        assert_eq!(scan_ids(&mut ctx, &id_only).await.unwrap(), vec![4]);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn read_only_predicate_scans_evaluate_the_scanned_record_without_reading_it_again() {
        let db = test_support::open_db("pull-scan-predicate-record-reuse").await;
        scan_predicate_fixture(&db).await;
        for (predicate, expected) in scan_predicate_cases() {
            let plan = node_scan_plan(predicate.clone());
            let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
            ctx.enable_request_read_view().await.unwrap();
            assert_eq!(scan_ids(&mut ctx, &plan).await.unwrap(), expected);
            let work = ctx.pull_work.snapshot();
            assert_eq!(work.raw_gets, 0, "{predicate:?}");
            assert_eq!(work.multi_get_keys, 0, "{predicate:?}");
        }
        db.close().await.unwrap();
    }

    /// In a write transaction a consumer can write a row the scan already
    /// passed over; each predicate reads its row through the transaction.
    #[tokio::test]
    async fn write_transaction_scans_read_rows_written_after_the_scan_opened() {
        use crate::encoding::property::{encode_properties, Property};
        use crate::transaction::Mutation;
        let db = test_support::open_db("pull-scan-predicate-write-tx").await;
        scan_predicate_fixture(&db).await;
        let plan = node_scan_plan(helix_ast::expr::Predicate::eq("status", "active"));
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.enable_request_write_scope().await.unwrap();
        let mut source = Source::new(&ctx, Plan::Access(&plan)).unwrap();
        let first = source.next(&mut ctx).await.unwrap().unwrap();
        assert_eq!(
            ctx.stream_rows(first, "test").unwrap()[0].current,
            Some(ElementRef::Node(1))
        );
        ctx.active_write_tx()
            .unwrap()
            .txn
            .put(
                keys::DataKey::Data {
                    scope: keys::scope::DataScope::LegacyUnscoped,
                    kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(3)),
                }
                .to_bytes(),
                encode_properties(&[Property::string("status", "inactive")]),
            )
            .unwrap();
        let mut rest = Vec::new();
        while let Some(value) = source.next(&mut ctx).await.unwrap() {
            rest.extend(
                ctx.stream_rows(value, "test")
                    .unwrap()
                    .into_iter()
                    .map(|row| row.current.unwrap().id()),
            );
        }
        assert_eq!(rest, vec![6]);
        assert!(ctx.pull_work.snapshot().raw_gets > 0);
        ctx.abort_request_write_scope();
        db.close().await.unwrap();
    }

    /// A predicate scan charges every row it evaluates to the request's
    /// row-memory budget, as the storage read it once made did: a row larger
    /// than the budget fails the scan, and every charge is released after it.
    #[tokio::test]
    async fn predicate_scans_charge_evaluated_rows_to_the_row_memory_budget() {
        let db = test_support::open_db("pull-scan-predicate-budget").await;
        scan_predicate_fixture(&db).await;
        let plan = node_scan_plan(helix_ast::expr::Predicate::eq("status", "active"));
        // Row 3 holds a 6 KiB embedding and row 7 a 4 KiB blob.
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.enable_request_read_view().await.unwrap();
        ctx.row_memory = Some(crate::query_resources::Budget::new(2048));
        assert!(matches!(
            scan_ids(&mut ctx, &plan).await,
            Err(HelixDbError::QueryMemoryLimitExceeded)
        ));
        let limit = 1 << 20;
        let budget = crate::query_resources::Budget::new(limit);
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.enable_request_read_view().await.unwrap();
        ctx.row_memory = Some(budget.clone());
        assert_eq!(scan_ids(&mut ctx, &plan).await.unwrap(), vec![1, 3, 6]);
        assert!(
            budget.peak() >= 1536 * size_of::<f32>(),
            "{}",
            budget.peak()
        );
        assert_eq!(budget.available(), limit);
        db.close().await.unwrap();
    }

    /// Reusing scanned records reads each row once: the scan reports its
    /// rows and no predicate makes a point read.
    #[tokio::test]
    async fn predicate_scans_report_scanned_rows_without_point_reads() {
        let db = test_support::open_db("pull-scan-predicate-usage").await;
        scan_predicate_fixture(&db).await;
        for (predicate, expected) in scan_predicate_cases() {
            let budget = crate::query_resources::Budget::new(1 << 20);
            let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
            ctx.enable_request_read_view().await.unwrap();
            ctx.row_memory = Some(budget.clone());
            assert_eq!(
                scan_ids(&mut ctx, &node_scan_plan(predicate.clone()))
                    .await
                    .unwrap(),
                expected
            );
            let reads = budget.reads();
            assert_eq!(
                (reads.scan_rows, reads.point_gets, reads.multi_get_keys),
                (7, 0, 0),
                "{predicate:?}"
            );
        }
        db.close().await.unwrap();
    }
}
