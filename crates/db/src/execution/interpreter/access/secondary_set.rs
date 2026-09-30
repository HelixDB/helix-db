//! V2-aware managed secondary-index ID-set execution.
//!
//! The planner supplies logical identities only. This module resolves them
//! through the request-authorized Active catalog, combines verified IDs, and
//! preserves an ordered range driver until all filters have been applied.
//!
//! Every `Intersect`, `Union` and `OrderedIntersect` reads its children
//! concurrently through one bounded stream that yields their results in plan
//! order, so the combined IDs and the first error in plan order are exactly
//! those of a sequential read. An empty child does not end the read early.
//! One resolved set keeps at most [`PARALLEL_INDEX_READS`] leaf index reads in
//! flight at any nesting depth: each composite splits its budget over the
//! children it reads at once, and `width * floor(reads / width) <= reads`, so a
//! `Union` nested in an `Intersect` cannot multiply the concurrency. An ordered
//! range driver still runs only after all of its filters are resolved.
//!
//! Every child read beyond the first of a composite also takes one of the
//! request's [`SharedIndexReads`], which every step context of the request
//! shares, parallel steps included. A composite that finds none free reads
//! its children one at a time, as it did before reads were concurrent. So one
//! request keeps at most
//!
//! ```text
//! concurrent resolves + PARALLEL_INDEX_READS - 1
//! ```
//!
//! index reads in flight, where a concurrent resolve is one set read by one
//! step at a time (a membership with an `Evaluate` policy counts twice: its
//! set and its label bitmap). A lone set still reaches the full per-set bound.

use core::num::NonZeroUsize;
use core::sync::atomic::{AtomicUsize, Ordering};

use futures::future::BoxFuture;
use futures::{future, stream, FutureExt, Stream, StreamExt, TryStreamExt};
use helix_planner::{exec, properties};
use roaring::RoaringTreemap;

use super::super::ExecutionContext;
use crate::encoding::v2::values::property::equality_index_value;
use crate::error::Result;

/// Concurrent child reads one secondary-index set keeps in flight.
///
/// This is [`helix_planner::cost::MAX_PARALLEL_KV_READS`], the default of the
/// planner's `max_parallel_kv_reads`, so pricing and execution share one bound.
pub(in crate::execution::interpreter) const PARALLEL_INDEX_READS: NonZeroUsize =
    helix_planner::cost::MAX_PARALLEL_KV_READS;

/// Concurrent child reads one request may add beyond one read per set it is
/// resolving, shared by every step context of the request.
///
/// A composite takes what is free when it starts reading and returns it when
/// its read ends; taking never waits, so no set can block on another.
#[derive(Debug)]
pub(in crate::execution::interpreter) struct SharedIndexReads(AtomicUsize);

impl Default for SharedIndexReads {
    fn default() -> Self {
        Self(AtomicUsize::new(PARALLEL_INDEX_READS.get() - 1))
    }
}

impl SharedIndexReads {
    /// Take up to `wanted` extra reads, as many as are free now.
    fn take(&self, wanted: usize) -> ExtraIndexReads<'_> {
        let (Ok(free) | Err(free)) =
            self.0
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |free| {
                    Some(free - free.min(wanted))
                });
        ExtraIndexReads {
            pool: self,
            taken: free.min(wanted),
        }
    }
}

/// Extra reads one composite holds until its read ends.
struct ExtraIndexReads<'a> {
    pool: &'a SharedIndexReads,
    taken: usize,
}

impl Drop for ExtraIndexReads<'_> {
    fn drop(&mut self) {
        self.pool.0.fetch_add(self.taken, Ordering::SeqCst);
    }
}

/// Intersect `children` in the order they arrive.
///
/// The first error ends the fold and is returned. No children intersect to
/// the empty set.
pub(in crate::execution::interpreter) async fn intersection(
    children: impl Stream<Item = Result<RoaringTreemap>>,
) -> Result<RoaringTreemap> {
    children
        .try_fold(None, |ids: Option<RoaringTreemap>, child| {
            future::ready(Ok(Some(match ids {
                None => child,
                Some(ids) => ids & child,
            })))
        })
        .await
        .map(Option::unwrap_or_default)
}

/// Unite `children` in the order they arrive.
///
/// The first error ends the fold and is returned.
pub(in crate::execution::interpreter) async fn union(
    children: impl Stream<Item = Result<RoaringTreemap>>,
) -> Result<RoaringTreemap> {
    children
        .try_fold(RoaringTreemap::new(), |mut ids, child| {
            ids |= child;
            future::ready(Ok(ids))
        })
        .await
}

/// One set child counted in the database's in-flight test counters from its
/// creation until it finishes or is dropped.
#[cfg(test)]
struct InFlightChild<'a>(&'a crate::HelixDBInner);

#[cfg(test)]
impl<'a> InFlightChild<'a> {
    fn start(db: &'a crate::HelixDBInner) -> Self {
        let in_flight = db
            .index_child_reads_in_flight
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        db.peak_index_child_reads
            .fetch_max(in_flight, std::sync::atomic::Ordering::SeqCst);
        Self(db)
    }
}

#[cfg(test)]
impl Drop for InFlightChild<'_> {
    fn drop(&mut self) {
        self.0
            .index_child_reads_in_flight
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

enum SecondaryIds {
    Unordered(RoaringTreemap),
    Ordered(Vec<u64>),
}

impl SecondaryIds {
    fn into_bitmap(self) -> RoaringTreemap {
        match self {
            Self::Unordered(ids) => ids,
            Self::Ordered(ids) => RoaringTreemap::from_iter(ids),
        }
    }

    fn into_vec(self, limit: Option<properties::PositiveUsize>) -> Vec<u64> {
        let limit = limit.map_or(usize::MAX, properties::PositiveUsize::get);
        match self {
            Self::Unordered(ids) => ids.into_iter().take(limit).collect(),
            Self::Ordered(ids) => ids.into_iter().take(limit).collect(),
        }
    }
}

impl<'db> ExecutionContext<'db> {
    /// Read `children` concurrently and yield their results in plan order.
    ///
    /// At most `width = min(children, reads)` children are read at once, and
    /// each child gets `reads / width` (at least 1) of the budget for its own
    /// children. A nested set therefore never keeps more than `reads` leaf
    /// reads in flight at any depth. Every child beyond the first also needs
    /// one of the request's [`SharedIndexReads`], taken once when the stream
    /// is created, so a busy request narrows `width`, down to one child at a
    /// time. Results, and so the first error, arrive in plan order. Later
    /// children may already be in flight, and they are dropped when an
    /// earlier child fails.
    pub(in crate::execution::interpreter) fn read_children<'a, C: ?Sized + 'a, T: 'a>(
        &'a self,
        children: Vec<&'a C>,
        reads: NonZeroUsize,
        read: impl Fn(&'a C, NonZeroUsize) -> BoxFuture<'a, Result<T>> + 'a,
    ) -> impl Stream<Item = Result<T>> + 'a {
        // `buffered(0)` would never poll a child, so an empty list keeps width 1.
        let extra = self
            .shared_index_reads
            .take(children.len().clamp(1, reads.get()) - 1);
        let width = 1 + extra.taken;
        let budget = NonZeroUsize::new(reads.get() / width).unwrap_or(NonZeroUsize::MIN);
        stream::iter(children)
            .map(move |child| {
                // The closure owns the extra reads, so they return to the
                // request when the stream is dropped.
                let _extra = &extra;
                let child = read(child, budget);
                // Tests count the child from its creation until it finishes.
                #[cfg(test)]
                let child = {
                    let in_flight = InFlightChild::start(&self.db.inner);
                    async move {
                        let _in_flight = in_flight;
                        child.await
                    }
                    .boxed()
                };
                child
            })
            .buffered(width)
    }

    /// Resolve the filters of an ordered node intersection concurrently, in
    /// plan order, and intersect them into the one bitmap the range driver
    /// checks.
    ///
    /// Intersecting before the driver runs keeps one bitmap no larger than the
    /// smallest filter alive for the scan, one probe per driver entry, and lets
    /// disjoint non-empty filters skip the range scan entirely.
    pub(in crate::execution::interpreter) async fn node_secondary_filter_intersection(
        &self,
        filters: &[exec::ExecNodeSecondarySetPlan],
        reads: NonZeroUsize,
    ) -> Result<RoaringTreemap> {
        intersection(
            self.read_children(filters.iter().collect(), reads, |filter, reads| {
                self.node_secondary_ids(filter, None, reads)
            })
            .map_ok(SecondaryIds::into_bitmap),
        )
        .await
    }

    /// Resolve the filters of an ordered edge intersection concurrently, in
    /// plan order, and intersect them into the one bitmap the range driver
    /// checks.
    ///
    /// Intersecting before the driver runs keeps one bitmap no larger than the
    /// smallest filter alive for the scan, one probe per driver entry, and lets
    /// disjoint non-empty filters skip the range scan entirely.
    pub(in crate::execution::interpreter) async fn edge_secondary_filter_intersection(
        &self,
        filters: &[exec::ExecEdgeSecondarySetPlan],
        reads: NonZeroUsize,
    ) -> Result<RoaringTreemap> {
        intersection(
            self.read_children(filters.iter().collect(), reads, |filter, reads| {
                self.edge_secondary_ids(filter, None, reads)
            })
            .map_ok(SecondaryIds::into_bitmap),
        )
        .await
    }

    pub(in crate::execution::interpreter) async fn node_secondary_set_ids(
        &self,
        set: &exec::ExecNodeSecondarySetPlan,
        limit: Option<properties::PositiveUsize>,
    ) -> Result<Vec<u64>> {
        self.node_secondary_ids(set, limit, PARALLEL_INDEX_READS)
            .await
            .map(|ids| ids.into_vec(limit))
    }

    /// Resolve a node secondary set to an unordered ID bitmap.
    pub(in crate::execution::interpreter) async fn node_secondary_set_bitmap(
        &self,
        set: &exec::ExecNodeSecondarySetPlan,
    ) -> Result<RoaringTreemap> {
        self.node_secondary_ids(set, None, PARALLEL_INDEX_READS)
            .await
            .map(SecondaryIds::into_bitmap)
    }

    /// Whether `set` resolves from index reads alone in this request.
    ///
    /// Literal null equality, and runtime parameters or domains that bind null
    /// or values without an exact index encoding, verify the label rows
    /// outside the equality lane record by record, so they return `false`. A
    /// runtime domain of indexed values is index-served at any size.
    /// Range scans verify every in-range record of the label with its own
    /// authoritative read, and runtime bounds may have no range encoding at
    /// all, so any set with a range scan returns `false` as well.
    ///
    /// An unbound runtime parameter also returns `false`: the per-row filter
    /// reads a parameter only for rows that reach it, so the request must fail
    /// only when such a row exists, not when the set is resolved up front.
    pub(in crate::execution::interpreter) fn node_secondary_set_is_index_served(
        &self,
        set: &exec::ExecNodeSecondarySetPlan,
    ) -> Result<bool> {
        match set {
            exec::ExecNodeSecondarySetPlan::Empty
            | exec::ExecNodeSecondarySetPlan::Bitmap(_)
            | exec::ExecNodeSecondarySetPlan::UniqueUnion { .. }
            | exec::ExecNodeSecondarySetPlan::Unique { .. } => Ok(true),
            exec::ExecNodeSecondarySetPlan::AuthoritativeScan(_)
            | exec::ExecNodeSecondarySetPlan::Range(_)
            | exec::ExecNodeSecondarySetPlan::OrderedIntersect { .. } => Ok(false),
            exec::ExecNodeSecondarySetPlan::DynamicEquality { param, .. } => {
                let Ok(value) = self.param_value(param) else {
                    return Ok(false);
                };
                Ok(matches!(
                    equality_index_value::project_equality_value(&value),
                    equality_index_value::EqualityValueProjection::Indexed(_)
                        | equality_index_value::EqualityValueProjection::NonReflexive
                ))
            }
            exec::ExecNodeSecondarySetPlan::DynamicMembership { values, .. } => {
                let Ok(domain) = self.runtime_equality_domain(values) else {
                    return Ok(false);
                };
                Ok(matches!(
                    domain,
                    super::membership::RuntimeEqualityDomain::Indexed(_)
                ))
            }
            exec::ExecNodeSecondarySetPlan::Intersect { driver, rest }
            | exec::ExecNodeSecondarySetPlan::Union { driver, rest } => {
                core::iter::once(driver.as_ref())
                    .chain(rest.iter())
                    .try_fold(true, |served, child| {
                        Ok(served && self.node_secondary_set_is_index_served(child)?)
                    })
            }
        }
    }

    pub(in crate::execution::interpreter) async fn edge_secondary_set_ids(
        &self,
        set: &exec::ExecEdgeSecondarySetPlan,
        limit: Option<properties::PositiveUsize>,
    ) -> Result<Vec<u64>> {
        self.edge_secondary_ids(set, limit, PARALLEL_INDEX_READS)
            .await
            .map(|ids| ids.into_vec(limit))
    }

    fn node_secondary_ids<'a>(
        &'a self,
        set: &'a exec::ExecNodeSecondarySetPlan,
        range_limit: Option<properties::PositiveUsize>,
        reads: NonZeroUsize,
    ) -> BoxFuture<'a, Result<SecondaryIds>> {
        async move {
            self.check_execution_deadline()?;
            match set {
                exec::ExecNodeSecondarySetPlan::Empty => {
                    Ok(SecondaryIds::Unordered(RoaringTreemap::new()))
                }
                exec::ExecNodeSecondarySetPlan::Bitmap(bitmap) => self
                    .node_bitmap(bitmap, reads)
                    .await
                    .map(SecondaryIds::Unordered),
                exec::ExecNodeSecondarySetPlan::UniqueUnion { index, key, values } => {
                    super::super::count::validate_node_equality_index(
                        &index.metadata().index_id,
                        key,
                    )?;
                    let values = values
                        .iter()
                        .map(super::super::count::indexed_value)
                        .collect::<Vec<_>>();
                    let ids = self
                        .lookup_managed_equality_batch(
                            crate::index_lifecycle::IndexElementKind::Node,
                            key,
                            &values,
                            true,
                        )
                        .await?;
                    self.check_execution_deadline()?;
                    Ok(SecondaryIds::Unordered(ids))
                }
                exec::ExecNodeSecondarySetPlan::Unique {
                    lookup,
                    verification,
                } => {
                    let read = self.verified_node_unique_owner(lookup, verification);
                    Ok(SecondaryIds::Unordered(read.await?.into_iter().collect()))
                }
                exec::ExecNodeSecondarySetPlan::AuthoritativeScan(
                    exec::ExecNodeAuthoritativeScanPredicate::NullEquality { key },
                ) => self
                    .null_equality_rows(crate::index_lifecycle::IndexElementKind::Node, key, None)
                    .await
                    .map(SecondaryIds::Unordered),
                exec::ExecNodeSecondarySetPlan::AuthoritativeScan(
                    exec::ExecNodeAuthoritativeScanPredicate::Predicate(predicate),
                ) => {
                    let read = self.scan_element_ids(exec::ElementKeyspace::NodeProperty, None);
                    let ids = read.await?;
                    let mut matches = RoaringTreemap::new();
                    for id in ids {
                        let row =
                            super::super::ExecutionRow::current(super::super::ElementRef::Node(id));
                        if self.eval_predicate(&row, predicate.predicate()).await? {
                            matches.insert(id);
                        }
                    }
                    Ok(SecondaryIds::Unordered(matches))
                }
                exec::ExecNodeSecondarySetPlan::DynamicEquality { index, key, param } => {
                    super::super::count::validate_node_equality_index(&index.index_id, key)?;
                    let value =
                        self.index_value(&helix_planner::ir::IndexValue::Param(param.clone()))?;
                    self.lookup_managed_equality_union(
                        crate::index_lifecycle::IndexElementKind::Node,
                        key,
                        core::slice::from_ref(&value),
                    )
                    .await
                    .map(SecondaryIds::Unordered)
                }
                exec::ExecNodeSecondarySetPlan::DynamicMembership { index, key, values } => {
                    super::super::count::validate_node_equality_index(&index.index_id, key)?;
                    self.dynamic_membership_ids(
                        crate::index_lifecycle::IndexElementKind::Node,
                        key,
                        values,
                        reads,
                    )
                    .await
                    .map(SecondaryIds::Unordered)
                }
                exec::ExecNodeSecondarySetPlan::Range(range) => self
                    .range_index_ids(
                        crate::index_lifecycle::IndexElementKind::Node,
                        &range.key,
                        &range.range,
                        range.iteration,
                        &[],
                        range_limit,
                    )
                    .await
                    .map(SecondaryIds::Ordered),
                exec::ExecNodeSecondarySetPlan::Intersect { driver, rest } => {
                    // Null equalities verify only the rows the other children
                    // keep, so those children are read first.
                    let (nulls, others): (Vec<_>, Vec<_>) = core::iter::once(driver.as_ref())
                        .chain(rest.iter())
                        .partition(|child| {
                            matches!(
                                child,
                                exec::ExecNodeSecondarySetPlan::AuthoritativeScan(
                                    exec::ExecNodeAuthoritativeScanPredicate::NullEquality { .. }
                                )
                            )
                        });
                    let mut ids = if others.is_empty() {
                        None
                    } else {
                        Some(
                            intersection(
                                self.read_children(others, reads, |child, reads| {
                                    self.node_secondary_ids(child, None, reads)
                                })
                                .map_ok(SecondaryIds::into_bitmap),
                            )
                            .await?,
                        )
                    };
                    for null in nulls {
                        let exec::ExecNodeSecondarySetPlan::AuthoritativeScan(
                            exec::ExecNodeAuthoritativeScanPredicate::NullEquality { key },
                        ) = null
                        else {
                            unreachable!("partitioned null equalities");
                        };
                        if ids.as_ref().is_some_and(RoaringTreemap::is_empty) {
                            break;
                        }
                        ids = Some(
                            self.null_equality_rows(
                                crate::index_lifecycle::IndexElementKind::Node,
                                key,
                                ids.as_ref(),
                            )
                            .await?,
                        );
                    }
                    Ok(SecondaryIds::Unordered(ids.unwrap_or_default()))
                }
                exec::ExecNodeSecondarySetPlan::Union { driver, rest } => union(
                    self.read_children(
                        core::iter::once(driver.as_ref())
                            .chain(rest.iter())
                            .collect(),
                        reads,
                        |child, reads| self.node_secondary_ids(child, None, reads),
                    )
                    .map_ok(SecondaryIds::into_bitmap),
                )
                .await
                .map(SecondaryIds::Unordered),
                exec::ExecNodeSecondarySetPlan::OrderedIntersect { driver, filters } => {
                    let allowed = self
                        .node_secondary_filter_intersection(filters, reads)
                        .await?;
                    self.range_index_ids(
                        crate::index_lifecycle::IndexElementKind::Node,
                        &driver.key,
                        &driver.range,
                        driver.iteration,
                        core::slice::from_ref(&allowed),
                        range_limit,
                    )
                    .await
                    .map(SecondaryIds::Ordered)
                }
            }
        }
        .boxed()
    }

    fn edge_secondary_ids<'a>(
        &'a self,
        set: &'a exec::ExecEdgeSecondarySetPlan,
        range_limit: Option<properties::PositiveUsize>,
        reads: NonZeroUsize,
    ) -> BoxFuture<'a, Result<SecondaryIds>> {
        async move {
            self.check_execution_deadline()?;
            match set {
                exec::ExecEdgeSecondarySetPlan::Empty => {
                    Ok(SecondaryIds::Unordered(RoaringTreemap::new()))
                }
                exec::ExecEdgeSecondarySetPlan::Bitmap(bitmap) => self
                    .edge_bitmap(bitmap, reads)
                    .await
                    .map(SecondaryIds::Unordered),
                exec::ExecEdgeSecondarySetPlan::AuthoritativeScan(
                    exec::ExecEdgeAuthoritativeScanPredicate::NullEquality { key },
                ) => self
                    .null_equality_rows(crate::index_lifecycle::IndexElementKind::Edge, key, None)
                    .await
                    .map(SecondaryIds::Unordered),
                exec::ExecEdgeSecondarySetPlan::AuthoritativeScan(
                    exec::ExecEdgeAuthoritativeScanPredicate::Predicate(predicate),
                ) => {
                    let read = self.scan_element_ids(exec::ElementKeyspace::EdgeEndpoints, None);
                    let ids = read.await?;
                    let mut matches = RoaringTreemap::new();
                    for id in ids {
                        let row =
                            super::super::ExecutionRow::current(super::super::ElementRef::Edge(id));
                        if self.eval_predicate(&row, predicate.predicate()).await? {
                            matches.insert(id);
                        }
                    }
                    Ok(SecondaryIds::Unordered(matches))
                }
                exec::ExecEdgeSecondarySetPlan::DynamicEquality { index, key, param } => {
                    super::super::count::validate_edge_equality_index(&index.index_id, key)?;
                    let value =
                        self.index_value(&helix_planner::ir::IndexValue::Param(param.clone()))?;
                    self.lookup_managed_equality_union(
                        crate::index_lifecycle::IndexElementKind::Edge,
                        key,
                        core::slice::from_ref(&value),
                    )
                    .await
                    .map(SecondaryIds::Unordered)
                }
                exec::ExecEdgeSecondarySetPlan::DynamicMembership { index, key, values } => {
                    super::super::count::validate_edge_equality_index(&index.index_id, key)?;
                    self.dynamic_membership_ids(
                        crate::index_lifecycle::IndexElementKind::Edge,
                        key,
                        values,
                        reads,
                    )
                    .await
                    .map(SecondaryIds::Unordered)
                }
                exec::ExecEdgeSecondarySetPlan::Range(range) => self
                    .range_index_ids(
                        crate::index_lifecycle::IndexElementKind::Edge,
                        &range.key,
                        &range.range,
                        range.iteration,
                        &[],
                        range_limit,
                    )
                    .await
                    .map(SecondaryIds::Ordered),
                exec::ExecEdgeSecondarySetPlan::Intersect { driver, rest } => {
                    // Null equalities verify only the rows the other children
                    // keep, so those children are read first.
                    let (nulls, others): (Vec<_>, Vec<_>) = core::iter::once(driver.as_ref())
                        .chain(rest.iter())
                        .partition(|child| {
                            matches!(
                                child,
                                exec::ExecEdgeSecondarySetPlan::AuthoritativeScan(
                                    exec::ExecEdgeAuthoritativeScanPredicate::NullEquality { .. }
                                )
                            )
                        });
                    let mut ids = if others.is_empty() {
                        None
                    } else {
                        Some(
                            intersection(
                                self.read_children(others, reads, |child, reads| {
                                    self.edge_secondary_ids(child, None, reads)
                                })
                                .map_ok(SecondaryIds::into_bitmap),
                            )
                            .await?,
                        )
                    };
                    for null in nulls {
                        let exec::ExecEdgeSecondarySetPlan::AuthoritativeScan(
                            exec::ExecEdgeAuthoritativeScanPredicate::NullEquality { key },
                        ) = null
                        else {
                            unreachable!("partitioned null equalities");
                        };
                        if ids.as_ref().is_some_and(RoaringTreemap::is_empty) {
                            break;
                        }
                        ids = Some(
                            self.null_equality_rows(
                                crate::index_lifecycle::IndexElementKind::Edge,
                                key,
                                ids.as_ref(),
                            )
                            .await?,
                        );
                    }
                    Ok(SecondaryIds::Unordered(ids.unwrap_or_default()))
                }
                exec::ExecEdgeSecondarySetPlan::Union { driver, rest } => union(
                    self.read_children(
                        core::iter::once(driver.as_ref())
                            .chain(rest.iter())
                            .collect(),
                        reads,
                        |child, reads| self.edge_secondary_ids(child, None, reads),
                    )
                    .map_ok(SecondaryIds::into_bitmap),
                )
                .await
                .map(SecondaryIds::Unordered),
                exec::ExecEdgeSecondarySetPlan::OrderedIntersect { driver, filters } => {
                    let allowed = self
                        .edge_secondary_filter_intersection(filters, reads)
                        .await?;
                    self.range_index_ids(
                        crate::index_lifecycle::IndexElementKind::Edge,
                        &driver.key,
                        &driver.range,
                        driver.iteration,
                        core::slice::from_ref(&allowed),
                        range_limit,
                    )
                    .await
                    .map(SecondaryIds::Ordered)
                }
            }
        }
        .boxed()
    }
}
