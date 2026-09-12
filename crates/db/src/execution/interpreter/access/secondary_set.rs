//! V2-aware managed secondary-index ID-set execution.
//!
//! The planner supplies logical identities only. This module resolves them
//! through the request-authorized Active catalog, combines verified IDs, and
//! preserves an ordered range driver until all filters have been applied.

use futures::future::BoxFuture;
use futures::FutureExt;
use helix_planner::{exec, properties};

use super::super::ExecutionContext;
use crate::error::Result;
use crate::query_resources::{self, bitmap};

enum SecondaryIds {
    Unordered(bitmap::Bitmap),
    Ordered(Vec<u64>),
}

impl SecondaryIds {
    fn into_bitmap(self, budget: Option<&query_resources::Budget>) -> Result<bitmap::Bitmap> {
        match self {
            Self::Unordered(ids) => Ok(ids),
            Self::Ordered(ids) => {
                bitmap::Bitmap::retain_legacy(roaring::RoaringTreemap::from_iter(ids), budget)
            }
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
    pub(super) async fn node_secondary_set_ids(
        &self,
        set: &exec::ExecNodeSecondarySetPlan,
        limit: Option<properties::PositiveUsize>,
    ) -> Result<Vec<u64>> {
        self.node_secondary_ids(set, limit)
            .await
            .map(|ids| ids.into_vec(limit))
    }

    pub(super) async fn edge_secondary_set_ids(
        &self,
        set: &exec::ExecEdgeSecondarySetPlan,
        limit: Option<properties::PositiveUsize>,
    ) -> Result<Vec<u64>> {
        self.edge_secondary_ids(set, limit)
            .await
            .map(|ids| ids.into_vec(limit))
    }

    fn node_secondary_ids<'a>(
        &'a self,
        set: &'a exec::ExecNodeSecondarySetPlan,
        range_limit: Option<properties::PositiveUsize>,
    ) -> BoxFuture<'a, Result<SecondaryIds>> {
        async move {
            self.check_execution_deadline()?;
            match set {
                exec::ExecNodeSecondarySetPlan::Empty => Ok(SecondaryIds::Unordered(
                    bitmap::Bitmap::empty(self.row_memory.as_ref())?,
                )),
                exec::ExecNodeSecondarySetPlan::Bitmap(bitmap) => {
                    self.node_bitmap(bitmap).await.map(SecondaryIds::Unordered)
                }
                exec::ExecNodeSecondarySetPlan::Unique {
                    lookup,
                    verification,
                } => {
                    let read = self.verified_node_unique_owner(lookup, verification);
                    Ok(SecondaryIds::Unordered(match read.await? {
                        Some(id) => bitmap::Bitmap::singleton(id, self.row_memory.as_ref())?,
                        None => bitmap::Bitmap::empty(self.row_memory.as_ref())?,
                    }))
                }
                exec::ExecNodeSecondarySetPlan::AuthoritativeScan(predicate) => {
                    let read = self.scan_element_ids(exec::ElementKeyspace::NodeProperty, None);
                    let ids = read.await?;
                    let mut matches = bitmap::Builder::new(self.row_memory.as_ref())?;
                    for id in ids {
                        let row =
                            super::super::ExecutionRow::current(super::super::ElementRef::Node(id));
                        let accepted = match predicate {
                            exec::ExecNodeAuthoritativeScanPredicate::NullEquality { key } => {
                                self.scoped_null_matches(&row, key).await?
                            }
                            exec::ExecNodeAuthoritativeScanPredicate::Predicate(predicate) => {
                                self.eval_predicate_plan(&row, predicate).await?
                            }
                        };
                        if accepted {
                            matches.insert(id)?;
                        }
                    }
                    Ok(SecondaryIds::Unordered(matches.finish()))
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
                    let mut ids = self
                        .node_secondary_ids(driver, None)
                        .await?
                        .into_bitmap(self.row_memory.as_ref())?;
                    for child in rest {
                        ids = ids.intersect(
                            self.node_secondary_ids(child, None)
                                .await?
                                .into_bitmap(self.row_memory.as_ref())?,
                        )?;
                    }
                    Ok(SecondaryIds::Unordered(ids))
                }
                exec::ExecNodeSecondarySetPlan::Union { driver, rest } => {
                    let mut ids = self
                        .node_secondary_ids(driver, None)
                        .await?
                        .into_bitmap(self.row_memory.as_ref())?;
                    for child in rest {
                        ids = ids.union(
                            self.node_secondary_ids(child, None)
                                .await?
                                .into_bitmap(self.row_memory.as_ref())?,
                        )?;
                    }
                    Ok(SecondaryIds::Unordered(ids))
                }
                exec::ExecNodeSecondarySetPlan::OrderedIntersect { driver, filters } => {
                    let mut filters = filters.iter();
                    let first = filters
                        .next()
                        .expect("ordered intersection has at least one filter");
                    let read = self.node_secondary_ids(first, None);
                    let mut allowed = read.await?.into_bitmap(self.row_memory.as_ref())?;
                    for filter in filters {
                        allowed = allowed.intersect(
                            self.node_secondary_ids(filter, None)
                                .await?
                                .into_bitmap(self.row_memory.as_ref())?,
                        )?;
                    }
                    let ordered = self
                        .range_index_ids(
                            crate::index_lifecycle::IndexElementKind::Node,
                            &driver.key,
                            &driver.range,
                            driver.iteration,
                            &[&allowed],
                            range_limit,
                        )
                        .await?;
                    Ok(SecondaryIds::Ordered(ordered))
                }
            }
        }
        .boxed()
    }

    fn edge_secondary_ids<'a>(
        &'a self,
        set: &'a exec::ExecEdgeSecondarySetPlan,
        range_limit: Option<properties::PositiveUsize>,
    ) -> BoxFuture<'a, Result<SecondaryIds>> {
        async move {
            self.check_execution_deadline()?;
            match set {
                exec::ExecEdgeSecondarySetPlan::Empty => Ok(SecondaryIds::Unordered(
                    bitmap::Bitmap::empty(self.row_memory.as_ref())?,
                )),
                exec::ExecEdgeSecondarySetPlan::Bitmap(bitmap) => {
                    self.edge_bitmap(bitmap).await.map(SecondaryIds::Unordered)
                }
                exec::ExecEdgeSecondarySetPlan::AuthoritativeScan(predicate) => {
                    let read = self.scan_element_ids(exec::ElementKeyspace::EdgeEndpoints, None);
                    let ids = read.await?;
                    let mut matches = bitmap::Builder::new(self.row_memory.as_ref())?;
                    for id in ids {
                        let row =
                            super::super::ExecutionRow::current(super::super::ElementRef::Edge(id));
                        let accepted = match predicate {
                            exec::ExecEdgeAuthoritativeScanPredicate::NullEquality { key } => {
                                self.scoped_null_matches(&row, key).await?
                            }
                            exec::ExecEdgeAuthoritativeScanPredicate::Predicate(predicate) => {
                                self.eval_predicate_plan(&row, predicate).await?
                            }
                        };
                        if accepted {
                            matches.insert(id)?;
                        }
                    }
                    Ok(SecondaryIds::Unordered(matches.finish()))
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
                    let mut ids = self
                        .edge_secondary_ids(driver, None)
                        .await?
                        .into_bitmap(self.row_memory.as_ref())?;
                    for child in rest {
                        ids = ids.intersect(
                            self.edge_secondary_ids(child, None)
                                .await?
                                .into_bitmap(self.row_memory.as_ref())?,
                        )?;
                    }
                    Ok(SecondaryIds::Unordered(ids))
                }
                exec::ExecEdgeSecondarySetPlan::Union { driver, rest } => {
                    let mut ids = self
                        .edge_secondary_ids(driver, None)
                        .await?
                        .into_bitmap(self.row_memory.as_ref())?;
                    for child in rest {
                        ids = ids.union(
                            self.edge_secondary_ids(child, None)
                                .await?
                                .into_bitmap(self.row_memory.as_ref())?,
                        )?;
                    }
                    Ok(SecondaryIds::Unordered(ids))
                }
                exec::ExecEdgeSecondarySetPlan::OrderedIntersect { driver, filters } => {
                    let mut filters = filters.iter();
                    let first = filters
                        .next()
                        .expect("ordered intersection has at least one filter");
                    let read = self.edge_secondary_ids(first, None);
                    let mut allowed = read.await?.into_bitmap(self.row_memory.as_ref())?;
                    for filter in filters {
                        allowed = allowed.intersect(
                            self.edge_secondary_ids(filter, None)
                                .await?
                                .into_bitmap(self.row_memory.as_ref())?,
                        )?;
                    }
                    let ordered = self
                        .range_index_ids(
                            crate::index_lifecycle::IndexElementKind::Edge,
                            &driver.key,
                            &driver.range,
                            driver.iteration,
                            &[&allowed],
                            range_limit,
                        )
                        .await?;
                    Ok(SecondaryIds::Ordered(ordered))
                }
            }
        }
        .boxed()
    }
}
