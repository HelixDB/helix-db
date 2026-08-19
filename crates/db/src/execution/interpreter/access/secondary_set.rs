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

enum SecondaryIds {
    Unordered(roaring::RoaringTreemap),
    Ordered(Vec<u64>),
}

impl SecondaryIds {
    fn into_bitmap(self) -> roaring::RoaringTreemap {
        match self {
            Self::Unordered(ids) => ids,
            Self::Ordered(ids) => roaring::RoaringTreemap::from_iter(ids),
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
                exec::ExecNodeSecondarySetPlan::Empty => {
                    Ok(SecondaryIds::Unordered(roaring::RoaringTreemap::new()))
                }
                exec::ExecNodeSecondarySetPlan::Bitmap(bitmap) => {
                    self.node_bitmap(bitmap).await.map(SecondaryIds::Unordered)
                }
                exec::ExecNodeSecondarySetPlan::Unique {
                    lookup,
                    verification,
                } => {
                    let read = self.verified_node_unique_owner(lookup, verification);
                    Ok(SecondaryIds::Unordered(read.await?.into_iter().collect()))
                }
                exec::ExecNodeSecondarySetPlan::AuthoritativeScan(predicate) => {
                    let read = self.scan_element_ids(exec::ElementKeyspace::NodeProperty, None);
                    let ids = read.await?;
                    let mut matches = roaring::RoaringTreemap::new();
                    for id in ids {
                        let row =
                            super::super::ExecutionRow::current(super::super::ElementRef::Node(id));
                        let accepted = match predicate {
                            exec::ExecNodeAuthoritativeScanPredicate::NullEquality { key } => {
                                self.scoped_null_matches(&row, key).await?
                            }
                            exec::ExecNodeAuthoritativeScanPredicate::Predicate(predicate) => {
                                self.eval_predicate(&row, predicate.predicate()).await?
                            }
                        };
                        if accepted {
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
                exec::ExecNodeSecondarySetPlan::Range(range) => self
                    .node_range_index_ids(&range.key, &range.range, range_limit)
                    .await
                    .map(SecondaryIds::Ordered),
                exec::ExecNodeSecondarySetPlan::Intersect { driver, rest } => {
                    let mut ids = self.node_secondary_ids(driver, None).await?.into_bitmap();
                    for child in rest {
                        ids &= self.node_secondary_ids(child, None).await?.into_bitmap();
                    }
                    Ok(SecondaryIds::Unordered(ids))
                }
                exec::ExecNodeSecondarySetPlan::Union { driver, rest } => {
                    let mut ids = self.node_secondary_ids(driver, None).await?.into_bitmap();
                    for child in rest {
                        ids |= self.node_secondary_ids(child, None).await?.into_bitmap();
                    }
                    Ok(SecondaryIds::Unordered(ids))
                }
                exec::ExecNodeSecondarySetPlan::OrderedIntersect { driver, filters } => {
                    let mut filters = filters.iter();
                    let first = filters
                        .next()
                        .expect("ordered intersection has at least one filter");
                    let read = self.node_secondary_ids(first, None);
                    let mut allowed = read.await?.into_bitmap();
                    for filter in filters {
                        allowed &= self.node_secondary_ids(filter, None).await?.into_bitmap();
                    }
                    let read = self.node_range_index_ids(&driver.key, &driver.range, None);
                    let ordered = read
                        .await?
                        .into_iter()
                        .filter(|id| allowed.contains(*id))
                        .collect();
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
                exec::ExecEdgeSecondarySetPlan::Empty => {
                    Ok(SecondaryIds::Unordered(roaring::RoaringTreemap::new()))
                }
                exec::ExecEdgeSecondarySetPlan::Bitmap(bitmap) => {
                    self.edge_bitmap(bitmap).await.map(SecondaryIds::Unordered)
                }
                exec::ExecEdgeSecondarySetPlan::AuthoritativeScan(predicate) => {
                    let read = self.scan_element_ids(exec::ElementKeyspace::EdgeEndpoints, None);
                    let ids = read.await?;
                    let mut matches = roaring::RoaringTreemap::new();
                    for id in ids {
                        let row =
                            super::super::ExecutionRow::current(super::super::ElementRef::Edge(id));
                        let accepted = match predicate {
                            exec::ExecEdgeAuthoritativeScanPredicate::NullEquality { key } => {
                                self.scoped_null_matches(&row, key).await?
                            }
                            exec::ExecEdgeAuthoritativeScanPredicate::Predicate(predicate) => {
                                self.eval_predicate(&row, predicate.predicate()).await?
                            }
                        };
                        if accepted {
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
                exec::ExecEdgeSecondarySetPlan::Range(range) => self
                    .edge_range_index_ids(&range.key, &range.range, range_limit)
                    .await
                    .map(SecondaryIds::Ordered),
                exec::ExecEdgeSecondarySetPlan::Intersect { driver, rest } => {
                    let mut ids = self.edge_secondary_ids(driver, None).await?.into_bitmap();
                    for child in rest {
                        ids &= self.edge_secondary_ids(child, None).await?.into_bitmap();
                    }
                    Ok(SecondaryIds::Unordered(ids))
                }
                exec::ExecEdgeSecondarySetPlan::Union { driver, rest } => {
                    let mut ids = self.edge_secondary_ids(driver, None).await?.into_bitmap();
                    for child in rest {
                        ids |= self.edge_secondary_ids(child, None).await?.into_bitmap();
                    }
                    Ok(SecondaryIds::Unordered(ids))
                }
                exec::ExecEdgeSecondarySetPlan::OrderedIntersect { driver, filters } => {
                    let mut filters = filters.iter();
                    let first = filters
                        .next()
                        .expect("ordered intersection has at least one filter");
                    let read = self.edge_secondary_ids(first, None);
                    let mut allowed = read.await?.into_bitmap();
                    for filter in filters {
                        allowed &= self.edge_secondary_ids(filter, None).await?.into_bitmap();
                    }
                    let read = self.edge_range_index_ids(&driver.key, &driver.range, None);
                    let ordered = read
                        .await?
                        .into_iter()
                        .filter(|id| allowed.contains(*id))
                        .collect();
                    Ok(SecondaryIds::Ordered(ordered))
                }
            }
        }
        .boxed()
    }
}
