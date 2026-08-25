//! Range-index executable access contracts.
//!
//! This module owns executable node/edge range-index scans, dynamic bound
//! evaluation, and the storage-facing direction mapping. Configured indexes
//! keep their deployed path. A present canonical V2 identity must be Active,
//! lease-revalidated, and scanned through its generation-qualified rows.

use helix_planner::{catalog, ir, properties};
use slatedb::DbReadOps;

use super::super::stream::ast_to_db_value;
use super::super::*;
use crate::encoding::indexes::range::RangeIndexDirection as StorageRangeIndexDirection;
#[cfg(test)]
use crate::HelixStorage;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn node_range_index_count_with_membership(
        &self,
        key: &catalog::ScopedPropertyDirectionKey,
        range: &ir::IndexRange,
        membership: &[roaring::RoaringTreemap],
        limit: Option<usize>,
    ) -> Result<usize> {
        self.range_index_count_with_membership(
            crate::index_lifecycle::IndexElementKind::Node,
            key,
            range,
            membership,
            limit,
        )
        .await
    }

    pub(in crate::execution::interpreter) async fn edge_range_index_count_with_membership(
        &self,
        key: &catalog::ScopedPropertyDirectionKey,
        range: &ir::IndexRange,
        membership: &[roaring::RoaringTreemap],
        limit: Option<usize>,
    ) -> Result<usize> {
        self.range_index_count_with_membership(
            crate::index_lifecycle::IndexElementKind::Edge,
            key,
            range,
            membership,
            limit,
        )
        .await
    }

    async fn range_index_count_with_membership(
        &self,
        element_kind: crate::index_lifecycle::IndexElementKind,
        key: &catalog::ScopedPropertyDirectionKey,
        range: &ir::IndexRange,
        membership: &[roaring::RoaringTreemap],
        limit: Option<usize>,
    ) -> Result<usize> {
        let direction = storage_range_direction(key.direction);
        let query = range_query(self, range)?;
        let identity =
            secondary_range_identity(element_kind, key.label.as_ref(), key.property.as_ref())?;
        if let Some(active) = self.active_write_tx() {
            return count_range_with_membership_in_view(
                self,
                &active.txn,
                &identity,
                &query,
                direction,
                limit,
                membership,
            )
            .await;
        }
        if let Some(view) = self.request_read_view() {
            return count_range_with_membership_in_view(
                self, view, &identity, &query, direction, limit, membership,
            )
            .await;
        }
        Err(HelixDbError::InvariantViolation(
            "filtered secondary range lookup escaped its request read view".to_string(),
        ))
    }

    pub(in crate::execution::interpreter) async fn node_range_index_ids(
        &self,
        key: &catalog::ScopedPropertyDirectionKey,
        range: &ir::IndexRange,
        limit: Option<properties::PositiveUsize>,
    ) -> Result<Vec<u64>> {
        let direction = storage_range_direction(key.direction);
        let limit = limit.map(|limit| limit.get());
        let query = range_query(self, range)?;
        let identity = secondary_range_identity(
            crate::index_lifecycle::IndexElementKind::Node,
            key.label.as_ref(),
            key.property.as_ref(),
        )?;
        if let Some(active) = self.active_write_tx() {
            return scan_node_range_in_view(self, &active.txn, &identity, &query, direction, limit)
                .await;
        }
        if let Some(view) = self.request_read_view() {
            return scan_node_range_in_view(self, view, &identity, &query, direction, limit).await;
        }
        #[cfg(test)]
        {
            match self.db.storage() {
                HelixStorage::Reader(reader) => {
                    scan_node_range_in_view(
                        self,
                        reader.as_ref(),
                        &identity,
                        &query,
                        direction,
                        limit,
                    )
                    .await
                }
                HelixStorage::Writer(writer) => {
                    scan_node_range_in_view(self, writer.db(), &identity, &query, direction, limit)
                        .await
                }
            }
        }
        #[cfg(not(test))]
        Err(HelixDbError::InvariantViolation(
            "node secondary range lookup escaped its request read view".to_string(),
        ))
    }

    pub(in crate::execution::interpreter) async fn edge_range_index_ids(
        &self,
        key: &catalog::ScopedPropertyDirectionKey,
        range: &ir::IndexRange,
        limit: Option<properties::PositiveUsize>,
    ) -> Result<Vec<u64>> {
        let direction = storage_range_direction(key.direction);
        let limit = limit.map(|limit| limit.get());
        let query = range_query(self, range)?;
        let identity = secondary_range_identity(
            crate::index_lifecycle::IndexElementKind::Edge,
            key.label.as_ref(),
            key.property.as_ref(),
        )?;
        if let Some(active) = self.active_write_tx() {
            return scan_edge_range_in_view(self, &active.txn, &identity, &query, direction, limit)
                .await;
        }
        if let Some(view) = self.request_read_view() {
            return scan_edge_range_in_view(self, view, &identity, &query, direction, limit).await;
        }
        #[cfg(test)]
        {
            match self.db.storage() {
                HelixStorage::Reader(reader) => {
                    scan_edge_range_in_view(
                        self,
                        reader.as_ref(),
                        &identity,
                        &query,
                        direction,
                        limit,
                    )
                    .await
                }
                HelixStorage::Writer(writer) => {
                    scan_edge_range_in_view(self, writer.db(), &identity, &query, direction, limit)
                        .await
                }
            }
        }
        #[cfg(not(test))]
        Err(HelixDbError::InvariantViolation(
            "edge secondary range lookup escaped its request read view".to_string(),
        ))
    }
}

/// Constructs the direction-independent identity for one range index.
///
/// Direction remains part of the validated definition carried by the Active
/// handle and is checked against the planner request before physical I/O.
fn secondary_range_identity(
    element_kind: crate::index_lifecycle::IndexElementKind,
    label: &str,
    property: &str,
) -> Result<crate::index_lifecycle::IndexIdentity> {
    Ok(crate::index_lifecycle::IndexIdentity::new(
        crate::index_lifecycle::IndexIdentityFamily::SecondaryRange,
        element_kind,
        crate::index_lifecycle::IndexComponent::try_new("label", label)?,
        crate::index_lifecycle::IndexComponent::try_new("property", property)?,
    ))
}

/// Routes node range access through the exact canonical V2 identity.
async fn scan_node_range_in_view(
    context: &ExecutionContext<'_>,
    reader: &(impl DbReadOps + Send + Sync),
    identity: &crate::index_lifecycle::IndexIdentity,
    query: &OwnedRangeQuery,
    direction: StorageRangeIndexDirection,
    limit: Option<usize>,
) -> Result<Vec<u64>> {
    let managed_query = match query {
        OwnedRangeQuery::All => None,
        OwnedRangeQuery::Bounded(query) => Some(query),
    };
    scan_managed_range_in_view(context, reader, identity, managed_query, direction, limit).await
}

/// Routes edge range access through the exact canonical V2 identity.
async fn scan_edge_range_in_view(
    context: &ExecutionContext<'_>,
    reader: &(impl DbReadOps + Send + Sync),
    identity: &crate::index_lifecycle::IndexIdentity,
    query: &OwnedRangeQuery,
    direction: StorageRangeIndexDirection,
    limit: Option<usize>,
) -> Result<Vec<u64>> {
    let managed_query = match query {
        OwnedRangeQuery::All => None,
        OwnedRangeQuery::Bounded(query) => Some(query),
    };
    scan_managed_range_in_view(context, reader, identity, managed_query, direction, limit).await
}

/// Resolves, leases, and scans a present canonical range identity.
///
/// An absent/non-Active record or direction mismatch fails closed.
async fn scan_managed_range_in_view(
    context: &ExecutionContext<'_>,
    reader: &(impl DbReadOps + Send + Sync),
    identity: &crate::index_lifecycle::IndexIdentity,
    query: Option<&crate::index_lifecycle::secondary::SecondaryRangeQuery>,
    requested_direction: StorageRangeIndexDirection,
    limit: Option<usize>,
) -> Result<Vec<u64>> {
    let active =
        active_range_handle_in_view(context, reader, identity, requested_direction).await?;
    crate::index_lifecycle::secondary::scan_active_range_generation(reader, &active, query, limit)
        .await
}

async fn active_range_handle_in_view(
    context: &ExecutionContext<'_>,
    reader: &(impl DbReadOps + Send + Sync),
    identity: &crate::index_lifecycle::IndexIdentity,
    requested_direction: StorageRangeIndexDirection,
) -> Result<crate::index_lifecycle::ActiveIndexHandle> {
    let Some(record) = crate::index_lifecycle::repository::load_index_record(
        reader,
        context.tenant_scope,
        identity,
    )
    .await?
    else {
        return Err(HelixDbError::IndexLifecycleUnavailable {
            family: crate::error::IndexFamily::Secondary,
            reason: crate::error::IndexLifecycleUnavailableReason::CanonicalStateUnavailable,
        });
    };
    let Some(active) =
        crate::index_lifecycle::ActiveIndexHandle::try_from_record(context.tenant_scope, &record)
    else {
        return Err(HelixDbError::IndexLifecycleUnavailable {
            family: crate::error::IndexFamily::Secondary,
            reason: crate::error::IndexLifecycleUnavailableReason::CanonicalStateUnavailable,
        });
    };
    let Some(definition) = active.secondary_definition() else {
        return Err(HelixDbError::IndexCatalogCorruption(
            "secondary range identity resolved another Active family".to_string(),
        ));
    };
    let configured_direction = match definition.direction() {
        crate::config::RangeIndexDirection::Asc => StorageRangeIndexDirection::Asc,
        crate::config::RangeIndexDirection::Desc => StorageRangeIndexDirection::Desc,
    };
    if configured_direction != requested_direction {
        return Err(HelixDbError::IndexCatalogCorruption(
            "planner range direction disagrees with its Active secondary definition".to_string(),
        ));
    }
    Ok(active)
}

async fn count_range_with_membership_in_view(
    context: &ExecutionContext<'_>,
    reader: &(impl DbReadOps + Send + Sync),
    identity: &crate::index_lifecycle::IndexIdentity,
    query: &OwnedRangeQuery,
    requested_direction: StorageRangeIndexDirection,
    limit: Option<usize>,
    membership: &[roaring::RoaringTreemap],
) -> Result<usize> {
    let managed_query = match query {
        OwnedRangeQuery::All => None,
        OwnedRangeQuery::Bounded(query) => Some(query),
    };
    let active =
        active_range_handle_in_view(context, reader, identity, requested_direction).await?;
    crate::index_lifecycle::secondary::count_active_range_generation_with_membership(
        reader,
        &active,
        managed_query,
        limit,
        membership,
    )
    .await
}

enum OwnedRangeQuery {
    All,
    Bounded(crate::index_lifecycle::secondary::SecondaryRangeQuery),
}

fn range_query(ctx: &ExecutionContext<'_>, range: &ir::IndexRange) -> Result<OwnedRangeQuery> {
    match range {
        ir::IndexRange::All => Ok(OwnedRangeQuery::All),
        ir::IndexRange::Lower { lower } => {
            let value = range_value(ctx, bound_value(lower))?;
            Ok(OwnedRangeQuery::Bounded(
                crate::index_lifecycle::secondary::SecondaryRangeQuery::Lower {
                    value,
                    inclusive: bound_is_inclusive(lower),
                },
            ))
        }
        ir::IndexRange::Upper { upper } => {
            let value = range_value(ctx, bound_value(upper))?;
            Ok(OwnedRangeQuery::Bounded(
                crate::index_lifecycle::secondary::SecondaryRangeQuery::Upper {
                    value,
                    inclusive: bound_is_inclusive(upper),
                },
            ))
        }
        ir::IndexRange::Between(bounds) => {
            let lower = range_value(ctx, bound_value(bounds.lower()))?;
            let upper = range_value(ctx, bound_value(bounds.upper()))?;
            Ok(OwnedRangeQuery::Bounded(
                crate::index_lifecycle::secondary::SecondaryRangeQuery::Between {
                    lower,
                    lower_inclusive: bound_is_inclusive(bounds.lower()),
                    upper,
                    upper_inclusive: bound_is_inclusive(bounds.upper()),
                },
            ))
        }
    }
}

fn bound_is_inclusive(bound: &ir::IndexBound) -> bool {
    matches!(bound, ir::IndexBound::Inclusive(_))
}

fn bound_value(bound: &ir::IndexBound) -> &ir::RangeIndexValue {
    match bound {
        ir::IndexBound::Inclusive(value) | ir::IndexBound::Exclusive(value) => value,
    }
}

fn range_value(
    ctx: &ExecutionContext<'_>,
    value: &ir::RangeIndexValue,
) -> Result<crate::encoding::property::property_value::PropertyValue> {
    Ok(match value {
        ir::RangeIndexValue::Literal(value) => ast_to_db_value(value.to_property_value()),
        ir::RangeIndexValue::Param(param) => ctx.param_value(param)?,
    })
}

fn storage_range_direction(
    direction: helix_ast::index::RangeIndexDirection,
) -> StorageRangeIndexDirection {
    match direction {
        helix_ast::index::RangeIndexDirection::Asc => StorageRangeIndexDirection::Asc,
        helix_ast::index::RangeIndexDirection::Desc => StorageRangeIndexDirection::Desc,
    }
}
