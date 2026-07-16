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
    pub(super) async fn node_range_index_ids(
        &self,
        key: &catalog::ScopedPropertyDirectionKey,
        range: &ir::IndexRange,
        limit: Option<properties::PositiveUsize>,
    ) -> Result<Vec<u64>> {
        let property = scoped_direction_property_key(key);
        let direction = storage_range_direction(key.direction);
        let limit = limit.map(|limit| limit.get());
        let query = range_query(self, range)?;
        let identity = secondary_range_identity(
            crate::index_v2::IndexElementKind::Node,
            key.label.as_ref(),
            key.property.as_ref(),
        )?;
        if let Some(active) = self.active_write_tx() {
            return scan_node_range_in_view(
                self,
                &active.txn,
                &identity,
                &property,
                &query,
                direction,
                limit,
            )
            .await;
        }
        if let Some(view) = self.request_read_view() {
            return scan_node_range_in_view(
                self, view, &identity, &property, &query, direction, limit,
            )
            .await;
        }
        #[cfg(test)]
        {
            match self.db.storage() {
                HelixStorage::Reader(reader) => {
                    scan_node_range_in_view(
                        self,
                        reader.as_ref(),
                        &identity,
                        &property,
                        &query,
                        direction,
                        limit,
                    )
                    .await
                }
                HelixStorage::Writer(writer) => {
                    scan_node_range_in_view(
                        self,
                        writer.db(),
                        &identity,
                        &property,
                        &query,
                        direction,
                        limit,
                    )
                    .await
                }
            }
        }
        #[cfg(not(test))]
        Err(HelixDbError::InvariantViolation(
            "node secondary range lookup escaped its request read view".to_string(),
        ))
    }

    pub(super) async fn edge_range_index_ids(
        &self,
        key: &catalog::ScopedPropertyDirectionKey,
        range: &ir::IndexRange,
        limit: Option<properties::PositiveUsize>,
    ) -> Result<Vec<u64>> {
        let property = scoped_direction_property_key(key);
        let direction = storage_range_direction(key.direction);
        let limit = limit.map(|limit| limit.get());
        let query = range_query(self, range)?;
        let identity = secondary_range_identity(
            crate::index_v2::IndexElementKind::Edge,
            key.label.as_ref(),
            key.property.as_ref(),
        )?;
        if let Some(active) = self.active_write_tx() {
            return scan_edge_range_in_view(
                self,
                &active.txn,
                &identity,
                &property,
                &query,
                direction,
                limit,
            )
            .await;
        }
        if let Some(view) = self.request_read_view() {
            return scan_edge_range_in_view(
                self, view, &identity, &property, &query, direction, limit,
            )
            .await;
        }
        #[cfg(test)]
        {
            match self.db.storage() {
                HelixStorage::Reader(reader) => {
                    scan_edge_range_in_view(
                        self,
                        reader.as_ref(),
                        &identity,
                        &property,
                        &query,
                        direction,
                        limit,
                    )
                    .await
                }
                HelixStorage::Writer(writer) => {
                    scan_edge_range_in_view(
                        self,
                        writer.db(),
                        &identity,
                        &property,
                        &query,
                        direction,
                        limit,
                    )
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
    element_kind: crate::index_v2::IndexElementKind,
    label: &str,
    property: &str,
) -> Result<crate::index_v2::IndexIdentity> {
    Ok(crate::index_v2::IndexIdentity::new(
        crate::index_v2::IndexIdentityFamily::SecondaryRange,
        element_kind,
        crate::index_v2::IndexComponent::try_new("label", label)?,
        crate::index_v2::IndexComponent::try_new("property", property)?,
    ))
}

/// Routes node range access to V2 only when a canonical identity is present.
async fn scan_node_range_in_view(
    context: &ExecutionContext<'_>,
    reader: &(impl DbReadOps + Send + Sync),
    identity: &crate::index_v2::IndexIdentity,
    property: &str,
    query: &OwnedRangeQuery,
    direction: StorageRangeIndexDirection,
    limit: Option<usize>,
) -> Result<Vec<u64>> {
    let managed_query = match query {
        OwnedRangeQuery::All => None,
        OwnedRangeQuery::Bounded(query) => Some(query.as_borrowed()),
    };
    if let Some(owners) = scan_managed_range_in_view(
        context,
        reader,
        identity,
        managed_query.as_ref(),
        direction,
        limit,
    )
    .await?
    {
        return Ok(owners);
    }
    match query {
        OwnedRangeQuery::All => {
            crate::search::scan_range_index_with_direction_limited_scoped(
                reader,
                property,
                direction,
                limit,
                context.tenant_scope,
            )
            .await
        }
        OwnedRangeQuery::Bounded(query) => {
            crate::search::scan_range_index_bounded_with_direction_limited_scoped(
                reader,
                property,
                query.as_borrowed(),
                direction,
                limit,
                context.tenant_scope,
            )
            .await
        }
    }
}

/// Routes global edge range access to the same V2 generation contract.
async fn scan_edge_range_in_view(
    context: &ExecutionContext<'_>,
    reader: &(impl DbReadOps + Send + Sync),
    identity: &crate::index_v2::IndexIdentity,
    property: &str,
    query: &OwnedRangeQuery,
    direction: StorageRangeIndexDirection,
    limit: Option<usize>,
) -> Result<Vec<u64>> {
    let managed_query = match query {
        OwnedRangeQuery::All => None,
        OwnedRangeQuery::Bounded(query) => Some(query.as_borrowed()),
    };
    if let Some(owners) = scan_managed_range_in_view(
        context,
        reader,
        identity,
        managed_query.as_ref(),
        direction,
        limit,
    )
    .await?
    {
        return Ok(owners);
    }
    match query {
        OwnedRangeQuery::All => {
            crate::search::scan_global_edge_range_index_all_with_direction_limited_scoped(
                reader,
                property,
                direction,
                limit,
                context.tenant_scope,
            )
            .await
        }
        OwnedRangeQuery::Bounded(query) => {
            crate::search::scan_global_edge_range_index_with_direction_limited_scoped(
                reader,
                property,
                query.as_borrowed(),
                direction,
                limit,
                context.tenant_scope,
            )
            .await
        }
    }
}

/// Resolves, leases, and scans a present canonical range identity.
///
/// `None` means no canonical record exists and the configured legacy route may
/// run. A present non-Active record or direction mismatch always fails closed.
async fn scan_managed_range_in_view(
    context: &ExecutionContext<'_>,
    reader: &(impl DbReadOps + Send + Sync),
    identity: &crate::index_v2::IndexIdentity,
    query: Option<&crate::search::RangeQuery<'_>>,
    requested_direction: StorageRangeIndexDirection,
    limit: Option<usize>,
) -> Result<Option<Vec<u64>>> {
    let Some(record) =
        crate::index_v2::repository::load_index_record(reader, context.tenant_scope, identity)
            .await?
    else {
        return Ok(None);
    };
    let Some(active) =
        crate::index_v2::ActiveIndexHandle::try_from_record(context.tenant_scope, &record)
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
    let lease_generation = context.acquire_index_read_lease(reader, &active).await?;
    context
        .run_index_read_batch(
            lease_generation,
            crate::index_v2::secondary::scan_active_range_generation(reader, &active, query, limit),
        )
        .await
        .map(Some)
}

enum OwnedRangeQuery {
    All,
    Bounded(OwnedBoundedRangeQuery),
}

enum OwnedBoundedRangeQuery {
    Lower(OwnedRangeBound),
    Upper(OwnedRangeBound),
    Between(OwnedRangeBetween),
}

struct OwnedRangeBound {
    value: String,
    inclusive: bool,
}

impl OwnedRangeBound {
    fn new(value: String, inclusive: bool) -> Self {
        Self { value, inclusive }
    }
}

struct OwnedRangeBetween {
    lower: OwnedRangeBound,
    upper: OwnedRangeBound,
}

impl OwnedBoundedRangeQuery {
    fn as_borrowed(&self) -> crate::search::RangeQuery<'_> {
        match self {
            Self::Lower(bound) => {
                if bound.inclusive {
                    crate::search::RangeQuery::Gte(&bound.value)
                } else {
                    crate::search::RangeQuery::Gt(&bound.value)
                }
            }
            Self::Upper(bound) => {
                if bound.inclusive {
                    crate::search::RangeQuery::Lte(&bound.value)
                } else {
                    crate::search::RangeQuery::Lt(&bound.value)
                }
            }
            Self::Between(bounds) => crate::search::RangeQuery::BetweenBounds {
                min: &bounds.lower.value,
                min_inclusive: bounds.lower.inclusive,
                max: &bounds.upper.value,
                max_inclusive: bounds.upper.inclusive,
            },
        }
    }
}

fn range_query(ctx: &ExecutionContext<'_>, range: &ir::IndexRange) -> Result<OwnedRangeQuery> {
    match range {
        ir::IndexRange::All => Ok(OwnedRangeQuery::All),
        ir::IndexRange::Lower { lower } => {
            let value = range_value(ctx, bound_value(lower))?;
            Ok(OwnedRangeQuery::Bounded(OwnedBoundedRangeQuery::Lower(
                owned_range_bound(value, lower),
            )))
        }
        ir::IndexRange::Upper { upper } => {
            let value = range_value(ctx, bound_value(upper))?;
            Ok(OwnedRangeQuery::Bounded(OwnedBoundedRangeQuery::Upper(
                owned_range_bound(value, upper),
            )))
        }
        ir::IndexRange::Between(bounds) => {
            let lower = range_value(ctx, bound_value(bounds.lower()))?;
            let upper = range_value(ctx, bound_value(bounds.upper()))?;
            Ok(OwnedRangeQuery::Bounded(OwnedBoundedRangeQuery::Between(
                OwnedRangeBetween {
                    lower: owned_range_bound(lower, bounds.lower()),
                    upper: owned_range_bound(upper, bounds.upper()),
                },
            )))
        }
    }
}

fn owned_range_bound(value: String, bound: &ir::IndexBound) -> OwnedRangeBound {
    OwnedRangeBound::new(
        value,
        match bound {
            ir::IndexBound::Inclusive(_) => true,
            ir::IndexBound::Exclusive(_) => false,
        },
    )
}

fn bound_value(bound: &ir::IndexBound) -> &ir::RangeIndexValue {
    match bound {
        ir::IndexBound::Inclusive(value) | ir::IndexBound::Exclusive(value) => value,
    }
}

fn range_value(ctx: &ExecutionContext<'_>, value: &ir::RangeIndexValue) -> Result<String> {
    let value = match value {
        ir::RangeIndexValue::Literal(value) => ast_to_db_value(value.to_property_value()),
        ir::RangeIndexValue::Param(param) => ctx.param_value(param)?,
    };
    Ok(crate::search::property_value_to_index_string(&value))
}

fn scoped_direction_property_key(key: &catalog::ScopedPropertyDirectionKey) -> String {
    crate::config::scoped_secondary_index_property(key.label.as_ref(), key.property.as_ref())
}

fn storage_range_direction(
    direction: helix_ast::index::RangeIndexDirection,
) -> StorageRangeIndexDirection {
    match direction {
        helix_ast::index::RangeIndexDirection::Asc => StorageRangeIndexDirection::Asc,
        helix_ast::index::RangeIndexDirection::Desc => StorageRangeIndexDirection::Desc,
    }
}
