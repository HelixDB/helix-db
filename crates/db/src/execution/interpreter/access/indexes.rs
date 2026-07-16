//! Storage index lookup contracts for executable access.
//!
//! Configured secondary indexes retain their deployed physical lookup path.
//! Canonical V2 identities resolve through the request view, acquire and
//! revalidate their exact Active generation, then scan only generation-qualified
//! rows inside the shared request reader-lease guard.

use std::future::Future;

use helix_planner::{catalog, ir, properties};
use slatedb::DbReadOps;

use super::super::stream::ast_to_db_value;
use super::super::ExecutionContext;
use crate::encoding::property::property_value::PropertyValue as DbPropertyValue;
use crate::error::{HelixDbError, Result};
use crate::HelixStorage;

impl<'db> ExecutionContext<'db> {
    pub(super) fn index_value(&self, value: &ir::IndexValue) -> Result<DbPropertyValue> {
        match value {
            ir::IndexValue::Literal(value) => {
                Ok(ast_to_db_value(value.as_property_value().clone()))
            }
            ir::IndexValue::Param(param) => self.param_value(param),
        }
    }

    pub(super) async fn lookup_equality_index_set(
        &self,
        property: &str,
        value: &str,
    ) -> Result<roaring::RoaringTreemap> {
        let identity = crate::config::split_scoped_secondary_index_property(property)
            .map(|(label, property)| {
                secondary_identity(
                    crate::index_v2::IndexIdentityFamily::SecondaryEquality,
                    crate::index_v2::IndexElementKind::Node,
                    label,
                    property,
                )
            })
            .transpose()?;
        if let Some(active) = self.active_write_tx() {
            return lookup_equality_or_legacy(
                self,
                &active.txn,
                identity.as_ref(),
                value,
                crate::search::lookup_equality_index_set_scoped(
                    &active.txn,
                    property,
                    value,
                    self.tenant_scope,
                ),
            )
            .await;
        }
        if let Some(view) = self.request_read_view() {
            return lookup_equality_or_legacy(
                self,
                view,
                identity.as_ref(),
                value,
                crate::search::lookup_equality_index_set_scoped(
                    view,
                    property,
                    value,
                    self.tenant_scope,
                ),
            )
            .await;
        }
        #[cfg(test)]
        {
            match self.db.storage() {
                HelixStorage::Reader(reader) => {
                    lookup_equality_or_legacy(
                        self,
                        reader.as_ref(),
                        identity.as_ref(),
                        value,
                        crate::search::lookup_equality_index_set_scoped(
                            reader.as_ref(),
                            property,
                            value,
                            self.tenant_scope,
                        ),
                    )
                    .await
                }
                HelixStorage::Writer(writer) => {
                    lookup_equality_or_legacy(
                        self,
                        writer.db(),
                        identity.as_ref(),
                        value,
                        crate::search::lookup_equality_index_set_scoped(
                            writer.db(),
                            property,
                            value,
                            self.tenant_scope,
                        ),
                    )
                    .await
                }
            }
        }
        #[cfg(not(test))]
        {
            Err(HelixDbError::InvariantViolation(
                "secondary equality lookup escaped its request read view".to_string(),
            ))
        }
    }

    pub(super) async fn lookup_global_edge_label_index(
        &self,
        label: &str,
    ) -> Result<roaring::RoaringTreemap> {
        if let Some(active) = self.active_write_tx() {
            return crate::search::lookup_global_edge_label_index_scoped(
                &active.txn,
                label,
                self.tenant_scope,
            )
            .await;
        }
        match self.db.storage() {
            HelixStorage::Reader(reader) => {
                crate::search::lookup_global_edge_label_index_scoped(
                    reader.as_ref(),
                    label,
                    self.tenant_scope,
                )
                .await
            }
            HelixStorage::Writer(writer) => {
                crate::search::lookup_global_edge_label_index_scoped(
                    writer.db(),
                    label,
                    self.tenant_scope,
                )
                .await
            }
        }
    }

    pub(super) async fn lookup_global_edge_equality_index(
        &self,
        property: &str,
        value: &str,
    ) -> Result<roaring::RoaringTreemap> {
        let identity = crate::config::split_scoped_secondary_index_property(property)
            .map(|(label, property)| {
                secondary_identity(
                    crate::index_v2::IndexIdentityFamily::SecondaryEquality,
                    crate::index_v2::IndexElementKind::Edge,
                    label,
                    property,
                )
            })
            .transpose()?;
        if let Some(active) = self.active_write_tx() {
            return lookup_equality_or_legacy(
                self,
                &active.txn,
                identity.as_ref(),
                value,
                crate::search::lookup_global_edge_equality_index_scoped(
                    &active.txn,
                    property,
                    value,
                    self.tenant_scope,
                ),
            )
            .await;
        }
        if let Some(view) = self.request_read_view() {
            return lookup_equality_or_legacy(
                self,
                view,
                identity.as_ref(),
                value,
                crate::search::lookup_global_edge_equality_index_scoped(
                    view,
                    property,
                    value,
                    self.tenant_scope,
                ),
            )
            .await;
        }
        #[cfg(test)]
        {
            match self.db.storage() {
                HelixStorage::Reader(reader) => {
                    lookup_equality_or_legacy(
                        self,
                        reader.as_ref(),
                        identity.as_ref(),
                        value,
                        crate::search::lookup_global_edge_equality_index_scoped(
                            reader.as_ref(),
                            property,
                            value,
                            self.tenant_scope,
                        ),
                    )
                    .await
                }
                HelixStorage::Writer(writer) => {
                    lookup_equality_or_legacy(
                        self,
                        writer.db(),
                        identity.as_ref(),
                        value,
                        crate::search::lookup_global_edge_equality_index_scoped(
                            writer.db(),
                            property,
                            value,
                            self.tenant_scope,
                        ),
                    )
                    .await
                }
            }
        }
        #[cfg(not(test))]
        {
            Err(HelixDbError::InvariantViolation(
                "edge secondary equality lookup escaped its request read view".to_string(),
            ))
        }
    }

    pub(in crate::execution::interpreter) async fn lookup_out_neighbors_by_label(
        &self,
        node_id: u64,
        label: &str,
    ) -> Result<roaring::RoaringTreemap> {
        if let Some(active) = self.active_write_tx() {
            return crate::search::lookup_out_neighbors_by_label_scoped(
                &active.txn,
                node_id,
                label,
                self.tenant_scope,
            )
            .await;
        }
        match self.db.storage() {
            HelixStorage::Reader(reader) => {
                crate::search::lookup_out_neighbors_by_label_scoped(
                    reader.as_ref(),
                    node_id,
                    label,
                    self.tenant_scope,
                )
                .await
            }
            HelixStorage::Writer(writer) => {
                crate::search::lookup_out_neighbors_by_label_scoped(
                    writer.db(),
                    node_id,
                    label,
                    self.tenant_scope,
                )
                .await
            }
        }
    }

    pub(in crate::execution::interpreter) async fn lookup_in_neighbors_by_label(
        &self,
        node_id: u64,
        label: &str,
    ) -> Result<roaring::RoaringTreemap> {
        if let Some(active) = self.active_write_tx() {
            return crate::search::lookup_in_neighbors_by_label_scoped(
                &active.txn,
                node_id,
                label,
                self.tenant_scope,
            )
            .await;
        }
        match self.db.storage() {
            HelixStorage::Reader(reader) => {
                crate::search::lookup_in_neighbors_by_label_scoped(
                    reader.as_ref(),
                    node_id,
                    label,
                    self.tenant_scope,
                )
                .await
            }
            HelixStorage::Writer(writer) => {
                crate::search::lookup_in_neighbors_by_label_scoped(
                    writer.db(),
                    node_id,
                    label,
                    self.tenant_scope,
                )
                .await
            }
        }
    }

    pub(super) async fn lookup_edge_pair_index(
        &self,
        from: u64,
        to: u64,
    ) -> Result<roaring::RoaringTreemap> {
        if let Some(active) = self.active_write_tx() {
            return crate::search::lookup_edge_pair_index_scoped(
                &active.txn,
                from,
                to,
                self.tenant_scope,
            )
            .await;
        }
        match self.db.storage() {
            HelixStorage::Reader(reader) => {
                crate::search::lookup_edge_pair_index_scoped(
                    reader.as_ref(),
                    from,
                    to,
                    self.tenant_scope,
                )
                .await
            }
            HelixStorage::Writer(writer) => {
                crate::search::lookup_edge_pair_index_scoped(
                    writer.db(),
                    from,
                    to,
                    self.tenant_scope,
                )
                .await
            }
        }
    }

    pub(in crate::execution::interpreter) async fn get_edge_endpoints(
        &self,
        edge_id: u64,
    ) -> Result<Option<(u64, u64)>> {
        if let Some(active) = self.active_write_tx() {
            return crate::search::get_edge_endpoints_scoped(
                &active.txn,
                edge_id,
                self.tenant_scope,
            )
            .await;
        }
        match self.db.storage() {
            HelixStorage::Reader(reader) => {
                crate::search::get_edge_endpoints_scoped(
                    reader.as_ref(),
                    edge_id,
                    self.tenant_scope,
                )
                .await
            }
            HelixStorage::Writer(writer) => {
                crate::search::get_edge_endpoints_scoped(writer.db(), edge_id, self.tenant_scope)
                    .await
            }
        }
    }
}

/// Constructs the canonical V2 identity corresponding to one planner key.
fn secondary_identity(
    family: crate::index_v2::IndexIdentityFamily,
    element_kind: crate::index_v2::IndexElementKind,
    label: &str,
    property: &str,
) -> Result<crate::index_v2::IndexIdentity> {
    Ok(crate::index_v2::IndexIdentity::new(
        family,
        element_kind,
        crate::index_v2::IndexComponent::try_new("label", label)?,
        crate::index_v2::IndexComponent::try_new("property", property)?,
    ))
}

/// Selects V2 generation rows only when a canonical identity exists.
///
/// An absent record preserves the configured legacy path. Any present
/// non-Active V2 state fails closed instead of falling back to unqualified rows.
async fn lookup_equality_or_legacy(
    context: &ExecutionContext<'_>,
    reader: &(impl DbReadOps + Sync),
    identity: Option<&crate::index_v2::IndexIdentity>,
    value: &str,
    legacy: impl Future<Output = Result<roaring::RoaringTreemap>>,
) -> Result<roaring::RoaringTreemap> {
    if let Some(identity) = identity
        && let Some(owners) =
            lookup_managed_equality_in_view(context, reader, identity, value).await?
    {
        return Ok(owners);
    }
    legacy.await
}

/// Point-loads, leases, and scans a present canonical equality identity.
///
/// `None` means the identity has no V2 record, so the caller may use the
/// configured legacy index. Every present record is authoritative and either
/// grants an exact Active generation or returns a typed fail-closed error.
async fn lookup_managed_equality_in_view(
    context: &ExecutionContext<'_>,
    reader: &(impl DbReadOps + Sync),
    identity: &crate::index_v2::IndexIdentity,
    value: &str,
) -> Result<Option<roaring::RoaringTreemap>> {
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
    if !matches!(active, crate::index_v2::ActiveIndexHandle::Secondary { .. }) {
        return Err(HelixDbError::IndexCatalogCorruption(
            "secondary equality identity resolved another Active family".to_string(),
        ));
    }
    let lease_generation = context.acquire_index_read_lease(reader, &active).await?;
    context
        .run_index_read_batch(
            lease_generation,
            crate::index_v2::secondary::lookup_active_equality_generation(reader, &active, value),
        )
        .await
        .map(Some)
}

pub(super) fn limited_index_ids(
    ids: roaring::RoaringTreemap,
    limit: Option<properties::PositiveUsize>,
) -> Vec<u64> {
    match limit {
        Some(limit) => ids.into_iter().take(limit.get()).collect(),
        None => ids.into_iter().collect(),
    }
}

pub(super) fn scoped_property_key(key: &catalog::ScopedPropertyKey) -> String {
    crate::config::scoped_secondary_index_property(key.label.as_ref(), key.property.as_ref())
}

#[cfg(test)]
mod tests {
    use helix_ast::query::QueryValue;
    use helix_ast::value::PropertyValue;
    use helix_planner::context;
    use slatedb::IsolationLevel;

    use super::super::super::runtime_context::ActiveWriteTx;
    use super::super::super::search_index::TextIndexMaintenanceOutcome;
    use super::super::super::test_support;
    use super::*;

    fn name(value: &str) -> ir::NonEmptyString {
        test_support::name(value)
    }

    fn positive(value: usize) -> properties::PositiveUsize {
        properties::PositiveUsize::new(value).expect("positive test limit")
    }

    #[tokio::test]
    async fn index_value_converts_literals_and_runtime_parameters() {
        let db = test_support::open_db("access-index-value-conversion").await;
        let static_param = name("static_age");
        let dynamic_param = name("dynamic_name");
        let ctx = ExecutionContext::new(
            &db,
            context::ParamBindings::default()
                .with_value(static_param.clone(), PropertyValue::I64(42))
                .with_query_value(
                    dynamic_param.clone(),
                    QueryValue::String("alice".to_string()),
                ),
        );
        let literal = ir::IndexValue::Literal(
            ir::SecondaryIndexLiteral::new(PropertyValue::from("active"))
                .expect("literal is secondary-index compatible"),
        );

        assert_eq!(
            ctx.index_value(&literal).expect("literal converts"),
            DbPropertyValue::String("active".to_string())
        );
        assert_eq!(
            ctx.index_value(&ir::IndexValue::Param(static_param))
                .expect("static parameter converts"),
            DbPropertyValue::I64(42)
        );
        assert_eq!(
            ctx.index_value(&ir::IndexValue::Param(dynamic_param))
                .expect("dynamic parameter converts"),
            DbPropertyValue::String("alice".to_string())
        );
    }

    #[tokio::test]
    async fn index_value_rejects_missing_parameters() {
        let db = test_support::open_db("access-index-value-missing-param").await;
        let ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let err = ctx
            .index_value(&ir::IndexValue::Param(name("missing")))
            .expect_err("missing parameter should fail");

        assert!(err.to_string().contains("parameter `missing` is not bound"));
    }

    #[test]
    fn limited_index_ids_preserve_storage_order_and_apply_positive_limits() {
        let ids = roaring::RoaringTreemap::from_iter([9, 1, 5, 3]);

        assert_eq!(limited_index_ids(ids.clone(), None), vec![1, 3, 5, 9]);
        assert_eq!(limited_index_ids(ids, Some(positive(2))), vec![1, 3]);
    }

    #[test]
    fn scoped_property_key_uses_internal_secondary_index_scope() {
        let key = catalog::ScopedPropertyKey::try_new("User", "email")
            .expect("valid scoped property key");

        assert_eq!(
            scoped_property_key(&key),
            crate::config::scoped_secondary_index_property("User", "email")
        );
    }

    #[tokio::test]
    async fn reader_storage_dispatches_all_index_lookup_contracts() {
        let config = test_support::in_memory_config("access-reader-index-lookups")
            .with_equality_index("User", "status")
            .with_edge_equality_index("FOLLOWS", "status");
        let writer = test_support::open_db_with_config(config.clone()).await;
        let alice = test_support::add_node_with_properties(
            &writer,
            "User",
            vec![("status", PropertyValue::from("active"))],
        )
        .await;
        let bob = test_support::add_user(&writer, "bob").await;
        let edge = test_support::add_edge_with_properties(
            &writer,
            alice,
            bob,
            "FOLLOWS",
            vec![("status", PropertyValue::from("active"))],
        )
        .await;
        drop(writer);
        let reader = test_support::open_reader_with_config(config).await;
        let context = ExecutionContext::new(&reader, context::ParamBindings::default());

        assert_eq!(
            context
                .lookup_equality_index_set(
                    &crate::config::scoped_secondary_index_property("User", "status"),
                    "active",
                )
                .await
                .expect("reader node equality lookup succeeds")
                .into_iter()
                .collect::<Vec<_>>(),
            vec![alice]
        );
        assert_eq!(
            context
                .lookup_global_edge_label_index("FOLLOWS")
                .await
                .expect("reader global edge label lookup succeeds")
                .into_iter()
                .collect::<Vec<_>>(),
            vec![edge]
        );
        assert_eq!(
            context
                .lookup_global_edge_equality_index(
                    &crate::config::scoped_secondary_index_property("FOLLOWS", "status"),
                    "active",
                )
                .await
                .expect("reader global edge equality lookup succeeds")
                .into_iter()
                .collect::<Vec<_>>(),
            vec![edge]
        );
        assert_eq!(
            context
                .lookup_out_neighbors_by_label(alice, "FOLLOWS")
                .await
                .expect("reader out-neighbor lookup succeeds")
                .into_iter()
                .collect::<Vec<_>>(),
            vec![bob]
        );
        assert_eq!(
            context
                .lookup_in_neighbors_by_label(bob, "FOLLOWS")
                .await
                .expect("reader in-neighbor lookup succeeds")
                .into_iter()
                .collect::<Vec<_>>(),
            vec![alice]
        );
        assert_eq!(
            context
                .lookup_edge_pair_index(alice, bob)
                .await
                .expect("reader edge-pair lookup succeeds")
                .into_iter()
                .collect::<Vec<_>>(),
            vec![edge]
        );
        assert_eq!(
            context
                .get_edge_endpoints(edge)
                .await
                .expect("reader endpoint lookup succeeds"),
            Some((alice, bob))
        );
    }

    #[tokio::test]
    async fn active_transaction_dispatches_index_lookup_contracts() {
        let config = test_support::in_memory_config("access-active-index-lookups")
            .with_equality_index("User", "status")
            .with_edge_equality_index("FOLLOWS", "status");
        let db = test_support::open_db_with_config(config).await;
        let alice = test_support::add_node_with_properties(
            &db,
            "User",
            vec![("status", PropertyValue::from("active"))],
        )
        .await;
        let bob = test_support::add_user(&db, "bob").await;
        let edge = test_support::add_edge_with_properties(
            &db,
            alice,
            bob,
            "FOLLOWS",
            vec![("status", PropertyValue::from("active"))],
        )
        .await;
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("snapshot transaction begins");
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        context.request_write_scope =
            super::super::super::runtime_context::RequestWriteScopeState::Active(Box::new(
                ActiveWriteTx {
                    txn,
                    text_indexes: TextIndexMaintenanceOutcome::default(),
                    configured_indexes: crate::index_v2::ConfiguredIndexCatalog::default(),
                    index_context: super::super::super::mutation::MutationIndexContext::for_configured_index_test(
                        std::sync::Arc::clone(db.simhasher_registry()),
                    ),
                },
            ));

        assert_eq!(
            context
                .lookup_equality_index_set(
                    &crate::config::scoped_secondary_index_property("User", "status"),
                    "active",
                )
                .await
                .expect("transaction node equality lookup succeeds")
                .into_iter()
                .collect::<Vec<_>>(),
            vec![alice]
        );
        assert_eq!(
            context
                .lookup_global_edge_equality_index(
                    &crate::config::scoped_secondary_index_property("FOLLOWS", "status"),
                    "active",
                )
                .await
                .expect("transaction edge equality lookup succeeds")
                .into_iter()
                .collect::<Vec<_>>(),
            vec![edge]
        );
        assert_eq!(
            context
                .lookup_in_neighbors_by_label(bob, "FOLLOWS")
                .await
                .expect("transaction in-neighbor lookup succeeds")
                .into_iter()
                .collect::<Vec<_>>(),
            vec![alice]
        );
        assert_eq!(
            context
                .get_edge_endpoints(edge)
                .await
                .expect("transaction endpoint lookup succeeds"),
            Some((alice, bob))
        );
    }
}
