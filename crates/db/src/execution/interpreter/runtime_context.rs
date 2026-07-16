//! Mutable state carried by one executable-plan interpreter run.
//!
//! Operation modules receive a shared [`ExecutionContext`] contract instead of
//! owning request state themselves. Fields are interpreter-visible so focused
//! contract modules can update state directly while the public facade remains
//! narrow.

use std::collections::BTreeMap;

use slatedb::DbTransaction;

use super::search_index::TextIndexMaintenanceOutcome;
use super::*;

pub(in crate::execution::interpreter) struct ActiveWriteTx {
    pub(in crate::execution::interpreter) txn: DbTransaction,
    pub(in crate::execution::interpreter) text_indexes: TextIndexMaintenanceOutcome,
    pub(in crate::execution::interpreter) configured_indexes:
        crate::index_v2::ConfiguredIndexCatalog,
    pub(in crate::execution::interpreter) index_context: super::mutation::MutationIndexContext,
}

/// Complete request-scoped write ownership state.
///
/// A write plan acquires its lifecycle permit, transaction, and catalog
/// snapshot together before its first executable step becomes [`Self::Active`].
/// No variant can represent a write request that has lifecycle ownership but no
/// stable read/write view, or an active transaction whose DDL gate was lost.
pub(in crate::execution::interpreter) enum RequestWriteScopeState {
    /// Read plans and isolated parallel contexts own no mutation resources.
    Disabled,
    /// Write request owns one transaction, catalog snapshot, and the same gate.
    ///
    /// The payload is boxed because SlateDB transaction and catalog state are
    /// substantially larger than the ready permit. Keeping them indirect
    /// avoids inflating every execution context to the active-state size.
    Active(Box<ActiveWriteTx>),
}

impl RequestWriteScopeState {
    /// Borrows the active transaction when the first mutation has started it.
    pub(in crate::execution::interpreter) fn active(&self) -> Option<&ActiveWriteTx> {
        match self {
            Self::Active(active) => Some(active.as_ref()),
            Self::Disabled => None,
        }
    }

    /// Returns whether reads must stay on the request transaction snapshot.
    pub(in crate::execution::interpreter) const fn is_active(&self) -> bool {
        matches!(self, Self::Active(_))
    }

    /// Returns whether this context belongs to an enclosing write request.
    pub(in crate::execution::interpreter) const fn is_enabled(&self) -> bool {
        matches!(self, Self::Active(_))
    }
}

pub(in crate::execution::interpreter) struct ExecutionContext<'db> {
    pub(in crate::execution::interpreter) db: &'db HelixDB,
    pub(in crate::execution::interpreter) tenant_scope: crate::encoding::keys::tenant::DataScope,
    pub(in crate::execution::interpreter) params: context::ParamBindings,
    pub(in crate::execution::interpreter) variables: BTreeMap<ir::NonEmptyString, ExecutionValue>,
    pub(in crate::execution::interpreter) step_outputs: BTreeMap<exec::ExecStepId, ExecutionValue>,
    pub(in crate::execution::interpreter) request_read_view:
        Option<Box<super::read_view::StableRequestReadView>>,
    pub(in crate::execution::interpreter) index_read_leases:
        crate::index_v2::read_guard::RequestIndexReadLeases,
    pub(in crate::execution::interpreter) request_write_scope: RequestWriteScopeState,
    pub(in crate::execution::interpreter) row_mode_max_rows: row_mode::RowModeMaxRowsSetting,
}

impl<'db> ExecutionContext<'db> {
    #[cfg(test)]
    pub(in crate::execution::interpreter) fn new(
        db: &'db HelixDB,
        params: context::ParamBindings,
    ) -> Self {
        Self::new_scoped(
            db,
            params,
            crate::encoding::keys::tenant::DataScope::LegacyUnscoped,
        )
    }

    pub(in crate::execution::interpreter) fn new_scoped(
        db: &'db HelixDB,
        params: context::ParamBindings,
        tenant_scope: crate::encoding::keys::tenant::DataScope,
    ) -> Self {
        Self {
            db,
            tenant_scope,
            params,
            variables: BTreeMap::new(),
            step_outputs: BTreeMap::new(),
            request_read_view: None,
            index_read_leases: crate::index_v2::read_guard::RequestIndexReadLeases::default(),
            request_write_scope: RequestWriteScopeState::Disabled,
            row_mode_max_rows: row_mode::RowModeMaxRowsSetting::default(),
        }
    }

    /// Borrows the active request transaction without exposing state transitions.
    pub(in crate::execution::interpreter) fn active_write_tx(&self) -> Option<&ActiveWriteTx> {
        self.request_write_scope.active()
    }

    /// Returns whether a mutation has opened the request transaction.
    pub(in crate::execution::interpreter) const fn has_active_write_tx(&self) -> bool {
        self.request_write_scope.is_active()
    }

    /// Returns whether a write plan must resume ownership after an inline DDL barrier.
    pub(in crate::execution::interpreter) const fn has_request_write_scope(&self) -> bool {
        self.request_write_scope.is_enabled()
    }

    pub(in crate::execution::interpreter) fn scoped_physical_index_name(
        &self,
        name: &str,
    ) -> String {
        self.tenant_scope.physical_index_name(name)
    }
}

#[cfg(test)]
mod tests {
    use helix_planner::context;

    use super::test_support;
    use super::*;

    #[tokio::test]
    async fn new_context_starts_with_request_params_and_empty_runtime_state() {
        let db = test_support::open_db("runtime-context-new").await;
        let param = test_support::name("limit");
        let params = context::ParamBindings::default().with_value(param.clone(), 3);

        let ctx = ExecutionContext::new(&db, params);

        assert_eq!(
            ctx.params
                .values
                .get(&param)
                .and_then(|value| value.as_i64()),
            Some(3)
        );
        assert!(ctx.variables.is_empty());
        assert!(ctx.step_outputs.is_empty());
        assert!(matches!(
            ctx.request_write_scope,
            RequestWriteScopeState::Disabled
        ));
        assert_eq!(
            ctx.row_mode_max_rows,
            row_mode::RowModeMaxRowsSetting::Unread
        );
    }
}
