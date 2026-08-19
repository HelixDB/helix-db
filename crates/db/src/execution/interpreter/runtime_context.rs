//! Mutable state carried by one executable-plan interpreter run.
//!
//! Operation modules receive a shared [`ExecutionContext`] contract instead of
//! owning request state themselves. Fields are interpreter-visible so focused
//! contract modules can update state directly while the public facade remains
//! narrow.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::ops::{Deref, DerefMut};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrayvec::ArrayVec;
use slatedb::DbTransaction;

use super::*;

#[cfg(test)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::execution::interpreter) struct ProjectionReadSnapshot {
    pub(in crate::execution::interpreter) property_gets: usize,
    pub(in crate::execution::interpreter) property_decodes: usize,
    pub(in crate::execution::interpreter) endpoint_gets: usize,
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(in crate::execution::interpreter) struct ProjectionReadCounters {
    property_gets: AtomicUsize,
    property_decodes: AtomicUsize,
    endpoint_gets: AtomicUsize,
}

#[cfg(test)]
impl ProjectionReadCounters {
    fn snapshot(&self) -> ProjectionReadSnapshot {
        ProjectionReadSnapshot {
            property_gets: self.property_gets.load(Ordering::Relaxed),
            property_decodes: self.property_decodes.load(Ordering::Relaxed),
            endpoint_gets: self.endpoint_gets.load(Ordering::Relaxed),
        }
    }
}

pub(in crate::execution::interpreter) struct ActiveWriteTx {
    pub(in crate::execution::interpreter) txn: DbTransaction,
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

/// One-shot catalog freshness supplied when a request opens its first write view.
pub(in crate::execution::interpreter) enum PendingCatalogFreshness {
    /// Public plan execution has no coupled planner observation.
    Unverified,
    /// Query-service planning refreshed this exact runtime catalog.
    Prepared(crate::CatalogRefreshProof),
    /// The one request-coupled observation was already consumed or unavailable.
    Consumed,
}

/// Request parameter ownership that allocates sharing only for parallel work.
pub(in crate::execution::interpreter) enum ParamBindingsOwnership {
    /// Serial execution owns the bindings directly.
    Unique(context::ParamBindings),
    /// Parallel isolated contexts share one immutable binding table.
    Shared(Arc<context::ParamBindings>),
}

impl ParamBindingsOwnership {
    pub(in crate::execution::interpreter) fn shallow_snapshot(&mut self) -> Self {
        match self {
            Self::Shared(params) => Self::Shared(Arc::clone(params)),
            Self::Unique(_) => {
                let Self::Unique(params) =
                    std::mem::replace(self, Self::Unique(context::ParamBindings::default()))
                else {
                    unreachable!("the parameter variant was matched before replacement");
                };
                let params = Arc::new(params);
                *self = Self::Shared(Arc::clone(&params));
                Self::Shared(params)
            }
        }
    }
}

impl Deref for ParamBindingsOwnership {
    type Target = context::ParamBindings;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Unique(params) => params,
            Self::Shared(params) => params,
        }
    }
}

impl DerefMut for ParamBindingsOwnership {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Unique(params) => params,
            Self::Shared(params) => Arc::make_mut(params),
        }
    }
}

const INLINE_STEP_OUTPUT_USES: usize = 4;

/// Nonzero future step-output consumers with an allocation-free common case.
///
/// Small executable plans remain inline. Large plans promote once to a tree so
/// lookup cost continues to scale logarithmically without trusting step IDs to
/// be dense enough for direct indexing.
pub(in crate::execution::interpreter) enum StepOutputUsePlan {
    Inline(ArrayVec<(exec::ExecStepId, NonZeroUsize), INLINE_STEP_OUTPUT_USES>),
    Tree(BTreeMap<exec::ExecStepId, NonZeroUsize>),
}

impl Default for StepOutputUsePlan {
    fn default() -> Self {
        Self::Inline(ArrayVec::new())
    }
}

impl StepOutputUsePlan {
    pub(in crate::execution::interpreter) fn is_empty(&self) -> bool {
        match self {
            Self::Inline(uses) => uses.is_empty(),
            Self::Tree(uses) => uses.is_empty(),
        }
    }

    pub(in crate::execution::interpreter) fn contains_key(&self, step: &exec::ExecStepId) -> bool {
        match self {
            Self::Inline(uses) => uses.iter().any(|(candidate, _)| candidate == step),
            Self::Tree(uses) => uses.contains_key(step),
        }
    }

    pub(in crate::execution::interpreter) fn get(
        &self,
        step: &exec::ExecStepId,
    ) -> Option<&NonZeroUsize> {
        match self {
            Self::Inline(uses) => uses
                .iter()
                .find(|(candidate, _)| candidate == step)
                .map(|(_, count)| count),
            Self::Tree(uses) => uses.get(step),
        }
    }

    pub(in crate::execution::interpreter) fn get_mut(
        &mut self,
        step: &exec::ExecStepId,
    ) -> Option<&mut NonZeroUsize> {
        match self {
            Self::Inline(uses) => uses
                .iter_mut()
                .find(|(candidate, _)| candidate == step)
                .map(|(_, count)| count),
            Self::Tree(uses) => uses.get_mut(step),
        }
    }

    pub(in crate::execution::interpreter) fn insert(
        &mut self,
        step: exec::ExecStepId,
        count: NonZeroUsize,
    ) -> Option<NonZeroUsize> {
        match self {
            Self::Inline(uses) => {
                if let Some((_, current)) =
                    uses.iter_mut().find(|(candidate, _)| *candidate == step)
                {
                    return Some(std::mem::replace(current, count));
                }
                if uses.len() < INLINE_STEP_OUTPUT_USES {
                    uses.push((step, count));
                    return None;
                }
            }
            Self::Tree(uses) => return uses.insert(step, count),
        }

        let Self::Inline(uses) = std::mem::replace(self, Self::Tree(BTreeMap::new())) else {
            unreachable!("only a full inline use plan reaches promotion");
        };
        let mut tree = uses.into_iter().collect::<BTreeMap<_, _>>();
        let previous = tree.insert(step, count);
        *self = Self::Tree(tree);
        previous
    }

    pub(in crate::execution::interpreter) fn remove(
        &mut self,
        step: &exec::ExecStepId,
    ) -> Option<NonZeroUsize> {
        match self {
            Self::Inline(uses) => uses
                .iter()
                .position(|(candidate, _)| candidate == step)
                .map(|index| uses.swap_remove(index).1),
            Self::Tree(uses) => uses.remove(step),
        }
    }

    pub(in crate::execution::interpreter) fn iter(&self) -> StepOutputUseIter<'_> {
        match self {
            Self::Inline(uses) => StepOutputUseIter::Inline(uses.iter()),
            Self::Tree(uses) => StepOutputUseIter::Tree(uses.iter()),
        }
    }
}

pub(in crate::execution::interpreter) enum StepOutputUseIter<'a> {
    Inline(std::slice::Iter<'a, (exec::ExecStepId, NonZeroUsize)>),
    Tree(std::collections::btree_map::Iter<'a, exec::ExecStepId, NonZeroUsize>),
}

impl<'a> Iterator for StepOutputUseIter<'a> {
    type Item = (&'a exec::ExecStepId, &'a NonZeroUsize);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Inline(uses) => uses.next().map(|(step, count)| (step, count)),
            Self::Tree(uses) => uses.next(),
        }
    }
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
    pub(in crate::execution::interpreter) params: ParamBindingsOwnership,
    pub(in crate::execution::interpreter) variables: ExecutionValueStore<ir::NonEmptyString>,
    pub(in crate::execution::interpreter) step_outputs: ExecutionValueStore<exec::ExecStepId>,
    pub(in crate::execution::interpreter) step_output_uses: StepOutputUsePlan,
    pub(in crate::execution::interpreter) request_read_scope:
        super::read_view::RequestReadScopeState,
    pub(in crate::execution::interpreter) request_write_scope: RequestWriteScopeState,
    pub(in crate::execution::interpreter) pending_catalog_freshness: PendingCatalogFreshness,
    pub(in crate::execution::interpreter) row_mode_max_rows: row_mode::RowModeMaxRowsSetting,
    pub(in crate::execution::interpreter) execution_control:
        crate::execution_control::ExecutionControl,
    #[cfg(test)]
    pub(in crate::execution::interpreter) projection_reads: Arc<ProjectionReadCounters>,
    #[cfg(test)]
    pub(in crate::execution::interpreter) deadline_checks_remaining: AtomicUsize,
}

impl<'db> ExecutionContext<'db> {
    #[cfg(any(test, feature = "production-coverage"))]
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

    #[cfg(any(test, feature = "production-coverage"))]
    pub(in crate::execution::interpreter) fn new_scoped(
        db: &'db HelixDB,
        params: context::ParamBindings,
        tenant_scope: crate::encoding::keys::tenant::DataScope,
    ) -> Self {
        Self::new_scoped_controlled(
            db,
            params,
            tenant_scope,
            crate::execution_control::ExecutionControl::unlimited(),
        )
    }

    pub(in crate::execution::interpreter) fn new_scoped_controlled(
        db: &'db HelixDB,
        params: context::ParamBindings,
        tenant_scope: crate::encoding::keys::tenant::DataScope,
        execution_control: crate::execution_control::ExecutionControl,
    ) -> Self {
        Self::new_scoped_controlled_with_catalog_freshness(
            db,
            params,
            tenant_scope,
            execution_control,
            PendingCatalogFreshness::Unverified,
        )
    }

    pub(in crate::execution::interpreter) fn new_scoped_controlled_with_catalog_freshness(
        db: &'db HelixDB,
        params: context::ParamBindings,
        tenant_scope: crate::encoding::keys::tenant::DataScope,
        execution_control: crate::execution_control::ExecutionControl,
        catalog_freshness: PendingCatalogFreshness,
    ) -> Self {
        Self {
            db,
            tenant_scope,
            params: ParamBindingsOwnership::Unique(params),
            variables: ExecutionValueStore::default(),
            step_outputs: ExecutionValueStore::default(),
            step_output_uses: StepOutputUsePlan::default(),
            request_read_scope: super::read_view::RequestReadScopeState::Disabled,
            request_write_scope: RequestWriteScopeState::Disabled,
            pending_catalog_freshness: catalog_freshness,
            row_mode_max_rows: row_mode::RowModeMaxRowsSetting::default(),
            execution_control,
            #[cfg(test)]
            projection_reads: Arc::new(ProjectionReadCounters::default()),
            #[cfg(test)]
            deadline_checks_remaining: AtomicUsize::new(usize::MAX),
        }
    }

    #[cfg(test)]
    pub(in crate::execution::interpreter) fn projection_read_snapshot(
        &self,
    ) -> ProjectionReadSnapshot {
        self.projection_reads.snapshot()
    }

    #[cfg(test)]
    pub(in crate::execution::interpreter) fn record_property_get(&self) {
        self.projection_reads
            .property_gets
            .fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(in crate::execution::interpreter) fn record_property_decode(&self) {
        self.projection_reads
            .property_decodes
            .fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(in crate::execution::interpreter) fn record_endpoint_get(&self) {
        self.projection_reads
            .endpoint_gets
            .fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(in crate::execution::interpreter) fn fail_deadline_after(&self, successful_checks: usize) {
        self.deadline_checks_remaining
            .store(successful_checks, Ordering::Relaxed);
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

    pub(in crate::execution::interpreter) fn check_execution_deadline(&self) -> Result<()> {
        #[cfg(test)]
        if self.deadline_checks_remaining.load(Ordering::Relaxed) != usize::MAX
            && self
                .deadline_checks_remaining
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_err()
        {
            return Err(HelixDbError::QueryDeadlineExceeded);
        }
        self.execution_control.check()
    }
}

#[cfg(test)]
mod tests {
    use helix_planner::context;

    use super::test_support;
    use super::*;

    fn step_id(value: usize) -> exec::ExecStepId {
        exec::ExecStepId::new(value).expect("positive step ID")
    }

    #[test]
    fn step_output_use_plan_promotes_without_losing_nonzero_counts() {
        let mut uses = StepOutputUsePlan::default();
        for value in 1..=INLINE_STEP_OUTPUT_USES {
            uses.insert(step_id(value), NonZeroUsize::MIN);
        }
        assert!(matches!(uses, StepOutputUsePlan::Inline(_)));

        uses.insert(
            step_id(INLINE_STEP_OUTPUT_USES + 1),
            NonZeroUsize::new(2).expect("two consumers"),
        );

        assert!(matches!(uses, StepOutputUsePlan::Tree(_)));
        assert_eq!(uses.iter().count(), INLINE_STEP_OUTPUT_USES + 1);
        assert_eq!(
            uses.remove(&step_id(INLINE_STEP_OUTPUT_USES + 1)),
            NonZeroUsize::new(2)
        );
        assert!(!uses.contains_key(&step_id(INLINE_STEP_OUTPUT_USES + 1)));
    }

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
        assert!(matches!(
            ctx.pending_catalog_freshness,
            PendingCatalogFreshness::Unverified
        ));
        assert_eq!(
            ctx.row_mode_max_rows,
            row_mode::RowModeMaxRowsSetting::Unread
        );
    }
}
