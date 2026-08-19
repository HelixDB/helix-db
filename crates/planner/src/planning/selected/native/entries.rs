//! Native AST batch-entry conversion.
//!
//! This module converts individual AST batch entries into selected executable
//! IR entries and prepares native query roots for batched Cascades selection.

use helix_ast::batch::NamedQuery;
use std::collections::BTreeSet;

use super::super::{cache, SelectedCascadesPlanner};
use super::rejection::{self, NativeUnsupportedReason};
use super::scope::NativeAstScope;
use super::{conditions, context_usage, names, scoped};
use crate::{error, exec, ir};

impl SelectedCascadesPlanner<'_> {
    pub(super) fn prepare_selected_ast_query_root(
        &self,
        query: &NamedQuery,
        late_bound_params: &BTreeSet<ir::NonEmptyString>,
        pending: &mut cache::PendingSelectedRunRoots,
    ) -> Result<cache::SelectedRunRootUse, error::PlannerError> {
        context_usage::validate_query_root_context(&query.root)?;
        let mut scoped_ctx = self.ctx().clone();
        scoped_ctx
            .late_bound_params
            .extend(late_bound_params.iter().cloned());
        let logical_root = match scoped::scoped_selectable_root_from_ast(
            &scoped_ctx,
            &query.root,
            NativeAstScope::QueryRoot,
        )? {
            scoped::ScopedSelectableRoot::Root(root) => *root,
            scoped::ScopedSelectableRoot::NotSelectable => {
                return Err(rejection::unsupported(
                    NativeUnsupportedReason::QueryRootUnsupported,
                ));
            }
        };
        if let Some(selected) = self.cached_selected_run_root(&logical_root) {
            return Ok(cache::SelectedRunRootUse::Ready(selected));
        }
        Ok(pending.push_or_reuse(logical_root))
    }
}

pub(super) fn initial_query_entry(
    query: &NamedQuery,
    selected: cache::SelectedRunRoot,
) -> Result<exec::SelectedInitialExecutableBatchEntry, error::PlannerError> {
    Ok(exec::SelectedInitialExecutableBatchEntry::Run(Box::new(
        exec::SelectedExecutableRunEntry {
            root: selected.root,
            output: query_output(query)?,
            condition: initial_query_condition(query)?,
        },
    )))
}

pub(super) fn followup_query_entry(
    query: &NamedQuery,
    selected: cache::SelectedRunRoot,
) -> Result<exec::SelectedFollowupExecutableBatchEntry, error::PlannerError> {
    Ok(exec::SelectedFollowupExecutableBatchEntry::Run(Box::new(
        exec::SelectedExecutableRunEntry {
            root: selected.root,
            output: query_output(query)?,
            condition: followup_query_condition(query)?,
        },
    )))
}

fn query_output(query: &NamedQuery) -> Result<ir::BatchOutputPlan, error::PlannerError> {
    match query.name.as_deref() {
        Some(name) => names::non_empty(name, ir::NameField::Name).map(ir::BatchOutputPlan::Bind),
        None => Ok(ir::BatchOutputPlan::Discard),
    }
}

fn initial_query_condition(
    query: &NamedQuery,
) -> Result<ir::RunConditionPlan<ir::BatchVariableConditionPlan>, error::PlannerError> {
    match query.condition.as_ref() {
        Some(condition) => conditions::initial(condition).map(ir::RunConditionPlan::If),
        None => Ok(ir::RunConditionPlan::Always),
    }
}

fn followup_query_condition(
    query: &NamedQuery,
) -> Result<ir::RunConditionPlan<ir::BatchConditionPlan>, error::PlannerError> {
    match query.condition.as_ref() {
        Some(condition) => conditions::followup(condition).map(ir::RunConditionPlan::If),
        None => Ok(ir::RunConditionPlan::Always),
    }
}
