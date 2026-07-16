use super::*;

pub(in crate::exec) fn lower_selected_executable_alternative(
    source_expr: &logical::LogicalExpr,
    alternative: &physical::PhysicalAlternative,
    profile: &cost::StorageCostProfile,
    dependencies: Vec<ExecStepId>,
    output: ir::BatchOutputPlan,
    condition: ExecCondition,
) -> Result<ExecutableSubplan, ExecPlanError> {
    let mut lowering = ExecutableDagBuilder::new(profile);
    let selected = SelectedPhysicalPlan::from(alternative);
    let root = lowering.push_selected_executable_alternative(
        source_expr,
        &selected,
        dependencies,
        output,
        condition,
    )?;
    let (delivered, cost) = selected.clone_contract();
    lowering.override_step_contract(root, delivered, cost)?;
    lowering.finish_with_root(root, rejection::Reason::SelectedAlternativeEmptyDag)
}

pub(in crate::exec) fn lower_selected_executable_batch_entries(
    entries: SelectedExecutableBatchEntries,
    profile: &cost::StorageCostProfile,
) -> Result<ExecutableSubplan, ExecPlanError> {
    let mut lowering = ExecutableDagBuilder::new(profile);
    lowering.push_selected_entries(entries)?;
    lowering.finish_with_previous(
        rejection::Reason::SelectedBatchEntriesMissingRoot,
        rejection::Reason::SelectedBatchEntriesEmptyDag,
    )
}
