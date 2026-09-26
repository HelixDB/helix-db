//! Derived scheduling and row layouts are deliberately absent from production
//! plan serialization. Capture them separately so buffering regressions cannot
//! hide behind an unchanged serialized DAG. No runtime/transport format changes.
use helix_planner::{exec, relational};
use serde_json::{json, Value};
use std::collections::BTreeMap;

pub(super) fn native(plan: &exec::ExecutablePlan) -> Value {
    let returns = match plan.executable_returns() {
        exec::ExecutableReturns::None => Vec::new(),
        exec::ExecutableReturns::Variables(variables) => variables
            .as_ref()
            .iter()
            .map(|returned| json!({"name": returned.name(), "shape": returned.shape()}))
            .collect(),
    };
    json!({
        "returns": returns,
        "dag": dag(plan.steps(), &plan.execution_order(), plan.execution_program()),
    })
}

pub(super) fn row(plan: &relational::RowPlan) -> Value {
    let program = plan.program();
    let query = program.query();
    let windows: Vec<_> = (0..query.operators().len())
        .map(|index| program.input_window(index))
        .collect();
    let consumers: Vec<_> = (0..query.operators().len())
        .map(|index| program.batch_consumer(index))
        .collect();
    let matches: BTreeMap<_, _> = program
        .matches()
        .iter()
        .map(|(index, graph)| {
            let sources: Vec<_> = graph
                .sources
                .iter()
                .map(|source| json!({"slot": source.slot, "execution": native(&source.access)}))
                .collect();
            (
                *index,
                json!({"steps": graph.steps, "incoming": graph.incoming, "sources": sources}),
            )
        })
        .collect();
    json!({
        "layout": format!("{:?}", program.layout_mode()),
        "width": query.width(),
        "operators": query.operators(),
        "returns": query.returns(),
        "input_windows": windows,
        "batch_consumers": consumers,
        "matches": matches,
    })
}

fn dag(
    steps: &[exec::ExecStep],
    order: &exec::ExecExecutionOrder,
    program: &exec::ExecProgram,
) -> Value {
    let regions: Vec<_> = program
        .regions()
        .map(|(terminal, region)| json!({"terminal": terminal, "steps": region.steps()}))
        .collect();
    let steps: Vec<_> = steps
        .iter()
        .map(|step| {
            json!({
                "id": step.id,
                "pull_capability": format!("{:?}", exec::ExecPullCapability::of(&step.op)),
                "absorbed": program.is_absorbed(step.id),
                "children": children(&step.op),
            })
        })
        .collect();
    json!({"order": order, "regions": regions, "steps": steps})
}

fn subplan(plan: &exec::ExecutableSubplan) -> Value {
    dag(
        plan.steps(),
        &plan.execution_order(),
        plan.execution_program(),
    )
}

fn children(op: &exec::ExecOp) -> BTreeMap<String, Value> {
    // Exhaustive matching makes new control-flow variants a review obligation.
    // Child names preserve branch order without depending on incidental IDs.
    match op {
        exec::ExecOp::Branch { plan } => match plan {
            exec::ExecBranchPlan::Union(branches) => branches
                .as_ref()
                .iter()
                .enumerate()
                .map(|(index, plan)| (format!("union/{index}"), subplan(plan)))
                .collect(),
            exec::ExecBranchPlan::Coalesce(branches) => branches
                .as_ref()
                .iter()
                .enumerate()
                .map(|(index, plan)| (format!("coalesce/{index}"), subplan(plan)))
                .collect(),
            exec::ExecBranchPlan::Choose { then_plan, .. } => {
                BTreeMap::from([("then".into(), subplan(then_plan))])
            }
            exec::ExecBranchPlan::ChooseElse {
                then_plan,
                else_plan,
                ..
            } => BTreeMap::from([
                ("then".into(), subplan(then_plan)),
                ("else".into(), subplan(else_plan)),
            ]),
            exec::ExecBranchPlan::Optional(plan) => {
                BTreeMap::from([("optional".into(), subplan(plan))])
            }
        },
        exec::ExecOp::Repeat { plan } => BTreeMap::from([("repeat".into(), subplan(&plan.body))]),
        exec::ExecOp::ForEach { body, .. } => BTreeMap::from([("foreach".into(), subplan(body))]),
        exec::ExecOp::Access { .. }
        | exec::ExecOp::Count { .. }
        | exec::ExecOp::KvRead(_)
        | exec::ExecOp::Expand { .. }
        | exec::ExecOp::VectorSearch { .. }
        | exec::ExecOp::TextSearch { .. }
        | exec::ExecOp::Filter { .. }
        | exec::ExecOp::Limit { .. }
        | exec::ExecOp::Skip { .. }
        | exec::ExecOp::Range { .. }
        | exec::ExecOp::Distinct
        | exec::ExecOp::Order { .. }
        | exec::ExecOp::Project { .. }
        | exec::ExecOp::Aggregate { .. }
        | exec::ExecOp::Variable { .. }
        | exec::ExecOp::ShortestPath { .. }
        | exec::ExecOp::Mutation { .. }
        | exec::ExecOp::IndexDdl { .. }
        | exec::ExecOp::Merge { .. }
        | exec::ExecOp::Reserved { .. }
        | exec::ExecOp::Barrier { .. }
        | exec::ExecOp::Noop => BTreeMap::new(),
    }
}
