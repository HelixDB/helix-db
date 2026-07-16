use super::*;
use crate::exec::{
    ExecCondition, ExecExecutionStage, ExecOp, ExecPlanError, ExecSchedule, ExecStep, ExecStepId,
};
use crate::{cost, ir, properties};

fn id(value: usize) -> ExecStepId {
    ExecStepId::new(value).unwrap()
}

fn steps(items: Vec<ExecStep>) -> ir::AtLeast<ExecStep, 1> {
    ir::AtLeast::<_, 1>::try_from_vec(items).unwrap()
}

fn step(value: usize, dependencies: Vec<usize>) -> ExecStep {
    ExecStep {
        id: id(value),
        dependencies: dependencies.into_iter().map(id).collect(),
        output: ir::BatchOutputPlan::Discard,
        condition: ExecCondition::Always,
        op: ExecOp::Noop,
        schedule: ExecSchedule::Pipeline,
        delivered: properties::DeliveredProperties::default(),
        cost: cost::CostVector::ZERO,
    }
}

#[test]
fn validated_step_index_rejects_duplicate_ids_before_graph_checks() {
    let duplicate_steps = steps(vec![step(1, vec![]), step(1, vec![])]);
    let Err(err) = index::ValidatedStepIndex::new(&duplicate_steps, id(1)) else {
        panic!("duplicate step IDs must be rejected");
    };
    assert_eq!(err, ExecPlanError::DuplicateStepId { id: id(1) });
}

#[test]
fn graph_reachability_supports_transitive_previous_conditions() {
    let mut root = step(3, vec![2]);
    root.condition = ExecCondition::PreviousStepNotEmpty { dependency: id(1) };
    let graph_steps = steps(vec![step(1, vec![]), step(2, vec![1]), root]);
    let index = index::ValidatedStepIndex::new(&graph_steps, id(3)).unwrap();

    assert!(graph::dependency_reachable(&index, &[id(2)], id(1)));
    assert!(!graph::dependency_reachable(&index, &[id(2)], id(99)));
}

#[test]
fn order_stage_contract_distinguishes_single_parallel_and_empty_sets() {
    assert_eq!(
        order::stage_from_ready(vec![id(1)]).unwrap(),
        ExecExecutionStage::Single(id(1))
    );
    let ExecExecutionStage::Parallel(stage) = order::stage_from_ready(vec![id(1), id(2)]).unwrap()
    else {
        panic!("two ready steps should form a parallel stage");
    };
    assert_eq!(stage.ids(), &[id(1), id(2)]);
    assert_eq!(stage.max_concurrency().get(), 2);
    assert!(stage.preserve_order());
    assert_eq!(
        order::stage_from_ready(Vec::new()).unwrap_err(),
        ExecPlanError::InvalidExecutionStage { actual: 0 }
    );
}
