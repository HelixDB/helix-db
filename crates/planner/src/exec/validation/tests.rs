use super::*;
use crate::exec::{
    ExecCondition, ExecCountDependency, ExecCountPlan, ExecCountStreamPlan,
    ExecCountValidationError, ExecCountWindowPlan, ExecExecutionStage, ExecOp, ExecPlanError,
    ExecSchedule, ExecStep, ExecStepId,
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
        semantic_return_shape: None,
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

fn count_step(value: usize, dependencies: Vec<usize>, plan: ExecCountPlan) -> ExecStep {
    let mut step = step(value, dependencies);
    step.op = ExecOp::Count {
        plan: Box::new(plan),
    };
    step
}

#[test]
fn validated_step_index_enforces_count_dependency_shapes() {
    for (plan, dependencies, expected) in [
        (
            ExecCountPlan::InputRows {
                window: ExecCountWindowPlan::identity(),
            },
            Vec::new(),
            ExecCountDependency::Rows,
        ),
        (
            ExecCountPlan::InputScalars {
                window: ExecCountWindowPlan::identity(),
            },
            vec![1, 2],
            ExecCountDependency::Scalars,
        ),
        (
            ExecCountPlan::Constant(0),
            vec![1, 2],
            ExecCountDependency::Direct,
        ),
    ] {
        let graph_steps = steps(vec![
            step(1, Vec::new()),
            step(2, Vec::new()),
            count_step(3, dependencies.clone(), plan),
        ]);
        let Err(error) = index::ValidatedStepIndex::new(&graph_steps, id(3)) else {
            panic!("invalid count dependency shape must be rejected")
        };
        assert!(error.to_string().contains(&format!("{expected:?} input")));
        assert_eq!(
            error,
            ExecPlanError::InvalidCountDependencyCount {
                step: id(3),
                dependency: expected,
                actual: dependencies.len(),
            }
        );
    }

    let sequenced_direct = steps(vec![
        step(1, Vec::new()),
        count_step(2, vec![1], ExecCountPlan::Constant(0)),
    ]);
    assert!(index::ValidatedStepIndex::new(&sequenced_direct, id(2)).is_ok());

    let one_row_input = steps(vec![
        step(1, Vec::new()),
        count_step(
            2,
            vec![1],
            ExecCountPlan::InputRows {
                window: ExecCountWindowPlan::identity(),
            },
        ),
    ]);
    assert!(index::ValidatedStepIndex::new(&one_row_input, id(2)).is_ok());
}

#[test]
fn validated_step_index_rejects_malformed_count_programs() {
    let malformed = ExecCountPlan::Stream(ExecCountStreamPlan {
        cursor: crate::exec::ExecCountCursorPlan::Intersect {
            driver: Box::new(crate::exec::ExecCountCursorPlan::InputRows),
            rest: ir::AtLeast::from_one(crate::exec::ExecCountCursorPlan::InputRows),
        },
        window: ExecCountWindowPlan::identity(),
    });
    let graph_steps = steps(vec![count_step(1, Vec::new(), malformed)]);
    let Err(error) = index::ValidatedStepIndex::new(&graph_steps, id(1)) else {
        panic!("malformed count program must be rejected")
    };
    assert!(error.to_string().contains("invalid program"));

    assert_eq!(
        error,
        ExecPlanError::InvalidCountProgram {
            step: id(1),
            reason: ExecCountValidationError::MultipleRowInputs,
        }
    );
}
