//! Shared fixtures for interpreter mutation contract tests.

pub(super) use helix_ast::value::PropertyValue;
use helix_planner::{cost, properties, trace};
pub(super) use helix_planner::{exec, ir};

pub(super) use super::super::*;
pub(super) use crate::{HelixDB, HelixDbSource};

pub(super) fn name(value: &str) -> ir::NonEmptyString {
    ir::NonEmptyString::new(value).expect("valid test name")
}

pub(super) fn step(
    id: usize,
    dependencies: Vec<exec::ExecStepId>,
    op: exec::ExecOp,
) -> exec::ExecStep {
    exec::ExecStep {
        id: exec::ExecStepId::new(id).expect("positive step id"),
        dependencies,
        output: ir::BatchOutputPlan::Discard,
        condition: exec::ExecCondition::Always,
        op,
        schedule: exec::ExecSchedule::Pipeline,
        delivered: properties::DeliveredProperties::default(),
        cost: cost::CostVector::ZERO,
    }
}

pub(super) fn executable(
    kind: ir::PlanKind,
    steps: Vec<exec::ExecStep>,
    root: usize,
) -> exec::ExecutablePlan {
    exec::ExecutablePlan::new(
        kind,
        ir::ReturnPlan::None,
        ir::AtLeast::<_, 1>::try_from_vec(steps).expect("non-empty test plan"),
        exec::ExecStepId::new(root).expect("positive root"),
        trace::PlanningTrace::default(),
        exec::PlannerMetrics::default(),
    )
    .expect("valid executable test plan")
}

pub(super) fn assignments(items: Vec<(&str, PropertyValue)>) -> ir::PropertyAssignments {
    ir::PropertyAssignments::try_from_vec(
        items
            .into_iter()
            .map(|(name, value)| (self::name(name), ir::PropertyInputPlan::Value(value)))
            .collect(),
    )
    .expect("valid property assignments")
}

pub(super) fn ids(values: Vec<u64>) -> ir::ElementIds {
    ir::ElementIds::new(ir::AtLeast::<_, 1>::try_from_vec(values).expect("test ids are non-empty"))
        .expect("test ids are valid")
}

pub(super) fn add_node_plan(
    label: &str,
    properties: ir::PropertyAssignments,
) -> exec::ExecutablePlan {
    executable(
        ir::PlanKind::Write,
        vec![step(
            1,
            Vec::new(),
            exec::ExecOp::Mutation {
                plan: exec::ExecMutationPlan::AddNodeSource {
                    label: name(label),
                    properties,
                },
            },
        )],
        1,
    )
}

pub(super) async fn add_node(
    db: &HelixDB,
    label: &str,
    properties: Vec<(&str, PropertyValue)>,
) -> u64 {
    let plan = add_node_plan(label, assignments(properties));
    let result = db
        .execute(&plan, helix_planner::context::ParamBindings::default())
        .await
        .expect("node write succeeds");
    let Some(ExecutionValue::Stream(rows)) = result.last else {
        panic!("node write should return a stream");
    };
    let Some(ExecutionRow {
        current: Some(ElementRef::Node(id)),
        ..
    }) = rows.first()
    else {
        panic!("node write should return a node row");
    };
    *id
}

pub(super) async fn add_user(db: &HelixDB, username: &str) -> u64 {
    add_node(db, "User", vec![("name", PropertyValue::from(username))]).await
}

pub(super) async fn add_edge_with_properties(
    db: &HelixDB,
    from: u64,
    to: u64,
    label: &str,
    properties: Vec<(&str, PropertyValue)>,
) -> u64 {
    let from_param = name("from");
    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    let add_edge = executable(
        ir::PlanKind::Write,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::FromParam {
                            param: from_param.clone(),
                        },
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::AddEdge {
                        label: name(label),
                        to: ir::NodeTargetPlan::PointIds { ids: ids(vec![to]) },
                        properties: assignments(properties),
                    },
                },
            ),
        ],
        2,
    );
    let result = db
        .execute(
            &add_edge,
            helix_planner::context::ParamBindings::default()
                .with_value(from_param, PropertyValue::I64(from as i64)),
        )
        .await
        .expect("edge write succeeds");
    let Some(ExecutionValue::Stream(rows)) = result.last else {
        panic!("edge write should return a stream");
    };
    let Some(ExecutionRow {
        current: Some(ElementRef::Edge(id)),
        ..
    }) = rows.first()
    else {
        panic!("edge write should return an edge row");
    };
    *id
}

pub(super) async fn add_edge(db: &HelixDB, from: u64, to: u64, label: &str) -> u64 {
    add_edge_with_properties(db, from, to, label, Vec::new()).await
}

pub(super) fn access_label_count_plan(label: &str) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    executable(
        ir::PlanKind::Read,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::LabelScan { label: name(label) },
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Count,
                },
            ),
        ],
        2,
    )
}

pub(super) fn access_edge_param_id_plan(param: ir::NonEmptyString) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    executable(
        ir::PlanKind::Read,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Edge(
                        exec::ExecEdgeAccessPlan::FromParam {
                            param: param.clone(),
                        },
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Id,
                },
            ),
        ],
        2,
    )
}

pub(super) fn expand_node_ids_plan(
    from_param: ir::NonEmptyString,
    direction: ir::ExpandDirection,
    label: &str,
) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    let expand_id = exec::ExecStepId::new(2).expect("positive step id");
    executable(
        ir::PlanKind::Read,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::FromParam {
                            param: from_param.clone(),
                        },
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Expand {
                    plan: ir::ExpandPlan {
                        direction,
                        label: ir::ExpandLabelPlan::Label(name(label)),
                        output: ir::ExpandOutput::Nodes,
                    },
                },
            ),
            step(
                3,
                vec![expand_id],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Id,
                },
            ),
        ],
        3,
    )
}

pub(super) fn access_param_value_plan(
    param: ir::NonEmptyString,
    property: &str,
) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    executable(
        ir::PlanKind::Read,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::FromParam {
                            param: param.clone(),
                        },
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Values(
                        ir::PropertyNames::new(ir::AtLeast::<_, 1>::from_one(name(property)))
                            .expect("valid property projection"),
                    ),
                },
            ),
        ],
        2,
    )
}
