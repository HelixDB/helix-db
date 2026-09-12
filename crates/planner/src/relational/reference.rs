//! Diagnostic access construction without optimizer-selected indexes, join
//! implementations, cardinality ordering, or row-pipeline fusion.
use super::{
    GraphPatternOrder, MatchPlan, Operator, PatternExpansion, PatternSource, PlannedNode, Query,
    QueryError, Result,
};
use crate::{cost, exec, ir, properties, trace};
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn matches(query: &Query) -> Result<BTreeMap<usize, MatchPlan>> {
    let mut matches = BTreeMap::new();
    for (index, operator) in query.operators().iter().enumerate() {
        let Operator::Match { pattern, .. } = operator else {
            continue;
        };
        let mut sources = Vec::new();
        for slot in pattern
            .nodes
            .iter()
            .map(|node| node.slot)
            .collect::<BTreeSet<_>>()
        {
            let root = exec::ExecStepId::new(1).expect("positive root");
            let access = exec::ExecutablePlan::new(
                ir::PlanKind::Read,
                ir::ReturnPlan::None,
                ir::AtLeast::try_from_vec(vec![exec::ExecStep {
                    id: root,
                    dependencies: Vec::new(),
                    output: ir::BatchOutputPlan::Discard,
                    semantic_return_shape: None,
                    condition: exec::ExecCondition::Always,
                    op: exec::ExecOp::Access {
                        plan: Box::new(exec::ExecAccessPlan::Node(
                            exec::ExecNodeAccessPlan::AllScan,
                        )),
                    },
                    schedule: exec::ExecSchedule::Pipeline,
                    delivered: properties::DeliveredProperties::default(),
                    cost: cost::CostVector::ZERO,
                }])
                .expect("one scan step"),
                root,
                trace::PlanningTrace::default(),
                exec::PlannerMetrics::default(),
            )
            .map_err(|error| {
                QueryError::compile(
                    "InternalPlannerError",
                    "PhysicalPlanning",
                    error.to_string(),
                )
            })?;
            sources.push(PlannedNode {
                slot,
                access,
                estimated_rows: 1,
            });
        }
        let bound = query.contracts()[index].input().slots();
        // Equal estimates retain binding order. The ordinary validated schedule
        // builder supplies endpoint/relationship invariants, with no equality or
        // correlated lookup alternatives attached. Labels and properties remain
        // at the pattern validation boundary above these full scans.
        let order = GraphPatternOrder::new(
            sources
                .iter()
                .map(|source| PatternSource {
                    slot: source.slot,
                    rows: 1,
                    access_cost: cost::CostVector::ZERO,
                })
                .collect(),
            pattern
                .relationships
                .iter()
                .map(|relationship| PatternExpansion {
                    from: relationship.from,
                    to: relationship.to,
                    relationship: relationship.slot,
                    forward_rows: 1,
                    reverse_rows: 1,
                })
                .collect(),
            bound.clone(),
        )?;
        let schedule = order.schedule(&cost::StorageCostProfile::default());
        matches.insert(
            index,
            MatchPlan {
                sources,
                steps: schedule.steps,
                cartesian_products: schedule.cartesian_products,
                estimated_rows: schedule.estimated_rows,
                incoming: bound.intersection(&pattern.slots()).copied().collect(),
            },
        );
    }
    Ok(matches)
}
