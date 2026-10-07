//! Index-set vector search fusion.
//!
//! A filtered node vector search such as
//! `N<Doc>.where(category == 'x').vector_search(...)` plans an access that
//! reads an index ID set, and a vector search that consumes its rows. Run as
//! written, the access builds a row for every ID and the search turns those
//! rows back into the same ID set. When nothing else can observe the access,
//! the scheduler runs the pair as one: the access step produces nothing, and
//! the search ranks the access's ID set directly (see
//! [`super::super::access::IdSetVectorSearch`]), building only its result
//! rows. This is a runtime pairing of an unchanged plan.

use std::collections::BTreeMap;

use helix_planner::{exec, ir};

use super::super::access;
use super::super::RequestSideEffects;

/// What a step does in a fused index-set vector search.
#[derive(Debug, Clone, Copy)]
pub(in crate::execution::interpreter) enum Role<'a> {
    /// The access; its search reads the ID set itself, so it builds no rows.
    Source,
    /// The search, which ranks its source's ID set.
    Search(access::IdSetVectorSearch<'a>),
}

/// Pairs every node vector search in `steps` with its input access when the
/// two can run as one without any observable difference:
///
/// - the access reads an index-served node ID set and the search ranks nodes
///   ([`access::IdSetVectorSearch::new`]), with that access as its only input;
/// - the search is the access's only consumer, and the access output is
///   neither bound to a variable nor the DAG's root;
/// - both always run, so neither is skipped while the other runs;
/// - neither belongs to a pull region, which drives its own steps;
/// - no step of the DAG writes, so reading the set when the search runs sees
///   what the access would have seen when it ran.
///
/// Every other step, and every step of a DAG that writes, runs as planned.
pub(in crate::execution::interpreter) fn plan<'a>(
    steps: &'a [exec::ExecStep],
    root: exec::ExecStepId,
    program: &exec::ExecProgram,
) -> BTreeMap<exec::ExecStepId, Role<'a>> {
    // Nested plans reach here on every execution, so a DAG without a vector
    // search, or one that writes, costs no more than a scan of its steps.
    if !steps
        .iter()
        .any(|step| matches!(step.op, exec::ExecOp::VectorSearch { .. }))
        || steps
            .iter()
            .any(|step| RequestSideEffects::operation(&step.op) != RequestSideEffects::None)
    {
        return BTreeMap::new();
    }
    let mut uses = BTreeMap::<exec::ExecStepId, usize>::new();
    for step in steps {
        for dependency in &step.dependencies {
            *uses.entry(*dependency).or_default() += 1;
        }
        if let exec::ExecCondition::PreviousStepNotEmpty { dependency } = step.condition {
            *uses.entry(dependency).or_default() += 1;
        }
    }
    *uses.entry(root).or_default() += 1;
    let by_id = steps
        .iter()
        .map(|step| (step.id, step))
        .collect::<BTreeMap<_, _>>();
    let runs_alone = |step: &exec::ExecStep| {
        matches!(step.condition, exec::ExecCondition::Always)
            && !program.is_absorbed(step.id)
            && program.region(step.id).is_none()
    };
    steps
        .iter()
        .filter_map(|search| {
            let exec::ExecOp::VectorSearch { plan } = &search.op else {
                return None;
            };
            let [source_id] = search.dependencies.as_slice() else {
                return None;
            };
            let source = by_id.get(source_id)?;
            let exec::ExecOp::Access { plan: access } = &source.op else {
                return None;
            };
            let fused = access::IdSetVectorSearch::new(access, plan)?;
            (uses.get(source_id) == Some(&1)
                && matches!(source.output, ir::BatchOutputPlan::Discard)
                && runs_alone(source)
                && runs_alone(search))
            .then_some([(source.id, Role::Source), (search.id, Role::Search(fused))])
        })
        .flatten()
        .collect()
}
