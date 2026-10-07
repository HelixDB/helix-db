//! Validate physical producer/consumer boundaries once per selected strategy.
//! Cursor proofs visit each source and pattern step once; ordered lookups avoid
//! rescanning a long suffix for every potential producer.
use super as r;
use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
mod tests;

/// A prepared consumer always contains its source's window proof: splitting a
/// chain inside it would stop short of the proven window or drain a source
/// whose batches are sized to that proof's demand.
pub(super) fn prepare(
    pipeline: &r::RowPipeline,
    matches: &BTreeMap<usize, r::MatchPlan>,
) -> BTreeMap<usize, r::BatchConsumer> {
    if pipeline.execution() == r::RowExecution::Materialized {
        return BTreeMap::new();
    }
    let mut unsupported = BTreeSet::new();
    let mut initial_cursor = false;
    for (&index, pattern) in matches {
        let cursors: BTreeSet<_> = pattern
            .sources
            .iter()
            .filter_map(|source| {
                #[cfg(test)]
                tests::visited(0);
                let [step] = source.access.steps() else {
                    return None;
                };
                step.op.node_cursor_access().map(|_| source.slot)
            })
            .collect();
        let supported = pattern.steps.iter().all(|step| {
            #[cfg(test)]
            tests::visited(1);
            match step {
                r::MatchStep::Expand { .. } => true,
                r::MatchStep::Scan(slot) | r::MatchStep::HashJoin { slot, .. } => {
                    cursors.contains(slot)
                }
                r::MatchStep::IndexLookup(lookup) => cursors.contains(&lookup.slot),
            }
        });
        if !supported {
            unsupported.insert(index);
        }
        if index == 0 {
            initial_cursor =
                supported && matches!(pattern.steps.first(), Some(r::MatchStep::Scan(_)));
        }
    }
    let operators = pipeline.query().operators();
    let previous_projections: Vec<_> = operators
        .iter()
        .enumerate()
        .scan(None, |previous, (index, operator)| {
            #[cfg(test)]
            tests::visited(2);
            let before = *previous;
            if matches!(operator, r::Operator::Project { .. }) {
                *previous = Some(index);
            }
            Some(before)
        })
        .collect();
    operators
        .iter()
        .enumerate()
        .filter_map(|(source, _)| {
            #[cfg(test)]
            tests::visited(3);
            let mut consumer = pipeline.batch_consumer(source)?;
            if matches.contains_key(&source)
                && (unsupported.contains(&source) || (source == 0 && !initial_cursor))
            {
                return None;
            }
            if let r::BatchConsumer::Pipeline { end } = consumer
                && let Some(&blocked) = unsupported.range(source + 1..end).next()
            {
                // Without a consumer the source truncates to the proven demand.
                if pipeline
                    .input_window(source)
                    .is_some_and(|window| blocked < window.last_projection())
                {
                    return None;
                }
                let end = previous_projections[blocked].filter(|end| *end > source)?;
                consumer = if end == source + 1 {
                    r::BatchConsumer::Project {
                        termination: pipeline
                            .input_window(source)
                            .map_or(r::Termination::Drain, r::InputWindow::termination),
                    }
                } else {
                    r::BatchConsumer::Pipeline { end }
                };
            }
            if consumer == r::BatchConsumer::TopK && source == 0 && ordered_input(pipeline, matches)
            {
                consumer = r::BatchConsumer::OrderedWindow;
            }
            let covered = match consumer {
                r::BatchConsumer::Pipeline { end } => end,
                r::BatchConsumer::Project { .. }
                | r::BatchConsumer::Aggregate
                | r::BatchConsumer::Distinct
                | r::BatchConsumer::TopK
                | r::BatchConsumer::OrderedWindow => source + 1,
            };
            assert!(
                pipeline
                    .input_window(source)
                    .is_none_or(|window| window.last_projection() <= covered),
                "a prepared consumer contains its source window's proof"
            );
            Some((source, consumer))
        })
        .collect()
}

/// Whether the first MATCH delivers its rows in the order of the top-k
/// projection that directly consumes it: its one node is read through a
/// range index on the sort property in the sort direction. The MATCH's WHERE
/// holds that range, so every row has a non-null value of the range's type,
/// and values of one type order as the index does. `sort_key` admits only
/// projections that pass variables, literals and parameters along, and the
/// index read and verified the sort key on every row it returns.
fn ordered_input(pipeline: &r::RowPipeline, matches: &BTreeMap<usize, r::MatchPlan>) -> bool {
    let query = pipeline.query();
    let Some((slot, property, descending)) = r::planning::sort_key(query) else {
        return false;
    };
    let [r::Operator::Match {
        pattern,
        optional: false,
        predicate,
    }, ..] = query.operators()
    else {
        return false;
    };
    let Some(plan) = matches.get(&0) else {
        return false;
    };
    let ([r::MatchStep::Scan(scanned)], [source]) =
        (plan.steps.as_slice(), plan.sources.as_slice())
    else {
        return false;
    };
    let [step] = source.access.steps() else {
        return false;
    };
    let Some(crate::exec::ExecNodeCursor::Range {
        key,
        iteration: crate::ir::RangeScanIteration::Forward,
        ..
    }) = step.op.node_cursor_access()
    else {
        return false;
    };
    let ascending = key.direction == helix_ast::index::RangeIndexDirection::Asc;
    let totality = r::planning::Totality {
        pattern,
        bindings: query.bindings(),
        params: r::planning::Parameters::Validated,
    };
    *scanned == slot
        && key.property.as_ref() == property
        && ascending != descending
        && predicate
            .as_ref()
            .is_none_or(|predicate| totality.predicate(predicate.expression()))
}
