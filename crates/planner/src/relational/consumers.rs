//! Validate physical producer/consumer boundaries once per selected strategy.
//! Cursor proofs visit each source and pattern step once; ordered lookups avoid
//! rescanning a long suffix for every potential producer.
use super as r;
use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
mod tests;

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
            Some((source, consumer))
        })
        .collect()
}
