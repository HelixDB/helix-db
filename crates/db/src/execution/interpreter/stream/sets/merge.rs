//! Merge-mode execution contracts.

use std::collections::BTreeSet;

use super::distinct::distinct_rows;
use super::*;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) fn merge_values(
        &self,
        values: Vec<ExecutionValue>,
        mode: exec::ExecMergeMode,
    ) -> Result<ExecutionValue> {
        let streams = values
            .into_iter()
            .map(|value| self.stream_rows(value, "merge"))
            .collect::<Result<Vec<_>>>()?;
        Ok(ExecutionValue::Stream(merge_streams(streams, mode)))
    }
}

pub(in crate::execution::interpreter::stream) fn merge_streams(
    streams: Vec<Vec<ExecutionRow>>,
    mode: exec::ExecMergeMode,
) -> Vec<ExecutionRow> {
    match mode {
        exec::ExecMergeMode::Concat => streams.into_iter().flatten().collect(),
        exec::ExecMergeMode::Union => distinct_rows(streams.into_iter().flatten().collect()),
        exec::ExecMergeMode::Intersect => {
            let Some((first, rest)) = streams.split_first() else {
                return Vec::new();
            };
            let sets = rest
                .iter()
                .map(|rows| rows.iter().cloned().collect::<BTreeSet<_>>())
                .collect::<Vec<_>>();
            let mut emitted = BTreeSet::new();
            first
                .iter()
                .filter(|row| {
                    emitted.insert((*row).clone()) && sets.iter().all(|set| set.contains(*row))
                })
                .cloned()
                .collect()
        }
    }
}
