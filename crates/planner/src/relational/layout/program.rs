//! An immutable execution view derived from a validated logical plan. Mapped
//! cell IDs never become a Query binding catalog or a serialized logical plan.
use crate::relational as r;
use std::{collections::BTreeMap, sync::Arc};

mod memory;

/// Select the compact representation or an independent identity-layout oracle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowLayoutMode {
    Compact,
    Identity,
}

#[derive(Debug, PartialEq)]
struct MappedQuery {
    operators: Vec<r::Operator>,
    returns: Vec<(String, r::Slot)>,
}

/// Query operators in execution-cell space. Logical bindings and their types
/// remain owned by the source Query; this type cannot be constructed externally.
#[derive(Debug, PartialEq)]
pub struct RowProgramQuery {
    source: Arc<r::Query>,
    mapped: Option<MappedQuery>,
}

impl RowProgramQuery {
    pub fn width(&self) -> usize {
        match self.mapped {
            Some(_) => self.source.layout().width(),
            None => self.source.bindings().len(),
        }
    }
    pub fn operators(&self) -> &[r::Operator] {
        self.mapped
            .as_ref()
            .map_or_else(|| self.source.operators(), |mapped| &mapped.operators)
    }
    pub fn returns(&self) -> &[(String, r::Slot)] {
        self.mapped
            .as_ref()
            .map_or_else(|| self.source.returns(), |mapped| &mapped.returns)
    }
}

/// Compiled row operators and matching access metadata use the same cell map.
/// Only validated plans can construct this view. Optional/mutation boundaries,
/// operator ordinals, native access programs and consumer proofs stay unchanged.
#[derive(Debug, PartialEq)]
pub struct RowProgram {
    query: RowProgramQuery,
    pipeline: Arc<r::RowPipeline>,
    windows: Option<BTreeMap<usize, r::InputWindow>>,
    matches: Arc<BTreeMap<usize, r::MatchPlan>>,
    consumers: Arc<BTreeMap<usize, r::BatchConsumer>>,
    mode: RowLayoutMode,
}

impl RowProgram {
    pub(in crate::relational) fn new(
        pipeline: Arc<r::RowPipeline>,
        matches: Arc<BTreeMap<usize, r::MatchPlan>>,
        consumers: Arc<BTreeMap<usize, r::BatchConsumer>>,
        mode: RowLayoutMode,
    ) -> Self {
        let source = Arc::clone(&pipeline.query);
        let layout = source.layout();
        let remap = mode == RowLayoutMode::Compact
            && (layout.width() != source.bindings().len()
                || (0..source.bindings().len()).any(|slot| {
                    layout
                        .cell(r::Slot(slot as u32))
                        .is_none_or(|cell| cell.index() != slot)
                }));
        let (mapped, matches, windows) = if remap {
            let windows = pipeline
                .input_windows
                .iter()
                .map(|(index, window)| {
                    (
                        *index,
                        window.map_expressions(|expression| layout.expression(expression)),
                    )
                })
                .collect();
            let mapped = MappedQuery {
                operators: source
                    .operators()
                    .iter()
                    .map(|operator| layout.operator(operator))
                    .collect(),
                returns: source
                    .returns()
                    .iter()
                    .map(|(name, slot)| (name.clone(), layout.slot(*slot)))
                    .collect(),
            };
            let matches = Arc::new(
                matches
                    .iter()
                    .map(|(index, plan)| (*index, layout.match_plan(plan)))
                    .collect(),
            );
            (Some(mapped), matches, Some(windows))
        } else {
            (None, matches, None)
        };
        Self {
            query: RowProgramQuery { source, mapped },
            pipeline,
            windows,
            matches,
            consumers,
            mode,
        }
    }

    pub fn query(&self) -> &RowProgramQuery {
        &self.query
    }
    pub fn matches(&self) -> &BTreeMap<usize, r::MatchPlan> {
        &self.matches
    }
    pub fn batch_consumer(&self, source: usize) -> Option<r::BatchConsumer> {
        self.consumers.get(&source).copied()
    }
    pub fn input_window(&self, source: usize) -> Option<&r::InputWindow> {
        match &self.windows {
            Some(windows) => windows.get(&source),
            None => self.pipeline.input_window(source),
        }
    }
    pub fn layout_mode(&self) -> RowLayoutMode {
        self.mode
    }
}
