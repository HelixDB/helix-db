//! Physical cells derived from validated logical scopes. Inclusive lifetimes
//! keep an operator's inputs and outputs distinct, including projection ordering.
use super::{OperatorContract, Slot};
use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap};

mod program;
mod remap;
pub use program::{RowLayoutMode, RowProgram, RowProgramQuery};

/// A cell allocated by a validated row layout, distinct from a logical binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RowCell(usize);

impl RowCell {
    pub fn index(self) -> usize {
        self.0
    }
}

/// Immutable mapping from logical binding IDs to reusable physical cells.
/// A binding with no occurrence in a validated operator has no physical cell.
/// Construction scans existing scope contracts and sorts at most 4,096 binding
/// intervals; it performs no graph or join-order exploration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowLayout {
    cells: Vec<Option<RowCell>>,
    width: usize,
}

impl RowLayout {
    pub(super) fn new(bindings: usize, contracts: &[OperatorContract]) -> Self {
        let mut lifetimes = vec![None; bindings];
        for (operator, contract) in contracts.iter().enumerate() {
            for slot in contract
                .input()
                .columns()
                .keys()
                .chain(contract.output().columns().keys())
                .chain(contract.references())
            {
                let lifetime = &mut lifetimes[slot.0 as usize];
                let Some((_, last)) = lifetime else {
                    *lifetime = Some((operator, operator));
                    continue;
                };
                *last = operator;
            }
        }
        let mut intervals = lifetimes
            .into_iter()
            .enumerate()
            .filter_map(|(slot, lifetime)| lifetime.map(|(first, last)| (first, slot, last)))
            .collect::<Vec<_>>();
        intervals.sort_unstable();
        let mut active = BinaryHeap::<Reverse<(usize, RowCell)>>::new();
        let mut free = BTreeSet::new();
        let mut cells = vec![None; bindings];
        let mut width = 0;
        for (first, slot, last) in intervals {
            while active.peek().is_some_and(|Reverse((end, _))| *end < first) {
                let Reverse((_, cell)) = active.pop().expect("active interval was inspected");
                assert!(free.insert(cell), "an active cell is not already free");
            }
            let cell = free.pop_first().unwrap_or_else(|| {
                let cell = RowCell(width);
                width += 1;
                cell
            });
            cells[slot] = Some(cell);
            active.push(Reverse((last, cell)));
        }
        Self { cells, width }
    }

    /// Number of cells needed simultaneously by this validated query.
    pub fn width(&self) -> usize {
        self.width
    }

    /// Look up a logical binding without accepting an arbitrary physical cell.
    pub fn cell(&self, binding: Slot) -> Option<RowCell> {
        self.cells.get(binding.0 as usize).copied().flatten()
    }
}
