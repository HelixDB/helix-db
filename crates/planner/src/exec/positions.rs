//! Dense positions for an executable DAG's step IDs.
//!
//! Validation and region derivation keep one piece of state per step. Planning
//! numbers every DAG's steps consecutively, so a step's position is usually its
//! ID's offset from the smallest ID; any other set of IDs, such as a
//! deserialized plan's, falls back to a binary search. Per-step state then
//! lives in vectors indexed by position rather than in maps keyed by ID, and
//! positions follow ascending ID order, so iterating positions visits IDs in
//! the order the maps did.

use super::ExecStepId;

pub(in crate::exec) struct StepPositions {
    /// Every step ID, ascending and unique.
    ids: Vec<ExecStepId>,
    /// `ids` has no gaps, so a position is an offset from the first ID.
    consecutive: bool,
}

impl StepPositions {
    /// Index the given step IDs, or return the first ID that repeats, in the
    /// order given.
    pub(in crate::exec) fn new(
        ids: impl Iterator<Item = ExecStepId> + Clone,
    ) -> Result<Self, ExecStepId> {
        let mut sorted = ids.clone().collect::<Vec<_>>();
        sorted.sort_unstable();
        if sorted.windows(2).any(|pair| pair[0] == pair[1]) {
            let mut seen = std::collections::BTreeSet::new();
            return Err(ids
                .into_iter()
                .find(|id| !seen.insert(*id))
                .expect("a repeated ID repeats in the given order"));
        }
        let consecutive = match (sorted.first(), sorted.last()) {
            (Some(first), Some(last)) => last.get() - first.get() + 1 == sorted.len(),
            _ => true,
        };
        Ok(Self {
            ids: sorted,
            consecutive,
        })
    }

    pub(in crate::exec) fn len(&self) -> usize {
        self.ids.len()
    }

    /// Step IDs in ascending order; the index of each is its position.
    pub(in crate::exec) fn ids(&self) -> &[ExecStepId] {
        &self.ids
    }

    pub(in crate::exec) fn position(&self, id: ExecStepId) -> Option<usize> {
        match self.consecutive {
            true => {
                let offset = id.get().checked_sub(self.ids.first()?.get())?;
                (offset < self.ids.len()).then_some(offset)
            }
            false => self.ids.binary_search(&id).ok(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(values: &[usize]) -> Vec<ExecStepId> {
        values
            .iter()
            .map(|value| ExecStepId::new(*value).unwrap())
            .collect()
    }

    #[test]
    fn positions_follow_ascending_ids_with_or_without_gaps() {
        for (given, expected) in [
            (vec![3, 1, 2], vec![1, 2, 3]),
            (vec![7, 2, 40], vec![2, 7, 40]),
            (vec![5], vec![5]),
        ] {
            let positions = StepPositions::new(ids(&given).into_iter()).unwrap();
            assert_eq!(positions.ids(), ids(&expected).as_slice());
            assert_eq!(positions.len(), expected.len());
            for (position, id) in ids(&expected).into_iter().enumerate() {
                assert_eq!(positions.position(id), Some(position));
            }
            for missing in [1, 4, 6, 8, 39, 41, 100] {
                if !expected.contains(&missing) {
                    let id = ExecStepId::new(missing).unwrap();
                    assert_eq!(positions.position(id), None, "{given:?} {missing}");
                }
            }
        }
    }

    #[test]
    fn the_first_repeat_in_given_order_is_reported() {
        let Err(repeated) = StepPositions::new(ids(&[4, 9, 2, 9, 4]).into_iter()) else {
            panic!("repeated IDs must be rejected");
        };
        assert_eq!(repeated.get(), 9);
    }

    #[test]
    fn an_empty_set_has_no_positions() {
        let positions = StepPositions::new(std::iter::empty()).unwrap();
        assert_eq!(positions.len(), 0);
        assert_eq!(positions.position(ExecStepId::first()), None);
    }
}
