//! Incremental, unordered bitmap construction with admission before growth.
//! The private insertion-only contract rules out runs and retained capacity from
//! removals. Per-container cardinalities bound geometric array growth without
//! copying the accumulated set or rescanning earlier containers for each ID.
use super::{Bitmap, Budget};
use crate::error::Result;

pub(crate) struct Builder {
    bitmap: Bitmap,
    cardinalities: std::collections::BTreeMap<u64, usize>,
}

impl Builder {
    pub(crate) fn new(budget: Option<&Budget>) -> Result<Self> {
        Ok(Self {
            bitmap: Bitmap::empty(budget)?,
            cardinalities: std::collections::BTreeMap::new(),
        })
    }

    pub(crate) fn insert(&mut self, id: u64) -> Result<()> {
        let Some(memory) = &mut self.bitmap.memory else {
            self.bitmap.ids.insert(id);
            return Ok(());
        };
        if self.bitmap.ids.contains(id) {
            return Ok(());
        }
        let container = id >> u16::BITS;
        let previous = self.cardinalities.get(&container).copied().unwrap_or(0);
        let cardinality = previous + 1;
        let payload = match cardinality {
            1..=4096 => 4 * cardinality.next_power_of_two().max(4),
            _ => 3 * 8192,
        };
        let previous_payload = match previous {
            0 => 0,
            1..=4096 => 4 * previous.next_power_of_two().max(4),
            _ => 3 * 8192,
        };
        // Reserve tree/container vector growth and the tracking map before
        // either insertion. Charging every container for a tree partition is
        // conservative even when many containers share one partition. Arrays
        // reserve old + new capacity; the 4,097th insertion grows to a 16 KiB
        // array before conversion, overlapping with the new 8 KiB bitset.
        let additional = usize::from(previous == 0) * 4096 + payload - previous_payload;
        memory.resize(memory.bytes.saturating_add(additional))?;
        self.cardinalities.insert(container, cardinality);
        assert!(self.bitmap.ids.insert(id), "validated new bitmap member");
        Ok(())
    }

    /// Borrow the admitted prefix without cloning or permitting uncharged growth.
    pub(crate) fn iter(&self) -> roaring::treemap::Iter<'_> {
        self.bitmap.iter()
    }

    pub(crate) fn finish(self) -> Bitmap {
        // Keep the conservative construction reservation with the result. This
        // also bounds its consuming iterator without another allocation pass.
        self.bitmap
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::HelixDbError;

    #[test]
    fn unordered_growth_matches_an_independent_set_across_container_boundaries() {
        let budget = Budget::new(16 * 1024 * 1024);
        for admission in [None, Some(&budget)] {
            let mut builder = Builder::new(admission).unwrap();
            let mut expected = std::collections::BTreeSet::new();
            // A permutation visits arrays, promotions, multiple containers,
            // high tree partitions and duplicate input without sorted runs.
            for value in (0..100_003_u64).map(|value| value * 7919 % 100_003).chain([
                0,
                4096,
                65536,
                1 << 32,
                u64::MAX,
                u64::MAX,
            ]) {
                builder.insert(value).unwrap();
                expected.insert(value);
            }
            let bitmap = builder.finish();
            assert_eq!(
                bitmap.iter().collect::<Vec<_>>(),
                expected.into_iter().collect::<Vec<_>>()
            );
            if admission.is_some() {
                assert!(
                    super::super::super::bitmap_bytes(&bitmap)
                        <= 16 * 1024 * 1024 - budget.available()
                );
            }
            let mut cursor = bitmap.into_iter();
            assert_eq!(cursor.next(), Some(0));
            drop(cursor);
            assert_eq!(budget.available(), 16 * 1024 * 1024);
        }
    }

    #[test]
    fn failed_growth_preserves_membership_and_duplicate_insertion_needs_no_memory() {
        assert!(Builder::new(Some(&Budget::new(0))).is_err());
        let budget = Budget::new(4096 + size_of::<Bitmap>() + 16);
        let mut builder = Builder::new(Some(&budget)).unwrap();
        for id in [3, 1, 2, 0] {
            builder.insert(id).unwrap();
        }
        assert_eq!(budget.available(), 0);
        builder.insert(2).unwrap();
        for id in [4, u64::MAX] {
            assert!(matches!(
                builder.insert(id),
                Err(HelixDbError::QueryMemoryLimitExceeded)
            ));
        }
        assert_eq!(
            builder.finish().into_iter().collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
        assert_eq!(budget.available(), 4096 + size_of::<Bitmap>() + 16);
    }

    #[test]
    fn bitset_promotion_reserves_the_grown_array_and_new_bitset_together() {
        let budget = Budget::new(4096 + size_of::<Bitmap>() + 3 * 8192 - 1);
        let mut builder = Builder::new(Some(&budget)).unwrap();
        for id in (0..4096).rev() {
            builder.insert(id).unwrap();
        }
        assert!(matches!(
            builder.insert(4096),
            Err(HelixDbError::QueryMemoryLimitExceeded)
        ));
        assert_eq!(builder.finish().len(), 4096);
        let budget = Budget::new(4096 + size_of::<Bitmap>() + 3 * 8192);
        let mut builder = Builder::new(Some(&budget)).unwrap();
        for id in (0..8192).rev() {
            builder.insert(id).unwrap();
        }
        assert_eq!(budget.available(), 0);
        assert_eq!(builder.finish().len(), 8192);
    }
}
