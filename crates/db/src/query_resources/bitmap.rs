//! Bitmap ownership at the storage/execution boundary. Decoding reserves its
//! structural bound first; consuming iteration retains that reservation. Only
//! immutable bitmap access is exposed, so growth cannot bypass admission.
use super::{Budget, Reservation};
use crate::encoding::v2::values::indexes::equality;
use crate::error::Result;

mod builder;
pub(crate) use builder::Builder;

pub(crate) struct Bitmap {
    ids: roaring::RoaringTreemap,
    memory: Option<Reservation>,
}

impl std::fmt::Debug for Bitmap {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Bitmap")
            .field("ids", &self.ids)
            .finish_non_exhaustive()
    }
}

impl Bitmap {
    /// Read one compressed index row from the caller's storage snapshot. Raw
    /// bytes stay admitted during preflight and decoding, then the returned
    /// bitmap owns only its decoder allocation reservation.
    pub(crate) async fn read(
        reader: &(impl slatedb::DbReadOps + Sync),
        key: &[u8],
        budget: Option<&Budget>,
    ) -> Result<Self> {
        if let Some(budget) = budget {
            budget.record_reads(super::StorageReadUsage {
                point_gets: 1,
                ..Default::default()
            });
        }
        let Some(bytes) = reader.get(key).await? else {
            return Self::empty(budget);
        };
        let _raw = budget
            .map(|budget| budget.reserve(bytes.len()))
            .transpose()?;
        Self::decode_builtin(&bytes, budget)
    }

    pub(crate) fn empty(budget: Option<&Budget>) -> Result<Self> {
        let memory = budget
            .map(|budget| budget.reserve(size_of::<Self>()))
            .transpose()?;
        Ok(Self {
            ids: roaring::RoaringTreemap::new(),
            memory,
        })
    }

    pub(crate) fn decode(bytes: &[u8], budget: Option<&Budget>) -> Result<Self> {
        let prepared = equality::SecondaryEqualityBitmapValue::prepare(bytes)?;
        Self::from_prepared(prepared, budget)
    }

    /// Built-in index rows preserve the existing portable-prefix compatibility
    /// contract. Managed index rows use the stricter `decode` entry point.
    pub(crate) fn decode_builtin(bytes: &[u8], budget: Option<&Budget>) -> Result<Self> {
        Self::from_prepared(equality::SecondaryEqualityValue::prepare(bytes)?, budget)
    }

    fn from_prepared(
        prepared: equality::PreparedBitmap<'_>,
        budget: Option<&Budget>,
    ) -> Result<Self> {
        let memory = budget
            .map(|budget| {
                budget.reserve(
                    prepared
                        .allocation_bound()
                        .saturating_add(size_of::<Self>()),
                )
            })
            .transpose()?;
        let ids = prepared.decode()?.into_ids();
        Ok(Self { ids, memory })
    }

    /// Bridge for native paths that still construct their own bitmap. This
    /// admits retention, not construction; those paths require further migration.
    pub(crate) fn retain_legacy(
        ids: roaring::RoaringTreemap,
        budget: Option<&Budget>,
    ) -> Result<Self> {
        let Some(budget) = budget else {
            return Ok(Self { ids, memory: None });
        };
        let memory = Some(budget.reserve(operation_bound(&ids))?);
        // Native producers can retain opaque spare capacity after filtering.
        // A fresh clone compacts their vectors to current lengths, so the new
        // owner's bound is independent of that history. Construction of the
        // legacy input itself remains an explicit migration gap.
        Ok(Self {
            ids: ids.clone(),
            memory,
        })
    }

    /// Used only by legacy, unbudgeted storage APIs. An admitted bitmap cannot
    /// lose its guard through this compatibility adapter.
    pub(crate) fn into_unbudgeted(self) -> roaring::RoaringTreemap {
        assert!(
            self.memory.is_none(),
            "admitted bitmap escaped its resource owner"
        );
        self.ids
    }

    pub(crate) fn singleton(id: u64, budget: Option<&Budget>) -> Result<Self> {
        // One tree node, one growing container vector and one short array.
        let memory = budget.map(|budget| budget.reserve(2048)).transpose()?;
        Ok(Self {
            ids: [id].into_iter().collect(),
            memory,
        })
    }

    pub(crate) fn union(self, other: Self) -> Result<Self> {
        self.combine(other, false)
    }

    pub(crate) fn intersect(self, other: Self) -> Result<Self> {
        self.combine(other, true)
    }

    fn combine(self, other: Self, intersection: bool) -> Result<Self> {
        let budget = match (&self.memory, &other.memory) {
            (Some(left), Some(right)) => {
                assert!(
                    std::sync::Arc::ptr_eq(&left.budget.0, &right.budget.0),
                    "bitmap operation crossed request budgets"
                );
                Some(&left.budget)
            }
            (Some(memory), None) | (None, Some(memory)) => Some(&memory.budget),
            (None, None) => None,
        };
        let Some(budget) = budget else {
            return Ok(Self {
                ids: if intersection {
                    self.ids & other.ids
                } else {
                    self.ids | other.ids
                },
                memory: None,
            });
        };
        if self.memory.is_some()
            && other.memory.is_some()
            && (self.ids.is_empty() || other.ids.is_empty())
        {
            return if intersection {
                if self.ids.is_empty() {
                    Ok(self)
                } else {
                    Ok(other)
                }
            } else if self.ids.is_empty() {
                Ok(other)
            } else {
                Ok(self)
            };
        }
        // Build a fresh output from borrowed inputs. This bounds spare capacity
        // by the current containers, independent of earlier intersections. Both
        // input guards remain alive while output and conversion scratch allocate.
        let bound = operation_bound(&self.ids).saturating_add(operation_bound(&other.ids));
        let memory = Some(budget.reserve(bound)?);
        let ids = if intersection {
            &self.ids & &other.ids
        } else {
            &self.ids | &other.ids
        };
        assert!(
            super::bitmap_bytes(&ids) <= bound,
            "bitmap operation exceeded admission bound"
        );
        Ok(Self { ids, memory })
    }
}

/// Locked roaring 0.11.3 operations can promote runs/arrays to 8 KiB bitsets,
/// grow container vectors geometrically and retain temporary cloned containers.
/// Four times the current payload plus a bitset allowance for every run and
/// container/tree overhead bounds those allocations without expanding IDs.
fn operation_bound(ids: &roaring::RoaringTreemap) -> usize {
    ids.bitmaps()
        .fold(size_of::<Bitmap>(), |total, (_, bitmap)| {
            let stats = bitmap.statistics();
            let payload = stats
                .n_bytes_array_containers
                .saturating_add(stats.n_bytes_run_containers)
                .saturating_add(
                    u64::from(stats.n_bitset_containers + stats.n_run_containers)
                        .saturating_mul(65536 / u64::from(u8::BITS)),
                );
            total
                .saturating_add(
                    usize::try_from(payload)
                        .unwrap_or(usize::MAX)
                        .saturating_mul(4),
                )
                .saturating_add((stats.n_containers as usize).saturating_mul(1024))
                .saturating_add(2048)
        })
}

impl std::ops::Deref for Bitmap {
    type Target = roaring::RoaringTreemap;
    fn deref(&self) -> &Self::Target {
        &self.ids
    }
}

pub(crate) struct IntoIter {
    ids: roaring::treemap::IntoIter,
    _memory: Option<Reservation>,
}
impl IntoIterator for Bitmap {
    type Item = u64;
    type IntoIter = IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            ids: self.ids.into_iter(),
            _memory: self.memory,
        }
    }
}
impl Iterator for IntoIter {
    type Item = u64;
    fn next(&mut self) -> Option<Self::Item> {
        self.ids.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.ids.size_hint()
    }
}

/// Monotone ID builder for storage's ordered legacy equality entries. Admission
/// follows container growth without rescanning or copying earlier IDs. The
/// current container's cardinality determines array capacity; promoting to a
/// bitset reserves both the grown 16 KiB array and the new 8 KiB bitset.
pub(crate) struct SortedBuilder {
    bitmap: Bitmap,
    last: Option<u64>,
    cardinality: usize,
    payload: usize,
}
impl SortedBuilder {
    pub(crate) fn new(budget: Option<&Budget>) -> Result<Self> {
        Ok(Self {
            bitmap: Bitmap::empty(budget)?,
            last: None,
            cardinality: 0,
            payload: 0,
        })
    }

    pub(crate) fn push(&mut self, id: u64) -> Result<()> {
        if self.last.is_some_and(|previous| previous >= id) {
            return Err(crate::error::HelixDbError::IndexCatalogCorruption(
                "equality entry IDs are not strictly increasing".into(),
            ));
        }
        let partition = self.last.is_none_or(|previous| previous >> 32 != id >> 32);
        let container = self.last.is_none_or(|previous| previous >> 16 != id >> 16);
        let cardinality = if container { 1 } else { self.cardinality + 1 };
        let payload = if cardinality <= 4096 {
            // Covers old and new vectors during geometric growth as well as
            // the conservative retained-allocation estimate.
            4 * cardinality.next_power_of_two().max(4)
        } else {
            // The insertion grows the array before converting its contents.
            3 * 8192
        };
        let additional = usize::from(partition) * 2048 + usize::from(container) * 1024 + payload
            - if container { 0 } else { self.payload };
        if let Some(memory) = &mut self.bitmap.memory {
            memory.resize(memory.bytes.saturating_add(additional))?;
        }
        assert!(
            self.bitmap.ids.try_push(id).is_ok(),
            "ordered bitmap insertion rejected a validated ID"
        );
        self.last = Some(id);
        self.cardinality = cardinality;
        self.payload = payload;
        Ok(())
    }

    pub(crate) fn finish(self) -> Bitmap {
        self.bitmap
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::HelixDbError;

    #[test]
    fn decoding_admits_before_validation_and_iteration_owns_the_charge() {
        let bytes =
            equality::SecondaryEqualityBitmapValue::new([1, 2, 3].into_iter().collect()).encode();
        let mut malformed = bytes.to_vec();
        // Portable array payload is last: duplicate its first two values while
        // preserving a structurally complete buffer. The real decoder rejects it.
        let length = malformed.len();
        malformed[length - 3 * size_of::<u16>()..length - 2 * size_of::<u16>()]
            .copy_from_slice(&2_u16.to_le_bytes());
        let budget = Budget::new(1_000_000);
        assert!(Bitmap::decode(&malformed, Some(&budget)).is_err());
        assert_eq!(budget.available(), 1_000_000);
        let too_small = Budget::new(1);
        assert!(matches!(
            Bitmap::decode(&malformed, Some(&too_small)),
            Err(HelixDbError::QueryMemoryLimitExceeded)
        ));
        assert!(Bitmap::decode(&[0], Some(&budget)).is_err());
        let bitmap = Bitmap::decode(&bytes, Some(&budget)).unwrap();
        assert!(format!("{bitmap:?}").starts_with("Bitmap { ids:"));
        let available = budget.available();
        assert!(available < 1_000_000);
        let mut cursor = bitmap.into_iter();
        assert_eq!(cursor.size_hint(), (3, Some(3)));
        assert_eq!(cursor.next(), Some(1));
        assert_eq!(budget.available(), available);
        drop(cursor); // LIMIT/cancellation releases unread containers as well.
        assert_eq!(budget.available(), 1_000_000);
        assert_eq!(
            Bitmap::decode(&bytes, None)
                .unwrap()
                .into_unbudgeted()
                .len(),
            3
        );
        assert!(Bitmap::singleton(0, Some(&too_small)).is_err());
        assert!(Bitmap::empty(Some(&too_small)).is_err());
        assert!(Bitmap::retain_legacy([1].into_iter().collect(), Some(&too_small)).is_err());
        assert!(Bitmap::retain_legacy(roaring::RoaringTreemap::new(), None)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn set_operations_match_an_independent_oracle_and_release_all_input_guards() {
        let mut runs = (0..200_000).collect::<roaring::RoaringBitmap>();
        runs.optimize();
        let fixtures: Vec<roaring::RoaringTreemap> = vec![
            roaring::RoaringTreemap::new(),
            [0, 3, u64::MAX].into_iter().collect(),
            (0..20_000).map(|id| id * 2).collect(),
            (0..1000).map(|id| (id << 32) + id).collect(),
            roaring::RoaringTreemap::from_bitmaps([(0, runs)]),
        ];
        let budget = Budget::new(64 * 1024 * 1024);
        let limit = budget.available();
        for left in &fixtures {
            let oracle_left = left.iter().collect::<std::collections::BTreeSet<_>>();
            for right in &fixtures {
                let oracle_right = right.iter().collect::<std::collections::BTreeSet<_>>();
                for intersection in [true, false] {
                    let expected = if intersection {
                        oracle_left
                            .intersection(&oracle_right)
                            .copied()
                            .collect::<Vec<_>>()
                    } else {
                        oracle_left
                            .union(&oracle_right)
                            .copied()
                            .collect::<Vec<_>>()
                    };
                    for admission in [None, Some(&budget)] {
                        let left = Bitmap::decode(
                            &equality::SecondaryEqualityBitmapValue::new(left.clone()).encode(),
                            admission,
                        )
                        .unwrap();
                        let right = Bitmap::decode(
                            &equality::SecondaryEqualityBitmapValue::new(right.clone()).encode(),
                            admission,
                        )
                        .unwrap();
                        let output = if intersection {
                            left.intersect(right)
                        } else {
                            left.union(right)
                        }
                        .unwrap();
                        assert_eq!(output.into_iter().collect::<Vec<_>>(), expected);
                        assert_eq!(budget.available(), limit);
                    }
                }
            }
        }
        // Both transitional directions still reserve the result in the active
        // budget; an unbudgeted input cannot drop the other input's ownership.
        for reverse in [false, true] {
            let owned = Bitmap::singleton(1, Some(&budget)).unwrap();
            let legacy = Bitmap::singleton(2, None).unwrap();
            let output = if reverse {
                legacy.union(owned)
            } else {
                owned.union(legacy)
            }
            .unwrap();
            assert_eq!(output.iter().collect::<Vec<_>>(), vec![1, 2]);
            assert!(budget.available() < limit);
            drop(output);
            assert_eq!(budget.available(), limit);
        }
        let budget = Budget::new(4096);
        let first = Bitmap::singleton(1, Some(&budget)).unwrap();
        let second = Bitmap::singleton(2, Some(&budget)).unwrap();
        assert!(matches!(
            first.union(second),
            Err(HelixDbError::QueryMemoryLimitExceeded)
        ));
        assert_eq!(budget.available(), 4096);
    }

    #[test]
    #[should_panic(expected = "crossed request budgets")]
    fn different_query_budgets_cannot_be_combined() {
        let left = Budget::new(10000);
        let right = Budget::new(10000);
        let left = Bitmap::singleton(1, Some(&left)).unwrap();
        let right = Bitmap::singleton(2, Some(&right)).unwrap();
        let _ = left.union(right);
    }

    #[test]
    #[should_panic(expected = "escaped its resource owner")]
    fn legacy_adapter_cannot_discard_an_admitted_owner() {
        let budget = Budget::new(10000);
        Bitmap::singleton(1, Some(&budget))
            .unwrap()
            .into_unbudgeted();
    }

    #[test]
    fn sorted_builder_admits_container_growth_and_preserves_state_on_failure() {
        let budget = Budget::new(1_000_000);
        let mut builder = SortedBuilder::new(Some(&budget)).unwrap();
        let ids = (0..100_000).chain([1 << 32, u64::MAX]).collect::<Vec<_>>();
        for id in &ids {
            builder.push(*id).unwrap();
        }
        let owned = builder.finish();
        assert!(super::super::bitmap_bytes(&owned) < 1_000_000 - budget.available());
        assert_eq!(owned.into_iter().collect::<Vec<_>>(), ids);
        assert_eq!(budget.available(), 1_000_000);
        for admission in [None, Some(&budget)] {
            let mut builder = SortedBuilder::new(admission).unwrap();
            builder.push(2).unwrap();
            assert!(builder.push(1).is_err());
            assert!(builder.push(2).is_err());
            builder.push(3).unwrap();
            assert_eq!(builder.finish().iter().collect::<Vec<_>>(), vec![2, 3]);
        }
        let small = Budget::new(3200);
        let mut builder = SortedBuilder::new(Some(&small)).unwrap();
        builder.push(0).unwrap();
        assert!(matches!(
            builder.push(u64::MAX),
            Err(HelixDbError::QueryMemoryLimitExceeded)
        ));
        builder.push(1).unwrap();
        assert_eq!(builder.finish().into_iter().collect::<Vec<_>>(), vec![0, 1]);
        assert_eq!(small.available(), 3200);
        assert!(SortedBuilder::new(Some(&Budget::new(0))).is_err());
        // The 4,097th insertion doubles the array before bitset conversion.
        for (available, succeeds) in [
            (3072 + size_of::<Bitmap>() + 3 * 8192 - 1, false),
            (3072 + size_of::<Bitmap>() + 3 * 8192, true),
        ] {
            let budget = Budget::new(available);
            let mut builder = SortedBuilder::new(Some(&budget)).unwrap();
            for id in 0..4096 {
                builder.push(id).unwrap();
            }
            assert_eq!(builder.push(4096).is_ok(), succeeds);
            assert_eq!(builder.finish().len(), 4096 + u64::from(succeeds));
            assert_eq!(budget.available(), available);
        }
    }
}
