//! Admission for single-member, last-write-wins bitmap delta construction.
//!
//! The locked roaring implementation uses only arrays and bitmaps when starting
//! empty and applying individual insertions/removals. Union membership grows
//! monotonically: an ID always remains in either additions or removals. Per-
//! container high-water counts therefore bound both sides without charging each
//! repeated flip or scanning earlier members. Decoding, composition, range
//! insertion and run optimization are deliberately outside this contract.
use super::{Budget, Reservation};
use crate::{encoding::v2::values::indexes::BitmapMembershipDelta, error::Result};
use helix_planner::relational::allocation;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Change {
    Present,
    Absent,
}

#[derive(Default)]
pub(crate) struct Delta {
    value: BitmapMembershipDelta,
    // Boxed only for budgeted callers; native unbudgeted rows retain no ledger.
    admitted: Option<Box<Admitted>>,
}
impl std::fmt::Debug for Delta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Delta")
            .field("value", &self.value)
            .finish_non_exhaustive()
    }
}
struct Admitted {
    counts: Counts,
    memory: Reservation,
}

/// Most graph rows touch one 16-bit container. Its counters fit inline; promote
/// only when a second distinct container needs hierarchical lookup.
#[derive(Default)]
enum Counts {
    #[default]
    Empty,
    One {
        container: u64,
        cardinality: usize,
    },
    Many {
        containers: BTreeMap<u64, usize>,
        partitions: BTreeMap<u32, usize>,
    },
}
#[derive(Default)]
struct Shape {
    cardinality: usize,
    in_partition: usize,
    containers: usize,
    partitions: usize,
}
impl Counts {
    fn shape(&self, container: u64) -> Shape {
        let partition = (container >> u16::BITS) as u32;
        match self {
            Self::Empty => Shape::default(),
            Self::One {
                container: previous,
                cardinality,
            } => Shape {
                cardinality: if *previous == container {
                    *cardinality
                } else {
                    0
                },
                in_partition: usize::from((*previous >> u16::BITS) as u32 == partition),
                containers: 1,
                partitions: 1,
            },
            Self::Many {
                containers,
                partitions,
            } => Shape {
                cardinality: containers.get(&container).copied().unwrap_or(0),
                in_partition: partitions.get(&partition).copied().unwrap_or(0),
                containers: containers.len(),
                partitions: partitions.len(),
            },
        }
    }

    fn apply(&mut self, growth: &Growth) {
        match self {
            Self::Empty => {
                assert_eq!(growth.cardinality, 1);
                *self = Self::One {
                    container: growth.container,
                    cardinality: 1,
                };
            }
            Self::One {
                container,
                cardinality,
            } if *container == growth.container => {
                assert_eq!(*cardinality + 1, growth.cardinality);
                *cardinality = growth.cardinality;
            }
            Self::One {
                container,
                cardinality,
            } => {
                assert_eq!(growth.cardinality, 1);
                // Single insertions avoid a temporary bulk-collection buffer.
                // Both tree allocations were included in the promotion proposal.
                let mut containers = BTreeMap::new();
                containers.insert(*container, *cardinality);
                containers.insert(growth.container, growth.cardinality);
                let mut partitions = BTreeMap::new();
                partitions.insert((*container >> u16::BITS) as u32, 1);
                partitions.insert(growth.partition, growth.containers_in_partition);
                *self = Self::Many {
                    containers,
                    partitions,
                };
            }
            Self::Many {
                containers,
                partitions,
            } => {
                let previous = containers
                    .insert(growth.container, growth.cardinality)
                    .unwrap_or(0);
                assert_eq!(
                    previous + 1,
                    growth.cardinality,
                    "prepared union cardinality"
                );
                partitions.insert(growth.partition, growth.containers_in_partition);
            }
        }
    }
}
struct Growth {
    container: u64,
    cardinality: usize,
    partition: u32,
    containers_in_partition: usize,
    memory: Reservation,
}

/// A provisional reservation borrows its owner until applied or abandoned.
/// Preparing both adjacency directions before applying either preserves atomic
/// collection on admission failure; dropping a proposal changes no membership.
#[must_use = "apply the change or drop it to abandon its reservation"]
pub(crate) struct PreparedChange<'a> {
    owner: &'a mut Delta,
    id: u64,
    change: Change,
    growth: Option<Growth>,
}
impl PreparedChange<'_> {
    pub(crate) fn apply(self) {
        let Self {
            owner,
            id,
            change,
            growth,
        } = self;
        // The provisional guard already covers both the raw mutation and its
        // bookkeeping before either can allocate. Both operations are infallible.
        match change {
            Change::Present => owner.value.add(id),
            Change::Absent => owner.value.remove(id),
        }
        let Some(growth) = growth else {
            return;
        };
        let admitted = owner.admitted.as_mut().expect("growth requires admission");
        admitted.counts.apply(&growth);
        admitted.memory.absorb(growth.memory);
    }
}

impl Delta {
    pub(crate) fn new(budget: Option<&Budget>) -> Result<Self> {
        let admitted = budget
            .map(|budget| {
                let memory = budget.reserve(size_of::<Admitted>())?;
                Ok::<_, crate::HelixDbError>(Box::new(Admitted {
                    counts: Counts::Empty,
                    memory,
                }))
            })
            .transpose()?;
        Ok(Self {
            value: BitmapMembershipDelta::default(),
            admitted,
        })
    }

    pub(crate) fn prepare(&mut self, id: u64, change: Change) -> Result<PreparedChange<'_>> {
        let growth = match self.admitted.as_ref() {
            Some(admitted) if !self.value.contains_member(id) => {
                let container = id >> u16::BITS;
                let partition = (id >> u32::BITS) as u32;
                let previous = admitted.counts.shape(container);
                let cardinality = previous.cardinality + 1;
                assert!(
                    cardinality <= 1 << u16::BITS,
                    "union cardinality fits a container"
                );
                let containers_in_partition =
                    previous.in_partition + usize::from(previous.cardinality == 0);
                let container_count = previous.containers + usize::from(previous.cardinality == 0);
                let partition_count = previous.partitions + usize::from(previous.in_partition == 0);
                let additional = payload_bound(cardinality) - payload_bound(previous.cardinality)
                    + vector_bound(containers_in_partition)
                    - vector_bound(previous.in_partition)
                    + tracking_bound(container_count, partition_count)
                    - tracking_bound(previous.containers, previous.partitions)
                    + 2 * (tree_bound::<u32, roaring::RoaringBitmap>(partition_count)
                        - tree_bound::<u32, roaring::RoaringBitmap>(previous.partitions));
                Some(Growth {
                    container,
                    cardinality,
                    partition,
                    containers_in_partition,
                    memory: admitted.memory.budget.reserve(additional)?,
                })
            }
            _ => None,
        };
        Ok(PreparedChange {
            owner: self,
            id,
            change,
            growth,
        })
    }

    /// Transfer the raw delta and its admission together to an epoch owner.
    /// The caller must retain the guard until the raw delta has been consumed.
    pub(crate) fn into_parts(self) -> (BitmapMembershipDelta, Option<Reservation>) {
        let Self { value, admitted } = self;
        let memory = admitted.map(|admitted| admitted.memory);
        (value, memory)
    }
}

fn tree_bound<K, V>(count: usize) -> usize {
    if count == 0 {
        0
    } else {
        allocation::btree_bytes::<K, V>(count)
    }
}

fn tracking_bound(containers: usize, partitions: usize) -> usize {
    if containers <= 1 {
        return 0;
    }
    tree_bound::<u64, usize>(containers) + tree_bound::<u32, usize>(partitions)
}

fn payload_bound(cardinality: usize) -> usize {
    match cardinality {
        0 => 0,
        // Two sides, each retaining old + new geometric array capacity.
        1..=4096 => 8 * cardinality.next_power_of_two().max(4),
        // At 4,097 entries, a 16 KiB grown array overlaps an 8 KiB bitset.
        // Include this transition on both sides and bitset-to-array conversion.
        _ => 2 * 3 * 8192,
    }
}

fn vector_bound(containers: usize) -> usize {
    if containers == 0 {
        return 0;
    }
    // The locked Container is at most eight words (tag, array/bitset storage,
    // key and padding). Both sides may retain full union capacity. Reserve old
    // and new vectors together when either grows; removals never shrink capacity.
    4 * containers.next_power_of_two().max(4) * size_of::<[usize; 8]>()
}

#[cfg(test)]
mod tests;
