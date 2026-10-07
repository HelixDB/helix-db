//! Compact immutable scope snapshots with a lazy column-map compatibility view.
use crate::relational as r;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, OnceLock},
};

const WORD_BITS: usize = u64::BITS as usize;

/// A row's visible bindings, types and nullability. Clones share immutable
/// storage and outlive the query that produced them. Sparse binding IDs remain
/// distinct when a frontend shadows a name. Compilation shares catalog facts;
/// scopes store only membership and optional-match nullability overrides.
///
/// ```
/// use helix_planner::relational as r;
/// let query = r::Query::new(
///     vec![r::Binding {
///         name: "x".into(), kind: r::BindingType::Scalar,
///         nullable: false, value_type: r::ValueType::Integer,
///     }],
///     vec![r::Operator::Unwind {
///         expression: r::Expression::Literal(r::Value::List(vec![r::Value::Integer(1)])),
///         slot: r::Slot(0),
///     }],
///     vec![],
/// )?;
/// let schema = query.contracts()[0].output().clone();
/// drop(query);
/// assert_eq!(schema.get(r::Slot(0)).unwrap().value_type, r::ValueType::Integer);
/// assert_eq!(schema.iter().count(), 1);
/// assert_eq!(schema.columns().len(), 1);
/// # Ok::<(), r::QueryError>(())
/// ```
#[derive(Clone)]
pub struct RowSchema(Arc<Snapshot>);

struct Snapshot {
    catalog: Arc<[r::ColumnType]>,
    members: SlotSet,
    nullable: SlotSet,
    columns: OnceLock<BTreeMap<r::Slot, r::ColumnType>>,
}

impl RowSchema {
    pub(in crate::relational) fn empty(bindings: &[r::Binding]) -> Self {
        Self(Arc::new(Snapshot {
            catalog: bindings
                .iter()
                .map(|binding| r::ColumnType {
                    value_type: binding.value_type,
                    nullable: binding.nullable,
                })
                .collect(),
            members: SlotSet::Empty,
            nullable: SlotSet::Empty,
            columns: OnceLock::new(),
        }))
    }

    pub(in crate::relational) fn derive(&self, output: &BTreeSet<r::Slot>, optional: bool) -> Self {
        if output.len() == self.len() && output.iter().copied().eq(self.slot_iter()) {
            return self.clone();
        }
        assert!(
            output
                .last()
                .is_none_or(|slot| (slot.0 as usize) < self.0.catalog.len()),
            "query validation checks every scope output before deriving its schema"
        );
        let members = SlotSet::new(output.iter().copied());
        // Catalog nullability is shared. Store only facts introduced by an
        // optional boundary, preserving them for incoming bindings.
        let nullable = SlotSet::new(output.iter().copied().filter(|&slot| {
            !self.0.catalog[slot.0 as usize].nullable
                && if self.0.members.contains(slot) {
                    self.0.nullable.contains(slot)
                } else {
                    optional
                }
        }));
        Self(Arc::new(Snapshot {
            catalog: self.0.catalog.clone(),
            members,
            nullable,
            columns: OnceLock::new(),
        }))
    }

    /// Visible columns in binding-ID order. The compatibility map is built
    /// once on demand and shared by clones. Use `iter`, `get` or `slots` when a
    /// retained map is unnecessary.
    pub fn columns(&self) -> &BTreeMap<r::Slot, r::ColumnType> {
        self.0.columns.get_or_init(|| self.iter().collect())
    }

    /// Number of visible bindings, without constructing a column map.
    pub fn len(&self) -> usize {
        self.0.members.len()
    }

    /// Whether this scope has no visible bindings.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Read the facts for one visible binding without expanding the scope.
    pub fn get(&self, slot: r::Slot) -> Option<r::ColumnType> {
        if !self.0.members.contains(slot) {
            return None;
        }
        let mut column = self.0.catalog[slot.0 as usize];
        column.nullable |= self.0.nullable.contains(slot);
        Some(column)
    }

    /// Iterate visible columns in binding-ID order without allocating.
    pub fn iter(
        &self,
    ) -> impl ExactSizeIterator<Item = (r::Slot, r::ColumnType)> + std::iter::FusedIterator + '_
    {
        self.slot_iter()
            .map(|slot| (slot, self.get(slot).expect("member belongs to its catalog")))
    }

    /// Own the visible binding IDs. This preserves the existing set contract
    /// without constructing the column-map compatibility view.
    pub fn slots(&self) -> BTreeSet<r::Slot> {
        self.slot_iter().collect()
    }

    pub(in crate::relational) fn slot_iter(&self) -> Slots<'_> {
        self.0.members.iter()
    }
}

// Keep common empty and singleton scopes inline. Large sparse IDs use a
// sorted array; dense scopes use one bit per catalog entry. Boxing the large
// variants keeps every snapshot small, including long alias-only pipelines.
enum SlotSet {
    Empty,
    Single(r::Slot),
    Many(Box<ManySlots>),
}
enum ManySlots {
    Sparse(Box<[r::Slot]>),
    Dense { words: Box<[u64]>, len: usize },
}
impl SlotSet {
    // Callers supply ordered, unique slots from validated output sets. The
    // assertion also protects future internal callers before facts are lost.
    fn new(slots: impl Iterator<Item = r::Slot> + Clone) -> Self {
        let len = slots.clone().count();
        let Some(last) = slots.clone().last() else {
            return Self::Empty;
        };
        if len == 1 {
            return Self::Single(last);
        }
        let words = last.0 as usize / WORD_BITS + 1;
        let mut previous = None;
        let checked = slots.inspect(|&slot| {
            assert!(
                previous.is_none_or(|previous| previous < slot),
                "scope slots are ordered and unique"
            );
            previous = Some(slot);
        });
        let many = if len * size_of::<r::Slot>() < words * size_of::<u64>() {
            ManySlots::Sparse(checked.collect())
        } else {
            let mut bits = vec![0_u64; words].into_boxed_slice();
            for slot in checked {
                bits[slot.0 as usize / WORD_BITS] |= 1_u64 << (slot.0 as usize % WORD_BITS);
            }
            ManySlots::Dense { words: bits, len }
        };
        Self::Many(Box::new(many))
    }
    fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Single(_) => 1,
            Self::Many(many) => match many.as_ref() {
                ManySlots::Sparse(slots) => slots.len(),
                ManySlots::Dense { len, .. } => *len,
            },
        }
    }
    fn contains(&self, slot: r::Slot) -> bool {
        match self {
            Self::Empty => false,
            Self::Single(member) => *member == slot,
            Self::Many(many) => match many.as_ref() {
                ManySlots::Sparse(slots) => slots.binary_search(&slot).is_ok(),
                ManySlots::Dense { words, .. } => words
                    .get(slot.0 as usize / WORD_BITS)
                    .is_some_and(|word| word & (1_u64 << (slot.0 as usize % WORD_BITS)) != 0),
            },
        }
    }
    fn iter(&self) -> Slots<'_> {
        match self {
            Self::Empty => Slots::Single(None),
            Self::Single(slot) => Slots::Single(Some(*slot)),
            Self::Many(many) => match many.as_ref() {
                ManySlots::Sparse(slots) => Slots::Sparse(slots.iter()),
                ManySlots::Dense { words, len } => Slots::Dense {
                    words,
                    word: 0,
                    pending: 0,
                    remaining: *len,
                },
            },
        }
    }
}

pub(in crate::relational) enum Slots<'a> {
    Single(Option<r::Slot>),
    Sparse(std::slice::Iter<'a, r::Slot>),
    Dense {
        words: &'a [u64],
        word: usize,
        pending: u64,
        remaining: usize,
    },
}
impl Iterator for Slots<'_> {
    type Item = r::Slot;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Single(slot) => slot.take(),
            Self::Sparse(slots) => slots.next().copied(),
            Self::Dense {
                words,
                word,
                pending,
                remaining,
            } => loop {
                if *pending != 0 {
                    let bit = pending.trailing_zeros() as usize;
                    *pending &= *pending - 1;
                    *remaining -= 1;
                    return Some(r::Slot(((*word - 1) * WORD_BITS + bit) as u32));
                }
                let &bits = words.get(*word)?;
                *word += 1;
                *pending = bits;
            },
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = match self {
            Self::Single(slot) => usize::from(slot.is_some()),
            Self::Sparse(slots) => slots.len(),
            Self::Dense { remaining, .. } => *remaining,
        };
        (remaining, Some(remaining))
    }
}
impl ExactSizeIterator for Slots<'_> {}
impl std::iter::FusedIterator for Slots<'_> {}

impl PartialEq for RowSchema {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
            || (self.len() == other.len() && self.iter().eq(other.iter()))
    }
}
impl Eq for RowSchema {}

// Both adapters stream the same canonical map; diagnostic formatting and
// serialization must not populate every scope's compatibility cache.
struct ColumnMap<'a>(&'a RowSchema);
impl fmt::Debug for ColumnMap<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.0.iter()).finish()
    }
}
impl fmt::Debug for RowSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RowSchema").field(&ColumnMap(self)).finish()
    }
}
impl serde::Serialize for ColumnMap<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(self.0.iter())
    }
}
impl serde::Serialize for RowSchema {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_newtype_struct("RowSchema", &ColumnMap(self))
    }
}

#[cfg(test)]
mod tests;
