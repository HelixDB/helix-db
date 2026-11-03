//! Admission for canonical disjoint membership merges and their history.
//!
//! A batch owns transient frames, discriminator construction and validation
//! working memory. Its transaction retains copied operands, operation histories
//! and conflict metadata through commit. Monotone totals bound unique
//! keys/tokens by all submitted entries, without another per-member or per-key
//! index.
//!
//! Staging never reads the stored row. Each operand is validated on its own with
//! the merge operator's decoders, so a write costs the size of its delta, not
//! the size of the row it changes. Stored rows stay decodable because full-value
//! puts to these rows are validated by [`super::Mutation::put`]. Upstream
//! secondary-index collection is outside this construction scope.

use std::collections::HashSet;

use bytes::Bytes;
use helix_planner::relational::allocation;
use parking_lot::Mutex;
use slatedb::MergeOperator;

use crate::{
    encoding::v2::{
        keys::DataKey,
        values::{adjacency, indexes::equality},
    },
    merge_operator::{HelixMergeOperator, MembershipRow},
    query_resources::{Budget, Reservation},
    HelixDbError, Result,
};

#[cfg(test)]
mod tests;

/// Typed topology keys are encoded only after admission. Secondary-index keys
/// have already been constructed upstream; their retained alias is admitted.
pub(crate) enum Key<'a> {
    Typed(DataKey<'a>),
    Encoded(&'a Bytes),
}

struct KeyOwner {
    bytes: Bytes,
    _memory: Reservation,
}
impl AsRef<[u8]> for KeyOwner {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Key<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Typed(key) => key.encoded_len(),
            Self::Encoded(key) => key.len(),
        }
    }

    fn encode(self, budget: Option<&Budget>) -> Result<Bytes> {
        // Owner + reference counts and the underlying Bytes sharing header.
        // Admission precedes both typed encoding and a pre-encoded key's clone.
        let memory = budget
            .map(|budget| {
                budget.reserve(
                    self.len()
                        .saturating_add(size_of::<KeyOwner>())
                        .saturating_add(size_of::<[usize; 6]>()),
                )
            })
            .transpose()?;
        let bytes = match self {
            Self::Typed(key) => key.to_bytes(),
            Self::Encoded(key) => key.clone(),
        };
        Ok(match memory {
            Some(memory) => Bytes::from_owner(KeyOwner {
                bytes,
                _memory: memory,
            }),
            None => bytes,
        })
    }
}

#[derive(Clone, Copy, Default)]
struct Totals {
    entries: usize,
    keys: usize,
    operands: usize,
    tokens: usize,
}
impl Totals {
    fn plus(self, other: Self) -> Result<Self> {
        Ok(Self {
            entries: self
                .entries
                .checked_add(other.entries)
                .ok_or(HelixDbError::QueryMemoryLimitExceeded)?,
            keys: self
                .keys
                .checked_add(other.keys)
                .ok_or(HelixDbError::QueryMemoryLimitExceeded)?,
            operands: self
                .operands
                .checked_add(other.operands)
                .ok_or(HelixDbError::QueryMemoryLimitExceeded)?,
            tokens: self
                .tokens
                .checked_add(other.tokens)
                .ok_or(HelixDbError::QueryMemoryLimitExceeded)?,
        })
    }

    fn history_bytes(self) -> usize {
        if self.entries == 0 {
            return size_of::<Tracker>();
        }
        // Pinned SlateDB: B-tree<Bytes, SmallVec<[WriteOp; 1]>>, optional
        // HashMap<Bytes, MergeConflictKind>, growing per-key operation arrays.
        // Sixteen words bound an operation/header. Count every operation as a
        // distinct key: repeat epochs remain linear in retained write history.
        // Two trees cover commit clones. Six hash tables cover compatibility
        // metadata, its clone, submitted writes, transaction write state and
        // its commit clone, plus growth overlap. The transient keys() set is
        // separate. Four operation/token arrays cover geometric growth,
        // old/new unions and Vec-to-Arc overlap.
        size_of::<Tracker>()
            .saturating_add(
                allocation::btree_bytes::<Bytes, [usize; 16]>(self.entries).saturating_mul(2),
            )
            .saturating_add(
                allocation::hash_table_retained_bytes::<Bytes, [usize; 8]>(self.entries)
                    .saturating_mul(6),
            )
            .saturating_add(allocation::hash_table_bytes::<Bytes, ()>(self.entries))
            .saturating_add(self.entries.saturating_mul(4 * size_of::<[usize; 16]>()))
            .saturating_add(self.keys)
            .saturating_add(self.operands)
            .saturating_add(self.tokens.saturating_mul(4 * size_of::<u128>()))
    }
}

pub(super) struct Tracker {
    state: Mutex<History>,
}
struct History {
    totals: Totals,
    memory: Reservation,
}
impl Tracker {
    pub(super) fn new(budget: &Budget) -> Result<Self> {
        let memory = budget.reserve(size_of::<Self>())?;
        Ok(Self {
            state: Mutex::new(History {
                totals: Totals::default(),
                memory,
            }),
        })
    }
    fn retain(&self, incoming: Totals) -> Result<()> {
        let mut history = self.state.lock();
        let proposed = history.totals.plus(incoming)?;
        history.memory.resize(proposed.history_bytes())?;
        history.totals = proposed;
        Ok(())
    }
}

/// Membership row family of one operand. It selects the allocation-free
/// preflight that bounds the operand's validation decode before it runs, and
/// must match the row the operand merges into.
#[derive(Clone, Copy, Debug)]
enum Family {
    Bitmap,
    Adjacency,
}

/// One encoded operand awaiting validation and staging.
struct Entry {
    family: Family,
    key: Bytes,
    tokens: Vec<u128>,
    operand: Bytes,
}

/// Closed, capacity-bounded construction followed by all-or-nothing staging.
/// Key aliases carry their own admission beyond this batch's lifetime.
pub(crate) struct Batch<'a> {
    view: super::View<'a>,
    entries: Vec<Entry>,
    capacity: usize,
    totals: Totals,
    memory: Option<Reservation>,
}
impl<'a> Batch<'a> {
    pub(super) fn new(view: super::View<'a>, capacity: usize) -> Result<Self> {
        let memory = view
            .tracking
            .map(|tracker| {
                tracker
                    .budget
                    .reserve(Self::construction_bytes(capacity, Totals::default()))
            })
            .transpose()?;
        Ok(Self {
            view,
            entries: Vec::with_capacity(capacity),
            capacity,
            totals: Totals::default(),
            memory,
        })
    }

    fn construction_bytes(capacity: usize, totals: Totals) -> usize {
        // Entry Vec plus the duplicate-key set built while staging. Encoders
        // write exact frames; another frame bounds roaring run-header scratch
        // even for decoded/optimized deltas. Keys have separate owners. Each
        // entry's token Vec is reused in place by the backend before its Arc.
        capacity
            .saturating_mul(4 * size_of::<Entry>() + 4 * size_of::<Bytes>())
            .saturating_add(allocation::hash_table_bytes::<Bytes, ()>(capacity))
            .saturating_add(totals.operands.saturating_mul(2))
            .saturating_add(totals.tokens.saturating_mul(4 * size_of::<u128>()))
            .saturating_add(totals.entries.saturating_mul(size_of::<[usize; 8]>()))
    }

    pub(crate) fn bitmap(
        &mut self,
        key: Key<'_>,
        delta: &equality::BitmapMembershipDelta,
    ) -> Result<Bytes> {
        let encoded = delta.prepare_encoding();
        self.push(
            Family::Bitmap,
            key,
            encoded.encoded_len(),
            delta.members().map(u128::from),
            || encoded.encode(),
        )
    }

    pub(crate) fn adjacency(
        &mut self,
        key: Key<'_>,
        delta: &adjacency::AdjacencyMembershipDelta,
    ) -> Result<Bytes> {
        let encoded = delta.prepare_encoding();
        // Direction is part of conflict identity, including self-loop members.
        const INCOMING: u128 = 1_u128 << u64::BITS;
        let tokens = delta
            .outgoing_members()
            .map(u128::from)
            .chain(delta.incoming_members().map(|id| INCOMING | u128::from(id)));
        self.push(
            Family::Adjacency,
            key,
            encoded.encoded_len(),
            tokens,
            || encoded.encode(),
        )
    }

    fn push(
        &mut self,
        family: Family,
        key: Key<'_>,
        operand_len: usize,
        tokens: impl Iterator<Item = u128>,
        encode: impl FnOnce() -> Bytes,
    ) -> Result<Bytes> {
        assert!(
            self.entries.len() < self.capacity,
            "merge batch exceeds its declared capacity"
        );
        // The typed roaring iterators expose cardinality without walking IDs.
        // Preserve their size hints for the backend's one token collection pass.
        let proposed = match &mut self.memory {
            Some(memory) => {
                let tokens = tokens
                    .size_hint()
                    .1
                    .ok_or(HelixDbError::QueryMemoryLimitExceeded)?;
                let proposed = self.totals.plus(Totals {
                    entries: 1,
                    keys: key.len(),
                    operands: operand_len,
                    tokens,
                })?;
                memory.resize(Self::construction_bytes(self.capacity, proposed))?;
                proposed
            }
            None => self.totals,
        };
        let key = key.encode(self.view.tracking.map(|tracker| &tracker.budget))?;
        let operand = encode();
        assert_eq!(operand.len(), operand_len, "prepared merge operand length");
        let tokens = tokens.collect::<Vec<_>>();
        assert!(
            !tokens.is_empty(),
            "disjoint merge discriminators cannot be empty"
        );
        self.entries.push(Entry {
            family,
            key: key.clone(),
            tokens,
            operand,
        });
        self.totals = proposed;
        Ok(key)
    }

    /// Validates every entry, then stages each in push order.
    ///
    /// A batch with a repeated key, an operand whose family does not match its
    /// row, or an operand the merge operator cannot decode stages nothing and
    /// fails with one `Storage` invalid-argument error. Validation never reads
    /// the stored row. The construction reservation, grown by the largest
    /// validation decode before it runs, lives until the last operand is staged.
    ///
    /// The backend takes its write-batch lock once per operand; it has no
    /// read-free call that stages several under one lock. No other reader can
    /// observe a partly staged batch: this is synchronous, so nothing on the
    /// transaction's task interleaves, and Helix never shares a transaction
    /// across threads. Staging cannot fail midway either: the backend's only
    /// error is a database-wide missing merge operator, raised on the first
    /// operand before anything is staged.
    pub(crate) fn stage(self) -> Result<()> {
        let Self {
            view,
            entries,
            capacity,
            totals,
            mut memory,
        } = self;
        let invalid = |message: String| HelixDbError::Storage(slatedb::Error::invalid(message));
        let mut keys = HashSet::with_capacity(entries.len());
        entries.iter().try_for_each(|entry| {
            if !keys.insert(&entry.key) {
                return Err(invalid(format!(
                    "disjoint merge batch contains duplicate key {:?}",
                    entry.key
                )));
            }
            let row = MembershipRow::of(&entry.key);
            match (entry.family, row) {
                (Family::Bitmap, Some(MembershipRow::Bitmap(_)))
                | (Family::Adjacency, Some(MembershipRow::Adjacency)) => Ok(()),
                (Family::Bitmap | Family::Adjacency, _) => Err(invalid(format!(
                    "{:?} membership delta cannot merge into {row:?} row {:?}",
                    entry.family, entry.key
                ))),
            }
        })?;
        drop(keys);
        let undecodable = |error: Box<dyn std::error::Error + Send + Sync>| {
            HelixDbError::Storage(
                slatedb::Error::invalid("membership delta does not decode".to_owned())
                    .with_source(error),
            )
        };
        memory
            .as_mut()
            .map(|memory| {
                // Allocation-free preflight bounds each decode; one runs at a time.
                let decode = entries.iter().try_fold(0, |largest, entry| {
                    let bound = match entry.family {
                        Family::Bitmap => {
                            equality::SecondaryEqualityBitmapValue::prepare(&entry.operand)
                                .map(|prepared| prepared.allocation_bound())
                        }
                        Family::Adjacency => adjacency::prepare_edges(&entry.operand)
                            .map(|prepared| prepared.allocation_bound()),
                    }
                    .map_err(|error| undecodable(Box::new(error)))?;
                    Ok::<_, HelixDbError>(largest.max(bound))
                })?;
                memory.resize(Self::construction_bytes(capacity, totals).saturating_add(decode))
            })
            .transpose()?;
        let operator = HelixMergeOperator::new();
        entries.iter().try_for_each(|entry| {
            operator
                .validate_merge_with_base(&entry.key, None, std::slice::from_ref(&entry.operand))
                .map_err(|error| undecodable(Box::new(error)))
        })?;
        view.merges
            .map(|tracker| tracker.retain(totals))
            .transpose()?;
        entries.into_iter().try_for_each(|entry| {
            view.raw
                .merge_disjoint_tokens(entry.key, entry.tokens, entry.operand)
        })?;
        drop(memory);
        Ok(())
    }
}
