//! Admission for canonical checked-disjoint merge construction and history.
//!
//! A batch owns transient frames, discriminator construction and backend batch
//! containers. Its transaction retains copied operands, operation histories and
//! conflict metadata through commit. Monotone totals bound unique keys/tokens
//! by all submitted entries, without another per-member or per-key index. This
//! deliberately retains a conservative bound after a rejected backend batch.
//!
//! SlateDB's untracked existing-row reads and full merge validation are separate
//! working memory: input lengths cannot bound an arbitrarily large existing row.
//! Upstream secondary-index collection is also outside this construction scope.

use bytes::Bytes;
use helix_planner::relational::allocation;
use parking_lot::Mutex;

use crate::{
    encoding::v2::{
        keys::DataKey,
        values::{adjacency, indexes::equality},
    },
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

/// Closed, capacity-bounded construction followed by one checked backend call.
/// Key aliases carry their own admission beyond this batch's lifetime.
pub(crate) struct Batch<'a> {
    view: super::View<'a>,
    entries: Vec<slatedb::DisjointMergeBatchEntry>,
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
        // Input Vec plus growing backend prepared/key SmallVecs and key set.
        // Encoders write exact frames; another frame bounds roaring run-header
        // scratch even for decoded/optimized deltas. Keys have separate owners.
        capacity
            .saturating_mul(
                4 * size_of::<slatedb::DisjointMergeBatchEntry>() + 4 * size_of::<Bytes>(),
            )
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
        self.push(key, encoded.encoded_len(), tokens, || encoded.encode())
    }

    fn push(
        &mut self,
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
        self.entries
            .push(slatedb::DisjointMergeBatchEntry::from_tokens(
                key.clone(),
                tokens,
                operand,
            ));
        self.totals = proposed;
        Ok(key)
    }

    pub(crate) async fn stage(self) -> Result<()> {
        let Self {
            view,
            entries,
            totals,
            memory,
            ..
        } = self;
        view.merges
            .map(|tracker| tracker.retain(totals))
            .transpose()?;
        // No ledger lock crosses an await. A backend error leaves a conservative
        // history charge until the statement transaction aborts or completes.
        let result = view.raw.merge_disjoint_checked_batch(entries).await;
        drop(memory);
        Ok(result?)
    }
}
