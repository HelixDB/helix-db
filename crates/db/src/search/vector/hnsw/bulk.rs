//! Parallel in-memory HNSW construction for bulk vector index builds.
//!
//! A bulk build holds every source vector of one unpartitioned generation in
//! memory ([`BulkVectors`]), links them on a caller-owned set of OS threads
//! ([`BulkGraph::construct`]), and hands the finished graph to the lifecycle
//! writer, which stores each node once through [`VectorIndex::stage_bulk_node`].
//!
//! # Contract
//!
//! - Nodes are addressed by ordinal: their position in ascending entity-ID
//!   order, which is source-scan order. Ordinals order ties exactly as entity
//!   IDs do in the serial path.
//! - Construction applies the rules of the serial insert in
//!   [`super::mutation`]: the same degree limits and beam widths
//!   ([`MutationDegreeLimits`]), the caller's layer per node, a greedy descent
//!   above the node's layer, a beam search per layer truncated to twice the
//!   degree, the diverse-neighbor selection, and reciprocal links pruned by the
//!   same selection with mirrored removal. Inserts run concurrently, so the
//!   edges differ from the ones a serial build would choose; every stored row
//!   keeps the serial path's key and value encoding.
//! - A constructed graph is symmetric (a link exists only with its reverse),
//!   which the stored reverse locators require, and has no node without a
//!   layer-0 link unless it holds a single node. Its entry point is the first
//!   node in source order on the top layer.
//! - [`BulkGraph::unlink`] removes a node whose source changed after it was
//!   read and reports every row it changed, so the writer can restage rows it
//!   already stored.
//!
//! # Scaling
//!
//! Memory is [`BulkParameters::bytes_per_vector`] per node and is never
//! spilled; the caller admits a build against its budget before reading the
//! source. An insertion that raises the top layer serializes construction for
//! its duration, which happens about `ln(n)` times. Re-linking a node left
//! without layer-0 links is one beam search; the final sequential pass and
//! [`BulkGraph::unlink`] run it on one thread.

use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};

use parking_lot::{Mutex, MutexGuard, RwLock};

use super::index::VectorIndex;
use super::mutation::MutationDegreeLimits;
use crate::encoding::NodeId;
use crate::error::HelixDbError;
use crate::search::vector::distance::{ActiveVectorSemantics, Distance};
use crate::search::vector::item::Item;
use crate::search::vector::storage::VectorWriteRows;
use crate::search::vector::unaligned_vector::UnalignedVector;
use crate::search::vector::{
    encode_item, DistanceScore, MeasuredVectorTransaction, ValidatedMetricVector, VectorDimension,
    VectorIndexConfig, VectorIndexMetadata,
};

/// Most bytes one vector chunk holds, unless one vector is larger. Chunks keep
/// growth from copying or doubling the whole vector store while the source is
/// scanned, and bound the bytes its partly filled last chunk holds unused.
const CHUNK_BYTES: usize = 2 << 20;

/// Link-slot value of an unused slot. No ordinal reaches it.
const EMPTY: u32 = u32::MAX;

/// Distance-ranked ordinal: ordered by distance, then source order.
type Scored = (DistanceScore, u32);

/// Source vectors of one bulk build, in ascending entity-ID order.
pub(crate) struct BulkVectors<D: Distance> {
    dimension: usize,
    /// Binary logarithm of the vectors per chunk, so a node's chunk and
    /// offset are a shift and a mask.
    chunk_shift: u32,
    chunks: Vec<Vec<f32>>,
    headers: Vec<D::Header>,
    entity_ids: Vec<NodeId>,
    source_hashes: Vec<u64>,
    levels: Vec<u8>,
}

impl<D: Distance> BulkVectors<D> {
    /// Starts an empty store of `dimension`-component vectors.
    ///
    /// # Panics
    ///
    /// Panics when `dimension` is zero, which no index definition holds.
    pub(crate) fn new(dimension: usize) -> Self {
        assert!(dimension > 0, "bulk vector dimension");
        Self {
            dimension,
            chunk_shift: (CHUNK_BYTES / (dimension * size_of::<f32>()))
                .max(1)
                .ilog2(),
            chunks: Vec::new(),
            headers: Vec::new(),
            entity_ids: Vec::new(),
            source_hashes: Vec::new(),
            levels: Vec::new(),
        }
    }

    /// Number of vectors held.
    pub(crate) fn len(&self) -> usize {
        self.entity_ids.len()
    }

    /// Hash that identifies a source vector byte for byte, as queue
    /// publication compares vectors: `-0.0` differs from `0.0`.
    pub(crate) fn source_hash(vector: &[f32]) -> u64 {
        xxhash_rust::xxh3::xxh3_64(bytemuck::cast_slice(vector))
    }

    /// Appends the next source vector with the layer its node will occupy.
    ///
    /// # Panics
    ///
    /// Panics when `entity_id` does not ascend, `vector` has another
    /// dimension, or `level` exceeds the layer selection's cap: each means the
    /// caller broke source order or the index definition.
    pub(crate) fn push(&mut self, entity_id: NodeId, vector: &[f32], level: u16) {
        assert!(
            self.entity_ids.last().is_none_or(|last| *last < entity_id),
            "bulk vectors arrive in ascending entity order"
        );
        assert_eq!(vector.len(), self.dimension, "bulk vector dimension");
        let level = u8::try_from(level).expect("selected HNSW layers fit one byte");
        if self.len() & ((1 << self.chunk_shift) - 1) == 0 {
            self.chunks
                .push(Vec::with_capacity(self.dimension << self.chunk_shift));
        }
        self.chunks
            .last_mut()
            .expect("a chunk was opened for this vector")
            .extend_from_slice(vector);
        self.headers.push(D::new_header(
            &UnalignedVector::<D::VectorCodec>::from_slice(vector),
        ));
        self.entity_ids.push(entity_id);
        self.source_hashes.push(Self::source_hash(vector));
        self.levels.push(level);
    }

    fn item(&self, node: u32) -> Item<'_, D> {
        let node = node as usize;
        let offset = (node & ((1 << self.chunk_shift) - 1)) * self.dimension;
        Item {
            header: self.headers[node],
            vector: UnalignedVector::<D::VectorCodec>::from_slice(
                &self.chunks[node >> self.chunk_shift][offset..offset + self.dimension],
            ),
        }
    }
}

/// Construction parameters of one index, validated as the serial insert
/// validates them.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BulkParameters {
    limits: MutationDegreeLimits,
    ef_construction: usize,
    ml: f32,
}

impl BulkParameters {
    /// Reads the degree limits and beam width a serial insert would use.
    pub(crate) fn try_from_config(config: &VectorIndexConfig) -> Result<Self, HelixDbError> {
        Ok(Self {
            limits: MutationDegreeLimits::try_from_config(config)?,
            ef_construction: config.ef_construction,
            ml: config.ml,
        })
    }

    fn degree(self, layer: u8) -> usize {
        match layer {
            0 => self.limits.layer0.get(),
            1..=u8::MAX => self.limits.upper.get(),
        }
    }

    fn search_width(self, layer: u8) -> usize {
        self.limits
            .search_width(u16::from(layer), self.ef_construction)
    }

    /// Bytes one `dimension`-component vector occupies during a bulk build:
    /// its components and distance header, entity ID, source hash, layer and
    /// unlinked flag, its layer-0 link row, and its expected share of upper
    /// link rows (a node reaches layer `l` with probability `exp(-l / ml)`).
    ///
    /// At the default parameters this is the component bytes plus about 160.
    pub(crate) fn bytes_per_vector<D: Distance>(self, dimension: usize) -> u64 {
        let row = |degree: usize| degree * size_of::<AtomicU32>() + size_of::<Mutex<()>>();
        let expected_upper_rows = 1.0 / f64::from(self.ml.recip()).exp_m1();
        let fixed = dimension * size_of::<f32>()
            + size_of::<D::Header>()
            + size_of::<NodeId>()
            + size_of::<u64>()
            + size_of::<u8>()
            + size_of::<bool>()
            + size_of::<u32>()
            + row(self.limits.layer0.get());
        // An `ml` too large to bound the upper rows saturates, and no budget
        // holds such a vector.
        (fixed as u64).saturating_add(
            (row(self.limits.upper.get()) as f64 * expected_upper_rows).ceil() as u64,
        )
    }
}

/// Fixed-degree link rows of one layer class, each behind its own lock.
///
/// A row is a strictly ascending prefix of ordinals followed by [`EMPTY`]
/// slots, so it holds a node once and is read in the order a stored neighbor
/// row is: searches then visit neighbors, and break distance ties, exactly as
/// the serial insert does. Slots are atomics only so rows can share one
/// allocation without `unsafe`; a row is read and written under its lock.
struct LinkRows {
    degree: usize,
    locks: Box<[Mutex<()>]>,
    slots: Box<[AtomicU32]>,
}

impl LinkRows {
    fn new(rows: usize, degree: usize) -> Self {
        Self {
            degree,
            locks: (0..rows).map(|_| Mutex::new(())).collect(),
            slots: (0..rows * degree).map(|_| AtomicU32::new(EMPTY)).collect(),
        }
    }

    fn lock(&self, row: usize) -> LinkRow<'_> {
        LinkRow {
            _guard: self.locks[row].lock(),
            slots: &self.slots[row * self.degree..][..self.degree],
        }
    }
}

/// One locked link row.
struct LinkRow<'a> {
    _guard: MutexGuard<'a, ()>,
    slots: &'a [AtomicU32],
}

impl LinkRow<'_> {
    fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.slots
            .iter()
            .map(|slot| slot.load(Ordering::Relaxed))
            .take_while(|node| *node != EMPTY)
    }

    fn len(&self) -> usize {
        self.iter().count()
    }

    fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    fn contains(&self, node: u32) -> bool {
        self.iter().any(|linked| linked == node)
    }

    /// Inserts `node` at its place in ascending order.
    ///
    /// # Panics
    ///
    /// Panics when the row is full or already holds `node`.
    fn push(&mut self, node: u32) {
        let len = self.len();
        let position = self.iter().take_while(|linked| *linked < node).count();
        assert!(
            position == len || self.slots[position].load(Ordering::Relaxed) != node,
            "a link row holds a node once"
        );
        // Shifts the larger ordinals up one slot; indexing past the degree
        // panics on a full row.
        (position..len).rev().for_each(|slot| {
            self.slots[slot + 1].store(self.slots[slot].load(Ordering::Relaxed), Ordering::Relaxed)
        });
        self.slots[position].store(node, Ordering::Relaxed);
    }

    /// Replaces the row with `nodes`.
    ///
    /// # Panics
    ///
    /// Panics when `nodes` exceed the degree or do not strictly ascend.
    fn replace(&mut self, nodes: &[u32]) {
        assert!(nodes.len() <= self.slots.len(), "link row degree");
        assert!(
            nodes.windows(2).all(|pair| pair[0] < pair[1]),
            "a link row ascends and holds a node once"
        );
        self.slots.iter().enumerate().for_each(|(slot, stored)| {
            stored.store(nodes.get(slot).copied().unwrap_or(EMPTY), Ordering::Relaxed)
        });
    }

    /// Removes `node`, returning whether the row held it.
    fn remove(&mut self, node: u32) -> bool {
        let Some(position) = self.iter().position(|linked| linked == node) else {
            return false;
        };
        let last = self.len() - 1;
        (position..last).for_each(|slot| {
            self.slots[slot].store(
                self.slots[slot + 1].load(Ordering::Relaxed),
                Ordering::Relaxed,
            )
        });
        self.slots[last].store(EMPTY, Ordering::Relaxed);
        true
    }
}

/// Node a search descends from, and the highest layer it occupies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Entry {
    node: u32,
    layer: u8,
}

/// Reusable buffers of one construction thread.
#[derive(Default)]
struct Scratch {
    visited: foldhash::HashSet<u32>,
    frontier: BinaryHeap<Reverse<Scored>>,
    nearest: BinaryHeap<Scored>,
    row: Vec<u32>,
    ranked: Vec<Scored>,
    selected: Vec<u32>,
    chosen: Vec<Vec<u32>>,
}

/// What adding a node to a link row came to ([`BulkGraph::link_back`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LinkedBack {
    /// The row already held the node.
    Held,
    /// The row took the node in a free slot.
    Added,
    /// The full row took the node in place of this one, which lost its link
    /// to the row's node.
    Replaced(u32),
    /// The full row kept its links: the selection dropped the node itself.
    Refused,
}

/// Whether a reported link now exists or no longer does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LinkChangeKind {
    Added,
    Removed,
}

/// One directed link changed after construction, reported so the writer can
/// restage the source's row and the link's reverse locator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LinkChange {
    pub(crate) layer: u8,
    pub(crate) source: u32,
    pub(crate) target: u32,
    pub(crate) kind: LinkChangeKind,
}

/// Outcome of [`BulkGraph::construct`].
pub(crate) enum BulkConstruction<D: Distance> {
    /// Every vector is linked.
    Built(Box<BulkGraph<D>>),
    /// The caller's cancellation flag was raised; the partial graph is gone.
    Cancelled,
}

/// A fully linked in-memory HNSW graph over [`BulkVectors`].
pub(crate) struct BulkGraph<D: Distance> {
    vectors: BulkVectors<D>,
    parameters: BulkParameters,
    layer0: LinkRows,
    upper: LinkRows,
    /// First upper row of each node; a node on layer `l > 0` owns rows
    /// `slot..slot + l` for layers `1..=l`.
    upper_slots: Vec<u32>,
    /// Where searches start. Read and replaced under its lock while threads
    /// construct; the rule-based entry point afterwards.
    entry: RwLock<Entry>,
    unlinked: Vec<bool>,
    linked: usize,
}

impl<D: Distance> BulkGraph<D> {
    /// Links `vectors` on `threads` scoped OS threads owned by the calling
    /// thread, which must not be an async runtime worker: the call computes
    /// for the whole construction.
    ///
    /// Returns [`BulkConstruction::Cancelled`] soon after `cancelled` is
    /// raised. A distance kernel emitting an invalid score fails the build
    /// with the serial path's invariant error. `linked` counts the vectors
    /// as their insertions end, for whoever watches the construction: the
    /// first takes none, and the passes after the insertions add nothing.
    ///
    /// # Panics
    ///
    /// Panics when `vectors` is empty or holds `u32::MAX` or more vectors;
    /// the caller admits neither.
    pub(crate) fn construct(
        vectors: BulkVectors<D>,
        parameters: BulkParameters,
        threads: NonZeroUsize,
        cancelled: &AtomicBool,
        linked: &AtomicU64,
    ) -> Result<BulkConstruction<D>, HelixDbError> {
        let nodes = vectors.len();
        assert!(nodes > 0, "a bulk build holds at least one vector");
        assert!(
            nodes < EMPTY as usize,
            "bulk ordinals fit below the sentinel"
        );
        let upper_slots = vectors
            .levels
            .iter()
            .scan(0_u32, |next, level| {
                let slot = *next;
                *next = next
                    .checked_add(u32::from(*level))
                    .expect("upper link rows fit u32");
                Some(slot)
            })
            .collect::<Vec<_>>();
        let upper_rows = vectors.levels.iter().map(|level| usize::from(*level)).sum();
        let mut graph = Self {
            layer0: LinkRows::new(nodes, parameters.degree(0)),
            upper: LinkRows::new(upper_rows, parameters.degree(1)),
            upper_slots,
            entry: RwLock::new(Entry {
                node: 0,
                layer: vectors.levels[0],
            }),
            unlinked: vec![false; nodes],
            linked: nodes,
            vectors,
            parameters,
        };

        let all = |position: usize| position as u32;
        linked.store(1, Ordering::Relaxed);
        let built = graph.in_parallel(threads, cancelled, nodes - 1, |position, scratch| {
            graph.insert(position as u32 + 1, scratch)?;
            linked.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })? && graph.in_parallel(threads, cancelled, nodes, |position, scratch| {
            graph.drop_unreciprocated(all(position), scratch);
            Ok(())
        })?;
        if !built {
            return Ok(BulkConstruction::Cancelled);
        }

        // Later inserts can prune every link of an earlier node. Each such
        // node is linked again against the complete graph: concurrently with
        // the insert's own pruning first, then, for the few that pruning
        // dropped again, one at a time without displacing any other node.
        let pruned = graph.orphans();
        if nodes > 1 && !pruned.is_empty() {
            let relinked =
                graph.in_parallel(threads, cancelled, pruned.len(), |position, scratch| {
                    let node = pruned[position];
                    let Some(start) = graph.relink_start(node) else {
                        return Ok(());
                    };
                    graph.connect(node, 0, start, scratch)
                })? && graph.in_parallel(threads, cancelled, nodes, |position, scratch| {
                    graph.drop_unreciprocated(all(position), scratch);
                    Ok(())
                })?;
            if !relinked {
                return Ok(BulkConstruction::Cancelled);
            }
        }
        let mut scratch = Scratch::default();
        let mut ignored = Vec::new();
        let remaining = graph.orphans();
        // Only the single node of a one-node graph stays without a link.
        remaining.iter().try_for_each(|node| {
            graph
                .relink_orphan(*node, &mut scratch, &mut ignored)
                .map(|relinked| assert!(relinked || nodes == 1, "bulk node left without a link"))
        })?;
        *graph.entry.get_mut() = graph
            .entry_by_rule()
            .expect("a constructed graph has nodes");
        tracing::info!(
            nodes,
            pruned_and_relinked = pruned.len(),
            relinked_sequentially = remaining.len(),
            "bulk vector graph constructed"
        );
        Ok(BulkConstruction::Built(Box::new(graph)))
    }

    /// Runs `work` for every position below `count` on `threads` scoped
    /// threads, returning `false` when `cancelled` stopped it early.
    fn in_parallel(
        &self,
        threads: NonZeroUsize,
        cancelled: &AtomicBool,
        count: usize,
        work: impl Fn(usize, &mut Scratch) -> Result<(), HelixDbError> + Sync,
    ) -> Result<bool, HelixDbError> {
        let next = AtomicUsize::new(0);
        let failed = AtomicBool::new(false);
        let outcome = std::thread::scope(|scope| {
            let workers = (0..threads.get().min(count.max(1)))
                .map(|_| {
                    scope.spawn(|| {
                        let mut scratch = Scratch::default();
                        // Imperative: each iteration claims the next position
                        // and stops on the first failure or cancellation.
                        loop {
                            if cancelled.load(Ordering::Relaxed) || failed.load(Ordering::Relaxed) {
                                return Ok(());
                            }
                            let position = next.fetch_add(1, Ordering::Relaxed);
                            if position >= count {
                                return Ok(());
                            }
                            let Err(error) = work(position, &mut scratch) else {
                                continue;
                            };
                            failed.store(true, Ordering::Relaxed);
                            return Err(error);
                        }
                    })
                })
                .collect::<Vec<_>>();
            workers
                .into_iter()
                .try_for_each(|worker| worker.join().expect("bulk construction thread panicked"))
        });
        outcome.map(|()| !cancelled.load(Ordering::Relaxed))
    }

    fn row(&self, layer: u8, node: u32) -> LinkRow<'_> {
        let index = node as usize;
        assert!(
            layer <= self.vectors.levels[index],
            "bulk node is linked only on layers it occupies"
        );
        match layer {
            0 => self.layer0.lock(index),
            layer => self
                .upper
                .lock(self.upper_slots[index] as usize + usize::from(layer) - 1),
        }
    }

    fn score(&self, query: &Item<'_, D>, node: u32) -> Result<DistanceScore, HelixDbError> {
        DistanceScore::try_new(D::distance(query, &self.vectors.item(node))).map_err(|error| {
            HelixDbError::InvariantViolation(format!(
                "vector distance kernel emitted an invalid score: {error}"
            ))
        })
    }

    fn pair(&self, left: u32, right: u32) -> Result<DistanceScore, HelixDbError> {
        self.score(&self.vectors.item(left), right)
    }

    /// Walks `layer` from `start` to a node no neighbor of which is closer to
    /// `query`, never moving to `exclude`.
    fn greedy(
        &self,
        query: &Item<'_, D>,
        exclude: u32,
        start: u32,
        layer: u8,
        scratch: &mut Scratch,
    ) -> Result<u32, HelixDbError> {
        let Scratch { visited, row, .. } = scratch;
        visited.clear();
        visited.insert(exclude);
        visited.insert(start);
        let mut current = start;
        let mut distance = self.score(query, start)?;
        loop {
            row.clear();
            row.extend(
                self.row(layer, current)
                    .iter()
                    .filter(|neighbor| visited.insert(*neighbor)),
            );
            let mut moved = false;
            for &neighbor in row.iter() {
                let candidate = self.score(query, neighbor)?;
                if candidate < distance {
                    current = neighbor;
                    distance = candidate;
                    moved = true;
                }
            }
            if !moved {
                return Ok(current);
            }
        }
    }

    /// Leaves the up to `ef` nodes of `layer` nearest to `query` that a beam
    /// from `start` reaches in `scratch.ranked`, closest first, never
    /// visiting `exclude`.
    fn beam(
        &self,
        query: &Item<'_, D>,
        exclude: u32,
        start: u32,
        layer: u8,
        ef: usize,
        scratch: &mut Scratch,
    ) -> Result<(), HelixDbError> {
        assert_ne!(start, exclude, "a beam starts at another node");
        let Scratch {
            visited,
            frontier,
            nearest,
            row,
            ranked,
            ..
        } = scratch;
        visited.clear();
        frontier.clear();
        nearest.clear();
        visited.insert(exclude);
        visited.insert(start);
        let start = (self.score(query, start)?, start);
        frontier.push(Reverse(start));
        nearest.push(start);
        while let Some(Reverse((distance, current))) = frontier.pop() {
            if nearest.len() >= ef && nearest.peek().is_some_and(|worst| distance > worst.0) {
                break;
            }
            row.clear();
            row.extend(
                self.row(layer, current)
                    .iter()
                    .filter(|neighbor| visited.insert(*neighbor)),
            );
            for &neighbor in row.iter() {
                let candidate = (self.score(query, neighbor)?, neighbor);
                if nearest.len() < ef || nearest.peek().is_some_and(|worst| candidate.0 < worst.0) {
                    frontier.push(Reverse(candidate));
                    nearest.push(candidate);
                    if nearest.len() > ef {
                        nearest.pop();
                    }
                }
            }
        }
        ranked.clear();
        ranked.extend(nearest.drain());
        // A node enters the beam once, so an unstable sort is deterministic.
        ranked.sort_unstable();
        Ok(())
    }

    /// Inserts `node` on its layers, starting from the current entry.
    fn insert(&self, node: u32, scratch: &mut Scratch) -> Result<(), HelixDbError> {
        let level = self.vectors.levels[node as usize];
        let mut entry = *self.entry.read();
        // A node above the top layer holds the entry lock for its whole
        // insertion, so the layers it opens appear together with its links.
        let mut raising = None;
        if level > entry.layer {
            let guard = self.entry.write();
            entry = *guard;
            raising = (level > entry.layer).then_some(guard);
        }
        self.connect(node, level, entry, scratch)?;
        let Some(mut raised) = raising else {
            return Ok(());
        };
        *raised = Entry { node, layer: level };
        Ok(())
    }

    /// Links `node` into layers `0..=level`, descending from `start`, with the
    /// serial insert's search, selection and reciprocal pruning.
    fn connect(
        &self,
        node: u32,
        level: u8,
        start: Entry,
        scratch: &mut Scratch,
    ) -> Result<(), HelixDbError> {
        let query = self.vectors.item(node);
        let mut current = (level.saturating_add(1)..=start.layer)
            .rev()
            .try_fold(start.node, |current, layer| {
                self.greedy(&query, node, current, layer, scratch)
            })?;
        let top = level.min(start.layer);
        let mut chosen = std::mem::take(&mut scratch.chosen);
        chosen.resize_with(usize::from(top) + 1, Vec::new);
        for layer in (0..=top).rev() {
            let degree = self.parameters.degree(layer);
            self.beam(
                &query,
                node,
                current,
                layer,
                self.parameters.search_width(layer),
                scratch,
            )?;
            scratch.ranked.truncate(degree * 2);
            select_diverse(
                &scratch.ranked,
                degree,
                |left, right| self.pair(left, right),
                &mut chosen[usize::from(layer)],
            )?;
            current = scratch.ranked.first().map_or(current, |closest| closest.1);
        }
        // Every row of the node is in place before a reciprocal link lets
        // another thread reach it and descend through it. A row ascends;
        // reciprocal links follow in selection order, closest first, as the
        // serial insert adds them.
        for (layer, links) in chosen.iter().enumerate() {
            scratch.row.clear();
            scratch.row.extend_from_slice(links);
            scratch.row.sort_unstable();
            self.row(layer as u8, node).replace(&scratch.row);
        }
        for (layer, links) in chosen.iter().enumerate().rev() {
            for &neighbor in links {
                self.link_back(layer as u8, node, neighbor, scratch)?;
            }
        }
        scratch.chosen = chosen;
        Ok(())
    }

    /// Adds `from` to the `layer` row of `to`, pruning a full row with the
    /// diverse selection and removing `to` from the node the row dropped.
    ///
    /// At most one row lock is held at a time, so concurrent calls can leave a
    /// link without its reverse; [`Self::drop_unreciprocated`] removes those.
    fn link_back(
        &self,
        layer: u8,
        from: u32,
        to: u32,
        scratch: &mut Scratch,
    ) -> Result<LinkedBack, HelixDbError> {
        let degree = self.parameters.degree(layer);
        let Scratch {
            ranked, selected, ..
        } = scratch;
        {
            let mut row = self.row(layer, to);
            if row.contains(from) {
                return Ok(LinkedBack::Held);
            }
            if row.len() < degree {
                row.push(from);
                return Ok(LinkedBack::Added);
            }
            let target = self.vectors.item(to);
            ranked.clear();
            for neighbor in row.iter().chain(std::iter::once(from)) {
                ranked.push((self.score(&target, neighbor)?, neighbor));
            }
            ranked.sort_unstable();
            select_diverse(
                ranked,
                degree,
                |left, right| self.pair(left, right),
                selected,
            )?;
            selected.sort_unstable();
            row.replace(selected);
        }
        // The full row and `from` were ranked and the selection fills the
        // degree, so it dropped exactly one of them.
        let dropped = ranked
            .iter()
            .map(|(_, neighbor)| *neighbor)
            .find(|neighbor| !selected.contains(neighbor))
            .expect("a selection of a full row and one more drops one");
        self.row(layer, dropped).remove(to);
        Ok(if dropped == from {
            LinkedBack::Refused
        } else {
            LinkedBack::Replaced(dropped)
        })
    }

    /// Links `source` again on `layer` after one of its neighbors was
    /// unlinked, as the serial delete relinks every source it affects
    /// ([`VectorIndex::relink_neighbor`]): the nearest of `candidates`, up
    /// to the layer's degree, are ranked with the links `source` has left,
    /// and the ones the diverse selection keeps are linked first, then the
    /// rest of them, closest first, until the row is full. Every link
    /// `source` gains is added to the other node's row, which the selection
    /// may cut in turn.
    ///
    /// The graph stays symmetric, where the serial path leaves a link whose
    /// reverse a full row refused: a node a full row refuses is not linked,
    /// and a link a row drops for `source` is dropped from the other side
    /// too. For the same reason `source` keeps every link it has: dropping
    /// one for a closer node, as the serial selection may, would take a
    /// link from a node that lost none. Every change is appended to
    /// `changes`. Must not run concurrently with other link changes.
    fn relink(
        &self,
        layer: u8,
        source: u32,
        candidates: &[u32],
        scratch: &mut Scratch,
        changes: &mut Vec<LinkChange>,
    ) -> Result<(), HelixDbError> {
        let degree = self.parameters.degree(layer);
        let item = self.vectors.item(source);
        let link = |source: u32, target: u32, kind: LinkChangeKind| LinkChange {
            layer,
            source,
            target,
            kind,
        };
        let ranked = |nodes: &mut dyn Iterator<Item = u32>| {
            let mut ranked = nodes
                .map(|node| Ok((self.score(&item, node)?, node)))
                .collect::<Result<Vec<Scored>, HelixDbError>>()?;
            // A node is ranked once, so an unstable sort is deterministic.
            ranked.sort_unstable();
            Ok::<_, HelixDbError>(ranked)
        };
        let old = self.row(layer, source).iter().collect::<Vec<_>>();
        let mut nearest = ranked(
            &mut candidates
                .iter()
                .copied()
                .filter(|node| *node != source && !old.contains(node)),
        )?;
        nearest.truncate(degree);
        select_diverse(
            &ranked(
                &mut old
                    .iter()
                    .copied()
                    .chain(nearest.iter().map(|(_, node)| *node)),
            )?,
            degree,
            |left, right| self.pair(left, right),
            &mut scratch.selected,
        )?;
        // The nodes the serial selection would add, then the others.
        let (kept, others): (Vec<u32>, Vec<u32>) = nearest
            .iter()
            .map(|(_, node)| *node)
            .partition(|node| scratch.selected.contains(node));
        let mut free = degree - old.len();
        // Imperative: each link changes the rows the next one is added to,
        // and the row may fill before the candidates end.
        for gained in kept.into_iter().chain(others) {
            if free == 0 {
                break;
            }
            match self.link_back(layer, source, gained, scratch)? {
                LinkedBack::Refused => continue,
                // A symmetric graph has no link back without the link.
                LinkedBack::Held | LinkedBack::Added => {}
                LinkedBack::Replaced(evicted) => changes.extend([
                    link(gained, evicted, LinkChangeKind::Removed),
                    link(evicted, gained, LinkChangeKind::Removed),
                ]),
            }
            self.row(layer, source).push(gained);
            changes.extend([
                link(source, gained, LinkChangeKind::Added),
                link(gained, source, LinkChangeKind::Added),
            ]);
            free -= 1;
        }
        Ok(())
    }

    /// Removes every link of `node` whose reverse is missing.
    ///
    /// A link with its reverse is never removed, so calls for different nodes
    /// may run concurrently and leave exactly the links that had a reverse.
    fn drop_unreciprocated(&self, node: u32, scratch: &mut Scratch) {
        for layer in 0..=self.vectors.levels[node as usize] {
            scratch.row.clear();
            scratch.row.extend(self.row(layer, node).iter());
            scratch
                .row
                .iter()
                .filter(|neighbor| !self.row(layer, **neighbor).contains(node))
                .for_each(|neighbor| {
                    self.row(layer, node).remove(*neighbor);
                });
        }
    }

    /// Linked nodes without a layer-0 link, in source order.
    fn orphans(&self) -> Vec<u32> {
        (0..self.vectors.len() as u32)
            .filter(|node| !self.unlinked[*node as usize] && self.row(0, *node).is_empty())
            .collect()
    }

    /// The first linked node in source order on the highest linked layer.
    fn entry_by_rule(&self) -> Option<Entry> {
        (0..self.vectors.len() as u32)
            .filter(|node| !self.unlinked[*node as usize])
            .map(|node| (self.vectors.levels[node as usize], Reverse(node)))
            .max()
            .map(|(layer, Reverse(node))| Entry { node, layer })
    }

    /// Where a search that re-links `node` starts: the entry, or when `node`
    /// is the entry, its highest upper-layer neighbor, or failing that any
    /// other linked node.
    fn relink_start(&self, node: u32) -> Option<Entry> {
        let entry = *self.entry.read();
        if entry.node != node {
            return Some(entry);
        }
        (1..=entry.layer)
            .rev()
            .find_map(|layer| {
                let neighbor = self.row(layer, node).iter().next()?;
                Some(Entry {
                    node: neighbor,
                    layer,
                })
            })
            .or_else(|| {
                (0..self.vectors.len() as u32)
                    .find(|other| *other != node && !self.unlinked[*other as usize])
                    .map(|other| Entry {
                        node: other,
                        layer: 0,
                    })
            })
    }

    /// Gives `node` a layer-0 link unless it has one, without leaving any
    /// other node with none. Must not run concurrently with other link
    /// changes. Returns `false` only when no other node is linked.
    ///
    /// A node that already has a link is left as it is: re-linking one node
    /// can link another that lost its links with it, and linking that one
    /// again would add the same link twice.
    ///
    /// The diverse neighbors with a free slot are linked. When none has one,
    /// the closest candidate that can spare a link trades its furthest
    /// neighbor that keeps another link for `node`.
    ///
    /// Some candidate always can. Layer-0 rows hold at least two links and the
    /// beam keeps at least two candidates, so were every candidate full with
    /// single-link neighbors only, one found over a link would have been found
    /// from such a neighbor, which only the start can be; the beam would then
    /// have visited the start, that candidate and its other single-link
    /// neighbors alone, and kept one of them, which has a free slot.
    fn relink_orphan(
        &self,
        node: u32,
        scratch: &mut Scratch,
        changes: &mut Vec<LinkChange>,
    ) -> Result<bool, HelixDbError> {
        if !self.row(0, node).is_empty() {
            return Ok(true);
        }
        let Some(start) = self.relink_start(node) else {
            return Ok(false);
        };
        let query = self.vectors.item(node);
        let nearest = (1..=start.layer)
            .rev()
            .try_fold(start.node, |current, layer| {
                self.greedy(&query, node, current, layer, scratch)
            })?;
        let degree = self.parameters.degree(0);
        self.beam(
            &query,
            node,
            nearest,
            0,
            self.parameters.search_width(0),
            scratch,
        )?;
        let candidates = std::mem::take(&mut scratch.ranked);
        select_diverse(
            &candidates[..candidates.len().min(degree * 2)],
            degree,
            |left, right| self.pair(left, right),
            &mut scratch.selected,
        )?;
        let link = |source: u32, target: u32, kind: LinkChangeKind| LinkChange {
            layer: 0,
            source,
            target,
            kind,
        };
        let before = changes.len();
        for &neighbor in scratch.selected.iter() {
            let mut row = self.row(0, neighbor);
            if row.len() < degree {
                row.push(node);
                drop(row);
                self.row(0, node).push(neighbor);
                changes.extend([
                    link(neighbor, node, LinkChangeKind::Added),
                    link(node, neighbor, LinkChangeKind::Added),
                ]);
            }
        }
        if changes.len() > before {
            scratch.ranked = candidates;
            return Ok(true);
        }
        for &(_, candidate) in &candidates {
            scratch.row.clear();
            scratch.row.extend(self.row(0, candidate).iter());
            if scratch.row.len() == degree {
                let target = self.vectors.item(candidate);
                let mut furthest = None;
                for &neighbor in scratch.row.iter() {
                    if self.row(0, neighbor).len() < 2 {
                        continue;
                    }
                    let scored = (self.score(&target, neighbor)?, neighbor);
                    furthest = furthest.max(Some(scored));
                }
                let Some((_, evicted)) = furthest else {
                    continue;
                };
                self.row(0, candidate).remove(evicted);
                self.row(0, evicted).remove(candidate);
                changes.extend([
                    link(candidate, evicted, LinkChangeKind::Removed),
                    link(evicted, candidate, LinkChangeKind::Removed),
                ]);
            }
            self.row(0, candidate).push(node);
            self.row(0, node).push(candidate);
            changes.extend([
                link(candidate, node, LinkChangeKind::Added),
                link(node, candidate, LinkChangeKind::Added),
            ]);
            scratch.ranked = candidates;
            return Ok(true);
        }
        Err(HelixDbError::InvariantViolation(
            "bulk vector graph has no node that can take a link".to_string(),
        ))
    }

    /// Number of nodes, linked or not.
    pub(crate) fn len(&self) -> usize {
        self.vectors.len()
    }

    /// Number of nodes not unlinked.
    pub(crate) fn linked_len(&self) -> usize {
        self.linked
    }

    /// Entity the node at `node` indexes.
    pub(crate) fn entity_id(&self, node: u32) -> NodeId {
        self.vectors.entity_ids[node as usize]
    }

    /// Ordinal of `entity_id`, or the ordinal of the first node above it.
    pub(crate) fn position(&self, entity_id: NodeId) -> Result<u32, u32> {
        self.vectors
            .entity_ids
            .binary_search(&entity_id)
            .map(|node| node as u32)
            .map_err(|node| node as u32)
    }

    /// [`BulkVectors::source_hash`] of the vector `node` was linked for.
    pub(crate) fn source_hash(&self, node: u32) -> u64 {
        self.vectors.source_hashes[node as usize]
    }

    /// Whether [`Self::unlink`] removed `node`.
    pub(crate) fn is_unlinked(&self, node: u32) -> bool {
        self.unlinked[node as usize]
    }

    /// Entity IDs `node` links to on `layer`, ascending as a stored neighbor
    /// row holds them: ordinals ascend with entity IDs.
    pub(crate) fn links(&self, layer: u8, node: u32) -> Vec<NodeId> {
        self.row(layer, node)
            .iter()
            .map(|linked| self.entity_id(linked))
            .collect()
    }

    /// Removes `node` from the graph because its source no longer holds the
    /// vector it was linked for, appending every link change to `changes`.
    ///
    /// Its own rows are cleared without a report: an unlinked node is never
    /// stored. On every layer it occupied, each neighbor that lost its link
    /// to it is linked again as the serial delete relinks it
    /// ([`Self::relink`]), from the neighbors and their own neighbors, so a
    /// graph many nodes were unlinked from keeps the degree a serial build
    /// under the same deletes has. A node this leaves without a layer-0 link
    /// is then linked as construction links one. Unlinking twice changes
    /// nothing.
    ///
    /// What it costs, on the calling thread, for a node with `d` neighbors
    /// on a layer of degree `d`: each neighbor is scored against every
    /// candidate (the `d` neighbors and up to `d * d` of theirs: about 34,000
    /// scores at a degree of 32), then runs the diverse selection over its
    /// links and its nearest `d` candidates (up to `2 * d * d` pair scores
    /// each, 65,000 in all), and every link it gains makes the full row at
    /// the other end select again (`d` scores and up to `d * d` pair scores
    /// a link). Measured on one thread at the default degree of 32: 4.3 ms
    /// an unlink at 128 dimensions and 20.9 ms at 1,536, about 200 link
    /// changes each.
    pub(crate) fn unlink(
        &mut self,
        node: u32,
        changes: &mut Vec<LinkChange>,
    ) -> Result<(), HelixDbError> {
        if std::mem::replace(&mut self.unlinked[node as usize], true) {
            return Ok(());
        }
        self.linked -= 1;
        let reported = changes.len();
        let mut scratch = Scratch::default();
        let mut candidates = Vec::new();
        for layer in 0..=self.vectors.levels[node as usize] {
            let sources = self.row(layer, node).iter().collect::<Vec<_>>();
            self.row(layer, node).replace(&[]);
            for &source in &sources {
                self.row(layer, source).remove(node);
                changes.push(LinkChange {
                    layer,
                    source,
                    target: node,
                    kind: LinkChangeKind::Removed,
                });
            }
            // The serial delete's candidates: the sources and their
            // neighbors, without the removed node.
            candidates.clear();
            candidates.extend_from_slice(&sources);
            for &source in &sources {
                candidates.extend(self.row(layer, source).iter());
            }
            candidates.sort_unstable();
            candidates.dedup();
            for &source in &sources {
                self.relink(layer, source, &candidates, &mut scratch, changes)?;
            }
        }
        if self.entry.get_mut().node == node {
            let Some(entry) = self.entry_by_rule() else {
                return Ok(());
            };
            *self.entry.get_mut() = entry;
        }
        // Every node that lost a layer-0 link here; only the last linked
        // node stays without one.
        let mut lost = changes[reported..]
            .iter()
            .filter(|change| change.layer == 0 && change.kind == LinkChangeKind::Removed)
            .map(|change| change.source)
            .collect::<Vec<_>>();
        lost.sort_unstable();
        lost.dedup();
        lost.into_iter().try_for_each(|neighbor| {
            self.relink_orphan(neighbor, &mut scratch, changes)
                .map(|relinked| {
                    assert!(
                        relinked || self.linked == 1,
                        "bulk node left without a link"
                    )
                })
        })
    }

    /// Entry point and top layer of the linked nodes: the first in source
    /// order on the highest linked layer, or `None` once all are unlinked.
    pub(crate) fn entry_point(&self) -> Option<(NodeId, u16)> {
        let entry = *self.entry.read();
        (self.linked > 0).then(|| (self.entity_id(entry.node), u16::from(entry.layer)))
    }
}

/// The serial path's diverse-neighbor selection over ordinals.
///
/// Mirrors [`crate::search::vector::select_diverse`] with every candidate
/// resolvable: a candidate, taken closest first, is kept unless a kept one is
/// closer to it than it is to the reference node, and the closest rejected
/// candidates fill the remaining slots. `pair` scores two candidates.
fn select_diverse(
    candidates: &[Scored],
    degree: usize,
    pair: impl Fn(u32, u32) -> Result<DistanceScore, HelixDbError>,
    selected: &mut Vec<u32>,
) -> Result<(), HelixDbError> {
    selected.clear();
    // Imperative: each decision reads the selection the previous ones built
    // and stops at the first closer neighbor or failed score.
    for &(distance, candidate) in candidates {
        if selected.len() >= degree {
            break;
        }
        let mut diverse = true;
        for &kept in selected.iter() {
            if pair(candidate, kept)? < distance {
                diverse = false;
                break;
            }
        }
        if diverse {
            selected.push(candidate);
        }
    }
    for &(_, candidate) in candidates {
        if selected.len() >= degree {
            break;
        }
        if !selected.contains(&candidate) {
            selected.push(candidate);
        }
    }
    Ok(())
}

impl<D: Distance> VectorIndex<D> {
    /// Stages every row of one linked bulk node: the rows a serial insert of
    /// `vector` would write, with the graph's links.
    ///
    /// `vector` is the source vector as currently stored; the caller has
    /// matched it against [`BulkGraph::source_hash`]. Rows are a function of
    /// the graph and the vector, so staging a node again writes the same rows.
    pub(crate) fn stage_bulk_node(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        graph: &BulkGraph<D>,
        node: u32,
        vector: &[f32],
    ) -> Result<(), HelixDbError> {
        let semantics = ActiveVectorSemantics::for_distance::<D>().ok_or_else(|| {
            HelixDbError::Config(format!(
                "vector distance '{}' has no stable durable semantic identity",
                D::name()
            ))
        })?;
        let dimension = VectorDimension::try_new(graph.vectors.dimension)
            .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?;
        let vector = ValidatedMetricVector::try_new(
            UnalignedVector::<D::VectorCodec>::from_slice(vector),
            semantics.distance_metric(),
            dimension,
        )
        .map_err(HelixDbError::from)?;
        let node_id = graph.entity_id(node);
        let level = graph.vectors.levels[node as usize];
        let simhash = self
            .simhash_cache(dimension.get())?
            .compute_and_cache_measured(txn, node_id, &vector)?;
        let canonical_key = self.canonical_vector_key_from_simhash(node_id, simhash);
        let encoded_item = encode_item(&Item::<D> {
            header: D::new_header(vector.values()),
            vector: Cow::Borrowed(vector.values()),
        });
        let rows = VectorWriteRows::new(txn, self.row_keyspace());
        if level > 0 {
            rows.put_upper_vector(node_id, encoded_item.clone())?;
        }
        rows.put_canonical_vector(&canonical_key, encoded_item)?;
        if self.simhash_directory_enabled() {
            rows.put_simhash_directory_entry(&canonical_key)?;
        }
        for layer in 0..=level {
            let links = graph.links(layer, node);
            match layer {
                0 => rows.put_layer0_neighbors(node_id, &links)?,
                layer => rows.put_upper_neighbors(u16::from(layer), node_id, &links)?,
            }
            links.iter().try_for_each(|target| {
                rows.put_reverse_locator(*target, u16::from(layer), node_id)
            })?;
        }
        rows.put_entry_candidate(node_id, u16::from(level))
    }

    /// Restages what `change` altered for a source node whose rows are already
    /// stored: its neighbor row as the graph now holds it, and the reverse
    /// locator of the changed link.
    pub(crate) fn stage_bulk_link_change(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        graph: &BulkGraph<D>,
        change: LinkChange,
    ) -> Result<(), HelixDbError> {
        let rows = VectorWriteRows::new(txn, self.row_keyspace());
        let source = graph.entity_id(change.source);
        let target = graph.entity_id(change.target);
        let links = graph.links(change.layer, change.source);
        match change.layer {
            0 => rows.put_layer0_neighbors(source, &links)?,
            layer => rows.put_upper_neighbors(u16::from(layer), source, &links)?,
        }
        match change.kind {
            LinkChangeKind::Added => {
                rows.put_reverse_locator(target, u16::from(change.layer), source)
            }
            LinkChangeKind::Removed => {
                rows.delete_reverse_locator(target, u16::from(change.layer), source)
            }
        }
    }

    /// Stages the metadata row that makes a completely stored bulk graph a
    /// searchable index. A graph with every node unlinked stages nothing: the
    /// generation stays unallocated, as after a serial build of no vectors.
    ///
    /// The row must be the last one a bulk build writes: its absence is what
    /// marks a partially stored graph.
    pub(crate) async fn stage_bulk_metadata(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        graph: &BulkGraph<D>,
        config: VectorIndexConfig,
    ) -> Result<(), HelixDbError> {
        let Some((entry_point, max_layer)) = graph.entry_point() else {
            return Ok(());
        };
        let mut metadata = VectorIndexMetadata::new(config);
        metadata.entry_point = Some(entry_point);
        metadata.max_layer = max_layer;
        metadata.count = graph.linked_len() as u64;
        self.update_metadata(txn, &metadata).await
    }
}

/// Stored-row views for tests outside the vector module, which cannot name
/// its storage types.
#[cfg(test)]
impl<D: Distance> VectorIndex<D> {
    /// Layer-0 links stored for `entity_id` and the layer-0 sources its
    /// reverse locators name, both ascending.
    pub(crate) async fn stored_layer0_links(
        &self,
        reader: &(impl slatedb::DbReadOps + Send + Sync),
        entity_id: NodeId,
    ) -> Result<(Vec<NodeId>, Vec<NodeId>), HelixDbError> {
        let rows = crate::search::vector::storage::VectorRows::new(reader, self.row_keyspace());
        Ok((
            rows.layer0_neighbors(entity_id).await?,
            rows.reverse_sources_for_target(entity_id)
                .await?
                .sources_at(0)
                .to_vec(),
        ))
    }

    /// The links stored for `entity_id` on each layer and the sources its
    /// reverse locators name there, both ascending: every layer up to
    /// `max_layer`, and any above it that a locator names.
    pub(crate) async fn stored_links(
        &self,
        reader: &(impl slatedb::DbReadOps + Send + Sync),
        entity_id: NodeId,
        max_layer: u16,
    ) -> Result<std::collections::BTreeMap<u16, (Vec<NodeId>, Vec<NodeId>)>, HelixDbError> {
        let rows = crate::search::vector::storage::VectorRows::new(reader, self.row_keyspace());
        let sources = rows.reverse_sources_for_target(entity_id).await?;
        let layers = sources
            .sources_by_layer()
            .keys()
            .copied()
            .fold(max_layer, u16::max);
        let mut stored = std::collections::BTreeMap::new();
        for layer in 0..=layers {
            let mut links = match layer {
                0 => rows.layer0_neighbors(entity_id).await?,
                layer => rows
                    .upper_neighbors(layer, entity_id)
                    .await?
                    .unwrap_or_default(),
            };
            links.sort_unstable();
            let mut named = sources.sources_at(layer).to_vec();
            named.sort_unstable();
            stored.insert(layer, (links, named));
        }
        Ok(stored)
    }

    /// Every stored row of this index's physical namespace.
    pub(crate) async fn stored_rows(
        &self,
        reader: &(impl slatedb::DbReadOps + Send + Sync),
    ) -> Result<std::collections::BTreeMap<bytes::Bytes, bytes::Bytes>, HelixDbError> {
        use crate::encoding::v2::keys::indexes::vector::VectorStorageLane;

        let keyspace = self.row_keyspace();
        let mut stored = std::collections::BTreeMap::new();
        for lane in VectorStorageLane::ALL {
            let prefix = keyspace.key(lane.prefix_key(keyspace.index_id()));
            let mut rows = reader.scan_prefix(prefix, ..).await?;
            while let Some(row) = rows.next().await? {
                stored.insert(row.key, row.value);
            }
        }
        Ok(stored)
    }
}

#[cfg(test)]
#[path = "../../../../tests/unit/vector/bulk_graph.rs"]
mod tests;
