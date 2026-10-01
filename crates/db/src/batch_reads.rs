//! Batch point reads that overlap their cold block fetches.
//!
//! SlateDB resolves one `multi_get` one SST at a time, and within an SST one
//! missed block range at a time. Keys of a random batch (HNSW neighbours,
//! search hits, traversal targets) rarely share a block, so a cold batch of
//! `n` keys waits for `n` fetches in turn: disk-cache reads, or whole
//! object-store parts the first time a part is touched.
//! [`BatchReads::Concurrent`] sorts the keys, splits them into contiguous runs
//! and resolves the runs concurrently, so a cold batch waits for about one
//! run's worth of fetches instead.
//!
//! Every run must read the same view, so a batch returns exactly what one
//! call would: callers pass a transaction, snapshot or request read view,
//! which bound each call by their start sequence, never a live database
//! handle.
//!
//! Runs beyond the first draw from one process-wide allowance, so a burst of
//! cold requests cannot multiply its object-store downloads without bound.

use bytes::Bytes;
use futures::StreamExt as _;
use slatedb::DbReadOps;
use tokio::sync::Semaphore;

use crate::error::{HelixDbError, Result};

/// Fewest keys in one concurrent run. Each run repeats SlateDB's per-call
/// work (every L0 filter, each covering SST's filter and index), so small
/// batches stay a few runs long rather than one key per call.
const MIN_RUN_KEYS: usize = 4;
/// Most keys in one concurrent run, and so the most fetches a cold run waits
/// for in turn.
const MAX_RUN_KEYS: usize = 32;
/// Runs in flight per batch. With [`MAX_RUN_KEYS`] this bounds one batch to
/// 16 outstanding block fetches, the same as the vector chunking it replaces.
const MAX_RUNS_IN_FLIGHT: usize = 16;
/// Runs beyond each batch's first that every batch in the process may have in
/// flight together. A cold run holds up to one object-store part (4 MiB)
/// while it downloads, so this bounds a burst of concurrent cold requests to
/// about 256 MiB of part buffers and as many connections.
const PROCESS_EXTRA_RUNS: usize = 64;

/// The process-wide allowance of [`PROCESS_EXTRA_RUNS`].
static EXTRA_RUNS: Semaphore = Semaphore::const_new(PROCESS_EXTRA_RUNS);

/// How a database resolves one batch of point reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BatchReads {
    /// One `multi_get` per batch. Used without a SlateDB block cache, where
    /// every extra call would refetch SST filters and indexes from storage.
    Single,
    /// Sorted keys in contiguous runs of [`MIN_RUN_KEYS`] to [`MAX_RUN_KEYS`]
    /// keys, [`MAX_RUNS_IN_FLIGHT`] runs at a time. Needs a SlateDB block
    /// cache, which serves the repeated per-call filter and index reads.
    Concurrent,
}

impl BatchReads {
    /// The policy for a database whose SlateDB block cache is `block_cache`.
    pub(crate) const fn for_block_cache<T: ?Sized>(block_cache: Option<&T>) -> Self {
        match block_cache {
            Some(_) => Self::Concurrent,
            None => Self::Single,
        }
    }

    /// Keys per run for a batch of `keys` keys. A batch of at most this many
    /// keys is read with one call.
    ///
    /// Runs are as short as spreading the batch over [`MAX_RUNS_IN_FLIGHT`]
    /// runs allows, clamped to [`MIN_RUN_KEYS`]..=[`MAX_RUN_KEYS`].
    const fn run_keys(self, keys: usize) -> usize {
        match self {
            Self::Single => keys,
            Self::Concurrent => {
                let spread = keys.div_ceil(MAX_RUNS_IN_FLIGHT);
                if spread < MIN_RUN_KEYS {
                    MIN_RUN_KEYS
                } else if spread > MAX_RUN_KEYS {
                    MAX_RUN_KEYS
                } else {
                    spread
                }
            }
        }
    }

    /// Reads `keys` through `read`, one value or absence per key, in caller
    /// order.
    ///
    /// `read` must be one consistent view (a transaction, snapshot or request
    /// read view): each run is its own call, and only a view bound to a start
    /// sequence makes the runs agree. Duplicate keys then read the same value.
    /// The first failed run fails the batch and cancels the others, so no
    /// partial result escapes. A backend that returns the wrong number of
    /// rows fails closed with [`HelixDbError::InvariantViolation`].
    pub(crate) async fn multi_get<R, K>(self, read: &R, keys: &[K]) -> Result<Vec<Option<Bytes>>>
    where
        R: DbReadOps + Sync + ?Sized,
        K: AsRef<[u8]> + Send + Sync,
    {
        self.multi_get_within(read, keys, &EXTRA_RUNS).await
    }

    /// [`Self::multi_get`], drawing runs beyond the first from `extra_runs`.
    ///
    /// The batch takes the extra runs that are free when it starts, never
    /// waiting for one, and returns them when it ends: with none free it
    /// still reads its runs one at a time.
    async fn multi_get_within<R, K>(
        self,
        read: &R,
        keys: &[K],
        extra_runs: &Semaphore,
    ) -> Result<Vec<Option<Bytes>>>
    where
        R: DbReadOps + Sync + ?Sized,
        K: AsRef<[u8]> + Send + Sync,
    {
        let run_keys = self.run_keys(keys.len());
        if keys.len() <= run_keys {
            return one_row_per_key(keys.len(), read.multi_get(keys).await?);
        }
        let wanted = keys.len().div_ceil(run_keys).min(MAX_RUNS_IN_FLIGHT) - 1;
        let extra = (1..=wanted)
            .rev()
            .find_map(|runs| extra_runs.try_acquire_many(runs as u32).ok());
        // `extra` holds its permits until the batch ends.
        let width = 1 + extra.as_ref().map_or(0, |permits| permits.num_permits());
        // Contiguous runs of sorted keys keep keys that share an SST, a block
        // or an object-store part in one call, whatever order the caller used.
        let mut order = (0..keys.len()).collect::<Vec<_>>();
        order.sort_unstable_by(|&left, &right| keys[left].as_ref().cmp(keys[right].as_ref()));
        let sorted = order
            .iter()
            .map(|&position| keys[position].as_ref())
            .collect::<Vec<_>>();
        let mut sorted_rows = vec![None; keys.len()];
        // A plain loop rather than `stream::iter(..).map(..).buffer_unordered`:
        // a closure returning a future that borrows its argument makes rustc
        // unable to prove callers' boxed futures `Send`.
        {
            let mut runs = sorted_rows
                .chunks_mut(run_keys)
                .zip(sorted.chunks(run_keys));
            let mut in_flight = futures::stream::FuturesUnordered::new();
            loop {
                while in_flight.len() < width {
                    let Some((slots, run)) = runs.next() else {
                        break;
                    };
                    in_flight.push(read_run(read, run, slots));
                }
                let Some(finished) = in_flight.next().await else {
                    break;
                };
                finished?;
            }
        }
        let mut rows = vec![None; keys.len()];
        order
            .into_iter()
            .zip(sorted_rows)
            .for_each(|(position, row)| rows[position] = row);
        Ok(rows)
    }
}

/// Reads one run of sorted keys into its slots of the batch result.
async fn read_run<R>(read: &R, run: &[&[u8]], slots: &mut [Option<Bytes>]) -> Result<()>
where
    R: DbReadOps + Sync + ?Sized,
{
    let rows = one_row_per_key(run.len(), read.multi_get(run).await?)?;
    slots
        .iter_mut()
        .zip(rows)
        .for_each(|(slot, row)| *slot = row);
    Ok(())
}

/// Returns `rows` when it has one entry per requested key.
fn one_row_per_key(keys: usize, rows: Vec<Option<Bytes>>) -> Result<Vec<Option<Bytes>>> {
    match rows.len() == keys {
        true => Ok(rows),
        false => Err(HelixDbError::InvariantViolation(format!(
            "multi_get returned {} rows for {keys} keys",
            rows.len()
        ))),
    }
}

#[cfg(test)]
mod tests;
