//! Pacing of the background vector cache refresh loop on reader nodes.
//!
//! A reader store attaches only to request snapshots at the exact sequence it
//! was hydrated at, so a reader refreshes after it applies newer state. Every
//! rescan can read up to the whole cache budget, and the reader may advance
//! again at any moment. [`run_reader_refreshes`] therefore bounds the share of
//! the node that passes take by how long each one ran, backs off while the
//! reader keeps outrunning loads, and retries early once the reader goes
//! quiet. Stores stay exact-sequence, so pacing never lets a reader serve rows
//! older than its snapshot.

use std::future::Future;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::Instant;

use super::hydration::{reader_advances_past, VectorCacheHydrationOutcome};

/// Idle time after every reader pass, in doublings of how long it ran.
///
/// No pass starts until `2^2 = 4` times the previous pass's duration has
/// elapsed since it ended, so reader rescans take at most a fifth of the
/// node's time however often the reader advances.
const MIN_IDLE_DOUBLINGS: u32 = 2;

/// Most idle doublings while the reader keeps outrunning passes.
///
/// At `2^5 = 32` times the pass, rescans the reader keeps outrunning take at
/// most one part in 33 of the node's time.
const MAX_IDLE_DOUBLINGS: u32 = 5;

/// Runs reader refresh passes until shutdown or until `pass` returns `None`.
///
/// `sequence` projects the reader's applied sequence out of `status`, and
/// each `pass` call runs one refresh and reports its outcome, or `None` once
/// the database is gone.
///
/// * The first pass starts at once, and `initial_refresh` becomes `true` when
///   it ends.
/// * A pass becomes due when the applied sequence rises above the one seen
///   when the previous pass started, when the previous pass was outrun and
///   left stale stores behind, or `interval` after the previous pass ended.
///   Status changes that keep the sequence never make a pass due, and a
///   closed status channel leaves the interval as the only trigger.
/// * After a pass that ran for `P`, no pass starts until `P * 2^d` has
///   elapsed since it ended. `d` is two plus the outrun streak, capped at
///   five; the streak grows by one after each outrun pass and shrinks by one
///   after each settled pass, so a settled pass after a run of outrun ones
///   steps the backoff down one level instead of resetting it. Rescans
///   therefore take at most a fifth of the node's time whatever the
///   outcomes, and one part in 33 while the reader keeps outrunning them.
/// * Past `4P`, a backed-off pass starts early once the reader has gone `P`
///   without advancing, so the cache is rebuilt soon after writes stop
///   instead of after the whole backoff.
/// * Shutdown, a dropped shutdown sender, or `pass` returning `None` ends the
///   loop.
pub(crate) async fn run_reader_refreshes<T, Pass, PassFuture>(
    mut status: watch::Receiver<T>,
    sequence: impl Fn(&T) -> u64,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
    initial_refresh: watch::Sender<bool>,
    mut pass: Pass,
) where
    Pass: FnMut() -> PassFuture,
    PassFuture: Future<Output = Option<VectorCacheHydrationOutcome>>,
{
    let mut outrun_streak = 0_u32;
    loop {
        if *shutdown.borrow() {
            return;
        }
        let mut applied = sequence(&status.borrow());
        let started = Instant::now();
        let Some(outcome) = pass().await else {
            return;
        };
        let ended = Instant::now();
        let ran = ended.saturating_duration_since(started);
        initial_refresh.send_if_modified(|refreshed| !std::mem::replace(refreshed, true));
        outrun_streak = match outcome {
            VectorCacheHydrationOutcome::Settled => outrun_streak.saturating_sub(1),
            VectorCacheHydrationOutcome::Outrun => {
                (outrun_streak + 1).min(MAX_IDLE_DOUBLINGS - MIN_IDLE_DOUBLINGS)
            }
        };
        // Deadlines are offsets from `ended`: `Duration` arithmetic saturates
        // where adding a huge duration to an `Instant` would panic.
        let floor = ran.saturating_mul(1 << MIN_IDLE_DOUBLINGS);
        let backoff = ran.saturating_mul(1 << (MIN_IDLE_DOUBLINGS + outrun_streak));
        let mut due = outcome == VectorCacheHydrationOutcome::Outrun;
        let mut quiet_since = Duration::ZERO;
        loop {
            let start_at = floor.max(backoff.min(quiet_since.saturating_add(ran)));
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
                advanced = reader_advances_past(&mut status, &sequence, applied) => {
                    applied = advanced;
                    due = true;
                    quiet_since = ended.elapsed();
                }
                () = tokio::time::sleep(interval.saturating_sub(ended.elapsed())), if !due => {
                    due = true;
                }
                () = tokio::time::sleep(start_at.saturating_sub(ended.elapsed())), if due => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    const PASS: Duration = Duration::from_secs(1);
    const INTERVAL: Duration = Duration::from_secs(20);
    /// Reader advance cadence, shorter than a pass and never landing on a
    /// scripted pass boundary.
    const ADVANCE_EVERY: Duration = Duration::from_millis(350);

    /// Runs the loop over scripted passes of [`PASS`] and returns when each
    /// started, in milliseconds since the loop began rounded to 10 ms so timer
    /// rounding cannot flip an assertion.
    async fn pass_starts<T>(
        status: watch::Receiver<T>,
        sequence: fn(&T) -> u64,
        outcomes: &[VectorCacheHydrationOutcome],
    ) -> Vec<u128> {
        let origin = Instant::now();
        let script = Mutex::new(outcomes.iter().copied());
        let starts = Mutex::new(Vec::new());
        let (_shutdown, shutdown_rx) = watch::channel(false);
        let (initial_refresh, refreshed) = watch::channel(false);
        let (script, recorded) = (&script, &starts);
        run_reader_refreshes(
            status,
            sequence,
            INTERVAL,
            shutdown_rx,
            initial_refresh,
            move || async move {
                let outcome = script.lock().unwrap().next()?;
                recorded
                    .lock()
                    .unwrap()
                    .push((origin.elapsed().as_millis() + 5) / 10 * 10);
                tokio::time::sleep(PASS).await;
                Some(outcome)
            },
        )
        .await;
        assert!(
            *refreshed.borrow(),
            "the first pass signals the initial refresh"
        );
        starts.into_inner().unwrap()
    }

    /// Advances the reader's sequence every [`ADVANCE_EVERY`], `advances` times.
    fn advance_reader(status: watch::Sender<u64>, advances: usize) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            for _ in 0..advances {
                tokio::time::sleep(ADVANCE_EVERY).await;
                status.send_modify(|sequence| *sequence += 1);
            }
            std::future::pending::<()>().await;
        })
    }

    #[tokio::test(start_paused = true)]
    async fn settled_passes_idle_four_times_their_run_under_steady_advances() {
        let (status, observed) = watch::channel(0_u64);
        let writer = advance_reader(status, usize::MAX);

        let starts = pass_starts(
            observed,
            |sequence| *sequence,
            &[VectorCacheHydrationOutcome::Settled; 4],
        )
        .await;
        writer.abort();

        // Each 1 s pass is followed by 4 s without one, although the reader
        // advances every 350 ms throughout.
        assert_eq!(starts, [0, 5_000, 10_000, 15_000]);
    }

    #[tokio::test(start_paused = true)]
    async fn outrun_passes_back_off_to_the_cap_and_settled_passes_step_back_down() {
        let (status, observed) = watch::channel(0_u64);
        let writer = advance_reader(status, usize::MAX);

        let starts = pass_starts(
            observed,
            |sequence| *sequence,
            &[
                VectorCacheHydrationOutcome::Outrun,
                VectorCacheHydrationOutcome::Outrun,
                VectorCacheHydrationOutcome::Outrun,
                VectorCacheHydrationOutcome::Outrun,
                VectorCacheHydrationOutcome::Settled,
                VectorCacheHydrationOutcome::Settled,
                VectorCacheHydrationOutcome::Settled,
            ],
        )
        .await;
        writer.abort();

        // Idle after each 1 s pass: 8, 16, 32 and (capped) 32 s while outrun,
        // then 16 and 8 s as settled passes shrink the streak one step each.
        assert_eq!(starts, [0, 9_000, 26_000, 59_000, 92_000, 109_000, 118_000]);
    }

    #[tokio::test(start_paused = true)]
    async fn backed_off_pass_starts_once_the_reader_stays_quiet_for_a_pass() {
        let (status, observed) = watch::channel(0_u64);
        // Seven advances, the last at 2.45 s.
        let writer = advance_reader(status, 7);

        let starts = pass_starts(
            observed,
            |sequence| *sequence,
            &[
                VectorCacheHydrationOutcome::Outrun,
                VectorCacheHydrationOutcome::Settled,
                VectorCacheHydrationOutcome::Settled,
            ],
        )
        .await;
        writer.abort();

        // The outrun pass backs off 8 s, but the reader is quiet from 2.45 s,
        // so the retry starts at the 4 s floor (5 s). The settled retry leaves
        // nothing due until the interval, 20 s after it ended.
        assert_eq!(starts, [0, 5_000, 26_000]);
    }

    #[tokio::test(start_paused = true)]
    async fn status_changes_that_keep_the_sequence_never_start_a_pass() {
        let (status, observed) = watch::channel((5_u64, 0_u64));
        let manifests = tokio::spawn(async move {
            loop {
                tokio::time::sleep(ADVANCE_EVERY).await;
                status.send_modify(|(_, manifest)| *manifest += 1);
            }
        });

        let starts = pass_starts(
            observed,
            |(sequence, _)| *sequence,
            &[VectorCacheHydrationOutcome::Settled; 2],
        )
        .await;
        manifests.abort();

        assert_eq!(starts, [0, 21_000]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_closed_status_channel_leaves_the_interval_as_the_only_trigger() {
        let (status, observed) = watch::channel(0_u64);
        drop(status);

        let starts = pass_starts(
            observed,
            |sequence| *sequence,
            &[
                VectorCacheHydrationOutcome::Outrun,
                VectorCacheHydrationOutcome::Settled,
                VectorCacheHydrationOutcome::Settled,
            ],
        )
        .await;

        // The outrun pass retries at its 4 s floor; the settled one waits for the interval.
        assert_eq!(starts, [0, 5_000, 26_000]);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_ends_the_loop_while_it_waits() {
        let (status, observed) = watch::channel(0_u64);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let (initial_refresh, mut refreshed) = watch::channel(false);
        let refreshes = tokio::spawn(run_reader_refreshes(
            observed,
            |sequence: &u64| *sequence,
            INTERVAL,
            shutdown_rx,
            initial_refresh,
            || async {
                tokio::time::sleep(PASS).await;
                Some(VectorCacheHydrationOutcome::Settled)
            },
        ));

        refreshed.wait_for(|refreshed| *refreshed).await.unwrap();
        shutdown.send_replace(true);
        tokio::time::timeout(Duration::from_secs(1), refreshes)
            .await
            .expect("shutdown ends the loop without another pass")
            .unwrap();
        drop(status);
    }
}
