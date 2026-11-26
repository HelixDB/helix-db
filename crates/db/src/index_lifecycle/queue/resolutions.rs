//! Memory that searches' storage reads of queues hold, bounded by bytes.
//!
//! # Why
//!
//! A search that no shared view serves (see [`super::shared`]) reads its
//! target's queue from storage: SlateDB resolves the whole merge-backed value
//! inside the read, and the search decodes it, so a read holds about twice
//! the bytes it resolves until the search has selected from them. Unbounded,
//! 32 concurrent searches of a 0.9 GB backlog held 65 GB. Bounding them by a
//! count of worst-case reads instead left one read at a time on hosts under
//! 32 GB, so empty queues waited behind large ones.
//!
//! # Rule
//!
//! Every read is charged, against one byte budget (an eighth of memory),
//! twice the bytes it resolves, from before it reads until its search has
//! selected. It cannot know that size before reading, so it is charged what
//! it expects and re-charged to what it resolved as soon as it has read:
//!
//! - A read at a snapshot its target was read at before is exact: a queue
//!   resolves the same bytes at one snapshot however often it is read. Each
//!   target keeps the sizes of its [`KNOWN_SIZES`] newest snapshots read.
//! - A read at any other snapshot, older or newer, probes: per target, one
//!   probe runs at a time, and a read that finds one waits for it, then
//!   charges what it knows by then. Concurrent searches mostly share a
//!   snapshot, so one probe serves them all.
//! - A probe expects the size at the nearest snapshot known (the larger of
//!   two equally near), or, for a target never read, the most one read can
//!   resolve, so the first read of a target is charged for the worst case.
//! - A read that resolved more than it was charged gives back what it holds
//!   and waits for its whole charge before it decodes, so no read holds
//!   budget while it waits for more, and no two reads can wait on each other.
//!   A probe ends only once its charge is granted.
//!
//! The budget is split in two lanes, each granted in arrival order: an
//! eighth of it for small charges, at most a 32nd of that lane each, and the
//! rest for all others. One queue would hold every small charge behind a
//! large one waiting at its head; with two, reads of empty and small queues
//! never wait behind large ones, and large ones are never overtaken
//! indefinitely. A read larger than its lane is charged all of it and runs
//! alone there.
//!
//! ```text
//! target t read at 5 resolved 2 KiB (needs 4 KiB):
//! read t@5 -> charged 4 KiB (exact)
//! read t@4 -> probes, charged 4 KiB (nearest: 5); read t@6 waits for it,
//!             then probes, expecting t@5's size (or t@4's, if larger)
//! ```
//!
//! # Bound
//!
//! Charged bytes never exceed the budget. Only a probe can resolve more than
//! it is charged: by how much its queue differs between its snapshot and the
//! nearest one known, larger or smaller, as when a queue that held a large
//! backlog at older snapshots drained since. It holds that excess only until
//! its charge is granted, and per target one probe runs at a time, until
//! then. So per target at most one read at a time holds more than it is
//! charged, and a backlog nobody read at a snapshot is found by one read,
//! not by every search that arrives.
//!
//! # What does not scale
//!
//! - Every target ever read keeps up to [`KNOWN_SIZES`] sizes (about 12
//!   bytes each), one set per index generation read during the process's
//!   lifetime.
//! - Reads of one target at snapshots not read before, as write
//!   transactions' searches are (each at its own snapshot, newer or older
//!   than the read requests'), probe one at a time; only their storage reads
//!   and any wait for a larger charge serialize, not their decodes.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use super::QueueTarget;

/// Bytes per semaphore permit: a charge of `n` bytes takes `n / UNIT`
/// permits, rounded up, so a `u32` permit count covers terabytes.
const UNIT: u64 = 1 << 10;

/// The small lane is this fraction of the budget (as a divisor).
const SMALL_LANE_SHARE: u32 = 8;

/// Sizes of the newest snapshots read that each target keeps: reads at
/// them are exact. Concurrent searches mostly share a few snapshots.
const KNOWN_SIZES: usize = 8;

/// A charge is small when it is at most this fraction (as a divisor) of the
/// small lane, so the lane holds at least this many at once.
const SMALL_CHARGES: u32 = 32;

/// The byte budget every storage read of a queue by a search is charged
/// against; see the module documentation.
#[derive(Debug)]
pub(crate) struct ResolutionBudget {
    small: Lane,
    large: Lane,
    /// Permits the largest possible read is charged, at most the large
    /// lane's.
    worst: u32,
    targets: Mutex<HashMap<QueueTarget, Expected>>,
    /// Signalled whenever a probe ends.
    probed: Notify,
}

/// Part of the budget, granted in arrival order.
#[derive(Debug)]
struct Lane {
    permits: Arc<Semaphore>,
    capacity: u32,
}

impl Lane {
    fn new(capacity: u32) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(capacity as usize)),
            capacity,
        }
    }

    async fn acquire(&self, permits: u32) -> OwnedSemaphorePermit {
        Arc::clone(&self.permits)
            .acquire_many_owned(permits.min(self.capacity))
            .await
            .expect("the budget is never closed")
    }
}

/// Which lane a charge holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LaneKind {
    Small,
    Large,
}

/// What reads of one target expect.
#[derive(Debug, Default)]
struct Expected {
    /// Permits reads at each of the [`KNOWN_SIZES`] newest snapshots read
    /// needed, by snapshot.
    known: BTreeMap<u64, u32>,
    /// A read at a snapshot not in `known` runs and has not been granted
    /// its charge.
    probing: bool,
}

impl ResolutionBudget {
    /// A budget of an eighth of `memory_ceiling`, whose largest read resolves
    /// `max_retained_bytes`, one index's retained-byte limit. An unknown
    /// ceiling allows one largest read at a time.
    pub(crate) fn for_host(memory_ceiling: Option<u64>, max_retained_bytes: u64) -> Self {
        let worst = max_retained_bytes.saturating_mul(2);
        Self::new(memory_ceiling.map_or(worst, |ceiling| ceiling / 8), worst)
    }

    /// A budget of `bytes`, whose largest read is charged `worst` bytes; both
    /// are rounded up to whole units of a kibibyte. Each lane holds at least
    /// one unit.
    pub(crate) fn new(bytes: u64, worst: u64) -> Self {
        let capacity = units(bytes).min(u32::try_from(Semaphore::MAX_PERMITS).unwrap_or(u32::MAX));
        let small = (capacity / SMALL_LANE_SHARE).max(1);
        let large = capacity.saturating_sub(small).max(1);
        Self {
            small: Lane::new(small),
            large: Lane::new(large),
            worst: units(worst).min(large),
            targets: Mutex::new(HashMap::new()),
            probed: Notify::new(),
        }
    }

    /// The lane a charge of `permits` takes.
    fn lane(&self, permits: u32) -> LaneKind {
        if permits <= (self.small.capacity / SMALL_CHARGES).max(1) {
            LaneKind::Small
        } else {
            LaneKind::Large
        }
    }

    const fn of(&self, kind: LaneKind) -> &Lane {
        match kind {
            LaneKind::Small => &self.small,
            LaneKind::Large => &self.large,
        }
    }

    /// Charges a read of `target` at `snapshot` what it expects, waiting for
    /// a probe of the target or for its lane as the module documentation
    /// describes. Call [`Charge::resolved`] once the read returns.
    pub(crate) async fn charge(self: &Arc<Self>, target: QueueTarget, snapshot: u64) -> Charge {
        let (expected, probe) = loop {
            let probed = self.probed.notified();
            tokio::pin!(probed);
            probed.as_mut().enable();
            {
                let mut targets = self.targets.lock();
                let expected = targets.entry(target).or_default();
                match (expected.known.get(&snapshot), expected.probing) {
                    (Some(&permits), _) => break (permits, false),
                    (None, false) => {
                        expected.probing = true;
                        let below = expected.known.range(..snapshot).next_back();
                        let above = expected.known.range(snapshot..).next();
                        let nearest = match (below, above) {
                            (Some((&older, &low)), Some((&newer, &high))) => {
                                match (snapshot - older).cmp(&(newer - snapshot)) {
                                    std::cmp::Ordering::Less => low,
                                    std::cmp::Ordering::Greater => high,
                                    std::cmp::Ordering::Equal => low.max(high),
                                }
                            }
                            (Some((_, &permits)), None) | (None, Some((_, &permits))) => permits,
                            (None, None) => self.worst,
                        };
                        break (nearest, true);
                    }
                    (None, true) => {}
                }
            }
            probed.await;
        };
        // Built before it waits for its lane, so a probe cancelled there ends
        // and wakes the reads waiting on it.
        let mut charge = Charge {
            budget: Arc::clone(self),
            target,
            snapshot,
            probe,
            held: None,
        };
        let lane = self.lane(expected);
        charge.held = Some((lane, self.of(lane).acquire(expected).await));
        charge
    }

    /// Permits a read that resolved `bytes` needs: twice them, at least one.
    fn need(bytes: u64) -> u32 {
        units(bytes.saturating_mul(2))
    }

    /// The snapshots `target`'s size is known at, oldest first, for tests.
    #[cfg(test)]
    pub(crate) fn known(&self, target: QueueTarget) -> Vec<u64> {
        self.targets
            .lock()
            .get(&target)
            .map(|expected| expected.known.keys().copied().collect())
            .unwrap_or_default()
    }

    /// Permits charged right now in each lane, small then large, for tests.
    #[cfg(test)]
    pub(crate) fn charged(&self) -> (u32, u32) {
        let charged = |lane: &Lane| {
            lane.capacity
                - u32::try_from(lane.permits.available_permits()).expect("within capacity")
        };
        (charged(&self.small), charged(&self.large))
    }
}

#[cfg(test)]
impl Default for ResolutionBudget {
    /// A budget of a gibibyte whose largest read is charged a mebibyte: no
    /// test read waits for it unless the test means to.
    fn default() -> Self {
        Self::new(1 << 30, 1 << 20)
    }
}

/// `bytes` in whole units, rounded up, at least one.
fn units(bytes: u64) -> u32 {
    u32::try_from(bytes.div_ceil(UNIT).max(1)).unwrap_or(u32::MAX)
}

/// One read's share of the budget, held until dropped: through the read,
/// and, once [`Self::resolved`] re-charged it, until its search has selected
/// from what it read.
#[derive(Debug)]
pub(crate) struct Charge {
    budget: Arc<ResolutionBudget>,
    target: QueueTarget,
    snapshot: u64,
    /// This read probes its target until its charge is granted after it
    /// resolved, or it is dropped.
    probe: bool,
    /// `None` only while it waits for its lane.
    held: Option<(LaneKind, OwnedSemaphorePermit)>,
}

impl Charge {
    /// Records that the read resolved `bytes` and re-charges it to what that
    /// needs: giving back what it does not, or giving back what it holds and
    /// waiting for the whole charge in the lane it now belongs in. A probe
    /// ends once that charge is granted.
    pub(crate) async fn resolved(&mut self, bytes: u64) {
        let need = ResolutionBudget::need(bytes);
        {
            let mut targets = self.budget.targets.lock();
            let known = &mut targets.entry(self.target).or_default().known;
            known.insert(self.snapshot, need);
            if known.len() > KNOWN_SIZES {
                known.pop_first();
            }
        }
        let (lane, mut permit) = self.held.take().expect("a charge holds its lane");
        let need_here = need.min(self.budget.of(lane).capacity);
        let held = u32::try_from(permit.num_permits()).expect("charges fit a u32");
        let moves = lane == LaneKind::Small && self.budget.lane(need) == LaneKind::Large;
        self.held = Some(if !moves && need_here <= held {
            if need_here < held {
                drop(permit.split((held - need_here) as usize));
            }
            (lane, permit)
        } else {
            drop(permit);
            let lane = self.budget.lane(need);
            (lane, self.budget.of(lane).acquire(need).await)
        });
        self.end_probe();
    }

    /// Ends this read's probe, if it probes, and wakes the reads waiting
    /// for it.
    fn end_probe(&mut self) {
        if !std::mem::take(&mut self.probe) {
            return;
        }
        self.budget
            .targets
            .lock()
            .get_mut(&self.target)
            .expect("a probed target keeps its record")
            .probing = false;
        self.budget.probed.notify_waiters();
    }

    /// The lane and permits this charge holds, for tests.
    #[cfg(test)]
    pub(crate) fn held(&self) -> Option<(bool, usize)> {
        self.held
            .as_ref()
            .map(|(lane, permit)| (*lane == LaneKind::Small, permit.num_permits()))
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.end_probe();
    }
}

#[cfg(test)]
#[path = "resolutions_tests.rs"]
mod tests;
