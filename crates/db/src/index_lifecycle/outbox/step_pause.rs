//! Test pause of one step between staging and commit.
//!
//! [`StepPause::arm`] holds the first step on one database whose claimed
//! progress matches, after it stages and before it commits, until the pause
//! is released or dropped. It counts every staging of a matching step, so a
//! step whose commit failed and ran again stages twice.
//! [`StepPause::arm_in_process`] does the same for a database that is not
//! open yet, whose first steps run before its opener returns.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use slatedb::Db;
use tokio::sync::Notify;

use crate::index_lifecycle::{IndexOperationProgress, IndexOperationRecord};

/// Selects the steps a pause matches by their claimed progress.
pub(crate) type StepMatcher = fn(&IndexOperationProgress) -> bool;

struct Armed {
    /// Address of the database whose steps this pause matches, or `None`
    /// for every database of the process.
    db: Option<usize>,
    matches: StepMatcher,
    stagings: AtomicUsize,
    reached: Notify,
    released: Notify,
}

/// Pauses of every open test database; each matches only its own database.
static ARMED: Mutex<Vec<Arc<Armed>>> = Mutex::new(Vec::new());

/// One armed pause, disarmed and released on drop.
pub(crate) struct StepPause(Arc<Armed>);

impl StepPause {
    /// Arms a pause for steps on `db` whose claimed progress `matches`.
    pub(crate) fn arm(db: &Db, matches: StepMatcher) -> Self {
        Self::armed(Some(std::ptr::from_ref(db).addr()), matches)
    }

    /// Arms a pause for steps on any database of this process whose claimed
    /// progress `matches`. The caller runs in a process of its own.
    pub(crate) fn arm_in_process(matches: StepMatcher) -> Self {
        Self::armed(None, matches)
    }

    fn armed(db: Option<usize>, matches: StepMatcher) -> Self {
        let armed = Arc::new(Armed {
            db,
            matches,
            stagings: AtomicUsize::new(0),
            reached: Notify::new(),
            released: Notify::new(),
        });
        ARMED
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Arc::clone(&armed));
        Self(armed)
    }

    /// Waits until the first matching step has staged and is held.
    pub(crate) async fn reached(&self) {
        self.0.reached.notified().await;
    }

    /// Lets the held step go on to commit.
    pub(crate) fn release(&self) {
        self.0.released.notify_one();
    }

    /// Returns how many times a matching step has staged.
    pub(crate) fn stagings(&self) -> usize {
        self.0.stagings.load(Ordering::SeqCst)
    }
}

impl Drop for StepPause {
    fn drop(&mut self) {
        ARMED
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|armed| !Arc::ptr_eq(armed, &self.0));
        self.0.released.notify_one();
    }
}

/// Counts a staged step against every matching pause and holds the first.
pub(super) async fn hold(db: &Db, operation: &IndexOperationRecord) {
    let db = std::ptr::from_ref(db).addr();
    let matching = ARMED
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .filter(|armed| {
            armed.db.is_none_or(|own| own == db) && (armed.matches)(operation.progress())
        })
        .cloned()
        .collect::<Vec<_>>();
    for armed in matching {
        if armed.stagings.fetch_add(1, Ordering::SeqCst) == 0 {
            armed.reached.notify_one();
            armed.released.notified().await;
        }
    }
}
