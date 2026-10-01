use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use slatedb::config::{ReadOptions, ScanOptions};
use slatedb::object_store::memory::InMemory;
use slatedb::{Db, DbReadOps, IsolationLevel};

use super::*;

/// Reader that records every `multi_get` call and can fail or short-read one.
///
/// Each call yields before delegating, so runs of one batch are all started
/// before any finishes and the in-flight peak is observable on one thread.
struct Recording<'a, R: ?Sized> {
    inner: &'a R,
    calls: Mutex<Vec<Vec<Vec<u8>>>>,
    in_flight: AtomicUsize,
    peak_in_flight: AtomicUsize,
    fault: Fault,
}

#[derive(Clone, Copy)]
enum Fault {
    None,
    /// The call with this 1-based number fails.
    FailCall(usize),
    /// Every call returns one row fewer than it was asked for.
    ShortRead,
}

impl<'a, R: ?Sized> Recording<'a, R> {
    fn new(inner: &'a R, fault: Fault) -> Self {
        Self {
            inner,
            calls: Mutex::new(Vec::new()),
            in_flight: AtomicUsize::new(0),
            peak_in_flight: AtomicUsize::new(0),
            fault,
        }
    }

    fn calls(&self) -> Vec<Vec<Vec<u8>>> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl<R: DbReadOps + Send + Sync + ?Sized> DbReadOps for Recording<'_, R> {
    async fn get_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> std::result::Result<Option<Bytes>, slatedb::Error> {
        self.inner.get_with_options(key, options).await
    }

    async fn multi_get_with_options<K>(
        &self,
        keys: &[K],
        options: &ReadOptions,
    ) -> std::result::Result<Vec<Option<Bytes>>, slatedb::Error>
    where
        K: AsRef<[u8]> + Send + Sync,
    {
        let call = {
            let mut calls = self.calls.lock().unwrap();
            calls.push(keys.iter().map(|key| key.as_ref().to_vec()).collect());
            calls.len()
        };
        let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak_in_flight.fetch_max(in_flight, Ordering::SeqCst);
        tokio::task::yield_now().await;
        let rows = match self.fault {
            Fault::FailCall(failing) if failing == call => Err(slatedb::Error::unavailable(
                format!("injected failure on call {call}"),
            )),
            Fault::None | Fault::FailCall(_) => {
                self.inner.multi_get_with_options(keys, options).await
            }
            Fault::ShortRead => {
                self.inner
                    .multi_get_with_options(keys, options)
                    .await
                    .map(|mut rows| {
                        rows.pop();
                        rows
                    })
            }
        };
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        rows
    }

    async fn get_key_value_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> std::result::Result<Option<slatedb::KeyValue>, slatedb::Error> {
        self.inner.get_key_value_with_options(key, options).await
    }

    async fn scan_with_options<T>(
        &self,
        range: T,
        options: &ScanOptions,
    ) -> std::result::Result<slatedb::DbIterator, slatedb::Error>
    where
        T: slatedb::ByteRangeBounds + Send,
    {
        self.inner.scan_with_options(range, options).await
    }
}

fn key(id: usize) -> Vec<u8> {
    format!("key-{id:06}").into_bytes()
}

/// Every third key is absent; the others hold their own key as the value.
fn present(id: usize) -> bool {
    !id.is_multiple_of(3)
}

async fn database(ids: std::ops::RangeInclusive<usize>) -> Db {
    let db = Db::open("batch-reads", Arc::new(InMemory::new()))
        .await
        .unwrap();
    let mut batch = slatedb::WriteBatch::new();
    ids.filter(|id| present(*id))
        .for_each(|id| batch.put(key(id), key(id)));
    db.write(batch).await.unwrap();
    db
}

#[test]
fn runs_spread_a_batch_over_at_most_sixteen_runs_of_four_to_thirty_two_keys() {
    for (keys, run_keys) in [
        (0, 4),
        (1, 4),
        (4, 4),
        (5, 4),
        (32, 4),
        (64, 4),
        (65, 5),
        (256, 16),
        (512, 32),
        (513, 32),
        (4_096, 32),
    ] {
        assert_eq!(BatchReads::Concurrent.run_keys(keys), run_keys, "{keys}");
        assert_eq!(BatchReads::Single.run_keys(keys), keys, "{keys}");
    }
}

/// Concurrent runs return exactly what one call returns, in caller order,
/// with absent keys and duplicates in place, while each run reads a sorted,
/// disjoint slice of the keys and at most sixteen runs are in flight.
#[tokio::test]
async fn concurrent_runs_match_one_call_in_caller_order() {
    let db = database(1..=1_100).await;
    for len in [0, 1, 3, 4, 5, 17, 32, 33, 64, 65, 257, 1_031] {
        // Descending order is the opposite of key order; one key repeats on
        // both sides of the batch.
        let mut keys = (1..=len).rev().map(key).collect::<Vec<_>>();
        if len > 2 {
            keys.push(key(len / 2));
        }
        let expected = keys
            .iter()
            .map(|key| {
                let id = std::str::from_utf8(&key[4..]).unwrap().parse().unwrap();
                present(id).then(|| Bytes::from(key.clone()))
            })
            .collect::<Vec<_>>();

        let single = Recording::new(&db, Fault::None);
        assert_eq!(
            BatchReads::Single.multi_get(&single, &keys).await.unwrap(),
            expected,
            "single {len}"
        );
        assert_eq!(single.calls().len(), 1, "single {len}");

        let concurrent = Recording::new(&db, Fault::None);
        let extra_runs = Semaphore::new(PROCESS_EXTRA_RUNS);
        assert_eq!(
            BatchReads::Concurrent
                .multi_get_within(&concurrent, &keys, &extra_runs)
                .await
                .unwrap(),
            expected,
            "concurrent {len}"
        );
        assert_eq!(extra_runs.available_permits(), PROCESS_EXTRA_RUNS);
        let run_keys = BatchReads::Concurrent.run_keys(keys.len());
        let calls = concurrent.calls();
        assert_eq!(
            calls.len(),
            keys.len().div_ceil(run_keys).max(1),
            "concurrent {len}"
        );
        let mut read = calls.iter().flatten().cloned().collect::<Vec<_>>();
        let mut requested = keys.clone();
        read.sort();
        requested.sort();
        assert_eq!(read, requested, "every key is read exactly once");
        let mut runs = calls
            .iter()
            .filter(|run| !run.is_empty())
            .collect::<Vec<_>>();
        runs.sort();
        assert!(runs.iter().all(|run| run.len() <= run_keys));
        // One call keeps the caller's order; several read sorted, disjoint runs.
        assert!(runs.len() == 1 || runs.iter().all(|run| run.is_sorted()));
        assert!(
            runs.len() == 1
                || runs
                    .windows(2)
                    .all(|pair| pair[0].last() <= pair[1].first()),
            "runs are disjoint slices of the sorted keys"
        );
        let peak = concurrent.peak_in_flight.load(Ordering::SeqCst);
        assert_eq!(
            peak,
            calls.len().min(MAX_RUNS_IN_FLIGHT),
            "concurrent {len}"
        );
    }
}

/// Every run reads the transaction's snapshot and staged writes, even when
/// later commits and a flush land after the transaction started.
#[tokio::test]
async fn every_run_reads_the_transaction_view() {
    let db = database(1..=40).await;
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    txn.put(key(1), b"staged").unwrap();
    txn.delete(key(2)).unwrap();
    db.put(key(4), b"committed later").await.unwrap();
    db.put(key(3), b"created later").await.unwrap();
    db.flush().await.unwrap();

    let keys = (1..=40).map(key).collect::<Vec<_>>();
    let mut expected = Vec::new();
    for key in &keys {
        expected.push(txn.get(key).await.unwrap());
    }
    assert_eq!(expected[0], Some(Bytes::from_static(b"staged")));
    assert_eq!(expected[1], None);
    assert_eq!(expected[2], None);
    assert_eq!(expected[3], Some(Bytes::from(key(4))));

    let read = Recording::new(&txn, Fault::None);
    assert_eq!(
        BatchReads::Concurrent
            .multi_get(&read, &keys)
            .await
            .unwrap(),
        expected
    );
    assert_eq!(read.calls().len(), 10);
    txn.rollback();
}

/// A failed run fails the whole batch; no partial rows escape.
#[tokio::test]
async fn a_failed_run_fails_the_batch() {
    let db = database(1..=33).await;
    let keys = (1..=33).map(key).collect::<Vec<_>>();
    for (batch_reads, failing_call) in [(BatchReads::Single, 1), (BatchReads::Concurrent, 5)] {
        let read = Recording::new(&db, Fault::FailCall(failing_call));
        let error = batch_reads.multi_get(&read, &keys).await.unwrap_err();
        assert!(
            matches!(error, HelixDbError::Storage(_)),
            "{batch_reads:?}: {error}"
        );
    }
}

/// A backend that drops rows fails closed instead of misaligning values.
#[tokio::test]
async fn a_short_read_fails_closed() {
    let db = database(1..=33).await;
    let keys = (1..=33).map(key).collect::<Vec<_>>();
    for batch_reads in [BatchReads::Single, BatchReads::Concurrent] {
        let read = Recording::new(&db, Fault::ShortRead);
        let error = batch_reads.multi_get(&read, &keys).await.unwrap_err();
        assert!(
            matches!(error, HelixDbError::InvariantViolation(ref message) if message.starts_with("multi_get returned")),
            "{batch_reads:?}: {error}"
        );
    }
}

/// Runs beyond the first come only from the free allowance: a batch never
/// waits for it, reads its runs one at a time when none is free, and returns
/// what it took.
#[tokio::test]
async fn extra_runs_come_only_from_the_free_allowance() {
    let db = database(1..=64).await;
    let keys = (1..=64).map(key).collect::<Vec<_>>();
    let expected = BatchReads::Single.multi_get(&db, &keys).await.unwrap();
    for (free, peak) in [(0, 1), (3, 4), (15, 16), (40, 16)] {
        let extra_runs = Semaphore::new(free);
        let read = Recording::new(&db, Fault::None);
        assert_eq!(
            BatchReads::Concurrent
                .multi_get_within(&read, &keys, &extra_runs)
                .await
                .unwrap(),
            expected,
            "{free} free"
        );
        assert_eq!(read.calls().len(), 16, "{free} free");
        assert_eq!(
            read.peak_in_flight.load(Ordering::SeqCst),
            peak,
            "{free} free"
        );
        assert_eq!(extra_runs.available_permits(), free, "{free} free");
    }
}
