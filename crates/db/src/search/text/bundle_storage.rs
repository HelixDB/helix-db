use std::fmt;
use std::io;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use range_cache::{CachedReader, RangeCache, RangeReader, ReadError, ReaderConfig};
use slatedb::object_store::path::Path as ObjectStorePath;
use slatedb::object_store::{ObjectStore, ObjectStoreExt};

use super::split::TextSplitFooterData;

const MAX_FETCH_CONCURRENCY: usize = 16;

#[async_trait]
pub(crate) trait SplitStorage: fmt::Debug + Send + Sync + 'static {
    async fn get_slice(&self, path: &Path, range: Range<usize>) -> io::Result<Bytes>;

    fn file_num_bytes(&self, path: &Path) -> io::Result<usize>;
}

struct SplitStorageReader {
    storage: Arc<dyn SplitStorage>,
}

#[async_trait]
impl RangeReader<PathBuf> for SplitStorageReader {
    type Error = io::Error;

    async fn read_range(&self, path: &PathBuf, range: Range<usize>) -> io::Result<Bytes> {
        self.storage.get_slice(path, range).await
    }
}

pub(crate) struct CachedSplitStorage {
    reader: CachedReader<PathBuf, SplitStorageReader>,
}

impl CachedSplitStorage {
    pub(crate) fn new(storage: Arc<dyn SplitStorage>, cache: RangeCache<PathBuf>) -> Self {
        Self {
            reader: CachedReader::new(
                Arc::new(SplitStorageReader { storage }),
                cache,
                ReaderConfig::new(
                    NonZeroUsize::new(MAX_FETCH_CONCURRENCY)
                        .expect("split fetch concurrency is non-zero"),
                ),
            ),
        }
    }
}

impl fmt::Debug for CachedSplitStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("CachedSplitStorage").finish()
    }
}

#[async_trait]
impl SplitStorage for CachedSplitStorage {
    async fn get_slice(&self, path: &Path, range: Range<usize>) -> io::Result<Bytes> {
        self.reader
            .read(&path.to_path_buf(), range)
            .await
            .map_err(|error| match error {
                ReadError::Range(error) => io::Error::new(io::ErrorKind::InvalidInput, error),
                ReadError::Source(error) => error,
                ReadError::ShortRead {
                    range,
                    expected,
                    actual,
                } => io::Error::new(
                    if actual < expected {
                        io::ErrorKind::UnexpectedEof
                    } else {
                        io::ErrorKind::InvalidData
                    },
                    format!(
                        "split storage returned {actual} bytes for {range:?}; expected {expected}"
                    ),
                ),
            })
    }

    fn file_num_bytes(&self, path: &Path) -> io::Result<usize> {
        self.reader.source().storage.file_num_bytes(path)
    }
}

#[derive(Clone)]
pub(crate) struct ObjectStoreSplitBundleStorage {
    store: Arc<dyn ObjectStore>,
    split_path: ObjectStorePath,
    footer: Arc<TextSplitFooterData>,
}

impl ObjectStoreSplitBundleStorage {
    pub(crate) fn new(
        store: Arc<dyn ObjectStore>,
        split_path: ObjectStorePath,
        footer: Arc<TextSplitFooterData>,
    ) -> Self {
        Self {
            store,
            split_path,
            footer,
        }
    }

    fn entry_for_path(&self, path: &Path) -> io::Result<&super::split::TextSplitFooterEntry> {
        let path_key = path.to_string_lossy();
        self.footer.files.get(path_key.as_ref()).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("split bundle is missing '{}'", path.display()),
            )
        })
    }

    fn split_range_for_path(&self, path: &Path, range: Range<usize>) -> io::Result<Range<u64>> {
        let entry = self.entry_for_path(path)?;
        let file_len = usize::try_from(entry.size_bytes).map_err(|_| {
            io::Error::other(format!(
                "file '{}' length exceeds platform limits",
                path.display()
            ))
        })?;
        if range.end > file_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "requested range {}..{} exceeds '{}' length {}",
                    range.start,
                    range.end,
                    path.display(),
                    file_len
                ),
            ));
        }
        Ok((entry.start + range.start as u64)..(entry.start + range.end as u64))
    }
}

impl fmt::Debug for ObjectStoreSplitBundleStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObjectStoreSplitBundleStorage")
            .field("split_path", &self.split_path)
            .finish()
    }
}

#[async_trait]
impl SplitStorage for ObjectStoreSplitBundleStorage {
    async fn get_slice(&self, path: &Path, range: Range<usize>) -> io::Result<Bytes> {
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        let split_range = self.split_range_for_path(path, range)?;
        self.store
            .get_range(&self.split_path, split_range.clone())
            .await
            .map_err(|error| {
                io::Error::other(format!(
                    "failed to read split range {}..{} for '{}': {error}",
                    split_range.start,
                    split_range.end,
                    path.display()
                ))
            })
    }

    fn file_num_bytes(&self, path: &Path) -> io::Result<usize> {
        let entry = self.entry_for_path(path)?;
        usize::try_from(entry.size_bytes).map_err(|_| {
            io::Error::other(format!(
                "file '{}' length exceeds platform limits",
                path.display()
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use range_cache::CacheCapacity;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::PutPayload;

    use super::*;

    fn footer() -> Arc<TextSplitFooterData> {
        Arc::new(TextSplitFooterData {
            version: 1,
            files: BTreeMap::from([
                (
                    "segment.term".to_string(),
                    super::super::split::TextSplitFooterEntry {
                        start: 2,
                        end: 8,
                        size_bytes: 6,
                    },
                ),
                (
                    "empty".to_string(),
                    super::super::split::TextSplitFooterEntry {
                        start: 8,
                        end: 8,
                        size_bytes: 0,
                    },
                ),
            ]),
        })
    }

    #[derive(Debug)]
    struct CountingStorage {
        data: Bytes,
        calls: AtomicUsize,
        delay: Duration,
    }

    #[async_trait]
    impl SplitStorage for CountingStorage {
        async fn get_slice(&self, _path: &Path, range: Range<usize>) -> io::Result<Bytes> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            Ok(self.data.slice(range))
        }

        fn file_num_bytes(&self, _path: &Path) -> io::Result<usize> {
            Ok(self.data.len())
        }
    }

    #[derive(Debug)]
    struct ShortStorage {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl SplitStorage for ShortStorage {
        async fn get_slice(&self, _path: &Path, _range: Range<usize>) -> io::Result<Bytes> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Bytes::new())
        }

        fn file_num_bytes(&self, _path: &Path) -> io::Result<usize> {
            Ok(4)
        }
    }

    #[derive(Debug)]
    struct FailingStorage {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl SplitStorage for FailingStorage {
        async fn get_slice(&self, _path: &Path, _range: Range<usize>) -> io::Result<Bytes> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(io::Error::other("read failed"))
        }

        fn file_num_bytes(&self, _path: &Path) -> io::Result<usize> {
            Ok(4)
        }
    }

    #[tokio::test]
    async fn cached_split_storage_fetches_only_missing_ranges_and_reuses_bytes() {
        let source = Arc::new(CountingStorage {
            data: Bytes::from_static(b"abcdefghijkl"),
            calls: AtomicUsize::new(0),
            delay: Duration::ZERO,
        });
        let cache = RangeCache::new(CacheCapacity::Unbounded);
        cache
            .insert(
                Path::new("segment.term").to_path_buf(),
                0..2,
                Bytes::from_static(b"ab"),
            )
            .expect("valid cache insert");
        let source_storage: Arc<dyn SplitStorage> = source.clone();
        let storage = CachedSplitStorage::new(source_storage, cache);

        assert_eq!(
            storage.file_num_bytes(Path::new("segment.term")).unwrap(),
            12
        );
        assert_eq!(
            storage
                .get_slice(Path::new("segment.term"), 0..6)
                .await
                .expect("first read"),
            Bytes::from_static(b"abcdef")
        );
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            storage
                .get_slice(Path::new("segment.term"), 1..5)
                .await
                .expect("cached read"),
            Bytes::from_static(b"bcde")
        );
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            storage
                .get_slice(Path::new("segment.term"), 0..12)
                .await
                .expect("read all"),
            Bytes::from_static(b"abcdefghijkl")
        );
        assert_eq!(source.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cached_split_storage_coalesces_identical_reads() {
        let source = Arc::new(CountingStorage {
            data: Bytes::from_static(b"abcdefghijklmnop"),
            calls: AtomicUsize::new(0),
            delay: Duration::from_millis(25),
        });
        let source_storage: Arc<dyn SplitStorage> = source.clone();
        let storage =
            CachedSplitStorage::new(source_storage, RangeCache::new(CacheCapacity::Unbounded));

        let (left, right) = tokio::join!(
            storage.get_slice(Path::new("segment.term"), 0..16),
            storage.get_slice(Path::new("segment.term"), 0..16),
        );
        assert_eq!(left.expect("left"), Bytes::from_static(b"abcdefghijklmnop"));
        assert_eq!(
            right.expect("right"),
            Bytes::from_static(b"abcdefghijklmnop")
        );
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cached_split_storage_rejects_short_reads_and_retries_failures() {
        let short = Arc::new(ShortStorage {
            calls: AtomicUsize::new(0),
        });
        let short_storage: Arc<dyn SplitStorage> = short.clone();
        let cached_short =
            CachedSplitStorage::new(short_storage, RangeCache::new(CacheCapacity::Unbounded));
        assert_eq!(
            cached_short
                .get_slice(Path::new("segment.term"), 0..4)
                .await
                .expect_err("short read")
                .kind(),
            io::ErrorKind::UnexpectedEof
        );
        assert_eq!(short.calls.load(Ordering::SeqCst), 1);

        let failing = Arc::new(FailingStorage {
            calls: AtomicUsize::new(0),
        });
        let failing_storage: Arc<dyn SplitStorage> = failing.clone();
        let cached_failure =
            CachedSplitStorage::new(failing_storage, RangeCache::new(CacheCapacity::Unbounded));
        for _ in 0..2 {
            assert!(cached_failure
                .get_slice(Path::new("segment.term"), 0..4)
                .await
                .expect_err("source failure")
                .to_string()
                .contains("read failed"));
        }
        assert_eq!(failing.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn object_store_split_storage_maps_file_ranges_into_bundle_ranges() {
        let store = Arc::new(InMemory::new());
        let split_path = ObjectStorePath::from("splits/generation.split");
        store
            .put(
                &split_path,
                PutPayload::from_bytes(Bytes::from_static(b"--abcdef")),
            )
            .await
            .expect("write split bundle");
        let storage = ObjectStoreSplitBundleStorage::new(store, split_path, footer());

        assert_eq!(
            storage
                .get_slice(Path::new("segment.term"), 1..4)
                .await
                .expect("read slice"),
            Bytes::from_static(b"bcd")
        );
        assert_eq!(
            storage
                .get_slice(Path::new("segment.term"), 0..6)
                .await
                .expect("read all"),
            Bytes::from_static(b"abcdef")
        );
        assert!(storage
            .get_slice(Path::new("empty"), 0..0)
            .await
            .expect("empty slice")
            .is_empty());
        assert_eq!(
            storage
                .file_num_bytes(Path::new("segment.term"))
                .expect("file length"),
            6
        );
    }

    #[tokio::test]
    async fn object_store_split_storage_rejects_missing_and_out_of_bounds_paths() {
        let store = Arc::new(InMemory::new());
        let split_path = ObjectStorePath::from("splits/generation.split");
        store
            .put(
                &split_path,
                PutPayload::from_bytes(Bytes::from_static(b"--abcdef")),
            )
            .await
            .expect("write split bundle");
        let storage = ObjectStoreSplitBundleStorage::new(store, split_path, footer());

        assert_eq!(
            storage
                .file_num_bytes(Path::new("missing"))
                .expect_err("missing path")
                .kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            storage
                .get_slice(Path::new("segment.term"), 0..7)
                .await
                .expect_err("range exceeds file")
                .kind(),
            io::ErrorKind::UnexpectedEof
        );

        let missing_bundle = ObjectStoreSplitBundleStorage::new(
            Arc::new(InMemory::new()),
            ObjectStorePath::from("splits/missing.split"),
            footer(),
        );
        assert!(missing_bundle
            .get_slice(Path::new("segment.term"), 0..1)
            .await
            .expect_err("missing bundle read fails")
            .to_string()
            .contains("failed to read split range"));
    }
}
