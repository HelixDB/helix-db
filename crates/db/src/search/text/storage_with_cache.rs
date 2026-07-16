use std::fmt;
use std::io;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::try_join_all;
use tantivy::directory::OwnedBytes;

use super::bundle_storage::SplitStorage;
use super::byte_range_cache::ByteRangeCache;

#[derive(Clone)]
pub(crate) struct StorageWithCache {
    storage: Arc<dyn SplitStorage>,
    cache: ByteRangeCache,
}

impl StorageWithCache {
    pub(crate) fn new(storage: Arc<dyn SplitStorage>, cache: ByteRangeCache) -> Self {
        Self { storage, cache }
    }
}

impl fmt::Debug for StorageWithCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageWithCache").finish()
    }
}

#[async_trait]
impl SplitStorage for StorageWithCache {
    async fn get_slice(&self, path: &Path, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        if let Some(bytes) = self.cache.get_slice(path, byte_range.clone()) {
            return Ok(bytes);
        }

        let missing = self.cache.missing_ranges(path, byte_range.clone());
        if missing.is_empty() {
            return self.cache.get_slice(path, byte_range).ok_or_else(|| {
                io::Error::other(format!(
                    "storage cache could not satisfy '{}' after confirming coverage",
                    path.display()
                ))
            });
        }

        let fetched = try_join_all(missing.iter().map(|gap| {
            let gap = gap.clone();
            let storage = Arc::clone(&self.storage);
            let path = path.to_path_buf();
            async move { storage.get_slice(&path, gap).await }
        }))
        .await?;

        for (gap, bytes) in missing.into_iter().zip(fetched) {
            self.cache.put_slice(path.to_path_buf(), gap, bytes);
        }

        self.cache.get_slice(path, byte_range).ok_or_else(|| {
            io::Error::other(format!(
                "storage cache could not reconstruct '{}' requested bytes",
                path.display()
            ))
        })
    }

    async fn get_all(&self, path: &Path) -> io::Result<OwnedBytes> {
        let len = self.file_num_bytes(path)?;
        self.get_slice(path, 0..len).await
    }

    fn file_num_bytes(&self, path: &Path) -> io::Result<usize> {
        self.storage.file_num_bytes(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct CountingStorage {
        data: Vec<u8>,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl SplitStorage for CountingStorage {
        async fn get_slice(&self, _path: &Path, range: Range<usize>) -> io::Result<OwnedBytes> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(OwnedBytes::new(self.data[range].to_vec()))
        }

        fn file_num_bytes(&self, _path: &Path) -> io::Result<usize> {
            Ok(self.data.len())
        }
    }

    #[derive(Debug)]
    struct ShortStorage;

    #[async_trait]
    impl SplitStorage for ShortStorage {
        async fn get_slice(&self, _path: &Path, _range: Range<usize>) -> io::Result<OwnedBytes> {
            Ok(OwnedBytes::empty())
        }

        fn file_num_bytes(&self, _path: &Path) -> io::Result<usize> {
            Ok(4)
        }
    }

    #[tokio::test]
    async fn storage_with_cache_fetches_missing_ranges_and_reuses_cached_bytes() {
        let storage = Arc::new(CountingStorage {
            data: b"abcdefghijkl".to_vec(),
            calls: AtomicUsize::new(0),
        });
        let cache = ByteRangeCache::new();
        cache.put_slice(
            Path::new("segment.term").to_path_buf(),
            0..2,
            OwnedBytes::new(b"ab".to_vec()),
        );
        let cached = StorageWithCache::new(storage.clone(), cache);

        assert_eq!(format!("{cached:?}"), "StorageWithCache");
        assert_eq!(
            cached
                .file_num_bytes(Path::new("segment.term"))
                .expect("len"),
            12
        );
        assert_eq!(
            cached
                .get_slice(Path::new("segment.term"), 0..6)
                .await
                .expect("first read")
                .as_slice(),
            b"abcdef"
        );
        assert_eq!(storage.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            cached
                .get_slice(Path::new("segment.term"), 1..5)
                .await
                .expect("cached read")
                .as_slice(),
            b"bcde"
        );
        assert_eq!(storage.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            cached
                .get_all(Path::new("segment.term"))
                .await
                .expect("read all")
                .as_slice(),
            b"abcdefghijkl"
        );
        assert_eq!(storage.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn storage_with_cache_rejects_short_gap_reads() {
        let cached = StorageWithCache::new(Arc::new(ShortStorage), ByteRangeCache::new());

        assert_eq!(cached.file_num_bytes(Path::new("segment.term")).unwrap(), 4);
        assert!(cached
            .get_slice(Path::new("segment.term"), 0..4)
            .await
            .expect_err("short storage read cannot reconstruct requested bytes")
            .to_string()
            .contains("could not reconstruct"));
    }
}
