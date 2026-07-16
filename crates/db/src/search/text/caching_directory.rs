use std::fmt;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::try_join_all;
use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{DirectoryLock, FileHandle, OwnedBytes, WatchHandle, WritePtr};
use tantivy::{Directory, HasLen};

use super::byte_range_cache::ByteRangeCache;

#[derive(Clone)]
pub(crate) struct CachingDirectory {
    underlying: Arc<dyn Directory>,
    cache: ByteRangeCache,
}

impl CachingDirectory {
    pub(crate) fn new(underlying: Arc<dyn Directory>, cache: ByteRangeCache) -> Self {
        Self { underlying, cache }
    }
}

impl fmt::Debug for CachingDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CachingDirectory")
    }
}

struct CachingFileHandle {
    path: PathBuf,
    cache: ByteRangeCache,
    underlying: Arc<dyn FileHandle>,
}

impl fmt::Debug for CachingFileHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CachingFileHandle({})", self.path.display())
    }
}

impl CachingFileHandle {
    fn fill_missing_sync(&self, byte_range: Range<usize>) -> io::Result<Option<OwnedBytes>> {
        let missing = self.cache.missing_ranges(&self.path, byte_range.clone());
        if missing.is_empty() {
            return Ok(self.cache.get_slice(&self.path, byte_range));
        }

        for gap in missing {
            let bytes = self.underlying.read_bytes(gap.clone())?;
            self.cache.put_slice(self.path.clone(), gap, bytes);
        }

        Ok(self.cache.get_slice(&self.path, byte_range))
    }

    async fn fill_missing_async(&self, byte_range: Range<usize>) -> io::Result<Option<OwnedBytes>> {
        let missing = self.cache.missing_ranges(&self.path, byte_range.clone());
        if missing.is_empty() {
            return Ok(self.cache.get_slice(&self.path, byte_range));
        }

        let fetched = try_join_all(missing.iter().map(|gap| {
            let gap = gap.clone();
            let underlying = Arc::clone(&self.underlying);
            async move { underlying.read_bytes_async(gap).await }
        }))
        .await?;

        for (gap, bytes) in missing.into_iter().zip(fetched) {
            self.cache.put_slice(self.path.clone(), gap, bytes);
        }

        Ok(self.cache.get_slice(&self.path, byte_range))
    }
}

#[async_trait]
impl FileHandle for CachingFileHandle {
    fn read_bytes(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        if let Some(bytes) = self.cache.get_slice(&self.path, byte_range.clone()) {
            return Ok(bytes);
        }
        if let Some(bytes) = self.fill_missing_sync(byte_range.clone())? {
            return Ok(bytes);
        }
        let bytes = self.underlying.read_bytes(byte_range.clone())?;
        self.cache
            .put_slice(self.path.clone(), byte_range, bytes.clone());
        Ok(bytes)
    }

    async fn read_bytes_async(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        if let Some(bytes) = self.cache.get_slice(&self.path, byte_range.clone()) {
            return Ok(bytes);
        }
        if let Some(bytes) = self.fill_missing_async(byte_range.clone()).await? {
            return Ok(bytes);
        }
        let bytes = self.underlying.read_bytes_async(byte_range.clone()).await?;
        self.cache
            .put_slice(self.path.clone(), byte_range, bytes.clone());
        Ok(bytes)
    }
}

impl HasLen for CachingFileHandle {
    fn len(&self) -> usize {
        self.underlying.len()
    }
}

impl Directory for CachingDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let underlying = self.underlying.get_file_handle(path)?;
        Ok(Arc::new(CachingFileHandle {
            path: path.to_path_buf(),
            cache: self.cache.clone(),
            underlying,
        }))
    }

    fn open_read(&self, path: &Path) -> Result<tantivy::directory::FileSlice, OpenReadError> {
        let file_handle = self.get_file_handle(path)?;
        Ok(tantivy::directory::FileSlice::new_with_num_bytes(
            file_handle.clone(),
            file_handle.len(),
        ))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let file_handle = self.get_file_handle(path)?;
        let bytes = file_handle
            .read_bytes(0..file_handle.len())
            .map_err(|err| OpenReadError::wrap_io_error(err, path.to_path_buf()))?;
        Ok(bytes.as_slice().to_vec())
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.underlying.exists(path)
    }

    fn delete(&self, _path: &Path) -> Result<(), DeleteError> {
        unimplemented!("read-only")
    }

    fn open_write(&self, _path: &Path) -> Result<WritePtr, OpenWriteError> {
        unimplemented!("read-only")
    }

    fn atomic_write(&self, _path: &Path, _data: &[u8]) -> io::Result<()> {
        unimplemented!("read-only")
    }

    fn sync_directory(&self) -> io::Result<()> {
        unimplemented!("read-only")
    }

    fn watch(
        &self,
        _watch_callback: tantivy::directory::WatchCallback,
    ) -> tantivy::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }

    fn acquire_lock(&self, _lock: &tantivy::directory::Lock) -> Result<DirectoryLock, LockError> {
        Ok(DirectoryLock::from(Box::new(|| {})))
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::ops::Range;
    use std::path::Path;
    use std::sync::Arc;

    use tantivy::directory::{FileHandle, OwnedBytes, RamDirectory, WatchCallback};
    use tantivy::{Directory, HasLen};

    use super::{CachingDirectory, CachingFileHandle};
    use crate::search::text::byte_range_cache::ByteRangeCache;
    use crate::search::text::debug_proxy_directory::DebugProxyDirectory;

    #[derive(Debug)]
    struct EmptyFileHandle;

    impl HasLen for EmptyFileHandle {
        fn len(&self) -> usize {
            4
        }
    }

    #[async_trait::async_trait]
    impl FileHandle for EmptyFileHandle {
        fn read_bytes(&self, _byte_range: Range<usize>) -> io::Result<OwnedBytes> {
            Ok(OwnedBytes::empty())
        }

        async fn read_bytes_async(&self, _byte_range: Range<usize>) -> io::Result<OwnedBytes> {
            Ok(OwnedBytes::empty())
        }
    }

    #[test]
    fn caching_directory_reuses_exact_reads() -> tantivy::Result<()> {
        let ram_directory = RamDirectory::default();
        let test_path = Path::new("test");
        ram_directory.atomic_write(test_path, b"test")?;

        let debug_proxy_directory = Arc::new(DebugProxyDirectory::wrap(ram_directory));
        let caching_directory =
            CachingDirectory::new(debug_proxy_directory.clone(), ByteRangeCache::new());
        caching_directory.atomic_read(test_path)?;
        caching_directory.atomic_read(test_path)?;

        assert_eq!(debug_proxy_directory.drain_read_operations().count(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn caching_directory_fetches_only_missing_gap() -> tantivy::Result<()> {
        let ram_directory = RamDirectory::default();
        let test_path = Path::new("test");
        ram_directory.atomic_write(test_path, b"abcdefghijklmnop")?;

        let debug_proxy_directory = Arc::new(DebugProxyDirectory::wrap(ram_directory));
        let caching_directory =
            CachingDirectory::new(debug_proxy_directory.clone(), ByteRangeCache::new());

        let file_handle = caching_directory.get_file_handle(test_path)?;
        let first = file_handle.read_bytes_async(0..8).await?;
        assert_eq!(first.as_slice(), b"abcdefgh");
        let second = file_handle.read_bytes_async(4..12).await?;
        assert_eq!(second.as_slice(), b"efghijkl");

        let operations = debug_proxy_directory
            .drain_read_operations()
            .collect::<Vec<_>>();
        assert_eq!(operations.len(), 2);
        assert_eq!(operations[0].offset, 0);
        assert_eq!(operations[0].num_bytes, 8);
        assert_eq!(operations[1].offset, 8);
        assert_eq!(operations[1].num_bytes, 4);
        Ok(())
    }

    #[tokio::test]
    async fn caching_file_handle_returns_precovered_ranges_without_fetching() -> tantivy::Result<()>
    {
        let ram_directory = RamDirectory::default();
        let test_path = Path::new("test");
        ram_directory.atomic_write(test_path, b"test")?;
        let cache = ByteRangeCache::new();
        cache.put_slice(
            test_path.to_path_buf(),
            0..4,
            tantivy::directory::OwnedBytes::new(b"test".to_vec()),
        );
        let handle = CachingFileHandle {
            path: test_path.to_path_buf(),
            cache,
            underlying: ram_directory.get_file_handle(test_path)?,
        };

        assert_eq!(
            handle
                .fill_missing_sync(0..4)?
                .expect("covered sync range")
                .as_slice(),
            b"test"
        );
        assert_eq!(
            handle
                .fill_missing_async(0..4)
                .await?
                .expect("covered async range")
                .as_slice(),
            b"test"
        );
        Ok(())
    }

    #[tokio::test]
    async fn caching_file_handle_falls_back_when_a_gap_read_cannot_populate_cache() {
        let handle = CachingFileHandle {
            path: Path::new("short").to_path_buf(),
            cache: ByteRangeCache::new(),
            underlying: Arc::new(EmptyFileHandle),
        };

        assert_eq!(handle.len(), 4);
        assert!(handle
            .read_bytes(0..4)
            .expect("fallback returns underlying bytes")
            .is_empty());
        assert!(handle
            .read_bytes_async(0..4)
            .await
            .expect("async fallback returns underlying bytes")
            .is_empty());
    }

    #[test]
    fn caching_directory_open_read_exists_watch_and_lock_contracts() -> tantivy::Result<()> {
        let ram_directory = RamDirectory::default();
        let test_path = Path::new("test");
        ram_directory.atomic_write(test_path, b"abcdefghijklmnop")?;

        let caching_directory =
            CachingDirectory::new(Arc::new(ram_directory), ByteRangeCache::new());
        assert_eq!(format!("{caching_directory:?}"), "CachingDirectory");
        assert!(caching_directory.exists(test_path)?);
        assert!(!caching_directory.exists(Path::new("missing"))?);

        let file_handle = caching_directory.get_file_handle(test_path)?;
        assert_eq!(file_handle.len(), 16);
        assert!(format!("{file_handle:?}").contains("CachingFileHandle"));
        assert_eq!(
            caching_directory
                .open_read(test_path)?
                .read_bytes()?
                .as_slice(),
            b"abcdefghijklmnop"
        );
        assert_eq!(
            caching_directory.atomic_read(test_path)?.as_slice(),
            b"abcdefghijklmnop"
        );

        let _watch = caching_directory
            .watch(WatchCallback::new(|| {}))
            .expect("watch succeeds");
        let lock = tantivy::directory::Lock {
            filepath: Path::new("cache.lock").to_path_buf(),
            is_blocking: false,
        };
        let _guard = caching_directory
            .acquire_lock(&lock)
            .expect("lock succeeds");
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = caching_directory.delete(Path::new("read-only"));
        }))
        .is_err());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = caching_directory.open_write(Path::new("read-only"));
        }))
        .is_err());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = caching_directory.atomic_write(Path::new("read-only"), b"data");
        }))
        .is_err());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = caching_directory.sync_directory();
        }))
        .is_err());
        Ok(())
    }
}
