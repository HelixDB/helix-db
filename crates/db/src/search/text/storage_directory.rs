//! Read-only Tantivy directory backed by immutable split storage.

use std::fmt::{self, Debug};
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{DirectoryLock, FileHandle, OwnedBytes, WatchHandle, WritePtr};
use tantivy::{Directory, HasLen};

use super::bundle_storage::SplitStorage;

struct StorageDirectoryFileHandle {
    storage_directory: StorageDirectory,
    path: PathBuf,
    len: usize,
}

impl HasLen for StorageDirectoryFileHandle {
    fn len(&self) -> usize {
        self.len
    }
}

impl Debug for StorageDirectoryFileHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "StorageDirectoryFileHandle({:?}, dir={:?})",
            self.path, self.storage_directory
        )
    }
}

#[async_trait]
impl FileHandle for StorageDirectoryFileHandle {
    fn read_bytes(&self, _byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        Err(unsupported_operation(&self.path))
    }

    async fn read_bytes_async(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        if byte_range.is_empty() {
            return Ok(OwnedBytes::empty());
        }
        self.storage_directory
            .get_slice(&self.path, byte_range)
            .await
    }
}

#[derive(Clone)]
pub(crate) struct StorageDirectory {
    storage: Arc<dyn SplitStorage>,
}

impl Debug for StorageDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StorageDirectory")
    }
}

impl StorageDirectory {
    pub(crate) fn new(storage: Arc<dyn SplitStorage>) -> Self {
        Self { storage }
    }

    pub(crate) async fn get_slice(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> io::Result<OwnedBytes> {
        self.storage.get_slice(path, range).await
    }
}

fn unsupported_operation(path: &Path) -> io::Error {
    io::Error::other(format!(
        "unsupported operation: storage directory only supports async reads: {}",
        path.display()
    ))
}

impl Directory for StorageDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let len = self
            .storage
            .file_num_bytes(path)
            .map_err(|err| OpenReadError::wrap_io_error(err, path.to_path_buf()))?;
        Ok(Arc::new(StorageDirectoryFileHandle {
            storage_directory: self.clone(),
            path: path.to_path_buf(),
            len,
        }))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        Err(OpenReadError::wrap_io_error(
            unsupported_operation(path),
            path.to_path_buf(),
        ))
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        match self.storage.file_num_bytes(path) {
            Ok(_) => Ok(true),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(err) => Err(OpenReadError::wrap_io_error(err, path.to_path_buf())),
        }
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

    use async_trait::async_trait;
    use tantivy::directory::{OwnedBytes, WatchCallback};
    use tantivy::Directory;

    use super::StorageDirectory;
    use crate::search::text::bundle_storage::SplitStorage;

    #[derive(Debug)]
    struct FakeStorage;

    #[async_trait]
    impl SplitStorage for FakeStorage {
        async fn get_slice(&self, path: &Path, range: Range<usize>) -> io::Result<OwnedBytes> {
            if path == Path::new("broken.term") {
                return Err(io::Error::other("broken read"));
            }
            Ok(OwnedBytes::new(vec![b'x'; range.end - range.start]))
        }

        fn file_num_bytes(&self, path: &Path) -> io::Result<usize> {
            if path == Path::new("missing.term") {
                return Err(io::Error::new(io::ErrorKind::NotFound, "missing"));
            }
            if path == Path::new("broken.term") {
                return Err(io::Error::other("broken metadata"));
            }
            Ok(16)
        }
    }

    #[test]
    fn storage_directory_rejects_sync_reads() {
        let directory = StorageDirectory::new(Arc::new(FakeStorage));
        let file_handle = directory
            .get_file_handle(Path::new("segment.term"))
            .expect("file handle");
        let err = file_handle
            .read_bytes(0..4)
            .expect_err("sync read should fail");
        assert!(err.to_string().contains("only supports async reads"));
    }

    #[tokio::test]
    async fn storage_directory_serves_async_reads_and_reports_existence() {
        let directory = StorageDirectory::new(Arc::new(FakeStorage));
        let file_handle = directory
            .get_file_handle(Path::new("segment.term"))
            .expect("file handle");
        assert_eq!(file_handle.len(), 16);
        assert!(format!("{file_handle:?}").contains("StorageDirectoryFileHandle"));
        assert_eq!(
            file_handle
                .read_bytes_async(2..6)
                .await
                .expect("async read")
                .as_slice(),
            b"xxxx"
        );
        assert!(file_handle
            .read_bytes_async(6..6)
            .await
            .expect("empty async read")
            .is_empty());
        assert!(directory
            .exists(Path::new("segment.term"))
            .expect("exists succeeds"));
        assert!(!directory
            .exists(Path::new("missing.term"))
            .expect("missing exists succeeds"));
        assert!(directory.exists(Path::new("broken.term")).is_err());
        assert!(directory.atomic_read(Path::new("segment.term")).is_err());
        assert!(format!("{directory:?}").contains("StorageDirectory"));
        let _watch = directory
            .watch(WatchCallback::new(|| {}))
            .expect("watch handle");
        let lock = tantivy::directory::Lock {
            filepath: Path::new("storage-test.lock").to_path_buf(),
            is_blocking: false,
        };
        let _lock = directory.acquire_lock(&lock).expect("lock handle");

        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = directory.delete(Path::new("read-only"));
        }))
        .is_err());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = directory.open_write(Path::new("read-only"));
        }))
        .is_err());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = directory.atomic_write(Path::new("read-only"), b"data");
        }))
        .is_err());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = directory.sync_directory();
        }))
        .is_err());
    }
}
