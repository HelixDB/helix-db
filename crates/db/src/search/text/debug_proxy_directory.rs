use std::fmt;
use std::io;
use std::mem;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{DirectoryLock, FileHandle, OwnedBytes, WatchHandle, WritePtr};
use tantivy::{Directory, HasLen};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReadOperation {
    pub path: PathBuf,
    pub offset: usize,
    pub num_bytes: usize,
}

#[derive(Clone, Default)]
struct OperationBuffer(Arc<Mutex<Vec<ReadOperation>>>);

impl OperationBuffer {
    fn drain(&self) -> impl Iterator<Item = ReadOperation> + '_ {
        let mut guard = self.0.lock().expect("debug proxy buffer poisoned");
        let operations: Vec<ReadOperation> = mem::take(guard.as_mut());
        operations.into_iter()
    }

    fn push(&self, operation: ReadOperation) {
        self.0
            .lock()
            .expect("debug proxy buffer poisoned")
            .push(operation);
    }
}

pub(crate) struct DebugProxyDirectory<D: Directory> {
    underlying: Arc<D>,
    operations: OperationBuffer,
}

impl<D: Directory> Clone for DebugProxyDirectory<D> {
    fn clone(&self) -> Self {
        Self {
            underlying: Arc::clone(&self.underlying),
            operations: self.operations.clone(),
        }
    }
}

impl<D: Directory> DebugProxyDirectory<D> {
    pub(crate) fn wrap(directory: D) -> Self {
        Self {
            underlying: Arc::new(directory),
            operations: OperationBuffer::default(),
        }
    }

    pub(crate) fn drain_read_operations(&self) -> impl Iterator<Item = ReadOperation> + '_ {
        self.operations.drain()
    }

    fn record(&self, path: &Path, offset: usize, num_bytes: usize) {
        self.operations.push(ReadOperation {
            path: path.to_path_buf(),
            offset,
            num_bytes,
        });
    }
}

impl<D: Directory> fmt::Debug for DebugProxyDirectory<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DebugProxyDirectory")
    }
}

struct DebugProxyFileHandle<D: Directory> {
    directory: DebugProxyDirectory<D>,
    underlying: Arc<dyn FileHandle>,
    path: PathBuf,
}

impl<D: Directory> fmt::Debug for DebugProxyFileHandle<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DebugProxyFileHandle({})", self.path.display())
    }
}

#[async_trait]
impl<D: Directory> FileHandle for DebugProxyFileHandle<D> {
    fn read_bytes(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        let bytes = self.underlying.read_bytes(byte_range.clone())?;
        self.directory
            .record(&self.path, byte_range.start, bytes.len());
        Ok(bytes)
    }

    async fn read_bytes_async(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        let bytes = self.underlying.read_bytes_async(byte_range.clone()).await?;
        self.directory
            .record(&self.path, byte_range.start, bytes.len());
        Ok(bytes)
    }
}

impl<D: Directory> HasLen for DebugProxyFileHandle<D> {
    fn len(&self) -> usize {
        self.underlying.len()
    }
}

impl<D: Directory> Directory for DebugProxyDirectory<D> {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let underlying = self.underlying.get_file_handle(path)?;
        Ok(Arc::new(DebugProxyFileHandle {
            directory: self.clone(),
            underlying,
            path: path.to_path_buf(),
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
        let bytes = self.underlying.atomic_read(path)?;
        self.record(path, 0, bytes.len());
        Ok(bytes)
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
    use super::*;
    use tantivy::directory::{RamDirectory, WatchCallback};

    #[test]
    fn debug_proxy_records_atomic_open_and_file_handle_reads(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let ram = RamDirectory::default();
        let path = Path::new("segment.term");
        ram.atomic_write(path, b"abcdefghijkl")?;
        let proxy = DebugProxyDirectory::wrap(ram);

        assert_eq!(format!("{proxy:?}"), "DebugProxyDirectory");
        assert!(proxy.exists(path)?);
        assert_eq!(proxy.atomic_read(path)?, b"abcdefghijkl");

        let handle = proxy.get_file_handle(path)?;
        assert_eq!(handle.len(), 12);
        assert!(format!("{handle:?}").contains("DebugProxyFileHandle"));
        assert_eq!(handle.read_bytes(2..5)?.as_slice(), b"cde");

        let slice = proxy.open_read(path)?;
        assert_eq!(slice.read_bytes()?.as_slice(), b"abcdefghijkl");

        let operations = proxy.drain_read_operations().collect::<Vec<_>>();
        assert_eq!(
            operations,
            vec![
                ReadOperation {
                    path: path.to_path_buf(),
                    offset: 0,
                    num_bytes: 12,
                },
                ReadOperation {
                    path: path.to_path_buf(),
                    offset: 2,
                    num_bytes: 3,
                },
                ReadOperation {
                    path: path.to_path_buf(),
                    offset: 0,
                    num_bytes: 12,
                },
            ]
        );
        assert_eq!(proxy.drain_read_operations().count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn debug_proxy_records_async_reads_and_supports_watch_and_lock(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let ram = RamDirectory::default();
        let path = Path::new("segment.term");
        ram.atomic_write(path, b"abcdefghijkl")?;
        let proxy = DebugProxyDirectory::wrap(ram);

        let handle = proxy.get_file_handle(path)?;
        assert_eq!(handle.read_bytes_async(4..8).await?.as_slice(), b"efgh");
        let operations = proxy.drain_read_operations().collect::<Vec<_>>();
        assert_eq!(
            operations,
            vec![ReadOperation {
                path: path.to_path_buf(),
                offset: 4,
                num_bytes: 4,
            }]
        );

        let _watch = proxy
            .watch(WatchCallback::new(|| {}))
            .expect("watch succeeds");
        let lock = tantivy::directory::Lock {
            filepath: Path::new("debug.lock").to_path_buf(),
            is_blocking: false,
        };
        let _guard = proxy.acquire_lock(&lock).expect("lock succeeds");

        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = proxy.delete(Path::new("read-only"));
        }))
        .is_err());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = proxy.open_write(Path::new("read-only"));
        }))
        .is_err());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = proxy.atomic_write(Path::new("read-only"), b"data");
        }))
        .is_err());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = proxy.sync_directory();
        }))
        .is_err());
        Ok(())
    }
}
