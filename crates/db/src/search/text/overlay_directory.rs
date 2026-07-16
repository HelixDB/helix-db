use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{DirectoryLock, FileHandle, WatchHandle, WritePtr};
use tantivy::Directory;

#[derive(Clone, Debug)]
pub(crate) struct OverlayDirectory {
    directories: Arc<Vec<Box<dyn Directory>>>,
}

impl OverlayDirectory {
    pub(crate) fn union_of(directories: Vec<Box<dyn Directory>>) -> Self {
        Self {
            directories: Arc::new(directories),
        }
    }

    fn find_directory_for_path(&self, path: &Path) -> Result<&dyn Directory, OpenReadError> {
        for directory in self.directories.iter() {
            if directory.exists(path)? {
                return Ok(directory.as_ref());
            }
        }
        Err(OpenReadError::FileDoesNotExist(path.to_path_buf()))
    }
}

fn convert_open_to_delete_error(open_err: OpenReadError) -> DeleteError {
    match open_err {
        OpenReadError::FileDoesNotExist(path) => DeleteError::FileDoesNotExist(path),
        OpenReadError::IoError { io_error, filepath } => {
            DeleteError::IoError { io_error, filepath }
        }
        err @ OpenReadError::IncompatibleIndex(_) => DeleteError::IoError {
            io_error: Arc::new(io::Error::new(io::ErrorKind::Unsupported, err)),
            filepath: PathBuf::from("/"),
        },
    }
}

impl Directory for OverlayDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let directory = self.find_directory_for_path(path)?;
        directory.get_file_handle(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        match self.find_directory_for_path(path) {
            Ok(_) => Ok(true),
            Err(OpenReadError::FileDoesNotExist(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let directory = self.find_directory_for_path(path)?;
        directory.atomic_read(path)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        self.directories[0].open_write(path)
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        match self.directories[0].exists(path) {
            Ok(true) => self.directories[0].delete(path),
            Ok(false) => Err(DeleteError::FileDoesNotExist(path.to_path_buf())),
            Err(err) => Err(convert_open_to_delete_error(err)),
        }
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        self.directories[0].atomic_write(path, data)
    }

    fn watch(&self, callback: tantivy::directory::WatchCallback) -> tantivy::Result<WatchHandle> {
        self.directories[0].watch(callback)
    }

    fn sync_directory(&self) -> io::Result<()> {
        self.directories[0].sync_directory()
    }

    fn acquire_lock(&self, lock: &tantivy::directory::Lock) -> Result<DirectoryLock, LockError> {
        self.directories[0].acquire_lock(lock)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::directory::error::Incompatibility;
    use tantivy::directory::{RamDirectory, WatchCallback};

    #[test]
    fn overlay_reads_from_first_directory_that_contains_path(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let first = RamDirectory::default();
        let second = RamDirectory::default();
        first.atomic_write(Path::new("first"), b"one")?;
        second.atomic_write(Path::new("shared"), b"two")?;
        second.atomic_write(Path::new("second"), b"three")?;

        let overlay = OverlayDirectory::union_of(vec![Box::new(first), Box::new(second)]);

        assert!(overlay.exists(Path::new("first"))?);
        assert!(overlay.exists(Path::new("shared"))?);
        assert!(!overlay.exists(Path::new("missing"))?);
        assert_eq!(overlay.atomic_read(Path::new("first"))?, b"one");
        assert_eq!(overlay.atomic_read(Path::new("second"))?, b"three");
        let handle = overlay.get_file_handle(Path::new("shared"))?;
        assert_eq!(handle.read_bytes(0..3)?.as_slice(), b"two");
        assert!(matches!(
            overlay.atomic_read(Path::new("missing")),
            Err(OpenReadError::FileDoesNotExist(_))
        ));
        assert!(format!("{overlay:?}").contains("OverlayDirectory"));
        Ok(())
    }

    #[test]
    fn overlay_write_delete_watch_and_lock_delegate_to_primary_directory(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let first = RamDirectory::default();
        let second = RamDirectory::default();
        second.atomic_write(Path::new("secondary-only"), b"two")?;
        let overlay = OverlayDirectory::union_of(vec![Box::new(first), Box::new(second)]);

        overlay.atomic_write(Path::new("primary"), b"one")?;
        overlay.sync_directory()?;
        assert_eq!(overlay.atomic_read(Path::new("primary"))?, b"one");
        overlay.delete(Path::new("primary"))?;
        assert!(!overlay.exists(Path::new("primary"))?);
        assert!(matches!(
            overlay.delete(Path::new("secondary-only")),
            Err(DeleteError::FileDoesNotExist(_))
        ));

        let _watch = overlay
            .watch(WatchCallback::new(|| {}))
            .expect("watch succeeds");
        let lock = tantivy::directory::Lock {
            filepath: Path::new("overlay.lock").to_path_buf(),
            is_blocking: false,
        };
        let _guard = overlay.acquire_lock(&lock).expect("lock succeeds");
        let _writer = overlay
            .open_write(Path::new("writer-target"))
            .expect("open write delegates");
        Ok(())
    }

    #[test]
    fn open_read_errors_convert_to_delete_error_contracts() {
        assert!(matches!(
            convert_open_to_delete_error(OpenReadError::FileDoesNotExist(PathBuf::from("missing"))),
            DeleteError::FileDoesNotExist(path) if path == Path::new("missing")
        ));
        assert!(matches!(
            convert_open_to_delete_error(OpenReadError::wrap_io_error(
                io::Error::other("read failed"),
                PathBuf::from("broken"),
            )),
            DeleteError::IoError { filepath, .. } if filepath == Path::new("broken")
        ));
        assert!(matches!(
            convert_open_to_delete_error(OpenReadError::IncompatibleIndex(
                Incompatibility::CompressionMismatch {
                    library_compression_format: "lz4".to_string(),
                    index_compression_format: "zstd".to_string(),
                },
            )),
            DeleteError::IoError { filepath, .. } if filepath == Path::new("/")
        ));
    }
}
