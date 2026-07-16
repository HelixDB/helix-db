use std::fmt;
use std::io;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use slatedb::object_store::path::Path as ObjectStorePath;
use slatedb::object_store::{ObjectStore, ObjectStoreExt};
use tantivy::directory::OwnedBytes;

use super::split::TextSplitFooterData;

#[async_trait]
pub(crate) trait SplitStorage: fmt::Debug + Send + Sync + 'static {
    async fn get_slice(&self, path: &Path, range: Range<usize>) -> io::Result<OwnedBytes>;

    async fn get_all(&self, path: &Path) -> io::Result<OwnedBytes> {
        let len = self.file_num_bytes(path)?;
        self.get_slice(path, 0..len).await
    }

    fn file_num_bytes(&self, path: &Path) -> io::Result<usize>;
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectStoreSplitBundleStorage")
            .field("split_path", &self.split_path)
            .finish()
    }
}

#[async_trait]
impl SplitStorage for ObjectStoreSplitBundleStorage {
    async fn get_slice(&self, path: &Path, range: Range<usize>) -> io::Result<OwnedBytes> {
        if range.is_empty() {
            return Ok(OwnedBytes::empty());
        }
        let split_range = self.split_range_for_path(path, range)?;
        let bytes = self
            .store
            .get_range(&self.split_path, split_range.clone())
            .await
            .map_err(|err| {
                io::Error::other(format!(
                    "failed to read split range {}..{} for '{}': {err}",
                    split_range.start,
                    split_range.end,
                    path.display()
                ))
            })?;
        Ok(OwnedBytes::new(bytes.to_vec()))
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
    use super::*;
    use bytes::Bytes;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::PutPayload;
    use std::collections::BTreeMap;

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
                .expect("read slice")
                .as_slice(),
            b"bcd"
        );
        assert_eq!(
            storage
                .get_all(Path::new("segment.term"))
                .await
                .expect("read all")
                .as_slice(),
            b"abcdef"
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
        assert!(format!("{storage:?}").contains("ObjectStoreSplitBundleStorage"));
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
