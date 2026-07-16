use std::fmt;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use futures::future::{BoxFuture, FutureExt, Shared};
use tantivy::directory::OwnedBytes;

use super::bundle_storage::SplitStorage;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
enum DebouncedRequest {
    Slice { path: PathBuf, range: Range<usize> },
    All { path: PathBuf },
}

type SharedReadFuture = Shared<BoxFuture<'static, Result<Vec<u8>, Arc<str>>>>;

#[derive(Clone)]
pub(crate) struct DebouncedStorage {
    inner: Arc<dyn SplitStorage>,
    inflight: Arc<DashMap<DebouncedRequest, SharedReadFuture>>,
}

impl DebouncedStorage {
    pub(crate) fn new(inner: Arc<dyn SplitStorage>) -> Self {
        Self {
            inner,
            inflight: Arc::new(DashMap::new()),
        }
    }

    async fn execute(&self, request: DebouncedRequest) -> io::Result<OwnedBytes> {
        let shared = if let Some(existing) = self.inflight.get(&request) {
            existing.clone()
        } else {
            let future = match &request {
                DebouncedRequest::Slice { path, range } => {
                    let inner = Arc::clone(&self.inner);
                    let path = path.clone();
                    let range = range.clone();
                    async move {
                        inner
                            .get_slice(&path, range)
                            .await
                            .map(|bytes| bytes.as_slice().to_vec())
                            .map_err(|err| Arc::<str>::from(err.to_string()))
                    }
                    .boxed()
                    .shared()
                }
                DebouncedRequest::All { path } => {
                    let inner = Arc::clone(&self.inner);
                    let path = path.clone();
                    async move {
                        inner
                            .get_all(&path)
                            .await
                            .map(|bytes| bytes.as_slice().to_vec())
                            .map_err(|err| Arc::<str>::from(err.to_string()))
                    }
                    .boxed()
                    .shared()
                }
            };
            let inserted = self
                .inflight
                .entry(request.clone())
                .or_insert_with(|| future.clone());
            inserted.clone()
        };

        let result = shared.await;
        self.inflight.remove(&request);
        result
            .map(OwnedBytes::new)
            .map_err(|err| io::Error::other(err.to_string()))
    }
}

impl fmt::Debug for DebouncedStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DebouncedStorage").finish()
    }
}

#[async_trait]
impl SplitStorage for DebouncedStorage {
    async fn get_slice(&self, path: &Path, range: Range<usize>) -> io::Result<OwnedBytes> {
        self.execute(DebouncedRequest::Slice {
            path: path.to_path_buf(),
            range,
        })
        .await
    }

    async fn get_all(&self, path: &Path) -> io::Result<OwnedBytes> {
        self.execute(DebouncedRequest::All {
            path: path.to_path_buf(),
        })
        .await
    }

    fn file_num_bytes(&self, path: &Path) -> io::Result<usize> {
        self.inner.file_num_bytes(path)
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::ops::Range;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use tantivy::directory::OwnedBytes;

    use super::DebouncedStorage;
    use crate::search::text::bundle_storage::SplitStorage;

    #[derive(Debug)]
    struct CountingStorage {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl SplitStorage for CountingStorage {
        async fn get_slice(&self, path: &Path, range: Range<usize>) -> io::Result<OwnedBytes> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(25)).await;
            if path == Path::new("fail.term") {
                return Err(io::Error::other("read failed"));
            }
            Ok(OwnedBytes::new(vec![b'x'; range.end - range.start]))
        }

        fn file_num_bytes(&self, _path: &Path) -> io::Result<usize> {
            Ok(16)
        }
    }

    #[tokio::test]
    async fn identical_concurrent_requests_are_debounced() {
        let storage = Arc::new(CountingStorage {
            calls: AtomicUsize::new(0),
        });
        let debounced = DebouncedStorage::new(storage.clone());

        let (left, right) = tokio::join!(
            debounced.get_slice(Path::new("segment.term"), 0..4),
            debounced.get_slice(Path::new("segment.term"), 0..4),
        );

        assert_eq!(left.expect("left").as_slice(), b"xxxx");
        assert_eq!(right.expect("right").as_slice(), b"xxxx");
        assert_eq!(storage.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn identical_get_all_requests_are_debounced_and_delegate_metadata() {
        let storage = Arc::new(CountingStorage {
            calls: AtomicUsize::new(0),
        });
        let debounced = DebouncedStorage::new(storage.clone());

        assert_eq!(format!("{debounced:?}"), "DebouncedStorage");
        assert_eq!(
            debounced
                .file_num_bytes(Path::new("segment.term"))
                .expect("file length"),
            16
        );
        let (left, right) = tokio::join!(
            debounced.get_all(Path::new("segment.term")),
            debounced.get_all(Path::new("segment.term")),
        );

        assert_eq!(left.expect("left").as_slice(), [b'x'; 16].as_slice());
        assert_eq!(right.expect("right").as_slice(), [b'x'; 16].as_slice());
        assert_eq!(storage.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn failed_requests_are_removed_from_inflight_for_retries() {
        let storage = Arc::new(CountingStorage {
            calls: AtomicUsize::new(0),
        });
        let debounced = DebouncedStorage::new(storage.clone());

        let error = debounced
            .get_slice(Path::new("fail.term"), 0..4)
            .await
            .expect_err("first read fails");
        assert!(error.to_string().contains("read failed"));
        assert_eq!(storage.calls.load(Ordering::SeqCst), 1);

        let error = debounced
            .get_slice(Path::new("fail.term"), 0..4)
            .await
            .expect_err("retry read fails");
        assert!(error.to_string().contains("read failed"));
        assert_eq!(storage.calls.load(Ordering::SeqCst), 2);
    }
}
