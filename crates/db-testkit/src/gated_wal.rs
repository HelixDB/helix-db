//! An object store whose WAL uploads a test holds, fails, and counts.
//!
//! Every other path passes through, so a writer opens normally and a test
//! decides exactly when, and whether, a commit reaches durable storage.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use tokio::sync::watch;

/// In-memory object store with test control over WAL uploads.
///
/// ```
/// use helix_db_testkit::gated_wal::GatedWalStore;
///
/// let store = GatedWalStore::default();
/// store.hold();
/// store.release();
/// assert_eq!(store.uploads(), 0);
/// ```
#[derive(Debug)]
pub struct GatedWalStore {
    inner: InMemory,
    held: watch::Sender<bool>,
    failing: AtomicBool,
    uploads: AtomicUsize,
}

impl Default for GatedWalStore {
    fn default() -> Self {
        Self {
            inner: InMemory::new(),
            held: watch::Sender::new(false),
            failing: AtomicBool::new(false),
            uploads: AtomicUsize::new(0),
        }
    }
}

impl GatedWalStore {
    /// Makes WAL uploads wait until [`Self::release`] or [`Self::fail`].
    pub fn hold(&self) {
        self.held.send_replace(true);
    }

    /// Lets held and later WAL uploads proceed.
    pub fn release(&self) {
        self.held.send_replace(false);
    }

    /// Fails held and later WAL uploads, which closes the writer as a crash
    /// would stop it: nothing it has not uploaded reaches storage.
    pub fn fail(&self) {
        self.failing.store(true, Ordering::SeqCst);
        self.release();
    }

    /// Restores WAL uploads for the next writer.
    pub fn recover(&self) {
        self.failing.store(false, Ordering::SeqCst);
    }

    /// How many WAL uploads have reached storage.
    pub fn uploads(&self) -> usize {
        self.uploads.load(Ordering::SeqCst)
    }
}

impl fmt::Display for GatedWalStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("gated-wal-memory")
    }
}

#[async_trait::async_trait]
impl ObjectStore for GatedWalStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        if location.as_ref().contains("/wal/") {
            self.held
                .subscribe()
                .wait_for(|held| !held)
                .await
                .expect("the store owns the sender");
            if self.failing.load(Ordering::SeqCst) {
                // SlateDB retries transient errors, but not this one.
                return Err(object_store::Error::NotImplemented {
                    operation: "put".to_string(),
                    implementer: self.to_string(),
                });
            }
            self.uploads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
