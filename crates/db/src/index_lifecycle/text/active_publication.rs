//! Direct publication for request-owned Active text mutations.
//!
//! Immutable content-addressed split bytes are uploaded before the graph
//! transaction stages any manifest reference. SlateDB remains the only
//! visibility authority: an upload failure aborts the mutation, while a later
//! transaction failure can leave only an unreachable immutable blob.

use std::sync::Arc;

use futures::{stream, StreamExt, TryStreamExt};
use slatedb::object_store::ObjectStore;
use tokio::sync::Semaphore;

use crate::config::ActiveTextMutationLimits;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::work;

/// An epoch whose every immutable payload reached its content-addressed path.
///
/// Construction is private to publication, making an unpublished epoch
/// impossible to pass to the database staging boundary.
pub(crate) struct PublishedActiveTextEpoch {
    prepared: super::active_batch::PreparedActiveTextEpoch,
}

impl PublishedActiveTextEpoch {
    pub(super) const fn prepared(&self) -> &super::active_batch::PreparedActiveTextEpoch {
        &self.prepared
    }
}

/// Uploads the optional split from every live epoch destination.
pub(crate) async fn publish_active_text_epoch(
    object_store: &Arc<dyn ObjectStore>,
    database: &str,
    mut prepared: super::active_batch::PreparedActiveTextEpoch,
    limits: ActiveTextMutationLimits,
) -> Result<PublishedActiveTextEpoch> {
    let upload_count = prepared.upload_count();
    let published = publish_uploads(
        object_store,
        database,
        prepared.take_uploads(),
        limits.max_input_bytes().get(),
    )
    .await?;
    if published.len() != upload_count {
        return Err(HelixDbError::InvariantViolation(
            "Active text epoch publication returned an incomplete upload set".to_string(),
        ));
    }
    Ok(PublishedActiveTextEpoch { prepared })
}

async fn publish_uploads(
    object_store: &Arc<dyn ObjectStore>,
    database: &str,
    uploads: impl IntoIterator<Item = (bytes::Bytes, work::SplitRef)>,
    byte_budget: u64,
) -> Result<Vec<work::SplitRef>> {
    let uploads = uploads.into_iter().enumerate().collect::<Vec<_>>();
    let upload_concurrency = super::active_text_destination_concurrency(uploads.len());
    let budget_permits =
        usize::try_from(byte_budget.min(u64::from(u32::MAX))).expect("u32 byte budgets fit usize");
    let budget = Arc::new(Semaphore::new(budget_permits));
    let mut published = stream::iter(uploads)
        .map(|(ordinal, (payload, split))| {
            let object_store = Arc::clone(object_store);
            let budget = Arc::clone(&budget);
            async move {
                let reservation = u64::try_from(payload.len())
                    .unwrap_or(u64::MAX)
                    .min(byte_budget)
                    .min(u64::from(u32::MAX));
                let _permit = budget
                    .acquire_many_owned(
                        u32::try_from(reservation).expect("reservation is clamped to u32"),
                    )
                    .await
                    .map_err(|_| {
                        HelixDbError::InvariantViolation(
                            "Active text upload byte budget closed during publication".to_string(),
                        )
                    })?;
                crate::search::text::upload_prepared_blob(&object_store, database, payload, split)
                    .await?;
                Ok::<_, HelixDbError>((ordinal, split))
            }
        })
        .buffer_unordered(upload_concurrency.get())
        .try_collect::<Vec<_>>()
        .await?;
    published.sort_unstable_by_key(|(ordinal, _)| *ordinal);
    Ok(published
        .into_iter()
        .map(|(_, split)| split)
        .collect::<Vec<_>>())
}

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::sync::Mutex;

    use futures::stream::BoxStream;
    use sha2::{Digest, Sha256};
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::path::Path;
    use slatedb::object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
    };
    use tokio::sync::Notify;

    use super::*;
    const BUDGETED_UPLOADS: usize = 3;

    #[derive(Debug, Default)]
    struct PutState {
        active: usize,
        entered: usize,
        peak: usize,
        released: bool,
    }

    #[derive(Debug, Default)]
    struct BlockingPutStore {
        inner: InMemory,
        state: Mutex<PutState>,
        changed: Notify,
    }

    impl BlockingPutStore {
        async fn wait_until_entered(&self, expected: usize) {
            loop {
                let notified = self.changed.notified();
                if self.state.lock().expect("put state is healthy").entered >= expected {
                    return;
                }
                notified.await;
            }
        }

        fn release(&self) {
            self.state.lock().expect("put state is healthy").released = true;
            self.changed.notify_waiters();
        }

        fn peak(&self) -> usize {
            self.state.lock().expect("put state is healthy").peak
        }
    }

    struct ActivePutGuard<'a> {
        state: &'a Mutex<PutState>,
    }

    impl Drop for ActivePutGuard<'_> {
        fn drop(&mut self) {
            self.state.lock().expect("put state is healthy").active -= 1;
        }
    }

    impl fmt::Display for BlockingPutStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("blocking-put-memory")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for BlockingPutStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            let _guard = if location.to_string().contains("/fts/blobs/") {
                {
                    let mut state = self.state.lock().expect("put state is healthy");
                    state.active += 1;
                    state.entered += 1;
                    state.peak = state.peak.max(state.active);
                }
                self.changed.notify_waiters();
                loop {
                    let notified = self.changed.notified();
                    if self.state.lock().expect("put state is healthy").released {
                        break;
                    }
                    notified.await;
                }
                Some(ActivePutGuard { state: &self.state })
            } else {
                None
            };
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> ObjectStoreResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, ObjectStoreResult<Path>>,
        ) -> BoxStream<'static, ObjectStoreResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn upload(ordinal: usize) -> (bytes::Bytes, work::SplitRef) {
        let payload = bytes::Bytes::from(format!("active text upload {ordinal}"));
        let hash = Sha256::digest(&payload).into();
        let size = u64::try_from(payload.len()).expect("test payload length fits u64");
        let split = work::SplitRef::try_new(
            work::BlobRef::new(hash, size),
            0,
            0,
            0,
            size,
            work::SplitPruning::Unavailable,
        )
        .expect("test split metadata is valid");
        (payload, split)
    }

    #[tokio::test]
    async fn uploads_are_byte_bounded_and_returned_in_preparation_order() {
        let store = Arc::new(BlockingPutStore::default());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let uploads = (0..BUDGETED_UPLOADS + 2).map(upload).collect::<Vec<_>>();
        let expected = uploads.iter().map(|(_, split)| *split).collect::<Vec<_>>();
        let byte_budget = uploads
            .iter()
            .take(BUDGETED_UPLOADS)
            .map(|(payload, _)| u64::try_from(payload.len()).unwrap())
            .sum();

        let publisher = tokio::spawn(async move {
            publish_uploads(
                &object_store,
                "bounded-active-publication",
                uploads,
                byte_budget,
            )
            .await
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            store.wait_until_entered(BUDGETED_UPLOADS),
        )
        .await
        .expect("the full bounded upload window starts");
        assert_eq!(store.peak(), BUDGETED_UPLOADS);
        store.release();

        assert_eq!(
            publisher
                .await
                .expect("publication task joins")
                .expect("publication succeeds"),
            expected
        );
        assert_eq!(store.peak(), BUDGETED_UPLOADS);
    }

    #[tokio::test]
    async fn tiny_uploads_are_count_bounded_and_returned_in_preparation_order() {
        let store = Arc::new(BlockingPutStore::default());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let upload_count = super::super::ACTIVE_TEXT_DESTINATION_CONCURRENCY + 3;
        let uploads = (0..upload_count).map(upload).collect::<Vec<_>>();
        let expected = uploads.iter().map(|(_, split)| *split).collect::<Vec<_>>();

        let publisher = tokio::spawn(async move {
            publish_uploads(
                &object_store,
                "count-bounded-active-publication",
                uploads,
                u64::from(u32::MAX),
            )
            .await
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            store.wait_until_entered(super::super::ACTIVE_TEXT_DESTINATION_CONCURRENCY),
        )
        .await
        .expect("the full count-bounded upload window starts");
        assert_eq!(
            store.peak(),
            super::super::ACTIVE_TEXT_DESTINATION_CONCURRENCY
        );
        store.release();

        assert_eq!(
            publisher
                .await
                .expect("publication task joins")
                .expect("publication succeeds"),
            expected
        );
        assert_eq!(
            store.peak(),
            super::super::ACTIVE_TEXT_DESTINATION_CONCURRENCY
        );
    }

    #[test]
    fn destination_concurrency_is_positive_and_hard_capped() {
        assert_eq!(
            super::super::active_text_destination_concurrency(0).get(),
            1
        );
        assert_eq!(
            super::super::active_text_destination_concurrency(1).get(),
            1
        );
        assert_eq!(
            super::super::active_text_destination_concurrency(usize::MAX).get(),
            super::super::ACTIVE_TEXT_DESTINATION_CONCURRENCY
        );
    }
}
