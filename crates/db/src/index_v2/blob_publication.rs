//! Coordinated text-blob publication and deletion fences.
//!
//! Text lifecycle code must never issue an untracked object-store put or
//! delete. [`BlobPublicationCoordinator`] is the object-safe boundary that
//! makes publication admission, reference validation, delete fencing, and
//! terminal permit release explicit. The process-local implementation owns an
//! admitted put in a background task, so dropping a request future cannot make
//! a late write invisible to deletion quiescence.
//!
//! The authority values in this module have private constructors. Repository
//! transitions create them only after the corresponding durable proof has
//! committed; callers cannot replace that proof with a boolean.

use std::collections::HashMap;
use std::num::NonZeroU64;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use slatedb::object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use tokio::sync::Notify;

use super::{BlobGcRunId, BlobPublicationPermitId, BlobRef, TextUploadIntentId, WriterEpoch};

pub mod conformance;

/// Contract failure at the publication-coordinator boundary.
#[derive(Debug, thiserror::Error)]
pub enum BlobPublicationError {
    /// A duration contract cannot retain a zero interval.
    #[error("blob coordinator duration must be positive")]
    ZeroDuration,
    /// The permit ID is not owned by this coordinator.
    #[error("blob publication permit is unknown")]
    UnknownPermit,
    /// The permit expired before publication admission.
    #[error("blob publication permit expired before use")]
    PermitExpired,
    /// The release authority does not name this exact permit or terminal phase.
    #[error("blob publication release authority mismatch")]
    ReleaseAuthorityMismatch,
    /// Payload bytes disagree with their content-addressed identity.
    #[error("blob publication payload does not match its declared reference")]
    PayloadMismatch,
    /// A different GC run has closed this blob.
    #[error("blob publication is closed by a delete fence")]
    DeleteFenceClosed,
    /// The supplied runtime fence is not the currently held exact fence.
    #[error("blob delete fence is stale or belongs to another run")]
    InvalidDeleteFence,
    /// Publication/reference activity has not drained behind the delete fence.
    #[error("blob delete fence is not quiescent")]
    DeleteFenceNotQuiescent,
    /// Reference validation could not find the content-addressed object.
    #[error("blob reference target is absent")]
    ReferenceAbsent,
    /// Stored bytes disagree with the declared content hash or size.
    #[error("blob reference target does not match its declared metadata")]
    ReferenceMismatch,
    /// The coordinator owns an admitted request whose backend result is not
    /// definitive. Its public status remains `InFlight`.
    #[error("blob publication outcome is ambiguous: {0}")]
    PublicationOutcomeAmbiguous(String),
    /// Object-store operation failed outside an admitted publication.
    #[error("blob coordinator object-store operation failed: {0}")]
    ObjectStore(#[from] slatedb::object_store::Error),
    /// The configured coordinator backend is temporarily unavailable.
    #[error("blob publication coordinator is unavailable: {0}")]
    CoordinatorUnavailable(String),
}

/// Validated positive coordinator duration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlobOperationDuration(NonZeroU64);

impl BlobOperationDuration {
    /// Constructs a positive millisecond duration.
    pub const fn from_millis(millis: NonZeroU64) -> Self {
        Self(millis)
    }

    /// Validates a standard-library duration at millisecond precision.
    pub fn try_from_duration(duration: Duration) -> Result<Self, BlobPublicationError> {
        let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        let Some(millis) = NonZeroU64::new(millis) else {
            return Err(BlobPublicationError::ZeroDuration);
        };
        Ok(Self(millis))
    }

    /// Returns the coordinator interval as a standard duration.
    pub const fn get(self) -> Duration {
        Duration::from_millis(self.0.get())
    }
}

/// Validated publication timing defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobPublicationTiming {
    reservation_ttl: BlobOperationDuration,
    publish_timeout: BlobOperationDuration,
    safety_margin: BlobOperationDuration,
}

impl BlobPublicationTiming {
    /// Constructs a complete positive timing policy.
    pub const fn new(
        reservation_ttl: BlobOperationDuration,
        publish_timeout: BlobOperationDuration,
        safety_margin: BlobOperationDuration,
    ) -> Self {
        Self {
            reservation_ttl,
            publish_timeout,
            safety_margin,
        }
    }

    /// Returns the unadmitted reservation lifetime.
    pub const fn reservation_ttl(self) -> BlobOperationDuration {
        self.reservation_ttl
    }

    /// Returns the recommended client wait bound.
    pub const fn publish_timeout(self) -> BlobOperationDuration {
        self.publish_timeout
    }

    /// Returns the coordinator I/O safety margin.
    pub const fn safety_margin(self) -> BlobOperationDuration {
        self.safety_margin
    }
}

impl Default for BlobPublicationTiming {
    fn default() -> Self {
        Self::new(
            BlobOperationDuration::from_millis(
                NonZeroU64::new(30_000).expect("default reservation TTL is positive"),
            ),
            BlobOperationDuration::from_millis(
                NonZeroU64::new(5 * 60_000).expect("default publish timeout is positive"),
            ),
            BlobOperationDuration::from_millis(
                NonZeroU64::new(5_000).expect("default safety margin is positive"),
            ),
        )
    }
}

/// Exact single-use publication reservation persisted by one upload intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlobPublicationPermit {
    id: BlobPublicationPermitId,
}

impl BlobPublicationPermit {
    /// Reconstructs the opaque permit returned by a coordinator adapter.
    ///
    /// Constructing this value does not grant publication authority: every
    /// coordinator operation must still find the matching backend-owned
    /// reservation. Durable upload intents retain this ID so recovery never
    /// needs to persist the coordinator's reservation identity fields.
    pub const fn from_id(id: BlobPublicationPermitId) -> Self {
        Self { id }
    }

    /// Returns the persisted permit ID.
    pub const fn id(self) -> BlobPublicationPermitId {
        self.id
    }
}

/// Metadata verified by a definitive successful publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VerifiedBlobMetadata {
    blob: BlobRef,
}

impl VerifiedBlobMetadata {
    /// Constructs metadata after an adapter has verified publication.
    pub const fn new(blob: BlobRef) -> Self {
        Self { blob }
    }

    /// Returns the verified hash and size.
    pub const fn blob(self) -> BlobRef {
        self.blob
    }
}

/// Closed publication status retained until exact permit release.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BlobPublicationStatus {
    /// Reserved but not admitted to object I/O.
    Reserved,
    /// Admitted object I/O is coordinator-owned and not definitive.
    InFlight,
    /// Object publication definitively succeeded with matching metadata.
    Succeeded(VerifiedBlobMetadata),
    /// Publication definitively did not create the declared object.
    DefinitivelyFailed,
    /// An unused reservation expired under coordinator time.
    ExpiredUnused,
}

/// Exact `(blob, GC run)` identity reconstructible from a retained member.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlobDeleteFenceKey {
    blob: BlobRef,
    run_id: BlobGcRunId,
}

impl BlobDeleteFenceKey {
    /// Constructs an exact generation/upload reclaim fence identity.
    pub const fn new(blob: BlobRef, run_id: BlobGcRunId) -> Self {
        Self { blob, run_id }
    }

    /// Returns the closed blob identity.
    pub const fn blob(self) -> BlobRef {
        self.blob
    }

    /// Returns the owning GC run.
    pub const fn run_id(self) -> BlobGcRunId {
        self.run_id
    }
}

/// Narrow runtime authority returned by `begin_delete`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlobDeleteFence {
    key: BlobDeleteFenceKey,
}

impl BlobDeleteFence {
    /// Constructs the narrow guard returned by a coordinator adapter.
    ///
    /// The value is not sufficient authority by itself: quiescence and delete
    /// operations must revalidate it against backend-owned fence state.
    pub const fn new(key: BlobDeleteFenceKey) -> Self {
        Self { key }
    }

    /// Returns the durable identity represented by this runtime fence.
    pub const fn key(self) -> BlobDeleteFenceKey {
        self.key
    }
}

/// Closed delete-fence acquisition result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BeginBlobDelete {
    /// This call closed the blob for the requested run.
    Acquired(BlobDeleteFence),
    /// The same run already owns the closed blob.
    AlreadyHeldSameRun(BlobDeleteFence),
    /// Another run owns the closed blob; no progress is authorized.
    BusyOtherRun,
}

/// Definitive coordinator-mediated object disposition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BlobDeleteOutcome {
    /// The exact object was deleted or was already absent.
    DeletedOrAbsent,
}

/// Exact object observation made while one retained delete fence is closed.
///
/// Ordinary reference validation is deliberately unavailable behind a delete
/// fence because it would create new live ownership. Cleanup instead uses this
/// read-only observation to normalize a terminal upload without reopening the
/// blob to publication or reference traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FencedBlobObservation {
    /// The content-addressed object exists with the declared hash and size.
    Exact,
    /// The content-addressed object is absent.
    Absent,
    /// Bytes exist at the content address but fail hash or size validation.
    Mismatch,
}

/// Durable transition that authorizes removal of a terminal permit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobPermitReleaseReason {
    /// The exact upload intent reached durable `ReferenceCommitted` state.
    ReferenceCommitted,
    /// The intent and coordinator proved publication definitively absent.
    DefinitiveNonPublication,
    /// The exact delete fence selected and completed a safe disposition.
    FencedDisposition,
}

/// Non-forgeable proof authorizing terminal permit release.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobPermitReleaseAuthority {
    permit_id: BlobPublicationPermitId,
    reason: BlobPermitReleaseReason,
}

impl BlobPermitReleaseAuthority {
    pub(crate) const fn reference_committed(permit_id: BlobPublicationPermitId) -> Self {
        Self {
            permit_id,
            reason: BlobPermitReleaseReason::ReferenceCommitted,
        }
    }

    pub(crate) const fn definitive_non_publication(permit_id: BlobPublicationPermitId) -> Self {
        Self {
            permit_id,
            reason: BlobPermitReleaseReason::DefinitiveNonPublication,
        }
    }

    pub(crate) const fn fenced_disposition(permit_id: BlobPublicationPermitId) -> Self {
        Self {
            permit_id,
            reason: BlobPermitReleaseReason::FencedDisposition,
        }
    }

    /// Returns the exact permit named by the durable proof.
    pub const fn permit_id(self) -> BlobPublicationPermitId {
        self.permit_id
    }

    /// Returns the closed repository transition represented by the proof.
    pub const fn reason(self) -> BlobPermitReleaseReason {
        self.reason
    }
}

/// Non-forgeable proof that DB cleanup for one retained member committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CleanupCommittedAuthority {
    key: BlobDeleteFenceKey,
}

impl CleanupCommittedAuthority {
    pub(crate) const fn new(key: BlobDeleteFenceKey) -> Self {
        Self { key }
    }

    /// Returns the exact retained member/run whose cleanup committed.
    pub const fn key(self) -> BlobDeleteFenceKey {
        self.key
    }
}

/// Narrow reference guard held across the first-reference transaction.
pub struct BlobReferenceGuard {
    blob: BlobRef,
    release: Option<Box<dyn FnOnce() + Send + Sync + 'static>>,
}

impl BlobReferenceGuard {
    /// Constructs a guard whose release callback is owned by an adapter.
    ///
    /// This is intended for implementations of
    /// [`BlobPublicationCoordinator::validate_reference`]. Constructing a
    /// guard does not authorize a reference transaction; only a guard returned
    /// by the configured coordinator carries that runtime meaning.
    pub fn new(blob: BlobRef, release: impl FnOnce() + Send + Sync + 'static) -> Self {
        Self {
            blob,
            release: Some(Box::new(release)),
        }
    }

    /// Returns the exact blob protected by this guard.
    pub const fn blob(&self) -> BlobRef {
        self.blob
    }
}

impl core::fmt::Debug for BlobReferenceGuard {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("BlobReferenceGuard")
            .field("blob", &self.blob)
            .finish_non_exhaustive()
    }
}

impl Drop for BlobReferenceGuard {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            release();
        }
    }
}

/// Object-safe authority for every text index blob put/reference/delete.
#[async_trait]
pub trait BlobPublicationCoordinator: Send + Sync {
    /// Reserves one exact publication identity without starting object I/O.
    async fn reserve(
        &self,
        blob: BlobRef,
        intent_id: TextUploadIntentId,
        writer_epoch: WriterEpoch,
    ) -> Result<BlobPublicationPermit, BlobPublicationError>;

    /// Revalidates an unexpired permit before a bounded client operation.
    async fn validate_for(
        &self,
        permit: &BlobPublicationPermit,
        minimum_validity: BlobOperationDuration,
    ) -> Result<(), BlobPublicationError>;

    /// Admits and coordinator-owns an idempotent content-addressed put.
    async fn publish(
        &self,
        permit: &BlobPublicationPermit,
        payload: Bytes,
    ) -> Result<BlobPublicationStatus, BlobPublicationError>;

    /// Returns the exact retained status of one permit.
    async fn publication_status(
        &self,
        permit: &BlobPublicationPermit,
    ) -> Result<BlobPublicationStatus, BlobPublicationError>;

    /// Idempotently releases a terminal permit after durable proof.
    async fn release(
        &self,
        permit: &BlobPublicationPermit,
        authority: BlobPermitReleaseAuthority,
    ) -> Result<(), BlobPublicationError>;

    /// Validates an existing object and registers a narrow reference guard.
    async fn validate_reference(
        &self,
        blob: BlobRef,
    ) -> Result<BlobReferenceGuard, BlobPublicationError>;

    /// Closes one blob to new reservation, publication, and reference work.
    async fn begin_delete(
        &self,
        key: BlobDeleteFenceKey,
    ) -> Result<BeginBlobDelete, BlobPublicationError>;

    /// Checks backend-owned publication/reference quiescence for an exact fence.
    async fn check_quiescent(&self, fence: &BlobDeleteFence) -> Result<bool, BlobPublicationError>;

    /// Inspects the object while revalidating the exact retained delete fence.
    ///
    /// This method grants no reference authority. Implementations must keep the
    /// blob closed to publication and reference validation for the entire
    /// observation and must reject a stale or different-run fence.
    async fn inspect_fenced_blob(
        &self,
        fence: &BlobDeleteFence,
    ) -> Result<FencedBlobObservation, BlobPublicationError>;

    /// Deletes or confirms absence under the exact current runtime fence.
    async fn delete(
        &self,
        fence: &BlobDeleteFence,
    ) -> Result<BlobDeleteOutcome, BlobPublicationError>;

    /// Idempotently reopens only the same retained run after DB cleanup.
    async fn finish_delete(
        &self,
        key: BlobDeleteFenceKey,
        authority: CleanupCommittedAuthority,
    ) -> Result<(), BlobPublicationError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ReservationIdentity {
    blob: BlobRef,
    intent_id: TextUploadIntentId,
    writer_epoch: WriterEpoch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PermitState {
    Reserved,
    InFlight,
    Succeeded,
    DefinitivelyFailed,
    ExpiredUnused,
}

struct PermitRecord {
    permit: BlobPublicationPermit,
    identity: ReservationIdentity,
    expires_at: Instant,
    state: PermitState,
    ambiguous_error: Option<String>,
    notify: Arc<Notify>,
}

#[derive(Default)]
struct ProcessLocalBlobPublicationState {
    permits: HashMap<BlobPublicationPermitId, PermitRecord>,
    reservations: HashMap<ReservationIdentity, BlobPublicationPermitId>,
    delete_fences: HashMap<BlobRef, BlobGcRunId>,
    reference_guards: HashMap<BlobRef, usize>,
}

struct ProcessLocalBlobPublicationInner {
    store: Arc<dyn ObjectStore>,
    db_path: String,
    timing: BlobPublicationTiming,
    state: Mutex<ProcessLocalBlobPublicationState>,
    #[cfg(test)]
    fail_next_release: AtomicBool,
}

/// Deterministic adapter for one process-local database identity.
///
/// Phase 8 binds construction of this adapter to the non-forgeable in-memory
/// database token. Shared object stores require an injected external adapter;
/// this type must not be used as a cross-process coordination claim.
#[derive(Clone)]
pub(crate) struct ProcessLocalBlobPublicationCoordinator {
    inner: Arc<ProcessLocalBlobPublicationInner>,
}

impl core::fmt::Debug for ProcessLocalBlobPublicationCoordinator {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("ProcessLocalBlobPublicationCoordinator")
            .field("db_path", &self.inner.db_path)
            .field("timing", &self.inner.timing)
            .finish_non_exhaustive()
    }
}

impl ProcessLocalBlobPublicationCoordinator {
    /// Constructs a coordinator scoped to one database/object-store identity.
    pub(crate) fn new(
        store: Arc<dyn ObjectStore>,
        db_path: impl Into<String>,
        timing: BlobPublicationTiming,
    ) -> Self {
        Self {
            inner: Arc::new(ProcessLocalBlobPublicationInner {
                store,
                db_path: db_path.into(),
                timing,
                state: Mutex::new(ProcessLocalBlobPublicationState::default()),
                #[cfg(test)]
                fail_next_release: AtomicBool::new(false),
            }),
        }
    }

    /// Injects one coordinator outage at permit release for boundary tests.
    #[cfg(test)]
    pub(crate) fn fail_next_release(&self) {
        self.inner.fail_next_release.store(true, Ordering::SeqCst);
    }

    /// Deterministically expires one exact unused reservation in boundary tests.
    #[cfg(test)]
    pub(crate) fn expire_unused_permit(&self, permit: BlobPublicationPermit) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("blob coordinator state lock is not poisoned");
        let record = state
            .permits
            .get_mut(&permit.id)
            .expect("test permit is owned by this coordinator");
        assert_eq!(
            record.state,
            PermitState::Reserved,
            "only an unused reservation can be expired deterministically"
        );
        record.state = PermitState::ExpiredUnused;
    }

    fn exact_record<'a>(
        state: &'a mut ProcessLocalBlobPublicationState,
        permit: &BlobPublicationPermit,
        now: Instant,
    ) -> Result<&'a mut PermitRecord, BlobPublicationError> {
        let Some(record) = state.permits.get_mut(&permit.id) else {
            return Err(BlobPublicationError::UnknownPermit);
        };
        if record.state == PermitState::Reserved && now >= record.expires_at {
            record.state = PermitState::ExpiredUnused;
        }
        Ok(record)
    }

    fn public_status(record: &PermitRecord) -> Result<BlobPublicationStatus, BlobPublicationError> {
        match record.state {
            PermitState::Reserved => Ok(BlobPublicationStatus::Reserved),
            PermitState::InFlight => Ok(BlobPublicationStatus::InFlight),
            PermitState::Succeeded => Ok(BlobPublicationStatus::Succeeded(
                VerifiedBlobMetadata::new(record.identity.blob),
            )),
            PermitState::DefinitivelyFailed => Ok(BlobPublicationStatus::DefinitivelyFailed),
            PermitState::ExpiredUnused => Ok(BlobPublicationStatus::ExpiredUnused),
        }
    }

    fn fence_matches(state: &ProcessLocalBlobPublicationState, fence: &BlobDeleteFence) -> bool {
        state.delete_fences.get(&fence.key.blob).copied() == Some(fence.key.run_id)
    }

    fn expire_blob_reservations(
        state: &mut ProcessLocalBlobPublicationState,
        blob: BlobRef,
        now: Instant,
    ) {
        for record in state
            .permits
            .values_mut()
            .filter(|record| record.identity.blob == blob)
        {
            if record.state == PermitState::Reserved && now >= record.expires_at {
                record.state = PermitState::ExpiredUnused;
            }
        }
    }
}

#[async_trait]
impl BlobPublicationCoordinator for ProcessLocalBlobPublicationCoordinator {
    async fn reserve(
        &self,
        blob: BlobRef,
        intent_id: TextUploadIntentId,
        writer_epoch: WriterEpoch,
    ) -> Result<BlobPublicationPermit, BlobPublicationError> {
        let identity = ReservationIdentity {
            blob,
            intent_id,
            writer_epoch,
        };
        let now = Instant::now();
        let mut state = self
            .inner
            .state
            .lock()
            .expect("blob coordinator state lock is not poisoned");
        if state.delete_fences.contains_key(&blob) {
            return Err(BlobPublicationError::DeleteFenceClosed);
        }
        if let Some(existing_id) = state.reservations.get(&identity).copied() {
            let (permit, reusable) = {
                let record = state
                    .permits
                    .get_mut(&existing_id)
                    .expect("reservation map points at a permit record");
                if record.state == PermitState::Reserved && now >= record.expires_at {
                    record.state = PermitState::ExpiredUnused;
                }
                (record.permit, record.state != PermitState::ExpiredUnused)
            };
            if reusable {
                return Ok(permit);
            }
            state.reservations.remove(&identity);
        }
        let permit = BlobPublicationPermit::from_id(BlobPublicationPermitId::new_v4());
        let expires_at = now
            .checked_add(self.inner.timing.reservation_ttl().get())
            .expect("validated reservation TTL fits monotonic time");
        state.permits.insert(
            permit.id,
            PermitRecord {
                permit,
                identity,
                expires_at,
                state: PermitState::Reserved,
                ambiguous_error: None,
                notify: Arc::new(Notify::new()),
            },
        );
        state.reservations.insert(identity, permit.id);
        Ok(permit)
    }

    async fn validate_for(
        &self,
        permit: &BlobPublicationPermit,
        _minimum_validity: BlobOperationDuration,
    ) -> Result<(), BlobPublicationError> {
        // Process-local admission has no expiring backend lease: once a put is
        // admitted, the coordinator owns it until a definitive result. Any
        // positive requested interval is therefore supportable. The current
        // reservation is still revalidated here, and `publish` revalidates it
        // again in the linearized admission step so an intervening expiry is
        // a typed rejection rather than an untracked put.
        let mut state = self
            .inner
            .state
            .lock()
            .expect("blob coordinator state lock is not poisoned");
        let Some(identity) = state.permits.get(&permit.id).map(|record| record.identity) else {
            return Err(BlobPublicationError::UnknownPermit);
        };
        if state.delete_fences.contains_key(&identity.blob) {
            return Err(BlobPublicationError::DeleteFenceClosed);
        }
        let record = Self::exact_record(&mut state, permit, Instant::now())?;
        match record.state {
            PermitState::Reserved | PermitState::InFlight | PermitState::Succeeded => Ok(()),
            PermitState::ExpiredUnused => Err(BlobPublicationError::PermitExpired),
            PermitState::DefinitivelyFailed => Err(BlobPublicationError::PayloadMismatch),
        }
    }

    async fn publish(
        &self,
        permit: &BlobPublicationPermit,
        payload: Bytes,
    ) -> Result<BlobPublicationStatus, BlobPublicationError> {
        let digest: [u8; 32] = Sha256::digest(&payload).into();
        let notify = {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("blob coordinator state lock is not poisoned");
            let Some(identity) = state.permits.get(&permit.id).map(|record| record.identity) else {
                return Err(BlobPublicationError::UnknownPermit);
            };
            if digest != *identity.blob.hash()
                || u64::try_from(payload.len()).unwrap_or(u64::MAX) != identity.blob.size()
            {
                let record = Self::exact_record(&mut state, permit, Instant::now())?;
                if record.state == PermitState::Reserved {
                    record.state = PermitState::DefinitivelyFailed;
                    record.notify.notify_waiters();
                }
                return Err(BlobPublicationError::PayloadMismatch);
            }
            if state.delete_fences.contains_key(&identity.blob) {
                return Err(BlobPublicationError::DeleteFenceClosed);
            }
            let record = Self::exact_record(&mut state, permit, Instant::now())?;
            match record.state {
                PermitState::Reserved => {
                    record.state = PermitState::InFlight;
                    let notify = Arc::clone(&record.notify);
                    let inner = Arc::clone(&self.inner);
                    let permit = *permit;
                    let blob = identity.blob;
                    let payload = payload.clone();
                    tokio::spawn(async move {
                        let location = crate::search::text::blob_object_store_path(
                            &inner.db_path,
                            *blob.hash(),
                        );
                        let result = inner
                            .store
                            .put(&location, PutPayload::from_bytes(payload))
                            .await;
                        let mut state = inner
                            .state
                            .lock()
                            .expect("blob coordinator state lock is not poisoned");
                        let Some(record) = state.permits.get_mut(&permit.id) else {
                            debug_assert!(false, "admitted permit remains until terminal release");
                            return;
                        };
                        if record.state != PermitState::InFlight {
                            return;
                        }
                        match result {
                            Ok(_) => record.state = PermitState::Succeeded,
                            Err(error) => record.ambiguous_error = Some(error.to_string()),
                        }
                        record.notify.notify_waiters();
                    });
                    notify
                }
                PermitState::InFlight => Arc::clone(&record.notify),
                PermitState::Succeeded
                | PermitState::DefinitivelyFailed
                | PermitState::ExpiredUnused => return Self::public_status(record),
            }
        };

        loop {
            let notified = notify.notified();
            {
                let mut state = self
                    .inner
                    .state
                    .lock()
                    .expect("blob coordinator state lock is not poisoned");
                let record = Self::exact_record(&mut state, permit, Instant::now())?;
                if let Some(error) = &record.ambiguous_error {
                    return Err(BlobPublicationError::PublicationOutcomeAmbiguous(
                        error.clone(),
                    ));
                }
                if record.state != PermitState::InFlight {
                    return Self::public_status(record);
                }
            }
            notified.await;
        }
    }

    async fn publication_status(
        &self,
        permit: &BlobPublicationPermit,
    ) -> Result<BlobPublicationStatus, BlobPublicationError> {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("blob coordinator state lock is not poisoned");
        let record = Self::exact_record(&mut state, permit, Instant::now())?;
        Self::public_status(record)
    }

    async fn release(
        &self,
        permit: &BlobPublicationPermit,
        authority: BlobPermitReleaseAuthority,
    ) -> Result<(), BlobPublicationError> {
        #[cfg(test)]
        if self.inner.fail_next_release.swap(false, Ordering::SeqCst) {
            return Err(BlobPublicationError::CoordinatorUnavailable(
                "injected permit-release outage".to_string(),
            ));
        }
        if authority.permit_id != permit.id {
            return Err(BlobPublicationError::ReleaseAuthorityMismatch);
        }
        let mut state = self
            .inner
            .state
            .lock()
            .expect("blob coordinator state lock is not poisoned");
        let Some(record) = state.permits.get_mut(&permit.id) else {
            // Release authority can only be created from the retained durable
            // intent/member proof. Once a terminal record has been removed,
            // replay is therefore an exact safe no-op without an unbounded
            // in-memory tombstone.
            return Ok(());
        };
        if record.state == PermitState::Reserved && Instant::now() >= record.expires_at {
            record.state = PermitState::ExpiredUnused;
        }
        let allowed = matches!(
            (record.state, authority.reason),
            (
                PermitState::Succeeded,
                BlobPermitReleaseReason::ReferenceCommitted
                    | BlobPermitReleaseReason::FencedDisposition
            ) | (
                PermitState::Reserved,
                BlobPermitReleaseReason::DefinitiveNonPublication
            ) | (
                PermitState::DefinitivelyFailed | PermitState::ExpiredUnused,
                BlobPermitReleaseReason::DefinitiveNonPublication
                    | BlobPermitReleaseReason::FencedDisposition
            )
        );
        if !allowed {
            return Err(BlobPublicationError::ReleaseAuthorityMismatch);
        }
        let identity = record.identity;
        record.notify.notify_waiters();
        state.permits.remove(&permit.id);
        if state.reservations.get(&identity).copied() == Some(permit.id) {
            state.reservations.remove(&identity);
        }
        Ok(())
    }

    async fn validate_reference(
        &self,
        blob: BlobRef,
    ) -> Result<BlobReferenceGuard, BlobPublicationError> {
        {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("blob coordinator state lock is not poisoned");
            if state.delete_fences.contains_key(&blob) {
                return Err(BlobPublicationError::DeleteFenceClosed);
            }
            let count = state.reference_guards.entry(blob).or_default();
            *count = count
                .checked_add(1)
                .expect("process-local reference guard count remains bounded");
        }
        let inner = Arc::clone(&self.inner);
        let guard = BlobReferenceGuard::new(blob, move || {
            let mut state = inner
                .state
                .lock()
                .expect("blob coordinator state lock is not poisoned");
            let Some(count) = state.reference_guards.get_mut(&blob) else {
                debug_assert!(
                    false,
                    "reference guard must be registered before construction"
                );
                return;
            };
            *count = count
                .checked_sub(1)
                .expect("reference guard count is positive");
            if *count == 0 {
                state.reference_guards.remove(&blob);
            }
        });
        let location =
            crate::search::text::blob_object_store_path(&self.inner.db_path, *blob.hash());
        let payload = match self.inner.store.get(&location).await {
            Ok(result) => result.bytes().await?,
            Err(slatedb::object_store::Error::NotFound { .. }) => {
                return Err(BlobPublicationError::ReferenceAbsent);
            }
            Err(error) => return Err(error.into()),
        };
        let digest: [u8; 32] = Sha256::digest(&payload).into();
        if digest != *blob.hash() || u64::try_from(payload.len()).unwrap_or(u64::MAX) != blob.size()
        {
            return Err(BlobPublicationError::ReferenceMismatch);
        }
        Ok(guard)
    }

    async fn begin_delete(
        &self,
        key: BlobDeleteFenceKey,
    ) -> Result<BeginBlobDelete, BlobPublicationError> {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("blob coordinator state lock is not poisoned");
        let fence = BlobDeleteFence::new(key);
        match state.delete_fences.get(&key.blob).copied() {
            None => {
                state.delete_fences.insert(key.blob, key.run_id);
                Ok(BeginBlobDelete::Acquired(fence))
            }
            Some(run_id) if run_id == key.run_id => Ok(BeginBlobDelete::AlreadyHeldSameRun(fence)),
            Some(_) => Ok(BeginBlobDelete::BusyOtherRun),
        }
    }

    async fn check_quiescent(&self, fence: &BlobDeleteFence) -> Result<bool, BlobPublicationError> {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("blob coordinator state lock is not poisoned");
        if !Self::fence_matches(&state, fence) {
            return Err(BlobPublicationError::InvalidDeleteFence);
        }
        Self::expire_blob_reservations(&mut state, fence.key.blob, Instant::now());
        if state
            .reference_guards
            .get(&fence.key.blob)
            .is_some_and(|count| *count != 0)
        {
            return Ok(false);
        }
        Ok(!state.permits.values().any(|record| {
            record.identity.blob == fence.key.blob
                && matches!(record.state, PermitState::Reserved | PermitState::InFlight)
        }))
    }

    async fn inspect_fenced_blob(
        &self,
        fence: &BlobDeleteFence,
    ) -> Result<FencedBlobObservation, BlobPublicationError> {
        {
            let state = self
                .inner
                .state
                .lock()
                .expect("blob coordinator state lock is not poisoned");
            if !Self::fence_matches(&state, fence) {
                return Err(BlobPublicationError::InvalidDeleteFence);
            }
        }
        let location = crate::search::text::blob_object_store_path(
            &self.inner.db_path,
            *fence.key.blob.hash(),
        );
        let payload = match self.inner.store.get(&location).await {
            Ok(result) => result.bytes().await?,
            Err(slatedb::object_store::Error::NotFound { .. }) => {
                return Ok(FencedBlobObservation::Absent);
            }
            Err(error) => return Err(error.into()),
        };
        let digest: [u8; 32] = Sha256::digest(&payload).into();
        if digest == *fence.key.blob.hash()
            && u64::try_from(payload.len()).unwrap_or(u64::MAX) == fence.key.blob.size()
        {
            Ok(FencedBlobObservation::Exact)
        } else {
            Ok(FencedBlobObservation::Mismatch)
        }
    }

    async fn delete(
        &self,
        fence: &BlobDeleteFence,
    ) -> Result<BlobDeleteOutcome, BlobPublicationError> {
        if !self.check_quiescent(fence).await? {
            return Err(BlobPublicationError::DeleteFenceNotQuiescent);
        }
        let location = crate::search::text::blob_object_store_path(
            &self.inner.db_path,
            *fence.key.blob.hash(),
        );
        match self.inner.store.delete(&location).await {
            Ok(()) | Err(slatedb::object_store::Error::NotFound { .. }) => {
                Ok(BlobDeleteOutcome::DeletedOrAbsent)
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn finish_delete(
        &self,
        key: BlobDeleteFenceKey,
        authority: CleanupCommittedAuthority,
    ) -> Result<(), BlobPublicationError> {
        if authority.key != key {
            return Err(BlobPublicationError::InvalidDeleteFence);
        }
        let mut state = self
            .inner
            .state
            .lock()
            .expect("blob coordinator state lock is not poisoned");
        if state.delete_fences.get(&key.blob).copied() == Some(key.run_id) {
            state.delete_fences.remove(&key.blob);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::throttle::{ThrottleConfig, ThrottledStore};

    use super::*;

    fn blob(payload: &[u8]) -> BlobRef {
        BlobRef::new(
            Sha256::digest(payload).into(),
            u64::try_from(payload.len()).unwrap(),
        )
    }

    fn coordinator(timing: BlobPublicationTiming) -> ProcessLocalBlobPublicationCoordinator {
        ProcessLocalBlobPublicationCoordinator::new(
            Arc::new(InMemory::new()),
            "blob-coordinator-tests",
            timing,
        )
    }

    #[test]
    fn durations_reject_zero_and_retain_documented_defaults() {
        assert!(matches!(
            BlobOperationDuration::try_from_duration(Duration::ZERO),
            Err(BlobPublicationError::ZeroDuration)
        ));
        let timing = BlobPublicationTiming::default();
        assert_eq!(timing.reservation_ttl().get(), Duration::from_secs(30));
        assert_eq!(timing.publish_timeout().get(), Duration::from_secs(300));
        assert_eq!(timing.safety_margin().get(), Duration::from_secs(5));
    }

    #[tokio::test]
    async fn reservation_publication_reference_and_release_are_exact_and_idempotent() {
        let coordinator = coordinator(BlobPublicationTiming::default());
        let payload = Bytes::from_static(b"coordinated publication");
        let blob = blob(&payload);
        let intent_id = TextUploadIntentId::from_bytes([1; 16]).unwrap();
        let writer_epoch = WriterEpoch::from_bytes([2; 16]).unwrap();
        let permit = coordinator
            .reserve(blob, intent_id, writer_epoch)
            .await
            .unwrap();
        assert_eq!(
            coordinator
                .reserve(blob, intent_id, writer_epoch)
                .await
                .unwrap(),
            permit
        );
        let recovered_permit = BlobPublicationPermit::from_id(permit.id());
        assert_eq!(recovered_permit, permit);
        coordinator
            .validate_for(
                &recovered_permit,
                BlobPublicationTiming::default().publish_timeout(),
            )
            .await
            .unwrap();
        assert_eq!(
            coordinator
                .publish(&recovered_permit, payload)
                .await
                .unwrap(),
            BlobPublicationStatus::Succeeded(VerifiedBlobMetadata { blob })
        );
        assert_eq!(
            coordinator.publication_status(&permit).await.unwrap(),
            BlobPublicationStatus::Succeeded(VerifiedBlobMetadata { blob })
        );
        drop(coordinator.validate_reference(blob).await.unwrap());
        let authority = BlobPermitReleaseAuthority::reference_committed(permit.id());
        coordinator.release(&permit, authority).await.unwrap();
        coordinator.release(&permit, authority).await.unwrap();
        assert!(matches!(
            coordinator.publication_status(&permit).await,
            Err(BlobPublicationError::UnknownPermit)
        ));
    }

    #[tokio::test]
    async fn payload_mismatch_is_definitive_and_requires_nonpublication_authority() {
        let coordinator = coordinator(BlobPublicationTiming::default());
        let declared = blob(b"declared");
        let permit = coordinator
            .reserve(
                declared,
                TextUploadIntentId::from_bytes([3; 16]).unwrap(),
                WriterEpoch::from_bytes([4; 16]).unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            coordinator
                .publish(&permit, Bytes::from_static(b"different"))
                .await,
            Err(BlobPublicationError::PayloadMismatch)
        ));
        assert_eq!(
            coordinator.publication_status(&permit).await.unwrap(),
            BlobPublicationStatus::DefinitivelyFailed
        );
        assert!(matches!(
            coordinator
                .release(
                    &permit,
                    BlobPermitReleaseAuthority::reference_committed(permit.id())
                )
                .await,
            Err(BlobPublicationError::ReleaseAuthorityMismatch)
        ));
        coordinator
            .release(
                &permit,
                BlobPermitReleaseAuthority::definitive_non_publication(permit.id()),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn delete_fence_blocks_new_work_waits_for_guards_and_reopens_exact_run() {
        let coordinator = coordinator(BlobPublicationTiming::default());
        let payload = Bytes::from_static(b"delete-fenced publication");
        let blob = blob(&payload);
        let permit = coordinator
            .reserve(
                blob,
                TextUploadIntentId::from_bytes([5; 16]).unwrap(),
                WriterEpoch::from_bytes([6; 16]).unwrap(),
            )
            .await
            .unwrap();
        coordinator.publish(&permit, payload).await.unwrap();
        let reference = coordinator.validate_reference(blob).await.unwrap();
        let key = BlobDeleteFenceKey::new(blob, BlobGcRunId::from_bytes([7; 16]).unwrap());
        let BeginBlobDelete::Acquired(fence) = coordinator.begin_delete(key).await.unwrap() else {
            panic!("first delete closes the blob");
        };
        assert!(matches!(
            coordinator
                .reserve(
                    blob,
                    TextUploadIntentId::from_bytes([8; 16]).unwrap(),
                    WriterEpoch::from_bytes([9; 16]).unwrap(),
                )
                .await,
            Err(BlobPublicationError::DeleteFenceClosed)
        ));
        assert!(!coordinator.check_quiescent(&fence).await.unwrap());
        drop(reference);
        assert!(coordinator.check_quiescent(&fence).await.unwrap());
        assert_eq!(
            coordinator.inspect_fenced_blob(&fence).await.unwrap(),
            FencedBlobObservation::Exact
        );
        assert_eq!(
            coordinator.delete(&fence).await.unwrap(),
            BlobDeleteOutcome::DeletedOrAbsent
        );
        assert_eq!(
            coordinator.inspect_fenced_blob(&fence).await.unwrap(),
            FencedBlobObservation::Absent
        );
        let other = BlobDeleteFenceKey::new(blob, BlobGcRunId::from_bytes([10; 16]).unwrap());
        assert_eq!(
            coordinator.begin_delete(other).await.unwrap(),
            BeginBlobDelete::BusyOtherRun
        );
        let authority = CleanupCommittedAuthority::new(key);
        coordinator.finish_delete(key, authority).await.unwrap();
        coordinator.finish_delete(key, authority).await.unwrap();
        assert!(matches!(
            coordinator.inspect_fenced_blob(&fence).await,
            Err(BlobPublicationError::InvalidDeleteFence)
        ));
        let BeginBlobDelete::Acquired(other_fence) = coordinator.begin_delete(other).await.unwrap()
        else {
            panic!("newer run acquires the reopened blob");
        };
        coordinator.finish_delete(key, authority).await.unwrap();
        assert!(coordinator.check_quiescent(&other_fence).await.unwrap());
        assert!(matches!(
            coordinator
                .reserve(
                    blob,
                    TextUploadIntentId::from_bytes([11; 16]).unwrap(),
                    WriterEpoch::from_bytes([12; 16]).unwrap(),
                )
                .await,
            Err(BlobPublicationError::DeleteFenceClosed)
        ));
        coordinator
            .finish_delete(other, CleanupCommittedAuthority::new(other))
            .await
            .unwrap();
        assert!(
            coordinator
                .reserve(
                    blob,
                    TextUploadIntentId::from_bytes([13; 16]).unwrap(),
                    WriterEpoch::from_bytes([14; 16]).unwrap(),
                )
                .await
                .is_ok(),
            "same-hash publication reopens only after the current run finishes"
        );
    }

    #[tokio::test]
    async fn unused_reservation_expires_under_coordinator_time_and_drains() {
        let timing = BlobPublicationTiming::new(
            BlobOperationDuration::from_millis(NonZeroU64::MIN),
            BlobPublicationTiming::default().publish_timeout(),
            BlobPublicationTiming::default().safety_margin(),
        );
        let coordinator = coordinator(timing);
        let blob = blob(b"expires-unused");
        let permit = coordinator
            .reserve(
                blob,
                TextUploadIntentId::from_bytes([11; 16]).unwrap(),
                WriterEpoch::from_bytes([12; 16]).unwrap(),
            )
            .await
            .unwrap();
        let key = BlobDeleteFenceKey::new(blob, BlobGcRunId::from_bytes([13; 16]).unwrap());
        let BeginBlobDelete::Acquired(fence) = coordinator.begin_delete(key).await.unwrap() else {
            panic!("first delete closes the blob");
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(coordinator.check_quiescent(&fence).await.unwrap());
        assert_eq!(
            coordinator.publication_status(&permit).await.unwrap(),
            BlobPublicationStatus::ExpiredUnused
        );
        coordinator
            .release(
                &permit,
                BlobPermitReleaseAuthority::definitive_non_publication(permit.id()),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn unadmitted_reservation_releases_after_intent_noncommit() {
        let coordinator = coordinator(BlobPublicationTiming::default());
        let blob = blob(b"intent transaction did not commit");
        let permit = coordinator
            .reserve(
                blob,
                TextUploadIntentId::from_bytes([20; 16]).unwrap(),
                WriterEpoch::from_bytes([21; 16]).unwrap(),
            )
            .await
            .unwrap();
        coordinator
            .release(
                &permit,
                BlobPermitReleaseAuthority::definitive_non_publication(permit.id()),
            )
            .await
            .unwrap();
        assert!(matches!(
            coordinator.publication_status(&permit).await,
            Err(BlobPublicationError::UnknownPermit)
        ));
    }

    #[tokio::test]
    async fn cancelling_publish_waiter_keeps_admitted_put_visible_until_completion() {
        let store = Arc::new(ThrottledStore::new(
            InMemory::new(),
            ThrottleConfig {
                wait_put_per_call: Duration::from_millis(50),
                ..ThrottleConfig::default()
            },
        ));
        let coordinator = ProcessLocalBlobPublicationCoordinator::new(
            store,
            "cancelled-publish-tests",
            BlobPublicationTiming::default(),
        );
        let payload = Bytes::from_static(b"coordinator-owned-after-cancellation");
        let blob = blob(&payload);
        let permit = coordinator
            .reserve(
                blob,
                TextUploadIntentId::from_bytes([17; 16]).unwrap(),
                WriterEpoch::from_bytes([18; 16]).unwrap(),
            )
            .await
            .unwrap();
        let publish_coordinator = coordinator.clone();
        let publish_task =
            tokio::spawn(async move { publish_coordinator.publish(&permit, payload).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if coordinator.publication_status(&permit).await.unwrap()
                    == BlobPublicationStatus::InFlight
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("publication reaches coordinator-owned in-flight state");
        publish_task.abort();
        assert!(publish_task.await.unwrap_err().is_cancelled());

        let key = BlobDeleteFenceKey::new(blob, BlobGcRunId::from_bytes([19; 16]).unwrap());
        let BeginBlobDelete::Acquired(fence) = coordinator.begin_delete(key).await.unwrap() else {
            panic!("first delete closes the blob");
        };
        assert!(!coordinator.check_quiescent(&fence).await.unwrap());
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if matches!(
                    coordinator.publication_status(&permit).await.unwrap(),
                    BlobPublicationStatus::Succeeded(_)
                ) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("coordinator-owned put completes after waiter cancellation");
        assert!(coordinator.check_quiescent(&fence).await.unwrap());
    }

    #[test]
    fn authority_constructors_are_not_part_of_the_public_contract() {
        let permit_id = BlobPublicationPermitId::from_bytes([14; 16]).unwrap();
        let fenced = BlobPermitReleaseAuthority::fenced_disposition(permit_id);
        assert_eq!(fenced.permit_id, permit_id);
        let key = BlobDeleteFenceKey::new(
            BlobRef::new([15; 32], 1),
            BlobGcRunId::from_bytes([16; 16]).unwrap(),
        );
        assert_eq!(CleanupCommittedAuthority::new(key).key, key);
    }
}
