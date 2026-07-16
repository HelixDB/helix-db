//! Index lifecycle receipt and operation-status response contracts.
//!
//! ```
//! use helix_db::{IndexDdlReceipt, IndexOperationStatus};
//!
//! let receipt: IndexDdlReceipt = sonic_rs::from_str(
//!     r#"{"kind":"accepted","operation_id":"07070707-0707-0707-0707-070707070707","index_id":"42","generation":"3"}"#,
//! ).unwrap();
//! assert!(matches!(receipt, IndexDdlReceipt::Accepted { index_id: 42, .. }));
//!
//! let status: IndexOperationStatus = sonic_rs::from_str(
//!     r#"{"status":"queued","operation_id":"07070707-0707-0707-0707-070707070707","index_id":"42","generation":"3","operation_kind":"build","family":"secondary","stage":"scan","attempt":0,"progress":{"entities":"0","input_bytes":"0","output_operations":"0","output_bytes":"0"},"future":true}"#,
//! ).unwrap();
//! assert!(matches!(status, IndexOperationStatus::Queued { .. }));
//! ```

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

/// Result returned by CREATE or DROP.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum IndexDdlReceipt {
    /// A new durable operation was accepted.
    Accepted {
        /// Stable operation UUID.
        #[serde(with = "uuid_string")]
        operation_id: Uuid,
        /// Logical index ID.
        #[serde(with = "positive_u64_string")]
        index_id: u64,
        /// Physical generation.
        #[serde(with = "positive_u64_string")]
        generation: u64,
    },
    /// The request converged on existing work.
    ExistingOperation {
        /// Stable operation UUID.
        #[serde(with = "uuid_string")]
        operation_id: Uuid,
    },
    /// An identical index is already active.
    AlreadyActive {
        /// Logical index ID.
        #[serde(with = "positive_u64_string")]
        index_id: u64,
        /// Active physical generation.
        #[serde(with = "positive_u64_string")]
        generation: u64,
    },
}

/// BUILD or DROP.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexOperationKind {
    /// Build and activate.
    Build,
    /// Drain and remove.
    Drop,
}

/// Physical index family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexFamily {
    /// Equality/range secondary indexes.
    Secondary,
    /// Vector indexes.
    Vector,
    /// Text indexes.
    Text,
}

/// Frozen lifecycle stage returned by operation status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexOperationStage {
    /// Scan authoritative secondary/vector entities.
    Scan,
    /// Scan authoritative text partitions.
    ScanPartitions,
    /// Wait for one exact text upload child to attach or reconcile.
    AwaitUpload,
    /// Apply mutation deltas captured during the scan.
    CatchUp,
    /// Validate secondary ownership and uniqueness.
    Validate,
    /// Validate vector metadata against its canonical descriptor.
    ValidateDescriptor,
    /// Compact text split state.
    Compact,
    /// Construct bounded text manifest pages.
    PrepareManifests,
    /// Validate manifest topology, publication, and remaining build ownership.
    ValidateManifests,
    /// Publish the hidden generation.
    Activate,
    /// Begin ordinary DROP reader drain.
    BeginDrain,
    /// Delete secondary entries.
    DeleteEntries,
    /// Retire vector memory.
    RetireCache,
    /// Delete vector physical rows.
    DeletePhysical,
    /// Delete retained mutation deltas.
    DeleteDeltas,
    /// Prepare text blob candidates.
    PrepareCandidates,
    /// Acquire text deletion fences.
    AcquireDeleteFences,
    /// Retire text manifests.
    RetireManifest,
    /// Retire text artifacts.
    RetireArtifacts,
    /// Retire text upload intents.
    RetireUploadIntents,
    /// Prove global blob reachability.
    MarkReachability,
    /// Delete unreachable text blobs.
    DeleteBlobs,
    /// Delete text entity live state.
    DeleteEntityState,
    /// Finish ordinary DROP reader drain.
    FinishDrain,
    /// Finalize ordinary DROP cleanup.
    Finalize,
    /// Begin BUILD abort reader drain.
    AbortingBeginDrain,
    /// Delete secondary entries during BUILD abort.
    AbortingDeleteEntries,
    /// Retire vector memory during BUILD abort.
    AbortingRetireCache,
    /// Delete vector physical rows during BUILD abort.
    AbortingDeletePhysical,
    /// Delete mutation deltas during BUILD abort.
    AbortingDeleteDeltas,
    /// Prepare text candidates during BUILD abort.
    AbortingPrepareCandidates,
    /// Acquire text deletion fences during BUILD abort.
    AbortingAcquireDeleteFences,
    /// Retire text manifests during BUILD abort.
    AbortingRetireManifest,
    /// Retire text artifacts during BUILD abort.
    AbortingRetireArtifacts,
    /// Retire text upload intents during BUILD abort.
    AbortingRetireUploadIntents,
    /// Prove text reachability during BUILD abort.
    AbortingMarkReachability,
    /// Delete text blobs during BUILD abort.
    AbortingDeleteBlobs,
    /// Delete text entity state during BUILD abort.
    AbortingDeleteEntityState,
    /// Finish reader drain during BUILD abort.
    AbortingFinishDrain,
    /// Finalize BUILD abort cleanup.
    AbortingFinalize,
}

impl IndexOperationStage {
    /// Returns whether this stage belongs to BUILD abort cleanup.
    pub const fn is_aborting(self) -> bool {
        matches!(
            self,
            Self::AbortingBeginDrain
                | Self::AbortingDeleteEntries
                | Self::AbortingRetireCache
                | Self::AbortingDeletePhysical
                | Self::AbortingDeleteDeltas
                | Self::AbortingPrepareCandidates
                | Self::AbortingAcquireDeleteFences
                | Self::AbortingRetireManifest
                | Self::AbortingRetireArtifacts
                | Self::AbortingRetireUploadIntents
                | Self::AbortingMarkReachability
                | Self::AbortingDeleteBlobs
                | Self::AbortingDeleteEntityState
                | Self::AbortingFinishDrain
                | Self::AbortingFinalize
        )
    }
}

/// Stable blocked-operation reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexOperationBlockerCode {
    /// Authoritative source data cannot satisfy the index contract.
    InvalidSourceData,
    /// A unique index found more than one owner for a value.
    UniquenessViolation,
    /// One source entity exceeds the configured bounded-step limit.
    OversizedEntity,
    /// A text manifest exceeds its configured bound.
    ManifestLimit,
    /// Generation reader-drain coordination is unavailable.
    ReaderCoordinationUnavailable,
    /// Text object-store configuration is unavailable.
    ObjectStoreConfigurationUnavailable,
    /// Blob publication coordination is unavailable.
    BlobPublicationCoordinationUnavailable,
    /// The runtime could not prove an internal lifecycle invariant.
    InvariantViolation,
}

/// Stable index API error identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexErrorCode {
    /// The family has not reached its safe public capability state.
    IndexLifecycleUnavailable,
    /// The logical index already exists.
    IndexAlreadyExists,
    /// The requested definition conflicts with the canonical definition.
    IndexDefinitionConflict,
    /// The logical index is already changing lifecycle state.
    IndexBusy,
    /// The logical index does not exist in the request scope.
    IndexNotFound,
    /// The retained operation does not exist in the request scope.
    IndexOperationNotFound,
    /// The retained operation cannot legally be aborted.
    IndexOperationNotAbortable,
    /// Reader-drain coordination is unavailable.
    ReaderCoordinationUnavailable,
    /// The logical index ID namespace is exhausted.
    IndexIdExhausted,
    /// Blob publication coordination is unavailable.
    BlobPublicationCoordinationUnavailable,
    /// The vector physical ID namespace is exhausted.
    VectorPhysicalIdExhausted,
    /// The physical generation namespace is exhausted.
    IndexGenerationExhausted,
    /// The canonical index-record revision is exhausted.
    IndexRevisionExhausted,
    /// The operation-record revision is exhausted.
    IndexOperationRevisionExhausted,
    /// A retained active handle no longer names the canonical generation.
    StaleIndexGeneration,
    /// Writer fencing prevented the runtime from proving commit outcome.
    WriterFencedCommitOutcomeUnknown,
}

/// Monotonic public progress counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexOperationProgress {
    /// Authoritative source entities visited.
    #[serde(with = "u64_string")]
    pub entities: u64,
    /// Source bytes consumed.
    #[serde(with = "u64_string")]
    pub input_bytes: u64,
    /// Physical operations staged.
    #[serde(with = "u64_string")]
    pub output_operations: u64,
    /// Physical output bytes staged.
    #[serde(with = "u64_string")]
    pub output_bytes: u64,
}

/// Fields common to every operation status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexOperationStatusCommon {
    /// Stable operation UUID.
    #[serde(with = "uuid_string")]
    pub operation_id: Uuid,
    /// Logical index ID.
    #[serde(with = "positive_u64_string")]
    pub index_id: u64,
    /// Physical generation affected by this operation.
    #[serde(with = "positive_u64_string")]
    pub generation: u64,
    /// BUILD or DROP.
    pub operation_kind: IndexOperationKind,
    /// Secondary, vector, or text physical lane.
    pub family: IndexFamily,
    /// Frozen family stage name.
    pub stage: IndexOperationStage,
    /// Number of durable claim attempts.
    pub attempt: u32,
    /// Monotonic bounded-work counters.
    pub progress: IndexOperationProgress,
}

/// Status returned by get/retry/abort. Unknown additive fields are ignored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum IndexOperationStatus {
    /// Runnable, including work waiting for a bounded retry deadline.
    Queued {
        /// Fields shared by every status.
        #[serde(flatten)]
        common: IndexOperationStatusCommon,
    },
    /// Claimed by a fenced writer.
    Running {
        /// Fields shared by every status.
        #[serde(flatten)]
        common: IndexOperationStatusCommon,
    },
    /// Paused until an explicit retry or abort.
    Blocked {
        /// Fields shared by every status.
        #[serde(flatten)]
        common: IndexOperationStatusCommon,
        /// Stable machine-readable blocker.
        blocker_code: IndexOperationBlockerCode,
        /// Optional non-contractual diagnostic.
        #[serde(default)]
        message: Option<String>,
    },
    /// Build or drop completed successfully.
    Succeeded {
        /// Fields shared by every status.
        #[serde(flatten)]
        common: IndexOperationStatusCommon,
    },
    /// Build cleanup completed after an explicit abort.
    Aborted {
        /// Fields shared by every status.
        #[serde(flatten)]
        common: IndexOperationStatusCommon,
    },
}

#[derive(Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum IndexOperationStatusWire {
    Queued {
        #[serde(flatten)]
        common: IndexOperationStatusCommon,
    },
    Running {
        #[serde(flatten)]
        common: IndexOperationStatusCommon,
    },
    Blocked {
        #[serde(flatten)]
        common: IndexOperationStatusCommon,
        blocker_code: IndexOperationBlockerCode,
        #[serde(default)]
        message: Option<String>,
    },
    Succeeded {
        #[serde(flatten)]
        common: IndexOperationStatusCommon,
    },
    Aborted {
        #[serde(flatten)]
        common: IndexOperationStatusCommon,
    },
}

impl<'de> Deserialize<'de> for IndexOperationStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let status = IndexOperationStatusWire::deserialize(deserializer)?;
        Ok(match status {
            IndexOperationStatusWire::Queued { common } => Self::Queued { common },
            IndexOperationStatusWire::Running { common } => Self::Running { common },
            IndexOperationStatusWire::Blocked {
                common,
                blocker_code,
                message,
            } => Self::Blocked {
                common,
                blocker_code,
                message,
            },
            IndexOperationStatusWire::Succeeded { common } => Self::Succeeded { common },
            IndexOperationStatusWire::Aborted { common } => {
                if common.operation_kind != IndexOperationKind::Build || !common.stage.is_aborting()
                {
                    return Err(serde::de::Error::custom(
                        "aborted status must describe build cleanup",
                    ));
                }
                Self::Aborted { common }
            }
        })
    }
}

mod u64_string {
    use super::*;

    pub(super) fn serialize<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        let parsed = value.parse::<u64>().map_err(serde::de::Error::custom)?;
        if parsed.to_string() != value {
            return Err(serde::de::Error::custom(
                "expected a canonical unsigned decimal string",
            ));
        }
        Ok(parsed)
    }
}

mod positive_u64_string {
    use super::*;

    pub(super) fn serialize<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64_string::deserialize(deserializer)?;
        if value == 0 {
            return Err(serde::de::Error::custom("identifier must be non-zero"));
        }
        Ok(value)
    }
}

mod uuid_string {
    use super::*;

    pub(super) fn serialize<S>(value: &Uuid, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Uuid, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        let parsed = Uuid::parse_str(&value).map_err(serde::de::Error::custom)?;
        if parsed.is_nil() || parsed.to_string() != value {
            return Err(serde::de::Error::custom(
                "operation ID must be a canonical lowercase non-nil UUID",
            ));
        }
        Ok(parsed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_decoders_accept_additive_fields_and_reject_invalid_required_fields() {
        let receipt: IndexDdlReceipt = sonic_rs::from_str(
            r#"{"kind":"accepted","operation_id":"018f0c58-6bc7-7c56-8d3d-9c5f18a0f001","index_id":"42","generation":"3","future":true}"#,
        )
        .unwrap();
        assert!(matches!(
            receipt,
            IndexDdlReceipt::Accepted { index_id: 42, .. }
        ));

        let status: IndexOperationStatus = sonic_rs::from_str(
            r#"{"status":"blocked","operation_id":"018f0c58-6bc7-7c56-8d3d-9c5f18a0f001","index_id":"42","generation":"3","operation_kind":"build","family":"secondary","stage":"scan","attempt":2,"progress":{"entities":"9","input_bytes":"10","output_operations":"11","output_bytes":"12","future":true},"blocker_code":"uniqueness_violation","future":true}"#,
        )
        .unwrap();
        assert!(matches!(status, IndexOperationStatus::Blocked { .. }));
        for (stage, expected) in [
            ("await_upload", IndexOperationStage::AwaitUpload),
            ("validate_manifests", IndexOperationStage::ValidateManifests),
        ] {
            let status: IndexOperationStatus = sonic_rs::from_str(&format!(
                r#"{{"status":"queued","operation_id":"018f0c58-6bc7-7c56-8d3d-9c5f18a0f001","index_id":"42","generation":"3","operation_kind":"build","family":"text","stage":"{stage}","attempt":0,"progress":{{"entities":"0","input_bytes":"0","output_operations":"0","output_bytes":"0"}}}}"#,
            ))
            .unwrap();
            let IndexOperationStatus::Queued { common } = status else {
                panic!("valid text build stage must decode as queued");
            };
            assert_eq!(common.stage, expected);
        }
        let aborted: IndexOperationStatus = sonic_rs::from_str(
            r#"{"status":"aborted","operation_id":"018f0c58-6bc7-7c56-8d3d-9c5f18a0f001","index_id":"42","generation":"3","operation_kind":"build","family":"secondary","stage":"aborting_finalize","attempt":2,"progress":{"entities":"9","input_bytes":"10","output_operations":"11","output_bytes":"12"}}"#,
        )
        .unwrap();
        assert!(matches!(aborted, IndexOperationStatus::Aborted { .. }));

        assert!(sonic_rs::from_str::<IndexDdlReceipt>(
            r#"{"kind":"accepted","operation_id":"018F0C58-6BC7-7C56-8D3D-9C5F18A0F001","index_id":"42","generation":"3"}"#,
        )
        .is_err());
        assert!(sonic_rs::from_str::<IndexDdlReceipt>(
            r#"{"kind":"already_active","index_id":"0","generation":"03"}"#,
        )
        .is_err());
        assert!(sonic_rs::from_str::<IndexOperationStatus>(r#"{"status":"future"}"#).is_err());
        assert!(sonic_rs::from_str::<IndexOperationStatus>(
            r#"{"status":"queued","operation_id":"018f0c58-6bc7-7c56-8d3d-9c5f18a0f001","index_id":"42","generation":"3","operation_kind":"build","family":"secondary","stage":"future","attempt":0,"progress":{"entities":"0","input_bytes":"0","output_operations":"0","output_bytes":"0"}}"#,
        )
        .is_err());
        assert!(sonic_rs::from_str::<IndexOperationStatus>(
            r#"{"status":"aborted","operation_id":"018f0c58-6bc7-7c56-8d3d-9c5f18a0f001","index_id":"42","generation":"3","operation_kind":"drop","family":"secondary","stage":"finalize","attempt":0,"progress":{"entities":"0","input_bytes":"0","output_operations":"0","output_bytes":"0"}}"#,
        )
        .is_err());
    }
}
