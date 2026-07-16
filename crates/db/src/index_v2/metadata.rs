//! Typed V2 storage marker, allocator, and global queue-pointer values.

use crate::encoding::v1::keys::tenant::DataScope;

use super::{
    IndexGenerationId, IndexId, IndexOperationRevision, TextIntentRevision, VectorPhysicalIndexId,
};

/// Canonical V2 index format number written by this implementation.
pub(crate) const CURRENT_INDEX_STORAGE_VERSION: u16 = 0x0002;

/// Decoded non-zero index storage format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct IndexStorageVersion(u16);

impl IndexStorageVersion {
    pub(crate) const CURRENT: Self = Self(CURRENT_INDEX_STORAGE_VERSION);

    pub(crate) fn new(value: u16) -> Result<Self, crate::encoding::error::EncodingError> {
        if value == 0 {
            return Err(crate::encoding::error::EncodingError::Custom(
                "index storage version must be non-zero".to_string(),
            ));
        }
        Ok(Self(value))
    }

    pub(crate) const fn get(self) -> u16 {
        self.0
    }
}

/// Typed next logical index ID watermark.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct LogicalIndexIdWatermark {
    pub(crate) next_id: IndexId,
}

/// Typed next vector physical ID watermark.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct VectorPhysicalIdWatermark {
    pub(crate) next_id: VectorPhysicalIndexId,
}

/// Global operation pointer value cross-checked with its scoped operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct OperationQueuePointerValue {
    pub(crate) scope: DataScope,
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) record_revision: IndexOperationRevision,
}

/// Global upload pointer value cross-checked with its scoped intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct UploadQueuePointerValue {
    pub(crate) scope: DataScope,
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) record_revision: TextIntentRevision,
}

/// Values used only under the nine-lane global V2 keyspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum IndexV2MetadataValue {
    StorageVersion(IndexStorageVersion),
    LogicalIndexIdWatermark(LogicalIndexIdWatermark),
    VectorPhysicalIdWatermark(VectorPhysicalIdWatermark),
    OperationQueuePointer(OperationQueuePointerValue),
    UploadQueuePointer(UploadQueuePointerValue),
}
