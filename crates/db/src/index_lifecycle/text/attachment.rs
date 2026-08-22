//! Shared typed helpers for text build and manifest metadata.

#[cfg(any(test, feature = "production-coverage"))]
use bytes::Bytes;

use crate::encoding::v2::keys as index_keys;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::ManagedIndexKey;
use crate::encoding::v2::values as index_values;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::work;

#[cfg(any(test, feature = "production-coverage"))]
pub(super) fn scoped_key(scope: DataScope, logical_key: index_keys::ScopedKey) -> Bytes {
    ManagedIndexKey::Data {
        scope,
        kind: logical_key,
    }
    .to_bytes()
}

/// Decodes and cross-checks one generation-owned text build artifact.
pub(super) fn decode_build_artifact(
    scope: DataScope,
    operation: &super::super::IndexOperationRecord,
    key: &[u8],
    value: &[u8],
) -> Result<(
    index_keys::TextBuildArtifactKey,
    work::TextBuildArtifactValue,
)> {
    let ManagedIndexKey::Data {
        kind: index_keys::ScopedKey::TextBuildArtifact(key),
        ..
    } = ManagedIndexKey::parse_from_slice(scope, key)?
    else {
        return Err(corruption(
            "text artifact prefix yielded another typed key kind",
        ));
    };
    let artifact = index_values::decode_build_artifact(value)?;
    if key.root.index_id != operation.index_id()
        || key.root.generation != operation.generation()
        || key.root.partition != artifact.partition.fingerprint()
        || key.ordinal != artifact.artifact_ordinal
        || artifact.index_id != operation.index_id()
        || artifact.generation != operation.generation()
    {
        return Err(corruption(
            "text build artifact key/value ownership mismatch",
        ));
    }
    Ok((key, artifact))
}

fn corruption(reason: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(reason.into())
}
