//! Exact build-operation ownership validation for text upload transitions.
//!
//! Upload reconciliation may mutate physical work only while the durable
//! intent still names the canonical constructing text build at the recorded
//! operation revision. Keeping that contract here prevents attachment and
//! reclaim paths from drifting into subtly different ownership checks.

use slatedb::DbTransaction;

use crate::encoding::v1::keys::tenant::DataScope;
use crate::error::{HelixDbError, Result};

use super::super::{outbox, work};
use super::super::{IndexOperationFamily, IndexOperationKind, IndexOperationRecord, IndexRecordV2};

/// Loads and proves the exact constructing text build named by an intent.
pub(super) async fn load_exact(
    transaction: &DbTransaction,
    scope: DataScope,
    intent: &work::TextUploadIntentValue,
) -> Result<(IndexRecordV2, IndexOperationRecord)> {
    let work::TextUploadOwner::Build {
        operation_id,
        expected_operation_revision,
    } = intent.owner
    else {
        return Err(corruption(
            "text build ownership validation received an active upload",
        ));
    };
    let Some((index, operation, _pointer)) =
        outbox::load_exact_link(transaction, scope, operation_id).await?
    else {
        return Err(corruption(
            "text upload build owner has no exact runnable operation",
        ));
    };
    if operation.operation_revision() != expected_operation_revision
        || operation.index_id() != intent.index_id
        || operation.identity() != &intent.identity
        || operation.generation() != intent.generation
        || operation.kind() != IndexOperationKind::Build
        || operation.family() != IndexOperationFamily::Text
        || !operation.progress().is_constructing_build()
        || index.index_id() != intent.index_id
        || index.identity() != &intent.identity
        || index.state().generation() != intent.generation
    {
        return Err(corruption(
            "text upload build owner no longer names its exact Building checkpoint",
        ));
    }
    Ok((index, operation))
}

fn corruption(reason: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(reason.into())
}
