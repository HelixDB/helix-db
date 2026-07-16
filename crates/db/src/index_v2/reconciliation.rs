//! Writer-open structural reconciliation for global blob-GC work.
//!
//! This pass is intentionally capability-independent: persisted roots, marks,
//! and members must be structurally trustworthy before any text service is
//! allowed to execute them. It scans each global lane in bounded pages, rejects
//! orphans and key/value disagreement, and performs no physical object work.

use std::ops::Bound;

use bytes::Bytes;
use slatedb::{Db, DbReadOps};

use crate::encoding::v1::keys::index_v2::{
    BlobGcPass, GlobalIndexV2Key, GlobalIndexV2Kind, GLOBAL_INDEX_V2_SENTINEL,
};
use crate::encoding::v1::values::index_v2::{decode_work_value, IndexV2WorkValue};
use crate::error::{HelixDbError, Result};

use super::work::BlobGcEntryValue;
use super::BlobGcRunId;

const RECONCILIATION_PAGE_ROWS: usize = 64;
const GLOBAL_KIND_LEN: usize = core::mem::size_of::<u8>();
const GLOBAL_SUFFIX_OFFSET: usize = GLOBAL_INDEX_V2_SENTINEL.len() + GLOBAL_KIND_LEN;

/// Completes the fail-closed structural GC pass required before writer use.
pub(crate) async fn reconcile_blob_gc(db: &Db) -> Result<()> {
    let mut root_cursor = None;
    loop {
        let (rows, next, exhausted) =
            next_lane_page(db, GlobalIndexV2Kind::BlobGcRunRoot, root_cursor).await?;
        for (key, value) in rows {
            let GlobalIndexV2Key::BlobGcRunRoot(run_id) = GlobalIndexV2Key::parse_from_slice(&key)?
            else {
                return Err(corruption("GC-root lane yielded a different global key"));
            };
            let root = decode_root(&value)?;
            if root.run_id != run_id {
                return Err(corruption("GC root key and value disagree"));
            }
        }
        if exhausted {
            break;
        }
        root_cursor = Some(next.ok_or_else(|| {
            corruption("non-exhausted GC-root reconciliation page has no cursor")
        })?);
    }

    let mut mark_cursor = None;
    loop {
        let (rows, next, exhausted) =
            next_lane_page(db, GlobalIndexV2Kind::BlobGcReachabilityMark, mark_cursor).await?;
        for (key, value) in rows {
            let GlobalIndexV2Key::BlobGcReachabilityMark {
                run_id,
                pass,
                scan_attempt,
                blob_hash,
            } = GlobalIndexV2Key::parse_from_slice(&key)?
            else {
                return Err(corruption("GC-mark lane yielded a different global key"));
            };
            require_root(db, run_id).await?;
            let IndexV2WorkValue::BlobGcEntry(BlobGcEntryValue::ReachabilityMark(mark)) =
                decode_work_value(&value)?
            else {
                return Err(corruption("GC-mark key contains a different value kind"));
            };
            if mark.run_id != run_id
                || mark.first_pass != (pass == BlobGcPass::First)
                || mark.scan_attempt.get() != scan_attempt.get()
                || mark.blob_hash != blob_hash
            {
                return Err(corruption("GC mark key and value disagree"));
            }
        }
        if exhausted {
            break;
        }
        mark_cursor = Some(next.ok_or_else(|| {
            corruption("non-exhausted GC-mark reconciliation page has no cursor")
        })?);
    }

    let mut member_cursor = None;
    loop {
        let (rows, next, exhausted) =
            next_lane_page(db, GlobalIndexV2Kind::BlobGcCandidateMember, member_cursor).await?;
        for (key, value) in rows {
            let GlobalIndexV2Key::BlobGcCandidateMember { run_id, blob_hash } =
                GlobalIndexV2Key::parse_from_slice(&key)?
            else {
                return Err(corruption("GC-member lane yielded a different global key"));
            };
            require_root(db, run_id).await?;
            let IndexV2WorkValue::BlobGcEntry(BlobGcEntryValue::CandidateMember(member)) =
                decode_work_value(&value)?
            else {
                return Err(corruption("GC-member key contains a different value kind"));
            };
            if member.run_id != run_id || member.blob.hash() != blob_hash.as_bytes() {
                return Err(corruption("GC member key and value disagree"));
            }
        }
        if exhausted {
            return Ok(());
        }
        member_cursor = Some(next.ok_or_else(|| {
            corruption("non-exhausted GC-member reconciliation page has no cursor")
        })?);
    }
}

async fn next_lane_page(
    db: &Db,
    kind: GlobalIndexV2Kind,
    resume_after: Option<Bytes>,
) -> Result<(Vec<(Bytes, Bytes)>, Option<Bytes>, bool)> {
    let prefix = GlobalIndexV2Key::logical_prefix(kind);
    let start = resume_after.map_or(Bound::Unbounded, Bound::Excluded);
    let mut iterator = db
        .scan_prefix(&prefix, (start, Bound::<Bytes>::Unbounded))
        .await?;
    let mut rows = Vec::with_capacity(RECONCILIATION_PAGE_ROWS);
    let mut next = None;
    while rows.len() < RECONCILIATION_PAGE_ROWS {
        let Some(row) = iterator.next().await? else {
            break;
        };
        let suffix_len = row
            .key
            .len()
            .checked_sub(GLOBAL_SUFFIX_OFFSET)
            .ok_or_else(|| {
                corruption("global V2 reconciliation key is shorter than its typed prefix")
            })?;
        next = Some(Bytes::copy_from_slice(
            &row.key[GLOBAL_SUFFIX_OFFSET..GLOBAL_SUFFIX_OFFSET + suffix_len],
        ));
        rows.push((row.key, row.value));
    }
    let exhausted = rows.len() < RECONCILIATION_PAGE_ROWS;
    Ok((rows, next, exhausted))
}

async fn require_root(reader: &(impl DbReadOps + Sync), run_id: BlobGcRunId) -> Result<()> {
    let key = GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes();
    let Some(value) = reader.get(key).await? else {
        return Err(corruption("orphan GC mark/member has no run root"));
    };
    let root = decode_root(&value)?;
    if root.run_id != run_id {
        return Err(corruption("GC root lookup returned a different run"));
    }
    Ok(())
}

fn decode_root(value: &[u8]) -> Result<super::work::BlobGcRunRootValue> {
    let IndexV2WorkValue::BlobGcEntry(BlobGcEntryValue::RunRoot(root)) = decode_work_value(value)?
    else {
        return Err(corruption("GC-root key contains a different value kind"));
    };
    Ok(root)
}

fn corruption(reason: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(reason.into())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;

    use super::*;
    use crate::encoding::v1::keys::index_v2::BlobHash;
    use crate::encoding::v1::values::index_v2::{encode_work_value, IndexV2WorkValue};
    use crate::index_v2::work::{BlobGcCandidateMemberValue, BlobGcMemberState};
    use crate::index_v2::BlobRef;

    #[tokio::test]
    async fn orphan_candidate_member_fails_writer_reconciliation_closed() {
        let db = Db::builder("gc-orphan-member", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap();
        let run_id = BlobGcRunId::from_bytes([1; 16]).unwrap();
        let blob_hash = BlobHash::new([2; 32]);
        db.put(
            GlobalIndexV2Key::BlobGcCandidateMember { run_id, blob_hash }.to_bytes(),
            encode_work_value(&IndexV2WorkValue::BlobGcEntry(
                BlobGcEntryValue::CandidateMember(BlobGcCandidateMemberValue {
                    run_id,
                    blob: BlobRef::new([2; 32], 10),
                    state: BlobGcMemberState::PendingDisposition { owner_cursor: None },
                }),
            )),
        )
        .await
        .unwrap();

        let error = reconcile_blob_gc(&db)
            .await
            .expect_err("orphan member must fail closed");
        assert!(matches!(error, HelixDbError::IndexCatalogCorruption(_)));
        db.close().await.unwrap();
    }
}
