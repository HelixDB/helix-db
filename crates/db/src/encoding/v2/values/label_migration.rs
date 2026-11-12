//! Resume points of the label identity migration.
//!
//! The copy of hash-keyed label rows to exact keys and the cleanup of the hash
//! rows after it each record where they stopped, so an interrupted pass
//! resumes instead of starting over. Both resume after a physical storage key:
//! the last hash label row the pass committed.

use bytes::{BufMut, Bytes};

use crate::encoding::error::EncodingError;

const WRITER_EPOCH_LEN: usize = core::mem::size_of::<u64>();
const WRITER_EPOCH_OFFSET: usize = 0;

/// Progress of an unpublished label identity copy.
///
/// ```text
/// Value: [writer_epoch:8][resume_after: the rest, empty before any row]
/// ```
///
/// `writer_epoch` is the SlateDB writer epoch of the open that last advanced
/// the copy; a later open resumes only from its own predecessor's progress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LabelCopyProgressValue {
    writer_epoch: u64,
    resume_after: Option<Bytes>,
}

impl LabelCopyProgressValue {
    /// Progress of the open at `writer_epoch`, which copied every hash row up
    /// to and including `resume_after`, or none yet.
    ///
    /// # Panics
    ///
    /// When `resume_after` is empty: no storage key is.
    pub(crate) fn new(writer_epoch: u64, resume_after: Option<Bytes>) -> Self {
        assert!(
            resume_after.as_ref().is_none_or(|key| !key.is_empty()),
            "a resume key is a non-empty storage key"
        );
        Self {
            writer_epoch,
            resume_after,
        }
    }

    pub(crate) const fn writer_epoch(&self) -> u64 {
        self.writer_epoch
    }

    /// The last hash row the copy committed, or `None` before any.
    pub(crate) fn resume_after(&self) -> Option<&Bytes> {
        self.resume_after.as_ref()
    }

    pub(crate) fn encode(&self) -> Bytes {
        let resume_after = self.resume_after.as_deref().unwrap_or_default();
        let mut buf = Vec::with_capacity(WRITER_EPOCH_LEN + resume_after.len());
        buf.put_u64(self.writer_epoch);
        buf.put_slice(resume_after);
        Bytes::from(buf)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, EncodingError> {
        if bytes.len() < WRITER_EPOCH_LEN {
            return Err(EncodingError::BufferTooShort {
                expected: WRITER_EPOCH_LEN,
                actual: bytes.len(),
            });
        }
        let writer_epoch = u64::from_be_bytes(
            bytes[WRITER_EPOCH_OFFSET..WRITER_EPOCH_OFFSET + WRITER_EPOCH_LEN]
                .try_into()
                .expect("writer epoch slice is eight bytes"),
        );
        let resume_after = &bytes[WRITER_EPOCH_OFFSET + WRITER_EPOCH_LEN..];
        Ok(Self {
            writer_epoch,
            resume_after: (!resume_after.is_empty()).then(|| Bytes::copy_from_slice(resume_after)),
        })
    }
}

/// Progress of an unfinished hash label row cleanup: the last row it deleted.
///
/// ```text
/// Value: [resume_after: a non-empty storage key]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LabelCleanupProgressValue(Bytes);

impl LabelCleanupProgressValue {
    /// # Panics
    ///
    /// When `resume_after` is empty: no storage key is.
    pub(crate) fn new(resume_after: Bytes) -> Self {
        assert!(
            !resume_after.is_empty(),
            "a resume key is a non-empty storage key"
        );
        Self(resume_after)
    }

    pub(crate) fn resume_after(&self) -> &Bytes {
        &self.0
    }

    pub(crate) fn encode(&self) -> Bytes {
        self.0.clone()
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, EncodingError> {
        if bytes.is_empty() {
            return Err(EncodingError::BufferTooShort {
                expected: 1,
                actual: 0,
            });
        }
        Ok(Self(Bytes::copy_from_slice(bytes)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_progress_round_trips_its_exact_layout() {
        let before_any = LabelCopyProgressValue::new(7, None);
        assert_eq!(before_any.encode().as_ref(), &7u64.to_be_bytes());
        assert_eq!(
            LabelCopyProgressValue::decode(&before_any.encode()).unwrap(),
            before_any
        );
        assert_eq!(before_any.resume_after(), None);

        let key = Bytes::from_static(&[0x03, 0x04, 1, 2, 3, 4, 5, 6, 7, 8]);
        let after = LabelCopyProgressValue::new(u64::MAX, Some(key.clone()));
        let mut expected = u64::MAX.to_be_bytes().to_vec();
        expected.extend_from_slice(&key);
        assert_eq!(after.encode().as_ref(), expected.as_slice());
        let decoded = LabelCopyProgressValue::decode(&after.encode()).unwrap();
        assert_eq!(decoded, after);
        assert_eq!(
            (decoded.writer_epoch(), decoded.resume_after()),
            (u64::MAX, Some(&key))
        );

        assert!(matches!(
            LabelCopyProgressValue::decode(&[0; WRITER_EPOCH_LEN - 1]),
            Err(EncodingError::BufferTooShort { .. })
        ));
    }

    #[test]
    #[should_panic(expected = "non-empty storage key")]
    fn copy_progress_rejects_an_empty_resume_key() {
        LabelCopyProgressValue::new(1, Some(Bytes::new()));
    }

    #[test]
    fn cleanup_progress_round_trips_a_non_empty_key() {
        let key = Bytes::from_static(&[0x03, 0x10, 0x00]);
        let progress = LabelCleanupProgressValue::new(key.clone());
        assert_eq!(progress.encode(), key);
        assert_eq!(progress.resume_after(), &key);
        assert_eq!(
            LabelCleanupProgressValue::decode(&progress.encode()).unwrap(),
            progress
        );
        assert!(matches!(
            LabelCleanupProgressValue::decode(&[]),
            Err(EncodingError::BufferTooShort { .. })
        ));
    }

    #[test]
    #[should_panic(expected = "non-empty storage key")]
    fn cleanup_progress_rejects_an_empty_key() {
        LabelCleanupProgressValue::new(Bytes::new());
    }
}
