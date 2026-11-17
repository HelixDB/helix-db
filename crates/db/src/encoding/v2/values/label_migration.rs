//! Resume points of the label identity migration.
//!
//! The copy of hash-keyed label rows to exact keys and the cleanup of the hash
//! rows after it each record where they stopped, so an interrupted pass
//! resumes instead of starting over. Both resume after a physical storage key.

use std::num::NonZeroU64;

use bytes::{BufMut, Bytes};

use crate::encoding::error::EncodingError;

const NEWEST_SEQ_LEN: usize = core::mem::size_of::<u64>();
const NEWEST_SEQ_OFFSET: usize = 0;
const ROWS_LEN: usize = core::mem::size_of::<u64>();
const ROWS_OFFSET: usize = NEWEST_SEQ_OFFSET + NEWEST_SEQ_LEN;
const RESUME_AFTER_OFFSET: usize = ROWS_OFFSET + ROWS_LEN;

/// Progress of an unpublished label identity copy.
///
/// ```text
/// Value: [newest_seq:8][rows:8][resume_after: the rest, a non-empty storage key]
/// ```
///
/// The copy committed every hash label row up to and including
/// `resume_after`: `rows` rows, the newest written at SlateDB sequence number
/// `newest_seq`. A later open resumes after `resume_after` only if those rows
/// are still the ones it copied: as many, and none written later.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LabelCopyProgressValue {
    newest_seq: u64,
    rows: NonZeroU64,
    resume_after: Bytes,
}

impl LabelCopyProgressValue {
    /// # Panics
    ///
    /// When `resume_after` is empty: no storage key is.
    pub(crate) fn new(newest_seq: u64, rows: NonZeroU64, resume_after: Bytes) -> Self {
        assert!(
            !resume_after.is_empty(),
            "a resume key is a non-empty storage key"
        );
        Self {
            newest_seq,
            rows,
            resume_after,
        }
    }

    /// The newest sequence number among the copied hash rows.
    pub(crate) const fn newest_seq(&self) -> u64 {
        self.newest_seq
    }

    /// How many hash rows the copy passed, up to and including
    /// [`Self::resume_after`].
    pub(crate) const fn rows(&self) -> NonZeroU64 {
        self.rows
    }

    /// The last hash row the copy committed.
    pub(crate) const fn resume_after(&self) -> &Bytes {
        &self.resume_after
    }

    pub(crate) fn encode(&self) -> Bytes {
        let mut buf = Vec::with_capacity(RESUME_AFTER_OFFSET + self.resume_after.len());
        buf.put_u64(self.newest_seq);
        buf.put_u64(self.rows.get());
        buf.put_slice(&self.resume_after);
        Bytes::from(buf)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, EncodingError> {
        if bytes.len() <= RESUME_AFTER_OFFSET {
            return Err(EncodingError::BufferTooShort {
                expected: RESUME_AFTER_OFFSET + 1,
                actual: bytes.len(),
            });
        }
        let read_u64 = |offset: usize, len: usize| {
            u64::from_be_bytes(
                bytes[offset..offset + len]
                    .try_into()
                    .expect("progress field slice is eight bytes"),
            )
        };
        let newest_seq = read_u64(NEWEST_SEQ_OFFSET, NEWEST_SEQ_LEN);
        let Some(rows) = NonZeroU64::new(read_u64(ROWS_OFFSET, ROWS_LEN)) else {
            return Err(EncodingError::Custom(
                "label copy progress counts no copied row".to_string(),
            ));
        };
        Ok(Self {
            newest_seq,
            rows,
            resume_after: Bytes::copy_from_slice(&bytes[RESUME_AFTER_OFFSET..]),
        })
    }
}

/// Progress of an unfinished hash label row cleanup.
///
/// ```text
/// Value: [resume_after: a non-empty storage key]
/// ```
///
/// `resume_after` is the last row the cleanup deleted, or the first label
/// row prefix of the scope a step stopped before.
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
        let key = Bytes::from_static(&[0x03, 0x04, 1, 2, 3, 4, 5, 6, 7, 8]);
        let progress =
            LabelCopyProgressValue::new(u64::MAX, NonZeroU64::new(9).unwrap(), key.clone());
        let mut expected = u64::MAX.to_be_bytes().to_vec();
        expected.extend_from_slice(&9u64.to_be_bytes());
        expected.extend_from_slice(&key);
        assert_eq!(progress.encode().as_ref(), expected.as_slice());
        let decoded = LabelCopyProgressValue::decode(&progress.encode()).unwrap();
        assert_eq!(decoded, progress);
        assert_eq!(
            (
                decoded.newest_seq(),
                decoded.rows().get(),
                decoded.resume_after()
            ),
            (u64::MAX, 9, &key)
        );
    }

    #[test]
    fn copy_progress_rejects_short_values_and_a_zero_count() {
        for short in [&[0; RESUME_AFTER_OFFSET - 1][..], &[0; RESUME_AFTER_OFFSET]] {
            assert!(matches!(
                LabelCopyProgressValue::decode(short),
                Err(EncodingError::BufferTooShort { .. })
            ));
        }
        let mut zero_rows = vec![0; RESUME_AFTER_OFFSET];
        zero_rows.push(0x03);
        assert!(matches!(
            LabelCopyProgressValue::decode(&zero_rows),
            Err(EncodingError::Custom(_))
        ));
    }

    #[test]
    #[should_panic(expected = "non-empty storage key")]
    fn copy_progress_rejects_an_empty_resume_key() {
        LabelCopyProgressValue::new(1, NonZeroU64::MIN, Bytes::new());
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
