//! Resume points and markers of the label identity migration.
//!
//! The copy of hash-keyed label rows to exact keys, and the cleanup of the
//! hash rows after publication, each record where they stopped, so a later
//! step, of this writer or another, resumes instead of starting over. Each
//! resumes after a physical storage key.

use bytes::{BufMut, Bytes};

use crate::encoding::error::EncodingError;

const PHASE_LEN: usize = core::mem::size_of::<u8>();
const MEMBER_LEN: usize = core::mem::size_of::<u64>();
const ROWS: u8 = 0x00;
const MEMBERS: u8 = 0x01;

/// Progress of an unpublished label identity copy.
///
/// ```text
/// Rows:    [0x00][row: the rest, a non-empty storage key]
/// Members: [0x01][through:8 BE][row: the rest, a non-empty storage key]
/// ```
///
/// Each copy step commits its progress with the rows it wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LabelCopyProgressValue {
    /// Every hash label row up to and including `row` is copied.
    Rows { row: Bytes },
    /// Every hash label row before `row` is copied, and of `row`'s members
    /// those up to and including `through`: a row with more members than one
    /// step copies is copied a range of members at a time.
    Members { row: Bytes, through: u64 },
}

impl LabelCopyProgressValue {
    /// # Panics
    ///
    /// When `row` is empty: no storage key is.
    pub(crate) fn rows(row: Bytes) -> Self {
        assert!(!row.is_empty(), "a resume key is a non-empty storage key");
        Self::Rows { row }
    }

    /// # Panics
    ///
    /// When `row` is empty: no storage key is.
    pub(crate) fn members(row: Bytes, through: u64) -> Self {
        assert!(!row.is_empty(), "a resume key is a non-empty storage key");
        Self::Members { row, through }
    }

    pub(crate) fn encode(&self) -> Bytes {
        match self {
            Self::Rows { row } => {
                let mut buf = Vec::with_capacity(PHASE_LEN + row.len());
                buf.put_u8(ROWS);
                buf.put_slice(row);
                Bytes::from(buf)
            }
            Self::Members { row, through } => {
                let mut buf = Vec::with_capacity(PHASE_LEN + MEMBER_LEN + row.len());
                buf.put_u8(MEMBERS);
                buf.put_u64(*through);
                buf.put_slice(row);
                Bytes::from(buf)
            }
        }
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, EncodingError> {
        let Some((&phase, rest)) = bytes.split_first() else {
            return Err(EncodingError::BufferTooShort {
                expected: PHASE_LEN + 1,
                actual: 0,
            });
        };
        match phase {
            ROWS => {
                if rest.is_empty() {
                    return Err(EncodingError::BufferTooShort {
                        expected: PHASE_LEN + 1,
                        actual: bytes.len(),
                    });
                }
                Ok(Self::Rows {
                    row: Bytes::copy_from_slice(rest),
                })
            }
            MEMBERS => {
                if bytes.len() <= PHASE_LEN + MEMBER_LEN {
                    return Err(EncodingError::BufferTooShort {
                        expected: PHASE_LEN + MEMBER_LEN + 1,
                        actual: bytes.len(),
                    });
                }
                let through = u64::from_be_bytes(
                    bytes[PHASE_LEN..PHASE_LEN + MEMBER_LEN]
                        .try_into()
                        .expect("the member slice is eight bytes"),
                );
                Ok(Self::Members {
                    row: Bytes::copy_from_slice(&bytes[PHASE_LEN + MEMBER_LEN..]),
                    through,
                })
            }
            phase => Err(EncodingError::Custom(format!(
                "label copy progress has unknown phase {phase:#04x}"
            ))),
        }
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

/// The marker of a finished hash label row cleanup.
///
/// ```text
/// Value: [0x31] (ASCII "1")
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LabelCleanupReadyValue;

impl LabelCleanupReadyValue {
    const ENCODED: &'static [u8] = b"1";

    pub(crate) fn encode(self) -> Bytes {
        Bytes::from_static(Self::ENCODED)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, EncodingError> {
        if bytes != Self::ENCODED {
            return Err(EncodingError::Custom(format!(
                "label cleanup marker holds {} bytes other than \"1\"",
                bytes.len()
            )));
        }
        Ok(Self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_progress_round_trips_each_phase_in_its_exact_layout() {
        let row = Bytes::from_static(&[0x03, 0x04, 1, 2, 3, 4, 5, 6, 7, 8]);
        let rows = LabelCopyProgressValue::rows(row.clone());
        let mut expected = vec![ROWS];
        expected.extend_from_slice(&row);
        assert_eq!(rows.encode().as_ref(), expected.as_slice());

        let members = LabelCopyProgressValue::members(row.clone(), 0x0102_0304_0506_0708);
        let mut expected = vec![MEMBERS, 1, 2, 3, 4, 5, 6, 7, 8];
        expected.extend_from_slice(&row);
        assert_eq!(members.encode().as_ref(), expected.as_slice());

        for progress in [rows, members] {
            assert_eq!(
                LabelCopyProgressValue::decode(&progress.encode()).unwrap(),
                progress
            );
        }
    }

    #[test]
    fn copy_progress_rejects_short_values_and_unknown_phases() {
        for short in [
            &[][..],
            &[ROWS],
            &[MEMBERS],
            &[MEMBERS, 0, 0, 0, 0, 0, 0, 0, 1],
        ] {
            assert!(matches!(
                LabelCopyProgressValue::decode(short),
                Err(EncodingError::BufferTooShort { .. })
            ));
        }
        assert!(matches!(
            LabelCopyProgressValue::decode(&[0x02, 0x03]),
            Err(EncodingError::Custom(_))
        ));
    }

    #[test]
    #[should_panic(expected = "non-empty storage key")]
    fn row_progress_rejects_an_empty_row() {
        LabelCopyProgressValue::rows(Bytes::new());
    }

    #[test]
    #[should_panic(expected = "non-empty storage key")]
    fn member_progress_rejects_an_empty_row() {
        LabelCopyProgressValue::members(Bytes::new(), 1);
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

    #[test]
    fn cleanup_marker_is_exactly_one_ascii_one() {
        assert_eq!(LabelCleanupReadyValue.encode().as_ref(), b"1");
        assert_eq!(
            LabelCleanupReadyValue::decode(b"1").unwrap(),
            LabelCleanupReadyValue
        );
        for malformed in [&b""[..], b"0", b"11", b"true"] {
            assert!(matches!(
                LabelCleanupReadyValue::decode(malformed),
                Err(EncodingError::Custom(_))
            ));
        }
    }
}
