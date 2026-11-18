//! Resume points and markers of the label identity migration.
//!
//! The copy of hash-keyed label rows to exact keys, the clearing of a copy
//! another writer made stale, and the cleanup of the hash rows after
//! publication each record where they stopped, so an interrupted pass resumes
//! instead of starting over. Each resumes after a physical storage key.

use bytes::{BufMut, Bytes};

use crate::encoding::error::EncodingError;

const PHASE_LEN: usize = core::mem::size_of::<u8>();
const COPYING: u8 = 0x00;
const CLEARING: u8 = 0x01;

/// Progress of an unpublished label identity copy.
///
/// ```text
/// Value: [phase:1][resume_after: the rest, a non-empty storage key]
/// ```
///
/// Each phase commits its progress with the rows it wrote, so a later open
/// resumes after `resume_after`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LabelCopyProgressValue {
    /// Every hash label row up to and including the key is copied.
    Copying(Bytes),
    /// The copy went stale and every exact label row up to and including the
    /// key is deleted; the copy starts over once every one is.
    Clearing(Bytes),
}

impl LabelCopyProgressValue {
    /// # Panics
    ///
    /// When `resume_after` is empty: no storage key is.
    pub(crate) fn copying(resume_after: Bytes) -> Self {
        assert!(
            !resume_after.is_empty(),
            "a resume key is a non-empty storage key"
        );
        Self::Copying(resume_after)
    }

    /// # Panics
    ///
    /// When `resume_after` is empty: no storage key is.
    pub(crate) fn clearing(resume_after: Bytes) -> Self {
        assert!(
            !resume_after.is_empty(),
            "a resume key is a non-empty storage key"
        );
        Self::Clearing(resume_after)
    }

    pub(crate) fn encode(&self) -> Bytes {
        let (phase, resume_after) = match self {
            Self::Copying(resume_after) => (COPYING, resume_after),
            Self::Clearing(resume_after) => (CLEARING, resume_after),
        };
        let mut buf = Vec::with_capacity(PHASE_LEN + resume_after.len());
        buf.put_u8(phase);
        buf.put_slice(resume_after);
        Bytes::from(buf)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, EncodingError> {
        let Some((&phase, resume_after)) = bytes.split_first() else {
            return Err(EncodingError::BufferTooShort {
                expected: PHASE_LEN + 1,
                actual: 0,
            });
        };
        if resume_after.is_empty() {
            return Err(EncodingError::BufferTooShort {
                expected: PHASE_LEN + 1,
                actual: bytes.len(),
            });
        }
        let resume_after = Bytes::copy_from_slice(resume_after);
        match phase {
            COPYING => Ok(Self::Copying(resume_after)),
            CLEARING => Ok(Self::Clearing(resume_after)),
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
        let key = Bytes::from_static(&[0x03, 0x04, 1, 2, 3, 4, 5, 6, 7, 8]);
        for (progress, phase) in [
            (LabelCopyProgressValue::copying(key.clone()), COPYING),
            (LabelCopyProgressValue::clearing(key.clone()), CLEARING),
        ] {
            let mut expected = vec![phase];
            expected.extend_from_slice(&key);
            assert_eq!(progress.encode().as_ref(), expected.as_slice());
            assert_eq!(
                LabelCopyProgressValue::decode(&progress.encode()).unwrap(),
                progress
            );
        }
    }

    #[test]
    fn copy_progress_rejects_short_values_and_unknown_phases() {
        for short in [&[][..], &[COPYING], &[CLEARING]] {
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
    fn copy_progress_rejects_an_empty_resume_key() {
        LabelCopyProgressValue::copying(Bytes::new());
    }

    #[test]
    #[should_panic(expected = "non-empty storage key")]
    fn clear_progress_rejects_an_empty_resume_key() {
        LabelCopyProgressValue::clearing(Bytes::new());
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
