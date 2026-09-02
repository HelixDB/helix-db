//! Canonical graph-label identity.
//!
//! The digest is only a scan accelerator. Exact identity is always the
//! length-delimited UTF-8 label. Hash-only label keys are unrepresentable.

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use bytes::{BufMut, Bytes};
use sha2::{Digest, Sha256};

use crate::encoding::error::EncodingError;

pub(crate) const LABEL_DIGEST_LEN: usize = core::mem::size_of::<u64>();
pub(crate) const LABEL_LEN_LEN: usize = core::mem::size_of::<u32>();
pub(crate) const MAX_LABEL_CANONICAL_LEN: usize = 1024 * 1024 - 64;

/// Exact graph-label bytes and their bounded scan digest.
#[derive(Debug, Clone, Eq)]
pub(crate) struct CanonicalLabel {
    digest: [u8; LABEL_DIGEST_LEN],
    canonical: Bytes,
}

impl PartialEq for CanonicalLabel {
    fn eq(&self, other: &Self) -> bool {
        self.canonical == other.canonical
    }
}

impl Hash for CanonicalLabel {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.canonical.hash(state);
    }
}

impl PartialOrd for CanonicalLabel {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CanonicalLabel {
    fn cmp(&self, other: &Self) -> Ordering {
        self.canonical.cmp(&other.canonical)
    }
}

impl CanonicalLabel {
    fn new(canonical: Bytes) -> Self {
        let digest = label_digest(&canonical);
        Self { digest, canonical }
    }

    /// Constructs a label from its UTF-8 graph name.
    pub(crate) fn from_label(label: &str) -> Result<Self, EncodingError> {
        if label.len() > MAX_LABEL_CANONICAL_LEN {
            return Err(EncodingError::InvalidKey(format!(
                "canonical label exceeds {MAX_LABEL_CANONICAL_LEN} bytes"
            )));
        }
        Ok(Self::new(Bytes::copy_from_slice(label.as_bytes())))
    }

    /// Reconstructs a persisted label while validating its canonical frame.
    pub(crate) fn try_from_parts(
        digest: [u8; LABEL_DIGEST_LEN],
        canonical: Bytes,
    ) -> Result<Self, EncodingError> {
        if canonical.len() > MAX_LABEL_CANONICAL_LEN {
            return Err(EncodingError::InvalidKey(format!(
                "canonical label exceeds {MAX_LABEL_CANONICAL_LEN} bytes"
            )));
        }
        std::str::from_utf8(&canonical)?;
        if digest != label_digest(&canonical) {
            return Err(EncodingError::CanonicalLabelDigestMismatch);
        }
        Ok(Self { digest, canonical })
    }

    pub(crate) const fn digest(&self) -> &[u8; LABEL_DIGEST_LEN] {
        &self.digest
    }

    #[cfg(test)]
    pub(crate) fn as_str(&self) -> &str {
        std::str::from_utf8(&self.canonical).expect("canonical labels are validated UTF-8")
    }

    pub(crate) fn encoded_len(&self) -> usize {
        LABEL_DIGEST_LEN + LABEL_LEN_LEN + self.canonical.len()
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_slice(self.digest());
        buf.put_u32(
            u32::try_from(self.canonical.len()).expect("canonical labels are bounded below u32"),
        );
        buf.put_slice(&self.canonical);
    }

    /// Parses a complete canonical-label frame. Trailing or truncated bytes fail closed.
    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        let header = LABEL_DIGEST_LEN + LABEL_LEN_LEN;
        if slice.len() < header {
            return Err(EncodingError::BufferTooShort {
                expected: header,
                actual: slice.len(),
            });
        }
        let digest = slice[0..LABEL_DIGEST_LEN]
            .try_into()
            .expect("label digest slice is 8 bytes");
        let canonical_len = u32::from_be_bytes(
            slice[LABEL_DIGEST_LEN..LABEL_DIGEST_LEN + LABEL_LEN_LEN]
                .try_into()
                .expect("label length slice is 4 bytes"),
        ) as usize;
        let expected = header + canonical_len;
        match slice.len().cmp(&expected) {
            Ordering::Less => {
                return Err(EncodingError::BufferTooShort {
                    expected,
                    actual: slice.len(),
                });
            }
            Ordering::Equal => {}
            Ordering::Greater => {
                return Err(EncodingError::InvalidIndexKey(format!(
                    "expected {expected} bytes, got {}",
                    slice.len()
                )));
            }
        }
        Self::try_from_parts(
            digest,
            Bytes::copy_from_slice(
                &slice[LABEL_DIGEST_LEN + LABEL_LEN_LEN
                    ..LABEL_DIGEST_LEN + LABEL_LEN_LEN + canonical_len],
            ),
        )
    }

    /// Constructs an in-memory digest-collision fixture that must not be persisted.
    #[cfg(test)]
    pub(crate) fn with_test_digest_unchecked(label: &str, digest: [u8; LABEL_DIGEST_LEN]) -> Self {
        Self {
            digest,
            canonical: Bytes::copy_from_slice(label.as_bytes()),
        }
    }
}

fn label_digest(canonical: &[u8]) -> [u8; LABEL_DIGEST_LEN] {
    let hash = Sha256::digest(canonical);
    hash[..LABEL_DIGEST_LEN]
        .try_into()
        .expect("SHA-256 contains an eight-byte digest prefix")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_labels_are_exact_utf8_identity() {
        let user = CanonicalLabel::from_label("User").unwrap();
        assert_eq!(user.as_str(), "User");
        assert_eq!(
            CanonicalLabel::try_from_parts(
                *user.digest(),
                Bytes::copy_from_slice(user.canonical.as_ref())
            )
            .unwrap(),
            user
        );
        assert_ne!(
            CanonicalLabel::from_label("User").unwrap(),
            CanonicalLabel::from_label("user").unwrap()
        );
    }

    #[test]
    fn persisted_canonical_digest_mismatches_fail_closed() {
        let value = CanonicalLabel::from_label("User").unwrap();
        let mut mismatched = *value.digest();
        mismatched[0] ^= 0xFF;
        assert!(matches!(
            CanonicalLabel::try_from_parts(
                mismatched,
                Bytes::copy_from_slice(value.canonical.as_ref())
            ),
            Err(EncodingError::CanonicalLabelDigestMismatch)
        ));
    }

    #[test]
    fn digest_collisions_retain_distinct_exact_canonical_identity() {
        let digest = [0xA5; LABEL_DIGEST_LEN];
        let first = CanonicalLabel::with_test_digest_unchecked("alpha", digest);
        let second = CanonicalLabel::with_test_digest_unchecked("beta", digest);
        assert_eq!(first.digest(), second.digest());
        assert_ne!(first, second);
    }

    #[test]
    fn parse_rejects_invalid_utf8_and_old_hash_only_frames() {
        assert!(CanonicalLabel::parse_from_slice(&[1, 2, 3]).is_err());
        let mut invalid_utf8 = Vec::new();
        invalid_utf8.extend_from_slice(&label_digest(&[0xFF]));
        invalid_utf8.extend_from_slice(&1u32.to_be_bytes());
        invalid_utf8.push(0xFF);
        assert!(CanonicalLabel::parse_from_slice(&invalid_utf8).is_err());
    }
}
