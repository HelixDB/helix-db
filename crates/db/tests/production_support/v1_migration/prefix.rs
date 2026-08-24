//! Production-linked lexicographic prefix-successor observation.

use bytes::Bytes;

/// Exact inclusion evidence for the deployed prefix-successor helper.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V1PrefixSuccessorObservation {
    /// Every required suffix remained before the exclusive end.
    pub included: Vec<Vec<u8>>,
    /// The first non-prefixed key was excluded.
    pub first_outside_excluded: bool,
    /// An all-`0xFF` prefix correctly has no finite successor.
    pub all_ff_is_unbounded: bool,
}

/// Projects the production prefix-successor helper over the E4 boundary set.
pub async fn v1_prefix_successor_contract() -> V1PrefixSuccessorObservation {
    let prefix = Bytes::from_static(&[0x03, 0x01, 0xAA]);
    let end = crate::encoding::v2::keys::indexes::prefix::exclusive_prefix_end_bound(&prefix)
        .expect("typed E4 prefix has a successor");
    let candidates = [
        vec![0x03, 0x01, 0xAA],
        vec![0x03, 0x01, 0xAA, 0xFE],
        vec![0x03, 0x01, 0xAA, 0xFF],
        vec![0x03, 0x01, 0xAA, 0xFF, 0x00],
        vec![0x03, 0x01, 0xAA, 0xFF, 0x7A, 0xFE],
    ];
    let included = candidates
        .into_iter()
        .filter(|key| key.starts_with(prefix.as_ref()) && key.as_slice() < end.as_ref())
        .collect();
    V1PrefixSuccessorObservation {
        included,
        first_outside_excluded: [0x03, 0x01, 0xAB].as_slice() >= end.as_ref(),
        all_ff_is_unbounded:
            crate::encoding::v2::keys::indexes::prefix::exclusive_prefix_end_bound(
                &Bytes::from_static(&[0xFF]),
            )
            .is_none(),
    }
}
