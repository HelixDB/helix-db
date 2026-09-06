//! Pre-V6 range identities. Only writer migration may interpret an undirected
//! identity using the direction in its canonical definition.

use crate::index_lifecycle::{IndexIdentity, IndexIdentityFamily};

/// Projects a current identity onto its pre-V6 catalog key.
pub(crate) fn undirected(identity: &IndexIdentity) -> IndexIdentity {
    let family = match identity.family() {
        IndexIdentityFamily::SecondaryRangeDescending => {
            IndexIdentityFamily::SecondaryRangeAscending
        }
        family => family,
    };
    IndexIdentity::new(
        family,
        identity.element_kind(),
        identity.label().clone(),
        identity.property().clone(),
    )
}
