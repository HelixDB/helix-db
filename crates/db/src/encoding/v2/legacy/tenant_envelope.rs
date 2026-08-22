//! Markerless tenant envelope used before the current scoped-key marker.

#[cfg(any(test, feature = "production-coverage"))]
use bytes::{BufMut, Bytes};

use crate::encoding::v2::keys::scope::{TenantId, TENANT_ID_LEN};

/// Parsed `[tenant_id:16][logical_key]` envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LegacyTenantEnvelope<'a> {
    tenant: TenantId,
    logical_key: &'a [u8],
}

impl<'a> LegacyTenantEnvelope<'a> {
    /// Parses a structurally possible markerless tenant key.
    pub(crate) fn parse_candidate(key: &'a [u8]) -> Option<Self> {
        if key.len() <= TENANT_ID_LEN {
            return None;
        }
        let tenant = TenantId::from_u128(u128::from_be_bytes(
            key[..TENANT_ID_LEN]
                .try_into()
                .expect("validated legacy tenant ID is sixteen bytes"),
        ));
        Some(Self {
            tenant,
            logical_key: &key[TENANT_ID_LEN..],
        })
    }

    pub(crate) const fn tenant(self) -> TenantId {
        self.tenant
    }

    pub(crate) const fn logical_key(self) -> &'a [u8] {
        self.logical_key
    }
}

/// Encodes a markerless tenant envelope for migration fixtures.
#[cfg(any(test, feature = "production-coverage"))]
#[allow(dead_code)]
pub(crate) fn encode_for_contract(tenant: TenantId, logical_key: &[u8]) -> Bytes {
    assert!(
        !logical_key.is_empty(),
        "legacy logical key must not be empty"
    );
    let mut bytes = Vec::with_capacity(TENANT_ID_LEN + logical_key.len());
    bytes.put_u128(tenant.as_u128());
    bytes.put_slice(logical_key);
    Bytes::from(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markerless_tenant_bytes_are_frozen() {
        let tenant = TenantId::from_u128(0x0102030405060708090a0b0c0d0e0f10);
        let encoded = encode_for_contract(tenant, &[0xff, 0x01]);
        assert_eq!(
            encoded.as_ref(),
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0xff, 1,]
        );
        let decoded = LegacyTenantEnvelope::parse_candidate(&encoded).unwrap();
        assert_eq!(decoded.tenant(), tenant);
        assert_eq!(decoded.logical_key(), &[0xff, 1]);
        assert!(LegacyTenantEnvelope::parse_candidate(&encoded[..TENANT_ID_LEN]).is_none());
    }
}
