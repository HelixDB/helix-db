//! Request-scoped storage tenancy.
//!
//! Tenant identity is a trusted transport concern. The DB receives a typed
//! scope and applies it only at the physical storage-key boundary, so legacy
//! callers remain explicitly unscoped while tenant callers use an isolated key
//! namespace.

use crate::encoding::error::EncodingError;

/// Storage tenant identifier encoded into physical keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TenantId(u128);

impl TenantId {
    /// Construct from the canonical integer stored in typed scoped records.
    pub(crate) const fn from_u128(value: u128) -> Self {
        Self(value)
    }

    /// Decode a canonical ULID string into the 128-bit tenant-key prefix.
    pub fn from_ulid_str(value: &str) -> Result<Self, EncodingError> {
        const ULID_LEN: usize = 26;

        if value.len() != ULID_LEN {
            return Err(EncodingError::InvalidTenantId(format!(
                "expected {ULID_LEN} ULID characters, got {}",
                value.len()
            )));
        }

        let mut decoded = 0u128;
        for (index, byte) in value.bytes().enumerate() {
            let Some(part) = decode_crockford_base32(byte) else {
                return Err(EncodingError::InvalidTenantId(format!(
                    "invalid ULID character `{}` at byte offset {index}",
                    char::from(byte)
                )));
            };
            if index == 0 {
                if part > 0x07 {
                    return Err(EncodingError::InvalidTenantId(
                        "ULID overflows 128 bits".to_string(),
                    ));
                }
                decoded = u128::from(part);
            } else {
                decoded = (decoded << 5) | u128::from(part);
            }
        }

        Ok(Self(decoded))
    }

    /// Raw big-endian integer used by the storage key prefix.
    pub const fn as_u128(self) -> u128 {
        self.0
    }
}

/// Request data-storage namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DataScope {
    /// Legacy storage namespace.
    LegacyUnscoped,
    /// Tenant-isolated storage namespace.
    Tenant(TenantId),
}

impl DataScope {
    pub(crate) const PREFIX_LEN: usize = core::mem::size_of::<u128>();

    /// Return true if this request uses the legacy namespace.
    pub const fn is_unscoped(self) -> bool {
        matches!(self, Self::LegacyUnscoped)
    }

    pub(crate) const fn encoded_len(self) -> usize {
        match self {
            Self::LegacyUnscoped => 0,
            Self::Tenant(_) => Self::PREFIX_LEN,
        }
    }

    /// Strip this tenant scope from a physical key returned by storage scans.
    pub fn strip_key(self, key: &[u8]) -> Option<&[u8]> {
        match self {
            Self::LegacyUnscoped => Some(key),
            Self::Tenant(tenant_id) => {
                let prefix = tenant_id.as_u128().to_be_bytes();
                if key.len() < Self::PREFIX_LEN {
                    return None;
                }
                (key[0..Self::PREFIX_LEN] == prefix).then_some(&key[Self::PREFIX_LEN..])
            }
        }
    }

    /// Applies the request scope to a logical search-index physical name.
    ///
    /// Legacy storage retains the deployed name unchanged. Tenant storage uses
    /// the same fixed-width outer prefix for text and vector builders, lookup,
    /// cleanup, and lifecycle recovery, preventing those call sites from
    /// independently formatting divergent physical identities.
    pub(crate) fn physical_index_name(self, logical_name: &str) -> String {
        match self {
            Self::LegacyUnscoped => logical_name.to_string(),
            Self::Tenant(tenant_id) => {
                format!("tenant:{:032x}:{logical_name}", tenant_id.as_u128())
            }
        }
    }
}

fn decode_crockford_base32(byte: u8) -> Option<u8> {
    match byte {
        b'0' => Some(0),
        b'1' | b'I' | b'i' | b'L' | b'l' => Some(1),
        b'2' => Some(2),
        b'3' => Some(3),
        b'4' => Some(4),
        b'5' => Some(5),
        b'6' => Some(6),
        b'7' => Some(7),
        b'8' => Some(8),
        b'9' => Some(9),
        b'A' | b'a' => Some(10),
        b'B' | b'b' => Some(11),
        b'C' | b'c' => Some(12),
        b'D' | b'd' => Some(13),
        b'E' | b'e' => Some(14),
        b'F' | b'f' => Some(15),
        b'G' | b'g' => Some(16),
        b'H' | b'h' => Some(17),
        b'J' | b'j' => Some(18),
        b'K' | b'k' => Some(19),
        b'M' | b'm' => Some(20),
        b'N' | b'n' => Some(21),
        b'P' | b'p' => Some(22),
        b'Q' | b'q' => Some(23),
        b'R' | b'r' => Some(24),
        b'S' | b's' => Some(25),
        b'T' | b't' => Some(26),
        b'V' | b'v' => Some(27),
        b'W' | b'w' => Some(28),
        b'X' | b'x' => Some(29),
        b'Y' | b'y' => Some(30),
        b'Z' | b'z' => Some(31),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ulid_decodes_to_expected_u128() {
        let tenant =
            TenantId::from_ulid_str("00000000000000000000000001").expect("valid minimal ulid");

        assert_eq!(tenant.as_u128(), 1);
    }

    #[test]
    fn ulid_rejects_overflowing_first_character() {
        let err = TenantId::from_ulid_str("80000000000000000000000000")
            .expect_err("first ULID char cannot exceed 7");

        assert!(err.to_string().contains("overflows"));
    }

    #[test]
    fn ulid_rejects_wrong_lengths_and_invalid_characters() {
        let short = TenantId::from_ulid_str("0000000000000000000000000")
            .expect_err("short ULID is rejected");
        assert!(short.to_string().contains("expected 26 ULID characters"));

        let long = TenantId::from_ulid_str("000000000000000000000000000")
            .expect_err("long ULID is rejected");
        assert!(long.to_string().contains("got 27"));

        let invalid = TenantId::from_ulid_str("0000000000000000000000000O")
            .expect_err("invalid Crockford character is rejected");
        assert!(invalid.to_string().contains("invalid ULID character `O`"));
    }

    #[test]
    fn ulid_accepts_lowercase_and_crockford_aliases() {
        let tenant =
            TenantId::from_ulid_str("000000000000000000000000il").expect("aliases are valid");

        assert_eq!(tenant.as_u128(), 33);
    }

    #[test]
    fn ulid_crockford_alphabet_decodes_all_value_bits() {
        for (character, value) in [
            ('0', 0),
            ('1', 1),
            ('2', 2),
            ('3', 3),
            ('4', 4),
            ('5', 5),
            ('6', 6),
            ('7', 7),
            ('8', 8),
            ('9', 9),
            ('A', 10),
            ('B', 11),
            ('C', 12),
            ('D', 13),
            ('E', 14),
            ('F', 15),
            ('G', 16),
            ('H', 17),
            ('J', 18),
            ('K', 19),
            ('M', 20),
            ('N', 21),
            ('P', 22),
            ('Q', 23),
            ('R', 24),
            ('S', 25),
            ('T', 26),
            ('V', 27),
            ('W', 28),
            ('X', 29),
            ('Y', 30),
            ('Z', 31),
            ('a', 10),
            ('z', 31),
            ('I', 1),
            ('L', 1),
        ] {
            let ulid = format!("0000000000000000000000000{character}");
            let tenant = TenantId::from_ulid_str(&ulid).expect("valid Crockford character");
            assert_eq!(tenant.as_u128(), value);
        }
    }

    #[test]
    fn tenant_scope_prefixes_and_strips_keys() {
        let tenant = TenantId::from_ulid_str("0000000000000000000000000A").expect("valid tenant");
        let scope = DataScope::Tenant(tenant);
        let logical = b"\x02logical";
        let physical = {
            let mut bytes = tenant.as_u128().to_be_bytes().to_vec();
            bytes.extend_from_slice(logical);
            bytes
        };

        assert_eq!(physical.len(), 16 + logical.len());
        assert_eq!(scope.strip_key(&physical), Some(logical.as_ref()));
        assert_eq!(
            DataScope::LegacyUnscoped.strip_key(logical),
            Some(logical.as_ref())
        );
    }

    #[test]
    fn tenant_scope_rejects_short_or_wrong_prefixes() {
        let tenant = TenantId::from_ulid_str("0000000000000000000000000A").expect("valid tenant");
        let other = TenantId::from_ulid_str("0000000000000000000000000B").expect("valid tenant");
        let scope = DataScope::Tenant(tenant);
        let mut wrong_prefix = other.as_u128().to_be_bytes().to_vec();
        wrong_prefix.extend_from_slice(b"logical");

        assert!(DataScope::LegacyUnscoped.is_unscoped());
        assert_eq!(DataScope::LegacyUnscoped.encoded_len(), 0);
        assert_eq!(scope.encoded_len(), DataScope::PREFIX_LEN);
        assert_eq!(scope.strip_key(b"short"), None);
        assert_eq!(scope.strip_key(&wrong_prefix), None);
    }

    #[test]
    fn physical_index_names_preserve_legacy_and_prefix_tenant_identity() {
        let tenant = TenantId::from_u128(0x2a);
        assert_eq!(
            DataScope::LegacyUnscoped.physical_index_name("fts:n:logical"),
            "fts:n:logical"
        );
        assert_eq!(
            DataScope::Tenant(tenant).physical_index_name("fts:n:logical"),
            "tenant:0000000000000000000000000000002a:fts:n:logical"
        );
    }
}
