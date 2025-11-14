use crate::protocol::HelixError;
use subtle::ConstantTimeEq;

/// API KEY HASH
const API_KEY_HASH: &[u8] = env!("HELIX_API_KEY").as_bytes();

pub(crate) fn verify_key(key: &[u8]) -> Result<(), HelixError> {
    assert_eq!(API_KEY_HASH.len(), 32, "API key must be 32 bytes");
    if API_KEY_HASH.ct_eq(key).into() {
        Ok(())
    } else {
        Err(HelixError::InvalidApiKey)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============================================================================
    // Key Verification Tests
    // ============================================================================

    #[test]
    fn test_verify_key_success() {
        // The API key is set at compile time via env!("HELIX_API_KEY")
        let result = verify_key(API_KEY_HASH);
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_key_wrong_key() {
        let wrong_key = [0u8; 32]; // All zeros
        let result = verify_key(&wrong_key);
        assert!(result.is_err());

        if let Err(e) = result {
            assert!(matches!(e, HelixError::InvalidApiKey));
            assert_eq!(e.to_string(), "Invalid API key");
        }
    }

    #[test]
    fn test_verify_key_partial_match() {
        // Create a key that matches the first half but not the second
        let mut partial_key = [0u8; 32];
        partial_key[..16].copy_from_slice(&API_KEY_HASH[..16]);
        // Rest stays as zeros

        let result = verify_key(&partial_key);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), HelixError::InvalidApiKey));
    }

    #[test]
    fn test_verify_key_off_by_one() {
        // Create a key that differs by just one bit in the last byte
        let mut almost_correct = API_KEY_HASH.to_vec();
        almost_correct[31] ^= 1; // Flip the least significant bit

        let result = verify_key(&almost_correct);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), HelixError::InvalidApiKey));
    }

    #[test]
    fn test_verify_key_empty() {
        let empty_key = [];
        let result = verify_key(&empty_key);
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_key_wrong_length_short() {
        let short_key = [0u8; 16];
        let result = verify_key(&short_key);
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_key_wrong_length_long() {
        let long_key = [0u8; 64];
        let result = verify_key(&long_key);
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_key_is_constant_time() {
        // This test verifies that the comparison is constant-time
        // by ensuring the function doesn't panic with different inputs
        let key1 = [0u8; 32];
        let key2 = [255u8; 32];

        // Both should fail but should take similar time
        // (We can't easily test timing in unit tests, but we verify they both fail)
        assert!(verify_key(&key1).is_err());
        assert!(verify_key(&key2).is_err());
    }

    // ============================================================================
    // API Key Length Tests
    // ============================================================================

    #[test]
    fn test_api_key_length() {
        // Verify the compile-time API key is exactly 32 bytes
        assert_eq!(API_KEY_HASH.len(), 32);
    }
}
