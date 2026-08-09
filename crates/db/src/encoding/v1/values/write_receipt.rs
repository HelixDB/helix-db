//! Versioned value encoding for atomic idempotent-write receipts.

use bytes::{BufMut, Bytes};

use crate::encoding::error::EncodingError;

const VERSION: u8 = 1;
const VERSION_LEN: usize = core::mem::size_of::<u8>();
const EXPIRY_LEN: usize = core::mem::size_of::<u64>();
const LEN_LEN: usize = core::mem::size_of::<u32>();
pub(crate) const REQUEST_HASH_LEN: usize = 32;
const HEADER_LEN: usize = VERSION_LEN + EXPIRY_LEN + REQUEST_HASH_LEN + LEN_LEN + LEN_LEN;

/// One committed write response bound to its canonical request hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WriteReceiptValue {
    expires_at_unix_ms: u64,
    request_hash: [u8; REQUEST_HASH_LEN],
    response: Bytes,
    diagnostics: Bytes,
}

impl WriteReceiptValue {
    pub(crate) fn new(
        expires_at_unix_ms: u64,
        request_hash: [u8; REQUEST_HASH_LEN],
        response: Bytes,
        diagnostics: Bytes,
    ) -> Result<Self, EncodingError> {
        u32::try_from(response.len()).map_err(|_| {
            EncodingError::Custom("write receipt response exceeds u32 length".to_string())
        })?;
        u32::try_from(diagnostics.len()).map_err(|_| {
            EncodingError::Custom("write receipt diagnostics exceed u32 length".to_string())
        })?;
        Ok(Self {
            expires_at_unix_ms,
            request_hash,
            response,
            diagnostics,
        })
    }

    pub(crate) const fn expires_at_unix_ms(&self) -> u64 {
        self.expires_at_unix_ms
    }

    pub(crate) const fn request_hash(&self) -> &[u8; REQUEST_HASH_LEN] {
        &self.request_hash
    }

    pub(crate) const fn response(&self) -> &Bytes {
        &self.response
    }

    pub(crate) const fn diagnostics(&self) -> &Bytes {
        &self.diagnostics
    }

    pub(crate) fn encode(&self) -> Bytes {
        let response_len = u32::try_from(self.response.len())
            .expect("validated write receipt response length fits u32");
        let diagnostics_len = u32::try_from(self.diagnostics.len())
            .expect("validated write receipt diagnostics length fits u32");
        let mut bytes =
            Vec::with_capacity(HEADER_LEN + self.response.len() + self.diagnostics.len());
        bytes.put_u8(VERSION);
        bytes.put_u64(self.expires_at_unix_ms);
        bytes.put_slice(&self.request_hash);
        bytes.put_u32(response_len);
        bytes.put_u32(diagnostics_len);
        bytes.put_slice(&self.response);
        bytes.put_slice(&self.diagnostics);
        Bytes::from(bytes)
    }

    pub(crate) fn decode(data: &[u8]) -> Result<Self, EncodingError> {
        if data.len() < HEADER_LEN {
            return Err(EncodingError::BufferTooShort {
                expected: HEADER_LEN,
                actual: data.len(),
            });
        }
        if data[0] != VERSION {
            return Err(EncodingError::InvalidEncodingType(data[0]));
        }

        let expires_at_unix_ms = u64::from_be_bytes(
            data[VERSION_LEN..VERSION_LEN + EXPIRY_LEN]
                .try_into()
                .expect("write receipt expiry slice is 8 bytes"),
        );
        let request_hash = data
            [VERSION_LEN + EXPIRY_LEN..VERSION_LEN + EXPIRY_LEN + REQUEST_HASH_LEN]
            .try_into()
            .expect("write receipt hash slice is 32 bytes");
        let response_len_offset = VERSION_LEN + EXPIRY_LEN + REQUEST_HASH_LEN;
        let response_len = u32::from_be_bytes(
            data[response_len_offset..response_len_offset + LEN_LEN]
                .try_into()
                .expect("write receipt response length slice is 4 bytes"),
        ) as usize;
        let diagnostics_len_offset = response_len_offset + LEN_LEN;
        let diagnostics_len = u32::from_be_bytes(
            data[diagnostics_len_offset..diagnostics_len_offset + LEN_LEN]
                .try_into()
                .expect("write receipt diagnostics length slice is 4 bytes"),
        ) as usize;
        let response_offset = HEADER_LEN;
        let diagnostics_offset = response_offset.checked_add(response_len).ok_or_else(|| {
            EncodingError::Custom("write receipt response length overflows usize".to_string())
        })?;
        let end = diagnostics_offset
            .checked_add(diagnostics_len)
            .ok_or_else(|| {
                EncodingError::Custom(
                    "write receipt diagnostics length overflows usize".to_string(),
                )
            })?;
        if data.len() != end {
            return Err(EncodingError::Custom(format!(
                "write receipt length mismatch: expected {end}, got {}",
                data.len()
            )));
        }

        Self::new(
            expires_at_unix_ms,
            request_hash,
            Bytes::copy_from_slice(&data[response_offset..response_offset + response_len]),
            Bytes::copy_from_slice(&data[diagnostics_offset..diagnostics_offset + diagnostics_len]),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_receipt_round_trips_exact_bytes() {
        let receipt = WriteReceiptValue::new(
            42,
            [7; REQUEST_HASH_LEN],
            Bytes::from_static(br#"{"created":1}"#),
            Bytes::from_static(br#"{"statistics":{}}"#),
        )
        .unwrap();
        assert_eq!(
            WriteReceiptValue::decode(&receipt.encode()).unwrap(),
            receipt
        );
    }

    #[test]
    fn write_receipt_rejects_unknown_version_and_truncated_payload() {
        assert!(matches!(
            WriteReceiptValue::decode(&[2; HEADER_LEN]),
            Err(EncodingError::InvalidEncodingType(2))
        ));

        let mut encoded = WriteReceiptValue::new(
            42,
            [7; REQUEST_HASH_LEN],
            Bytes::from_static(b"response"),
            Bytes::from_static(b"diagnostics"),
        )
        .unwrap()
        .encode()
        .to_vec();
        encoded.pop();
        assert!(matches!(
            WriteReceiptValue::decode(&encoded),
            Err(EncodingError::Custom(message)) if message.contains("length mismatch")
        ));
    }
}
