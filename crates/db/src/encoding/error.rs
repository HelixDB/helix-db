/// Error type for encoding/decoding operations
#[derive(Debug, thiserror::Error)]
pub enum EncodingError {
    /// Invalid tenant ID
    #[error("Invalid tenant ID: {0}")]
    InvalidTenantId(String),

    /// Buffer too short for expected data
    #[error("Buffer too short: expected at least {expected} bytes, got {actual}")]
    BufferTooShort { expected: usize, actual: usize },

    /// Invalid encoding type byte
    #[error("Invalid encoding type: {0:#04x}")]
    InvalidEncodingType(u8),

    /// IO error during encoding/decoding
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Invalid UTF-8 in property data
    #[error("Invalid UTF-8 in property: {0}")]
    InvalidUtf8(#[from] std::str::Utf8Error),

    /// Invalid key prefix
    #[error("Invalid key prefix: {0}")]
    InvalidKeyPrefix(u8),

    /// Invalid index prefix
    #[error("Invalid index prefix: {0}")]
    InvalidIndexPrefix(u8),

    /// Invalid key
    #[error("Invalid key: {0}")]
    InvalidKey(String),

    /// Invalid index key
    #[error("Invalid index key: {0}")]
    InvalidIndexKey(String),

    /// A typed key referenced a value from another persisted value family.
    #[error("Unexpected V2 value kind: expected {expected:#04x}, got {actual:#04x}")]
    UnexpectedValueKind { expected: u8, actual: u8 },

    /// Persisted equality digest does not match its canonical value.
    #[error("Canonical equality digest does not match canonical bytes")]
    CanonicalEqualityDigestMismatch,

    /// Persisted graph-label digest does not match its canonical UTF-8 bytes.
    #[error("Canonical label digest does not match canonical bytes")]
    CanonicalLabelDigestMismatch,

    /// Invalid range index direction
    #[error("Invalid range index direction: {0}")]
    InvalidRangeIndexDirection(u8),
    /// Invalid edge equality direction
    #[error("Invalid edge equality direction: {0}")]
    InvalidEdgeIndexDirection(u8),

    /// rkyv serialization/deserialization error
    #[error("Property serialization error: {0}")]
    Rkyv(String),

    /// Custom error message
    #[error("{0}")]
    Custom(String),
}
