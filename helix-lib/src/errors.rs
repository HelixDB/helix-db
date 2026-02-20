/// Errors that can occur during database operations.
#[derive(Debug)]
pub enum HelixError {
    /// Database path is invalid or inaccessible
    InvalidPath(String),

    /// Database already exists at the specified path
    AlreadyExists(String),

    /// Database does not exist at the specified path
    NotFound(String),

    /// Storage engine error
    StorageError(String),

    /// I/O error
    IoError(std::io::Error),

    /// Error during deserialization
    DeserializationError(String),

    /// A required field was missing from a response or data structure
    MissingField(String),
}

pub type Result<T> = std::result::Result<T, HelixError>;

impl From<std::io::Error> for HelixError {
    fn from(err: std::io::Error) -> Self {
        HelixError::IoError(err)
    }
}

impl std::fmt::Display for HelixError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HelixError::InvalidPath(path) => write!(f, "Invalid database path: {}", path),
            HelixError::AlreadyExists(path) => write!(f, "Database already exists at: {}", path),
            HelixError::NotFound(path) => write!(f, "Database not found at: {}", path),
            HelixError::StorageError(msg) => write!(f, "Storage error: {}", msg),
            HelixError::IoError(err) => write!(f, "I/O error: {}", err),
            HelixError::DeserializationError(msg) => {
                write!(f, "Deserialization error: {}", msg)
            }
            HelixError::MissingField(field) => {
                write!(f, "Missing field in response: {}", field)
            }
        }
    }
}

impl std::error::Error for HelixError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            HelixError::IoError(err) => Some(err),
            _ => None,
        }
    }
}
