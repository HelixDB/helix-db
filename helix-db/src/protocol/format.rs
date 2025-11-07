use std::fmt::Display;
use std::{borrow::Cow, error::Error, ops::Deref, str::FromStr};

use serde::{Deserialize, Serialize};
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;

use crate::helix_engine::types::GraphError;
use crate::protocol::Response;

/// This enum represents the formats that input or output values of HelixDB can be represented as
/// It also includes tooling to facilitate copy or zero-copy formats
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub enum Format {
    /// JSON (JavaScript Object Notation)
    /// The current implementation uses sonic_rs
    #[default]
    Json,
}

/// Methods using to format for serialization/deserialization
impl Format {
    /// Serialize the value to bytes.
    /// If using a zero-copy format it will return a Cow::Borrowed, with a lifetime corresponding to the value.
    /// Otherwise, it returns a Cow::Owned.
    ///
    /// # Panics
    /// This method will panic if serialization fails. Ensure that the value being serialized
    /// is compatible with the chosen format to avoid panics.
    pub fn serialize<T: Serialize>(self, val: &T) -> Cow<'_, [u8]> {
        match self {
            Format::Json => sonic_rs::to_vec(val).unwrap().into(),
        }
    }

    /// Serialize the value to the supplied async writer.
    /// This will use an underlying async implementation if possible, otherwise it will buffer it
    pub async fn serialize_to_async<T: Serialize>(
        self,
        val: &T,
        writer: &mut BufWriter<impl AsyncWrite + Unpin>,
    ) -> Result<(), Box<dyn Error>> {
        match self {
            Format::Json => {
                let encoded = sonic_rs::to_vec(val)?;
                writer.write_all(&encoded).await?;
            }
        }
        Ok(())
    }

    pub fn create_response<T: Serialize>(self, val: &T) -> Response {
        Response {
            body: self.serialize(val).to_vec(),
            fmt: self,
        }
    }

    /// Deserialize the provided value
    /// Returns a MaybeOwned::Borrowed if using a zero-copy format
    /// or a MaybeOwned::Owned otherwise
    pub fn deserialize<'a, T: Deserialize<'a>>(
        self,
        val: &'a [u8],
    ) -> Result<MaybeOwned<'a, T>, GraphError> {
        match self {
            Format::Json => Ok(MaybeOwned::Owned(
                sonic_rs::from_slice::<T>(val)
                    .map_err(|e| GraphError::DecodeError(e.to_string()))?,
            )),
        }
    }

    /// Deserialize the provided value
    pub fn deserialize_owned<'a, T: Deserialize<'a>>(self, val: &'a [u8]) -> Result<T, GraphError> {
        match self {
            Format::Json => Ok(sonic_rs::from_slice::<T>(val)
                .map_err(|e| GraphError::DecodeError(e.to_string()))?),
        }
    }
}

impl FromStr for Format {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "application/json" => Ok(Format::Json),
            _ => Err(()),
        }
    }
}

impl Display for Format {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Format::Json => write!(f, "application/json"),
        }
    }
}

/// A wrapper for a value which might be owned or borrowed
/// The key difference from Cow, is that this doesn't require the value to implement Clone
pub enum MaybeOwned<'a, T> {
    Owned(T),
    Borrowed(&'a T),
}

impl<'a, T> Deref for MaybeOwned<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            MaybeOwned::Owned(v) => v,
            MaybeOwned::Borrowed(v) => v,
        }
    }
}

impl<'a, T: Clone> MaybeOwned<'a, T> {
    pub fn into_owned(self) -> T {
        match self {
            MaybeOwned::Owned(v) => v,
            MaybeOwned::Borrowed(v) => v.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
    struct TestData {
        name: String,
        value: i32,
    }

    // ============================================================================
    // Format::serialize and deserialize tests
    // ============================================================================

    #[test]
    fn test_format_serialize_json() {
        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        let bytes = Format::Json.serialize(&data);
        assert!(!bytes.is_empty());

        // Verify it's valid JSON
        let json_str = std::str::from_utf8(&bytes).unwrap();
        assert!(json_str.contains("test"));
        assert!(json_str.contains("42"));
    }

    #[test]
    fn test_format_deserialize_json() {
        let json = r#"{"name":"test","value":42}"#;
        let bytes = json.as_bytes();

        let result: Result<MaybeOwned<TestData>, GraphError> = Format::Json.deserialize(bytes);
        assert!(result.is_ok());

        let data = result.unwrap();
        assert_eq!(data.name, "test");
        assert_eq!(data.value, 42);
    }

    #[test]
    fn test_format_serialize_deserialize_roundtrip() {
        let original = TestData {
            name: "roundtrip".to_string(),
            value: 123,
        };

        let bytes = Format::Json.serialize(&original);
        let result: MaybeOwned<TestData> = Format::Json.deserialize(&bytes).unwrap();

        assert_eq!(*result, original);
    }

    #[test]
    fn test_format_deserialize_owned() {
        let json = r#"{"name":"owned","value":99}"#;
        let bytes = json.as_bytes();

        let result: Result<TestData, GraphError> = Format::Json.deserialize_owned(bytes);
        assert!(result.is_ok());

        let data = result.unwrap();
        assert_eq!(data.name, "owned");
        assert_eq!(data.value, 99);
    }

    #[test]
    fn test_format_deserialize_invalid_json() {
        let invalid_json = b"not valid json {";

        let result: Result<MaybeOwned<TestData>, GraphError> = Format::Json.deserialize(invalid_json);
        assert!(result.is_err());

        if let Err(GraphError::DecodeError(msg)) = result {
            assert!(!msg.is_empty());
        } else {
            panic!("Expected DecodeError");
        }
    }

    #[test]
    fn test_format_deserialize_owned_invalid_json() {
        let invalid_json = b"{ invalid }";

        let result: Result<TestData, GraphError> = Format::Json.deserialize_owned(invalid_json);
        assert!(result.is_err());
    }

    #[test]
    fn test_format_create_response() {
        let data = TestData {
            name: "response".to_string(),
            value: 77,
        };

        let response = Format::Json.create_response(&data);

        assert!(!response.body.is_empty());
        assert!(matches!(response.fmt, Format::Json));

        // Verify we can deserialize the response body
        let decoded: TestData = Format::Json.deserialize_owned(&response.body).unwrap();
        assert_eq!(decoded, data);
    }

    // ============================================================================
    // Format::FromStr and Display tests
    // ============================================================================

    #[test]
    fn test_format_from_str_json() {
        let result = "application/json".parse::<Format>();
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), Format::Json));
    }

    #[test]
    fn test_format_from_str_invalid() {
        let result = "application/xml".parse::<Format>();
        assert!(result.is_err());

        let result = "text/plain".parse::<Format>();
        assert!(result.is_err());

        let result = "invalid".parse::<Format>();
        assert!(result.is_err());
    }

    #[test]
    fn test_format_display() {
        let fmt = Format::Json;
        assert_eq!(fmt.to_string(), "application/json");
    }

    #[test]
    fn test_format_default() {
        let fmt = Format::default();
        assert!(matches!(fmt, Format::Json));
    }

    // ============================================================================
    // MaybeOwned tests
    // ============================================================================

    #[test]
    fn test_maybe_owned_deref_owned() {
        let data = TestData {
            name: "owned".to_string(),
            value: 100,
        };

        let maybe_owned = MaybeOwned::Owned(data.clone());
        assert_eq!(maybe_owned.name, "owned");
        assert_eq!(maybe_owned.value, 100);
    }

    #[test]
    fn test_maybe_owned_deref_borrowed() {
        let data = TestData {
            name: "borrowed".to_string(),
            value: 200,
        };

        let maybe_owned = MaybeOwned::Borrowed(&data);
        assert_eq!(maybe_owned.name, "borrowed");
        assert_eq!(maybe_owned.value, 200);
    }

    #[test]
    fn test_maybe_owned_into_owned_from_owned() {
        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        let maybe_owned = MaybeOwned::Owned(data.clone());
        let owned = maybe_owned.into_owned();

        assert_eq!(owned, data);
    }

    #[test]
    fn test_maybe_owned_into_owned_from_borrowed() {
        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        let maybe_owned = MaybeOwned::Borrowed(&data);
        let owned = maybe_owned.into_owned();

        assert_eq!(owned, data);
    }

    // ============================================================================
    // UTF-8 and Edge Cases
    // ============================================================================

    #[test]
    fn test_format_serialize_utf8() {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Utf8Data {
            text: String,
        }

        let data = Utf8Data {
            text: "Hello 世界 🚀".to_string(),
        };

        let bytes = Format::Json.serialize(&data);
        let decoded: Utf8Data = Format::Json.deserialize_owned(&bytes).unwrap();

        assert_eq!(decoded, data);
    }

    #[test]
    fn test_format_serialize_empty_struct() {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Empty {}

        let data = Empty {};
        let bytes = Format::Json.serialize(&data);
        let decoded: Empty = Format::Json.deserialize_owned(&bytes).unwrap();

        assert_eq!(decoded, data);
    }

    #[test]
    fn test_format_serialize_nested_structures() {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Inner {
            value: i32,
        }

        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Outer {
            inner: Inner,
            name: String,
        }

        let data = Outer {
            inner: Inner { value: 42 },
            name: "nested".to_string(),
        };

        let bytes = Format::Json.serialize(&data);
        let decoded: Outer = Format::Json.deserialize_owned(&bytes).unwrap();

        assert_eq!(decoded, data);
    }

    #[test]
    fn test_format_serialize_vec() {
        let data = vec![1, 2, 3, 4, 5];
        let bytes = Format::Json.serialize(&data);
        let decoded: Vec<i32> = Format::Json.deserialize_owned(&bytes).unwrap();

        assert_eq!(decoded, data);
    }

    #[test]
    fn test_format_serialize_option() {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct WithOption {
            value: Option<i32>,
        }

        let with_some = WithOption { value: Some(42) };
        let bytes = Format::Json.serialize(&with_some);
        let decoded: WithOption = Format::Json.deserialize_owned(&bytes).unwrap();
        assert_eq!(decoded, with_some);

        let with_none = WithOption { value: None };
        let bytes = Format::Json.serialize(&with_none);
        let decoded: WithOption = Format::Json.deserialize_owned(&bytes).unwrap();
        assert_eq!(decoded, with_none);
    }
}
