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
#[derive(Debug, Default, Clone, Copy)]
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
