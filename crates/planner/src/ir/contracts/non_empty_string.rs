use std::fmt;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A string that is statically known not to be empty.
///
/// Use this for physical plan identifiers where an empty string cannot be
/// interpreted as a valid name. Constructors preserve the invariant, and
/// deserialization rejects invalid external payloads.
///
/// ```
/// use helix_planner::ir::NonEmptyString;
///
/// assert!(NonEmptyString::new("").is_none());
/// assert_eq!(NonEmptyString::new("users").unwrap().as_ref(), "users");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NonEmptyString {
    value: String,
}

impl NonEmptyString {
    /// Build a string-backed identifier, returning `None` when it is empty.
    pub fn new(value: impl Into<String>) -> Option<Self> {
        let value = value.into();
        (!value.is_empty()).then_some(Self { value })
    }

    /// Build a string-backed identifier from an internal static string.
    ///
    /// This constructor is intended for closed inventories of planner-owned
    /// static strings. Rust cannot encode a non-empty string literal in the
    /// type system, so the invariant is asserted at the boundary.
    ///
    /// ```
    /// use helix_planner::ir::NonEmptyString;
    ///
    /// let reason = NonEmptyString::from_static("unsupported selected shape");
    /// assert_eq!(reason.as_ref(), "unsupported selected shape");
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if `value` is empty.
    pub fn from_static(value: &'static str) -> Self {
        assert!(
            !value.is_empty(),
            "static NonEmptyString values must not be empty"
        );
        Self {
            value: value.to_owned(),
        }
    }

    /// Build a non-empty generated identifier from a non-empty static prefix
    /// and a displayable suffix.
    ///
    /// This constructor is intended for planner-owned generated IDs where the
    /// prefix itself proves non-emptiness, even when the suffix is supplied by a
    /// generic formatter.
    ///
    /// ```
    /// use helix_planner::ir::NonEmptyString;
    ///
    /// let id = NonEmptyString::from_prefixed_display("node_eq:", "User:email");
    /// assert_eq!(id.as_ref(), "node_eq:User:email");
    ///
    /// let id = NonEmptyString::from_prefixed_display("prefix:", "");
    /// assert_eq!(id.as_ref(), "prefix:");
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if `prefix` is empty.
    pub fn from_prefixed_display(prefix: &'static str, suffix: impl fmt::Display) -> Self {
        assert!(
            !prefix.is_empty(),
            "generated NonEmptyString prefixes must not be empty"
        );
        Self {
            value: format!("{prefix}{suffix}"),
        }
    }

    /// Consume the wrapper and return the validated string.
    ///
    /// ```
    /// use helix_planner::ir::NonEmptyString;
    ///
    /// let name = NonEmptyString::new("users").unwrap();
    /// assert_eq!(name.into_string(), "users");
    /// ```
    pub fn into_string(self) -> String {
        self.value
    }
}

impl AsRef<str> for NonEmptyString {
    fn as_ref(&self) -> &str {
        &self.value
    }
}

impl std::borrow::Borrow<str> for NonEmptyString {
    fn borrow(&self) -> &str {
        &self.value
    }
}

impl PartialEq<str> for NonEmptyString {
    fn eq(&self, other: &str) -> bool {
        self.value == other
    }
}

impl PartialEq<&str> for NonEmptyString {
    fn eq(&self, other: &&str) -> bool {
        self.value == *other
    }
}

impl std::ops::Deref for NonEmptyString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl std::fmt::Display for NonEmptyString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

impl Serialize for NonEmptyString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.value)
    }
}

impl<'de> Deserialize<'de> for NonEmptyString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).ok_or_else(|| D::Error::custom("expected non-empty string"))
    }
}
