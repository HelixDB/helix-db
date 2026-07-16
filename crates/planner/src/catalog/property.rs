use helix_ast::index::RangeIndexDirection;
use serde::{Deserialize, Serialize};

use crate::ir::NonEmptyString;

/// Scoped property key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ScopedPropertyKey {
    /// Label scope.
    pub label: NonEmptyString,
    /// Property name.
    pub property: NonEmptyString,
}

impl ScopedPropertyKey {
    /// Build a key from validated components.
    pub fn new(label: NonEmptyString, property: NonEmptyString) -> Self {
        Self { label, property }
    }

    /// Try to build a key from raw strings.
    pub fn try_new(label: impl Into<String>, property: impl Into<String>) -> Option<Self> {
        Some(Self::new(
            NonEmptyString::new(label)?,
            NonEmptyString::new(property)?,
        ))
    }
}

impl std::fmt::Display for ScopedPropertyKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.label, self.property)
    }
}

/// Scoped property key with range direction.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ScopedPropertyDirectionKey {
    /// Label scope.
    pub label: NonEmptyString,
    /// Property name.
    pub property: NonEmptyString,
    /// Physical index direction.
    pub direction: RangeIndexDirection,
}

impl ScopedPropertyDirectionKey {
    /// Build a key from validated components.
    pub fn new(
        label: NonEmptyString,
        property: NonEmptyString,
        direction: RangeIndexDirection,
    ) -> Self {
        Self {
            label,
            property,
            direction,
        }
    }

    /// Try to build a key from raw strings.
    pub fn try_new(
        label: impl Into<String>,
        property: impl Into<String>,
        direction: RangeIndexDirection,
    ) -> Option<Self> {
        Some(Self::new(
            NonEmptyString::new(label)?,
            NonEmptyString::new(property)?,
            direction,
        ))
    }
}

impl std::fmt::Display for ScopedPropertyDirectionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}:{:?}", self.label, self.property, self.direction)
    }
}
