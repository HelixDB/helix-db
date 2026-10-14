use std::borrow::Borrow;
use std::hash::{Hash, Hasher};

use helix_ast::index::RangeIndexDirection;
use serde::{Deserialize, Serialize};

use crate::ir::NonEmptyString;

/// Scoped property key.
///
/// It hashes and compares as its [`ScopedPropertyKeyView`], so maps keyed by
/// it can be queried with a borrowed `(label, property)` pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

/// Scoped property key with a physical range-index direction.
///
/// It hashes and compares as its [`ScopedPropertyDirectionKeyView`], so maps
/// keyed by it can be queried with a borrowed `(label, property, direction)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

/// A borrowed `(label, property)` pair that queries maps keyed by
/// [`ScopedPropertyKey`] without building one.
///
/// ```
/// use std::collections::HashMap;
///
/// use helix_planner::catalog::{ScopedPropertyKey, ScopedPropertyKeyView};
///
/// let indexes = HashMap::from([(ScopedPropertyKey::try_new("User", "email").unwrap(), 1)]);
/// assert_eq!(indexes.get(&("User", "email") as &dyn ScopedPropertyKeyView), Some(&1));
/// assert_eq!(indexes.get(&("User", "name") as &dyn ScopedPropertyKeyView), None);
/// ```
pub trait ScopedPropertyKeyView {
    /// Label scope.
    fn label(&self) -> &str;
    /// Property name.
    fn property(&self) -> &str;
}

impl ScopedPropertyKeyView for ScopedPropertyKey {
    fn label(&self) -> &str {
        self.label.as_ref()
    }

    fn property(&self) -> &str {
        self.property.as_ref()
    }
}

impl ScopedPropertyKeyView for (&str, &str) {
    fn label(&self) -> &str {
        self.0
    }

    fn property(&self) -> &str {
        self.1
    }
}

impl<'a> Borrow<dyn ScopedPropertyKeyView + 'a> for ScopedPropertyKey {
    fn borrow(&self) -> &(dyn ScopedPropertyKeyView + 'a) {
        self
    }
}

impl Hash for dyn ScopedPropertyKeyView + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.label().hash(state);
        self.property().hash(state);
    }
}

impl PartialEq for dyn ScopedPropertyKeyView + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.label() == other.label() && self.property() == other.property()
    }
}

impl Eq for dyn ScopedPropertyKeyView + '_ {}

impl Hash for ScopedPropertyKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self as &dyn ScopedPropertyKeyView).hash(state);
    }
}

/// A borrowed `(label, property, direction)` that queries maps keyed by
/// [`ScopedPropertyDirectionKey`] without building one.
///
/// ```
/// use std::collections::HashMap;
///
/// use helix_ast::index::RangeIndexDirection;
/// use helix_planner::catalog::{ScopedPropertyDirectionKey, ScopedPropertyDirectionKeyView};
///
/// let key = ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap();
/// let indexes = HashMap::from([(key, 1)]);
/// let asc = ("User", "age", RangeIndexDirection::Asc);
/// let desc = ("User", "age", RangeIndexDirection::Desc);
/// assert_eq!(indexes.get(&asc as &dyn ScopedPropertyDirectionKeyView), Some(&1));
/// assert_eq!(indexes.get(&desc as &dyn ScopedPropertyDirectionKeyView), None);
/// ```
pub trait ScopedPropertyDirectionKeyView {
    /// Label scope.
    fn label(&self) -> &str;
    /// Property name.
    fn property(&self) -> &str;
    /// Physical index direction.
    fn direction(&self) -> RangeIndexDirection;
}

impl ScopedPropertyDirectionKeyView for ScopedPropertyDirectionKey {
    fn label(&self) -> &str {
        self.label.as_ref()
    }

    fn property(&self) -> &str {
        self.property.as_ref()
    }

    fn direction(&self) -> RangeIndexDirection {
        self.direction
    }
}

impl ScopedPropertyDirectionKeyView for (&str, &str, RangeIndexDirection) {
    fn label(&self) -> &str {
        self.0
    }

    fn property(&self) -> &str {
        self.1
    }

    fn direction(&self) -> RangeIndexDirection {
        self.2
    }
}

impl<'a> Borrow<dyn ScopedPropertyDirectionKeyView + 'a> for ScopedPropertyDirectionKey {
    fn borrow(&self) -> &(dyn ScopedPropertyDirectionKeyView + 'a) {
        self
    }
}

impl Hash for dyn ScopedPropertyDirectionKeyView + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.label().hash(state);
        self.property().hash(state);
        self.direction().hash(state);
    }
}

impl PartialEq for dyn ScopedPropertyDirectionKeyView + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.label() == other.label()
            && self.property() == other.property()
            && self.direction() == other.direction()
    }
}

impl Eq for dyn ScopedPropertyDirectionKeyView + '_ {}

impl Hash for ScopedPropertyDirectionKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self as &dyn ScopedPropertyDirectionKeyView).hash(state);
    }
}
