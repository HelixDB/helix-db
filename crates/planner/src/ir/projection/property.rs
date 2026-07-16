//! Projection property-list contracts.

use std::collections::BTreeSet;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::super::{AtLeast, NonEmptyString};

/// Property selection for terminals that can project either every property or
/// a non-empty explicit property list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PropertySelection {
    /// Project all properties.
    All,
    /// Project an explicit non-empty property list.
    Selected(PropertyNames),
}

/// Invalid property-name payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertyNamesError {
    /// More than one property entry references the same name.
    DuplicateName {
        /// Duplicate property name.
        name: NonEmptyString,
    },
}

/// Non-empty property names with no duplicates.
///
/// ```
/// use helix_planner::ir::{AtLeast, NonEmptyString, PropertyNames, PropertyNamesError};
///
/// let name = NonEmptyString::new("name").unwrap();
/// assert!(PropertyNames::new(AtLeast::<_, 1>::from_one_and_rest(name.clone(), Vec::new())).is_ok());
///
/// let duplicate = AtLeast::<_, 1>::from_one_and_rest(name.clone(), vec![name]);
/// assert!(matches!(
///     PropertyNames::new(duplicate),
///     Err(PropertyNamesError::DuplicateName { .. })
/// ));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyNames {
    names: AtLeast<NonEmptyString, 1>,
}

impl PropertyNames {
    /// Build a property-name list, returning an error for duplicate names.
    pub fn new(names: AtLeast<NonEmptyString, 1>) -> Result<Self, PropertyNamesError> {
        let mut seen = BTreeSet::new();
        for name in &names {
            if !seen.insert(name.clone()) {
                return Err(PropertyNamesError::DuplicateName { name: name.clone() });
            }
        }
        Ok(Self { names })
    }
}

impl AsRef<[NonEmptyString]> for PropertyNames {
    fn as_ref(&self) -> &[NonEmptyString] {
        self.names.as_ref()
    }
}

impl Serialize for PropertyNames {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.names.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PropertyNames {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let names = AtLeast::<NonEmptyString, 1>::deserialize(deserializer)?;
        Self::new(names).map_err(|err| match err {
            PropertyNamesError::DuplicateName { name } => {
                D::Error::custom(format!("duplicate property `{name}`"))
            }
        })
    }
}
