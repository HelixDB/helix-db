use std::{collections::BTreeSet, ops::Range};

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::AtLeast;

/// Invalid concrete element ID payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElementIdsError {
    /// More than one entry referenced the same ID.
    DuplicateId {
        /// Duplicate concrete element ID.
        id: u64,
    },
}

/// Non-empty concrete element IDs with no duplicates.
///
/// ```
/// use helix_planner::ir::{AtLeast, ElementIds, ElementIdsError};
///
/// let ids = ElementIds::new(AtLeast::<_, 1>::from_one_and_rest(7, vec![9])).unwrap();
/// assert_eq!(serde_json::to_string(&ids).unwrap(), "[7,9]");
/// assert_eq!(
///     ElementIds::new(AtLeast::<_, 1>::from_one_and_rest(7, vec![7])),
///     Err(ElementIdsError::DuplicateId { id: 7 })
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElementIds {
    ids: AtLeast<u64, 1>,
}

impl ElementIds {
    /// Build concrete element IDs, returning an error for duplicate IDs.
    pub fn new(ids: AtLeast<u64, 1>) -> Result<Self, ElementIdsError> {
        let mut seen = BTreeSet::new();
        for id in &ids {
            if !seen.insert(*id) {
                return Err(ElementIdsError::DuplicateId { id: *id });
            }
        }
        Ok(Self { ids })
    }

    /// Return a non-empty ordered subset while preserving uniqueness by
    /// construction.
    ///
    /// ```
    /// use helix_planner::ir::{AtLeast, ElementIds};
    ///
    /// let ids = ElementIds::new(AtLeast::<_, 1>::from_one_and_rest(
    ///     10,
    ///     vec![20, 30, 40],
    /// ))
    /// .unwrap();
    ///
    /// assert_eq!(ids.slice(1..3).unwrap().as_ref(), &[20, 30]);
    /// assert!(ids.slice(1..1).is_none());
    /// assert!(ids.slice(4..5).is_none());
    /// ```
    pub fn slice(&self, range: Range<usize>) -> Option<Self> {
        let slice = self.ids.as_ref().get(range)?;
        let (first, rest) = slice.split_first()?;
        Some(Self {
            ids: AtLeast::from_one_and_rest(*first, rest.to_vec()),
        })
    }

    /// Borrow concrete IDs with their non-empty invariant preserved.
    pub const fn as_at_least(&self) -> &AtLeast<u64, 1> {
        &self.ids
    }
}

impl AsRef<[u64]> for ElementIds {
    fn as_ref(&self) -> &[u64] {
        self.ids.as_ref()
    }
}

impl Serialize for ElementIds {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.ids.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ElementIds {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ids = AtLeast::<u64, 1>::deserialize(deserializer)?;
        Self::new(ids).map_err(|err| match err {
            ElementIdsError::DuplicateId { id } => {
                D::Error::custom(format!("duplicate element id {id}"))
            }
        })
    }
}
