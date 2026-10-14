//! Exact traversal candidate membership for full-text search.

use std::num::NonZeroU64;
use std::sync::Arc;

use roaring::RoaringTreemap;

use crate::error::HelixDbError;

const MAX_RESTRICTED_CANDIDATES: u64 = 1_000_000;

/// Deduplicated, bounded candidate IDs supplied by an upstream traversal.
#[derive(Debug, Clone)]
pub(crate) enum RestrictedTextCandidates {
    /// The traversal produced no unique IDs.
    Empty,
    /// The traversal produced a non-empty exact bitmap.
    NonEmpty(RoaringTreemap),
}

/// Builds an exact candidate bitmap while enforcing its unique-ID limit.
#[derive(Debug)]
pub(crate) struct RestrictedTextCandidatesBuilder {
    bitmap: RoaringTreemap,
    limit: NonZeroU64,
}

impl RestrictedTextCandidatesBuilder {
    pub(crate) fn new() -> Self {
        Self {
            bitmap: RoaringTreemap::new(),
            limit: NonZeroU64::new(MAX_RESTRICTED_CANDIDATES)
                .expect("the restricted candidate limit is non-zero"),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_limit(limit: NonZeroU64) -> Self {
        Self {
            bitmap: RoaringTreemap::new(),
            limit,
        }
    }

    /// Returns whether the ID was newly retained.
    pub(crate) fn try_insert(&mut self, id: u64) -> Result<bool, HelixDbError> {
        if self.bitmap.contains(id) {
            return Ok(false);
        }
        if self.bitmap.len() >= self.limit.get() {
            return Err(HelixDbError::Query(format!(
                "restricted text search accepts at most {} unique candidates",
                self.limit
            )));
        }
        let inserted = self.bitmap.insert(id);
        debug_assert!(inserted, "a previously absent candidate must be inserted");
        Ok(true)
    }

    pub(crate) fn finish(self) -> RestrictedTextCandidates {
        if self.bitmap.is_empty() {
            RestrictedTextCandidates::Empty
        } else {
            RestrictedTextCandidates::NonEmpty(self.bitmap)
        }
    }
}

impl RestrictedTextCandidates {
    /// Canonicalizes duplicate traversal rows into one exact bounded bitmap.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) fn from_ids(ids: impl IntoIterator<Item = u64>) -> Result<Self, HelixDbError> {
        let mut builder = RestrictedTextCandidatesBuilder::new();
        for id in ids {
            builder.try_insert(id)?;
        }
        Ok(builder.finish())
    }

    pub(crate) fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }

    pub(crate) fn contains(&self, entity_id: u64) -> bool {
        match self {
            Self::Empty => false,
            Self::NonEmpty(bitmap) => bitmap.contains(entity_id),
        }
    }

    /// Removes `excluded` IDs, collapsing to `Empty` when none remain.
    pub(crate) fn without(&self, excluded: &RoaringTreemap) -> Self {
        match self {
            Self::Empty => Self::Empty,
            Self::NonEmpty(bitmap) => {
                let bitmap = bitmap - excluded;
                if bitmap.is_empty() {
                    Self::Empty
                } else {
                    Self::NonEmpty(bitmap)
                }
            }
        }
    }
}

/// Explicit unrestricted or exact-candidate scope for every physical FTS read.
#[derive(Debug, Clone)]
pub(crate) enum TextSearchScope {
    Unrestricted,
    Restricted(Arc<RestrictedTextCandidates>),
}

impl TextSearchScope {
    pub(crate) fn restricted(candidates: Arc<RestrictedTextCandidates>) -> Self {
        Self::Restricted(candidates)
    }

    pub(crate) fn candidates(&self) -> Option<&RestrictedTextCandidates> {
        match self {
            Self::Unrestricted => None,
            Self::Restricted(candidates) => Some(candidates),
        }
    }

    pub(crate) fn is_empty_restricted(&self) -> bool {
        self.candidates()
            .is_some_and(RestrictedTextCandidates::is_empty)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidates_deduplicate_and_preserve_exact_membership() {
        let candidates = RestrictedTextCandidates::from_ids([9, 2, 9, 4]).unwrap();

        let RestrictedTextCandidates::NonEmpty(bitmap) = &candidates else {
            panic!("non-empty candidate input must produce a non-empty bitmap");
        };
        assert_eq!(bitmap.len(), 3);
        assert!(candidates.contains(2));
        assert!(candidates.contains(4));
        assert!(candidates.contains(9));
        assert!(!candidates.contains(5));
    }

    #[test]
    fn without_removes_exactly_the_excluded_ids() {
        let candidates = RestrictedTextCandidates::from_ids([2, 4, 9]).unwrap();
        let remaining = candidates.without(&RoaringTreemap::from_iter([4, 7]));
        assert!(remaining.contains(2));
        assert!(!remaining.contains(4));
        assert!(remaining.contains(9));
        assert!(candidates.contains(4), "the original set is unchanged");

        assert!(candidates
            .without(&RoaringTreemap::from_iter([2, 4, 9]))
            .is_empty());
        assert!(RestrictedTextCandidates::Empty
            .without(&RoaringTreemap::new())
            .is_empty());
    }

    #[test]
    fn candidates_represent_empty_and_reject_unique_id_overflow() {
        let empty = RestrictedTextCandidates::from_ids([]).unwrap();
        assert!(empty.is_empty());

        let error = RestrictedTextCandidates::from_ids(0..=MAX_RESTRICTED_CANDIDATES)
            .expect_err("the unique candidate cap must fail closed");
        assert!(error.to_string().contains("at most 1000000"));
    }

    #[test]
    fn builder_rejects_before_retaining_an_overflowing_unique_id() {
        let mut builder = RestrictedTextCandidatesBuilder::with_limit(NonZeroU64::new(2).unwrap());
        assert!(builder.try_insert(1).unwrap());
        assert!(!builder.try_insert(1).unwrap());
        assert!(builder.try_insert(2).unwrap());

        let error = builder
            .try_insert(3)
            .expect_err("the third unique ID must fail before insertion");
        assert!(error.to_string().contains("at most 2"));

        let candidates = builder.finish();
        assert!(candidates.contains(1));
        assert!(candidates.contains(2));
        assert!(!candidates.contains(3));
    }
}
