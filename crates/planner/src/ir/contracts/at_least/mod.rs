//! Fixed lower-bound collection contract.

mod iter;
mod ops;
mod serde_impl;

/// A collection that is statically known to contain at least `MIN` items.
///
/// Constructors preserve the invariant, and deserialization rejects invalid
/// external payloads.
///
/// ```
/// use helix_planner::ir::AtLeast;
///
/// assert!(AtLeast::<i32, 2>::try_from_vec(vec![1]).is_none());
/// assert_eq!(AtLeast::<_, 1>::from_one(1).as_ref(), &[1]);
/// assert_eq!(AtLeast::<_, 2>::from_pair(1, 2).as_ref(), &[1, 2]);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtLeast<T, const MIN: usize> {
    pub(super) items: Vec<T>,
}

impl<T, const MIN: usize> AtLeast<T, MIN> {
    /// Build a collection from a vector, returning `None` when it has fewer
    /// than `MIN` items.
    pub fn try_from_vec(items: Vec<T>) -> Option<Self> {
        (items.len() >= MIN).then_some(Self { items })
    }
}

impl<T> AtLeast<T, 1> {
    /// Build a collection from one required item.
    pub fn from_one(first: T) -> Self {
        Self { items: vec![first] }
    }

    /// Build a collection from one required item plus any remaining items.
    pub fn from_one_and_rest(first: T, rest: Vec<T>) -> Self {
        let mut items = Vec::with_capacity(rest.len() + 1);
        items.push(first);
        items.extend(rest);
        Self { items }
    }
}

impl<T> AtLeast<T, 2> {
    /// Build a collection from exactly two required items.
    pub fn from_pair(first: T, second: T) -> Self {
        Self {
            items: vec![first, second],
        }
    }

    /// Build a collection from two required items plus any remaining items.
    pub fn from_pair_and_rest(first: T, second: T, rest: Vec<T>) -> Self {
        let mut items = Vec::with_capacity(rest.len() + 2);
        items.push(first);
        items.push(second);
        items.extend(rest);
        Self { items }
    }
}
