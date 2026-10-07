//! Canonical runtime ownership for bounded HNSW neighbor sets.
//!
//! Persisted neighbor rows retain their deployed byte codecs. The vector core
//! converts them into [`NeighborSet`] before mutation or difference work, so a
//! set is always sorted, unique, self-free, and within its layer degree limit.
//! Historical upper-layer rows may be ordered by distance rather than node ID,
//! and damaged rows may link their own node; [`NeighborSet::try_from_deployed`]
//! sorts that decoded compatibility input and drops the self-link in memory
//! without rewriting the row. New core state uses the strict canonical
//! constructor and encodes through the unchanged existing codecs.

use std::num::NonZeroUsize;

use crate::encoding::NodeId;

/// Positive maximum number of neighbors allowed for one HNSW layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NeighborDegreeLimit(NonZeroUsize);

impl NeighborDegreeLimit {
    /// Validates a layer degree before any neighbor allocation or arithmetic.
    pub(crate) fn try_new(limit: usize) -> Result<Self, NeighborSetError> {
        let Some(limit) = NonZeroUsize::new(limit) else {
            return Err(NeighborSetError::ZeroDegreeLimit);
        };
        Ok(Self(limit))
    }

    /// Returns the positive maximum neighbor count.
    pub(crate) const fn get(self) -> usize {
        self.0.get()
    }
}

/// Complete layer-specific degree policy owned by one mutation cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NeighborDegreeLimits {
    layer0: NeighborDegreeLimit,
    upper: NeighborDegreeLimit,
}

impl NeighborDegreeLimits {
    /// Validates both final layer limits before graph mutation begins.
    pub(crate) fn try_new(layer0: usize, upper: usize) -> Result<Self, NeighborSetError> {
        Ok(Self {
            layer0: NeighborDegreeLimit::try_new(layer0)?,
            upper: NeighborDegreeLimit::try_new(upper)?,
        })
    }

    /// Returns the exact final degree limit for a physical HNSW layer.
    pub(crate) const fn for_layer(self, layer: u16) -> NeighborDegreeLimit {
        if layer == 0 {
            self.layer0
        } else {
            self.upper
        }
    }
}

/// Sorted, unique, self-free neighbors bounded for one owner and layer policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NeighborSet {
    owner: NodeId,
    degree_limit: NeighborDegreeLimit,
    nodes: Box<[NodeId]>,
}

impl NeighborSet {
    /// Creates the unique empty set for one owner and validated layer policy.
    pub(crate) fn empty(owner: NodeId, degree_limit: NeighborDegreeLimit) -> Self {
        Self {
            owner,
            degree_limit,
            nodes: Box::new([]),
        }
    }

    /// Validates already canonical neighbors without silently repairing input.
    ///
    /// Callers producing new graph state must sort explicitly before crossing
    /// this boundary. This makes accidental quadratic membership and unstable
    /// row ordering visible during development rather than hiding it in a codec.
    /// The set owns `nodes` exactly: a boxed slice is kept as is, and a `Vec`
    /// whose capacity exceeds its length is shrunk.
    pub(crate) fn try_from_canonical(
        owner: NodeId,
        degree_limit: NeighborDegreeLimit,
        nodes: impl Into<Box<[NodeId]>>,
    ) -> Result<Self, NeighborSetError> {
        let nodes = nodes.into();
        if nodes.len() > degree_limit.get() {
            return Err(NeighborSetError::DegreeExceeded {
                limit: degree_limit.get(),
                actual: nodes.len(),
            });
        }
        if nodes.contains(&owner) {
            return Err(NeighborSetError::ContainsOwner(owner));
        }
        for pair in nodes.windows(2) {
            match pair[0].cmp(&pair[1]) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => {
                    return Err(NeighborSetError::Duplicate(pair[0]));
                }
                std::cmp::Ordering::Greater => return Err(NeighborSetError::Unsorted),
            }
        }
        Ok(Self {
            owner,
            degree_limit,
            nodes,
        })
    }

    /// Adapts a decoded deployed row into canonical runtime order.
    ///
    /// Existing upper-neighbor rows may retain distance order, and a row
    /// damaged by a released version may link its own node. Sorting and
    /// dropping the self-link here are runtime-only: traversal never follows
    /// a self-link, so the row means the same without it, and a mutation that
    /// changes the row stores it self-free. Failing instead would fail every
    /// mutation that loads the row, including the ones that would change it.
    /// Duplicates and degree violations still fail closed as corrupt graph
    /// state.
    pub(crate) fn try_from_deployed(
        owner: NodeId,
        degree_limit: NeighborDegreeLimit,
        mut nodes: Vec<NodeId>,
    ) -> Result<Self, NeighborSetError> {
        nodes.retain(|node| *node != owner);
        nodes.sort_unstable();
        Self::try_from_canonical(owner, degree_limit, nodes)
    }

    /// Returns canonical node IDs for unchanged row encoders and traversal.
    pub(crate) fn as_slice(&self) -> &[NodeId] {
        &self.nodes
    }

    /// Uses binary search because canonical ordering is encoded by the type.
    pub(crate) fn contains(&self, node_id: NodeId) -> bool {
        self.nodes.binary_search(&node_id).is_ok()
    }

    /// Returns this set without `node_id`, or `None` when it does not hold it.
    ///
    /// Removing a member keeps the set canonical and within its degree, so
    /// the result needs no validation and owns one exactly sized allocation.
    pub(crate) fn without(&self, node_id: NodeId) -> Option<Self> {
        let position = self.nodes.binary_search(&node_id).ok()?;
        Some(Self {
            owner: self.owner,
            degree_limit: self.degree_limit,
            nodes: self.nodes[..position]
                .iter()
                .chain(&self.nodes[position + 1..])
                .copied()
                .collect(),
        })
    }

    /// Pairs two sets of the same owner and degree for linear differencing.
    ///
    /// Nothing is computed or allocated here: each side of the returned
    /// difference is one merge pass of at most `old.len() + new.len()` steps.
    pub(crate) fn difference<'set>(
        &'set self,
        next: &'set Self,
    ) -> Result<NeighborDifference<'set>, NeighborSetError> {
        if self.owner != next.owner {
            return Err(NeighborSetError::OwnerMismatch {
                expected: self.owner,
                actual: next.owner,
            });
        }
        if self.degree_limit != next.degree_limit {
            return Err(NeighborSetError::DegreeLimitMismatch);
        }
        Ok(NeighborDifference {
            old: &self.nodes,
            new: &next.nodes,
        })
    }
}

/// Linear difference between two canonical sets owned by the same node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NeighborDifference<'set> {
    old: &'set [NodeId],
    new: &'set [NodeId],
}

impl<'set> NeighborDifference<'set> {
    /// Returns true when no physical neighbor or reverse-locator write is needed.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) fn is_empty(&self) -> bool {
        self.old == self.new
    }

    /// Returns the IDs only the old set holds, ascending.
    pub(crate) fn removed(&self) -> SortedDifference<'set> {
        SortedDifference {
            left: self.old,
            right: self.new,
        }
    }

    /// Returns the IDs only the new set holds, ascending.
    pub(crate) fn added(&self) -> SortedDifference<'set> {
        SortedDifference {
            left: self.new,
            right: self.old,
        }
    }

    /// Collects both ordered sides.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) fn into_parts(self) -> (Vec<NodeId>, Vec<NodeId>) {
        (self.removed().collect(), self.added().collect())
    }
}

/// Members of one ascending, unique slice that another such slice lacks, in
/// ascending order.
///
/// One merge pass over both slices yields them: every step consumes an
/// element of `left`, `right`, or both.
#[derive(Debug, Clone)]
pub(crate) struct SortedDifference<'set> {
    left: &'set [NodeId],
    right: &'set [NodeId],
}

impl Iterator for SortedDifference<'_> {
    type Item = NodeId;

    fn next(&mut self) -> Option<NodeId> {
        loop {
            let (&candidate, left) = self.left.split_first()?;
            self.left = left;
            while let Some((&other, right)) = self.right.split_first()
                && other < candidate
            {
                self.right = right;
            }
            match self.right.split_first() {
                Some((&other, right)) if other == candidate => self.right = right,
                _ => return Some(candidate),
            }
        }
    }
}

/// Invalid canonical neighbor state or incompatible difference operands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum NeighborSetError {
    /// A zero degree cannot describe an active HNSW layer policy.
    #[error("neighbor degree limit must be non-zero")]
    ZeroDegreeLimit,
    /// The final set exceeds the validated layer degree.
    #[error("neighbor count {actual} exceeds layer degree limit {limit}")]
    DegreeExceeded { limit: usize, actual: usize },
    /// A node cannot be its own HNSW neighbor.
    #[error("neighbor set contains its owner {0}")]
    ContainsOwner(NodeId),
    /// Canonical input must be strictly ascending.
    #[error("neighbor set is not sorted")]
    Unsorted,
    /// Canonical input cannot contain the same node twice.
    #[error("neighbor set contains duplicate node {0}")]
    Duplicate(NodeId),
    /// A difference cannot combine state for different owners.
    #[error("neighbor owner mismatch: expected {expected}, got {actual}")]
    OwnerMismatch { expected: NodeId, actual: NodeId },
    /// A difference cannot combine sets validated under different layer limits.
    #[error("neighbor degree limit mismatch")]
    DegreeLimitMismatch,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limit(value: usize) -> NeighborDegreeLimit {
        NeighborDegreeLimit::try_new(value).unwrap()
    }

    #[test]
    fn strict_construction_rejects_every_invalid_state() {
        assert_eq!(
            NeighborDegreeLimit::try_new(0),
            Err(NeighborSetError::ZeroDegreeLimit)
        );
        assert_eq!(
            NeighborSet::try_from_canonical(9, limit(3), vec![2, 1]),
            Err(NeighborSetError::Unsorted)
        );
        assert_eq!(
            NeighborSet::try_from_canonical(9, limit(3), vec![1, 1]),
            Err(NeighborSetError::Duplicate(1))
        );
        assert_eq!(
            NeighborSet::try_from_canonical(9, limit(3), vec![1, 9]),
            Err(NeighborSetError::ContainsOwner(9))
        );
        assert_eq!(
            NeighborSet::try_from_canonical(9, limit(1), vec![1, 2]),
            Err(NeighborSetError::DegreeExceeded {
                limit: 1,
                actual: 2
            })
        );
    }

    #[test]
    fn deployed_adapter_canonicalizes_order_and_self_links_but_not_corruption() {
        let set = NeighborSet::try_from_deployed(9, limit(3), vec![3, 1, 2]).unwrap();
        assert_eq!(set.as_slice(), &[1, 2, 3]);
        // A full row that also links its owner fits once the self-link drops.
        let set = NeighborSet::try_from_deployed(9, limit(3), vec![3, 9, 1, 2]).unwrap();
        assert_eq!(set.as_slice(), &[1, 2, 3]);
        assert!(!set.contains(9));
        assert_eq!(
            NeighborSet::try_from_deployed(9, limit(3), vec![9]).unwrap(),
            NeighborSet::empty(9, limit(3))
        );
        assert_eq!(
            NeighborSet::try_from_deployed(9, limit(3), vec![2, 1, 2]),
            Err(NeighborSetError::Duplicate(2))
        );
        assert_eq!(
            NeighborSet::try_from_deployed(9, limit(3), vec![4, 3, 9, 1, 2]),
            Err(NeighborSetError::DegreeExceeded {
                limit: 3,
                actual: 4
            })
        );
    }

    #[test]
    fn canonical_construction_accepts_slices_and_vectors_alike() {
        let from_vec = NeighborSet::try_from_canonical(9, limit(3), vec![1, 2]).unwrap();
        let from_slice = NeighborSet::try_from_canonical(9, limit(3), &[1_u64, 2][..]).unwrap();
        assert_eq!(from_vec, from_slice);
        assert_eq!(
            NeighborSet::try_from_canonical(9, limit(3), &[2_u64, 1][..]),
            Err(NeighborSetError::Unsorted)
        );
    }

    #[test]
    fn without_removes_exactly_one_member() {
        let set = NeighborSet::try_from_canonical(9, limit(3), vec![1, 2, 3]).unwrap();
        for (removed, rest) in [(1, vec![2, 3]), (2, vec![1, 3]), (3, vec![1, 2])] {
            let next = set.without(removed).unwrap();
            assert_eq!(next.as_slice(), rest.as_slice());
            assert_eq!(
                next,
                NeighborSet::try_from_canonical(9, limit(3), rest).unwrap()
            );
        }
        assert!(set.without(4).is_none());
        assert!(set.without(9).is_none());
        let single = NeighborSet::try_from_canonical(9, limit(3), vec![5]).unwrap();
        assert_eq!(single.without(5).unwrap(), NeighborSet::empty(9, limit(3)));
        assert!(NeighborSet::empty(9, limit(3)).without(5).is_none());
    }

    proptest::proptest! {
        /// `without` equals validating the filtered canonical vector.
        #[test]
        fn without_matches_filtering_and_revalidating(
            nodes in proptest::collection::btree_set(0_u64..64, 0..16),
            removed in 0_u64..64,
        ) {
            let nodes = nodes.into_iter().filter(|node| *node != 99).collect::<Vec<_>>();
            let set = NeighborSet::try_from_canonical(99, limit(16), nodes.clone()).unwrap();
            let expected = nodes.contains(&removed).then(|| {
                let rest = nodes.iter().copied().filter(|node| *node != removed).collect::<Vec<_>>();
                NeighborSet::try_from_canonical(99, limit(16), rest).unwrap()
            });
            proptest::prop_assert_eq!(set.without(removed), expected);
        }
    }

    #[test]
    fn difference_is_exact_at_boundaries() {
        let old = NeighborSet::try_from_canonical(9, limit(5), vec![1, 3, 5]).unwrap();
        let new = NeighborSet::try_from_canonical(9, limit(5), vec![2, 3, 4]).unwrap();
        let difference = old.difference(&new).unwrap();
        assert!(!difference.is_empty());
        assert_eq!(difference.into_parts(), (vec![1, 5], vec![2, 4]));
        assert!(old.difference(&old).unwrap().is_empty());
        let empty = NeighborSet::empty(9, limit(5));
        assert_eq!(
            old.difference(&empty).unwrap().into_parts(),
            (vec![1, 3, 5], vec![])
        );
        assert_eq!(
            empty.difference(&old).unwrap().into_parts(),
            (vec![], vec![1, 3, 5])
        );
        assert_eq!(
            NeighborSet::empty(9, limit(1)).difference(&empty),
            Err(NeighborSetError::DegreeLimitMismatch)
        );
    }

    #[test]
    fn sorted_difference_takes_one_merge_pass() {
        let left = [1, 4, 6, 9];
        let right = [0, 4, 5, 9, 12];
        let mut difference = SortedDifference {
            left: &left,
            right: &right,
        };
        assert_eq!(difference.next(), Some(1));
        assert_eq!(difference.next(), Some(6));
        assert_eq!(difference.next(), None);
        // Exhausting `left` consumed every `right` element up to its last.
        assert!(difference.left.is_empty());
        assert_eq!(difference.right, [12].as_slice());
    }

    proptest::proptest! {
        /// Both sides equal ordered-set differences, and together with the
        /// common members they partition the union.
        #[test]
        fn difference_matches_ordered_set_differences(
            old in proptest::collection::btree_set(0_u64..48, 0..24),
            new in proptest::collection::btree_set(0_u64..48, 0..24),
        ) {
            let old_set =
                NeighborSet::try_from_canonical(99, limit(24), old.iter().copied().collect::<Vec<_>>())
                    .unwrap();
            let new_set =
                NeighborSet::try_from_canonical(99, limit(24), new.iter().copied().collect::<Vec<_>>())
                    .unwrap();
            let difference = old_set.difference(&new_set).unwrap();
            proptest::prop_assert_eq!(
                difference.removed().collect::<Vec<_>>(),
                old.difference(&new).copied().collect::<Vec<_>>()
            );
            proptest::prop_assert_eq!(
                difference.added().collect::<Vec<_>>(),
                new.difference(&old).copied().collect::<Vec<_>>()
            );
            proptest::prop_assert_eq!(difference.is_empty(), old == new);
        }
    }

    #[test]
    fn canonical_set_encodes_to_unchanged_neighbor_bytes() {
        let nodes = vec![1, 2, 3];
        let set = NeighborSet::try_from_canonical(9, limit(3), nodes.clone()).unwrap();
        assert_eq!(
            crate::encoding::v2::values::indexes::vector::neighbors::encode_upper_neighbors(
                set.as_slice(),
            )
            .unwrap(),
            crate::encoding::v2::values::indexes::vector::neighbors::encode_upper_neighbors(&nodes)
                .unwrap(),
        );
        assert_eq!(
            crate::encoding::v2::values::indexes::vector::encode_layer0_neighbors(set.as_slice()),
            crate::encoding::v2::values::indexes::vector::encode_layer0_neighbors(&nodes),
        );
    }
}
