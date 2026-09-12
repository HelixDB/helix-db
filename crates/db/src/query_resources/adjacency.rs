//! Query-owned adjacency buffers. The guard covers both directions and remains
//! alive while callers borrow their compressed iterators.
use super::{Budget, Reservation};
use crate::encoding::keys::indexes::EdgeDirection;
use crate::encoding::v2::values::adjacency;
use crate::error::Result;

pub(crate) struct Adjacency {
    edges: adjacency::Edges,
    _memory: Option<Reservation>,
}

impl Adjacency {
    pub(crate) fn decode(bytes: &[u8], budget: Option<&Budget>) -> Result<Self> {
        let prepared = adjacency::prepare_edges(bytes)?;
        let memory = budget
            .map(|budget| {
                budget.reserve(
                    prepared
                        .allocation_bound()
                        .saturating_add(size_of::<Self>()),
                )
            })
            .transpose()?;
        Ok(Self {
            edges: prepared.decode()?,
            _memory: memory,
        })
    }

    /// Consuming adjacency iteration retains both decoded directions' guard.
    /// `None` requests both directions; a single direction drops the unused IDs.
    pub(crate) fn into_neighbors(self, direction: Option<EdgeDirection>) -> IntoNeighbors {
        IntoNeighbors {
            outgoing: (direction != Some(EdgeDirection::In))
                .then(|| self.edges.nxts_out.into_iter()),
            incoming: (direction != Some(EdgeDirection::Out))
                .then(|| self.edges.nxts_in.into_iter()),
            _memory: self._memory,
        }
    }
}

pub(crate) struct IntoNeighbors {
    outgoing: Option<roaring::treemap::IntoIter>,
    incoming: Option<roaring::treemap::IntoIter>,
    _memory: Option<Reservation>,
}

impl Iterator for IntoNeighbors {
    type Item = (EdgeDirection, u64);

    fn next(&mut self) -> Option<Self::Item> {
        let Some(id) = self.outgoing.as_mut().and_then(Iterator::next) else {
            return self
                .incoming
                .as_mut()
                .and_then(Iterator::next)
                .map(|id| (EdgeDirection::In, id));
        };
        Some((EdgeDirection::Out, id))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (out, out_max) = self
            .outgoing
            .as_ref()
            .map_or((0, Some(0)), Iterator::size_hint);
        let (incoming, in_max) = self
            .incoming
            .as_ref()
            .map_or((0, Some(0)), Iterator::size_hint);
        (
            out.saturating_add(incoming),
            out_max.zip(in_max).and_then(|(a, b)| a.checked_add(b)),
        )
    }
}

impl std::ops::Deref for Adjacency {
    type Target = adjacency::Edges;
    fn deref(&self) -> &Self::Target {
        &self.edges
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::HelixDbError;

    #[test]
    fn both_directions_remain_admitted_until_the_owner_is_dropped() {
        let mut edges = adjacency::Edges::new();
        for id in 0..10_000 {
            edges.add_out(id);
            edges.add_in(id + 1);
        }
        let bytes = adjacency::encode_edges(&edges);
        let bound =
            adjacency::prepare_edges(&bytes).unwrap().allocation_bound() + size_of::<Adjacency>();
        let too_small = Budget::new(bound - 1);
        assert!(matches!(
            Adjacency::decode(&bytes, Some(&too_small)),
            Err(HelixDbError::QueryMemoryLimitExceeded)
        ));
        assert_eq!(too_small.available(), bound - 1);
        let budget = Budget::new(bound);
        for admission in [Some(&budget), None] {
            let decoded = Adjacency::decode(&bytes, admission).unwrap();
            assert_eq!(
                decoded.iter_out().collect::<Vec<_>>(),
                (0..10_000).collect::<Vec<_>>()
            );
            assert_eq!(
                decoded.iter_in().collect::<Vec<_>>(),
                (1..10_001).collect::<Vec<_>>()
            );
            assert_eq!(
                budget.available(),
                if admission.is_some() { 0 } else { bound }
            );
            drop(decoded);
            assert_eq!(budget.available(), bound);
        }
        for direction in [None, Some(EdgeDirection::Out), Some(EdgeDirection::In)] {
            let decoded = Adjacency::decode(&bytes, Some(&budget)).unwrap();
            let mut neighbors = decoded.into_neighbors(direction);
            let count = if direction.is_none() { 20_000 } else { 10_000 };
            assert_eq!(neighbors.size_hint(), (count, Some(count)));
            assert_eq!(
                neighbors.next(),
                Some(match direction {
                    Some(EdgeDirection::In) => (EdgeDirection::In, 1),
                    Some(EdgeDirection::Out) | None => (EdgeDirection::Out, 0),
                })
            );
            assert_eq!(budget.available(), 0);
            drop(neighbors);
            assert_eq!(budget.available(), bound);
        }
        assert!(Adjacency::decode(&[255], Some(&budget)).is_err());
        assert_eq!(budget.available(), bound);
        let mut small = adjacency::Edges::new();
        small.add_in(1);
        small.add_in(2);
        let mut malformed = adjacency::encode_edges(&small).to_vec();
        let length = malformed.len();
        // Structurally complete duplicate array members reach the ordinary
        // decoder after admission; its failure releases that reservation.
        malformed[length - size_of::<u16>()..].copy_from_slice(&1_u16.to_le_bytes());
        assert!(Adjacency::decode(&malformed, Some(&budget)).is_err());
        assert_eq!(budget.available(), bound);
    }
}
