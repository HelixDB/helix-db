//! Randomness ownership for vector insertion and search.
//!
//! Vector-index handles own long-lived randomness policies, while each search
//! invocation receives its own mutable session. This separation preserves the
//! deployed query-derived seed contract, prevents concurrent searches from
//! sharing RNG state, and gives tests deterministic replay without global
//! mutable state. The module contains no persistence or graph-mutation logic.

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use rand::{RngExt, SeedableRng};

use crate::encoding::NodeId;

use super::{select_layer, SimHash};

/// Chooses HNSW layers for one index handle.
///
/// Production handles use process randomness. Tests may install a non-empty
/// scripted cycle so layer-sensitive graph transitions are exactly replayable.
pub(crate) enum LayerSelector {
    /// Draw every insertion layer from the process RNG.
    Random,
    #[cfg(test)]
    /// Cycle through an explicitly non-empty test sequence.
    Scripted {
        first: u16,
        rest: Box<[u16]>,
        next: AtomicUsize,
    },
}

impl LayerSelector {
    /// Creates the production layer-selection policy.
    pub(crate) const fn random() -> Self {
        Self::Random
    }

    /// Selects the next insertion layer using this handle's policy.
    pub(crate) fn select(&self, ml: f32) -> u16 {
        match self {
            Self::Random => {
                let mut rng = rand::rng();
                select_layer(ml, &mut rng)
            }
            #[cfg(test)]
            Self::Scripted { first, rest, next } => {
                let layer_count = 1 + rest.len();
                let index = next.fetch_add(1, Ordering::Relaxed) % layer_count;
                if index == 0 {
                    *first
                } else {
                    rest[index - 1]
                }
            }
        }
    }

    /// Creates a deterministic, repeating layer policy for tests.
    ///
    /// Empty scripts are rejected so selection cannot enter an unrepresentable
    /// state with no next layer.
    #[cfg(test)]
    pub(crate) fn scripted(layers: Vec<u16>) -> Result<Self, ScriptedLayerSelectorError> {
        let Some((&first, rest)) = layers.split_first() else {
            return Err(ScriptedLayerSelectorError::Empty);
        };
        Ok(Self::Scripted {
            first,
            rest: rest.into(),
            next: AtomicUsize::new(0),
        })
    }
}

/// Why a deterministic test layer policy could not be constructed.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScriptedLayerSelectorError {
    /// A repeating layer policy requires at least one layer.
    Empty,
}

/// Factory for query-local random state.
///
/// The production arm preserves the existing query-derived seed contract. The
/// test arm fixes the seed independently of query and entry-point inputs so a
/// complete sampling and tie sequence can be replayed.
#[derive(Clone, Copy)]
pub(crate) enum SearchRandomness {
    /// Derive the stable seed from query SimHash, entry point, and beam width.
    QueryDerived,
    #[cfg(test)]
    /// Replay every search from a fixed test-only seed.
    Seeded(u64),
}

impl SearchRandomness {
    /// Starts isolated RNG state for one search invocation.
    ///
    /// The seed calculation is part of deterministic query behavior and must
    /// remain stable unless that contract is deliberately versioned.
    pub(crate) fn start(
        self,
        query_simhash: &SimHash,
        entry_point: NodeId,
        ef: usize,
    ) -> SearchSession {
        let seed = match self {
            Self::QueryDerived => {
                query_simhash.bits() ^ entry_point.rotate_left(17) ^ (ef as u64).rotate_left(7)
            }
            #[cfg(test)]
            Self::Seeded(seed) => seed,
        };
        SearchSession::seeded(seed)
    }
}

/// Mutable random state isolated to one vector-search invocation.
pub(crate) struct SearchSession {
    seed: u64,
    sampling_rng: Option<rand::rngs::StdRng>,
}

impl SearchSession {
    /// Starts a reproducible session without constructing an RNG until needed.
    ///
    /// Exhaustive searches never draw randomness, so deferring initialization
    /// removes state expansion from their hot path without changing the first
    /// or any subsequent sampled value.
    pub(crate) fn seeded(seed: u64) -> Self {
        Self {
            seed,
            sampling_rng: None,
        }
    }

    /// Returns the query-local generator, initializing it from the stable seed.
    fn sampling_rng(&mut self) -> &mut rand::rngs::StdRng {
        self.sampling_rng
            .get_or_insert_with(|| rand::rngs::StdRng::seed_from_u64(self.seed))
    }

    /// Samples one candidate using the closed probability interval.
    ///
    /// Boundary probabilities avoid advancing the RNG, keeping later choices
    /// reproducible when sampling is configured as always or never.
    pub(crate) fn should_sample(&mut self, sampling_ratio: f32) -> bool {
        if sampling_ratio >= 1.0 {
            return true;
        }
        if sampling_ratio <= 0.0 {
            return false;
        }

        self.sampling_rng().random::<f32>() < sampling_ratio
    }

    /// Chooses a candidate offset, returning `None` for an empty frontier.
    pub(crate) fn choose_index(&mut self, candidate_count: usize) -> Option<usize> {
        (candidate_count > 0).then(|| self.sampling_rng().random_range(0..candidate_count))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scripted_layer_selector_requires_layers_and_cycles() {
        assert!(matches!(
            LayerSelector::scripted(Vec::new()),
            Err(ScriptedLayerSelectorError::Empty)
        ));

        let selector = LayerSelector::scripted(vec![0, 1, 7]).unwrap();
        assert_eq!(selector.select(0.0), 0);
        assert_eq!(selector.select(0.0), 1);
        assert_eq!(selector.select(0.0), 7);
        assert_eq!(selector.select(0.0), 0);
    }

    #[test]
    fn search_session_handles_probability_and_frontier_boundaries() {
        let mut session = SearchSession::seeded(42);
        assert!(session.should_sample(1.0));
        assert!(!session.should_sample(0.0));
        assert_eq!(session.choose_index(0), None);
    }

    #[test]
    fn query_derived_randomness_preserves_seed_contract() {
        let query_simhash = SimHash::from_bits(0x0123_4567_89AB_CDEF);
        let entry_point = 42u64;
        let ef = 128usize;
        let expected_seed =
            query_simhash.bits() ^ entry_point.rotate_left(17) ^ (ef as u64).rotate_left(7);
        let mut actual = SearchRandomness::QueryDerived.start(&query_simhash, entry_point, ef);
        let mut expected = SearchSession::seeded(expected_seed);

        for _ in 0..100 {
            assert_eq!(actual.should_sample(0.37), expected.should_sample(0.37));
            assert_eq!(actual.choose_index(11), expected.choose_index(11));
        }
    }
}
