//! Typed vector result identities and explicitly versioned distance output.
//!
//! HNSW uses a physical `u64` local ID and current generation score internally.
//! Dynamic query boundaries bind that ID to the descriptor's node/edge kind and
//! bind the score to its persisted semantic before row materialization. Existing
//! responses select [`DistanceOutputVersion::CurrentScore`], so server upgrades
//! cannot silently change `$distance` units.
//!
//! Physical results remain crate-private until the generation descriptor binds
//! their local identity and score semantic. Public callers select an explicit
//! [`DistanceOutputVersion`] only after that binding.

use crate::encoding::v2::values::indexes::vector::{ActiveScoreSemantic, VectorEntityKind};
use crate::encoding::{EdgeId, NodeId};

use super::DistanceScore;

/// Validated score and local identity returned by one physical vector index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SearchResult {
    entity_id: u64,
    score: DistanceScore,
}

impl SearchResult {
    /// Constructs a physical result from an already validated kernel score.
    pub(crate) const fn new(entity_id: u64, score: DistanceScore) -> Self {
        Self { entity_id, score }
    }

    /// Returns the physical entity ID local to the bound index generation.
    pub(crate) const fn entity_id(self) -> u64 {
        self.entity_id
    }

    /// Returns the validated descriptor-defined ranking score.
    pub(crate) const fn score(self) -> DistanceScore {
        self.score
    }
}

/// Graph entity identity proven at the dynamic vector-search boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum VectorEntityId {
    /// Node identifier from a node-bound vector generation.
    Node(NodeId),
    /// Edge identifier from an edge-bound vector generation.
    Edge(EdgeId),
}

impl VectorEntityId {
    /// Binds a physical local ID to the descriptor's closed entity kind.
    pub(crate) const fn from_kind(kind: VectorEntityKind, entity_id: u64) -> Self {
        match kind {
            VectorEntityKind::Node => Self::Node(entity_id),
            VectorEntityKind::Edge => Self::Edge(entity_id),
        }
    }

    /// Returns the graph-local ID after the generation descriptor has already
    /// proven the element family at the search boundary.
    pub(crate) const fn local_id(self) -> u64 {
        match self {
            Self::Node(id) | Self::Edge(id) => id,
        }
    }
}

/// Explicit public materialization contract for vector score units.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum DistanceOutputVersion {
    /// Preserve every existing `$distance` number and its generation semantic.
    #[default]
    CurrentScore,
    /// Request metric-oriented units where a defined conversion exists.
    ///
    /// Squared Euclidean receives exactly one square root. Manhattan is already
    /// metric distance. Current half-cosine remains numerically unchanged and is
    /// labeled as such rather than silently doubled.
    MetricDistance,
}

/// Unit label accompanying a materialized vector distance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceOutputUnit {
    /// Current half-cosine f32 ranking score.
    HalfCosineScore,
    /// Current squared-Euclidean f32 ranking score.
    SquaredEuclideanScore,
    /// Euclidean L2 distance produced by the explicit metric-distance version.
    EuclideanDistance,
    /// Manhattan L1 distance; current and metric-oriented values are identical.
    ManhattanDistance,
}

/// Public numeric distance paired with an explicit unit label.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MaterializedVectorDistance {
    value: f32,
    unit: DistanceOutputUnit,
}

impl MaterializedVectorDistance {
    /// Returns the public numeric value.
    pub const fn value(self) -> f32 {
        self.value
    }

    /// Returns the explicit unit/semantic label.
    pub const fn unit(self) -> DistanceOutputUnit {
        self.unit
    }
}

/// Score and entity kind bound before interpreter row materialization.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct TypedVectorSearchResult {
    entity_id: VectorEntityId,
    score: DistanceScore,
    semantic: ActiveScoreSemantic,
}

impl TypedVectorSearchResult {
    /// Binds one validated physical result to descriptor-derived identity and score meaning.
    pub(crate) const fn from_physical(
        kind: VectorEntityKind,
        semantic: ActiveScoreSemantic,
        result: SearchResult,
    ) -> Self {
        Self {
            entity_id: VectorEntityId::from_kind(kind, result.entity_id()),
            score: result.score(),
            semantic,
        }
    }

    /// Returns the node/edge identity proven by the generation binding.
    pub(crate) const fn entity_id(self) -> VectorEntityId {
        self.entity_id
    }

    /// Materializes this result under an explicit response-unit version.
    pub(crate) fn materialize_distance(
        self,
        version: DistanceOutputVersion,
    ) -> MaterializedVectorDistance {
        let score = self.score.get();
        match (version, self.semantic) {
            (DistanceOutputVersion::CurrentScore, ActiveScoreSemantic::CosineHalfF32V1)
            | (DistanceOutputVersion::MetricDistance, ActiveScoreSemantic::CosineHalfF32V1) => {
                MaterializedVectorDistance {
                    value: score,
                    unit: DistanceOutputUnit::HalfCosineScore,
                }
            }
            (DistanceOutputVersion::CurrentScore, ActiveScoreSemantic::SquaredEuclideanF32V1) => {
                MaterializedVectorDistance {
                    value: score,
                    unit: DistanceOutputUnit::SquaredEuclideanScore,
                }
            }
            (DistanceOutputVersion::MetricDistance, ActiveScoreSemantic::SquaredEuclideanF32V1) => {
                MaterializedVectorDistance {
                    value: score.sqrt(),
                    unit: DistanceOutputUnit::EuclideanDistance,
                }
            }
            (
                DistanceOutputVersion::CurrentScore | DistanceOutputVersion::MetricDistance,
                ActiveScoreSemantic::ManhattanF32V1,
            ) => MaterializedVectorDistance {
                value: score,
                unit: DistanceOutputUnit::ManhattanDistance,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn physical(entity_id: u64, distance: f32) -> SearchResult {
        SearchResult::new(entity_id, DistanceScore::try_new(distance).unwrap())
    }

    #[test]
    fn entity_binding_is_exhaustive_for_nodes_and_edges() {
        let node = TypedVectorSearchResult::from_physical(
            VectorEntityKind::Node,
            ActiveScoreSemantic::ManhattanF32V1,
            physical(7, 1.0),
        );
        let edge = TypedVectorSearchResult::from_physical(
            VectorEntityKind::Edge,
            ActiveScoreSemantic::ManhattanF32V1,
            physical(8, 1.0),
        );

        assert_eq!(node.entity_id(), VectorEntityId::Node(7));
        assert_eq!(edge.entity_id(), VectorEntityId::Edge(8));
    }

    #[test]
    fn explicit_output_version_preserves_current_numbers_and_converts_euclidean_once() {
        let result = TypedVectorSearchResult::from_physical(
            VectorEntityKind::Node,
            ActiveScoreSemantic::SquaredEuclideanF32V1,
            physical(1, 25.0),
        );

        let current = result.materialize_distance(DistanceOutputVersion::CurrentScore);
        assert_eq!(current.value(), 25.0);
        assert_eq!(current.unit(), DistanceOutputUnit::SquaredEuclideanScore);

        let metric = result.materialize_distance(DistanceOutputVersion::MetricDistance);
        assert_eq!(metric.value(), 5.0);
        assert_eq!(metric.unit(), DistanceOutputUnit::EuclideanDistance);
    }

    #[test]
    fn half_cosine_is_labeled_and_never_silently_doubled() {
        let result = TypedVectorSearchResult::from_physical(
            VectorEntityKind::Node,
            ActiveScoreSemantic::CosineHalfF32V1,
            physical(1, 0.25),
        );

        for version in [
            DistanceOutputVersion::CurrentScore,
            DistanceOutputVersion::MetricDistance,
        ] {
            let distance = result.materialize_distance(version);
            assert_eq!(distance.value(), 0.25);
            assert_eq!(distance.unit(), DistanceOutputUnit::HalfCosineScore);
        }
    }

    #[test]
    fn non_finite_or_negative_scores_cannot_form_a_physical_result() {
        for invalid in [f32::NAN, f32::INFINITY, -1.0] {
            assert!(DistanceScore::try_new(invalid).is_err());
        }
    }
}
