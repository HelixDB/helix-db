use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};

use super::louvain::{
    assignment_for_original_nodes, canonical_communities, compress_communities, modularity,
    next_random, shuffle, validate_community_graph, Community, LevelGraph,
};
use crate::{Graph, GraphError, PositiveFiniteF64};

/// Weighted Leiden options matching Graphify's graspologic defaults while
/// making convergence work explicitly bounded.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct LeidenOptions {
    /// Positive finite modularity resolution.
    pub resolution: PositiveFiniteF64,
    /// Positive finite stochastic refinement temperature.
    pub randomness: PositiveFiniteF64,
    /// Deterministic base seed.
    pub seed: u64,
    /// Independent starts; the highest-quality canonical partition wins.
    pub trials: NonZeroUsize,
    /// Maximum local-moving sweeps at each level.
    pub max_iterations: NonZeroUsize,
    /// Maximum refinement and aggregation levels.
    pub max_levels: NonZeroUsize,
}

impl Default for LeidenOptions {
    fn default() -> Self {
        Self {
            resolution: PositiveFiniteF64::new(1.0).expect("one is positive"),
            randomness: PositiveFiniteF64::new(0.001).expect("default randomness is positive"),
            seed: 42,
            trials: NonZeroUsize::new(1).expect("one is non-zero"),
            max_iterations: NonZeroUsize::new(100).expect("100 is non-zero"),
            max_levels: NonZeroUsize::new(10).expect("10 is non-zero"),
        }
    }
}

/// Canonical weighted Leiden partition and diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LeidenResult {
    /// Canonically ordered communities.
    pub communities: Vec<Community>,
    /// Final weighted modularity quality.
    pub modularity: f64,
    /// Number of refinement and aggregation levels in the winning trial.
    pub levels: usize,
    /// Zero-based winning trial index.
    pub winning_trial: usize,
}

struct TrialResult {
    communities: Vec<Community>,
    modularity: f64,
    levels: usize,
}

impl Graph {
    /// Compute weighted Leiden communities with connected refinement.
    pub fn leiden(&self, options: LeidenOptions) -> Result<LeidenResult, GraphError> {
        validate_community_graph(self, "Leiden")?;
        if self.node_count() == 0 {
            return Ok(LeidenResult {
                communities: Vec::new(),
                modularity: 0.0,
                levels: 0,
                winning_trial: 0,
            });
        }

        let mut best: Option<(usize, TrialResult)> = None;
        for trial in 0..options.trials.get() {
            let seed = options.seed.wrapping_add(
                u64::try_from(trial)
                    .expect("usize trial fits in u64")
                    .wrapping_mul(0x9e37_79b9_7f4a_7c15),
            );
            let candidate = leiden_trial(self, options, seed);
            let replace = match &best {
                None => true,
                Some((_, current)) => match candidate.modularity.total_cmp(&current.modularity) {
                    Ordering::Greater => true,
                    Ordering::Equal => candidate.communities < current.communities,
                    Ordering::Less => false,
                },
            };
            if replace {
                best = Some((trial, candidate));
            }
        }
        let (winning_trial, best) = best.expect("non-zero trials produce a result");
        Ok(LeidenResult {
            communities: best.communities,
            modularity: best.modularity,
            levels: best.levels,
            winning_trial,
        })
    }
}

fn leiden_trial(graph: &Graph, options: LeidenOptions, mut seed: u64) -> TrialResult {
    let original = LevelGraph::from_original(graph);
    let original_edges = original.edges.clone();
    if original.total_weight == 0.0 {
        return TrialResult {
            communities: canonical_communities(graph, &(0..graph.node_count()).collect::<Vec<_>>()),
            modularity: 0.0,
            levels: 0,
        };
    }

    let mut level = original;
    let mut initial = (0..level.node_members.len()).collect::<Vec<_>>();
    let mut final_assignment = (0..graph.node_count()).collect::<Vec<_>>();
    let mut final_modularity = 0.0;
    let mut levels = 0;
    for _ in 0..options.max_levels.get() {
        let (moved, moved_any) = level.local_move(
            &initial,
            options.resolution.get(),
            &mut seed,
            options.max_iterations.get(),
        );
        let refined = refine_connected(
            &level,
            &moved,
            options.resolution.get(),
            options.randomness.get(),
            &mut seed,
        );
        levels += 1;
        final_assignment =
            assignment_for_original_nodes(graph.node_count(), &level.node_members, &refined);
        final_modularity = modularity(
            graph.node_count(),
            &original_edges,
            &final_assignment,
            options.resolution.get(),
        );

        let refined_count = refined.iter().max().map_or(0, |maximum| maximum + 1);
        if !moved_any || refined_count == level.node_members.len() {
            break;
        }
        initial = aggregate_initial_partition(&moved, &refined);
        level = level.aggregate(&refined);
    }

    TrialResult {
        communities: canonical_communities(graph, &final_assignment),
        modularity: final_modularity,
        levels,
    }
}

/// Refine each locally moved community from singleton, connected pieces.
/// Nodes can only join a same-parent community reached by a positive-weight
/// edge, so every refined community is connected by construction.
fn refine_connected(
    graph: &LevelGraph,
    parent: &[usize],
    resolution: f64,
    randomness: f64,
    seed: &mut u64,
) -> Vec<usize> {
    let node_count = graph.node_members.len();
    if graph.total_weight == 0.0 {
        return (0..node_count).collect();
    }
    let mut refined = (0..node_count).collect::<Vec<_>>();
    let mut community_degrees = graph.degrees.clone();
    let mut community_sizes = vec![1_usize; node_count];
    let mut order = (0..node_count).collect::<Vec<_>>();
    shuffle(&mut order, seed);

    for node in order {
        let old_community = refined[node];
        if community_sizes[old_community] != 1 {
            continue;
        }
        let mut edge_weight_by_community = BTreeMap::<usize, f64>::new();
        for (neighbor, weight) in &graph.adjacency[node] {
            if *weight > 0.0 && parent[*neighbor] == parent[node] {
                *edge_weight_by_community
                    .entry(refined[*neighbor])
                    .or_default() += *weight;
            }
        }
        if edge_weight_by_community.is_empty() {
            continue;
        }

        let node_degree = graph.degrees[node];
        let denominator = 2.0 * graph.total_weight * graph.total_weight;
        let mut candidates = vec![(old_community, 1.0_f64)];
        for (community, edge_weight) in edge_weight_by_community {
            if community == old_community {
                continue;
            }
            let gain = edge_weight / graph.total_weight
                - resolution * community_degrees[community] * node_degree / denominator;
            if gain >= 0.0 {
                candidates.push((community, (gain / randomness).exp()));
            }
        }
        let total = candidates.iter().map(|(_, weight)| weight).sum::<f64>();
        let chosen = if total.is_finite() {
            let random = random_unit(seed) * total;
            let mut cumulative = 0.0;
            candidates
                .iter()
                .find_map(|(community, weight)| {
                    cumulative += weight;
                    (random < cumulative).then_some(*community)
                })
                .unwrap_or(old_community)
        } else {
            candidates
                .iter()
                .max_by(|left, right| {
                    left.1
                        .total_cmp(&right.1)
                        .then_with(|| right.0.cmp(&left.0))
                })
                .map_or(old_community, |(community, _)| *community)
        };
        if chosen != old_community {
            refined[node] = chosen;
            community_degrees[old_community] -= node_degree;
            community_degrees[chosen] += node_degree;
            community_sizes[old_community] -= 1;
            community_sizes[chosen] += 1;
        }
    }
    compress_communities(&refined)
}

fn aggregate_initial_partition(parent: &[usize], refined: &[usize]) -> Vec<usize> {
    let refined_count = refined.iter().max().map_or(0, |maximum| maximum + 1);
    let mut parents = vec![usize::MAX; refined_count];
    for (node, refined_community) in refined.iter().enumerate() {
        let parent_community = &mut parents[*refined_community];
        if *parent_community == usize::MAX {
            *parent_community = parent[node];
        } else {
            assert_eq!(
                *parent_community, parent[node],
                "refinement communities remain inside their parent"
            );
        }
    }
    compress_communities(&parents)
}

fn random_unit(seed: &mut u64) -> f64 {
    const DENOMINATOR: f64 = (1_u64 << 53) as f64;
    ((next_random(seed) >> 11) as f64) / DENOMINATOR
}

#[cfg(test)]
mod tests {
    use approx::assert_abs_diff_eq;

    use super::*;
    use crate::{Edge, GraphKind, Node};

    fn bridged_triangles(kind: GraphKind) -> Graph {
        Graph::new(
            kind,
            ["a", "b", "c", "d", "e", "f", "isolated"]
                .into_iter()
                .map(Node::new),
            [
                Edge::new("ab", "a", "b").with_weight(2.0),
                Edge::new("bc", "b", "c").with_weight(2.0),
                Edge::new("ca", "c", "a").with_weight(2.0),
                Edge::new("de", "d", "e").with_weight(2.0),
                Edge::new("ef", "e", "f").with_weight(2.0),
                Edge::new("fd", "f", "d").with_weight(2.0),
                Edge::new("cd", "c", "d").with_weight(0.1),
            ],
        )
        .unwrap()
    }

    #[test]
    fn weighted_leiden_is_seeded_and_keeps_isolates() {
        let graph = bridged_triangles(GraphKind::Graph);
        let first = graph.leiden(LeidenOptions::default()).unwrap();
        let second = graph.leiden(LeidenOptions::default()).unwrap();
        assert_eq!(first, second);
        let options_json = serde_json::to_vec(&LeidenOptions::default()).unwrap();
        assert_eq!(
            serde_json::from_slice::<LeidenOptions>(&options_json).unwrap(),
            LeidenOptions::default()
        );
        let result_json = serde_json::to_vec(&first).unwrap();
        let decoded = serde_json::from_slice::<LeidenResult>(&result_json).unwrap();
        assert_eq!(decoded.communities, first.communities);
        assert_eq!(decoded.levels, first.levels);
        assert_eq!(decoded.winning_trial, first.winning_trial);
        assert_abs_diff_eq!(decoded.modularity, first.modularity, epsilon = f64::EPSILON);
        assert_eq!(first.communities.len(), 3);
        assert_eq!(first.communities[0].node_ids, ["a", "b", "c"]);
        assert_eq!(first.communities[1].node_ids, ["d", "e", "f"]);
        assert_eq!(first.communities[2].node_ids, ["isolated"]);
        assert!(first.modularity > 0.0);
    }

    #[test]
    fn leiden_supports_parallel_edges_self_loops_trials_and_caps() {
        let graph = Graph::new(
            GraphKind::MultiGraph,
            [Node::new("a"), Node::new("b"), Node::new("c")],
            [
                Edge::new("ab1", "a", "b").with_weight(2.0),
                Edge::new("ab2", "a", "b").with_weight(3.0),
                Edge::new("bc", "b", "c").with_weight(0.1),
                Edge::new("aa", "a", "a").with_weight(0.5),
            ],
        )
        .unwrap();
        let result = graph
            .leiden(LeidenOptions {
                trials: NonZeroUsize::new(3).unwrap(),
                max_iterations: NonZeroUsize::new(1).unwrap(),
                max_levels: NonZeroUsize::new(1).unwrap(),
                ..LeidenOptions::default()
            })
            .unwrap();
        assert!(result.winning_trial < 3);
        assert_eq!(result.levels, 1);
        assert!(result.communities.iter().any(|community| {
            community.node_ids.contains(&"a".into()) && community.node_ids.contains(&"b".into())
        }));
    }

    #[test]
    fn refinement_never_merges_disconnected_components() {
        let graph = Graph::new(
            GraphKind::Graph,
            [
                Node::new("a"),
                Node::new("b"),
                Node::new("c"),
                Node::new("d"),
            ],
            [Edge::new("ab", "a", "b"), Edge::new("cd", "c", "d")],
        )
        .unwrap();
        let level = LevelGraph::from_original(&graph);
        let refined = refine_connected(&level, &[0, 0, 0, 0], 1.0, 0.001, &mut 42);
        assert_eq!(refined[0], refined[1]);
        assert_eq!(refined[2], refined[3]);
        assert_ne!(refined[0], refined[2]);
    }

    #[test]
    fn leiden_rejects_directed_graphs_and_handles_empty_or_zero_weight_graphs() {
        assert!(matches!(
            bridged_triangles(GraphKind::DiGraph).leiden(LeidenOptions::default()),
            Err(GraphError::InvalidOption(_))
        ));
        let empty = Graph::new(GraphKind::Graph, [], []).unwrap();
        assert_eq!(empty.leiden(LeidenOptions::default()).unwrap().levels, 0);
        let zero = Graph::new(
            GraphKind::Graph,
            [Node::new("a"), Node::new("b")],
            [Edge::new("ab", "a", "b").with_weight(0.0)],
        )
        .unwrap()
        .leiden(LeidenOptions::default())
        .unwrap();
        assert_eq!(zero.communities.len(), 2);
        assert_abs_diff_eq!(zero.modularity, 0.0);
    }

    #[test]
    fn randomness_resolution_and_canonical_trial_ties_are_explicit() {
        assert!(PositiveFiniteF64::new(0.0).is_err());
        let graph = bridged_triangles(GraphKind::Graph);
        let high_resolution = graph
            .leiden(LeidenOptions {
                resolution: PositiveFiniteF64::new(2.0).unwrap(),
                randomness: PositiveFiniteF64::new(0.01).unwrap(),
                seed: 7,
                trials: NonZeroUsize::new(2).unwrap(),
                ..LeidenOptions::default()
            })
            .unwrap();
        assert!(high_resolution.winning_trial < 2);
        assert!(high_resolution.modularity.is_finite());
    }
}
