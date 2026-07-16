use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};

use crate::{Graph, GraphError, NodeId, NonNegativeFiniteF64, PositiveFiniteF64};

/// Proper weighted multi-level Louvain options.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct LouvainOptions {
    /// Positive finite modularity resolution.
    pub resolution: PositiveFiniteF64,
    /// Non-negative finite minimum modularity improvement.
    pub threshold: NonNegativeFiniteF64,
    /// Deterministic node-visit seed.
    pub seed: u64,
    /// Maximum aggregation levels.
    pub max_levels: NonZeroUsize,
}

impl Default for LouvainOptions {
    fn default() -> Self {
        Self {
            resolution: PositiveFiniteF64::new(1.0).expect("one is positive"),
            threshold: NonNegativeFiniteF64::new(1e-4).expect("threshold is non-negative"),
            seed: 42,
            max_levels: NonZeroUsize::new(10).expect("10 is non-zero"),
        }
    }
}

/// One canonical community.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Community {
    /// Stable ID equal to the lexicographically smallest member.
    pub id: NodeId,
    /// Members in deterministic ID order.
    pub node_ids: Vec<NodeId>,
}

/// Louvain partition and diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommunityResult {
    /// Canonically ordered communities.
    pub communities: Vec<Community>,
    /// Final weighted modularity.
    pub modularity: f64,
    /// Number of local-move/aggregation levels executed.
    pub levels: usize,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct LevelEdge {
    pub(super) source: usize,
    pub(super) target: usize,
    pub(super) weight: f64,
}

pub(super) struct LevelGraph {
    pub(super) node_members: Vec<Vec<usize>>,
    pub(super) edges: Vec<LevelEdge>,
    pub(super) adjacency: Vec<Vec<(usize, f64)>>,
    pub(super) degrees: Vec<f64>,
    pub(super) total_weight: f64,
}

impl LevelGraph {
    pub(super) fn from_original(graph: &Graph) -> Self {
        let edges = graph
            .edges()
            .iter()
            .map(|edge| LevelEdge {
                source: graph
                    .node_index(&edge.source)
                    .expect("validated edge source exists"),
                target: graph
                    .node_index(&edge.target)
                    .expect("validated edge target exists"),
                weight: edge.weight.unwrap_or(1.0),
            })
            .collect::<Vec<_>>();
        Self::new(
            (0..graph.node_count()).map(|node| vec![node]).collect(),
            edges,
        )
    }

    pub(super) fn new(node_members: Vec<Vec<usize>>, edges: Vec<LevelEdge>) -> Self {
        let mut adjacency = vec![Vec::new(); node_members.len()];
        let mut degrees = vec![0.0; node_members.len()];
        let mut total_weight = 0.0;
        for edge in &edges {
            total_weight += edge.weight;
            if edge.source == edge.target {
                degrees[edge.source] += edge.weight * 2.0;
            } else {
                degrees[edge.source] += edge.weight;
                degrees[edge.target] += edge.weight;
                adjacency[edge.source].push((edge.target, edge.weight));
                adjacency[edge.target].push((edge.source, edge.weight));
            }
        }
        for row in &mut adjacency {
            row.sort_by_key(|(neighbor, _)| *neighbor);
        }
        Self {
            node_members,
            edges,
            adjacency,
            degrees,
            total_weight,
        }
    }

    fn one_level(&self, resolution: f64, seed: &mut u64) -> Vec<usize> {
        self.local_move(
            &(0..self.node_members.len()).collect::<Vec<_>>(),
            resolution,
            seed,
            usize::MAX,
        )
        .0
    }

    pub(super) fn local_move(
        &self,
        initial: &[usize],
        resolution: f64,
        seed: &mut u64,
        max_iterations: usize,
    ) -> (Vec<usize>, bool) {
        let node_count = self.node_members.len();
        assert_eq!(initial.len(), node_count, "one community per level node");
        let mut communities = compress_communities(initial);
        if self.total_weight == 0.0 {
            return (communities, false);
        }
        let mut community_degrees = vec![0.0; node_count];
        for (node, community) in communities.iter().enumerate() {
            community_degrees[*community] += self.degrees[node];
        }
        let mut order = (0..node_count).collect::<Vec<_>>();
        shuffle(&mut order, seed);
        let mut moved_any = false;
        for _ in 0..max_iterations {
            let mut moved = false;
            for node in &order {
                let old_community = communities[*node];
                let degree = self.degrees[*node];
                let mut weights_by_community = BTreeMap::<usize, f64>::new();
                for (neighbor, weight) in &self.adjacency[*node] {
                    *weights_by_community
                        .entry(communities[*neighbor])
                        .or_default() += *weight;
                }
                community_degrees[old_community] -= degree;
                let denominator = 2.0 * self.total_weight * self.total_weight;
                let remove_cost = -weights_by_community
                    .get(&old_community)
                    .copied()
                    .unwrap_or(0.0)
                    / self.total_weight
                    + resolution * community_degrees[old_community] * degree / denominator;
                let mut best_community = old_community;
                let mut best_gain = 0.0;
                for (candidate, edge_weight) in weights_by_community {
                    let gain = remove_cost + edge_weight / self.total_weight
                        - resolution * community_degrees[candidate] * degree / denominator;
                    if gain > best_gain
                        || (gain == best_gain && gain > 0.0 && candidate < best_community)
                    {
                        best_gain = gain;
                        best_community = candidate;
                    }
                }
                community_degrees[best_community] += degree;
                if best_community != old_community {
                    communities[*node] = best_community;
                    moved = true;
                    moved_any = true;
                }
            }
            if !moved {
                break;
            }
        }
        (compress_communities(&communities), moved_any)
    }

    pub(super) fn aggregate(&self, communities: &[usize]) -> Self {
        let community_count = communities.iter().max().map_or(0, |max| max + 1);
        let mut members = vec![Vec::new(); community_count];
        for (node, community) in communities.iter().enumerate() {
            members[*community].extend(self.node_members[node].iter().copied());
        }
        let mut edge_weights = BTreeMap::<(usize, usize), f64>::new();
        for edge in &self.edges {
            let source = communities[edge.source];
            let target = communities[edge.target];
            let pair = if source <= target {
                (source, target)
            } else {
                (target, source)
            };
            *edge_weights.entry(pair).or_default() += edge.weight;
        }
        Self::new(
            members,
            edge_weights
                .into_iter()
                .map(|((source, target), weight)| LevelEdge {
                    source,
                    target,
                    weight,
                })
                .collect(),
        )
    }
}

impl Graph {
    /// Compute a deterministic weighted multi-level Louvain partition.
    pub fn louvain_communities(
        &self,
        options: LouvainOptions,
    ) -> Result<CommunityResult, GraphError> {
        validate_community_graph(self, "Louvain")?;
        if self.node_count() == 0 {
            return Ok(CommunityResult {
                communities: Vec::new(),
                modularity: 0.0,
                levels: 0,
            });
        }

        let original_edges = LevelGraph::from_original(self).edges;
        let mut level = LevelGraph::from_original(self);
        let mut seed = options.seed;
        let mut final_assignment = (0..self.node_count()).collect::<Vec<_>>();
        let mut previous_modularity = f64::NEG_INFINITY;
        let mut final_modularity = 0.0;
        let mut levels = 0;
        for _ in 0..options.max_levels.get() {
            let communities = level.one_level(options.resolution.get(), &mut seed);
            levels += 1;
            final_assignment =
                assignment_for_original_nodes(self.node_count(), &level.node_members, &communities);
            final_modularity = modularity(
                self.node_count(),
                &original_edges,
                &final_assignment,
                options.resolution.get(),
            );
            let community_count = communities.iter().max().map_or(0, |max| max + 1);
            if community_count == level.node_members.len()
                || (previous_modularity.is_finite()
                    && final_modularity - previous_modularity <= options.threshold.get())
            {
                break;
            }
            previous_modularity = final_modularity;
            level = level.aggregate(&communities);
        }

        Ok(CommunityResult {
            communities: canonical_communities(self, &final_assignment),
            modularity: final_modularity,
            levels,
        })
    }
}

pub(super) fn assignment_for_original_nodes(
    original_node_count: usize,
    members: &[Vec<usize>],
    communities: &[usize],
) -> Vec<usize> {
    let mut assignment = vec![0; original_node_count];
    for (node, originals) in members.iter().enumerate() {
        for original in originals {
            assignment[*original] = communities[node];
        }
    }
    assignment
}

pub(super) fn compress_communities(communities: &[usize]) -> Vec<usize> {
    let mut remap = BTreeMap::new();
    let mut next = 0;
    communities
        .iter()
        .map(|community| {
            *remap.entry(*community).or_insert_with(|| {
                let current = next;
                next += 1;
                current
            })
        })
        .collect()
}

pub(super) fn modularity(
    node_count: usize,
    edges: &[LevelEdge],
    communities: &[usize],
    resolution: f64,
) -> f64 {
    let total_weight = edges.iter().map(|edge| edge.weight).sum::<f64>();
    if total_weight == 0.0 {
        return 0.0;
    }
    let community_count = communities.iter().max().map_or(0, |max| max + 1);
    let mut internal_weight = vec![0.0; community_count];
    let mut degree = vec![0.0; node_count];
    for edge in edges {
        if edge.source == edge.target {
            degree[edge.source] += edge.weight * 2.0;
        } else {
            degree[edge.source] += edge.weight;
            degree[edge.target] += edge.weight;
        }
        if communities[edge.source] == communities[edge.target] {
            internal_weight[communities[edge.source]] += edge.weight;
        }
    }
    let mut community_degree = vec![0.0; community_count];
    for (node, node_degree) in degree.into_iter().enumerate() {
        community_degree[communities[node]] += node_degree;
    }
    (0..community_count)
        .map(|community| {
            internal_weight[community] / total_weight
                - resolution * (community_degree[community] / (2.0 * total_weight)).powi(2)
        })
        .sum()
}

pub(super) fn next_random(seed: &mut u64) -> u64 {
    *seed = seed
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *seed
}

pub(super) fn shuffle(values: &mut [usize], seed: &mut u64) {
    for index in (1..values.len()).rev() {
        let other = (next_random(seed) as usize) % (index + 1);
        values.swap(index, other);
    }
}

pub(super) fn validate_community_graph(graph: &Graph, algorithm: &str) -> Result<(), GraphError> {
    if graph.is_directed() {
        return Err(GraphError::InvalidOption(format!(
            "{algorithm} requires an explicit undirected graph view"
        )));
    }
    Ok(())
}

pub(super) fn canonical_communities(graph: &Graph, assignment: &[usize]) -> Vec<Community> {
    let assignment = compress_communities(assignment);
    let community_count = assignment.iter().max().map_or(0, |max| max + 1);
    let mut members = vec![Vec::new(); community_count];
    for (node, community) in assignment.into_iter().enumerate() {
        members[community].push(graph.node_id(node).clone());
    }
    for community in &mut members {
        community.sort();
    }
    let mut communities = members
        .into_iter()
        .map(|node_ids| Community {
            id: node_ids[0].clone(),
            node_ids,
        })
        .collect::<Vec<_>>();
    communities.sort_by(|left, right| left.id.cmp(&right.id));
    communities
}

#[cfg(test)]
mod tests {
    use approx::assert_abs_diff_eq;

    use super::*;
    use crate::{Edge, GraphKind, Node};

    fn bridged_triangles() -> Graph {
        Graph::new(
            GraphKind::Graph,
            ["a", "b", "c", "d", "e", "f"].into_iter().map(Node::new),
            [
                Edge::new("ab", "a", "b"),
                Edge::new("bc", "b", "c"),
                Edge::new("ca", "c", "a"),
                Edge::new("de", "d", "e"),
                Edge::new("ef", "e", "f"),
                Edge::new("fd", "f", "d"),
                Edge::new("cd", "c", "d").with_weight(0.1),
            ],
        )
        .unwrap()
    }

    #[test]
    fn louvain_finds_dense_groups_and_is_seed_deterministic() {
        let graph = bridged_triangles();
        let first = graph
            .louvain_communities(LouvainOptions::default())
            .unwrap();
        let second = graph
            .louvain_communities(LouvainOptions::default())
            .unwrap();
        assert_eq!(first, second);
        assert_eq!(first.communities.len(), 2);
        assert_eq!(first.communities[0].node_ids, ["a", "b", "c"]);
        assert_eq!(first.communities[1].node_ids, ["d", "e", "f"]);
        assert!(first.modularity > 0.0);
        assert_abs_diff_eq!(first.modularity, 0.4836065573770493, epsilon = 1e-12);
    }

    #[test]
    fn louvain_keeps_isolated_nodes_as_singletons() {
        let graph = Graph::new(GraphKind::Graph, [Node::new("a"), Node::new("b")], []).unwrap();
        let result = graph
            .louvain_communities(LouvainOptions::default())
            .unwrap();
        assert_eq!(result.communities.len(), 2);
        assert_eq!(result.modularity, 0.0);
    }

    #[test]
    fn louvain_requires_undirected_graph_and_options_reject_invalid_values() {
        let graph = Graph::new(GraphKind::DiGraph, [Node::new("a")], []).unwrap();
        assert!(matches!(
            graph.louvain_communities(LouvainOptions::default()),
            Err(GraphError::InvalidOption(_))
        ));
        assert!(PositiveFiniteF64::new(0.0).is_err());
        assert!(NonNegativeFiniteF64::new(-1.0).is_err());
    }

    #[test]
    fn louvain_respects_resolution_threshold_level_cap_and_parallel_weights() {
        let graph = bridged_triangles();
        let high_resolution = graph
            .louvain_communities(LouvainOptions {
                resolution: PositiveFiniteF64::new(2.0).unwrap(),
                ..LouvainOptions::default()
            })
            .unwrap();
        assert_abs_diff_eq!(
            high_resolution.modularity,
            -0.016393442622950727,
            epsilon = 1e-12
        );
        let capped = graph
            .louvain_communities(LouvainOptions {
                threshold: NonNegativeFiniteF64::new(1.0).unwrap(),
                max_levels: NonZeroUsize::new(1).unwrap(),
                ..LouvainOptions::default()
            })
            .unwrap();
        assert_eq!(capped.levels, 1);

        let parallel = Graph::new(
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
        let result = parallel
            .louvain_communities(LouvainOptions::default())
            .unwrap();
        assert_eq!(result.communities[0].node_ids, ["a", "b", "c"]);
    }
}
