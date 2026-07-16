use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};

use super::TraversalDirection;
use crate::{EdgeId, ExternalId, Graph, GraphError, NodeId};

/// Source-selection policy for Brandes centrality.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum BetweennessMode {
    /// Use every node as a source.
    Exact,
    /// Use a deterministic sample of source nodes.
    Sampled {
        /// Requested unique sources, clamped to graph size.
        sample_count: NonZeroUsize,
        /// Deterministic shuffle seed.
        seed: u64,
    },
    /// Use exact mode through a threshold, then sampled mode.
    Auto {
        /// Largest graph using exact mode.
        exact_through: usize,
        /// Requested unique sources above the threshold.
        sample_count: NonZeroUsize,
        /// Deterministic shuffle seed.
        seed: u64,
    },
}

/// Whether shortest paths use edge weights.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PathWeight {
    /// Every edge costs one.
    Unweighted,
    /// Use the graph's selected weight, defaulting missing weights to one.
    Weighted,
}

/// Node and edge betweenness options.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BetweennessOptions {
    /// Exact, sampled, or graph-size-selected sources.
    pub mode: BetweennessMode,
    /// Apply NetworkX-compatible normalization.
    pub normalized: bool,
    /// Include path endpoints in node centrality. Ignored by edge centrality.
    pub endpoints: bool,
    /// Weighted or unweighted shortest paths.
    pub weight: PathWeight,
}

impl Default for BetweennessOptions {
    fn default() -> Self {
        Self {
            mode: BetweennessMode::Exact,
            normalized: true,
            endpoints: false,
            weight: PathWeight::Unweighted,
        }
    }
}

impl BetweennessOptions {
    /// Graphify's audited exact/sample policy.
    pub fn graphify_default() -> Self {
        Self {
            mode: BetweennessMode::Auto {
                exact_through: 1_000,
                sample_count: NonZeroUsize::new(100).expect("100 is non-zero"),
                seed: 42,
            },
            ..Self::default()
        }
    }
}

/// One node centrality score.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeScore {
    /// External node identity.
    pub node_id: NodeId,
    /// Centrality score.
    pub score: f64,
}

/// One stable edge centrality score.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeScore {
    /// Stable edge identity.
    pub edge_id: EdgeId,
    /// Optional Graphify multigraph key.
    pub graphify_key: Option<ExternalId>,
    /// Stored source identity.
    pub source: NodeId,
    /// Stored target identity.
    pub target: NodeId,
    /// Centrality score.
    pub score: f64,
}

#[derive(Debug, Clone, Copy)]
struct HeapState {
    distance: f64,
    order: usize,
    node: usize,
}

impl PartialEq for HeapState {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance && self.order == other.order && self.node == other.node
    }
}

impl Eq for HeapState {}

impl PartialOrd for HeapState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapState {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .distance
            .total_cmp(&self.distance)
            .then_with(|| other.order.cmp(&self.order))
            .then_with(|| other.node.cmp(&self.node))
    }
}

struct SingleSourceState {
    stack: Vec<usize>,
    predecessors: Vec<Vec<(usize, usize)>>,
    path_counts: Vec<f64>,
}

impl Graph {
    /// Compute node betweenness centrality with NetworkX-compatible scaling.
    pub fn betweenness_centrality(
        &self,
        options: BetweennessOptions,
    ) -> Result<Vec<NodeScore>, GraphError> {
        self.validate_centrality_weight(options.weight)?;
        let (sources, sampled_count) = centrality_sources(self.node_count(), options.mode);
        let mut scores = vec![0.0; self.node_count()];
        for source in sources {
            let mut state = self.single_source_paths(source, options.weight);
            let mut dependencies = vec![0.0; self.node_count()];
            if options.endpoints {
                scores[source] += state.stack.len().saturating_sub(1) as f64;
            }
            while let Some(node) = state.stack.pop() {
                assert!(
                    state.path_counts[node] > 0.0,
                    "a settled shortest-path node has at least one path"
                );
                let coefficient = (1.0 + dependencies[node]) / state.path_counts[node];
                for (predecessor, _edge) in &state.predecessors[node] {
                    dependencies[*predecessor] += state.path_counts[*predecessor] * coefficient;
                }
                if node != source {
                    scores[node] += dependencies[node] + f64::from(options.endpoints);
                }
            }
        }
        rescale_nodes(
            &mut scores,
            self.node_count(),
            options.normalized,
            self.is_directed(),
            sampled_count,
            options.endpoints,
        );
        Ok(scores
            .into_iter()
            .enumerate()
            .map(|(node, score)| NodeScore {
                node_id: self.node_id(node).clone(),
                score,
            })
            .collect())
    }

    /// Compute edge betweenness centrality, retaining parallel-edge identity.
    pub fn edge_betweenness_centrality(
        &self,
        options: BetweennessOptions,
    ) -> Result<Vec<EdgeScore>, GraphError> {
        self.validate_centrality_weight(options.weight)?;
        let (sources, sampled_count) = centrality_sources(self.node_count(), options.mode);
        let mut scores = vec![0.0; self.edge_count()];
        for source in sources {
            let mut state = self.single_source_paths(source, options.weight);
            let mut dependencies = vec![0.0; self.node_count()];
            while let Some(node) = state.stack.pop() {
                assert!(
                    state.path_counts[node] > 0.0,
                    "a settled shortest-path node has at least one path"
                );
                let coefficient = (1.0 + dependencies[node]) / state.path_counts[node];
                for (predecessor, edge) in &state.predecessors[node] {
                    let contribution = state.path_counts[*predecessor] * coefficient;
                    scores[*edge] += contribution;
                    dependencies[*predecessor] += contribution;
                }
            }
        }
        rescale_edges(
            &mut scores,
            self.node_count(),
            options.normalized,
            self.is_directed(),
            sampled_count,
        );
        Ok(scores
            .into_iter()
            .enumerate()
            .map(|(edge_index, score)| {
                let edge = self.edge_at(edge_index);
                EdgeScore {
                    edge_id: edge.id.clone(),
                    graphify_key: edge.graphify_key.clone(),
                    source: edge.source.clone(),
                    target: edge.target.clone(),
                    score,
                }
            })
            .collect())
    }

    fn validate_centrality_weight(&self, weight: PathWeight) -> Result<(), GraphError> {
        if weight == PathWeight::Weighted
            && let Some(edge) = self
                .edges()
                .iter()
                .find(|edge| edge.weight.is_some_and(|weight| weight == 0.0))
        {
            return Err(GraphError::InvalidOption(format!(
                "weighted Brandes requires positive weights; edge {} has zero weight",
                edge.id
            )));
        }
        Ok(())
    }

    fn single_source_paths(&self, source: usize, weight: PathWeight) -> SingleSourceState {
        match weight {
            PathWeight::Unweighted => self.single_source_unweighted(source),
            PathWeight::Weighted => self.single_source_weighted(source),
        }
    }

    fn single_source_unweighted(&self, source: usize) -> SingleSourceState {
        let mut stack = Vec::new();
        let mut predecessors = vec![Vec::new(); self.node_count()];
        let mut path_counts = vec![0.0; self.node_count()];
        let mut distance = vec![usize::MAX; self.node_count()];
        path_counts[source] = 1.0;
        distance[source] = 0;
        let mut queue = std::collections::VecDeque::from([source]);
        while let Some(node) = queue.pop_front() {
            stack.push(node);
            for arc in self.arcs(node, TraversalDirection::Out) {
                if distance[arc.neighbor] == usize::MAX {
                    distance[arc.neighbor] = distance[node] + 1;
                    queue.push_back(arc.neighbor);
                }
                if distance[arc.neighbor] == distance[node] + 1 {
                    path_counts[arc.neighbor] += path_counts[node];
                    predecessors[arc.neighbor].push((node, arc.edge));
                }
            }
        }
        SingleSourceState {
            stack,
            predecessors,
            path_counts,
        }
    }

    fn single_source_weighted(&self, source: usize) -> SingleSourceState {
        let mut stack = Vec::new();
        let mut predecessors = vec![Vec::new(); self.node_count()];
        let mut path_counts = vec![0.0; self.node_count()];
        let mut distance = vec![f64::INFINITY; self.node_count()];
        let mut settled = vec![false; self.node_count()];
        let mut heap = BinaryHeap::from([HeapState {
            distance: 0.0,
            order: 0,
            node: source,
        }]);
        let mut order = 1;
        distance[source] = 0.0;
        path_counts[source] = 1.0;
        while let Some(state) = heap.pop() {
            if settled[state.node] || state.distance > distance[state.node] {
                continue;
            }
            settled[state.node] = true;
            stack.push(state.node);
            for arc in self.arcs(state.node, TraversalDirection::Out) {
                let next_distance = state.distance + self.edge_at(arc.edge).weight.unwrap_or(1.0);
                if next_distance < distance[arc.neighbor] {
                    distance[arc.neighbor] = next_distance;
                    path_counts[arc.neighbor] = path_counts[state.node];
                    predecessors[arc.neighbor] = vec![(state.node, arc.edge)];
                    heap.push(HeapState {
                        distance: next_distance,
                        order,
                        node: arc.neighbor,
                    });
                    order += 1;
                } else if next_distance == distance[arc.neighbor] {
                    path_counts[arc.neighbor] += path_counts[state.node];
                    predecessors[arc.neighbor].push((state.node, arc.edge));
                }
            }
        }
        SingleSourceState {
            stack,
            predecessors,
            path_counts,
        }
    }
}

fn centrality_sources(node_count: usize, mode: BetweennessMode) -> (Vec<usize>, Option<usize>) {
    let (sample_count, seed) = match mode {
        BetweennessMode::Exact => return ((0..node_count).collect(), None),
        BetweennessMode::Sampled { sample_count, seed } => (sample_count.get(), seed),
        BetweennessMode::Auto {
            exact_through,
            sample_count,
            seed,
        } if node_count > exact_through => (sample_count.get(), seed),
        BetweennessMode::Auto { .. } => return ((0..node_count).collect(), None),
    };
    let sample_count = sample_count.min(node_count);
    let mut sources = (0..node_count).collect::<Vec<_>>();
    let mut rng = seed;
    for index in (1..sources.len()).rev() {
        let other = (next_random(&mut rng) as usize) % (index + 1);
        sources.swap(index, other);
    }
    sources.truncate(sample_count);
    (sources, Some(sample_count))
}

fn next_random(seed: &mut u64) -> u64 {
    *seed = seed
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *seed
}

fn rescale_nodes(
    scores: &mut [f64],
    node_count: usize,
    normalized: bool,
    directed: bool,
    sampled_count: Option<usize>,
    endpoints: bool,
) {
    let scale = if normalized && endpoints && node_count >= 2 {
        Some(1.0 / (node_count * (node_count - 1)) as f64)
    } else if normalized && !endpoints && node_count > 2 {
        Some(1.0 / ((node_count - 1) * (node_count - 2)) as f64)
    } else if !normalized && !directed {
        Some(0.5)
    } else {
        None
    };
    if let Some(mut scale) = scale {
        if let Some(sampled_count) = sampled_count
            && sampled_count > 0
        {
            scale *= node_count as f64 / sampled_count as f64;
        }
        scores.iter_mut().for_each(|score| *score *= scale);
    }
}

fn rescale_edges(
    scores: &mut [f64],
    node_count: usize,
    normalized: bool,
    directed: bool,
    sampled_count: Option<usize>,
) {
    let scale = if normalized && node_count > 1 {
        Some(1.0 / (node_count * (node_count - 1)) as f64)
    } else if !normalized && !directed {
        Some(0.5)
    } else {
        None
    };
    if let Some(mut scale) = scale {
        if let Some(sampled_count) = sampled_count
            && sampled_count > 0
        {
            scale *= node_count as f64 / sampled_count as f64;
        }
        scores.iter_mut().for_each(|score| *score *= scale);
    }
}

#[cfg(test)]
mod tests {
    use approx::assert_abs_diff_eq;

    use super::*;
    use crate::{Edge, GraphKind, Node};

    fn path(direction: GraphKind) -> Graph {
        Graph::new(
            direction,
            [Node::new("a"), Node::new("b"), Node::new("c")],
            [Edge::new("ab", "a", "b"), Edge::new("bc", "b", "c")],
        )
        .unwrap()
    }

    #[test]
    fn exact_node_and_edge_scores_match_path_fixture() {
        let graph = path(GraphKind::Graph);
        let nodes = graph
            .betweenness_centrality(BetweennessOptions {
                normalized: false,
                ..BetweennessOptions::default()
            })
            .unwrap();
        assert_abs_diff_eq!(nodes[1].score, 1.0);

        let edges = graph
            .edge_betweenness_centrality(BetweennessOptions {
                normalized: false,
                ..BetweennessOptions::default()
            })
            .unwrap();
        assert_abs_diff_eq!(edges[0].score, 2.0);
        assert_abs_diff_eq!(edges[1].score, 2.0);
    }

    #[test]
    fn sampled_scores_are_seed_deterministic() {
        let graph = path(GraphKind::DiGraph);
        let options = BetweennessOptions {
            mode: BetweennessMode::Sampled {
                sample_count: NonZeroUsize::new(2).unwrap(),
                seed: 42,
            },
            ..BetweennessOptions::default()
        };
        assert_eq!(
            graph.betweenness_centrality(options).unwrap(),
            graph.betweenness_centrality(options).unwrap()
        );
    }

    #[test]
    fn weighted_mode_rejects_zero_weight() {
        let graph = Graph::new(
            GraphKind::DiGraph,
            [Node::new("a"), Node::new("b")],
            [Edge::new("ab", "a", "b").with_weight(0.0)],
        )
        .unwrap();
        assert!(matches!(
            graph.betweenness_centrality(BetweennessOptions {
                weight: PathWeight::Weighted,
                ..BetweennessOptions::default()
            }),
            Err(GraphError::InvalidOption(_))
        ));
    }

    #[test]
    fn parallel_edges_remain_distinct_in_edge_scores() {
        let graph = Graph::new(
            GraphKind::MultiDiGraph,
            [Node::new("a"), Node::new("b")],
            [Edge::new("one", "a", "b"), Edge::new("two", "a", "b")],
        )
        .unwrap();
        let scores = graph
            .edge_betweenness_centrality(BetweennessOptions {
                normalized: false,
                ..BetweennessOptions::default()
            })
            .unwrap();
        assert_eq!(scores.len(), 2);
        assert_abs_diff_eq!(scores[0].score, 0.5);
        assert_abs_diff_eq!(scores[1].score, 0.5);
    }

    #[test]
    fn diamond_scores_match_networkx_3_4_2_golden_fixture() {
        let graph = Graph::new(
            GraphKind::Graph,
            ["a", "b", "c", "d"].into_iter().map(Node::new),
            [
                Edge::new("ab", "a", "b"),
                Edge::new("ac", "a", "c"),
                Edge::new("bd", "b", "d"),
                Edge::new("cd", "c", "d"),
                Edge::new("bc", "b", "c"),
            ],
        )
        .unwrap();
        let nodes = graph
            .betweenness_centrality(BetweennessOptions::default())
            .unwrap();
        assert_abs_diff_eq!(nodes[0].score, 0.0);
        assert_abs_diff_eq!(nodes[1].score, 1.0 / 6.0);
        assert_abs_diff_eq!(nodes[2].score, 1.0 / 6.0);
        assert_abs_diff_eq!(nodes[3].score, 0.0);

        let endpoints = graph
            .betweenness_centrality(BetweennessOptions {
                endpoints: true,
                ..BetweennessOptions::default()
            })
            .unwrap();
        assert_abs_diff_eq!(endpoints[0].score, 0.5);
        assert_abs_diff_eq!(endpoints[1].score, 7.0 / 12.0);
        assert_abs_diff_eq!(endpoints[2].score, 7.0 / 12.0);
        assert_abs_diff_eq!(endpoints[3].score, 0.5);
    }

    #[test]
    fn weighted_directed_scores_match_networkx_3_4_2_golden_fixture() {
        let graph = Graph::new(
            GraphKind::DiGraph,
            ["a", "b", "c", "d"].into_iter().map(Node::new),
            [
                Edge::new("ab", "a", "b").with_weight(1.0),
                Edge::new("ac", "a", "c").with_weight(1.0),
                Edge::new("ad", "a", "d").with_weight(3.0),
                Edge::new("bd", "b", "d").with_weight(1.0),
                Edge::new("cd", "c", "d").with_weight(1.0),
            ],
        )
        .unwrap();
        let options = BetweennessOptions {
            normalized: false,
            weight: PathWeight::Weighted,
            ..BetweennessOptions::default()
        };
        let nodes = graph.betweenness_centrality(options).unwrap();
        assert_abs_diff_eq!(nodes[1].score, 0.5);
        assert_abs_diff_eq!(nodes[2].score, 0.5);
        let edges = graph.edge_betweenness_centrality(options).unwrap();
        let by_id = edges
            .into_iter()
            .map(|edge| (edge.edge_id, edge.score))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_abs_diff_eq!(by_id[&crate::EdgeId::from("ab")], 1.5);
        assert_abs_diff_eq!(by_id[&crate::EdgeId::from("ac")], 1.5);
        assert_abs_diff_eq!(by_id[&crate::EdgeId::from("ad")], 0.0);
        assert_abs_diff_eq!(by_id[&crate::EdgeId::from("bd")], 1.5);
        assert_abs_diff_eq!(by_id[&crate::EdgeId::from("cd")], 1.5);
    }

    #[test]
    fn multigraph_scores_match_networkx_3_4_2_golden_fixture() {
        let graph = Graph::new(
            GraphKind::MultiGraph,
            [Node::new("a"), Node::new("b"), Node::new("c")],
            [
                Edge::new("one", "a", "b"),
                Edge::new("two", "a", "b"),
                Edge::new("bc", "b", "c"),
            ],
        )
        .unwrap();
        let options = BetweennessOptions {
            normalized: false,
            ..BetweennessOptions::default()
        };
        let nodes = graph.betweenness_centrality(options).unwrap();
        assert_abs_diff_eq!(nodes[1].score, 1.0);
        let by_id = graph
            .edge_betweenness_centrality(options)
            .unwrap()
            .into_iter()
            .map(|edge| (edge.edge_id, edge.score))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_abs_diff_eq!(by_id[&crate::EdgeId::from("one")], 1.0);
        assert_abs_diff_eq!(by_id[&crate::EdgeId::from("two")], 1.0);
        assert_abs_diff_eq!(by_id[&crate::EdgeId::from("bc")], 2.0);
    }

    #[test]
    fn graphify_auto_and_sampled_edge_modes_cover_exact_and_clamped_sources() {
        let graph = path(GraphKind::DiGraph);
        let exact = graph
            .betweenness_centrality(BetweennessOptions::graphify_default())
            .unwrap();
        assert_eq!(exact.len(), 3);

        let options = BetweennessOptions {
            mode: BetweennessMode::Auto {
                exact_through: 1,
                sample_count: NonZeroUsize::new(100).unwrap(),
                seed: 42,
            },
            ..BetweennessOptions::default()
        };
        assert_eq!(
            graph.edge_betweenness_centrality(options).unwrap(),
            graph.edge_betweenness_centrality(options).unwrap()
        );
        let empty = Graph::new(GraphKind::DiGraph, [], []).unwrap();
        assert!(empty
            .edge_betweenness_centrality(BetweennessOptions {
                mode: BetweennessMode::Sampled {
                    sample_count: NonZeroUsize::new(1).unwrap(),
                    seed: 7,
                },
                ..BetweennessOptions::default()
            })
            .unwrap()
            .is_empty());
    }
}
