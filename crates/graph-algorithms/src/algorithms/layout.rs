use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};

use crate::{Graph, GraphError, NodeId, PositiveFiniteF64};

/// Deterministic Fruchterman-Reingold layout options.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LayoutOptions {
    /// Optional ideal edge distance. `None` uses `sqrt(1 / node_count)`.
    pub k: Option<PositiveFiniteF64>,
    /// Number of force/cooling iterations.
    pub iterations: NonZeroUsize,
    /// Deterministic position seed.
    pub seed: u64,
    /// Whether edge attraction multiplies selected edge weights.
    pub weighted: bool,
    /// Optional deterministic position overrides. Nodes without an override
    /// retain their seeded initial position.
    pub initial_positions: Vec<NodePosition>,
}

impl Default for LayoutOptions {
    fn default() -> Self {
        Self {
            k: None,
            iterations: NonZeroUsize::new(50).expect("50 is non-zero"),
            seed: 42,
            weighted: true,
            initial_positions: Vec::new(),
        }
    }
}

/// One final two-dimensional node position.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodePosition {
    /// External node identity.
    pub node_id: NodeId,
    /// Rescaled horizontal coordinate in `[-1, 1]`.
    pub x: f64,
    /// Rescaled vertical coordinate in `[-1, 1]`.
    pub y: f64,
}

impl Graph {
    /// Compute deterministic Fruchterman-Reingold positions.
    ///
    /// This exact kernel is quadratic in node count per iteration. It is
    /// intentionally explicit rather than silently switching to an
    /// approximate layout for larger graphs.
    pub fn spring_layout(&self, options: LayoutOptions) -> Result<Vec<NodePosition>, GraphError> {
        let mut positioned = BTreeSet::new();
        for position in &options.initial_positions {
            if !position.x.is_finite() || !position.y.is_finite() {
                return Err(GraphError::InvalidOption(format!(
                    "initial position for {} must be finite",
                    position.node_id
                )));
            }
            self.node_index(&position.node_id)?;
            if !positioned.insert(position.node_id.clone()) {
                return Err(GraphError::InvalidOption(format!(
                    "duplicate initial position for {}",
                    position.node_id
                )));
            }
        }
        match self.node_count() {
            0 => return Ok(Vec::new()),
            1 => {
                return Ok(vec![NodePosition {
                    node_id: self.node_id(0).clone(),
                    x: 0.0,
                    y: 0.0,
                }]);
            }
            _ => {}
        }

        let k = options
            .k
            .map(PositiveFiniteF64::get)
            .unwrap_or_else(|| (1.0 / self.node_count() as f64).sqrt());
        let mut seed = options.seed;
        let mut positions = (0..self.node_count())
            .map(|_| (random_unit(&mut seed), random_unit(&mut seed)))
            .collect::<Vec<_>>();
        for position in &options.initial_positions {
            let node = self.node_index(&position.node_id)?;
            positions[node] = (position.x, position.y);
        }
        const MIN_DISTANCE: f64 = 1e-9;
        for step in 0..options.iterations.get() {
            let mut displacement = vec![(0.0, 0.0); self.node_count()];
            for left in 0..self.node_count() {
                for right in left + 1..self.node_count() {
                    let mut dx = positions[left].0 - positions[right].0;
                    let mut dy = positions[left].1 - positions[right].1;
                    let mut distance = dx.hypot(dy);
                    if distance < MIN_DISTANCE {
                        let jitter = deterministic_jitter(left, right);
                        dx = jitter.0;
                        dy = jitter.1;
                        distance = dx.hypot(dy);
                    }
                    let force = k * k / distance;
                    let force_x = dx / distance * force;
                    let force_y = dy / distance * force;
                    displacement[left].0 += force_x;
                    displacement[left].1 += force_y;
                    displacement[right].0 -= force_x;
                    displacement[right].1 -= force_y;
                }
            }
            for edge in self.edges() {
                let source = self.node_index(&edge.source)?;
                let target = self.node_index(&edge.target)?;
                if source == target {
                    continue;
                }
                let dx = positions[source].0 - positions[target].0;
                let dy = positions[source].1 - positions[target].1;
                let distance = dx.hypot(dy).max(MIN_DISTANCE);
                let weight = if options.weighted {
                    edge.weight.unwrap_or(1.0)
                } else {
                    1.0
                };
                let force = distance * distance / k * weight;
                let force_x = dx / distance * force;
                let force_y = dy / distance * force;
                displacement[source].0 -= force_x;
                displacement[source].1 -= force_y;
                displacement[target].0 += force_x;
                displacement[target].1 += force_y;
            }
            let temperature = 0.1 * (1.0 - step as f64 / options.iterations.get() as f64).max(0.0);
            for node in 0..self.node_count() {
                let (dx, dy) = displacement[node];
                let distance = dx.hypot(dy).max(MIN_DISTANCE);
                positions[node].0 += dx / distance * distance.min(temperature);
                positions[node].1 += dy / distance * distance.min(temperature);
            }
        }
        rescale_positions(&mut positions);
        Ok(positions
            .into_iter()
            .enumerate()
            .map(|(node, (x, y))| NodePosition {
                node_id: self.node_id(node).clone(),
                x,
                y,
            })
            .collect())
    }
}

fn random_unit(seed: &mut u64) -> f64 {
    *seed = seed
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    (*seed >> 11) as f64 / ((1_u64 << 53) - 1) as f64
}

fn deterministic_jitter(left: usize, right: usize) -> (f64, f64) {
    let angle = ((left.wrapping_mul(31) ^ right.wrapping_mul(17)) % 360) as f64
        * std::f64::consts::PI
        / 180.0;
    (angle.cos() * 1e-6, angle.sin() * 1e-6)
}

fn rescale_positions(positions: &mut [(f64, f64)]) {
    let mean_x = positions.iter().map(|position| position.0).sum::<f64>() / positions.len() as f64;
    let mean_y = positions.iter().map(|position| position.1).sum::<f64>() / positions.len() as f64;
    let scale = positions
        .iter()
        .map(|(x, y)| (x - mean_x).abs().max((y - mean_y).abs()))
        .fold(0.0, f64::max);
    if scale == 0.0 {
        return;
    }
    for position in positions {
        position.0 = (position.0 - mean_x) / scale;
        position.1 = (position.1 - mean_y) / scale;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Edge, GraphKind, Node};

    #[test]
    fn layout_handles_empty_and_singleton_graphs() {
        let empty = Graph::new(GraphKind::Graph, [], []).unwrap();
        assert!(empty
            .spring_layout(LayoutOptions::default())
            .unwrap()
            .is_empty());

        let singleton = Graph::new(GraphKind::Graph, [Node::new("a")], []).unwrap();
        assert_eq!(
            singleton.spring_layout(LayoutOptions::default()).unwrap(),
            [NodePosition {
                node_id: "a".into(),
                x: 0.0,
                y: 0.0,
            }]
        );
    }

    #[test]
    fn layout_is_finite_seeded_and_rescaled() {
        let graph = Graph::new(
            GraphKind::Graph,
            [Node::new("a"), Node::new("b"), Node::new("c")],
            [Edge::new("ab", "a", "b"), Edge::new("bc", "b", "c")],
        )
        .unwrap();
        let first = graph.spring_layout(LayoutOptions::default()).unwrap();
        let second = graph.spring_layout(LayoutOptions::default()).unwrap();
        assert_eq!(first, second);
        assert!(first.iter().all(|position| {
            position.x.is_finite()
                && position.y.is_finite()
                && position.x.abs() <= 1.0
                && position.y.abs() <= 1.0
        }));
    }

    #[test]
    fn layout_k_type_rejects_invalid_values() {
        assert!(PositiveFiniteF64::new(0.0).is_err());
        assert!(PositiveFiniteF64::new(f64::NAN).is_err());
    }

    #[test]
    fn layout_accepts_partial_initial_positions_and_rejects_bad_ones() {
        let graph = Graph::new(
            GraphKind::Graph,
            [Node::new("a"), Node::new("b")],
            [Edge::new("ab", "a", "b")],
        )
        .unwrap();
        let options = LayoutOptions {
            initial_positions: vec![NodePosition {
                node_id: "a".into(),
                x: 0.25,
                y: 0.75,
            }],
            ..LayoutOptions::default()
        };
        assert_eq!(graph.spring_layout(options.clone()).unwrap().len(), 2);
        let invalid = LayoutOptions {
            initial_positions: vec![NodePosition {
                node_id: "missing".into(),
                x: 0.0,
                y: 0.0,
            }],
            ..options
        };
        assert!(matches!(
            graph.spring_layout(invalid),
            Err(GraphError::UnknownNode(_))
        ));

        let non_finite = LayoutOptions {
            initial_positions: vec![NodePosition {
                node_id: "a".into(),
                x: f64::NAN,
                y: 0.0,
            }],
            ..LayoutOptions::default()
        };
        assert!(matches!(
            graph.spring_layout(non_finite),
            Err(GraphError::InvalidOption(_))
        ));
        let duplicate = LayoutOptions {
            initial_positions: vec![
                NodePosition {
                    node_id: "a".into(),
                    x: 0.0,
                    y: 0.0,
                },
                NodePosition {
                    node_id: "a".into(),
                    x: 1.0,
                    y: 1.0,
                },
            ],
            ..LayoutOptions::default()
        };
        assert!(matches!(
            graph.spring_layout(duplicate),
            Err(GraphError::InvalidOption(_))
        ));
    }

    #[test]
    fn layout_covers_coincident_unweighted_parallel_and_self_loop_forces() {
        let graph = Graph::new(
            GraphKind::MultiGraph,
            [Node::new("a"), Node::new("b")],
            [
                Edge::new("aa", "a", "a").with_weight(10.0),
                Edge::new("ab1", "a", "b").with_weight(2.0),
                Edge::new("ab2", "a", "b").with_weight(3.0),
            ],
        )
        .unwrap();
        let positions = graph
            .spring_layout(LayoutOptions {
                weighted: false,
                initial_positions: vec![
                    NodePosition {
                        node_id: "a".into(),
                        x: 0.0,
                        y: 0.0,
                    },
                    NodePosition {
                        node_id: "b".into(),
                        x: 0.0,
                        y: 0.0,
                    },
                ],
                ..LayoutOptions::default()
            })
            .unwrap();
        assert!(positions.iter().all(|position| position.x.is_finite()));
        let mut same = [(1.0, 1.0), (1.0, 1.0)];
        rescale_positions(&mut same);
        assert_eq!(same, [(1.0, 1.0), (1.0, 1.0)]);
    }
}
