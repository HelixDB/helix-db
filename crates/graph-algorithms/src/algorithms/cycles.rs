use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};

use super::TraversalDirection;
use crate::{EdgeId, Graph, NodeId};

/// Bounded cycle-enumeration options.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CycleOptions {
    /// Maximum number of edges in a returned cycle.
    pub length_bound: NonZeroUsize,
    /// Optional output cap.
    pub max_cycles: Option<NonZeroUsize>,
}

/// One canonical simple cycle. The first node is not repeated at the end.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cycle {
    /// Canonically rotated node sequence.
    pub node_ids: Vec<NodeId>,
    /// One representative edge per adjacent node pair, including the closing
    /// edge from the last node to the first.
    pub edge_ids: Vec<EdgeId>,
}

/// Bounded cycle output and truncation state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CycleResult {
    /// Cycles in canonical deterministic order.
    pub cycles: Vec<Cycle>,
    /// True only when `max_cycles` stopped enumeration.
    pub truncated: bool,
}

impl Graph {
    /// Enumerate bounded simple cycles with in-search pruning.
    pub fn simple_cycles(&self, options: CycleOptions) -> CycleResult {
        let components = if self.is_directed() {
            self.strongly_connected_components()
        } else {
            self.connected_components()
        };
        let mut canonical = BTreeMap::<Vec<NodeId>, Vec<EdgeId>>::new();
        let mut truncated = false;
        for component in components {
            if component.len() == 1 {
                let node = component[0];
                let has_self_loop = self
                    .arcs(node, TraversalDirection::Out)
                    .any(|arc| arc.neighbor == node);
                if !has_self_loop {
                    continue;
                }
            }
            let component_set = component.iter().copied().collect::<BTreeSet<_>>();
            for start in &component {
                let mut in_path = vec![false; self.node_count()];
                in_path[*start] = true;
                let mut path_nodes = vec![*start];
                let mut path_edges = Vec::new();
                if self.enumerate_cycles_from(
                    *start,
                    *start,
                    &component_set,
                    options,
                    &mut in_path,
                    &mut path_nodes,
                    &mut path_edges,
                    &mut canonical,
                ) {
                    truncated = true;
                    break;
                }
            }
            if truncated {
                break;
            }
        }
        CycleResult {
            cycles: canonical
                .into_iter()
                .map(|(node_ids, edge_ids)| Cycle { node_ids, edge_ids })
                .collect(),
            truncated,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn enumerate_cycles_from(
        &self,
        start: usize,
        current: usize,
        component: &BTreeSet<usize>,
        options: CycleOptions,
        in_path: &mut [bool],
        path_nodes: &mut Vec<usize>,
        path_edges: &mut Vec<usize>,
        canonical: &mut BTreeMap<Vec<NodeId>, Vec<EdgeId>>,
    ) -> bool {
        let direction = if self.is_directed() {
            TraversalDirection::Out
        } else {
            TraversalDirection::Both
        };
        for arc in self.arcs(current, direction) {
            if !component.contains(&arc.neighbor) || path_edges.contains(&arc.edge) {
                continue;
            }
            if arc.neighbor == start {
                let cycle_len = path_edges.len() + 1;
                if cycle_len <= options.length_bound.get() {
                    path_edges.push(arc.edge);
                    let (nodes, edges) = self.canonical_cycle(path_nodes, path_edges);
                    match canonical.get_mut(&nodes) {
                        Some(existing) if edges < *existing => *existing = edges,
                        Some(_) => {}
                        None => {
                            canonical.insert(nodes, edges);
                        }
                    }
                    path_edges.pop();
                    if options
                        .max_cycles
                        .is_some_and(|limit| canonical.len() >= limit.get())
                    {
                        return true;
                    }
                }
                continue;
            }
            if in_path[arc.neighbor] || path_edges.len() + 1 >= options.length_bound.get() {
                continue;
            }
            in_path[arc.neighbor] = true;
            path_nodes.push(arc.neighbor);
            path_edges.push(arc.edge);
            let stop = self.enumerate_cycles_from(
                start,
                arc.neighbor,
                component,
                options,
                in_path,
                path_nodes,
                path_edges,
                canonical,
            );
            path_edges.pop();
            path_nodes.pop();
            in_path[arc.neighbor] = false;
            if stop {
                return true;
            }
        }
        false
    }

    fn canonical_cycle(
        &self,
        path_nodes: &[usize],
        path_edges: &[usize],
    ) -> (Vec<NodeId>, Vec<EdgeId>) {
        let nodes = path_nodes
            .iter()
            .map(|node| self.node_id(*node).clone())
            .collect::<Vec<_>>();
        let edges = path_edges
            .iter()
            .map(|edge| self.edge_at(*edge).id.clone())
            .collect::<Vec<_>>();
        let mut candidates = rotations(&nodes, &edges);
        if !self.is_directed() {
            let reverse_nodes = nodes.iter().cloned().rev().collect::<Vec<_>>();
            let edge_count = edges.len();
            let reverse_edges = (0..edge_count)
                .map(|index| edges[(edge_count + edge_count - 2 - index) % edge_count].clone())
                .collect::<Vec<_>>();
            candidates.extend(rotations(&reverse_nodes, &reverse_edges));
        }
        candidates
            .into_iter()
            .min()
            .expect("a cycle always has at least one rotation")
    }

    fn connected_components(&self) -> Vec<Vec<usize>> {
        let mut visited = vec![false; self.node_count()];
        let mut components = Vec::new();
        for start in 0..self.node_count() {
            if visited[start] {
                continue;
            }
            visited[start] = true;
            let mut queue = VecDeque::from([start]);
            let mut component = Vec::new();
            while let Some(node) = queue.pop_front() {
                component.push(node);
                for arc in self.arcs(node, TraversalDirection::Both) {
                    if !visited[arc.neighbor] {
                        visited[arc.neighbor] = true;
                        queue.push_back(arc.neighbor);
                    }
                }
            }
            components.push(component);
        }
        components
    }

    fn strongly_connected_components(&self) -> Vec<Vec<usize>> {
        struct Tarjan {
            next_index: usize,
            indexes: Vec<Option<usize>>,
            lowlinks: Vec<usize>,
            stack: Vec<usize>,
            on_stack: Vec<bool>,
            components: Vec<Vec<usize>>,
        }

        fn visit(graph: &Graph, node: usize, state: &mut Tarjan) {
            let node_index = state.next_index;
            state.next_index += 1;
            state.indexes[node] = Some(node_index);
            state.lowlinks[node] = node_index;
            state.stack.push(node);
            state.on_stack[node] = true;
            for arc in graph.arcs(node, TraversalDirection::Out) {
                match state.indexes[arc.neighbor] {
                    None => {
                        visit(graph, arc.neighbor, state);
                        state.lowlinks[node] =
                            state.lowlinks[node].min(state.lowlinks[arc.neighbor]);
                    }
                    Some(index) if state.on_stack[arc.neighbor] => {
                        state.lowlinks[node] = state.lowlinks[node].min(index);
                    }
                    Some(_) => {}
                }
            }
            if state.lowlinks[node] == node_index {
                let mut component = Vec::new();
                loop {
                    let member = state.stack.pop().expect("SCC root is on the Tarjan stack");
                    state.on_stack[member] = false;
                    component.push(member);
                    if member == node {
                        break;
                    }
                }
                component.sort_by(|left, right| graph.node_id(*left).cmp(graph.node_id(*right)));
                state.components.push(component);
            }
        }

        let mut state = Tarjan {
            next_index: 0,
            indexes: vec![None; self.node_count()],
            lowlinks: vec![0; self.node_count()],
            stack: Vec::new(),
            on_stack: vec![false; self.node_count()],
            components: Vec::new(),
        };
        for node in 0..self.node_count() {
            if state.indexes[node].is_none() {
                visit(self, node, &mut state);
            }
        }
        state
            .components
            .sort_by(|left, right| self.node_id(left[0]).cmp(self.node_id(right[0])));
        state.components
    }
}

fn rotations(nodes: &[NodeId], edges: &[EdgeId]) -> Vec<(Vec<NodeId>, Vec<EdgeId>)> {
    (0..nodes.len())
        .map(|offset| {
            (
                nodes[offset..]
                    .iter()
                    .chain(nodes[..offset].iter())
                    .cloned()
                    .collect(),
                edges[offset..]
                    .iter()
                    .chain(edges[..offset].iter())
                    .cloned()
                    .collect(),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Edge, GraphKind, Node};

    #[test]
    fn directed_cycles_are_bounded_canonical_and_include_self_loops() {
        let graph = Graph::new(
            GraphKind::DiGraph,
            [Node::new("a"), Node::new("b"), Node::new("c")],
            [
                Edge::new("aa", "a", "a"),
                Edge::new("ab", "a", "b"),
                Edge::new("ba", "b", "a"),
                Edge::new("bc", "b", "c"),
                Edge::new("ca", "c", "a"),
            ],
        )
        .unwrap();
        let result = graph.simple_cycles(CycleOptions {
            length_bound: NonZeroUsize::new(2).unwrap(),
            max_cycles: None,
        });
        assert_eq!(
            result
                .cycles
                .iter()
                .map(|cycle| cycle.node_ids.clone())
                .collect::<Vec<_>>(),
            [
                vec!["a".to_string()],
                vec!["a".to_string(), "b".to_string()]
            ]
        );
        assert!(!result.truncated);
    }

    #[test]
    fn cycle_limit_stops_enumeration() {
        let graph = Graph::new(
            GraphKind::DiGraph,
            [Node::new("a"), Node::new("b")],
            [Edge::new("aa", "a", "a"), Edge::new("bb", "b", "b")],
        )
        .unwrap();
        let result = graph.simple_cycles(CycleOptions {
            length_bound: NonZeroUsize::new(1).unwrap(),
            max_cycles: NonZeroUsize::new(1),
        });
        assert_eq!(result.cycles.len(), 1);
        assert!(result.truncated);
    }

    #[test]
    fn undirected_reverse_cycles_deduplicate() {
        let graph = Graph::new(
            GraphKind::Graph,
            [Node::new("a"), Node::new("b"), Node::new("c")],
            [
                Edge::new("ab", "a", "b"),
                Edge::new("bc", "b", "c"),
                Edge::new("ca", "c", "a"),
            ],
        )
        .unwrap();
        let result = graph.simple_cycles(CycleOptions {
            length_bound: NonZeroUsize::new(3).unwrap(),
            max_cycles: None,
        });
        assert!(result.cycles.iter().any(|cycle| cycle.node_ids.len() == 3));
    }

    #[test]
    fn undirected_two_cycle_requires_distinct_parallel_edges() {
        let simple = Graph::new(
            GraphKind::Graph,
            [Node::new("a"), Node::new("b")],
            [Edge::new("ab", "a", "b")],
        )
        .unwrap();
        assert!(simple
            .simple_cycles(CycleOptions {
                length_bound: NonZeroUsize::new(2).unwrap(),
                max_cycles: None,
            })
            .cycles
            .is_empty());

        let parallel = Graph::new(
            GraphKind::MultiGraph,
            [Node::new("a"), Node::new("b")],
            [Edge::new("one", "a", "b"), Edge::new("two", "a", "b")],
        )
        .unwrap();
        let result = parallel.simple_cycles(CycleOptions {
            length_bound: NonZeroUsize::new(2).unwrap(),
            max_cycles: None,
        });
        assert_eq!(result.cycles.len(), 1);
        assert_eq!(result.cycles[0].node_ids, ["a", "b"]);
        assert_eq!(result.cycles[0].edge_ids.len(), 2);
    }
}
