use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use approx::assert_abs_diff_eq;
use helix_graph::{BetweennessOptions, Cycle, CycleOptions, Edge, Graph, GraphKind, Node};
use serde::Deserialize;

#[derive(Deserialize)]
struct Fixture {
    source: String,
    graph: FixtureGraph,
    node_betweenness_normalized: BTreeMap<String, f64>,
    edge_betweenness_normalized: BTreeMap<String, f64>,
    cycles_length_bound_4: Vec<Cycle>,
}

#[derive(Deserialize)]
struct FixtureGraph {
    direction: GraphKind,
    nodes: Vec<String>,
    edges: Vec<Edge>,
}

#[test]
fn native_results_match_checked_in_networkx_3_4_2_fixture() {
    let fixture: Fixture =
        serde_json::from_str(include_str!("../fixtures/networkx-3.4.2-diamond.json"))
            .expect("golden fixture is valid JSON");
    assert_eq!(fixture.source, "NetworkX 3.4.2");
    let graph = Graph::new(
        fixture.graph.direction,
        fixture.graph.nodes.into_iter().map(Node::new),
        fixture.graph.edges,
    )
    .expect("golden topology is valid");

    for score in graph
        .betweenness_centrality(BetweennessOptions::default())
        .expect("node centrality")
    {
        assert_abs_diff_eq!(
            score.score,
            fixture.node_betweenness_normalized[score
                .node_id
                .as_string()
                .expect("fixture node IDs are strings")],
            epsilon = 1e-12
        );
    }
    for score in graph
        .edge_betweenness_centrality(BetweennessOptions::default())
        .expect("edge centrality")
    {
        assert_abs_diff_eq!(
            score.score,
            fixture.edge_betweenness_normalized[score.edge_id.stored_id()],
            epsilon = 1e-12
        );
    }
    let cycles = graph.simple_cycles(CycleOptions {
        length_bound: NonZeroUsize::new(4).expect("non-zero"),
        max_cycles: None,
    });
    assert_eq!(cycles.cycles, fixture.cycles_length_bound_4);
    assert!(!cycles.truncated);
}
