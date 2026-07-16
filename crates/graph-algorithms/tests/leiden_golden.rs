use approx::assert_abs_diff_eq;
use helix_graph::{Edge, Graph, GraphKind, LeidenOptions, Node};
use serde::Deserialize;

#[derive(Deserialize)]
struct GoldenFile {
    source: Source,
    defaults: Defaults,
    fixtures: Vec<Fixture>,
}

#[derive(Deserialize)]
struct Source {
    graphify_commit: String,
    graspologic: String,
    graspologic_native: String,
    networkx: String,
}

#[derive(Deserialize)]
struct Defaults {
    resolution: f64,
    randomness: f64,
    seed: u64,
    trials: usize,
    iterations: usize,
}

#[derive(Deserialize)]
struct Fixture {
    name: String,
    nodes: Vec<String>,
    edges: Vec<(String, String, f64)>,
    communities: Vec<Vec<String>>,
    quality: f64,
}

#[test]
fn native_weighted_leiden_matches_audit_baseline_fixtures() {
    let golden: GoldenFile =
        serde_json::from_str(include_str!("../fixtures/graspologic-3.4.4-leiden.json"))
            .expect("golden fixture is valid JSON");
    assert_eq!(
        golden.source.graphify_commit,
        "91f4d120b630ee35c79bf3c75ccd186870a808f9"
    );
    assert_eq!(golden.source.graspologic, "3.4.4");
    assert_eq!(golden.source.graspologic_native, "1.2.5");
    assert_eq!(golden.source.networkx, "3.4.2");
    assert_eq!(golden.defaults.resolution, 1.0);
    assert_eq!(golden.defaults.randomness, 0.001);
    assert_eq!(golden.defaults.seed, 42);
    assert_eq!(golden.defaults.trials, 1);
    assert_eq!(golden.defaults.iterations, 1);

    for fixture in golden.fixtures {
        let graph = Graph::new(
            GraphKind::Graph,
            fixture.nodes.into_iter().map(Node::new),
            fixture
                .edges
                .into_iter()
                .enumerate()
                .map(|(index, (source, target, weight))| {
                    Edge::new(format!("{}-{index}", fixture.name), source, target)
                        .with_weight(weight)
                }),
        )
        .expect("golden graph is valid");
        let result = graph
            .leiden(LeidenOptions::default())
            .expect("weighted Leiden result");
        let communities = result
            .communities
            .into_iter()
            .map(|community| {
                community
                    .node_ids
                    .into_iter()
                    .map(|node| {
                        node.as_string()
                            .expect("fixture IDs are strings")
                            .to_string()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(communities, fixture.communities, "{}", fixture.name);
        assert_abs_diff_eq!(result.modularity, fixture.quality, epsilon = 1e-12);
    }
}
