use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use helix_graph::{
    BetweennessMode, BetweennessOptions, CycleOptions, Edge, Graph, GraphKind, LayoutOptions,
    LouvainOptions, Node, PathWeight, TraversalDirection, TraversalOptions,
};

fn graph(node_count: usize, degree: usize, direction: GraphKind) -> Graph {
    let nodes = (0..node_count).map(|node| Node::new(format!("n{node:06}")));
    let edges = (0..node_count).flat_map(|source| {
        (1..=degree).map(move |offset| {
            let target = (source + offset) % node_count;
            Edge::new(
                format!("e{source:06}-{target:06}"),
                format!("n{source:06}"),
                format!("n{target:06}"),
            )
            .with_weight((offset % 3 + 1) as f64)
            .with_label(if offset % 2 == 0 { "even" } else { "odd" })
        })
    });
    Graph::new(direction, nodes, edges).expect("benchmark topology is valid")
}

fn construction(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("construction");
    for nodes in [1_000, 10_000] {
        group.bench_with_input(BenchmarkId::new("csr", nodes), &nodes, |bencher, nodes| {
            bencher.iter(|| graph(black_box(*nodes), 4, GraphKind::DiGraph));
        });
    }
    group.finish();
}

fn algorithms(criterion: &mut Criterion) {
    let directed = graph(1_000, 4, GraphKind::DiGraph);
    let undirected = graph(1_000, 4, GraphKind::Graph);
    let mut group = criterion.benchmark_group("algorithms_1000_nodes_4000_edges");
    group.bench_function("sampled_node_betweenness", |bencher| {
        bencher.iter(|| {
            directed
                .betweenness_centrality(BetweennessOptions {
                    mode: BetweennessMode::Sampled {
                        sample_count: NonZeroUsize::new(100).expect("non-zero"),
                        seed: 42,
                    },
                    normalized: true,
                    endpoints: false,
                    weight: PathWeight::Unweighted,
                })
                .expect("benchmark graph supports centrality")
        });
    });
    group.bench_function("sampled_edge_betweenness", |bencher| {
        bencher.iter(|| {
            directed
                .edge_betweenness_centrality(BetweennessOptions {
                    mode: BetweennessMode::Sampled {
                        sample_count: NonZeroUsize::new(100).expect("non-zero"),
                        seed: 42,
                    },
                    ..BetweennessOptions::default()
                })
                .expect("benchmark graph supports centrality")
        });
    });
    group.bench_function("bounded_cycles", |bencher| {
        bencher.iter(|| {
            directed.simple_cycles(CycleOptions {
                length_bound: NonZeroUsize::new(5).expect("non-zero"),
                max_cycles: NonZeroUsize::new(1_000),
            })
        });
    });
    group.bench_function("louvain", |bencher| {
        bencher.iter(|| {
            undirected
                .louvain_communities(LouvainOptions::default())
                .expect("benchmark graph is undirected")
        });
    });
    group.bench_function("breadth_first", |bencher| {
        bencher.iter(|| {
            directed
                .traverse(&TraversalOptions::breadth_first(
                    ["n000000".to_string()],
                    10,
                ))
                .expect("benchmark seed exists")
        });
    });
    group.bench_function("shortest_path", |bencher| {
        bencher.iter(|| {
            directed.shortest_path(
                "n000000",
                "n000999",
                TraversalDirection::Out,
                &BTreeSet::new(),
                None,
            )
        });
    });
    group.bench_function("degree_all", |bencher| {
        bencher.iter(|| directed.degrees(helix_graph::DegreeKind::Total));
    });
    group.bench_function("induced_subgraph", |bencher| {
        let selected = (0..500)
            .map(|node| format!("n{node:06}"))
            .collect::<Vec<_>>();
        bencher.iter(|| {
            directed
                .induced_subgraph(black_box(selected.clone()))
                .expect("selected nodes exist")
        });
    });
    group.bench_function("spring_layout_500", |bencher| {
        let layout_graph = graph(500, 4, GraphKind::Graph);
        bencher.iter(|| {
            layout_graph
                .spring_layout(LayoutOptions::default())
                .expect("layout options are valid")
        });
    });
    group.finish();
}

criterion_group!(benches, construction, algorithms);
criterion_main!(benches);
