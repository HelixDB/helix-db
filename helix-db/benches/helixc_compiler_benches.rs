#[cfg(feature = "compiler")]
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
#[cfg(feature = "compiler")]
use helix_db::helixc::{
    analyzer,
    parser::{HelixParser, write_to_temp_file},
};

#[cfg(feature = "compiler")]
const SCHEMA_HEAVY: &str = include_str!("../src/helixc/fixtures/benchmarks/schema_heavy.hql");
#[cfg(feature = "compiler")]
const TRAVERSAL_HEAVY: &str = include_str!("../src/helixc/fixtures/benchmarks/traversal_heavy.hql");
#[cfg(feature = "compiler")]
const PROJECTION_HEAVY: &str =
    include_str!("../src/helixc/fixtures/benchmarks/projection_heavy.hql");
#[cfg(feature = "compiler")]
const VECTOR_SEARCH_HEAVY: &str =
    include_str!("../src/helixc/fixtures/benchmarks/vector_search_heavy.hql");
#[cfg(feature = "compiler")]
const MIGRATION_HEAVY: &str = include_str!("../src/helixc/fixtures/benchmarks/migration_heavy.hql");

#[cfg(feature = "compiler")]
fn make_corpus(size: &str) -> String {
    match size {
        "small" => [SCHEMA_HEAVY, TRAVERSAL_HEAVY].join("\n"),
        "medium" => [
            SCHEMA_HEAVY,
            TRAVERSAL_HEAVY,
            PROJECTION_HEAVY,
            VECTOR_SEARCH_HEAVY,
        ]
        .join("\n"),
        "large" => [
            SCHEMA_HEAVY,
            TRAVERSAL_HEAVY,
            PROJECTION_HEAVY,
            VECTOR_SEARCH_HEAVY,
            MIGRATION_HEAVY,
            SCHEMA_HEAVY,
            TRAVERSAL_HEAVY,
            PROJECTION_HEAVY,
            VECTOR_SEARCH_HEAVY,
        ]
        .join("\n"),
        _ => unreachable!("unknown corpus size"),
    }
}

#[cfg(feature = "compiler")]
fn benchmark_compiler(c: &mut Criterion) {
    let mut group = c.benchmark_group("helixc_compiler");
    let sizes = ["small", "medium", "large"];

    for label in sizes {
        let corpus = make_corpus(label);
        let content = write_to_temp_file(vec![corpus.as_str()]);

        group.bench_with_input(BenchmarkId::new("parse", label), &content, |b, content| {
            b.iter(|| {
                let parsed = HelixParser::parse_source(black_box(content));
                black_box(parsed.expect("parse should succeed"));
            });
        });

        group.bench_with_input(
            BenchmarkId::new("parse_analyze", label),
            &content,
            |b, content| {
                b.iter(|| {
                    let parsed = HelixParser::parse_source(black_box(content))
                        .expect("parse should succeed");
                    let analyzed = analyzer::analyze(black_box(&parsed));
                    black_box(analyzed.expect("analyze should succeed"));
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("parse_analyze_generate", label),
            &content,
            |b, content| {
                b.iter(|| {
                    let parsed = HelixParser::parse_source(black_box(content))
                        .expect("parse should succeed");
                    let (_, generated) =
                        analyzer::analyze(black_box(&parsed)).expect("analyze should succeed");
                    black_box(format!("{generated}"));
                });
            },
        );
    }

    group.finish();
}

#[cfg(feature = "compiler")]
criterion_group!(compiler_benches, benchmark_compiler);
#[cfg(feature = "compiler")]
criterion_main!(compiler_benches);

#[cfg(not(feature = "compiler"))]
fn main() {}
