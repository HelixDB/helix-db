//! Mixed-template probes against a running server, one JSON line per probe.
//!
//! Each probe draws a template, a group, an item and a fresh query vector, so
//! consecutive probes share no exact request, as a real evaluation varies its
//! queries. `BENCH_CONCURRENCY` probes run at once (1 runs them one by one).
//!
//! Sequential runs can attribute server-side work to each probe:
//! - `BENCH_CGROUP`: the server container's cgroup v2 directory, for CPU,
//!   throttling, block-device reads and page-cache counters.
//! - `BENCH_NET_PID`: a process in the server's network namespace, for bytes
//!   received (object-store downloads dominate them).
//! - `BENCH_PART_DIR`: the host path of the object-store cache tier, whose
//!   new part files count object-store GETs that went through the cache.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;

use futures::stream::{self, StreamExt};
use helix_ast::prelude::*;
use serde_json::json;

use crate::fixture::{self, env_or, Backend, Rng, GROUPS, TOTAL_ITEMS};

/// Query templates, in the order the reference evaluation reported them.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Template {
    GlobalVector,
    GlobalBm25,
    FamilyVector,
    FamilyBm25,
    FeatureVector,
    FamilyFeatureVector,
    ProductVector,
    ProductBm25,
}

impl Template {
    const ALL: [Self; 8] = [
        Self::GlobalVector,
        Self::GlobalBm25,
        Self::FamilyVector,
        Self::FamilyBm25,
        Self::FeatureVector,
        Self::FamilyFeatureVector,
        Self::ProductVector,
        Self::ProductBm25,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::GlobalVector => "global_vector_top50",
            Self::GlobalBm25 => "global_bm25_top50",
            Self::FamilyVector => "family_vector_k5",
            Self::FamilyBm25 => "family_bm25_top50",
            Self::FeatureVector => "feature_vector_top50",
            Self::FamilyFeatureVector => "family_feature_vector_top50",
            Self::ProductVector => "product_vector_k5",
            Self::ProductBm25 => "product_bm25_top50",
        }
    }
}

/// One drawn probe: everything needed to rebuild and label its request.
struct Probe {
    template: Template,
    group: usize,
    item: usize,
    vector: Vec<f32>,
    terms: String,
}

impl Probe {
    fn request(&self) -> QueryRequest {
        let family = || {
            g().n_with_label_where(
                "Group",
                SourcePredicate::eq("name", fixture::group_name(self.group)),
            )
            .in_(Some("IN_GROUP"))
            .out(Some("HAS_ATTRIBUTE"))
        };
        let product = || {
            g().n_with_label_where(
                "Item",
                SourcePredicate::eq("owner", format!("item-{:06}", self.item)),
            )
            .out(Some("HAS_ATTRIBUTE"))
        };
        let feature = || g().n_with_label_where("Attribute", SourcePredicate::eq("kind", "B"));
        let vector = self.vector.clone();
        let terms = self.terms.as_str();
        let (distance, score) = (fixture::distance_projection(), fixture::score_projection());
        fixture::read(match self.template {
            Template::GlobalVector => g()
                .vector_search_nodes("Attribute", "embedding", vector, 50, None)
                .project(distance),
            Template::GlobalBm25 => g()
                .text_search_nodes("Attribute", "text", terms, 50, None)
                .project(score),
            Template::FamilyVector => family()
                .vector_search("Attribute", "embedding", vector, 5, None)
                .project(distance),
            Template::FamilyBm25 => family()
                .text_search("Attribute", "text", terms, 50, None)
                .project(score),
            Template::FeatureVector => feature()
                .vector_search("Attribute", "embedding", vector, 50, None)
                .project(distance),
            Template::FamilyFeatureVector => family()
                .where_(Predicate::eq("kind", "B"))
                .vector_search("Attribute", "embedding", vector, 50, None)
                .project(distance),
            Template::ProductVector => product()
                .vector_search("Attribute", "embedding", vector, 5, None)
                .project(distance),
            Template::ProductBm25 => product()
                .text_search("Attribute", "text", terms, 50, None)
                .project(score),
        })
    }
}

/// Draws `count` probes, cycling through the templates in a shuffled order.
fn probes(count: usize, dimension: usize, seed: u64) -> Vec<Probe> {
    let vectors = fixture::query_vectors_seeded(count, dimension, seed);
    let mut rng = Rng(seed);
    let items = TOTAL_ITEMS as usize;
    vectors
        .into_iter()
        .enumerate()
        .map(|(index, vector)| {
            let round = index / Template::ALL.len();
            let slot = (index + round * 3 + rng.below(Template::ALL.len())) % Template::ALL.len();
            Probe {
                template: Template::ALL[slot],
                group: rng.below(GROUPS),
                item: rng.below(items),
                vector,
                terms: format!("topic t{} group g{}", rng.below(5_500), rng.below(GROUPS)),
            }
        })
        .collect()
}

/// Cumulative server-side counters, sampled before and after a probe.
#[derive(Default)]
struct Counters(BTreeMap<&'static str, u64>);

impl Counters {
    fn sample() -> Self {
        let mut counters = BTreeMap::new();
        if let Ok(cgroup) = std::env::var("BENCH_CGROUP") {
            let cgroup = Path::new(&cgroup);
            let read = |file: &str| std::fs::read_to_string(cgroup.join(file)).unwrap_or_default();
            let fields = |text: &str, wanted: &[(&'static str, &str)]| {
                let mut found = Vec::new();
                for line in text.lines() {
                    let mut parts = line.split_whitespace();
                    let (Some(key), Some(value)) = (parts.next(), parts.next()) else {
                        continue;
                    };
                    for (name, field) in wanted {
                        if key == *field {
                            found.push((*name, value.parse::<u64>().unwrap_or(0)));
                        }
                    }
                }
                found
            };
            counters.extend(fields(
                &read("cpu.stat"),
                &[
                    ("cpu_us", "usage_usec"),
                    ("throttled_periods", "nr_throttled"),
                    ("throttled_us", "throttled_usec"),
                ],
            ));
            counters.extend(fields(
                &read("memory.stat"),
                &[("mem_anon", "anon"), ("mem_file", "file")],
            ));
            // io.stat lines are `major:minor rbytes=.. wbytes=.. rios=.. ...`.
            for (name, field) in [("disk_read_bytes", "rbytes"), ("disk_reads", "rios")] {
                let total = read("io.stat")
                    .split_whitespace()
                    .filter_map(|pair| pair.strip_prefix(field)?.strip_prefix('='))
                    .filter_map(|value| value.parse::<u64>().ok())
                    .sum();
                counters.insert(name, total);
            }
        }
        if let Ok(pid) = std::env::var("BENCH_NET_PID") {
            let net = std::fs::read_to_string(format!("/proc/{pid}/net/dev")).unwrap_or_default();
            let received = net
                .lines()
                .filter(|line| !line.trim_start().starts_with("lo:"))
                .filter_map(|line| line.split_once(':'))
                .filter_map(|(_, stats)| stats.split_whitespace().next()?.parse::<u64>().ok())
                .sum();
            counters.insert("net_rx_bytes", received);
        }
        if let Ok(parts) = std::env::var("BENCH_PART_DIR") {
            counters.insert("cache_part_files", count_files(Path::new(&parts)));
        }
        Self(counters)
    }

    /// Per-counter growth since `before`; gauges (memory) report the new value.
    fn since(&self, before: &Self) -> serde_json::Map<String, serde_json::Value> {
        self.0
            .iter()
            .map(|(name, value)| {
                let reported = match name.starts_with("mem_") {
                    true => *value,
                    false => value.saturating_sub(before.0.get(name).copied().unwrap_or(0)),
                };
                (name.to_string(), json!(reported))
            })
            .collect()
    }
}

fn count_files(directory: &Path) -> u64 {
    std::fs::read_dir(directory)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| match entry.file_type() {
            Ok(kind) if kind.is_dir() => count_files(&entry.path()),
            Ok(_) => 1,
            Err(_) => 0,
        })
        .sum()
}

/// Runs the probes and prints one JSON line each, then a per-template summary.
pub async fn run(backend: &Backend) {
    let count = env_or("BENCH_PROBES", 75usize);
    let concurrency = env_or("BENCH_CONCURRENCY", 1usize).max(1);
    let seed = env_or("BENCH_SEED", 1u64);
    let filter = std::env::var("BENCH_TEMPLATES").ok();
    let drawn = probes(count, env_or("BENCH_DIM", 768), seed)
        .into_iter()
        .filter(|probe| {
            filter
                .as_deref()
                .is_none_or(|filter| filter.split(',').any(|name| probe.template.name() == name))
        })
        .collect::<Vec<_>>();
    let started = Instant::now();
    let results = stream::iter(drawn.iter().enumerate())
        .map(|(index, probe)| async move {
            let before = (concurrency == 1).then(Counters::sample);
            let queued_ms = started.elapsed().as_secs_f64() * 1_000.0;
            let request_started = Instant::now();
            let response = backend.query(probe.request()).await;
            let latency_ms = request_started.elapsed().as_secs_f64() * 1_000.0;
            let mut line = json!({
                "index": index,
                "template": probe.template.name(),
                "group": probe.group,
                "item": probe.item,
                "started_ms": queued_ms,
                "latency_ms": latency_ms,
            });
            match &response {
                Ok(body) => line["rows"] = json!(body["r"].as_array().map_or(0, Vec::len)),
                Err(error) => line["error"] = json!(error),
            }
            if let Some(before) = before {
                line["server"] = json!(Counters::sample().since(&before));
            }
            println!("{line}");
            (probe.template.name(), latency_ms, response.is_ok())
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;
    let wall_ms = started.elapsed().as_secs_f64() * 1_000.0;

    let mut by_template = BTreeMap::<&str, Vec<f64>>::new();
    for (template, latency, ok) in &results {
        if *ok {
            by_template.entry(template).or_default().push(*latency);
        }
    }
    let all = results
        .iter()
        .filter(|(_, _, ok)| *ok)
        .map(|(_, latency, _)| *latency)
        .collect::<Vec<_>>();
    for (template, latencies) in by_template.iter().chain([(&"all", &all)]) {
        let mut sorted = latencies.clone();
        sorted.sort_by(f64::total_cmp);
        let at = |fraction: f64| sorted[((sorted.len() - 1) as f64 * fraction).round() as usize];
        println!(
            "{}",
            json!({
                "summary": template,
                "count": sorted.len(),
                "p50_ms": at(0.5),
                "p95_ms": at(0.95),
                "max_ms": at(1.0),
                "mean_ms": sorted.iter().sum::<f64>() / sorted.len() as f64,
            })
        );
    }
    println!(
        "{}",
        json!({
            "summary": "run",
            "concurrency": concurrency,
            "errors": results.iter().filter(|(_, _, ok)| !ok).count(),
            "wall_ms": wall_ms,
        })
    );
}
