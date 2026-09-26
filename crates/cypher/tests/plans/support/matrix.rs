//! Fixed query, population, selectivity, latency and optimizer-budget inputs.
use super::{queries, Input, Query};
use helix_planner::{catalog, context, cost, experiments, ir, properties};
use std::collections::BTreeMap;

pub(super) const PROFILES: [&str; 3] = ["default", "cold_50ms", "cold_200ms"];

pub(super) fn profile(context: &mut context::PlannerContext, name: &str) {
    let latency = match name {
        "default" => None,
        "cold_50ms" => Some(50_000),
        "cold_200ms" => Some(200_000),
        _ => panic!("unknown storage profile"),
    };
    if let Some(latency) = latency {
        let latency = cost::LatencyEstimate::micros(latency);
        context.storage.object_get_latency = latency;
        context.storage.range_seek = latency;
        context.storage.multi_get_setup = latency;
        // Deliberately conservative cold verification. This is a sensitivity
        // scenario, not a claim about cache hits or measured object-store I/O.
        context.storage.authoritative_verify_per_id = latency;
    }
    context.optimizer_limits.optimization_micros =
        properties::PositiveUsize::at_least_one(60_000_000);
}

fn context(population: u64, indexed: bool) -> context::PlannerContext {
    let mut context = context::PlannerContext::default();
    context.stats = context
        .stats
        .with_node_label_cardinality(ir::NonEmptyString::new("User").unwrap(), population)
        .with_edge_label_cardinality(ir::NonEmptyString::new("FOLLOWS").unwrap(), population * 8)
        .with_node_label_cardinality(ir::NonEmptyString::new("Popular").unwrap(), population)
        .with_node_label_cardinality(ir::NonEmptyString::new("Rare").unwrap(), 1);
    if indexed {
        for (property, rows) in [("key", 1), ("email", 1), ("region", population / 4)] {
            let key = catalog::ScopedPropertyKey::try_new("User", property).unwrap();
            context.indexes = context.indexes.with_node_eq(key.clone());
            context.stats = context.stats.with_node_eq_cardinality(key.clone(), rows);
            if property == "email" {
                context.indexes.node_eq.insert(
                    key,
                    catalog::NodeEqualityIndexMeta::try_new("user_email")
                        .unwrap()
                        .with_uniqueness(catalog::IndexUniqueness::Unique),
                );
            }
        }
        for direction in [
            helix_ast::index::RangeIndexDirection::Asc,
            helix_ast::index::RangeIndexDirection::Desc,
        ] {
            let key =
                catalog::ScopedPropertyDirectionKey::try_new("User", "age", direction).unwrap();
            context.indexes = context.indexes.with_node_range(key.clone());
            context.stats = context
                .stats
                .with_node_range_cardinality(key, population / 3);
        }
    }
    context
}

pub fn inputs() -> BTreeMap<String, Input> {
    let mut inputs = BTreeMap::new();
    for storage in PROFILES {
        for population in [100, 100_000] {
            for indexed in [false, true] {
                let mut context = context(population, indexed);
                profile(&mut context, storage);
                if indexed {
                    super::boundaries::insert(&mut inputs, &context, population, storage);
                }
                for (name, query) in queries::CYPHER {
                    let key = format!(
                        "cypher/{name}/rows={population}/indexed={indexed}/storage={storage}"
                    );
                    assert!(inputs
                        .insert(
                            key,
                            Input {
                                query: Query::Cypher((*query).into()),
                                context: context.clone(),
                            }
                        )
                        .is_none());
                }
            }
        }
        // Skewed labels and an explicitly exhausted deterministic rule budget
        // exercise selective starts and the valid fallback at the same inputs.
        for (name, query) in [
            ("rare_tail", "MATCH (a:Popular)-[:FOLLOWS]->(b:Popular)-[:FOLLOWS]->(c:Rare) RETURN a"),
            ("rare_head", "MATCH (a:Rare)-[:FOLLOWS]->(b:Popular)-[:FOLLOWS]->(c:Popular) RETURN c"),
            ("optional_skew", "MATCH (a:Popular) OPTIONAL MATCH (a)-[:FOLLOWS]->(b:Rare) RETURN a,b LIMIT 10"),
            ("join_skew", "MATCH (a:Popular),(b:Rare) WHERE a.key=b.key RETURN a,b"),
            ("cycle_skew", "MATCH (a:Popular)-[:FOLLOWS]->(b:Popular)-[:FOLLOWS]->(c:Rare)-[:FOLLOWS]->(a) RETURN a"),
        ] {
            for exhausted in [false, true] {
                let mut context = context(100_000, true);
                profile(&mut context, storage);
                if exhausted {
                    context.optimizer_limits.rule_fires = properties::PositiveUsize::at_least_one(1);
                }
                let key = format!("skew/{name}/exhausted={exhausted}/storage={storage}");
                assert!(inputs.insert(key, Input {
                    query: Query::Cypher(query.into()), context,
                }).is_none());
            }
        }
        use experiments::PlanningScalabilityShape as S;
        for shape in [
            S::WideBooleanPredicates,
            S::ManyAvailableIndexes,
            S::BatchedRootReuse,
            S::ForEachBodyRootReuse,
            S::DeepTraversalChain,
            S::ManyMemoAlternatives,
            S::OverLimitIndexDisjunction,
            S::BranchHeavyQueries,
            S::OrderedRangeWindowPushdown,
            S::MutationHeavyBatches,
            S::SearchIndexDdlWorkloads,
            S::RuntimeDerivedMixedQueries,
        ] {
            for scale in [1, 8, 32] {
                let fixture = experiments::PlanScalabilityFixture::new(shape, scale)
                    .unwrap()
                    .case();
                let mut context = fixture.context().clone();
                profile(&mut context, storage);
                let query = match fixture.workload() {
                    experiments::PlanningScalabilityWorkload::Read(batch) => {
                        helix_ast::batch::BatchQuery::Read(batch.clone())
                    }
                    experiments::PlanningScalabilityWorkload::Write(batch) => {
                        helix_ast::batch::BatchQuery::Write(batch.clone())
                    }
                };
                let key = format!("native/{shape:?}/scale={scale}/storage={storage}");
                assert!(inputs
                    .insert(
                        key,
                        Input {
                            query: Query::Native(Box::new(query)),
                            context
                        }
                    )
                    .is_none());
            }
        }
    }
    assert_eq!(inputs.len(), 810, "fixed matrix denominator");
    inputs
}
