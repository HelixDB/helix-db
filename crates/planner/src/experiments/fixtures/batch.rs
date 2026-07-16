//! Batch construction for scalability fixtures.

use std::num;

use helix_ast::{batch, expr, graph, index, traversal, value};

use crate::properties;

use super::shape::PlanningScalabilityShape;
use super::workload::PlanningScalabilityWorkload;

pub(super) fn workload_for(
    shape: PlanningScalabilityShape,
    scale: properties::PositiveUsize,
) -> PlanningScalabilityWorkload {
    match shape {
        PlanningScalabilityShape::WideBooleanPredicates => {
            PlanningScalabilityWorkload::Read(wide_boolean_predicates(scale))
        }
        PlanningScalabilityShape::ManyAvailableIndexes => {
            PlanningScalabilityWorkload::Read(many_available_indexes(scale))
        }
        PlanningScalabilityShape::BatchedRootReuse => {
            PlanningScalabilityWorkload::Read(repeated_native_root_batch(scale.get()))
        }
        PlanningScalabilityShape::ForEachBodyRootReuse => PlanningScalabilityWorkload::Read(
            batch::ReadBatch::new()
                .for_each_param("items", repeated_native_root_batch(scale.get())),
        ),
        PlanningScalabilityShape::DeepTraversalChain => {
            PlanningScalabilityWorkload::Read(deep_traversal_chain(scale))
        }
        PlanningScalabilityShape::ManyMemoAlternatives => {
            PlanningScalabilityWorkload::Read(many_memo_alternatives(scale))
        }
        PlanningScalabilityShape::OverLimitIndexDisjunction => {
            PlanningScalabilityWorkload::Read(many_memo_alternatives(scale))
        }
        PlanningScalabilityShape::BranchHeavyQueries => {
            PlanningScalabilityWorkload::Read(branch_heavy_queries(scale))
        }
        PlanningScalabilityShape::OrderedRangeWindowPushdown => {
            PlanningScalabilityWorkload::Read(ordered_range_window_pushdown(scale))
        }
        PlanningScalabilityShape::MutationHeavyBatches => {
            PlanningScalabilityWorkload::Write(mutation_heavy_batch(scale))
        }
        PlanningScalabilityShape::SearchIndexDdlWorkloads => {
            PlanningScalabilityWorkload::Write(search_index_ddl_batch(scale))
        }
        PlanningScalabilityShape::RuntimeDerivedMixedQueries => {
            PlanningScalabilityWorkload::Write(runtime_derived_mixed_queries(scale))
        }
    }
}

fn wide_boolean_predicates(scale: properties::PositiveUsize) -> batch::ReadBatch {
    let predicate = expr::Predicate::and(
        (0..scale.get())
            .map(|index| expr::Predicate::eq(format!("p{index}"), index as i64))
            .collect(),
    );
    batch::read_batch()
        .var_as(
            "result",
            traversal::g().n_with_label_where("User", predicate),
        )
        .returning(["result"])
}

fn many_available_indexes(scale: properties::PositiveUsize) -> batch::ReadBatch {
    let property_index = scale.get().saturating_sub(1).min(7);
    let predicate = expr::Predicate::and(vec![
        expr::Predicate::eq("$label", "User"),
        expr::Predicate::eq(format!("p{property_index}"), property_index as i64),
        expr::Predicate::gte("age", 21_i64),
    ]);
    batch::read_batch()
        .var_as(
            "result",
            traversal::g().n_with_label_where("User", predicate),
        )
        .returning(["result"])
}

fn deep_traversal_chain(scale: properties::PositiveUsize) -> batch::ReadBatch {
    let traversal = (0..scale.get()).fold(
        traversal::g().n_with_label_where("User", expr::Predicate::eq("p0", 0_i64)),
        |traversal, _| traversal.out(Some("FOLLOWS")),
    );
    batch::read_batch()
        .var_as("result", traversal)
        .returning(["result"])
}

fn many_memo_alternatives(scale: properties::PositiveUsize) -> batch::ReadBatch {
    let predicate = expr::Predicate::or(
        (0..scale.get())
            .map(|index| expr::Predicate::eq(format!("p{index}"), index as i64))
            .collect(),
    );
    batch::read_batch()
        .var_as(
            "result",
            traversal::g().n_with_label_where("User", predicate),
        )
        .returning(["result"])
}

fn branch_heavy_queries(scale: properties::PositiveUsize) -> batch::ReadBatch {
    let branches = (0..scale.get().max(2))
        .map(|index| {
            traversal::sub()
                .out(Some("FOLLOWS"))
                .limit(index.saturating_add(1))
        })
        .collect();
    batch::read_batch()
        .var_as(
            "result",
            traversal::g()
                .n_with_label_where("User", expr::Predicate::eq("p0", 0_i64))
                .union(branches),
        )
        .returning(["result"])
}

fn ordered_range_window_pushdown(scale: properties::PositiveUsize) -> batch::ReadBatch {
    (0..scale.get()).fold(batch::ReadBatch::new(), |batch, index| {
        let name = format!("result_{index}");
        batch.var_as(
            &name,
            traversal::g()
                .n_with_label_where("User", expr::Predicate::gte("age", 21_i64))
                .order_by("age", traversal::Order::Asc)
                .range(2usize, 7usize),
        )
    })
}

fn mutation_heavy_batch(scale: properties::PositiveUsize) -> batch::WriteBatch {
    (0..scale.get()).fold(batch::WriteBatch::new(), |batch, index| {
        let event_id = format!("evt-{index}");
        let username = format!("user-{index}");
        batch
            .var_as(
                &format!("created_{index}"),
                traversal::g().add_n(
                    "Audit",
                    vec![
                        ("event_id", value::PropertyInput::from(event_id.clone())),
                        ("kind", value::PropertyInput::from("login")),
                    ],
                ),
            )
            .var_as(
                &format!("updated_{index}"),
                traversal::g()
                    .n_with_label_where("Audit", expr::Predicate::eq("event_id", event_id.clone()))
                    .set_property("status", "processed"),
            )
            .var_as(
                &format!("edge_updated_{index}"),
                traversal::g()
                    .e_with_label_where("MENTIONS", expr::Predicate::eq("event_id", event_id))
                    .set_property("seen", true),
            )
            .var_as(
                &format!("linked_{index}"),
                traversal::g()
                    .n_with_label_where("User", expr::Predicate::eq("username", username))
                    .add_e(
                        "MENTIONS",
                        graph::NodeRef::param("targets"),
                        vec![("score", value::PropertyInput::from(index as i64))],
                    ),
            )
    })
}

fn search_index_ddl_batch(scale: properties::PositiveUsize) -> batch::WriteBatch {
    let vector_dim = num::NonZeroUsize::new(4).expect("fixture vector dimension is positive");
    (0..scale.get()).fold(batch::WriteBatch::new(), |batch, index| {
        let secondary = format!("secondary_{index}");
        let embedding = format!("embedding_{index}");
        let body = format!("body_{index}");
        batch
            .var_as(
                &format!("create_node_eq_{index}"),
                traversal::g().create_index_if_not_exists(index::IndexSpec::node_equality(
                    "Doc",
                    secondary.clone(),
                )),
            )
            .var_as(
                &format!("create_edge_range_{index}"),
                traversal::g().create_index_if_not_exists(index::IndexSpec::edge_range_desc(
                    "MENTIONS", secondary,
                )),
            )
            .var_as(
                &format!("create_node_vector_{index}"),
                traversal::g().create_index_if_not_exists(index::IndexSpec::node_vector(
                    "Doc",
                    embedding.clone(),
                    vector_dim,
                    index::VectorDistanceMetric::Cosine,
                    Some("tenant_id"),
                )),
            )
            .var_as(
                &format!("create_edge_vector_{index}"),
                traversal::g().create_index_if_not_exists(index::IndexSpec::edge_vector(
                    "MENTIONS",
                    embedding,
                    vector_dim,
                    index::VectorDistanceMetric::Euclidean,
                    Some("tenant_id"),
                )),
            )
            .var_as(
                &format!("create_node_text_{index}"),
                traversal::g().create_index_if_not_exists(index::IndexSpec::node_text(
                    "Doc",
                    body.clone(),
                    Some("tenant_id"),
                )),
            )
            .var_as(
                &format!("drop_edge_text_{index}"),
                traversal::g().drop_index(index::IndexSpec::edge_text(
                    "MENTIONS",
                    body,
                    Some("tenant_id"),
                )),
            )
    })
}

fn runtime_derived_mixed_queries(scale: properties::PositiveUsize) -> batch::WriteBatch {
    (0..scale.get()).fold(batch::WriteBatch::new(), |batch, index| {
        let username = format!("user-{index}");
        let event_id = format!("evt-{index}");
        let cached_users = format!("cached_users_{index}");
        batch
            .var_as(
                &format!("recent_users_{index}"),
                traversal::g()
                    .n_with_label_where("User", expr::Predicate::eq("username", username.clone()))
                    .store(&cached_users),
            )
            .var_as(
                &format!("user_values_{index}"),
                traversal::g()
                    .inject(&cached_users)
                    .values(vec!["username", "status"]),
            )
            .var_as(
                &format!("range_window_{index}"),
                traversal::g()
                    .n_with_label_where("User", expr::Predicate::gte("age", 21_i64))
                    .order_by("age", traversal::Order::Asc)
                    .range(1usize, 6usize),
            )
            .var_as(
                &format!("doc_search_{index}"),
                traversal::g()
                    .vector_search_nodes(
                        "Doc",
                        "embedding",
                        vec![0.1f32, 0.2, 0.3, 0.4],
                        8,
                        Some("tenant-a".into()),
                    )
                    .limit(3usize),
            )
            .var_as(
                &format!("mention_search_{index}"),
                traversal::g()
                    .text_search_edges("MENTIONS", "body", "planner", 7, None)
                    .count(),
            )
            .var_as(
                &format!("audit_update_{index}"),
                traversal::g()
                    .n_with_label_where("Audit", expr::Predicate::eq("event_id", event_id))
                    .set_property("status", "processed"),
            )
            .var_as(
                &format!("linked_{index}"),
                traversal::g()
                    .n_with_label_where("User", expr::Predicate::eq("username", username))
                    .add_e(
                        "MENTIONS",
                        graph::NodeRef::param("targets"),
                        vec![("score", value::PropertyInput::from(index as i64))],
                    ),
            )
    })
}

fn repeated_native_root_batch(scale: usize) -> batch::ReadBatch {
    (0..scale).fold(batch::ReadBatch::new(), |batch, index| {
        let name = format!("result_{index}");
        batch.var_as(
            &name,
            traversal::g().n_with_label_where("User", expr::Predicate::eq("p0", 0_i64)),
        )
    })
}
