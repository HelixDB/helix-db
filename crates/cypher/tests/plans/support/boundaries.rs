//! Native index set boundaries, including shared work outside a union.
use super::{Input, Query};
use helix_ast::{batch, expr, traversal, value};
use helix_planner::context;
use std::collections::BTreeMap;

pub(super) fn insert(
    inputs: &mut BTreeMap<String, Input>,
    context: &context::PlannerContext,
    population: u64,
    storage: &str,
) {
    for count in [1, 2, 64, 65] {
        for (name, predicate) in [
            (
                "unique_membership",
                expr::Predicate::is_in(
                    "email",
                    value::PropertyValue::StringArray(
                        (0..count)
                            .map(|key| format!("user-{key}@example.test"))
                            .collect(),
                    ),
                ),
            ),
            (
                "membership",
                expr::Predicate::is_in("key", value::PropertyValue::I64Array((0..count).collect())),
            ),
            (
                "shared_disjunction",
                expr::Predicate::and(vec![
                    expr::Predicate::eq("region", "eu"),
                    expr::Predicate::or(
                        (0..count)
                            .map(|key| expr::Predicate::eq("key", key))
                            .collect(),
                    ),
                ]),
            ),
        ] {
            let query = batch::BatchQuery::Read(
                batch::read_batch()
                    .var_as(
                        "result",
                        traversal::g()
                            .n_with_label_where("User", predicate)
                            .values(vec!["key", "email"]),
                    )
                    .returning(["result"]),
            );
            let key =
                format!("native_boundary/{name}/count={count}/rows={population}/storage={storage}");
            assert!(inputs
                .insert(
                    key,
                    Input {
                        query: Query::Native(Box::new(query)),
                        context: context.clone(),
                    }
                )
                .is_none());
        }
    }
}
