//! A unique key drives its source filter.
//!
//! A single-valued equality on a unique index reads at most one candidate,
//! so every other conjunct is evaluated on that candidate instead of reading
//! an index set across the label. The claim shape below once intersected the
//! key with a status union and an expiry range scan, which verified every
//! record in range on each request.

use crate::planning::tests::support::*;

fn indexes(unique: bool) -> IndexCatalogSnapshot {
    let mut indexes = IndexCatalogSnapshot::default()
        .with_node_eq(ScopedPropertyKey::try_new("Job", "status").unwrap())
        .with_node_eq(ScopedPropertyKey::try_new("Job", "owner").unwrap())
        .with_node_range(
            ScopedPropertyDirectionKey::try_new("Job", "claimed_at", RangeIndexDirection::Asc)
                .unwrap(),
        );
    for (property, index_id) in [("job_id", "job-id"), ("token", "job-token")] {
        indexes.node_eq.insert(
            ScopedPropertyKey::try_new("Job", property).unwrap(),
            NodeEqualityIndexMeta::try_new(index_id)
                .unwrap()
                .with_uniqueness(if unique {
                    IndexUniqueness::Unique
                } else {
                    IndexUniqueness::NonUnique
                }),
        );
    }
    indexes
}

/// No statistics, statistics claiming a huge expiry range, and the
/// exploration budget exhausted: the key drives under each.
fn contexts(unique: bool) -> Vec<PlannerContext> {
    let base = PlannerContext {
        params: ParamBindings::default()
            .with_value(
                NonEmptyString::new("id").unwrap(),
                PropertyValue::from("j1"),
            )
            .into(),
        ..ctx(indexes(unique))
    };
    let mut stats = base.clone();
    stats.stats = StatsSnapshot::default()
        .with_node_label_cardinality(NonEmptyString::new("Job").unwrap(), 1_000_000)
        .with_node_eq_cardinality(
            ScopedPropertyKey::try_new("Job", "status").unwrap(),
            500_000,
        )
        .with_node_range_cardinality(
            ScopedPropertyDirectionKey::try_new("Job", "claimed_at", RangeIndexDirection::Asc)
                .unwrap(),
            900_000,
        );
    let mut exhausted = base.clone();
    exhausted.optimizer_limits.exploration_rule_fires =
        crate::properties::PositiveUsize::at_least_one(1);
    let mut late = base.clone();
    late.late_bound_params = [NonEmptyString::new("id").unwrap()].into_iter().collect();
    vec![base, stats, exhausted, late]
}

fn claim_guard() -> Predicate {
    Predicate::and(vec![
        Predicate::is_in(
            "status",
            PropertyValue::StringArray(vec!["available".to_owned(), "claimed".to_owned()]),
        ),
        Predicate::or(vec![
            Predicate::eq("status", "available"),
            Predicate::lt("claimed_at", 100_i64),
        ]),
    ])
}

fn claim(key: Predicate) -> Predicate {
    let Predicate::And { predicates } = claim_guard() else {
        unreachable!("the claim guard is a conjunction");
    };
    Predicate::and(std::iter::once(key).chain(predicates).collect())
}

fn claim_batch(predicate: Predicate) -> helix_ast::batch::WriteBatch {
    write_batch()
        .var_as(
            "claimed",
            g().n_with_label_where("Job", predicate)
                .set_property("status", "claimed")
                .set_property("claimed_at", 200_i64),
        )
        .returning(["claimed"])
}

fn filter_predicates(plan: &ExecutablePlan) -> Vec<&Predicate> {
    plan.steps()
        .iter()
        .filter_map(|step| match &step.op {
            ExecOp::Filter { predicate } => Some(predicate.predicate()),
            _ => None,
        })
        .collect()
}

/// One unique lookup, no other index read, and `residual` as the only
/// per-row filter.
fn assert_key_driven(plan: &ExecutablePlan, context: &PlannerContext, residual: &Predicate) {
    let accesses = crate::diagnostics::analyze(plan, context)
        .statistics
        .node_accesses;
    assert_eq!(accesses.range_index_scans, 0, "{:#?}", plan.steps());
    assert_eq!(accesses.label_scans, 0, "{:#?}", plan.steps());
    assert_eq!(accesses.equality_index_lookups, 1, "{:#?}", plan.steps());
    assert_eq!(filter_predicates(plan), [residual], "{:#?}", plan.steps());
}

#[test]
fn unique_key_drives_conditional_claims() {
    for context in contexts(true) {
        for key in [
            Predicate::eq("job_id", "j1"),
            Predicate::eq_param("job_id", "id"),
        ] {
            let output = crate::planning::plan_write_batch_with_diagnostics(
                &claim_batch(claim(key)),
                &context,
            )
            .unwrap();
            assert_key_driven(output.plan(), &context, &claim_guard());
            // Every guarded property is indexed and the key bounds the
            // rows, so nothing suggests an index or reports a scan.
            assert!(
                output.diagnostics().insights.is_empty(),
                "{:#?}",
                output.diagnostics()
            );
        }
    }
}

#[test]
fn unique_key_drives_reads_counts_and_label_scoped_predicates() {
    for context in contexts(true) {
        let label_scoped = executable_traversal(
            g().n_where(Predicate::and(vec![
                Predicate::eq("$label", "Job"),
                Predicate::and(vec![Predicate::eq_param("job_id", "id"), claim_guard()]),
            ]))
            .values(vec!["job_id"]),
            context.clone(),
        );
        assert_key_driven(&label_scoped, &context, &claim_guard());

        let count = executable_traversal(
            g().n_with_label_where("Job", claim(Predicate::eq("job_id", "j1")))
                .count(),
            context.clone(),
        );
        let counted = count.steps().iter().find_map(|step| match &step.op {
            ExecOp::Count { plan } => Some(plan.as_ref()),
            _ => None,
        });
        assert!(
            matches!(
                counted,
                Some(ExecCountPlan::Stream(crate::exec::ExecCountStreamPlan {
                    cursor: crate::exec::ExecCountCursorPlan::Filter { input, predicate },
                    ..
                })) if matches!(input.as_ref(), crate::exec::ExecCountCursorPlan::NodeUnique { .. })
                    && predicate.predicate() == &claim_guard()
            ),
            "{:#?}",
            count.steps()
        );
    }
}

#[test]
fn filters_over_a_unique_key_stay_per_candidate() {
    for context in contexts(true) {
        // A later `where` over the key, and a second unique key, are both
        // evaluated on the key's candidate.
        let chained = executable_traversal(
            g().n_with_label_where("Job", Predicate::eq_param("job_id", "id"))
                .where_(claim_guard())
                .values(vec!["job_id"]),
            context.clone(),
        );
        assert_key_driven(&chained, &context, &claim_guard());

        let two_keys = Predicate::and(vec![
            Predicate::eq("job_id", "j1"),
            Predicate::eq("token", "t1"),
            Predicate::eq("owner", "o1"),
        ]);
        let plan = executable_traversal(
            g().n_with_label_where("Job", two_keys)
                .values(vec!["job_id"]),
            context.clone(),
        );
        assert_key_driven(
            &plan,
            &context,
            &Predicate::and(vec![
                Predicate::eq("token", "t1"),
                Predicate::eq("owner", "o1"),
            ]),
        );
    }
}

#[test]
fn only_a_single_non_null_unique_value_is_a_key() {
    let context = contexts(true).remove(0);
    for key in [
        // Null matches every record missing the property.
        Predicate::eq("job_id", PropertyValue::Null),
        // A list reads one candidate per value.
        Predicate::is_in(
            "job_id",
            PropertyValue::StringArray(vec!["j1".to_owned(), "j2".to_owned()]),
        ),
    ] {
        let plan = executable_traversal(
            g().n_with_label_where("Job", claim(key.clone()))
                .values(vec!["job_id"]),
            context.clone(),
        );
        assert!(
            filter_predicates(&plan)
                .iter()
                .all(|predicate| **predicate != claim_guard()),
            "{key:?}: {:#?}",
            plan.steps()
        );
    }

    // A non-unique index is no key, so every indexed conjunct stays an
    // index read.
    for context in contexts(false) {
        let plan = crate::planning::plan_write_batch(
            &claim_batch(claim(Predicate::eq("job_id", "j1"))),
            &context,
        )
        .unwrap();
        assert!(filter_predicates(&plan).is_empty(), "{:#?}", plan.steps());
        assert_eq!(
            crate::diagnostics::analyze(&plan, &context)
                .statistics
                .node_accesses
                .range_index_scans,
            1
        );
    }
}
