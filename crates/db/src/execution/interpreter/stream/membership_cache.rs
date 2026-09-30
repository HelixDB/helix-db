//! Request-scoped cache of resolved index membership sets.

use std::sync::Arc;

use super::filter::PreparedIndexMembership;
use super::*;

/// Memberships resolved in one request, reused by later executions.
///
/// Branch bodies run their pipeline once per parent row and `ForEach` bodies
/// once per item, so the same plan can resolve many times per request. An
/// entry depends on:
///
/// * the plan;
/// * the request snapshot or write transaction, and the Active catalog loaded
///   with it, which decides whether an index still serves the set;
/// * the parameters its set reads while resolving, which are only those of
///   `DynamicEquality` and `DynamicMembership` leaves. Predicates and
///   residuals read their parameters per row, never from the entry;
/// * the node indexes it read: the `$label` bitmaps of a `Labels` set, and
///   for an `Index` set the secondary indexes of its label's properties plus,
///   under the `Evaluate` policy, that label's `$label` bitmap.
///
/// An entry is therefore forgotten exactly when one of them may change:
///
/// * a `ForEach` frame binds, replaces, or restores a parameter its set reads
///   ([`Self::forget_params`]);
/// * index DDL, every mutation, and any failed operation forget every entry
///   ([`Self::clear`]).
///
/// Entries that fell back to per-row evaluation follow the same rules. They
/// read no index, so keeping them is always exact.
///
/// Entries hold at most one set per distinct plan in the request. A plan that
/// is not equal to itself, because a predicate constant is NaN, is never
/// stored and resolves on every execution instead.
///
/// The cache is per step context. Parallel step contexts start empty, and
/// never resolve a membership: only serial steps run membership and count
/// operators.
///
/// Cost: a set resolves on its first node row, with no per-row prefix for
/// short streams, so a `ForEach` whose set reads a frame parameter reads one
/// label-sized set per frame, and every write forces the next statement to
/// read the whole set again.
#[derive(Debug, Default)]
pub(in crate::execution::interpreter) struct PreparedMemberships(
    Vec<(
        exec::ExecNodeIndexMembershipPlan,
        Arc<PreparedIndexMembership>,
    )>,
);

impl PreparedMemberships {
    /// Forget every resolved set before the state they were read from changes.
    pub(in crate::execution::interpreter) fn clear(&mut self) {
        self.0.clear();
    }

    pub(super) fn get(
        &self,
        plan: &exec::ExecNodeIndexMembershipPlan,
    ) -> Option<Arc<PreparedIndexMembership>> {
        self.0
            .iter()
            .find(|(cached, _)| cached == plan)
            .map(|(_, prepared)| Arc::clone(prepared))
    }

    /// Store the set resolved for `plan`, which has no entry yet.
    pub(super) fn insert(
        &mut self,
        plan: &exec::ExecNodeIndexMembershipPlan,
        prepared: Arc<PreparedIndexMembership>,
    ) {
        self.0.push((plan.clone(), prepared));
    }

    /// Forget every set that reads a parameter `rebound` names.
    ///
    /// `ForEach` calls this when a frame binds its fields and again when it
    /// restores them, so no set outlives the bindings it was resolved from.
    pub(in crate::execution::interpreter) fn forget_params(
        &mut self,
        rebound: impl Fn(&ir::NonEmptyString) -> bool,
    ) {
        self.0.retain(|(plan, _)| match &plan.set {
            exec::ExecNodeMembershipSet::Index { set, .. } => !reads_param(set, &rebound),
            exec::ExecNodeMembershipSet::Labels(_) => true,
        });
    }

    #[cfg(test)]
    pub(in crate::execution::interpreter) fn len(&self) -> usize {
        self.0.len()
    }
}

/// Whether resolving `set` reads a parameter `rebound` names.
///
/// Only runtime equality leaves read a parameter while the set resolves.
/// Literal leaves read none, and sets with a range or scan fall back to
/// per-row evaluation before any parameter is read.
fn reads_param(
    set: &exec::ExecNodeSecondarySetPlan,
    rebound: &impl Fn(&ir::NonEmptyString) -> bool,
) -> bool {
    match set {
        exec::ExecNodeSecondarySetPlan::DynamicEquality { param, .. } => rebound(param),
        exec::ExecNodeSecondarySetPlan::DynamicMembership { values, .. } => rebound(values.param()),
        exec::ExecNodeSecondarySetPlan::Intersect { driver, rest }
        | exec::ExecNodeSecondarySetPlan::Union { driver, rest } => {
            core::iter::once(driver.as_ref())
                .chain(rest.iter())
                .any(|child| reads_param(child, rebound))
        }
        exec::ExecNodeSecondarySetPlan::Empty
        | exec::ExecNodeSecondarySetPlan::Bitmap(_)
        | exec::ExecNodeSecondarySetPlan::UniqueUnion { .. }
        | exec::ExecNodeSecondarySetPlan::Unique { .. }
        | exec::ExecNodeSecondarySetPlan::AuthoritativeScan(_)
        | exec::ExecNodeSecondarySetPlan::Range(_)
        | exec::ExecNodeSecondarySetPlan::OrderedIntersect { .. } => false,
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use helix_ast::expr::Predicate;
    use helix_ast::index::RangeIndexDirection;
    use helix_planner::catalog;

    use super::*;

    fn name(value: &str) -> ir::NonEmptyString {
        ir::NonEmptyString::new(value).unwrap()
    }

    fn key(property: &str) -> catalog::ScopedPropertyKey {
        catalog::ScopedPropertyKey::try_new("Item", property).unwrap()
    }

    fn index(property: &str) -> catalog::NodeEqualityIndexMeta {
        catalog::IndexCatalogSnapshot::default()
            .with_node_eq(key(property))
            .node_eq[&key(property)]
            .clone()
    }

    fn literals(
        property: &str,
        uniqueness: catalog::IndexUniqueness,
        values: &[&str],
    ) -> exec::ExecNodeSecondarySetPlan {
        exec::ExecNodeSecondarySetPlan::exact_equalities(
            index(property).with_uniqueness(uniqueness),
            key(property),
            ir::AtLeast::try_from_vec(
                values
                    .iter()
                    .map(|value| {
                        ir::IndexValue::Literal(
                            ir::SecondaryIndexLiteral::new((*value).into()).unwrap(),
                        )
                    })
                    .collect(),
            )
            .unwrap(),
        )
    }

    /// One point read of `property`.
    fn point(property: &str) -> exec::ExecNodeSecondarySetPlan {
        literals(property, catalog::IndexUniqueness::NonUnique, &["B"])
    }

    fn unique(property: &str) -> exec::ExecNodeSecondarySetPlan {
        literals(property, catalog::IndexUniqueness::Unique, &["a1"])
    }

    fn unique_union(property: &str) -> exec::ExecNodeSecondarySetPlan {
        let exec::ExecNodeSecondarySetPlan::Unique { lookup, .. } = unique(property) else {
            panic!("a unique literal lowers to a unique lookup");
        };
        exec::ExecNodeSecondarySetPlan::UniqueUnion {
            index: lookup.index,
            key: lookup.key,
            values: ir::AtLeast::from_pair(lookup.value.clone(), lookup.value),
        }
    }

    fn dynamic(property: &str, param: &str) -> exec::ExecNodeSecondarySetPlan {
        exec::ExecNodeSecondarySetPlan::DynamicEquality {
            index: index(property),
            key: key(property),
            param: name(param),
        }
    }

    fn domain(property: &str, param: &str) -> exec::ExecNodeSecondarySetPlan {
        exec::ExecNodeSecondarySetPlan::DynamicMembership {
            index: index(property),
            key: key(property),
            values: ir::RuntimeEqualitySet::new(name(param), NonZeroUsize::new(2).unwrap()),
        }
    }

    fn range_plan(property: &str) -> exec::ExecNodeSecondaryRangePlan {
        let key = catalog::ScopedPropertyDirectionKey::try_new(
            "Item",
            property,
            RangeIndexDirection::Asc,
        )
        .unwrap();
        exec::ExecNodeSecondaryRangePlan {
            index: catalog::IndexCatalogSnapshot::default()
                .with_node_range(key.clone())
                .node_range[&key]
                .clone(),
            key,
            range: ir::IndexRange::All,
            iteration: ir::RangeScanIteration::Forward,
        }
    }

    fn union(
        driver: exec::ExecNodeSecondarySetPlan,
        rest: exec::ExecNodeSecondarySetPlan,
    ) -> exec::ExecNodeSecondarySetPlan {
        exec::ExecNodeSecondarySetPlan::Union {
            driver: Box::new(driver),
            rest: ir::AtLeast::from_one(rest),
        }
    }

    fn intersect(
        driver: exec::ExecNodeSecondarySetPlan,
        rest: exec::ExecNodeSecondarySetPlan,
    ) -> exec::ExecNodeSecondarySetPlan {
        exec::ExecNodeSecondarySetPlan::Intersect {
            driver: Box::new(driver),
            rest: ir::AtLeast::from_one(rest),
        }
    }

    /// Sets that read no parameter and more than point indexes.
    fn non_point_sets() -> Vec<exec::ExecNodeSecondarySetPlan> {
        let exec::ExecNodeSecondarySetPlan::Bitmap(first) = point("kind") else {
            panic!("a non-unique literal lowers to a bitmap");
        };
        vec![
            exec::ExecNodeSecondarySetPlan::Empty,
            exec::ExecNodeSecondarySetPlan::AuthoritativeScan(
                exec::ExecNodeAuthoritativeScanPredicate::NullEquality { key: key("kind") },
            ),
            exec::ExecNodeSecondarySetPlan::Range(range_plan("rank")),
            exec::ExecNodeSecondarySetPlan::OrderedIntersect {
                driver: range_plan("rank"),
                filters: ir::AtLeast::from_one(point("kind")),
            },
            exec::ExecNodeSecondarySetPlan::Bitmap(exec::ExecNodeBitmapExpr::Union {
                driver: Box::new(first.clone()),
                rest: ir::AtLeast::from_one(first.clone()),
            }),
            exec::ExecNodeSecondarySetPlan::Bitmap(exec::ExecNodeBitmapExpr::Intersect {
                driver: Box::new(first.clone()),
                rest: ir::AtLeast::from_one(first),
            }),
        ]
    }

    fn index_plan(set: exec::ExecNodeSecondarySetPlan) -> exec::ExecNodeIndexMembershipPlan {
        exec::ExecNodeIndexMembershipPlan {
            set: exec::ExecNodeMembershipSet::Index {
                set,
                label: name("Item"),
                outside_label: ir::NodeMembershipOutsideLabel::Evaluate,
            },
            predicate: ir::PredicatePlan::new(Predicate::eq("kind", "B")).unwrap(),
            residual: None,
        }
    }

    fn labels_plan(labels: &[&str]) -> exec::ExecNodeIndexMembershipPlan {
        exec::ExecNodeIndexMembershipPlan {
            set: exec::ExecNodeMembershipSet::Labels(
                ir::AtLeast::try_from_vec(labels.iter().map(|label| name(label)).collect())
                    .unwrap(),
            ),
            predicate: ir::PredicatePlan::new(Predicate::eq("$label", labels[0])).unwrap(),
            residual: None,
        }
    }

    fn cache(plans: &[exec::ExecNodeIndexMembershipPlan]) -> PreparedMemberships {
        let mut cache = PreparedMemberships::default();
        for plan in plans {
            cache.insert(plan, Arc::new(PreparedIndexMembership::PerRow));
        }
        cache
    }

    fn cached(
        cache: &PreparedMemberships,
        plans: &[exec::ExecNodeIndexMembershipPlan],
    ) -> Vec<bool> {
        plans.iter().map(|plan| cache.get(plan).is_some()).collect()
    }

    #[test]
    fn only_runtime_equality_leaves_read_parameters() {
        let named = |name: &'static str| move |param: &ir::NonEmptyString| param.as_ref() == name;
        for (set, reads) in [
            (dynamic("kind", "kind"), true),
            (domain("kind", "kind"), true),
            (dynamic("kind", "other"), false),
            (domain("kind", "other"), false),
            (union(point("kind"), dynamic("status", "kind")), true),
            (intersect(point("kind"), domain("status", "other")), false),
            (
                intersect(
                    point("kind"),
                    union(point("title"), dynamic("status", "kind")),
                ),
                true,
            ),
            (point("kind"), false),
            (
                literals("kind", catalog::IndexUniqueness::NonUnique, &["A", "B"]),
                false,
            ),
            (unique("uid"), false),
            (unique_union("uid"), false),
        ]
        .into_iter()
        .chain(non_point_sets().into_iter().map(|set| (set, false)))
        {
            assert_eq!(reads_param(&set, &named("kind")), reads, "{set:?}");
        }
    }

    #[test]
    fn forgetting_params_drops_only_sets_that_read_them() {
        let plans = [
            index_plan(dynamic("kind", "kind")),
            index_plan(domain("kind", "kinds")),
            index_plan(point("kind")),
            labels_plan(&["Item"]),
            index_plan(union(point("kind"), dynamic("kind", "other"))),
        ];
        let mut cache = cache(&plans);
        cache.forget_params(|name| name.as_ref() == "unrelated");
        assert_eq!(cached(&cache, &plans), [true; 5]);
        cache.forget_params(|name| name.as_ref() == "kind");
        assert_eq!(cached(&cache, &plans), [false, true, true, true, true]);
        cache.forget_params(|name| ["kinds", "other"].contains(&name.as_ref()));
        assert_eq!(cached(&cache, &plans), [false, false, true, true, false]);
        assert_eq!(cache.len(), 2);
        cache.clear();
        assert_eq!(cache.len(), 0);
    }
}
