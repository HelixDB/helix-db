//! Request-scoped cache of resolved index membership sets.

use std::sync::Arc;

use super::filter::PreparedIndexMembership;
use super::*;

/// Memberships resolved in one request, reused by later executions.
///
/// Branch bodies run their pipeline once per parent row and `ForEach` bodies
/// once per item, so the same set can resolve many times per request. Entries
/// are keyed by the membership set alone (its secondary set, label, and
/// outside-label policy), since that is all resolving reads: the predicate
/// and residual are evaluated per row against whichever plan decides the
/// row. Plans that differ only in a predicate or residual, such as statements
/// probing one set with different residual constants, share one entry.
///
/// An entry depends on:
///
/// * its set;
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
/// * a request-transaction write reaches its footprint
///   ([`Self::forget_writes`]);
/// * index DDL, an isolated mutation scope, the request transaction opening,
///   committing, or aborting, and any failed operation forget every entry
///   ([`Self::clear`]).
///
/// Entries that fell back to per-row evaluation follow the same rules. They
/// read no index, so keeping them is always exact.
///
/// Set equality is reflexive, so every stored set can be found again: a
/// predicate may hold a NaN constant, but executable set leaves hold only
/// index values that reject NaN (`ExecIndexedEqualityValue` and range
/// literals), parameter names, and labels. At most [`Self::MAX_ENTRIES`] sets are
/// held; storing another evicts the oldest, which only costs a later re-read.
///
/// The cache is per step context. Parallel step contexts start empty, and
/// never resolve a membership: only serial steps run membership and count
/// operators.
///
/// Cost: a set resolves on its first node row, with no per-row prefix for
/// short streams, so a `ForEach` whose set reads a frame parameter reads one
/// label-sized set per frame, and a write that creates, deletes, or relabels a
/// node of the set's label, or changes a property the set reads, forces the
/// next statement to read the whole set again. Lookups scan at most
/// [`Self::MAX_ENTRIES`] sets.
#[derive(Debug, Default)]
pub(in crate::execution::interpreter) struct PreparedMemberships(
    Vec<(exec::ExecNodeMembershipSet, Arc<PreparedIndexMembership>)>,
);

impl PreparedMemberships {
    /// Sets one request holds at once.
    ///
    /// Each entry holds up to two label-sized bitmaps (the set and, under the
    /// `Evaluate` policy, the label domain), so this bounds a request's cache
    /// memory and lookup scan. Requests rarely execute more distinct sets than
    /// this; ones that do re-read the evicted sets they execute again.
    pub(in crate::execution::interpreter) const MAX_ENTRIES: usize = 64;

    /// Forget every resolved set before the state they were read from changes.
    pub(in crate::execution::interpreter) fn clear(&mut self) {
        self.0.clear();
    }

    pub(super) fn get(
        &self,
        set: &exec::ExecNodeMembershipSet,
    ) -> Option<Arc<PreparedIndexMembership>> {
        self.0
            .iter()
            .find(|(cached, _)| cached == set)
            .map(|(_, prepared)| Arc::clone(prepared))
    }

    /// Store the membership resolved for `set`, which has no entry yet,
    /// evicting the oldest entry when the cache is full.
    pub(super) fn insert(
        &mut self,
        set: &exec::ExecNodeMembershipSet,
        prepared: Arc<PreparedIndexMembership>,
    ) {
        if self.0.len() == Self::MAX_ENTRIES {
            self.0.remove(0);
        }
        self.0.push((set.clone(), prepared));
    }

    /// Forget every set that reads a parameter `rebound` names.
    ///
    /// `ForEach` calls this when a frame binds its fields and again when it
    /// restores them, so no set outlives the bindings it was resolved from.
    pub(in crate::execution::interpreter) fn forget_params(
        &mut self,
        rebound: impl Fn(&ir::NonEmptyString) -> bool,
    ) {
        self.0.retain(|(set, _)| match set {
            exec::ExecNodeMembershipSet::Index { set, .. } => !reads_param(set, &rebound),
            exec::ExecNodeMembershipSet::Labels(_) => true,
        });
    }

    /// Forget every set whose footprint `writes` reached.
    ///
    /// A `Labels` set reads the `$label` bitmaps of its labels, which change
    /// only when a node of one of them is created, deleted, or relabelled. An
    /// `Index` set reads its label's secondary indexes on the properties of
    /// its leaves, and under the `Evaluate` policy the label's `$label`
    /// bitmap, so it survives writes that only change other properties of its
    /// label.
    pub(in crate::execution::interpreter) fn forget_writes(
        &mut self,
        writes: &mutation::NodeIndexWrites,
    ) {
        self.0.retain(|(set, _)| match set {
            exec::ExecNodeMembershipSet::Labels(labels) => !labels.iter().any(|label| {
                matches!(
                    writes.label(label.as_ref()),
                    Some(mutation::LabelWrites::Nodes)
                )
            }),
            exec::ExecNodeMembershipSet::Index { set, label, .. } => {
                match writes.label(label.as_ref()) {
                    None => true,
                    Some(mutation::LabelWrites::Nodes) => false,
                    Some(mutation::LabelWrites::Properties(changed)) => {
                        !reads_property(set, &|property| changed.contains(property.as_ref()))
                    }
                }
            }
        });
    }

    #[cfg(any(test, feature = "production-coverage"))]
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

/// Whether resolving `set` may read the index of a label property `changed`
/// names.
///
/// Every leaf of a membership set is scoped to the set's label, and a unique
/// lookup verifies only its owner's value of the same property, so a point
/// leaf reads exactly its key's property. Shapes validated membership plans
/// never carry (empty sets, and sets with a range, scan, or bitmap program)
/// are classified conservatively as reading every property, so any write to
/// the label forgets them; ranges and scans resolve per row, so that costs at
/// most a re-check.
fn reads_property(
    set: &exec::ExecNodeSecondarySetPlan,
    changed: &impl Fn(&ir::NonEmptyString) -> bool,
) -> bool {
    match set {
        exec::ExecNodeSecondarySetPlan::Bitmap(
            exec::ExecNodeBitmapExpr::PointRead { key, .. }
            | exec::ExecNodeBitmapExpr::BatchedUnionRead { key, .. },
        )
        | exec::ExecNodeSecondarySetPlan::UniqueUnion { key, .. }
        | exec::ExecNodeSecondarySetPlan::Unique {
            lookup: exec::ExecNodeUniqueOwnerReadPlan { key, .. },
            ..
        }
        | exec::ExecNodeSecondarySetPlan::DynamicEquality { key, .. }
        | exec::ExecNodeSecondarySetPlan::DynamicMembership { key, .. } => changed(&key.property),
        exec::ExecNodeSecondarySetPlan::Intersect { driver, rest }
        | exec::ExecNodeSecondarySetPlan::Union { driver, rest } => {
            core::iter::once(driver.as_ref())
                .chain(rest.iter())
                .any(|child| reads_property(child, changed))
        }
        exec::ExecNodeSecondarySetPlan::Empty
        | exec::ExecNodeSecondarySetPlan::Bitmap(
            exec::ExecNodeBitmapExpr::Union { .. } | exec::ExecNodeBitmapExpr::Intersect { .. },
        )
        | exec::ExecNodeSecondarySetPlan::AuthoritativeScan(_)
        | exec::ExecNodeSecondarySetPlan::Range(_)
        | exec::ExecNodeSecondarySetPlan::OrderedIntersect { .. } => true,
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
            cache.insert(&plan.set, Arc::new(PreparedIndexMembership::PerRow));
        }
        cache
    }

    fn cached(
        cache: &PreparedMemberships,
        plans: &[exec::ExecNodeIndexMembershipPlan],
    ) -> Vec<bool> {
        plans
            .iter()
            .map(|plan| cache.get(&plan.set).is_some())
            .collect()
    }

    /// Plans with one set share its entry whatever their predicate and
    /// residual.
    #[test]
    fn entries_are_keyed_by_set_alone() {
        let plan = index_plan(point("kind"));
        let probed = exec::ExecNodeIndexMembershipPlan {
            predicate: ir::PredicatePlan::new(Predicate::and(vec![
                Predicate::eq("kind", "B"),
                Predicate::eq("uid", "u1"),
            ]))
            .unwrap(),
            residual: Some(ir::PredicatePlan::new(Predicate::eq("uid", "u1")).unwrap()),
            ..plan.clone()
        };
        let cache = cache(std::slice::from_ref(&plan));
        assert_eq!(
            cached(&cache, &[probed, index_plan(point("status"))]),
            [true, false]
        );
    }

    /// The cache holds at most `MAX_ENTRIES` sets, evicting the oldest.
    #[test]
    fn a_full_cache_evicts_its_oldest_set() {
        let plans = (0..=PreparedMemberships::MAX_ENTRIES)
            .map(|value| {
                index_plan(literals(
                    "kind",
                    catalog::IndexUniqueness::NonUnique,
                    &[&format!("v{value}")],
                ))
            })
            .collect::<Vec<_>>();
        let cache = cache(&plans);
        assert_eq!(cache.len(), PreparedMemberships::MAX_ENTRIES);
        assert_eq!(
            cached(&cache, &plans),
            core::iter::once(false)
                .chain(core::iter::repeat_n(true, PreparedMemberships::MAX_ENTRIES))
                .collect::<Vec<_>>()
        );
    }

    /// Set leaves cannot hold NaN, so a set with a NaN literal still equals
    /// itself and every stored set can be found again.
    #[test]
    fn sets_equal_themselves_even_for_nan_literals() {
        let nan = || {
            ir::SecondaryIndexLiteral::new(helix_ast::value::PropertyValue::F64(f64::NAN)).unwrap()
        };
        assert!(exec::ExecIndexedEqualityValue::try_from(nan()).is_err());
        let set = index_plan(exec::ExecNodeSecondarySetPlan::exact_equalities(
            index("kind"),
            key("kind"),
            ir::AtLeast::from_one(ir::IndexValue::Literal(nan())),
        ))
        .set;
        assert_eq!(set, set.clone());
        let cache = cache(&[index_plan(point("kind"))]);
        assert!(cache.get(&index_plan(point("kind")).set).is_some());
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

    #[test]
    fn only_point_leaves_of_unchanged_properties_are_unread() {
        let changed = |names: &'static [&'static str]| {
            move |property: &ir::NonEmptyString| names.contains(&property.as_ref())
        };
        for (set, read) in [
            (point("kind"), &["kind"][..]),
            (
                literals("kind", catalog::IndexUniqueness::NonUnique, &["A", "B"]),
                &["kind"],
            ),
            (unique("uid"), &["uid"]),
            (unique_union("uid"), &["uid"]),
            (dynamic("kind", "kind"), &["kind"]),
            (domain("status", "kinds"), &["status"]),
            (
                union(
                    point("kind"),
                    intersect(unique("uid"), domain("status", "s")),
                ),
                &["kind", "uid", "status"],
            ),
        ] {
            assert!(!reads_property(&set, &changed(&["title"])), "{set:?}");
            for property in read {
                let property = name(property);
                assert!(
                    reads_property(&set, &|changed| *changed == property),
                    "{set:?} {property:?}"
                );
            }
        }
        for set in non_point_sets()
            .into_iter()
            .chain([union(point("kind"), exec::ExecNodeSecondarySetPlan::Empty)])
        {
            assert!(reads_property(&set, &changed(&["title"])), "{set:?}");
        }
    }

    /// Writes that create a node of each of `nodes` and change `properties`
    /// of existing nodes of their label.
    fn writes(nodes: &[&str], properties: &[(&str, &str)]) -> mutation::NodeIndexWrites {
        use crate::encoding::v2::keys::scope::DataScope;
        use crate::encoding::v2::values::property::Property;
        use crate::index_lifecycle::graph_mutation::{
            CanonicalPropertyRow, GraphEntity, GraphMutationTransition, PropertyEdit,
            PropertyEditOutcome,
        };

        let row = |label: &str| CanonicalPropertyRow::new(vec![Property::string("$label", label)]);
        let mut writes = mutation::NodeIndexWrites::default();
        for label in nodes {
            writes.record(&GraphMutationTransition::create(
                DataScope::LegacyUnscoped,
                GraphEntity::node(1),
                row(label),
            ));
        }
        for (label, property) in properties {
            let PropertyEditOutcome::Changed(transition) = GraphMutationTransition::edit(
                DataScope::LegacyUnscoped,
                GraphEntity::node(1),
                row(label),
                PropertyEdit::set(Property::string(*property, "v")),
            ) else {
                panic!("the edit changes the row");
            };
            writes.record(&transition);
        }
        writes
    }

    #[test]
    fn forgetting_writes_drops_only_sets_whose_footprint_they_reach() {
        let labels = labels_plan(&["Item", "Group"]);
        let kind = index_plan(point("kind"));
        let kind_and_status = index_plan(intersect(point("kind"), dynamic("status", "s")));
        let uid = index_plan(unique("uid"));
        let range = index_plan(exec::ExecNodeSecondarySetPlan::Range(range_plan("rank")));
        let plans = [labels, kind, kind_and_status, uid, range];
        for (nodes, properties, expected) in [
            (&[][..], &[][..], [true, true, true, true, true]),
            (&["Note"], &[("Note", "kind")], [true; 5]),
            (&["Group"], &[], [false, true, true, true, true]),
            (&["Item"], &[], [false; 5]),
            (&[], &[("Item", "title")], [true, true, true, true, false]),
            (&[], &[("Item", "status")], [true, true, false, true, false]),
            (&[], &[("Item", "kind")], [true, false, false, true, false]),
            (&[], &[("Item", "uid")], [true, true, true, false, false]),
            (&[], &[("Item", "$label")], [false; 5]),
        ] {
            let mut cache = cache(&plans);
            cache.forget_writes(&writes(nodes, properties));
            assert_eq!(cached(&cache, &plans), expected, "{nodes:?} {properties:?}");
        }
    }
}
