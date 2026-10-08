//! Index rewrites shared across one optimization run.
//!
//! Several rules derive the index rewrite of the same filter: the exploration
//! rule offers it, and implementation rules derive it again so that they
//! defer to it. Over the planner benchmarks each distinct filter was derived
//! about three times per plan. A rewrite depends only on the filter, the
//! catalog and the planner limits, so the optimizer driver installs a cache
//! for each run ([`with_run_cache`]) and [`cached`] answers repeated filters
//! from it. A call under any other catalog or limits, or outside a run,
//! derives its rewrite directly.

use std::cell::RefCell;
use std::collections::HashMap;

use super::super::AccessFilterRewrite;
use crate::{catalog, context, digest, ir, logical};

thread_local! {
    static RUN: RefCell<Option<RunCache>> = const { RefCell::new(None) };
}

struct RunCache {
    /// The catalog the run plans against, compared by address. It outlives
    /// the run, so no other catalog can have its address meanwhile.
    indexes: *const catalog::IndexCatalogSnapshot,
    planner_limits: context::PlannerLimits,
    /// Rewrites by the identity digests of a filter's access path and
    /// predicate, confirmed with [`same_filter`]. Identity digests tell the
    /// signs of zero apart, so filters that differ only in the sign of a zero
    /// literal never share a rewrite.
    rewrites: HashMap<FilterKey, Vec<(logical::AccessFilter, AccessFilterRewrite)>>,
    /// The predicate digest of each filter stored in `rewrites`, by where its
    /// predicate lives. The rules that ask for one filter's rewrite usually
    /// hold clones of one filter, so this spares serializing the predicate on
    /// their lookups. Only stored filters' predicates are recorded, and the
    /// stored filter keeps its predicate alive, so no other predicate can
    /// take a recorded address meanwhile.
    predicate_digests: HashMap<usize, digest::PlanDigest>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct FilterKey {
    access: digest::PlanDigest,
    predicate: digest::PlanDigest,
}

/// The same filter as `known`, given matching digests: an equal access path
/// and predicate, the predicate compared by allocation first.
fn same_filter(known: &logical::AccessFilter, filter: &logical::AccessFilter) -> bool {
    let (known_predicate, predicate) = (known.predicate(), filter.predicate());
    let shared = std::ptr::eq(known_predicate.predicate(), predicate.predicate())
        && std::ptr::eq(known_predicate.resolved(), predicate.resolved());
    (shared || known_predicate == predicate) && known.access() == filter.access()
}

/// Where `predicate` lives, to key [`RunCache::predicate_digests`].
fn allocation(predicate: &ir::PredicatePlan) -> usize {
    std::ptr::from_ref(predicate.predicate()).addr()
}

/// Runs `run` with index rewrites under `indexes` and `planner_limits` cached
/// for its duration. An enclosing run's cache is set aside meanwhile and
/// restored afterwards, even if `run` panics.
pub(crate) fn with_run_cache<R>(
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
    run: impl FnOnce() -> R,
) -> R {
    struct Restore(Option<RunCache>);
    impl Drop for Restore {
        fn drop(&mut self) {
            let enclosing = self.0.take();
            // Only fails while the thread itself is being torn down.
            let _ = RUN.try_with(|cache| *cache.borrow_mut() = enclosing);
        }
    }
    let enclosing = RUN.with(|cache| {
        cache.borrow_mut().replace(RunCache {
            indexes,
            planner_limits: planner_limits.clone(),
            rewrites: HashMap::new(),
            predicate_digests: HashMap::new(),
        })
    });
    let _restore = Restore(enclosing);
    run()
}

/// The rewrite of `filter`, from the current run's cache when it was derived
/// under the same `indexes` and `planner_limits`, otherwise from `derive`.
/// `derive` runs with the cache released, so it may itself ask for rewrites.
pub(super) fn cached(
    filter: &logical::AccessFilter,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
    derive: impl FnOnce() -> AccessFilterRewrite,
) -> AccessFilterRewrite {
    let lookup = RUN.with(|cache| {
        let cache = cache.borrow();
        let run = cache.as_ref().filter(|run| {
            std::ptr::eq(run.indexes, indexes) && run.planner_limits == *planner_limits
        })?;
        let predicate = run
            .predicate_digests
            .get(&allocation(filter.predicate()))
            .copied()
            .unwrap_or_else(|| digest::PlanDigest::for_value(filter.predicate()));
        let key = FilterKey {
            access: digest::PlanDigest::for_value(filter.access()),
            predicate,
        };
        let known = run
            .rewrites
            .get(&key)
            .and_then(|known| known.iter().find(|(known, _)| same_filter(known, filter)))
            .map(|(_, rewrite)| rewrite.clone());
        Some((key, known))
    });
    let (key, known) = match lookup {
        None => return derive(),
        Some(lookup) => lookup,
    };
    if let Some(rewrite) = known {
        return rewrite;
    }
    let rewrite = derive();
    RUN.with(|cache| {
        let mut cache = cache.borrow_mut();
        let Some(run) = cache.as_mut() else {
            return;
        };
        run.predicate_digests
            .insert(allocation(filter.predicate()), key.predicate);
        run.rewrites
            .entry(key)
            .or_default()
            .push((filter.clone(), rewrite.clone()));
    });
    rewrite
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;
    use crate::ir;

    fn filter(label: &str) -> logical::AccessFilter {
        logical::AccessFilter::new(
            logical::AccessPath::Node(logical::NodeAccessPath::new(
                ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::LabelScan {
                    label: ir::NonEmptyString::new(label).unwrap(),
                })
                .unwrap(),
            )),
            ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("age", 1_i64)).unwrap(),
        )
    }

    /// Counts how often [`cached`] derives a rewrite.
    fn derive_count(
        filter: &logical::AccessFilter,
        indexes: &catalog::IndexCatalogSnapshot,
        limits: &context::PlannerLimits,
        derived: &Cell<usize>,
    ) -> AccessFilterRewrite {
        cached(filter, indexes, limits, || {
            derived.set(derived.get() + 1);
            AccessFilterRewrite::NotApplicable
        })
    }

    #[test]
    fn a_run_derives_each_filter_once_under_its_own_catalog_and_limits() {
        let indexes = catalog::IndexCatalogSnapshot::default();
        let other_indexes = catalog::IndexCatalogSnapshot::default();
        let limits = context::PlannerLimits::default();
        let derived = Cell::new(0);

        derive_count(&filter("User"), &indexes, &limits, &derived);
        assert_eq!(derived.get(), 1, "outside a run every call derives");

        let user = filter("User");
        with_run_cache(&indexes, &limits, || {
            derive_count(&user, &indexes, &limits, &derived);
            derive_count(&user.clone(), &indexes, &limits, &derived);
            assert_eq!(derived.get(), 2, "a repeated filter is answered once");
            derive_count(&filter("Item"), &indexes, &limits, &derived);
            assert_eq!(derived.get(), 3, "a new filter derives");
            derive_count(&user, &other_indexes, &limits, &derived);
            assert_eq!(derived.get(), 4, "another catalog bypasses the cache");
            derive_count(&filter("User"), &indexes, &limits, &derived);
            assert_eq!(
                derived.get(),
                4,
                "an equal filter in other allocations is answered by digest"
            );

            let other_limits = context::PlannerLimits {
                max_index_union_branches: context::IndexUnionBranchLimit::limited(2).unwrap(),
            };
            with_run_cache(&indexes, &other_limits, || {
                derive_count(&user, &indexes, &other_limits, &derived);
                assert_eq!(derived.get(), 5, "a nested run starts empty");
            });
            derive_count(&user, &indexes, &limits, &derived);
            assert_eq!(derived.get(), 5, "the enclosing run's cache is restored");
        });
        derive_count(&user, &indexes, &limits, &derived);
        assert_eq!(derived.get(), 6, "the cache ends with its run");
    }
}
