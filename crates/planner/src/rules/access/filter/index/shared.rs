//! Shared access-filter index application flow.

use super::super::atoms::{
    AccessEqualityDomain, AccessFilterIndexAtom, AccessFilterIndexAtoms, AccessFilterIndexPlan,
    AccessFilterIndexPlanMatch,
};
use super::super::labels::access_filter_label;
use super::contracts::{
    AccessFilterIndexApplication, AccessFilterIndexRejection, IndexedSourceCombination,
    MissingAccessIndex, PartialIndexFilterApplication, PartialIndexFilterRejection,
};
use crate::{analysis, catalog, context, ir};

pub(super) trait AccessFilterIndexFamily {
    type Path;
    type Source: Clone + PartialEq;
    type EqualityIndex: Clone;
    type RangeIndex: Clone;

    fn path_source(path: &Self::Path) -> &Self::Source;
    fn source_common_label(source: &Self::Source) -> Option<&ir::NonEmptyString>;
    fn path_from_source(source: Self::Source) -> Self::Path;
    fn equality_index(
        indexes: &catalog::IndexCatalogSnapshot,
        key: &catalog::ScopedPropertyKey,
    ) -> Option<Self::EqualityIndex>;
    fn range_index(
        indexes: &catalog::IndexCatalogSnapshot,
        key: &catalog::ScopedPropertyDirectionKey,
    ) -> Option<Self::RangeIndex>;
    fn equality_source(
        index: Self::EqualityIndex,
        key: catalog::ScopedPropertyKey,
        value: ir::IndexValue,
    ) -> Self::Source;
    fn range_source(
        index: Self::RangeIndex,
        key: catalog::ScopedPropertyDirectionKey,
        range: ir::IndexRange,
    ) -> Self::Source;
    fn union_source(sources: Vec<Self::Source>) -> Self::Source;
    fn intersection_source(sources: Vec<Self::Source>) -> Self::Source;
    fn is_broad_source(source: &Self::Source) -> bool;
    fn intersect_pair(left: Self::Source, right: Self::Source) -> Self::Source;
}

pub(super) fn index_filter<F>(
    path: &F::Path,
    predicate: &helix_ast::expr::Predicate,
    predicate_label: &analysis::FeasibleLabelScope,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
) -> AccessFilterIndexApplication<F::Path>
where
    F: AccessFilterIndexFamily,
{
    let Some(label) = access_filter_label(
        F::source_common_label(F::path_source(path)),
        predicate_label,
    ) else {
        return AccessFilterIndexApplication::NotApplicable(AccessFilterIndexRejection::NoLabel);
    };
    let indexed = match predicate_index_source::<F>(predicate, &label, indexes, planner_limits) {
        Ok(indexed) => indexed,
        Err(reason) => return AccessFilterIndexApplication::NotApplicable(reason),
    };
    match combine_indexed_filter_source::<F>(F::path_source(path), indexed) {
        IndexedSourceCombination::Rewritten(source) => {
            AccessFilterIndexApplication::Rewritten(F::path_from_source(source))
        }
        IndexedSourceCombination::Unchanged => {
            AccessFilterIndexApplication::NotApplicable(AccessFilterIndexRejection::SourceUnchanged)
        }
    }
}

pub(super) fn partial_index_filter<F>(
    path: &F::Path,
    predicate: &helix_ast::expr::Predicate,
    predicate_label: &analysis::FeasibleLabelScope,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
) -> PartialIndexFilterApplication<F::Source>
where
    F: AccessFilterIndexFamily,
{
    let Some(label) = access_filter_label(
        F::source_common_label(F::path_source(path)),
        predicate_label,
    ) else {
        return PartialIndexFilterApplication::NotApplicable(PartialIndexFilterRejection::NoLabel);
    };
    let split =
        match conjunct_index_split::<F>(predicate, &label, indexes, planner_limits, |_, _| true) {
            Ok(split) => split,
            Err(reason) => return PartialIndexFilterApplication::NotApplicable(reason),
        };
    let source = match combine_indexed_filter_source::<F>(F::path_source(path), split.source) {
        IndexedSourceCombination::Rewritten(source) => source,
        IndexedSourceCombination::Unchanged if split.residual.is_empty() => {
            return PartialIndexFilterApplication::NotApplicable(
                PartialIndexFilterRejection::SourceUnchanged,
            );
        }
        IndexedSourceCombination::Unchanged => F::path_source(path).clone(),
    };

    PartialIndexFilterApplication::Rewritten {
        source,
        residual: conjunction_plan(split.residual),
    }
}

/// Index source for one feasible predicate under one proven label.
///
/// Unlike [`index_filter`], this does not combine with an access path, so a
/// predicate anywhere in a stream can be answered by the same index plan.
pub(super) fn predicate_index_source<F>(
    predicate: &helix_ast::expr::Predicate,
    label: &ir::NonEmptyString,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
) -> Result<F::Source, AccessFilterIndexRejection>
where
    F: AccessFilterIndexFamily,
{
    match super::index_plan(predicate, label, planner_limits) {
        AccessFilterIndexPlanMatch::Planned(plan) => {
            index_source_for_plan::<F>(label, &plan, indexes)
                .map_err(AccessFilterIndexRejection::MissingIndex)
        }
        AccessFilterIndexPlanMatch::NotIndexable(reason) => {
            Err(AccessFilterIndexRejection::Predicate(reason))
        }
    }
}

/// Indexed and residual conjuncts of one feasible conjunction under a label.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ConjunctIndexSplit<S> {
    /// Intersection of every admitted conjunct source.
    pub(super) source: S,
    /// Conjuncts decided by `source` for label rows, including conjuncts that
    /// are tautological for the label, in predicate order. Never empty.
    pub(super) decided: Vec<helix_ast::expr::Predicate>,
    /// Conjuncts `source` cannot decide, in predicate order.
    pub(super) residual: Vec<helix_ast::expr::Predicate>,
}

/// Split a conjunction into index-answerable and residual conjuncts.
///
/// `admit` may keep an indexable conjunct residual when the caller cannot
/// serve its source, such as a membership filter and a null equality.
pub(super) fn conjunct_index_split<F>(
    predicate: &helix_ast::expr::Predicate,
    label: &ir::NonEmptyString,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
    admit: impl Fn(&F::Source, &helix_ast::expr::Predicate) -> bool,
) -> Result<ConjunctIndexSplit<F::Source>, PartialIndexFilterRejection>
where
    F: AccessFilterIndexFamily,
{
    let helix_ast::expr::Predicate::And { predicates } = predicate else {
        return Err(PartialIndexFilterRejection::NotConjunction);
    };

    let mut indexed = Vec::new();
    let mut decided = Vec::new();
    let mut residual = Vec::new();
    for predicate in predicates {
        if analysis::predicate_is_tautological_for_label(predicate, label) {
            decided.push(predicate.clone());
            continue;
        }
        match predicate_index_source::<F>(predicate, label, indexes, planner_limits) {
            Ok(source) if admit(&source, predicate) => {
                indexed.push(source);
                decided.push(predicate.clone());
            }
            Ok(_) | Err(_) => residual.push(predicate.clone()),
        }
    }

    let source = match indexed.len() {
        0 => return Err(PartialIndexFilterRejection::NoIndexedConjunct),
        1 => indexed
            .pop()
            .expect("partial-index rewrite already proved one indexed conjunct"),
        _ => F::intersection_source(indexed),
    };
    Ok(ConjunctIndexSplit {
        source,
        decided,
        residual,
    })
}

/// Validated conjunction of already-validated conjuncts, if any.
pub(super) fn conjunction_plan(
    mut predicates: Vec<helix_ast::expr::Predicate>,
) -> Option<ir::PredicatePlan> {
    let predicate = match predicates.len() {
        0 => return None,
        1 => predicates.pop().expect("conjunction length was checked"),
        _ => helix_ast::expr::Predicate::and(predicates),
    };
    Some(
        ir::PredicatePlan::new(predicate)
            .expect("access-filter conjuncts are already validated predicates"),
    )
}

pub(super) fn index_source_for_plan<F>(
    label: &ir::NonEmptyString,
    plan: &AccessFilterIndexPlan,
    indexes: &catalog::IndexCatalogSnapshot,
) -> Result<F::Source, MissingAccessIndex>
where
    F: AccessFilterIndexFamily,
{
    match plan {
        AccessFilterIndexPlan::Conjunction(atoms) => {
            index_source_for_atoms::<F>(label, atoms, indexes)
        }
        AccessFilterIndexPlan::Disjunction(branches) => {
            let sources =
                branches.try_map_ref(|atoms| index_source_for_atoms::<F>(label, atoms, indexes))?;
            Ok(F::union_source(sources.into_iter().collect()))
        }
        AccessFilterIndexPlan::ConjunctionWithDisjunction { shared, branches } => {
            let shared = index_source_for_atoms::<F>(label, shared, indexes)?;
            let branches =
                branches.try_map_ref(|atoms| index_source_for_atoms::<F>(label, atoms, indexes))?;
            Ok(F::intersection_source(vec![
                shared,
                F::union_source(branches.into_iter().collect()),
            ]))
        }
    }
}

fn index_source_for_atoms<F>(
    label: &ir::NonEmptyString,
    atoms: &AccessFilterIndexAtoms,
    indexes: &catalog::IndexCatalogSnapshot,
) -> Result<F::Source, MissingAccessIndex>
where
    F: AccessFilterIndexFamily,
{
    let sources = atoms.try_map_ref(|atom| index_source_for_atom::<F>(label, atom, indexes))?;
    Ok(F::intersection_source(sources.into_iter().collect()))
}

pub(super) fn index_source_for_atom<F>(
    label: &ir::NonEmptyString,
    atom: &AccessFilterIndexAtom,
    indexes: &catalog::IndexCatalogSnapshot,
) -> Result<F::Source, MissingAccessIndex>
where
    F: AccessFilterIndexFamily,
{
    match atom {
        AccessFilterIndexAtom::Equality { property, domain } => {
            let key = catalog::ScopedPropertyKey::new(label.clone(), property.clone());
            let index = F::equality_index(indexes, &key).ok_or(MissingAccessIndex::Equality)?;
            Ok(match domain {
                AccessEqualityDomain::One(value) => F::equality_source(index, key, value.clone()),
                AccessEqualityDomain::Many(values) => F::union_source(
                    values
                        .iter()
                        .map(|value| F::equality_source(index.clone(), key.clone(), value.clone()))
                        .collect(),
                ),
                AccessEqualityDomain::Runtime(values) => {
                    F::equality_source(index, key, ir::IndexValue::ParamSet(values.clone()))
                }
            })
        }
        AccessFilterIndexAtom::Range { property, range } => [
            helix_ast::index::RangeIndexDirection::Asc,
            helix_ast::index::RangeIndexDirection::Desc,
        ]
        .into_iter()
        .find_map(|direction| {
            let key = catalog::ScopedPropertyDirectionKey::new(
                label.clone(),
                property.clone(),
                direction,
            );
            F::range_index(indexes, &key).map(|index| F::range_source(index, key, range.clone()))
        })
        .ok_or(MissingAccessIndex::Range),
    }
}

pub(super) fn combine_indexed_filter_source<F>(
    source: &F::Source,
    indexed: F::Source,
) -> IndexedSourceCombination<F::Source>
where
    F: AccessFilterIndexFamily,
{
    if source == &indexed {
        return IndexedSourceCombination::Unchanged;
    }
    if F::is_broad_source(source) {
        IndexedSourceCombination::Rewritten(indexed)
    } else {
        IndexedSourceCombination::Rewritten(F::intersect_pair(source.clone(), indexed))
    }
}
