//! Shared access-filter index application flow.

use super::super::atoms::{
    AccessFilterIndexAtom, AccessFilterIndexAtoms, AccessFilterIndexPlan,
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
    let plan = match super::index_plan(predicate, &label, planner_limits) {
        AccessFilterIndexPlanMatch::Planned(plan) => plan,
        AccessFilterIndexPlanMatch::NotIndexable(reason) => {
            return AccessFilterIndexApplication::NotApplicable(
                AccessFilterIndexRejection::Predicate(reason),
            );
        }
    };
    let indexed = match index_source_for_plan::<F>(&label, &plan, indexes) {
        Ok(indexed) => indexed,
        Err(reason) => {
            return AccessFilterIndexApplication::NotApplicable(
                AccessFilterIndexRejection::MissingIndex(reason),
            );
        }
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
    let helix_ast::expr::Predicate::And { predicates } = predicate else {
        return PartialIndexFilterApplication::NotApplicable(
            PartialIndexFilterRejection::NotConjunction,
        );
    };

    let mut indexed = Vec::new();
    let mut residual = Vec::new();
    for predicate in predicates {
        if analysis::predicate_is_tautological_for_label(predicate, &label) {
            continue;
        }
        match super::index_plan(predicate, &label, planner_limits) {
            AccessFilterIndexPlanMatch::Planned(plan) => {
                match index_source_for_plan::<F>(&label, &plan, indexes) {
                    Ok(source) => indexed.push(source),
                    Err(_) => residual.push(predicate.clone()),
                }
            }
            AccessFilterIndexPlanMatch::NotIndexable(_) => residual.push(predicate.clone()),
        }
    }

    if indexed.is_empty() {
        return PartialIndexFilterApplication::NotApplicable(
            PartialIndexFilterRejection::NoIndexedConjunct,
        );
    }

    let indexed = if indexed.len() == 1 {
        indexed
            .pop()
            .expect("partial-index rewrite already proved one indexed conjunct")
    } else {
        F::intersection_source(indexed)
    };
    let source = match combine_indexed_filter_source::<F>(F::path_source(path), indexed) {
        IndexedSourceCombination::Rewritten(source) => source,
        IndexedSourceCombination::Unchanged if residual.is_empty() => {
            return PartialIndexFilterApplication::NotApplicable(
                PartialIndexFilterRejection::SourceUnchanged,
            );
        }
        IndexedSourceCombination::Unchanged => F::path_source(path).clone(),
    };

    PartialIndexFilterApplication::Rewritten {
        source,
        residual: match residual.len() {
            0 => None,
            1 => Some(
                ir::PredicatePlan::new(
                    residual
                        .pop()
                        .expect("partial-index residual length was checked"),
                )
                .expect("access-filter residual predicate is already validated"),
            ),
            _ => Some(
                ir::PredicatePlan::new(helix_ast::expr::Predicate::and(residual))
                    .expect("access-filter residual predicate is already validated"),
            ),
        },
    }
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
        AccessFilterIndexAtom::Equality { property, value } => {
            let key = catalog::ScopedPropertyKey::new(label.clone(), property.clone());
            F::equality_index(indexes, &key)
                .map(|index| F::equality_source(index, key, value.clone()))
                .ok_or(MissingAccessIndex::Equality)
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
