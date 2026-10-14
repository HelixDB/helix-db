//! Recursive index translation of partly indexed predicates.
//!
//! A predicate is translated into disjunctive branches, `AND` into
//! intersection and `OR` into union. Every branch holds the index sets that
//! decide its indexed conjuncts and the conjuncts no index decides, which
//! only that branch's own index rows evaluate. So an indexed leaf is never
//! evaluated row by row, even inside an `OR` whose other branches mix in
//! unindexed conjuncts.
//!
//! The one exception is an `OR` with a branch no index narrows at all: every
//! row of the label may match that branch, so the whole `OR` is evaluated per
//! row. That reads no record a scan would not already read.

use super::super::atoms::access_filter_index_atom;
use super::super::labels::{access_filter_label, label_equality_matches};
use super::contracts::{
    IndexedSourceCombination, PartialIndexFilterApplication, PartialIndexFilterRejection,
};
use super::shared::{self, AccessFilterIndexFamily};
use crate::{analysis, catalog, context, ir};

/// One disjunct: index sets that decide its indexed conjuncts and the
/// conjuncts no index decides.
///
/// A branch with no sources and no residual is a tautology for the label.
#[derive(Debug, Clone, PartialEq)]
struct IndexBranch<S> {
    sources: Vec<S>,
    residual: Vec<helix_ast::expr::Predicate>,
}

impl<S: Clone> IndexBranch<S> {
    /// A branch that every label row satisfies.
    fn tautology() -> Self {
        Self {
            sources: Vec::new(),
            residual: Vec::new(),
        }
    }

    /// The conjunction of `self` and `other`, extending `self` in place: a
    /// long `AND` accumulates into one branch without copying it per child.
    fn and_extended(mut self, other: &Self) -> Self {
        self.sources.extend(other.sources.iter().cloned());
        self.residual.extend(other.residual.iter().cloned());
        self
    }

    /// The conjunction of `self` and `other`.
    fn and(&self, other: &Self) -> Self {
        Self {
            sources: self.sources.iter().chain(&other.sources).cloned().collect(),
            residual: self
                .residual
                .iter()
                .chain(&other.residual)
                .cloned()
                .collect(),
        }
    }
}

/// How far an `AND` may distribute over an `OR` with residual branches, and
/// whether an `OR` may become a union at all.
///
/// `Limited(n)` caps the branches a distribution produces at `n`. `Disabled`
/// keeps the opt-out: no distribution, and a multi-branch `OR` stays a
/// per-row residual.
fn branch_cap(planner_limits: &context::PlannerLimits) -> usize {
    match planner_limits.max_index_union_branches {
        context::IndexUnionBranchLimit::Disabled => 1,
        context::IndexUnionBranchLimit::Limited(limit) => limit.get(),
    }
}

/// Translate `predicate` over rows of `label` into index branches.
///
/// Every branch's sources are exact for the label's rows that satisfy its
/// indexed conjuncts, so the rows of the union of branches that pass their
/// own residuals are exactly the label rows that satisfy `predicate`.
/// Identical branches are kept once, the first time they occur.
fn translate<F>(
    predicate: &helix_ast::expr::Predicate,
    label: &ir::NonEmptyString,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
) -> Vec<IndexBranch<F::Source>>
where
    F: AccessFilterIndexFamily,
{
    let cap = branch_cap(planner_limits);
    match predicate {
        helix_ast::expr::Predicate::And { predicates } => {
            predicates
                .iter()
                .fold(vec![IndexBranch::tautology()], |conjunction, child| {
                    let child_branches = translate::<F>(child, label, indexes, planner_limits);
                    and_branches::<F>(conjunction, child_branches, child, cap)
                })
        }
        helix_ast::expr::Predicate::Or { predicates } => {
            let branches = dedup(
                predicates
                    .iter()
                    .flat_map(|child| translate::<F>(child, label, indexes, planner_limits))
                    .collect(),
            );
            if branches.contains(&IndexBranch::tautology()) {
                return vec![IndexBranch::tautology()];
            }
            // A branch no index narrows may match any label row, so the
            // whole disjunction is evaluated per row; with unions disabled,
            // so is every multi-branch disjunction.
            if branches.iter().any(|branch| branch.sources.is_empty())
                || (branches.len() > 1
                    && planner_limits.max_index_union_branches
                        == context::IndexUnionBranchLimit::Disabled)
            {
                return vec![IndexBranch {
                    sources: Vec::new(),
                    residual: vec![predicate.clone()],
                }];
            }
            branches
        }
        leaf => {
            if analysis::predicate_is_tautological_for_label(leaf, label)
                || label_equality_matches(leaf, label)
            {
                return vec![IndexBranch::tautology()];
            }
            let source = access_filter_index_atom(leaf, planner_limits)
                .ok()
                .and_then(|atom| shared::index_source_for_atom::<F>(label, &atom, indexes).ok());
            vec![match source {
                Some(source) => IndexBranch {
                    sources: vec![source],
                    residual: Vec::new(),
                },
                None => IndexBranch {
                    sources: Vec::new(),
                    residual: vec![leaf.clone()],
                },
            }]
        }
    }
}

/// `conjunction ∧ child`, where `child_branches` translates `child`.
fn and_branches<F>(
    conjunction: Vec<IndexBranch<F::Source>>,
    child_branches: Vec<IndexBranch<F::Source>>,
    child: &helix_ast::expr::Predicate,
    cap: usize,
) -> Vec<IndexBranch<F::Source>>
where
    F: AccessFilterIndexFamily,
{
    let merge_one = |conjunction: Vec<IndexBranch<F::Source>>, branch: &IndexBranch<F::Source>| {
        conjunction
            .into_iter()
            .map(|left| left.and_extended(branch))
            .collect()
    };
    let [one] = child_branches.as_slice() else {
        let all_narrowed = child_branches
            .iter()
            .all(|branch| !branch.sources.is_empty());
        // A residual-free disjunction is one exact union: the shared sets
        // are read once and intersected with it.
        if all_narrowed
            && child_branches
                .iter()
                .all(|branch| branch.residual.is_empty())
        {
            return merge_one(
                conjunction,
                &IndexBranch {
                    sources: vec![union_of_branches::<F>(&child_branches)],
                    residual: Vec::new(),
                },
            );
        }
        // Distribute the conjunction over the disjunction, so each branch
        // keeps its own residual, while the branch count stays in bounds.
        if all_narrowed && conjunction.len().saturating_mul(child_branches.len()) <= cap {
            return dedup(
                conjunction
                    .iter()
                    .flat_map(|left| child_branches.iter().map(|right| left.and(right)))
                    .collect(),
            );
        }
        // Past the bound, the union of the disjunction's sets still narrows
        // the rows, and the whole disjunction is their residual.
        let fallback = IndexBranch {
            sources: if all_narrowed {
                vec![union_of_branches::<F>(&child_branches)]
            } else {
                Vec::new()
            },
            residual: vec![child.clone()],
        };
        return merge_one(conjunction, &fallback);
    };
    merge_one(conjunction, one)
}

/// The index set of one branch: the intersection of its sources.
fn branch_source<F>(branch: &IndexBranch<F::Source>) -> F::Source
where
    F: AccessFilterIndexFamily,
{
    match branch.sources.as_slice() {
        [one] => one.clone(),
        sources => F::intersection_source(sources.to_vec()),
    }
}

/// The union of every branch's index set, ignoring residuals.
fn union_of_branches<F>(branches: &[IndexBranch<F::Source>]) -> F::Source
where
    F: AccessFilterIndexFamily,
{
    F::union_source(branches.iter().map(branch_source::<F>).collect())
}

fn dedup<S: PartialEq>(branches: Vec<IndexBranch<S>>) -> Vec<IndexBranch<S>> {
    branches.into_iter().fold(Vec::new(), |mut kept, branch| {
        if !kept.contains(&branch) {
            kept.push(branch);
        }
        kept
    })
}

/// Rewrite a filter over `path` by translating `predicate` into index
/// branches combined with the path source.
///
/// One branch narrows the source to its index sets with its residual as a
/// filter. Several residual-free branches are one union. Several branches
/// with residuals become a branch-residual union over a broad source, whose
/// residual-free branches are folded into one union branch; a
/// narrow source (point IDs, a search) is already bounded, so
/// its filter stays per row. Any branch no index narrows leaves the filter
/// per row.
pub(super) fn translated_index_filter<F>(
    path: &F::Path,
    predicate: &helix_ast::expr::Predicate,
    predicate_label: &analysis::FeasibleLabelScope,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
) -> PartialIndexFilterApplication<F::Source>
where
    F: AccessFilterIndexFamily,
{
    let source = F::path_source(path);
    let Some(label) = access_filter_label(F::source_common_label(source), predicate_label) else {
        return PartialIndexFilterApplication::NotApplicable(PartialIndexFilterRejection::NoLabel);
    };
    let branches = translate::<F>(predicate, &label, indexes, planner_limits);
    if branches.iter().any(|branch| branch.sources.is_empty()) {
        return PartialIndexFilterApplication::NotApplicable(
            PartialIndexFilterRejection::NoIndexedConjunct,
        );
    }
    let narrowed = |indexed: F::Source, residual: Vec<helix_ast::expr::Predicate>| {
        match shared::combine_indexed_filter_source::<F>(source, indexed) {
            IndexedSourceCombination::Rewritten(source) => {
                PartialIndexFilterApplication::Rewritten {
                    source,
                    residual: shared::conjunction_plan(residual),
                }
            }
            IndexedSourceCombination::Unchanged if residual.is_empty() => {
                PartialIndexFilterApplication::NotApplicable(
                    PartialIndexFilterRejection::SourceUnchanged,
                )
            }
            IndexedSourceCombination::Unchanged => PartialIndexFilterApplication::Rewritten {
                source: source.clone(),
                residual: shared::conjunction_plan(residual),
            },
        }
    };
    match branches.as_slice() {
        [one] => narrowed(branch_source::<F>(one), one.residual.clone()),
        many if many.iter().all(|branch| branch.residual.is_empty()) => {
            narrowed(union_of_branches::<F>(many), Vec::new())
        }
        // Residual-free branches need no per-branch filter, so they are one
        // union source (whose same-property literals become one batched
        // read); only branches with residuals stay separate.
        many if F::is_broad_source(source) => F::branch_residual_union({
            let (exact, residual): (Vec<_>, Vec<_>) = many
                .iter()
                .cloned()
                .partition(|branch| branch.residual.is_empty());
            let exact = match exact.as_slice() {
                [] => None,
                [one] => Some(branch_source::<F>(one)),
                exact => Some(union_of_branches::<F>(exact)),
            };
            residual
                .iter()
                .map(|branch| {
                    (
                        branch_source::<F>(branch),
                        shared::conjunction_plan(branch.residual.clone()),
                    )
                })
                .chain(exact.map(|source| (source, None)))
                .collect()
        })
        .map_or(
            PartialIndexFilterApplication::NotApplicable(
                PartialIndexFilterRejection::ResidualBranchesUnrepresentable,
            ),
            |source| PartialIndexFilterApplication::Rewritten {
                source,
                residual: None,
            },
        ),
        _ => PartialIndexFilterApplication::NotApplicable(
            PartialIndexFilterRejection::ResidualBranchesUnrepresentable,
        ),
    }
}
