//! Generic access-set normalization contract.
//!
//! Callers provide element-specific empty detection, nested-set inspection,
//! dedupe, and rebuild functions. This keeps node and edge algebra identical
//! without hiding their IR-specific invariants behind loosely typed callbacks.

use crate::ir;

/// Outcome of a canonicalization proof.
///
/// `Unchanged` is distinct from `Rewritten` so callers cannot accidentally
/// rebuild an equivalent tree and feed redundant alternatives into the memo.
pub(super) enum SourceSetRewrite<P> {
    /// The input set was already canonical.
    Unchanged,
    /// The input set was simplified to a new plan.
    Rewritten(P),
}

impl<P> SourceSetRewrite<P> {
    pub(super) fn into_simplification(self) -> SourceSetSimplification<P> {
        match self {
            Self::Unchanged => SourceSetSimplification::Unchanged,
            Self::Rewritten(plan) => SourceSetSimplification::Rewritten(plan),
        }
    }
}

/// Canonicalization outcome for an arbitrary access source.
///
/// `NotASet` is distinct from `Unchanged` so node/edge dispatch can preserve
/// whether canonicalization did not apply to this access family or proved that
/// a set was already canonical.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum SourceSetSimplification<P> {
    /// The access source was not a union or intersection set.
    NotASet,
    /// The access source was a set and was already canonical.
    Unchanged,
    /// The access source was rewritten to a new canonical plan.
    Rewritten(P),
}

pub(super) fn normalize_union<S, P, IsEmpty, Nested, Dedupe, Build>(
    plans: &ir::AtLeast<S, 2>,
    is_empty: IsEmpty,
    nested_union: Nested,
    dedupe: Dedupe,
    build: Build,
) -> SourceSetRewrite<P>
where
    S: Clone,
    IsEmpty: Fn(&S) -> bool,
    Nested: for<'a> Fn(&'a S) -> Option<&'a ir::AtLeast<S, 2>>,
    Dedupe: Fn(&mut Vec<S>, &mut bool),
    Build: FnOnce(Vec<S>) -> P,
{
    let mut changed = false;
    let mut flattened = Vec::new();
    plans.iter().for_each(|plan| {
        flatten_union(plan, &mut flattened, &mut changed, &is_empty, &nested_union);
    });
    dedupe(&mut flattened, &mut changed);
    if changed {
        SourceSetRewrite::Rewritten(build(flattened))
    } else {
        SourceSetRewrite::Unchanged
    }
}

pub(super) fn normalize_intersection<S, P, IsEmpty, Nested, Dedupe, Build, Empty>(
    plans: &ir::AtLeast<S, 2>,
    is_empty: IsEmpty,
    nested_intersection: Nested,
    dedupe: Dedupe,
    build: Build,
    empty: Empty,
) -> SourceSetRewrite<P>
where
    S: Clone,
    IsEmpty: Fn(&S) -> bool,
    Nested: for<'a> Fn(&'a S) -> Option<&'a ir::AtLeast<S, 2>>,
    Dedupe: Fn(&mut Vec<S>, &mut bool),
    Build: FnOnce(Vec<S>) -> P,
    Empty: FnOnce() -> P,
{
    let mut changed = false;
    let mut flattened = Vec::new();
    for plan in plans {
        if is_empty(plan) {
            return SourceSetRewrite::Rewritten(empty());
        }
        flatten_intersection(plan, &mut flattened, &mut changed, &nested_intersection);
    }
    dedupe(&mut flattened, &mut changed);
    if changed {
        SourceSetRewrite::Rewritten(build(flattened))
    } else {
        SourceSetRewrite::Unchanged
    }
}

fn flatten_union<S, IsEmpty, Nested>(
    plan: &S,
    flattened: &mut Vec<S>,
    changed: &mut bool,
    is_empty: &IsEmpty,
    nested_union: &Nested,
) where
    S: Clone,
    IsEmpty: Fn(&S) -> bool,
    Nested: for<'a> Fn(&'a S) -> Option<&'a ir::AtLeast<S, 2>>,
{
    if is_empty(plan) {
        *changed = true;
    } else if let Some(children) = nested_union(plan) {
        *changed = true;
        children.iter().for_each(|child| {
            flatten_union(child, flattened, changed, is_empty, nested_union);
        });
    } else {
        flattened.push(plan.clone());
    }
}

fn flatten_intersection<S, Nested>(
    plan: &S,
    flattened: &mut Vec<S>,
    changed: &mut bool,
    nested_intersection: &Nested,
) where
    S: Clone,
    Nested: for<'a> Fn(&'a S) -> Option<&'a ir::AtLeast<S, 2>>,
{
    if let Some(children) = nested_intersection(plan) {
        *changed = true;
        children.iter().for_each(|child| {
            flatten_intersection(child, flattened, changed, nested_intersection);
        });
    } else {
        flattened.push(plan.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum TestSource {
        Empty,
        Leaf(u8),
        Union(ir::AtLeast<TestSource, 2>),
        Intersect(ir::AtLeast<TestSource, 2>),
    }

    fn pair(left: TestSource, right: TestSource) -> ir::AtLeast<TestSource, 2> {
        ir::AtLeast::<_, 2>::from_pair(left, right)
    }

    fn is_empty(source: &TestSource) -> bool {
        matches!(source, TestSource::Empty)
    }

    fn union_children(source: &TestSource) -> Option<&ir::AtLeast<TestSource, 2>> {
        match source {
            TestSource::Union(children) => Some(children),
            TestSource::Empty | TestSource::Leaf(_) | TestSource::Intersect(_) => None,
        }
    }

    fn intersection_children(source: &TestSource) -> Option<&ir::AtLeast<TestSource, 2>> {
        match source {
            TestSource::Intersect(children) => Some(children),
            TestSource::Empty | TestSource::Leaf(_) | TestSource::Union(_) => None,
        }
    }

    fn dedupe_sources(sources: &mut Vec<TestSource>, changed: &mut bool) {
        let mut deduped = Vec::with_capacity(sources.len());
        sources.drain(..).for_each(|source| {
            if deduped.iter().any(|existing| existing == &source) {
                *changed = true;
            } else {
                deduped.push(source);
            }
        });
        *sources = deduped;
    }

    fn rewritten_sources(rewrite: SourceSetRewrite<Vec<TestSource>>) -> Vec<TestSource> {
        match rewrite.into_simplification() {
            SourceSetSimplification::Rewritten(sources) => sources,
            SourceSetSimplification::NotASet | SourceSetSimplification::Unchanged => {
                panic!("expected rewritten sources")
            }
        }
    }

    #[test]
    fn union_normalization_flattens_elides_empty_and_dedupes() {
        let rewrite = normalize_union(
            &pair(
                TestSource::Empty,
                TestSource::Union(pair(TestSource::Leaf(1), TestSource::Leaf(1))),
            ),
            is_empty,
            union_children,
            dedupe_sources,
            |sources| sources,
        );

        assert_eq!(rewritten_sources(rewrite), vec![TestSource::Leaf(1)]);
    }

    #[test]
    fn intersection_normalization_short_circuits_empty_sources() {
        let rewrite = normalize_intersection(
            &pair(TestSource::Leaf(1), TestSource::Empty),
            is_empty,
            intersection_children,
            dedupe_sources,
            |sources| sources,
            || vec![TestSource::Empty],
        );

        assert_eq!(rewritten_sources(rewrite), vec![TestSource::Empty]);
    }

    #[test]
    fn intersection_normalization_flattens_and_dedupes() {
        let rewrite = normalize_intersection(
            &pair(
                TestSource::Leaf(1),
                TestSource::Intersect(pair(TestSource::Leaf(2), TestSource::Leaf(1))),
            ),
            is_empty,
            intersection_children,
            dedupe_sources,
            |sources| sources,
            || vec![TestSource::Empty],
        );

        assert_eq!(
            rewritten_sources(rewrite),
            vec![TestSource::Leaf(1), TestSource::Leaf(2)]
        );
    }

    #[test]
    fn unchanged_sets_are_not_rebuilt() {
        let rewrite = normalize_union(
            &pair(TestSource::Leaf(1), TestSource::Leaf(2)),
            is_empty,
            union_children,
            dedupe_sources,
            |sources| sources,
        );

        assert_eq!(
            rewrite.into_simplification(),
            SourceSetSimplification::Unchanged
        );
    }
}
