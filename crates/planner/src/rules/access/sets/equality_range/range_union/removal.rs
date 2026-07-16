//! Shared retained-source contract for equality/range union pruning.

use crate::ir;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum CoveredSourceRemoval<S> {
    Removed(Vec<S>),
    Unchanged(Vec<S>),
}

pub(super) fn remove_covered_sources<'a, S, B, BuildBuckets, IsCovered>(
    plans: &'a ir::AtLeast<S, 2>,
    build_buckets: BuildBuckets,
    is_covered: IsCovered,
) -> CoveredSourceRemoval<S>
where
    S: Clone + 'a,
    BuildBuckets: FnOnce(&'a ir::AtLeast<S, 2>) -> B,
    IsCovered: Fn(&S, &B) -> bool,
{
    let buckets = build_buckets(plans);
    let mut changed = false;
    let mut retained = Vec::with_capacity(plans.len());
    for source in plans {
        if is_covered(source, &buckets) {
            changed = true;
        } else {
            retained.push(source.clone());
        }
    }
    if changed {
        CoveredSourceRemoval::Removed(retained)
    } else {
        CoveredSourceRemoval::Unchanged(retained)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removal_preserves_order_and_reports_changes() {
        let plans = ir::AtLeast::<_, 2>::try_from_vec(vec![1_u8, 2, 3]).unwrap();
        assert_eq!(
            remove_covered_sources(
                &plans,
                |plans| plans.iter().copied().collect::<Vec<_>>(),
                |source, buckets| buckets.contains(source) && *source != 1,
            ),
            CoveredSourceRemoval::Removed(vec![1])
        );
    }

    #[test]
    fn unchanged_sources_are_not_rebuilt() {
        let plans = ir::AtLeast::<_, 2>::from_pair(1_u8, 2);
        assert_eq!(
            remove_covered_sources(&plans, |_| (), |_, _| false),
            CoveredSourceRemoval::Unchanged(vec![1, 2])
        );
    }
}
