//! Access-distinct scheduler predicates.

use super::super::AccessDistinct;
use crate::logical;

pub(in crate::logical::access::unary) fn distinct_has_noop_candidate(
    distinct: &AccessDistinct,
) -> bool {
    distinct
        .access()
        .hard_cardinality_upper_bound()
        .is_some_and(|upper| upper <= 1)
        || matches!(
            distinct.access().source_kind(),
            logical::AccessSourceKind::PointIds
        )
}
