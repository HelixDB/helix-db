mod access;
mod cardinality;
mod contracts;
mod core;
mod physical_contracts;
mod registry;
mod root;
mod stream;

pub use self::{
    access::*, cardinality::*, contracts::*, core::*, registry::SeedRuleSet, root::*, stream::*,
};

pub(crate) use self::access::{missing_index_candidates, CandidateIndexKind};

use self::physical_contracts::*;
use crate::{ir, logical, optimizer, physical};

fn physical_result(alternative: physical::PhysicalAlternative) -> optimizer::RuleResult {
    optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(
        ir::AtLeast::<_, 1>::from_one(alternative),
    ))
}

fn access_path_result(access: logical::AccessPath) -> optimizer::RuleResult {
    optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(
        ir::AtLeast::<_, 1>::from_one(logical::LogicalExpr::AccessPath(access)),
    ))
}

fn access_window_result(window: logical::AccessWindow) -> optimizer::RuleResult {
    optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(
        ir::AtLeast::<_, 1>::from_one(logical::LogicalExpr::AccessWindow(window)),
    ))
}

#[cfg(test)]
mod tests;
