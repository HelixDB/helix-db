//! Physical seeding for logical element sources.

use crate::{exec, logical, optimizer, physical, rules};

/// Implement logical element sources as LSM range access.
pub struct SourceAccessImplementationRule {
    metadata: rules::RuleMetadata,
}

impl Default for SourceAccessImplementationRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::SeedSourceAccess),
                rules::RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for SourceAccessImplementationRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::Pure(logical::PureLogicalOp::Source { element }) = input.expr
        else {
            return optimizer::RuleResult::NotApplicable;
        };
        rules::physical_result(physical::PhysicalAlternative::new(
            physical::PhysicalExpr::Access {
                element: *element,
                access: physical::PhysicalAccess::Kv(exec::KvReadPlan::RangeScan {
                    keyspace: rules::element_keyspace(*element),
                    start: exec::KvKeyBound::Unbounded,
                    end: exec::KvKeyBound::Unbounded,
                    limit: None,
                }),
            },
            rules::access_delivered(*element),
            input
                .storage
                .range_scan(input.storage.default_unknown_scan_rows),
        ))
    }
}
