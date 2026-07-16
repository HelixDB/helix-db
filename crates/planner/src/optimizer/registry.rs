//! Validated optimizer rule registry.

use std::collections::BTreeSet;

use super::rule;
use crate::{ir, rules};

/// Error returned when an optimizer rule registry violates its construction
/// contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptimizerRuleRegistryError {
    /// A Cascades optimizer without rules cannot produce physical alternatives.
    Empty,
    /// Rule IDs are trace and provenance keys, so a registry may contain each
    /// ID at most once.
    DuplicateRuleId(rules::RuleId),
    /// The production known-rule registry may not contain custom rule IDs.
    CustomRuleId(rules::RuleId),
    /// The production known-rule registry must contain every known rule.
    MissingKnownRuleId(rules::KnownRuleId),
}

/// Non-empty, duplicate-free optimizer rule registry.
///
/// ```
/// use helix_planner::{optimizer, rules};
///
/// struct TestRule {
///     metadata: rules::RuleMetadata,
/// }
///
/// impl optimizer::OptimizerRule for TestRule {
///     fn metadata(&self) -> &rules::RuleMetadata {
///         &self.metadata
///     }
///
///     fn apply(&self, _input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
///         optimizer::RuleResult::NotApplicable
///     }
/// }
///
/// let rule = TestRule {
///     metadata: rules::RuleMetadata::new(
///         rules::RuleId::new("example_rule").unwrap(),
///         rules::RuleKind::Exploration,
///     ),
/// };
///
/// let registry = optimizer::OptimizerRuleRegistry::try_from_rules(vec![&rule]).unwrap();
///
/// assert_eq!(registry.rule_count(), 1);
/// ```
pub struct OptimizerRuleRegistry<'a> {
    rules: ir::AtLeast<&'a dyn rule::OptimizerRule, 1>,
}

impl<'a> OptimizerRuleRegistry<'a> {
    /// Build a registry from ordered rule references.
    pub fn try_from_rules(
        rules: Vec<&'a dyn rule::OptimizerRule>,
    ) -> Result<Self, OptimizerRuleRegistryError> {
        let rules =
            ir::AtLeast::<_, 1>::try_from_vec(rules).ok_or(OptimizerRuleRegistryError::Empty)?;
        let mut ids = BTreeSet::new();
        for rule in &rules {
            let id = rule.metadata().id.clone();
            if !ids.insert(id.clone()) {
                return Err(OptimizerRuleRegistryError::DuplicateRuleId(id));
            }
        }
        Ok(Self { rules })
    }

    /// Build the complete production known-rule registry.
    pub fn try_from_known_rules(
        rules: Vec<&'a dyn rule::OptimizerRule>,
    ) -> Result<Self, OptimizerRuleRegistryError> {
        let registry = Self::try_from_rules(rules)?;
        let known = registry
            .rules
            .iter()
            .map(|rule| {
                let id = &rule.metadata().id;
                id.known_inventory()
                    .ok_or_else(|| OptimizerRuleRegistryError::CustomRuleId(id.clone()))
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        for expected in rules::KnownRuleId::ALL {
            if !known.contains(expected) {
                return Err(OptimizerRuleRegistryError::MissingKnownRuleId(*expected));
            }
        }
        Ok(registry)
    }

    /// Return the number of rules in registry order.
    pub fn rule_count(&self) -> usize {
        self.rules.len()
    }

    pub(in crate::optimizer) fn len(&self) -> usize {
        self.rules.len()
    }

    pub(in crate::optimizer) fn as_slice(&self) -> &[&'a dyn rule::OptimizerRule] {
        self.rules.as_ref()
    }

    pub(in crate::optimizer) fn iter(
        &self,
    ) -> impl Iterator<Item = &'a dyn rule::OptimizerRule> + '_ {
        self.rules.iter().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestRule {
        metadata: rules::RuleMetadata,
    }

    impl TestRule {
        fn new(id: &'static str) -> Self {
            Self {
                metadata: rules::RuleMetadata::new(
                    rules::RuleId::new(id).unwrap(),
                    rules::RuleKind::Exploration,
                ),
            }
        }

        fn known(id: rules::KnownRuleId) -> Self {
            Self {
                metadata: rules::RuleMetadata::new(
                    rules::RuleId::known(id),
                    rules::RuleKind::Exploration,
                ),
            }
        }
    }

    impl rule::OptimizerRule for TestRule {
        fn metadata(&self) -> &rules::RuleMetadata {
            &self.metadata
        }

        fn apply(&self, _input: rule::RuleInput<'_>) -> rule::RuleResult {
            rule::RuleResult::NotApplicable
        }
    }

    #[test]
    fn rule_registry_rejects_empty_rule_sets() {
        match OptimizerRuleRegistry::try_from_rules(Vec::new()) {
            Err(error) => assert_eq!(error, OptimizerRuleRegistryError::Empty),
            Ok(_) => panic!("empty rule registry must be rejected"),
        }
    }

    #[test]
    fn rule_registry_rejects_duplicate_rule_ids() {
        let first = TestRule::new("same_rule");
        let second = TestRule::new("same_rule");

        match OptimizerRuleRegistry::try_from_rules(vec![&first, &second]) {
            Err(error) => assert_eq!(
                error,
                OptimizerRuleRegistryError::DuplicateRuleId(
                    rules::RuleId::new("same_rule").unwrap()
                )
            ),
            Ok(_) => panic!("duplicate rule IDs must be rejected"),
        }
    }

    #[test]
    fn rule_registry_preserves_unique_registry_order() {
        let first = TestRule::new("first");
        let second = TestRule::new("second");

        let registry = OptimizerRuleRegistry::try_from_rules(vec![&first, &second]).unwrap();

        assert_eq!(registry.rule_count(), 2);
        assert_eq!(
            registry
                .iter()
                .map(|rule| rule.metadata().id.as_ref())
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
    }

    #[test]
    fn known_rule_registry_requires_complete_known_inventory() {
        let rules = rules::KnownRuleId::ALL
            .iter()
            .copied()
            .map(TestRule::known)
            .collect::<Vec<_>>();
        let refs = rules
            .iter()
            .map(|rule| rule as &dyn rule::OptimizerRule)
            .collect();

        let registry = OptimizerRuleRegistry::try_from_known_rules(refs).unwrap();

        assert_eq!(registry.rule_count(), rules::KnownRuleId::ALL.len());
    }

    #[test]
    fn known_rule_registry_rejects_missing_known_rules() {
        let rules = rules::KnownRuleId::ALL[1..]
            .iter()
            .copied()
            .map(TestRule::known)
            .collect::<Vec<_>>();
        let refs = rules
            .iter()
            .map(|rule| rule as &dyn rule::OptimizerRule)
            .collect();

        match OptimizerRuleRegistry::try_from_known_rules(refs) {
            Err(error) => assert_eq!(
                error,
                OptimizerRuleRegistryError::MissingKnownRuleId(rules::KnownRuleId::FilterPushdown)
            ),
            Ok(_) => panic!("known rule registry must reject missing production rules"),
        }
    }

    #[test]
    fn known_rule_registry_rejects_custom_rule_ids() {
        let mut rules = rules::KnownRuleId::ALL
            .iter()
            .copied()
            .map(TestRule::known)
            .collect::<Vec<_>>();
        rules.push(TestRule::new("custom_rule"));
        let refs = rules
            .iter()
            .map(|rule| rule as &dyn rule::OptimizerRule)
            .collect();

        match OptimizerRuleRegistry::try_from_known_rules(refs) {
            Err(error) => assert_eq!(
                error,
                OptimizerRuleRegistryError::CustomRuleId(
                    rules::RuleId::new("custom_rule").unwrap()
                )
            ),
            Ok(_) => panic!("known rule registry must reject custom rule IDs"),
        }
    }
}
