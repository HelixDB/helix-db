//! Rule provenance captured by retained optimizer alternatives.

use serde::{Deserialize, Serialize};

use crate::rules;

/// Provenance for a physical alternative produced by one optimizer rule.
///
/// ```
/// use helix_planner::{optimizer::RuleProvenance, rules};
///
/// let metadata = rules::RuleMetadata::new(
///     rules::RuleId::new("source_access").unwrap(),
///     rules::RuleKind::Implementation,
/// );
/// let provenance = RuleProvenance::from_metadata(&metadata);
///
/// assert_eq!(provenance.rule_id().as_ref(), "source_access");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleProvenance {
    rule_id: rules::RuleId,
}

impl RuleProvenance {
    /// Capture the stable rule ID from rule metadata.
    pub fn from_metadata(metadata: &rules::RuleMetadata) -> Self {
        Self {
            rule_id: metadata.id.clone(),
        }
    }

    /// Stable ID of the rule that produced the alternative.
    pub const fn rule_id(&self) -> &rules::RuleId {
        &self.rule_id
    }
}
