use crate::{optimizer, rules};

#[test]
fn rule_provenance_captures_rule_metadata_id() {
    let metadata = rules::RuleMetadata::new(
        rules::RuleId::new("captured_rule").unwrap(),
        rules::RuleKind::Implementation,
    );

    let provenance = optimizer::RuleProvenance::from_metadata(&metadata);

    assert_eq!(provenance.rule_id().as_ref(), "captured_rule");
}
