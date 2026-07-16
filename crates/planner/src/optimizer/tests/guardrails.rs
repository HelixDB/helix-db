use super::support;
use crate::{ir, optimizer, properties, rules};

#[test]
fn cascades_optimizer_guardrails_rule_fires_expressions_and_alternatives() {
    let exploring = support::StaticRule::new(
        "explore",
        rules::RuleKind::Exploration,
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(
            ir::AtLeast::<_, 1>::from_one(support::limit()),
        )),
    );
    let implementing = support::StaticRule::new(
        "impl",
        rules::RuleKind::Implementation,
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(
            ir::AtLeast::<_, 1>::from_one_and_rest(
                support::alternative(1),
                vec![support::alternative(2)],
            ),
        )),
    );

    let mut limited = support::config();
    limited.limits.rule_fires = properties::PositiveUsize::new(1).unwrap();
    let optimizer = support::optimizer(vec![&exploring, &implementing]);
    assert_eq!(
        support::optimize(&optimizer, support::source(), &limited).guardrail(),
        Some(optimizer::OptimizerGuardrail::RuleFires)
    );

    let mut limited = support::config();
    limited.limits.memo_expressions = properties::PositiveUsize::new(1).unwrap();
    let optimizer = support::optimizer(vec![&exploring]);
    assert_eq!(
        support::optimize(&optimizer, support::source(), &limited).guardrail(),
        Some(optimizer::OptimizerGuardrail::MemoExpressions)
    );

    let mut limited = support::config();
    limited.limits.alternatives_per_group = properties::PositiveUsize::new(1).unwrap();
    let optimizer = support::optimizer(vec![&implementing]);
    assert_eq!(
        support::optimize(&optimizer, support::source(), &limited).guardrail(),
        Some(optimizer::OptimizerGuardrail::AlternativesPerGroup)
    );
}

#[test]
fn optimizer_guardrail_serializes_memo_integrity_stop_reason() {
    let json = serde_json::to_string(&optimizer::OptimizerGuardrail::MemoIntegrity).unwrap();

    assert_eq!(json, "\"memo_integrity\"");
    assert_eq!(
        serde_json::from_str::<optimizer::OptimizerGuardrail>(&json).unwrap(),
        optimizer::OptimizerGuardrail::MemoIntegrity
    );
}
