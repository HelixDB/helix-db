use crate::corpus;

pub fn scenario(id: &str, body: &str) -> corpus::Scenario {
    let feature = gherkin::Feature::parse(
        format!("Feature: Harness\n  Scenario: Fixture\n{body}\n"),
        gherkin::GherkinEnv::default(),
    )
    .unwrap();
    corpus::Scenario {
        id: id.into(),
        steps: feature.scenarios.into_iter().next().unwrap().steps,
    }
}
