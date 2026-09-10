//! Exact scenario ratchets. Updating a report never changes either manifest.
use crate::{
    corpus::{Result, Scenario},
    report::{Report, Status},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Required {
    pub revision: String,
    pub required: Vec<String>,
    pub exclusions: BTreeMap<String, String>,
    pub unclassified: Vec<String>,
}
#[derive(serde::Deserialize)]
struct Passing {
    revision: String,
    scenarios: Vec<String>,
}

pub fn verify(root: &Path, corpus: &[Scenario], report: &Report) -> Result<()> {
    let required: Required =
        serde_json::from_slice(&std::fs::read(root.join("required-mvp.json"))?)?;
    let previous: Passing =
        serde_json::from_slice(&std::fs::read(root.join("previously-passing.json"))?)?;
    if required.revision != report.revision || previous.revision != report.revision {
        return Err("manifest revision differs from pinned corpus".into());
    }
    let ids = corpus
        .iter()
        .map(|s| s.id.as_str())
        .collect::<BTreeSet<_>>();
    let reported = report
        .outcomes
        .iter()
        .map(|o| o.id.as_str())
        .collect::<BTreeSet<_>>();
    if report.scenarios != ids.len() || report.outcomes.len() != ids.len() || reported != ids {
        return Err("report must account for every scenario exactly once".into());
    }
    if report
        .outcomes
        .iter()
        .any(|o| matches!(o.status, Status::HarnessError | Status::NotExecuted))
    {
        return Err("harness errors and unexecuted scenarios cannot satisfy the gate".into());
    }
    if required
        .exclusions
        .values()
        .any(|reason| reason.trim().is_empty())
    {
        return Err("every exclusion requires a reviewed capability reason".into());
    }
    if previous.scenarios.iter().collect::<BTreeSet<_>>().len() != previous.scenarios.len() {
        return Err("regression manifest contains duplicate scenario IDs".into());
    }
    let classified = required
        .required
        .iter()
        .chain(required.exclusions.keys())
        .chain(&required.unclassified)
        .map(String::as_str)
        .collect::<Vec<_>>();
    if classified.len() != ids.len() || classified.iter().copied().collect::<BTreeSet<_>>() != ids {
        return Err("capability manifest must classify every scenario exactly once".into());
    }
    if !required.unclassified.is_empty() {
        return Err(format!(
            "{} unclassified scenarios remain",
            required.unclassified.len()
        )
        .into());
    }
    let passing = report
        .outcomes
        .iter()
        .filter(|o| matches!(o.status, Status::Passed))
        .map(|o| o.id.as_str())
        .collect::<BTreeSet<_>>();
    for id in &previous.scenarios {
        if !ids.contains(id.as_str()) {
            return Err(format!("previously passing scenario absent from corpus: {id}").into());
        }
    }
    let failures = required
        .required
        .iter()
        .chain(&previous.scenarios)
        .map(String::as_str)
        .filter(|id| !passing.contains(id))
        .collect::<BTreeSet<_>>();
    if !failures.is_empty() {
        return Err(format!(
            "{} required/regression scenarios did not pass; inspect local report",
            failures.len()
        )
        .into());
    }
    Ok(())
}

/// A review draft based on declared language boundaries, never pass/fail counts.
/// Unknown positive syntax remains unclassified and fails the gate.
pub fn draft(corpus: &[Scenario]) -> Required {
    let mut required = Vec::new();
    let mut exclusions = BTreeMap::new();
    let mut unclassified = Vec::new();
    for scenario in corpus {
        let negative = scenario
            .steps
            .iter()
            .any(|s| s.value.contains("should be raised"));
        let mut exclusion = None;
        let mut unknown = false;
        for step in &scenario.steps {
            if step.value.starts_with("there exists a procedure ") {
                exclusion = Some("Procedures".into());
                break;
            }
            // Both pinned binary-tree fixtures create exactly one label per
            // node. Their query text is covered by the fixture contract test.
            if matches!(
                step.value.as_str(),
                "the binary-tree-1 graph" | "the binary-tree-2 graph"
            ) {
                continue;
            }
            if !matches!(
                step.value.as_str(),
                "having executed:" | "executing query:" | "executing control query:"
            ) {
                continue;
            }
            let Some(text) = &step.docstring else {
                unknown = true;
                continue;
            };
            match helix_cypher::parse(text) {
                Err(e) if e.category == "UnsupportedFeature" => {
                    exclusion = Some(e.detail);
                    break;
                }
                Err(_) => {
                    if !negative || step.value == "having executed:" {
                        unknown = true;
                    }
                }
                Ok(ast) => {
                    if let Err(e) = helix_cypher::resolve(&ast)
                        && e.category == "UnsupportedFeature"
                    {
                        exclusion = Some(e.detail);
                        break;
                    }
                }
            }
        }
        match exclusion {
            Some(reason) => {
                exclusions.insert(scenario.id.clone(), reason);
            }
            None if unknown => unclassified.push(scenario.id.clone()),
            None => required.push(scenario.id.clone()),
        }
    }
    Required {
        revision: "007895aff5f33097d67b2e48a0a2babd6bd18590".into(),
        required,
        exclusions,
        unclassified,
    }
}

#[cfg(test)]
#[path = "tests/gate.rs"]
mod tests;
