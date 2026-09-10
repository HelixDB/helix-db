use crate::corpus::{Result, Scenario};
use std::collections::BTreeMap;

#[derive(Debug, serde::Serialize)]
pub struct Outcome {
    pub id: String,
    pub status: Status,
    pub parsed: bool,
    pub resolved: bool,
    pub planned: bool,
    pub reason: Option<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Passed,
    Failed,
    Unsupported,
    SetupBlocked,
    TimedOut,
    HarnessError,
    NotExecuted,
}

#[derive(serde::Serialize)]
pub struct Report {
    pub revision: &'static str,
    pub scenarios: usize,
    pub counts: BTreeMap<String, usize>,
    pub blockers: BTreeMap<String, usize>,
    pub outcomes: Vec<Outcome>,
}

pub fn inventory(corpus: &[Scenario]) -> Result<Report> {
    let mut outcomes = Vec::new();
    for scenario in corpus {
        let mut parsed = true;
        let mut resolved = true;
        let planned = false;
        let mut reason = None;
        for step in scenario
            .steps
            .iter()
            .filter(|s| s.value == "executing query:")
        {
            let Some(text) = &step.docstring else {
                return Err(format!("subject query has no docstring: {}", scenario.id).into());
            };
            match helix_cypher::parse(text) {
                Err(e) => {
                    parsed = false;
                    resolved = false;
                    reason = Some(e);
                }
                Ok(ast) => {
                    let Err(error) = helix_cypher::resolve(&ast) else {
                        continue;
                    };
                    resolved = false;
                    reason = Some(error);
                }
            }
        }
        let status = match &reason {
            Some(e) if e.category == "UnsupportedFeature" => Status::Unsupported,
            Some(_) => Status::NotExecuted,
            None => Status::NotExecuted,
        };
        outcomes.push(Outcome {
            id: scenario.id.clone(),
            status,
            parsed,
            resolved,
            planned,
            reason: reason.map(|e| format!("{}:{}", e.category, e.detail)),
        });
    }
    Ok(summarize(outcomes))
}

pub fn summarize(outcomes: Vec<Outcome>) -> Report {
    let mut counts = BTreeMap::new();
    let mut blockers = BTreeMap::new();
    for outcome in &outcomes {
        let name = serde_json::to_value(&outcome.status)
            .expect("status serializes")
            .as_str()
            .expect("status is string")
            .to_owned();
        *counts.entry(name).or_default() += 1;
        for (name, passed) in [
            ("parsed", outcome.parsed),
            ("resolved", outcome.resolved),
            ("planned", outcome.planned),
        ] {
            if passed {
                *counts.entry(name.into()).or_default() += 1;
            }
        }
        if let Some(reason) = &outcome.reason {
            *blockers.entry(reason.clone()).or_default() += 1;
        }
    }
    Report {
        revision: "007895aff5f33097d67b2e48a0a2babd6bd18590",
        scenarios: outcomes.len(),
        counts,
        blockers,
        outcomes,
    }
}
