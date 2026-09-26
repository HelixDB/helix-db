//! Complete plan captures shared by the regression gate and local review tool.
//! Only elapsed optimizer time is normalized, at explicitly typed boundaries.
//! Query values, cost estimates, traces and deterministic work stay observable.
use helix_planner::{context, planning, relational};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

mod boundaries;
mod execution;
mod matrix;
mod queries;
mod sdk;
pub use matrix::inputs;
pub use sdk::sdk_inputs;

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "frontend", content = "query", rename_all = "snake_case")]
pub enum Query {
    Cypher(String),
    Native(Box<helix_ast::batch::BatchQuery>),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Input {
    pub query: Query,
    pub context: context::PlannerContext,
}

#[derive(Serialize, Deserialize)]
pub struct Case {
    pub input: Input,
    pub outcome: Value,
}

#[derive(Serialize, Deserialize)]
pub struct Capture {
    pub schema: u32,
    pub cases: BTreeMap<String, Case>,
}

impl Capture {
    pub fn plan(inputs: BTreeMap<String, Input>) -> Self {
        Self {
            schema: 1,
            cases: inputs
                .into_iter()
                .map(|(name, input)| {
                    let outcome = execute(&input);
                    (name, Case { input, outcome })
                })
                .collect(),
        }
    }

    /// A replay uses the old queries, catalog, statistics and cost model inputs.
    /// It never regenerates fixtures from the candidate's default settings.
    pub fn replay(self) -> Self {
        assert_eq!(self.schema, 1, "unsupported capture schema");
        assert!(!self.cases.is_empty(), "empty capture cannot verify plans");
        Self::plan(
            self.cases
                .into_iter()
                .map(|(key, case)| (key, case.input))
                .collect(),
        )
    }

    /// Compact checked-in signatures cover the complete input and output. Human
    /// review uses the full captures, never just a changed hash or lower cost.
    pub fn manifest(&self) -> Value {
        let cases: BTreeMap<_, _> = self.cases.iter().map(|(name, case)| {
            let mut canonical = serde_json::to_value(case).unwrap();
            canonical.sort_all_objects();
            let digest = Sha256::digest(serde_json::to_vec(&canonical).unwrap());
            use std::fmt::Write;
            let mut sha256 = String::with_capacity(64);
            for byte in digest { write!(sha256, "{byte:02x}").unwrap(); }
            let metrics = case.outcome["plan"].get("metrics")
                .or_else(|| case.outcome["plan"].get("planner"));
            (name, json!({"sha256": sha256, "metrics": metrics, "error": case.outcome.get("error")}))
        }).collect();
        json!({"schema": self.schema, "cases": cases})
    }
}

fn execute(input: &Input) -> Value {
    match &input.query {
        Query::Cypher(query) => {
            let query = match helix_cypher::compile(query) {
                Ok(query) => query,
                Err(error) => return json!({"error": format!("parse/bind: {error:?}")}),
            };
            let plan = match relational::plan(query, &input.context) {
                Ok(plan) => plan,
                Err(error) => return json!({"error": format!("plan: {error:?}")}),
            };
            let windows: Vec<_> = (0..plan.query().operators().len())
                .map(|index| plan.input_window(index))
                .collect();
            let mut explanation = serde_json::to_value(plan.explain()).unwrap();
            normalize_cypher(&mut explanation);
            json!({"plan": explanation, "input_windows": windows, "execution": execution::row(&plan)})
        }
        Query::Native(query) => match planning::plan(query, &input.context) {
            Ok(plan) => {
                let execution = execution::native(&plan);
                let mut plan = serde_json::to_value(plan).unwrap();
                normalize_native(&mut plan);
                json!({"plan": plan, "execution": execution})
            }
            Err(error) => json!({"error": format!("plan: {error:?}")}),
        },
    }
}

fn normalize_native(plan: &mut Value) {
    // ExecutableSubplan deliberately has no metrics. Its expressions can have
    // arbitrary user keys, so recursively stripping matching names is unsafe.
    plan["metrics"]
        .as_object_mut()
        .expect("ExecutablePlan metrics")
        .remove("optimization_micros")
        .expect("elapsed native planning time");
}

fn normalize_cypher(plan: &mut Value) {
    plan["planner"]
        .as_object_mut()
        .expect("Explanation planner metrics")
        .remove("optimization_micros")
        .expect("elapsed row planning time");
    for operator in plan["operators"]
        .as_array_mut()
        .expect("Explanation operators")
    {
        let Some(graph) = operator.get_mut("graph").filter(|graph| !graph.is_null()) else {
            continue;
        };
        for source in graph["sources"]
            .as_array_mut()
            .expect("ExplainedMatch sources")
        {
            normalize_native(&mut source["access"]);
        }
    }
}

#[cfg(test)]
mod tests;
