//! Checks generated SDK query fixtures against the normalized planner properties.

use std::path::{Path, PathBuf};

use helix_ast::query::QueryRequest;
use helix_db_testkit::planner_domain;

const EXPECTED_SDK_CORPUS_REQUESTS: usize = 249;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Some(root) = std::env::args_os().nth(1).map(PathBuf::from) else {
        return Err("usage: check_planner_corpus <generated-rust-fixture-directory>".into());
    };
    let mut fixtures = Vec::new();
    collect_json(&root, &mut fixtures)?;
    fixtures.sort();
    if fixtures.len() != EXPECTED_SDK_CORPUS_REQUESTS {
        return Err(format!(
            "expected the complete {EXPECTED_SDK_CORPUS_REQUESTS}-request SDK corpus, found {}",
            fixtures.len(),
        )
        .into());
    }

    let mut planned = 0usize;
    let mut rejected = 0usize;
    for fixture in &fixtures {
        let bytes = std::fs::read(fixture)?;
        let request = serde_json::from_slice::<QueryRequest>(&bytes)
            .map_err(|error| format!("{}: {error}", fixture.display()))?;
        let outcome = planner_domain::check_query(
            request.query(),
            &helix_planner::context::PlannerContext::default(),
        )
        .map_err(|error| format!("{}: {error}", fixture.display()))?;
        match outcome {
            planner_domain::PlannerCaseOutcome::Planned { .. } => planned += 1,
            planner_domain::PlannerCaseOutcome::Rejected { .. } => rejected += 1,
        }
    }

    println!(
        "checked {} SDK planner seeds: {planned} planned, {rejected} typed rejections",
        fixtures.len()
    );
    Ok(())
}

fn collect_json(root: &Path, output: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_json(&path, output)?;
        } else if path
            .extension()
            .is_some_and(|extension| extension == "json")
        {
            output.push(path);
        }
    }
    Ok(())
}
