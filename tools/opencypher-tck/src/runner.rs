use crate::{
    corpus::{self, Scenario},
    report::Status,
    snapshot, values,
};
use db::{cypher, HelixDB, HelixDbSource};
use std::{collections::BTreeMap, path::Path, time::Duration};
use tokio::sync::watch;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Failure {
    pub status: Status,
    pub reason: String,
}
impl From<Box<dyn std::error::Error + Send + Sync>> for Failure {
    fn from(e: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self {
            status: Status::HarnessError,
            reason: e.to_string(),
        }
    }
}
impl From<db::error::HelixDbError> for Failure {
    fn from(e: db::error::HelixDbError) -> Self {
        Self {
            status: if matches!(e, db::error::HelixDbError::QueryDeadlineExceeded) {
                Status::TimedOut
            } else {
                Status::HarnessError
            },
            reason: e.to_string(),
        }
    }
}
fn failed(reason: impl Into<String>) -> Failure {
    Failure {
        status: Status::Failed,
        reason: reason.into(),
    }
}
fn blocked(error: cypher::Error) -> Failure {
    if matches!(
        error,
        cypher::Error::Storage(db::error::HelixDbError::QueryDeadlineExceeded)
    ) {
        return db::error::HelixDbError::QueryDeadlineExceeded.into();
    }
    Failure {
        status: Status::SetupBlocked,
        reason: format!("Setup:{error}"),
    }
}

pub(crate) async fn execute_tracked(
    scenario: &Scenario,
    planned: &watch::Sender<bool>,
    timeout: Duration,
) -> Result<(), Failure> {
    let source = HelixDbSource::InMemory {
        database: "opencypher-tck".into(),
    };
    let config = source
        .embedded_default_config()
        .with_query_telemetry(db::config::QueryTelemetry::Disabled);
    let db = HelixDB::open_with_config(source, config).await?;
    // Drop only the statement future on timeout; retain the database handle
    // until close has shut down its tasks, including aborted write transactions.
    let result = tokio::time::timeout(timeout, steps(&db, scenario, planned)).await;
    db.close().await?;
    result.unwrap_or_else(|_| {
        Err(Failure {
            status: Status::TimedOut,
            reason: "ScenarioTimeout".into(),
        })
    })
}

async fn steps(
    db: &HelixDB,
    scenario: &Scenario,
    planned: &watch::Sender<bool>,
) -> Result<(), Failure> {
    let mut parameters = BTreeMap::new();
    let mut subject: Option<cypher::Result<cypher::Response>> = None;
    let mut before = None;
    let mut assertion = false;
    for step in &scenario.steps {
        let text = step.value.trim();
        match text {
            "an empty graph" | "any graph" => {}
            "having executed:" => {
                db.cypher(request(step, parameters.clone())?)
                    .await
                    .map_err(blocked)?;
            }
            "parameters are:" => {
                for row in table(step)? {
                    if row.len() != 2 {
                        return Err(Failure {
                            status: Status::HarnessError,
                            reason: "parameter table width".into(),
                        });
                    }
                    parameters.insert(row[0].clone(), values::parse(&row[1])?.parameter()?);
                }
            }
            "executing query:" | "executing control query:" => {
                if subject.is_some() && !assertion {
                    return Err(Failure {
                        status: Status::HarnessError,
                        reason: "previous subject lacks a result/error assertion".into(),
                    });
                }
                before = Some(snapshot::take(db).await?);
                let result = db.cypher(request(step, parameters.clone())?).await;
                let did_plan = match &result {
                    Ok(_) => true,
                    Err(cypher::Error::Query(e)) => {
                        e.phase == helix_planner::relational::ErrorPhase::Runtime
                    }
                    Err(cypher::Error::Storage(_) | cypher::Error::Json(_)) => false,
                };
                if did_plan && !*planned.borrow() {
                    planned.send_replace(true);
                }
                subject = Some(result);
                assertion = false;
            }
            "the result should be empty" => {
                let result = result(&subject)?;
                if !result.rows.is_empty() {
                    return Err(failed("NonemptyResult"));
                }
                assertion = true;
            }
            "the result should be, in any order:"
            | "the result should be, in order:"
            | "the result should be (ignoring element order for lists):"
            | "the result should be, in order (ignoring element order for lists):" => {
                let response = result(&subject)?;
                let table = table(step)?;
                let Some(columns) = table.first() else {
                    return Err(Failure {
                        status: Status::HarnessError,
                        reason: "missing expected columns".into(),
                    });
                };
                if &response.columns != columns {
                    return Err(failed(format!(
                        "Columns: expected {columns:?}, received {:?}",
                        response.columns
                    )));
                }
                let expected = table[1..]
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|s| values::parse(s))
                            .collect::<corpus::Result<Vec<_>>>()
                    })
                    .collect::<corpus::Result<Vec<_>>>()?;
                let actual = response
                    .rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(values::wire)
                            .collect::<corpus::Result<Vec<_>>>()
                    })
                    .collect::<corpus::Result<Vec<_>>>()?;
                if expected
                    .iter()
                    .chain(&actual)
                    .any(|row| row.len() != columns.len())
                {
                    return Err(Failure {
                        status: Status::HarnessError,
                        reason: "NonrectangularResult".into(),
                    });
                }
                let unordered_lists = text.contains("ignoring element order");
                if !values::rows_equal(&expected, &actual, !text.contains("in order"), |a, b| {
                    values::rows_equal(a, b, false, |a, b| values::equal(a, b, unordered_lists))
                }) {
                    return Err(failed(format!(
                        "Rows: expected {expected:?}, received {actual:?}"
                    )));
                }
                assertion = true;
            }
            "no side effects" | "the side effects should be:" => {
                let before = before.as_ref().ok_or_else(|| Failure {
                    status: Status::HarnessError,
                    reason: "side effects without a subject".into(),
                })?;
                let actual = before.changes(&snapshot::take(db).await?);
                let mut expected = actual
                    .keys()
                    .map(|k| (k.clone(), 0))
                    .collect::<BTreeMap<_, _>>();
                if text != "no side effects" {
                    for row in table(step)? {
                        if row.len() != 2 || !expected.contains_key(&row[0]) {
                            return Err(Failure {
                                status: Status::HarnessError,
                                reason: "invalid side effect table".into(),
                            });
                        }
                        expected.insert(
                            row[0].clone(),
                            row[1]
                                .parse()
                                .map_err(|e: std::num::ParseIntError| Failure {
                                    status: Status::HarnessError,
                                    reason: e.to_string(),
                                })?,
                        );
                    }
                }
                if actual != expected {
                    return Err(failed(format!(
                        "SideEffects: expected {expected:?}, received {actual:?}"
                    )));
                }
            }
            _ if text.starts_with("a ") && text.contains(" should be raised at ") => {
                let (category, rest) = text[2..]
                    .split_once(" should be raised at ")
                    .expect("matched error step");
                let (phase, detail) = rest.split_once(": ").ok_or_else(|| Failure {
                    status: Status::HarnessError,
                    reason: "invalid error expectation".into(),
                })?;
                assert_error(&subject, category, phase, detail)?;
                let after = snapshot::take(db).await?;
                if before.as_ref() != Some(&after) {
                    return Err(failed("RollbackMismatch"));
                }
                assertion = true;
            }
            _ if text.starts_with("the ") && text.ends_with(" graph") => {
                let name = &text[4..text.len() - 6];
                setup_graph(
                    db,
                    name,
                    &Path::new(env!("CARGO_MANIFEST_DIR")).join("vendor/opencypher/tck/graphs"),
                )
                .await?;
            }
            _ if text.starts_with("there exists a procedure ") => {
                return Err(Failure {
                    status: Status::SetupBlocked,
                    reason: "Setup:Procedures".into(),
                })
            }
            _ => {
                return Err(Failure {
                    status: Status::HarnessError,
                    reason: format!("unrecognized step {text}"),
                })
            }
        }
    }
    if !assertion {
        return Err(Failure {
            status: Status::HarnessError,
            reason: "subject lacks result/error assertion".into(),
        });
    }
    Ok(())
}

fn result(
    subject: &Option<cypher::Result<cypher::Response>>,
) -> Result<&cypher::Response, Failure> {
    match subject {
        Some(Ok(result)) => Ok(result),
        Some(Err(cypher::Error::Storage(db::error::HelixDbError::QueryDeadlineExceeded))) => {
            Err(db::error::HelixDbError::QueryDeadlineExceeded.into())
        }
        Some(Err(cypher::Error::Query(error))) if error.category == "UnsupportedFeature" => {
            Err(Failure {
                status: Status::Unsupported,
                reason: error.detail.clone(),
            })
        }
        Some(Err(error)) => Err(failed(format!("UnexpectedError: {error}"))),
        None => Err(Failure {
            status: Status::HarnessError,
            reason: "assertion without query".into(),
        }),
    }
}
fn table(step: &gherkin::Step) -> Result<&[Vec<String>], Failure> {
    step.table
        .as_ref()
        .map(|t| t.rows.as_slice())
        .ok_or_else(|| Failure {
            status: Status::HarnessError,
            reason: format!("missing table for {}", step.value),
        })
}
fn request(
    step: &gherkin::Step,
    parameters: BTreeMap<String, helix_ast::query::QueryValue>,
) -> Result<cypher::Request, Failure> {
    let query = step.docstring.clone().ok_or_else(|| Failure {
        status: Status::HarnessError,
        reason: "query without docstring".into(),
    })?;
    Ok(cypher::Request {
        query,
        parameters,
        query_name: None,
    })
}

#[cfg(test)]
#[path = "tests/runner.rs"]
mod tests;

async fn setup_graph(db: &HelixDB, name: &str, root: &Path) -> Result<(), Failure> {
    if !matches!(name, "binary-tree-1" | "binary-tree-2") {
        return Err(Failure {
            status: Status::HarnessError,
            reason: format!("unknown named graph {name}"),
        });
    }
    let script =
        std::fs::read_to_string(root.join(name).join(format!("{name}.cypher"))).map_err(|e| {
            Failure {
                status: Status::HarnessError,
                reason: e.to_string(),
            }
        })?;
    for query in script.split(';').filter(|s| !s.trim().is_empty()) {
        db.cypher(cypher::Request::new(query))
            .await
            .map_err(blocked)?;
    }
    Ok(())
}

fn assert_error(
    subject: &Option<cypher::Result<cypher::Response>>,
    category: &str,
    phase: &str,
    detail: &str,
) -> Result<(), Failure> {
    let Some(Err(error)) = subject else {
        return Err(failed("ExpectedError: statement succeeded"));
    };
    if matches!(
        error,
        cypher::Error::Storage(db::error::HelixDbError::QueryDeadlineExceeded)
    ) {
        return Err(db::error::HelixDbError::QueryDeadlineExceeded.into());
    }
    let cypher::Error::Query(error) = error else {
        return Err(failed(format!("ExpectedCypherError: {error}")));
    };
    if error.category == "UnsupportedFeature" {
        return Err(Failure {
            status: Status::Unsupported,
            reason: error.detail.clone(),
        });
    }
    let phase_matches = match phase {
        "compile time" => error.phase == helix_planner::relational::ErrorPhase::Compile,
        "runtime" => error.phase == helix_planner::relational::ErrorPhase::Runtime,
        "any time" => true,
        _ => {
            return Err(Failure {
                status: Status::HarnessError,
                reason: "unknown error phase".into(),
            })
        }
    };
    if error.category != category || !phase_matches || (detail != "*" && error.detail != detail) {
        return Err(failed(format!(
            "ErrorMismatch: expected {category}/{phase}/{detail}, received {}/{:?}/{}",
            error.category, error.phase, error.detail
        )));
    }
    Ok(())
}
