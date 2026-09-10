//! Bounded local workers own scenario lifetimes, including database open/close.
//! A timed-out worker is killed and reaped before another scenario replaces it.

use crate::{corpus, report, runner};
use futures::{stream, StreamExt};
use std::{path::Path, process::Stdio, time::Duration};
use tokio::{
    io::BufReader,
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{watch, Mutex},
};

mod frames;

const CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
enum Reply {
    Planned {
        id: String,
    },
    Finished {
        id: String,
        planned: bool,
        result: Result<(), runner::Failure>,
    },
}

struct Worker {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
}

impl Worker {
    fn start(executable: &Path) -> corpus::Result<Self> {
        let mut child = Command::new(executable)
            .arg("--worker")
            .env("HELIX_TELEMETRY_LEVEL", "off")
            .env("HELIX_NO_UPDATE_CHECK", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        let input = child.stdin.take().expect("worker stdin is piped");
        let output = BufReader::new(child.stdout.take().expect("worker stdout is piped"));
        Ok(Self {
            child,
            input,
            output,
        })
    }

    async fn scenario(
        &mut self,
        scenario: &corpus::Scenario,
        planned: &mut bool,
    ) -> corpus::Result<Result<(), runner::Failure>> {
        frames::write(&mut self.input, scenario).await?;
        loop {
            let reply = frames::read(&mut self.output)
                .await?
                .ok_or("worker exited before completing its scenario")?;
            match reply {
                Reply::Planned { id } if id == scenario.id && !*planned => *planned = true,
                Reply::Finished {
                    id,
                    planned: completed,
                    result,
                } if id == scenario.id && (!*planned || completed) => {
                    if result.as_ref().err().is_some_and(|failure| {
                        matches!(
                            failure.status,
                            report::Status::Passed | report::Status::NotExecuted
                        )
                    }) {
                        return Err("worker returned an invalid failure status".into());
                    }
                    *planned = completed;
                    return Ok(result);
                }
                Reply::Planned { .. } | Reply::Finished { .. } => {
                    return Err(
                        "worker returned an invalid scenario identity or progress transition"
                            .into(),
                    )
                }
            }
        }
    }

    async fn terminate(mut self) -> corpus::Result<()> {
        // kill() also waits for the child. kill_on_drop remains armed if this
        // bounded cleanup itself fails, and the failure cannot satisfy the gate.
        tokio::time::timeout(CLEANUP_TIMEOUT, self.child.kill()).await??;
        Ok(())
    }

    async fn finish(self) -> corpus::Result<()> {
        let Self {
            mut child,
            input,
            output,
        } = self;
        drop(input);
        drop(output);
        let status = tokio::time::timeout(CLEANUP_TIMEOUT, child.wait()).await;
        match status {
            Ok(Ok(status)) if status.success() => Ok(()),
            failure => {
                tokio::time::timeout(CLEANUP_TIMEOUT, child.kill()).await??;
                Err(format!("WorkerShutdown:{failure:?}").into())
            }
        }
    }
}

pub(crate) async fn run(
    corpus: &[corpus::Scenario],
    parallelism: usize,
    executable: &Path,
    timeout: Duration,
) -> corpus::Result<report::Report> {
    if !(1..=32).contains(&parallelism) {
        return Err("parallelism must be between 1 and 32".into());
    }
    if timeout.is_zero() || timeout > Duration::from_secs(60) {
        return Err("scenario timeout must be between 1 and 60000 milliseconds".into());
    }
    let inventory = report::inventory(corpus)?;
    let queue = Mutex::new(corpus.iter().zip(inventory.outcomes));
    let mut outcomes = stream::iter(0..parallelism.min(corpus.len()))
        .map(|_| async {
            let mut worker = None;
            let mut outcomes = Vec::new();
            loop {
                let Some((scenario, mut outcome)) = queue.lock().await.next() else {
                    break;
                };
                let execution = tokio::time::timeout(timeout, async {
                    if worker.is_none() {
                        worker = Some(Worker::start(executable)?);
                    }
                    worker
                        .as_mut()
                        .expect("worker was started")
                        .scenario(scenario, &mut outcome.planned)
                        .await
                })
                .await
                .map_err(|_| runner::Failure {
                    status: report::Status::TimedOut,
                    reason: "ScenarioLifetimeTimeout".into(),
                })
                .and_then(|result| {
                    result.map_err(|error| runner::Failure {
                        status: report::Status::HarnessError,
                        reason: format!("WorkerProtocol:{error}"),
                    })
                });
                let result = match execution {
                    Ok(result) => result,
                    Err(failure) => {
                        let failure = match worker.take() {
                            Some(worker) => match worker.terminate().await {
                                Ok(()) => failure,
                                Err(error) => runner::Failure {
                                    status: report::Status::HarnessError,
                                    reason: format!("WorkerCleanup:{error}; {}", failure.reason),
                                },
                            },
                            None => failure,
                        };
                        Err(failure)
                    }
                };
                let (status, reason) = match result {
                    Ok(()) => (report::Status::Passed, None),
                    Err(failure) => (failure.status, Some(failure.reason)),
                };
                outcome.status = status;
                outcome.reason = reason;
                outcomes.push(outcome);
            }
            let Some(worker) = worker else {
                return outcomes;
            };
            match worker.finish().await {
                Ok(()) => {}
                Err(error) => {
                    let outcome = outcomes.last_mut().expect("a worker owns a scenario");
                    outcome.status = report::Status::HarnessError;
                    outcome.reason = Some(error.to_string());
                }
            }
            outcomes
        })
        .buffer_unordered(parallelism)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    outcomes.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(report::summarize(outcomes))
}

pub(crate) async fn serve() -> corpus::Result<()> {
    let mut input = BufReader::new(tokio::io::stdin());
    let mut output = tokio::io::stdout();
    while let Some(scenario) = frames::read::<_, corpus::Scenario>(&mut input).await? {
        let (progress, mut updates) = watch::channel(false);
        let result = {
            // The inner deadline leaves room for graceful database cleanup.
            // Only the supervising process may enforce the outer lifetime limit.
            let execution = runner::execute_tracked(&scenario, &progress, Duration::from_secs(55));
            tokio::pin!(execution);
            loop {
                tokio::select! {
                    result = &mut execution => break result,
                    changed = updates.changed() => {
                        changed?;
                        frames::write(&mut output, &Reply::Planned { id: scenario.id.clone() }).await?;
                    }
                }
            }
        };
        let planned = *updates.borrow();
        frames::write(
            &mut output,
            &Reply::Finished {
                id: scenario.id,
                planned,
                result,
            },
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/workers.rs"]
mod tests;
