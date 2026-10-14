#![recursion_limit = "256"]

mod corpus;
mod gate;
mod report;
mod runner;
mod snapshot;
mod values;
mod workers;

#[tokio::main(worker_threads = 2)]
async fn main() -> corpus::Result<()> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    if arguments == ["--worker"] {
        return workers::serve().await;
    }
    run(arguments, &std::env::current_exe()?).await
}

async fn run(arguments: Vec<String>, executable: &std::path::Path) -> corpus::Result<()> {
    let mut arguments = arguments.into_iter();
    let mut output = None;
    let mut inventory = false;
    let mut parallelism = 4;
    let mut filter = None;
    let mut gate = false;
    let mut draft = None;
    let mut timeout_ms = 60_000;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--output" => output = Some(arguments.next().ok_or("--output requires a path")?),
            "--inventory" => inventory = true,
            "--parallelism" => {
                parallelism = arguments.next().ok_or("missing parallelism")?.parse()?
            }
            "--scenario-timeout-ms" => {
                timeout_ms = arguments
                    .next()
                    .ok_or("missing scenario timeout")?
                    .parse()?
            }
            "--filter" => filter = Some(arguments.next().ok_or("missing filter")?),
            "--gate" => gate = true,
            "--profile-draft" => draft = Some(arguments.next().ok_or("missing draft path")?),
            _ => return Err(format!("unknown argument {argument}").into()),
        }
    }
    if !(1..=32).contains(&parallelism) {
        return Err("parallelism must be between 1 and 32".into());
    }
    if !(1..=60_000).contains(&timeout_ms) {
        return Err("scenario timeout must be between 1 and 60000 milliseconds".into());
    }
    if draft.is_some() && (gate || inventory || filter.is_some() || output.is_some()) {
        return Err("--profile-draft cannot be combined with execution/report flags".into());
    }
    let mut corpus =
        corpus::load(&std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("vendor"))?;
    if let Some(path) = draft {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        file.write_all(&serde_json::to_vec_pretty(&gate::draft(&corpus))?)?;
        return Ok(());
    }
    if let Some(filter) = filter {
        corpus.retain(|s| s.id.contains(&filter));
    }
    let report = if inventory {
        report::inventory(&corpus)?
    } else {
        workers::run(
            &corpus,
            parallelism,
            executable,
            std::time::Duration::from_millis(timeout_ms),
        )
        .await?
    };
    if let Some(output) = output {
        std::fs::write(output, serde_json::to_vec_pretty(&report)?)?;
    }
    println!(
        "{}",
        serde_json::to_string_pretty(
            &serde_json::json!({"scenarios":report.scenarios,"counts":report.counts,"blockers":report.blockers})
        )?
    );
    if report.counts.get("harness_error").copied().unwrap_or(0) > 0 {
        return Err("TCK harness errors must be resolved".into());
    }
    if gate {
        gate::verify(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("manifests"),
            &corpus,
            &report,
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod test_support;

#[cfg(test)]
#[path = "tests/cli.rs"]
mod cli_tests;
