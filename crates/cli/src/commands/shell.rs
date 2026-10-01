use super::query::{self, LocalOverrides, Target};
use crate::errors::CliError;
use crate::output::{self, Verbosity};
use eyre::Result;
use std::io::{self, Write as _};

/// Run an interactive JSON shell. Each non-command line must be one complete
/// v3 query request. Local requests keep the auth-disabled local path; Cloud
/// requests use the session-authenticated query broker.
///
/// Results go to stdout exactly as `helix query` prints them; the banner,
/// prompt, and errors go to stderr.
pub async fn run(instance: Option<String>) -> Result<()> {
    let target = Target::resolve(instance)?;
    output::intro(&format!("Helix shell · {}", target.name()));
    output::remark("Enter one v3 JSON request per line; :quit exits.");

    loop {
        if Verbosity::current().show_normal() {
            eprint!("helix> ");
            io::stderr().flush()?;
        }
        let mut line = String::new();
        if io::stdin().read_line(&mut line)? == 0 {
            break;
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if matches!(line, ":quit" | ":exit" | "quit" | "exit") {
            break;
        }
        let result = match serde_json::from_str(line) {
            Ok(request) => query::execute(&target, request, &LocalOverrides::default())
                .await
                .and_then(|outcome| query::print_outcome(&target, &outcome)),
            Err(error) => Err(CliError::new("invalid v3 query JSON")
                .with_caused_by(error.to_string())
                .into()),
        };
        let Err(error) = result else {
            continue;
        };
        output::print_error(&CliError::from_report(&error));
    }
    output::outro("Closed the shell");
    Ok(())
}
