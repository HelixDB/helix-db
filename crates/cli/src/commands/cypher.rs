use crate::{config::InstanceInfo, output, project::ProjectContext};
use eyre::{eyre, Result};

#[derive(clap::Args)]
#[command(group(clap::ArgGroup::new("cypher_input").required(true).args(["query", "file"])))]
pub struct Args {
    /// Local instance; defaults to dev or the sole linked instance
    instance: Option<String>,
    /// Cypher statement
    #[arg(short = 'e', long)]
    query: Option<String>,
    /// UTF-8 file containing one Cypher statement
    #[arg(short, long)]
    file: Option<String>,
    /// JSON object of query parameters
    #[arg(long, default_value = "{}")]
    parameters: String,
    #[arg(long)]
    host: Option<String>,
    #[arg(long)]
    port: Option<u16>,
    /// Show selected plans and blocking work without executing the statement
    #[arg(long)]
    explain: bool,
}

pub async fn run(args: Args) -> Result<()> {
    let Args {
        instance,
        query,
        file,
        parameters,
        host,
        port,
        explain,
    } = args;
    let project = ProjectContext::find_and_load(None)?;
    let instance = super::query::resolve_instance_name(&project, instance)?;
    let InstanceInfo::Local(config) = project.config.get_instance(&instance)? else {
        return Err(eyre!(
            "Cypher CLI requests currently require a local instance"
        ));
    };
    let query = match (query, file) {
        (Some(query), None) => query,
        (None, Some(file)) => std::fs::read_to_string(file)?,
        _ => return Err(eyre!("provide exactly one of --query or --file")),
    };
    let parameters: serde_json::Value = serde_json::from_str(&parameters)?;
    if !parameters.is_object() {
        return Err(eyre!("--parameters must be a JSON object"));
    }
    let endpoint = format!(
        "http://{}:{}/v2/cypher{}",
        host.as_deref().unwrap_or("localhost"),
        port.unwrap_or(config.port),
        if explain { "/explain" } else { "" },
    );
    let response = reqwest::Client::new()
        .post(endpoint)
        .json(&serde_json::json!({"query":query,"parameters":parameters}))
        .send()
        .await?;
    let status = response.status();
    let bytes = response.bytes().await?;
    if !status.is_success() {
        return Err(eyre!(
            "Cypher failed with HTTP {status}: {}",
            String::from_utf8_lossy(&bytes)
        ));
    }
    // Highlighted pretty JSON for humans, compact JSON under `--json`, as
    // `helix query` prints its responses.
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(());
    }
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_else(|_| {
        serde_json::Value::String(String::from_utf8_lossy(&bytes).into_owned())
    });
    output::emit(&body, |body| {
        println!("{}", output::json::pretty(body, console::colors_enabled()));
        Ok(())
    })
}
