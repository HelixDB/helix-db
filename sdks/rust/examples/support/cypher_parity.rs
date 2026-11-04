//! Real Cypher and SQL calls through the public Rust SDK. The common parity
//! checker validates these raw results against the independently authored
//! fixture set.
use helix_db::{CacheConfig, Client, DbConfig, HelixDbSource};
use std::{collections::BTreeMap, fs, path::Path};

#[derive(serde::Deserialize)]
struct Corpus {
    schema_version: u32,
    cases: Vec<Case>,
}

#[derive(serde::Deserialize)]
struct Case {
    name: String,
    #[serde(default)]
    language: Language,
    query: String,
    #[serde(default)]
    parameters: Option<serde_json::Value>,
    #[serde(default)]
    mappings: helix_db::SqlMappings,
    #[serde(default)]
    after_disk_reopen: bool,
}

#[derive(Default, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum Language {
    #[default]
    Cypher,
    Sql,
}

impl Case {
    /// Run the case once through the public Cypher or SQL method.
    async fn run(&self, client: &Client) -> Result<helix_db::CypherResponse, helix_db::HelixError> {
        let parameters = self.parameters.clone().unwrap_or(serde_json::Value::Null);
        match self.language {
            Language::Cypher => {
                let parameters = match parameters {
                    serde_json::Value::Null => BTreeMap::new(),
                    parameters => serde_json::from_value(parameters)?,
                };
                client.cypher(&self.query, parameters, Some(&self.name)).await
            }
            Language::Sql => {
                let parameters = match parameters {
                    serde_json::Value::Null => helix_db::SqlParameters::default(),
                    parameters => serde_json::from_value(parameters)?,
                };
                client
                    .sql(&self.query, parameters, &self.mappings, Some(&self.name))
                    .await
            }
        }
    }
}

fn load() -> Result<Vec<Case>, Box<dyn std::error::Error>> {
    let fixture = std::env::var("HELIX_CYPHER_PARITY_FIXTURES").unwrap_or_else(|_| {
        concat!(env!("CARGO_MANIFEST_DIR"), "/../tests/cypher/runtime.json").into()
    });
    let corpus: Corpus = serde_json::from_slice(&fs::read(fixture)?)?;
    if corpus.schema_version != 1 || corpus.cases.len() != 24 {
        return Err("unsupported or incomplete Cypher parity corpus".into());
    }
    let mut names = std::collections::BTreeSet::new();
    for case in &corpus.cases {
        if case.name.is_empty()
            || !case
                .name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            || !names.insert(&case.name)
        {
            return Err("Cypher parity names must be unique safe basenames".into());
        }
    }
    Ok(corpus.cases)
}

pub async fn run(source: HelixDbSource, results: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cases = load()?;
    let results = results.join("cypher");
    fs::create_dir_all(&results)?;
    let config = || DbConfig::new().with_cache(CacheConfig::default());
    let mut client = Client::open_with_config(source.clone(), config()).await?;
    let execution = async {
        for case in &cases {
            let response = if case.after_disk_reopen && matches!(source, HelixDbSource::Disk { .. })
            {
                client.close().await?;
                let reader = Client::open_reader_with_config(source.clone(), config()).await?;
                let response = case.run(&reader).await;
                reader.close().await?;
                client = Client::open_with_config(source.clone(), config()).await?;
                response
            } else {
                case.run(&client).await
            };
            // Record actual failures without substituting the expected detail.
            // Cypher writes are never retried by this fixture driver.
            let response = match response {
                Ok(response) => serde_json::json!({"result": {
                    "columns": response.columns,
                    "rows": response.rows,
                }}),
                Err(error) => serde_json::json!({"error": error.to_string(), "code": error.error_code(), "details": error.remote_details()}),
            };
            fs::write(
                results.join(format!("{}.json", case.name)),
                serde_json::to_vec(&response)?,
            )?;
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    let closed = client.close().await;
    execution?;
    closed?;
    Ok(())
}

/// Execute either side of the parent harness's local HTTP server restart.
pub async fn run_http(
    url: &str,
    results: &Path,
    phase: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let cases = load()?;
    let after = match phase {
        "before" => false,
        "after" => true,
        _ => return Err("invalid Cypher HTTP phase".into()),
    };
    let results = results.join("cypher");
    fs::create_dir_all(&results)?;
    let client = Client::server(Some(url))?;
    let execution = async {
        let mut reopened = false;
        for case in cases {
            reopened |= case.after_disk_reopen;
            if reopened != after {
                continue;
            }
            let response = match case.run(&client).await {
                Ok(response) => serde_json::json!({"result": {
                    "columns": response.columns,
                    "rows": response.rows,
                }}),
                Err(error) => serde_json::json!({"error": error.to_string(), "code": error.error_code(), "details": error.remote_details()}),
            };
            fs::write(
                results.join(format!("{}.json", case.name)),
                serde_json::to_vec(&response)?,
            )?;
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    let closed = client.close().await;
    execution?;
    closed?;
    Ok(())
}
