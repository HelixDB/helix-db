//! Pinned, checksum-verified upstream Gherkin inventory. No query is rewritten.
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    pub id: String,
    pub steps: Vec<gherkin::Step>,
}

#[derive(serde::Deserialize)]
struct Manifest {
    revision: String,
    files: BTreeMap<String, String>,
}

pub fn load(root: &Path) -> Result<Vec<Scenario>> {
    let manifest_bytes = std::fs::read(root.join("manifest.json"))?;
    let digest = Sha256::digest(&manifest_bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    if digest != "6f0985621c28db5aac127520a016d249f1798a3717c0e29c29fff975255ad61b" {
        return Err("pinned corpus manifest checksum mismatch".into());
    }
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes)?;
    if manifest.revision != "007895aff5f33097d67b2e48a0a2babd6bd18590" {
        return Err("unexpected TCK revision".into());
    }
    let source = root.join("opencypher");
    let files = files(&source)?;
    if files.len() != manifest.files.len() {
        return Err("vendored corpus file inventory differs from manifest".into());
    }
    for (path, expected) in &manifest.files {
        if Path::new(path)
            .components()
            .any(|c| !matches!(c, std::path::Component::Normal(_)))
        {
            return Err("unsafe manifest path".into());
        }
        let bytes = std::fs::read(source.join(path))?;
        if Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
            != *expected
        {
            return Err(format!("checksum mismatch: {path}").into());
        }
    }
    let mut scenarios = Vec::new();
    for path in files
        .into_iter()
        .filter(|p| p.extension().is_some_and(|x| x == "feature"))
    {
        let relative = path.strip_prefix(&source)?.to_string_lossy().to_string();
        let text = std::fs::read_to_string(&path)?;
        scenarios.extend(parse_feature(&relative, &text)?);
    }
    scenarios.sort_by(|a, b| a.id.cmp(&b.id));
    if scenarios.len() != 3897 {
        return Err("pinned scenario denominator changed".into());
    }
    if scenarios.windows(2).any(|s| s[0].id == s[1].id) {
        return Err("duplicate scenario identity".into());
    }
    Ok(scenarios)
}

fn parse_feature(path: &str, text: &str) -> Result<Vec<Scenario>> {
    let feature = gherkin::Feature::parse(table_escapes(text), gherkin::GherkinEnv::default())?;
    let mut scenarios = Vec::new();
    let background = feature.background.map(|b| b.steps).unwrap_or_default();
    for scenario in feature.scenarios {
        expand(path, &background, scenario, &mut scenarios)?;
    }
    for rule in feature.rules {
        let mut steps = background.clone();
        if let Some(background) = rule.background {
            steps.extend(background.steps);
        }
        for scenario in rule.scenarios {
            expand(path, &steps, scenario, &mut scenarios)?;
        }
    }
    Ok(scenarios)
}

fn expand(
    path: &str,
    background: &[gherkin::Step],
    scenario: gherkin::Scenario,
    out: &mut Vec<Scenario>,
) -> Result<()> {
    let mut steps = background.to_vec();
    steps.extend(scenario.steps);
    if scenario.examples.is_empty() {
        out.push(Scenario {
            id: format!("{path} :: {}", scenario.name),
            steps,
        });
        return Ok(());
    }
    for (block, examples) in scenario.examples.iter().enumerate() {
        let Some(table) = &examples.table else {
            return Err("scenario outline has no example table".into());
        };
        let Some(headers) = table.rows.first() else {
            return Err("empty example table".into());
        };
        for (index, row) in table.rows.iter().skip(1).enumerate() {
            if row.len() != headers.len() {
                return Err("nonrectangular example table".into());
            }
            let replace = |text: &str| {
                headers
                    .iter()
                    .zip(row)
                    .fold(text.to_owned(), |text, (key, value)| {
                        text.replace(&format!("<{key}>"), value)
                    })
            };
            let mut expanded = steps.clone();
            for step in &mut expanded {
                step.value = replace(&step.value);
                step.docstring = step.docstring.as_ref().map(|s| replace(s));
                if let Some(table) = &mut step.table {
                    for row in &mut table.rows {
                        for value in row {
                            *value = replace(value);
                        }
                    }
                }
            }
            out.push(Scenario {
                id: format!("{path} :: {} :: example {block}:{index}", scenario.name),
                steps: expanded,
            });
        }
    }
    Ok(())
}

fn files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut result = Vec::new();
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_symlink() {
            return Err("symlinks are not accepted in vendored resources".into());
        }
        if kind.is_dir() {
            result.extend(files(&entry.path())?);
        } else if kind.is_file() {
            result.push(entry.path());
        }
    }
    result.sort();
    Ok(result)
}

// Upstream Gherkin preserves unknown table escapes literally. gherkin-rs
// rejects them, so quote only those backslashes before its parser sees them.
// This leaves decoded table values unchanged; docstrings are never modified.
fn table_escapes(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut docstring = false;
    for line in text.lines() {
        if line.trim_start().starts_with("\"\"\"") {
            docstring = !docstring;
        }
        if !docstring && line.trim_start().starts_with('|') {
            let mut chars = line.chars().peekable();
            while let Some(c) = chars.next() {
                result.push(c);
                if c == '\\' {
                    match chars.peek().copied() {
                        Some('\\' | '|' | 'n') => {
                            result.push(chars.next().expect("peeked character"));
                        }
                        _ => result.push('\\'),
                    }
                }
            }
        } else {
            result.push_str(line);
        }
        result.push('\n');
    }
    result
}

#[cfg(test)]
#[path = "tests/corpus.rs"]
mod tests;
