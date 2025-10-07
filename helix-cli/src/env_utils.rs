use eyre::Result;
use std::collections::HashMap;
use std::path::Path;

/// Load environment variables from .env file and shell environment
///
/// Precedence order (highest to lowest):
/// 1. Shell environment variables
/// 2. .env file variables
pub fn load_env_variables(
    env_file_path: Option<&Path>,
    project_root: &Path,
) -> Result<HashMap<String, String>> {
    let mut vars = HashMap::new();

    // Load from .env file if specified
    if let Some(env_path) = env_file_path {
        let full_path = project_root.join(env_path);
        if full_path.exists() {
            // Parse the .env file with improved handling
            let env_content = std::fs::read_to_string(&full_path)?;
            for (line_num, line) in env_content.lines().enumerate() {
                let trimmed = line.trim();
                // Skip empty lines and comments
                if trimmed.is_empty() || trimmed.starts_with('#') {
                    continue;
                }

                // Parse KEY=VALUE pairs (find first = to handle values with = in them)
                if let Some(eq_pos) = trimmed.find('=') {
                    let key = trimmed[..eq_pos].trim();
                    let value = trimmed[eq_pos + 1..].trim();

                    // Validate key
                    if key.is_empty() {
                        eprintln!("Warning: Skipping empty key at line {} in {}",
                                  line_num + 1, full_path.display());
                        continue;
                    }

                    // Remove surrounding quotes if present (both single and double)
                    let value = if (value.starts_with('"') && value.ends_with('"'))
                        || (value.starts_with('\'') && value.ends_with('\'')) {
                        &value[1..value.len() - 1]
                    } else {
                        value
                    };

                    vars.insert(key.to_string(), value.to_string());
                } else if !trimmed.starts_with("export ") {
                    // Skip lines that start with "export " (shell syntax)
                    eprintln!("Warning: Skipping malformed line {} in {}: {}",
                              line_num + 1, full_path.display(), trimmed);
                }
            }
        } else {
            eprintln!("Warning: env_file '{}' not found, skipping", full_path.display());
        }
    }

    // Add shell environment variables (higher priority - overwrites .env)
    for (key, value) in std::env::vars() {
        // Skip HELIX_ prefixed variables as they're managed internally
        if !key.starts_with("HELIX_") {
            vars.insert(key, value);
        }
    }

    Ok(vars)
}