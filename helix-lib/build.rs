use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;
use regex::Regex;

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let workspace_root = find_workspace_root(&manifest_dir);
    
    // Scan for handlers in workspace
    let handlers = scan_workspace_for_handlers(&workspace_root);
    
    // Generate metadata file
    let metadata_code = generate_metadata_code(&handlers);
    let metadata_path = PathBuf::from(&out_dir).join("handler_metadata.rs");
    fs::write(&metadata_path, metadata_code).unwrap();
    
    // Generate method implementations
    let methods_code = generate_methods_code(&handlers);
    let methods_path = PathBuf::from(&out_dir).join("handler_methods.rs");
    fs::write(&methods_path, methods_code).unwrap();
    
    // Rerun if source files change
    println!("cargo:rerun-if-changed=build.rs");
    for handler in &handlers {
        println!("cargo:rerun-if-changed={}", handler.source_file);
    }
}

struct HandlerInfo {
    name: String,
    param_struct: Option<String>,
    return_type: Option<String>,
    source_file: String,
}

fn find_workspace_root(manifest_dir: &str) -> PathBuf {
    let mut current = PathBuf::from(manifest_dir);
    loop {
        let cargo_toml = current.join("Cargo.toml");
        if cargo_toml.exists() && let Ok(content) = fs::read_to_string(&cargo_toml) && content.contains("[workspace]") {
            return current;
        }
        if !current.pop() {
            break;
        }
    }
    PathBuf::from(manifest_dir) // Fallback
}

fn scan_workspace_for_handlers(workspace_root: &Path) -> Vec<HandlerInfo> {
    let mut handlers = Vec::new();
    let handler_regex = Regex::new(r"#\[handler[^\]]*\]").unwrap();
    let params_regex = Regex::new(r#"params\s*=\s*"([^"]+)""#).unwrap();
    let returns_regex = Regex::new(r#"returns\s*=\s*"([^"]+)""#).unwrap();
    let fn_regex = Regex::new(r"pub\s+fn\s+(\w+)").unwrap();
    
    for entry in WalkDir::new(workspace_root)
        .into_iter()
        .filter_entry(|e| {
            let path = e.path();
            let path_str = path.to_string_lossy();
            !path_str.contains("target")
                && !path_str.contains(".git")
                && !path_str.contains("/helix-lib/src")  // Exclude helix-lib source
                && !path_str.contains("helix-lib-example")  // Exclude example (types not in scope)
        })
    {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        let path = entry.path();
        
        if path.extension().and_then(|s| s.to_str()) == Some("rs") && let Ok(content) = fs::read_to_string(path) {
            let lines: Vec<&str> = content.lines().collect();
                // Find all #[handler] attributes
                for (line_num, line) in lines.iter().enumerate() {
                    if handler_regex.is_match(line) {
                        // Extract handler name from next few lines (function definition)
                        let mut found_fn = false;
                        for i in 1..=5 {
                            if let Some(next_line) = lines.get(line_num + i)
                                && let Some(captures) = fn_regex.captures(next_line)
                                && let Some(name_match) = captures.get(1) {
                                let name = name_match.as_str().to_string();
                                
                                let param_struct = params_regex
                                    .captures(line)
                                    .and_then(|c| c.get(1))
                                    .map(|m| m.as_str().to_string());
                                
                                let return_type = returns_regex
                                    .captures(line)
                                    .and_then(|c| c.get(1))
                                    .map(|m| m.as_str().to_string());
                                
                                handlers.push(HandlerInfo {
                                    name,
                                    param_struct,
                                    return_type,
                                    source_file: path.to_string_lossy().to_string(),
                                });
                                found_fn = true;
                                break;
                            }
                        }
                        if !found_fn {
                            // Try to extract from the same line or nearby
                            if let Some(captures) = fn_regex.captures(line)
                                && let Some(name_match) = captures.get(1) {
                                let name = name_match.as_str().to_string();
                                
                                let param_struct = params_regex
                                    .captures(line)
                                    .and_then(|c| c.get(1))
                                    .map(|m| m.as_str().to_string());
                                
                                let return_type = returns_regex
                                    .captures(line)
                                    .and_then(|c| c.get(1))
                                    .map(|m| m.as_str().to_string());
                                
                                handlers.push(HandlerInfo {
                                    name,
                                    param_struct,
                                    return_type,
                                    source_file: path.to_string_lossy().to_string(),
                                });
                            }
                        }
                    }
                }
            
        }
    }
    
    handlers
}

fn generate_metadata_code(handlers: &[HandlerInfo]) -> String {
    let mut code = String::from("// Auto-generated by build.rs\n\n");
    code.push_str("pub struct HandlerMetadata {\n");
    code.push_str("    pub name: &'static str,\n");
    code.push_str("    pub param_struct: Option<&'static str>,\n");
    code.push_str("    pub return_type: Option<&'static str>,\n");
    code.push_str("}\n\n");
    code.push_str("pub const HANDLERS: &[HandlerMetadata] = &[\n");
    
    for handler in handlers {
        code.push_str(&format!(
            "    HandlerMetadata {{\n        name: \"{}\",\n",
            handler.name
        ));
        if let Some(ref param) = handler.param_struct {
            code.push_str(&format!("        param_struct: Some(\"{}\"),\n", param));
        } else {
            code.push_str("        param_struct: None,\n");
        }
        if let Some(ref ret) = handler.return_type {
            code.push_str(&format!("        return_type: Some(\"{}\"),\n", ret));
        } else {
            code.push_str("        return_type: None,\n");
        }
        code.push_str("    },\n");
    }
    
    code.push_str("];\n");
    code
}

fn generate_methods_code(handlers: &[HandlerInfo]) -> String {
    let mut code = String::from("// Auto-generated handler methods by build.rs\n\n");
    // Note: Imports are not needed here because this code is included in client.rs
    // which already has the necessary imports
    code.push_str("use helix_db::protocol::{Request, Format, request::RequestType};\n");
    code.push_str("use serde_json;\n");
    code.push_str("use axum::body::Bytes;\n\n");
    
    code.push_str("impl HelixDB {\n");
    
    // Track seen handler names to avoid duplicates
    let mut seen = std::collections::HashSet::new();
    
    for handler in handlers {
        // Skip duplicates
        if seen.contains(&handler.name) {
            continue;
        }
        seen.insert(handler.name.clone());
        
        let method_name = sanitize_method_name(&handler.name);
        let handler_name = &handler.name;
        
        // Generate method signature
        if let Some(ref param_struct) = handler.param_struct {
            // Method with parameter struct
            if let Some(ref return_type) = handler.return_type {
                code.push_str(&format!(
                    "    pub fn {}(&self, params: {}) -> HelixResult<{}> {{\n",
                    method_name, param_struct, return_type
                ));
            } else {
                code.push_str(&format!(
                    "    pub fn {}(&self, params: {}) -> HelixResult<serde_json::Value> {{\n",
                    method_name, param_struct
                ));
            }
        } else {
            // Method without parameters
            if let Some(ref return_type) = handler.return_type {
                code.push_str(&format!(
                    "    pub fn {}(&self) -> HelixResult<{}> {{\n",
                    method_name, return_type
                ));
            } else {
                code.push_str(&format!(
                    "    pub fn {}(&self) -> HelixResult<serde_json::Value> {{\n",
                    method_name
                ));
            }
        }
        
        // Generate method body
        if handler.param_struct.is_some() {
            code.push_str("        // Serialize parameters\n");
            code.push_str("        let body = serde_json::to_vec(&params)\n");
            code.push_str("            .map_err(|e| HelixError::Serialization(e.to_string()))?;\n");
        } else {
            code.push_str("        // No parameters\n");
            code.push_str("        let body = serde_json::to_vec(&serde_json::json!({}))\n");
            code.push_str("            .map_err(|e| HelixError::Serialization(e.to_string()))?;\n");
        }
        
        code.push_str("\n        // Create Request\n");
        code.push_str(&format!(
            "        let request = Request {{\n            name: \"{}\".to_string(),\n",
            handler_name
        ));
        code.push_str("            req_type: RequestType::Query,\n");
        code.push_str("            api_key: None,\n");
        code.push_str("            body: Bytes::from(body),\n");
        code.push_str("            in_fmt: Format::Json,\n");
        code.push_str("            out_fmt: Format::Json,\n");
        code.push_str("        };\n\n");
        
        code.push_str("        // Create HandlerInput\n");
        code.push_str("        let input = HandlerInput {\n");
        code.push_str("            request,\n");
        code.push_str("            graph: self.engine.clone(),\n");
        code.push_str("        };\n\n");
        
        code.push_str("        // Look up handler\n");
        code.push_str(&format!(
            "        let handler = self.handlers.get(\"{}\")\n",
            handler_name
        ));
        code.push_str("            .ok_or_else(|| HelixError::HandlerNotFound(format!(\n");
        code.push_str(&format!(
            "                \"Handler '{}' not found. Available handlers: {{:?}}\",\n",
            handler_name
        ));
        code.push_str("                self.handlers.keys().collect::<Vec<_>>()\n");
        code.push_str("            )))?;\n\n");
        
        code.push_str("        // Call handler\n");
        code.push_str("        let response = handler(input)\n");
        code.push_str("            .map_err(HelixError::from)?;\n\n");
        
        code.push_str("        // Deserialize response\n");
        if let Some(ref return_type) = handler.return_type {
            code.push_str(&format!(
                "        let result: {} = serde_json::from_slice(&response.body)\n",
                return_type
            ));
        } else {
            code.push_str("        let result: serde_json::Value = serde_json::from_slice(&response.body)\n");
        }
        code.push_str("            .map_err(|e| {\n");
        code.push_str("                let body_str = String::from_utf8_lossy(&response.body);\n");
        code.push_str(&format!(
            "                HelixError::Deserialization(format!(\n                    \"Failed to deserialize response from handler '{}': {{}}. Response body: {{}}\",\n                    e, body_str\n                ))\n",
            handler_name
        ));
        code.push_str("            })?;\n\n");
        code.push_str("        Ok(result)\n");
        code.push_str("    }\n\n");
    }
    
    code.push_str("}\n");
    code
}

fn sanitize_method_name(name: &str) -> String {
    // Convert handler function names to method names in snake_case
    // e.g., "node_details_inner" -> "node_details"
    // e.g., "CreateUser" -> "create_user"
    let mut result = name.trim_end_matches("_inner")
        .trim_end_matches("_handler")
        .to_string();
    
    // Convert PascalCase/CamelCase to snake_case
    result = convert_to_snake_case(&result);
    
    result
}

fn convert_to_snake_case(name: &str) -> String {
    let mut result = String::new();
    let chars: Vec<char> = name.chars().collect();
    
    for (i, ch) in chars.iter().enumerate() {
        if ch.is_uppercase() && i > 0 {
            // Add underscore before uppercase if previous char is not uppercase
            if !chars[i - 1].is_uppercase() || (i + 1 < chars.len() && chars[i + 1].is_lowercase()) {
                result.push('_');
            }
        }
        result.push(ch.to_lowercase().next().unwrap_or(*ch));
    }
    
    result
}
