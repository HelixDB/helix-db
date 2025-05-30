use crate::{
    instance_manager::InstanceInfo,
    styled_string::StyledString,
    types::*,
};
use helixdb::helixc::{
    analyzer::analyzer::analyze,
    generator::{generator_types::Source as GeneratedSource, tsdisplay::ToTypeScript},
    parser::helix_parser::{Content, HelixParser, HxFile, Source},
};
use std::{
    fs::{self, DirEntry, File},
    io::{ErrorKind, Write},
    net::{SocketAddr, TcpListener},
    path::{PathBuf, Path},
    error::Error,
};
use toml::Value;
use reqwest::blocking::Client;
use serde_json::Value as JsonValue;

pub const DB_DIR: &str = "helixdb-cfg/";

pub const DEFAULT_SCHEMA: &str = r#"// Start building your schema here.
//
// The schema is used to to ensure a level of type safety in your queries.
//
// The schema is made up of Node types, denoted by N::,
// and Edge types, denoted by E::
//
// Under the Node types you can define fields that
// will be stored in the database.
//
// Under the Edge types you can define what type of node
// the edge will connect to and from, and also the
// properties that you want to store on the edge.
//
// Example:
//
// N::User {
//     Name: String,
//     Label: String,
//     Age: Integer,
//     IsAdmin: Boolean,
// }
//
// E::Knows {
//     From: User,
//     To: User,
//     Properties: {
//         Since: Integer,
//     }
// }
//
// For more information on how to write queries,
// see the documentation at https://docs.helix-db.com
// or checkout our GitHub at https://github.com/HelixDB/helix-db
"#;

pub const DEFAULT_QUERIES: &str = r#"// Start writing your queries here.
//
// You can use the schema to help you write your queries.
//
// Queries take the form:
//     QUERY {query name}({input name}: {input type}) =>
//         {variable} <- {traversal}
//         RETURN {variable}
//
// Example:
//     QUERY GetUserFriends(user_id: String) =>
//         friends <- N<User>(user_id)::Out<Knows>
//         RETURN friends
//
//
// For more information on how to write queries,
// see the documentation at https://docs.helix-db.com
// or checkout our GitHub at https://github.com/HelixDB/helix-db

QUERY hnswinsert(vector: [Float]) =>
    AddV<Vector>(vector)
    RETURN "Success"

QUERY hnswload(vectors: [[Float]]) =>
    res <- BatchAddV<Type>(vectors)
    RETURN res::{ID}

QUERY hnswsearch(query: [Float], k: Integer) =>
    res <- SearchV<Type>(query, k)
    RETURN res

QUERY ragloaddocs(docs: [{ doc: String, vecs: [[F64]] }]) =>
    FOR {doc, vec} IN docs {
        doc_node <- AddN<Type>({ content: doc })
        vectors <- BatchAddV<Doc>(vecs)
        FOR vec IN vectors {
            AddE<Contains>::From(doc_node)::To(vec)
        }
    }
    RETURN "Success"

QUERY ragsearchdocs(query: [F64], k: I32) =>
    vec <- SearchV<Vector>(query, k)
    doc_node <- vec::In<Contains>
    RETURN doc_node::{content}
"#;

pub fn check_helix_installation() -> Result<PathBuf, String> {
    let home_dir = dirs::home_dir().ok_or("Could not determine home directory")?;
    let repo_path = home_dir.join(".helix/repo/helix-db");
    let container_path = repo_path.join("helix-container");
    let cargo_path = container_path.join("Cargo.toml");

    if !repo_path.exists()
        || !repo_path.is_dir()
        || !container_path.exists()
        || !container_path.is_dir()
        || !cargo_path.exists()
    {
        return Err("run `helix install` first.".to_string());
    }

    Ok(container_path)
}

pub fn get_cfg_deploy_path(cmd_path: Option<String>) -> Result<String, CliError> {
    if let Some(path) = cmd_path {
        return Ok(path);
    }

    let cwd = ".";
    let files = match check_and_read_files(cwd) {
        Ok(files) => files,
        Err(_) => {
            return Ok(DB_DIR.to_string());
        }
    };

    if !files.is_empty() {
        return Ok(cwd.to_string());
    }

    Ok(DB_DIR.to_string())
}

pub fn find_available_port(start_port: u16) -> Option<u16> {
    let mut port = start_port;
    while port < 65535 {
        let addr = format!("0.0.0.0:{}", port).parse::<SocketAddr>().unwrap();
        match TcpListener::bind(addr) {
            Ok(listener) => {
                drop(listener);
                let localhost = format!("127.0.0.1:{}", port).parse::<SocketAddr>().unwrap();
                match TcpListener::bind(localhost) {
                    Ok(local_listener) => {
                        drop(local_listener);
                        return Some(port);
                    }
                    Err(e) => {
                        if e.kind() != ErrorKind::AddrInUse {
                            return None;
                        }
                        port += 1;
                        continue;
                    }
                }
            }
            Err(e) => {
                if e.kind() != ErrorKind::AddrInUse {
                    return None;
                }
                port += 1;
                continue;
            }
        }
    }
    None
}

pub fn check_and_read_files(path: &str) -> Result<Vec<DirEntry>, CliError> {
    if !fs::read_dir(&path)
        .map_err(CliError::Io)?
        .any(|file| file.unwrap().file_name() == "schema.hx")
    {
        return Err(CliError::from(format!(
            "{}",
            "No schema file found".red().bold()
        )));
    }

    if !fs::read_dir(&path)
        .map_err(CliError::Io)?
        .any(|file| file.unwrap().file_name() == "config.hx.json")
    {
        return Err(CliError::from(format!(
            "{}",
            "No config.hx.json file found".red().bold()
        )));
    }

    let files: Vec<DirEntry> = fs::read_dir(&path)?
        .filter_map(|entry| entry.ok())
        .filter(|file| file.file_name().to_string_lossy().ends_with(".hx"))
        .collect();

    // Check for query files (exclude schema.hx)
    let has_queries = files.iter().any(|file| file.file_name() != "schema.hx");
    if !has_queries {
        return Err(CliError::from(format!(
            "{}",
            "No query files (.hx) found".red().bold()
        )));
    }

    Ok(files)
}

pub fn to_snake_case(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut prev_is_uppercase = false;

    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 && !prev_is_uppercase {
                result.push('_');
            }
            result.push(c.to_lowercase().next().unwrap());
            prev_is_uppercase = true;
        } else {
            result.push(c);
            prev_is_uppercase = false;
        }
    }
    result
}

fn generate_content(files: &Vec<DirEntry>) -> Result<Content, CliError> {
    let files = files
        .iter()
        .map(|file| {
            let name = file.path().to_string_lossy().into_owned();
            let content = fs::read_to_string(file.path()).unwrap();
            HxFile { name, content }
        })
        .collect();

    let content = Content {
        content: String::new(),
        files,
        source: Source::default(),
    };

    Ok(content)
}

fn parse_content(content: &Content) -> Result<Source, CliError> {
    let source = match HelixParser::parse_source(&content) {
        Ok(source) => source,
        Err(e) => {
            return Err(CliError::from(format!("{}", e)));
        }
    };

    Ok(source)
}

fn analyze_source(source: Source) -> Result<GeneratedSource, CliError> {
    let (diagnostics, source) = analyze(&source);
    if !diagnostics.is_empty() {
        for diag in diagnostics {
            let filepath = diag.filepath.clone().unwrap_or("queries.hx".to_string());
            println!("{}", diag.render(&source.src, &filepath));
        }
        return Err(CliError::CompileFailed);
    }

    Ok(source)
}

pub fn generate(files: &Vec<DirEntry>) -> Result<(Content, GeneratedSource), CliError> {
    let mut content = generate_content(&files)?;
    content.source = parse_content(&content)?;
    let analyzed_source = analyze_source(content.source.clone())?;
    Ok((content, analyzed_source))
}

pub fn print_instnace(instance: &InstanceInfo) {
    let rg: bool = instance.running;
    println!(
        "{} {}{}",
        if rg {
            "Instance ID:".green().bold()
        } else {
            "Instance ID:".yellow().bold()
        },
        if rg {
            instance.id.green().bold()
        } else {
            instance.id.yellow().bold()
        },
        if rg {
            " (running)".to_string().green().bold()
        } else {
            " (not running)".to_string().yellow().bold()
        },
    );
    println!("└── Label: {}", instance.label.underline());
    println!("└── Port: {}", instance.port);
    println!("└── Available endpoints:");
    instance
        .available_endpoints
        .iter()
        .for_each(|ep| println!("    └── /{}", ep));
}

pub fn gen_typescript(source: &GeneratedSource, output_path: &str) -> Result<(), CliError> {
    let mut file = File::create(PathBuf::from(output_path).join("interface.d.ts"))?;

    for node in &source.nodes {
        write!(file, "{}", node.to_typescript())?;
    }
    for edge in &source.edges {
        write!(file, "{}", edge.to_typescript())?;
    }
    for vector in &source.vectors {
        write!(file, "{}", vector.to_typescript())?;
    }

    Ok(())
}

pub fn get_crate_version<P: AsRef<Path>>(path: P) -> Result<Version, String> {
    let cargo_toml_path = path.as_ref().join("Cargo.toml");
    if !cargo_toml_path.exists() {
        return Err("Not a Rust crate: Cargo.toml not found".to_string());
    }

    let contents = fs::read_to_string(&cargo_toml_path)
        .map_err(|e| format!("Failed to read Cargo.toml: {}", e))?;

    let parsed_toml = contents
        .parse::<Value>()
        .map_err(|e| format!("Failed to parse Cargo.toml: {}", e))?;

    let version = parsed_toml
        .get("package")
        .and_then(|pkg| pkg.get("version"))
        .and_then(|v| v.as_str())
        .ok_or("Version field not found in [package] section")?;

    let vers = Version::parse(version)?;
    Ok(vers)
}

pub fn get_remote_helix_version() -> Result<Version, Box<dyn Error>> {
    let client = Client::new();

    let url = "https://api.github.com/repos/HelixDB/helix-db/releases/latest";

    let response = client
        .get(url)
        .header("User-Agent", "rust")
        .header("Accept", "application/vnd.github+json")
        .send()?
        .text()?;

    let json: JsonValue = serde_json::from_str(&response)?;
    let tag_name = json
        .get("tag_name")
        .and_then(|v| v.as_str())
        .ok_or("Failed to find tag_name in response")?
        .to_string();

    Ok(Version::parse(&tag_name)?)
}

// TODO:
// Spinner::new
// Spinner::stop_with_message
// Dots9 style

