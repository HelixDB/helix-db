/// parse -> analyze -> generate -> compile

use crate::helixc::{
    generator::generator_types::Source as GeneratedSource,
    parser::helix_parser::{
        Source as ParsedSource,
        Content,
        HelixParser,
        HxFile,
    },
    analyzer::analyzer::analyze,
};

use std::{
    path::PathBuf,
    fmt::Write,
    fs,
    process::Command,
};

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

fn generate_content(content: String) -> Content {
    let name = "test_queries_and_schema".to_string();
    let files = vec![HxFile { name, content }];
    Content {
        content: String::new(),
        files,
        source: ParsedSource::default(),
    }
}

fn parse_content(content: &Content) -> Result<ParsedSource, String> {
    let source = match HelixParser::parse_source(&content) {
        Ok(source) => source,
        Err(e) => return Err(format!("{}", e)),
    };
    Ok(source)
}

fn analyze_source(source: ParsedSource) -> Result<GeneratedSource, String> {
    let (diagnostics, source) = analyze(&source);
    if !diagnostics.is_empty() {
        for diag in diagnostics {
            let filepath = diag.filepath.clone().unwrap_or("queries.hx".to_string());
            println!("{}", diag.render(&source.src, &filepath));
        }
        return Err("failed".to_string());
    }

    Ok(source)
}

fn generate(str_content: String) -> Result<GeneratedSource, String> {
    let mut content = generate_content(str_content);
    content.source = parse_content(&content)?;
    let analyzed_source = analyze_source(content.source.clone())?;
    Ok(analyzed_source)
}

fn compile(analyzed_source: GeneratedSource) -> Result<(), String> {
    match check_helix_installation() {
        Ok(_) => {}
        Err(e) => return Err(format!("Error, helix is not installed: {:?}", e)),
    };

    let output = dirs::home_dir().unwrap().join(".helix/repo/helix-db/helix-container".to_string());
    let file_path = PathBuf::from(&output).join("src/queries.rs");
    let mut generated_rust_code = String::new();
    match write!(&mut generated_rust_code, "{}", analyzed_source) {
        Ok(_) => {}
        Err(e) => return Err(format!("Failed to write queries file: {:?}", e)),
    }
    match fs::write(file_path, generated_rust_code) {
        Ok(_) => {},
        Err(e) => return Err(format!("Failed to write queries file: {:?}", e)),
    }

    let mut runner = Command::new("cargo");
    runner
        .arg("build")
        .arg("--release")
        .current_dir(PathBuf::from(&output))
        .env("RUSTFLAGS", "-Awarnings");

    match runner.output() {
        Ok(output) => {
            if output.status.success() {
                return Ok(());
            } else {
                return Err("failed to build helix".to_string());
            }
        }
        Err(e) => return Err(format!("failed to build helix: {:?}", e)),
    }
}

#[test]
fn generator_test_1() {
    let input = r#"
        N::Patient {
            name: String,
            age: I64
        }

        N::Doctor {
            name: String,
            city: String
        }

        E::Visit {
            From: Patient,
            To: Doctor,
            Properties: {
                doctors_summary: String,
                date: I64
            }
        }

        QUERY create_data(doctor_name: String, doctor_city: String, patient_name: String, patient_age: I64, summary: String, date: I64) =>
            doctor <- AddN<Doctor>({
                name: doctor_name,
                city: doctor_city
            })
            patient <- AddN<Patient>({
                name: patient_name,
                age: patient_age
            })
            AddE<Visit>({doctors_summary: summary, date: date})::From(patient)::To(doctor)
            RETURN patient

        QUERY get_patient(name: String) =>
            patient <- N<Patient>::WHERE(_::{name}::EQ(name))
            RETURN patient

        QUERY get_patients_visits_in_previous_month(name: String, date: I64) =>
            patient <- N<Patient>::WHERE(_::{name}::EQ(name))
            visits <- patient::OutE<Visit>::WHERE(_::{date}::GTE(date))
            RETURN visits

        QUERY get_visit_by_date(name: String, date: I64) =>
            patients <- N<Patient>
            patient <- patients::WHERE(_::{name}::EQ(name))
            visit <- patient::OutE<Visit>::WHERE(_::{date}::EQ(date))::RANGE(0, 1)
            RETURN patient, visit
    "#;

    match compile(generate(input.to_string()).unwrap()) {
        Ok(_) => {}
        Err(e) => {
            println!("error: {:?}", e);
            assert!(false);
        }
    };
}

#[test]
fn generator_test_2() {
    let input = r#"
        N::Chapter {
            chapter_index: I64
        }

        N::SubChapter {
            title: String,
            content: String
        }

        E::Contains {
            From: Chapter,
            To: SubChapter,
            Properties: {
            }
        }

        V::Embedding {
            chunk: String
        }

        E::EmbeddingOf {
            From: SubChapter,
            To: Embedding,
            Properties: {
                chunk: String
            }
        }

        QUERY loaddocs_rag(chapters: [{ id: I64, subchapters: [{ title: String, content: String, chunks: [{chunk: String, vector: [F64]}]}] }]) =>
            FOR {id, subchapters} IN chapters {
                chapter_node <- AddN<Chapter>({ chapter_index: id })
                FOR {title, content, chunks} IN subchapters {
                    subchapter_node <- AddN<SubChapter>({ title: title, content: content })
                    AddE<Contains>::From(chapter_node)::To(subchapter_node)
                    FOR {chunk, vector} IN chunks {
                        vec <- AddV<Embedding>(vector)
                        AddE<EmbeddingOf>({chunk: chunk})::From(subchapter_node)::To(vec)
                    }
                }
            }
            RETURN "Success"

        QUERY searchdocs_rag(query: [F64], k: I32) =>
            vecs <- SearchV<Embedding>(query, k)
            subchapters <- vecs::In<EmbeddingOf>
            RETURN subchapters::{title, content}
    "#;

    match compile(generate(input.to_string()).unwrap()) {
        Ok(_) => {}
        Err(e) => {
            println!("error: {:?}", e);
            assert!(false);
        }
    };
}

/*
#[test]
fn generator_test_3() {
    let input = r#"
    "#;

    match compile(generate(input.to_string()).unwrap()) {
        Ok(_) => {}
        Err(e) => {
            println!("error: {:?}", e);
            assert!(false);
        }
    };
}
*/

