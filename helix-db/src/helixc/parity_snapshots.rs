use crate::helixc::{
    analyzer::diagnostic::Diagnostic,
    generator::{Source as GeneratedSource, queries::Query as GeneratedQuery},
    parser::{
        HelixParser,
        types::{Content, ExpressionType, HxFile, ReturnType, Source as ParsedSource},
    },
};
use std::{fmt::Write, fs, path::PathBuf};

fn fixtures_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/helixc/fixtures/parity")
}

fn snapshots_root() -> PathBuf {
    fixtures_root().join("snapshots")
}

fn read_fixture(fixture_name: &str) -> String {
    let path = fixtures_root().join(fixture_name);
    fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("failed to read fixture {}: {err}", path.to_string_lossy()))
}

fn parse_fixture(fixture_name: &str) -> ParsedSource {
    let source = read_fixture(fixture_name);
    let content = Content {
        content: source.clone(),
        source: ParsedSource::default(),
        files: vec![HxFile {
            name: format!("src/helixc/fixtures/parity/{fixture_name}"),
            content: source,
        }],
    };

    HelixParser::parse_source(&content)
        .unwrap_or_else(|err| panic!("failed to parse parity fixture {fixture_name}: {err:?}"))
}

fn escape_snapshot_text(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('\t', "\\t")
}

fn assert_snapshot(snapshot_name: &str, actual: &str) {
    let snapshot_path = snapshots_root().join(snapshot_name);
    let should_update = std::env::var("UPDATE_HELIXC_SNAPSHOTS").ok().as_deref() == Some("1");

    if should_update {
        fs::create_dir_all(snapshots_root()).unwrap_or_else(|err| {
            panic!(
                "failed to create snapshot directory {}: {err}",
                snapshots_root().to_string_lossy()
            )
        });
        fs::write(&snapshot_path, actual).unwrap_or_else(|err| {
            panic!(
                "failed to write snapshot {}: {err}",
                snapshot_path.to_string_lossy()
            )
        });
    }

    let expected = fs::read_to_string(&snapshot_path).unwrap_or_else(|_| {
        panic!(
            "missing snapshot {}. run with UPDATE_HELIXC_SNAPSHOTS=1",
            snapshot_path.to_string_lossy()
        )
    });

    assert_eq!(
        expected,
        actual,
        "snapshot mismatch for {}. run with UPDATE_HELIXC_SNAPSHOTS=1",
        snapshot_path.to_string_lossy()
    );
}

fn diagnostics_snapshot(diagnostics: &[Diagnostic]) -> String {
    let mut out = String::new();

    for (idx, diagnostic) in diagnostics.iter().enumerate() {
        let filepath = diagnostic.filepath.as_deref().unwrap_or("<none>");
        let hint = diagnostic.hint.as_deref().unwrap_or("<none>");

        writeln!(
            out,
            "[{idx}] code={} severity={}",
            diagnostic.error_code,
            diagnostic.severity_str()
        )
        .unwrap();
        writeln!(out, "  file={filepath}").unwrap();
        writeln!(
            out,
            "  start={}:{}@{}",
            diagnostic.location.start.line,
            diagnostic.location.start.column,
            diagnostic.location.start.byte_offset
        )
        .unwrap();
        writeln!(
            out,
            "  end={}:{}@{}",
            diagnostic.location.end.line,
            diagnostic.location.end.column,
            diagnostic.location.end.byte_offset
        )
        .unwrap();
        writeln!(
            out,
            "  span={}",
            escape_snapshot_text(diagnostic.location.span.as_str())
        )
        .unwrap();
        writeln!(
            out,
            "  message={}",
            escape_snapshot_text(diagnostic.message.as_str())
        )
        .unwrap();
        writeln!(out, "  hint={}", escape_snapshot_text(hint)).unwrap();
    }

    out
}

fn canonicalize_expression(expr: &ExpressionType) -> String {
    match expr {
        ExpressionType::Identifier(value) => format!("id({value})"),
        ExpressionType::StringLiteral(value) => format!("str({value:?})"),
        ExpressionType::IntegerLiteral(value) => format!("i32({value})"),
        ExpressionType::FloatLiteral(value) => format!("f64({value})"),
        ExpressionType::BooleanLiteral(value) => format!("bool({value})"),
        ExpressionType::ArrayLiteral(values) => {
            let values = values
                .iter()
                .map(|value| canonicalize_expression(&value.expr))
                .collect::<Vec<_>>()
                .join(", ");
            format!("arr([{values}])")
        }
        ExpressionType::Empty => "expr(empty)".to_string(),
        other => format!("expr({other})"),
    }
}

fn canonicalize_return_value(value: &ReturnType) -> String {
    match value {
        ReturnType::Array(values) => {
            let values = values
                .iter()
                .map(canonicalize_return_value)
                .collect::<Vec<_>>()
                .join(", ");
            format!("[{values}]")
        }
        ReturnType::Object(map) => {
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort();

            let values = keys
                .into_iter()
                .map(|key| {
                    format!(
                        "{key}: {}",
                        canonicalize_return_value(
                            map.get(key)
                                .expect("sorted object key should exist while serializing")
                        )
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");

            format!("{{{values}}}")
        }
        ReturnType::Expression(expr) => canonicalize_expression(&expr.expr),
        ReturnType::Empty => "empty".to_string(),
    }
}

fn return_values_snapshot(parsed: &ParsedSource) -> String {
    let mut out = String::new();

    for query in &parsed.queries {
        writeln!(out, "QUERY {}", query.name).unwrap();
        for (index, return_value) in query.return_values.iter().enumerate() {
            writeln!(
                out,
                "  return[{index}] = {}",
                canonicalize_return_value(return_value)
            )
            .unwrap();
        }
    }

    out
}

fn generated_source_snapshot(generated: &GeneratedSource) -> String {
    let mut out = String::new();

    writeln!(out, "# Nodes").unwrap();
    for node in &generated.nodes {
        writeln!(out, "{node}").unwrap();
    }

    writeln!(out, "# Edges").unwrap();
    for edge in &generated.edges {
        writeln!(out, "{edge}").unwrap();
    }

    writeln!(out, "# Vectors").unwrap();
    for vector in &generated.vectors {
        writeln!(out, "{vector}").unwrap();
    }

    writeln!(out, "# Queries").unwrap();
    for query in &generated.queries {
        write!(out, "{}", generated_query_snapshot(query)).unwrap();
    }

    writeln!(out, "# Migrations").unwrap();
    for migration in &generated.migrations {
        writeln!(out, "{migration}").unwrap();
    }

    out
}

fn generated_query_snapshot(query: &GeneratedQuery) -> String {
    let mut out = String::new();

    writeln!(out, "QUERY {}", query.name).unwrap();
    writeln!(
        out,
        "  embedding_model={}",
        query.embedding_model_to_use.as_deref().unwrap_or("<none>")
    )
    .unwrap();
    writeln!(
        out,
        "  mcp_handler={}",
        query.mcp_handler.as_deref().unwrap_or("<none>")
    )
    .unwrap();
    writeln!(out, "  is_mut={}", query.is_mut).unwrap();
    writeln!(out, "  use_struct_returns={}", query.use_struct_returns).unwrap();

    for (index, parameter) in query.parameters.iter().enumerate() {
        writeln!(
            out,
            "  param[{index}] {}: {} optional={}",
            parameter.name, parameter.field_type, parameter.is_optional
        )
        .unwrap();
    }

    for (index, statement) in query.statements.iter().enumerate() {
        writeln!(out, "  stmt[{index}] {statement}").unwrap();
    }

    let mut return_values = query.return_values.iter().collect::<Vec<_>>();
    return_values.sort_by(|(left_name, _), (right_name, _)| left_name.cmp(right_name));

    for (index, (name, return_value)) in return_values.into_iter().enumerate() {
        writeln!(
            out,
            "  return_value[{index}] name={name} literal={}",
            return_value
                .literal_value
                .as_ref()
                .map(|value| value.to_string())
                .unwrap_or_else(|| "<none>".to_string())
        )
        .unwrap();

        let mut return_fields = return_value.fields.iter().collect::<Vec<_>>();
        return_fields.sort_by(|left, right| left.name.cmp(&right.name));
        for field in return_fields {
            writeln!(
                out,
                "    field {}: {} implicit={} nested={} nested_name={}",
                field.name,
                field.field_type,
                field.is_implicit,
                field.is_nested_traversal,
                field.nested_struct_name.as_deref().unwrap_or("<none>")
            )
            .unwrap();
        }
    }

    let mut return_structs = query.return_structs.iter().collect::<Vec<_>>();
    return_structs.sort_by(|left, right| left.name.cmp(&right.name));
    for return_struct in return_structs {
        writeln!(
            out,
            "  return_struct {} source={} collection={} aggregate={} group_by={} primitive={}",
            return_struct.name,
            return_struct.source_variable,
            return_struct.is_collection,
            return_struct.is_aggregate,
            return_struct.is_group_by,
            return_struct.is_primitive
        )
        .unwrap();

        let mut fields = return_struct.fields.iter().collect::<Vec<_>>();
        fields.sort_by(|left, right| left.name.cmp(&right.name));
        for field in fields {
            writeln!(out, "    struct_field {}: {}", field.name, field.field_type).unwrap();
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::{
        assert_snapshot, diagnostics_snapshot, generated_source_snapshot, parse_fixture,
        return_values_snapshot,
    };
    use crate::helixc::analyzer::analyze;

    #[test]
    fn diagnostics_parity_snapshot() {
        let parsed = parse_fixture("diagnostics_parity.hql");
        let (diagnostics, _) =
            analyze(&parsed).expect("analyzer should complete for diagnostics parity fixture");

        assert!(
            !diagnostics.is_empty(),
            "diagnostics parity fixture should emit diagnostics"
        );
        assert_snapshot(
            "diagnostics_parity.snap",
            diagnostics_snapshot(&diagnostics).as_str(),
        );
    }

    #[test]
    fn generated_output_parity_snapshot() {
        let parsed = parse_fixture("generated_output_parity.hql");
        let (diagnostics, generated) =
            analyze(&parsed).expect("analyzer should complete for output parity fixture");

        assert!(
            diagnostics.is_empty(),
            "generated output parity fixture should compile cleanly, got diagnostics: {}",
            diagnostics_snapshot(&diagnostics)
        );

        assert_snapshot(
            "generated_output_parity.snap",
            generated_source_snapshot(&generated).as_str(),
        );
    }

    #[test]
    fn return_value_parity_snapshot_uses_canonical_key_ordering() {
        let parsed = parse_fixture("return_value_parity.hql");

        assert_snapshot(
            "return_value_parity.snap",
            return_values_snapshot(&parsed).as_str(),
        );
    }
}
