use crate::helixc::{
    analyzer::{Ctx, error_codes::ErrorCode, errors::push_schema_warn},
    parser::types::Migration,
};

pub(crate) fn validate_migration(ctx: &mut Ctx, migration: &Migration) {
    push_schema_warn(
        ctx,
        migration.loc.clone(),
        ErrorCode::W102,
        "migration syntax is deprecated, please don't use them".to_string(),
        Some("remove MIGRATION blocks and perform data migrations externally".to_string()),
    );
}

#[cfg(test)]
mod tests {
    use crate::helixc::{
        analyzer::{analyze, diagnostic::DiagnosticSeverity, error_codes::ErrorCode},
        parser::{HelixParser, write_to_temp_file},
    };

    fn migration_source() -> &'static str {
        r#"
            schema::1 {
                N::User {
                    name: String
                }
            }

            schema::2 {
                N::User {
                    name: String,
                    age: U32
                }
            }

            MIGRATION schema::1 => schema::2 {
                N::User => N::User {
                    name: name,
                    age: 0
                }
            }

            QUERY healthcheck() =>
                RETURN 1
        "#
    }

    #[test]
    fn migration_syntax_emits_deprecation_warning() {
        let content = write_to_temp_file(vec![migration_source()]);
        let parsed = HelixParser::parse_source(&content).expect("migration should parse");
        let (diagnostics, _) = analyze(&parsed).expect("analysis should succeed");

        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::W102
            && matches!(d.severity, DiagnosticSeverity::Warning)));
        assert!(
            !diagnostics
                .iter()
                .any(|d| matches!(d.severity, DiagnosticSeverity::Error))
        );
    }

    #[test]
    fn migration_syntax_does_not_generate_migration_output() {
        let content = write_to_temp_file(vec![migration_source()]);
        let parsed = HelixParser::parse_source(&content).expect("migration should parse");
        let (_, generated) = analyze(&parsed).expect("analysis should succeed");

        assert!(generated.migrations.is_empty());
    }
}
