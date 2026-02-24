use crate::helixc::analyzer::error_codes::ErrorCode;
use crate::{
    generate_error,
    helixc::{
        analyzer::{
            Ctx,
            errors::{push_query_err, push_query_err_with_fix},
            fix::Fix,
            types::Type,
        },
        parser::{location::Loc, types::*},
    },
};
use paste::paste;
use std::collections::{HashMap, HashSet};

/// Iterates through the fields to exclude and validates that the exist on the type and have not been excluded previously.
///
/// # Arguments
///
/// * `ctx` - The context of the query
/// * `ex` - The exclude fields to validate
/// * `field_set` - The set of fields to validate
/// * `excluded` - The excluded fields
/// * `original_query` - The original query
/// * `type_name` - The name of the type
/// * `type_kind` - The kind of the type
/// * `span` - The span of the exclude fields
pub(crate) fn validate_exclude_fields<'a>(
    ctx: &mut Ctx<'a>,
    ex: &Exclude,
    field_set: &HashSet<&'a str>,
    excluded: &HashMap<&str, Loc>,
    original_query: &'a Query,
    type_name: &str,
    type_kind: &str,
    span: Option<Loc>,
) {
    for (loc, key) in &ex.fields {
        if let Some(loc) = excluded.get(key.as_str()) {
            push_query_err_with_fix(
                ctx,
                original_query,
                loc.clone(),
                ErrorCode::E643,
                key.clone(),
                "remove the exclusion of `{}`",
                Fix::new(span.clone(), Some(loc.clone()), None),
            );
        } else if !field_set.contains(key.as_str()) {
            generate_error!(
                ctx,
                original_query,
                loc.clone(),
                E202,
                key.as_str(),
                type_kind,
                type_name
            );
        }
    }
}

/// Validates the exclude fields for a given type
///
/// # Arguments
///
/// * `ctx` - The context of the query
/// * `cur_ty` - The current type of the traversal
/// * `tr` - The traversal to validate
/// * `ex` - The exclude fields to validate
/// * `excluded` - The excluded fields
/// * `original_query` - The original query
pub(crate) fn validate_exclude<'a>(
    ctx: &mut Ctx<'a>,
    cur_ty: &Type,
    tr: &Traversal,
    ex: &Exclude,
    excluded: &HashMap<&str, Loc>,
    original_query: &'a Query,
) {
    match &cur_ty {
        Type::Nodes(Some(node_ty)) | Type::Node(Some(node_ty)) => {
            let field_set = ctx
                .node_fields
                .get(node_ty.as_str())
                .map(|fields| fields.keys().copied().collect::<HashSet<_>>());
            if let Some(field_set) = field_set {
                validate_exclude_fields(
                    ctx,
                    ex,
                    &field_set,
                    excluded,
                    original_query,
                    node_ty,
                    "node",
                    Some(tr.loc.clone()),
                );
            }
        }
        Type::Edges(Some(edge_ty)) | Type::Edge(Some(edge_ty)) => {
            let field_set = ctx
                .edge_fields
                .get(edge_ty.as_str())
                .map(|fields| fields.keys().copied().collect::<HashSet<_>>());
            if let Some(field_set) = field_set {
                validate_exclude_fields(
                    ctx,
                    ex,
                    &field_set,
                    excluded,
                    original_query,
                    edge_ty,
                    "edge",
                    Some(tr.loc.clone()),
                );
            }
        }
        Type::Vectors(Some(vector_ty)) | Type::Vector(Some(vector_ty)) => {
            let field_set = ctx
                .vector_fields
                .get(vector_ty.as_str())
                .map(|fields| fields.keys().copied().collect::<HashSet<_>>());
            if let Some(field_set) = field_set {
                validate_exclude_fields(
                    ctx,
                    ex,
                    &field_set,
                    excluded,
                    original_query,
                    vector_ty,
                    "vector",
                    Some(tr.loc.clone()),
                );
            }
        }
        Type::Anonymous(ty) => {
            // validates the exclude on the inner type of the anonymous type
            validate_exclude(ctx, ty, tr, ex, excluded, original_query);
        }
        _ => {
            generate_error!(
                ctx,
                original_query,
                ex.fields[0].0.clone(),
                E203,
                cur_ty.kind_str()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::helixc::analyzer::error_codes::ErrorCode;
    use crate::helixc::parser::{HelixParser, write_to_temp_file};

    // ============================================================================
    // Field Exclusion Tests
    // ============================================================================

    #[test]
    fn test_exclude_valid_field() {
        let source = r#"
            N::Person { name: String, age: U32, email: String }

            QUERY test() =>
                people <- N<Person>::!{email}
                RETURN people
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_exclude_multiple_fields() {
        let source = r#"
            N::Person { name: String, age: U32, email: String, phone: String }

            QUERY test() =>
                people <- N<Person>::!{email, phone}
                RETURN people
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_exclude_nonexistent_field() {
        let source = r#"
            N::Person { name: String, age: U32 }

            QUERY test() =>
                people <- N<Person>::!{nonexistent}
                RETURN people
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E202));
    }

    #[test]
    fn test_exclude_on_edge() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person }

            QUERY test(id: ID) =>
                person <- N<Person>(id)
                edges <- person::OutE<Knows>::!{id}
                RETURN edges
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_exclude_after_filter() {
        let source = r#"
            N::Person { name: String, age: U32, email: String }

            QUERY test(minAge: U32, maxAge: U32) =>
                people <- N<Person>::WHERE(AND(_::{age}::GTE(minAge), _::{age}::LTE(maxAge)))::!{email}
                RETURN people
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_exclude_with_traversal() {
        let source = r#"
            N::Person { name: String, age: U32 }
            E::Knows { From: Person, To: Person }

            QUERY test(id: ID) =>
                friends <- N<Person>(id)::Out<Knows>::!{age}
                RETURN friends
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_exclude_single_field_keeps_others() {
        let source = r#"
            N::Person { name: String, age: U32, email: String }

            QUERY test() =>
                people <- N<Person>::!{age}
                RETURN people
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_exclude_implicit_field() {
        let source = r#"
            N::Person { name: String }

            QUERY test() =>
                people <- N<Person>::!{id}
                RETURN people
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_exclude_after_where() {
        let source = r#"
            N::Person { name: String, age: U32, email: String }

            QUERY test(minAge: U32) =>
                people <- N<Person>::WHERE(_::{age}::GT(minAge))::!{email}
                RETURN people
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_exclude_on_nodes_collection() {
        let source = r#"
            N::Person { name: String, age: U32, email: String }

            QUERY test() =>
                people <- N<Person>::!{email, age}
                RETURN people
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_exclude_with_multi_hop_traversal() {
        let source = r#"
            N::Person { name: String, age: U32 }
            N::Company { companyName: String }
            E::WorksAt { From: Person, To: Company }
            E::Knows { From: Person, To: Person }

            QUERY test(id: ID) =>
                colleagues <- N<Person>(id)::Out<WorksAt>::In<WorksAt>::!{age}
                RETURN colleagues
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_exclude_before_object_projection_is_valid() {
        let source = r#"
            N::Person { name: String, email: String }

            QUERY test() =>
                projected <- N<Person>::!{email}::{name}
                RETURN projected
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_exclude_before_traversal_step_emits_e644() {
        let source = r#"
            N::Person { name: String, email: String }
            E::Knows { From: Person, To: Person }

            QUERY test(id: ID) =>
                invalid <- N<Person>(id)::!{email}::Out<Knows>
                RETURN invalid
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E644));
    }
}
