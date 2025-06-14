use super::analyzer::{Diagnostic, analyze};
use crate::helixc::parser::helix_parser::{HelixParser, write_to_temp_file};

/// Convenience helper – parse text and return diagnostics.
fn run(src: &str) -> Vec<Diagnostic> {
    let input = write_to_temp_file(vec![src]);
    let parsed = HelixParser::parse_source(&input)
        .expect("parser should succeed – these tests are for the analyzer");
    analyze(&parsed).0
}

#[test]
fn reports_unknown_node_in_edge() {
    let hx = r#"
            E::Likes {
                From: User,
                To: Post,
                Properties: {}
            }
    "#;
    let diags = run(hx);
    assert!(
        diags
        .iter()
        .any(|d| d.message.contains("not a declared node type")),
        "expected a diagnostic about undeclared node types, got: {:?}",
        diags
    );
}

#[test]
fn detects_redeclared_variable() {
    let hx = r#"
            N::User { Name: String }

            QUERY dupVar() =>
                u <- N<User>
                u <- N<User>
                RETURN u
    "#;
    let diags = run(hx);
    assert!(
        diags.iter().any(|d| d.message.contains("already declared")),
        "expected a diagnostic about variable redeclaration, got: {:?}",
        diags
    );
}

#[test]
fn flags_invalid_property_access() {
    let hx = r#"
            N::User { name: String }

            QUERY badField() =>
                u <- N<User>
                n <- u::{age}
                RETURN n
        "#;
                let diags = run(hx);
                assert!(
                    diags
                    .iter()
                    .any(|d| d.message.contains("is not a field of node")),
                    "expected a diagnostic about invalid property access, got: {:?}",
                    diags
                );
}

#[test]
fn traversal_step_order_enforced() {
    let hx = r#"
            N::User { name: String }

            QUERY wrongStep() =>
                e <- N<User>::FromN   // OutN on nodes is illegal (needs Edge)
                RETURN e
        "#;
            let diags = run(hx);
            assert!(
                diags
                .iter()
                .any(|d| d.message.contains("cannot follow a step that returns")),
                "expected a diagnostic about illegal step ordering, got: {:?}",
                diags
            );
}

#[test]
fn clean_query_produces_no_diagnostics() {
    let hx = r#"
            N::User { name: String }
            N::Post { title: String }
            E::Wrote {
                From: User,
                To: Post,
                Properties: {}
            }

            QUERY ok(hey: User) =>
                u <- N<User>
                p <- u::Out<Wrote>
                RETURN p::!{title}::{title}
        "#;
        let diags = run(hx);
        for d in diags.iter() {
            println!("{}", d.render(hx, "query.hx"));
        }
        assert!(
            diags.is_empty(),
            "expected no diagnostics, got: {:?}",
            diags
        );
}

#[test]
fn validates_edge_properties() {
    let hx = r#"
            N::User { name: String }
            N::Post { title: String }
            E::Wrote {
                From: User,
                To: Post,
                Properties: {
                    date: String,
                    likes: I32
                }
            }

            QUERY badEdgeField() =>
                e <- N<User>::OutE<Wrote>
                n <- e::{invalid_field}
                RETURN n
        "#;
                let diags = run(hx);
                for d in diags.iter() {
                    println!("{}", d.render(hx, "query.hx"));
                }
                assert!(
                    diags
                    .iter()
                    .any(|d| d.message.contains("is not a field of edge")),
                    "expected a diagnostic about invalid edge property access, got: {:?}",
                    diags
                );
}

#[test]
fn validates_node_properties() {
    let hx = r#"
            N::User { name: String }
            N::Post { title: String }

            QUERY badNodeField() =>
                n <- N<User>::{invalid_field}
                RETURN n
        "#;
                let diags = run(hx);
                for d in diags.iter() {
                    println!("{}", d.render(hx, "query.hx"));
                }
                assert!(
                    diags
                    .iter()
                    .any(|d| d.message.contains("is not a field of edge")),
                    "expected a diagnostic about invalid edge property access, got: {:?}",
                    diags
                );
}

#[test]
fn validates_vector_properties() {
    let hx = r#"
            V::UserEmbedding {
                content: String
            }

            QUERY badVectorField(vec: [F64], content: String) =>
                v <- AddV<UserEmbedding>(vec,{content: content})
                RETURN v
        "#;
            let diags = run(hx);
            for d in diags.iter() {
                println!("{}", d.render(hx, "query.hx"));
            }
            assert!(
                diags
                .iter()
                .any(|d| d.message.contains("is not a field of vector")),
                "expected a diagnostic about invalid vector property access, got: {:?}",
                diags
            );
}

#[test]
fn handles_untyped_nodes() {
    let hx = r#"
            N::User { name: String }

            QUERY untypedNode() =>
                u <- N<User>::{some_field}
                RETURN n
        "#;
                let diags = run(hx);
                for d in diags.iter() {
                    println!("{}", d.render(hx, "query.hx"));
                }
                assert!(
                    diags.is_empty(),
                    "expected no diagnostics for untyped node access, got: {:?}",
                    diags
                );
}

#[test]
fn respects_excluded_fields() {
    let hx = r#"
            N::User { name: String, age: I32 }

            QUERY excludedField() =>
                u <- N<User>
                n <- u::!{name}::{name}
                RETURN n
        "#;
                let diags = run(hx);
                for d in diags.iter() {
                    println!("{}", d.render(hx, "query.hx"));
                }
                assert!(
                    diags
                    .iter()
                    .any(|d| d.message.contains("was previously excluded")),
                    "expected a diagnostic about accessing excluded field, got: {:?}",
                    diags
                );
}

#[test]
fn validates_add_node_fields() {
    let hx = r#"
            N::User { name: String }

            QUERY badAddNodeField() =>
                n <- AddN<User>({invalid_field: "test"})
                RETURN n
        "#;
            let diags = run(hx);
            assert!(
                diags
                .iter()
                .any(|d| d.message.contains("is not a field of node")),
                "expected a diagnostic about invalid node field, got: {:?}",
                diags
            );
}

#[test]
fn validates_add_edge_fields() {
    let hx = r#"
            N::User { name: String }
            N::Post { title: String }
            E::Wrote {
                From: User,
                To: Post,
                Properties: {
                    date: String
                }
            }

            QUERY badAddEdgeField() =>
                n1 <- AddN<User>({name: "test"})
                n2 <- AddN<Post>({title: "test"})
                e <- AddE<Wrote>({invalid_field: "test"})::To(n1)::From(n2)
                RETURN e
        "#;
            let diags = run(hx);
            assert!(
                diags
                .iter()
                .any(|d| d.message.contains("is not a field of edge")),
                "expected a diagnostic about invalid edge field, got: {:?}",
                diags
            );
}

#[test]
fn validates_add_vector_fields() {
    let hx = r#"
            V::UserEmbedding {
                content: String
            }

            QUERY badAddVectorField() =>
                v <- AddV<UserEmbedding>([1.0, 2.0], {invalid_field: "test"})
                RETURN v
        "#;
            let diags = run(hx);
            assert!(
                diags
                .iter()
                .any(|d| d.message.contains("is not a valid vector field")),
                "expected a diagnostic about invalid vector field, got: {:?}",
                diags
            );
}

#[test]
fn validate_boolean_comparison() {
    let hx = r#"
            N::User { name: String }

            QUERY booleanComparison() =>
                a <- N<User>::WHERE(_::{name}::EQ(10))
                RETURN a
        "#;
            let diags = run(hx);
            for d in diags.iter() {
                println!("{}", d.render(hx, "query.hx"));
            }
            assert!(
                diags.is_empty(),
                "expected no diagnostics, got: {:?}",
                diags
            );
}

