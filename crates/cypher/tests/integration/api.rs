use helix_cypher::api::{self, category, detail, ErrorClass};
use helix_cypher::{QueryError, Span};
use serde_json::json;

#[test]
fn paths_are_the_public_cypher_routes() {
    assert_eq!(api::HTTP_PATH, "/v2/cypher");
    assert_eq!(api::HTTP_EXPLAIN_PATH, "/v2/cypher/explain");
}

#[test]
fn every_category_maps_to_one_class_and_http_status() {
    let cases = [
        (category::RESOURCE_LIMIT, ErrorClass::ResourceLimit, 429),
        (category::ACCESS_MODE_ERROR, ErrorClass::WriterRequired, 503),
        (category::INTERNAL_PLANNER_ERROR, ErrorClass::Internal, 500),
        (category::SYNTAX_ERROR, ErrorClass::InvalidQuery, 400),
        (category::TYPE_ERROR, ErrorClass::InvalidQuery, 400),
        (category::UNSUPPORTED_FEATURE, ErrorClass::InvalidQuery, 400),
        (category::PARAMETER_MISSING, ErrorClass::InvalidQuery, 400),
        (category::ARGUMENT_ERROR, ErrorClass::InvalidQuery, 400),
        (category::ENTITY_NOT_FOUND, ErrorClass::InvalidQuery, 400),
        (category::ARITHMETIC_ERROR, ErrorClass::InvalidQuery, 400),
        (
            category::CONSTRAINT_VERIFICATION_FAILED,
            ErrorClass::InvalidQuery,
            400,
        ),
        // A category from a newer database node is still reported.
        ("category_added_later", ErrorClass::InvalidQuery, 400),
    ];
    assert!(
        category::ALL
            .iter()
            .all(|code| cases.iter().any(|(case, _, _)| case == code)),
        "every category has a reviewed class"
    );
    for (category, class, status) in cases {
        for error in [
            QueryError::compile(category, detail::INVALID_STATEMENT, "message"),
            QueryError::runtime(category, detail::INVALID_STATEMENT, "message"),
        ] {
            assert_eq!(ErrorClass::of(&error), class, "{category}");
            assert_eq!(ErrorClass::of(&error).http_status(), status, "{category}");
        }
    }
}

#[test]
fn every_code_is_unique_lower_snake_case() {
    for codes in [category::ALL, detail::ALL] {
        for (index, code) in codes.iter().enumerate() {
            assert!(
                code.split('_').all(|word| !word.is_empty()
                    && word.starts_with(|c: char| c.is_ascii_lowercase())
                    && word
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())),
                "{code} is not lower snake case"
            );
            assert!(!codes[index + 1..].contains(code), "{code} is listed twice");
        }
    }
}

#[test]
fn error_bodies_keep_the_documented_envelope() {
    let error = QueryError::runtime(
        category::RESOURCE_LIMIT,
        detail::MEMORY_LIMIT,
        "over budget",
    )
    .at(Span { start: 7, end: 12 });
    assert_eq!(
        serde_json::to_value(api::ErrorBody::from(&error)).unwrap(),
        json!({
            "error": "resource_limit",
            "msg": "over budget",
            "details": {"detail": "memory_limit", "phase": "runtime", "span": {"start": 7, "end": 12}},
        })
    );
    assert_eq!(error.code(), "resource_limit:runtime:memory_limit");

    let error = helix_cypher::compile("RETURN (").unwrap_err();
    let body = serde_json::to_value(api::ErrorBody::from(&error)).unwrap();
    assert_eq!(body["error"], category::SYNTAX_ERROR);
    assert_eq!(body["msg"], error.message);
    assert_eq!(body["details"]["detail"], error.detail);
    assert_eq!(body["details"]["phase"], "compile");
    assert_eq!(
        body["details"]["span"],
        serde_json::to_value(error.span).unwrap()
    );
}

#[test]
fn unsupported_functions_report_a_code_and_name_the_function_in_the_message() {
    for (query, function) in [
        ("RETURN sqrt(4) AS x", "sqrt"),
        ("RETURN DATE() AS d", "date"),
    ] {
        let error = helix_cypher::compile(query).unwrap_err();
        assert_eq!(error.category, category::UNSUPPORTED_FEATURE, "{query}");
        assert_eq!(error.detail, detail::FUNCTION, "{query}");
        assert!(
            error.message.contains(&format!("`{function}`")),
            "{}",
            error.message
        );
        assert_eq!(error.code(), "unsupported_feature:compile:function");
        let body = serde_json::to_value(api::ErrorBody::from(&error)).unwrap();
        assert_eq!(body["error"], "unsupported_feature");
        assert_eq!(body["details"]["detail"], "function");
    }
}
