use helix_cypher::api::{self, ErrorClass};
use helix_cypher::{QueryError, Span};
use serde_json::json;

#[test]
fn paths_are_the_public_cypher_routes() {
    assert_eq!(api::HTTP_PATH, "/v2/cypher");
    assert_eq!(api::HTTP_EXPLAIN_PATH, "/v2/cypher/explain");
}

#[test]
fn every_category_maps_to_one_class_and_http_status() {
    for (category, class, status) in [
        ("ResourceLimit", ErrorClass::ResourceLimit, 429),
        ("AccessModeError", ErrorClass::WriterRequired, 503),
        ("InternalPlannerError", ErrorClass::Internal, 500),
        ("SyntaxError", ErrorClass::InvalidQuery, 400),
        ("TypeError", ErrorClass::InvalidQuery, 400),
        ("UnsupportedFeature", ErrorClass::InvalidQuery, 400),
        ("ParameterMissing", ErrorClass::InvalidQuery, 400),
        ("ArgumentError", ErrorClass::InvalidQuery, 400),
        ("EntityNotFound", ErrorClass::InvalidQuery, 400),
        ("ArithmeticError", ErrorClass::InvalidQuery, 400),
        (
            "ConstraintVerificationFailed",
            ErrorClass::InvalidQuery,
            400,
        ),
        ("CategoryAddedLater", ErrorClass::InvalidQuery, 400),
    ] {
        for error in [
            QueryError::compile(category, "Detail", "message"),
            QueryError::runtime(category, "Detail", "message"),
        ] {
            assert_eq!(ErrorClass::of(&error), class, "{category}");
            assert_eq!(ErrorClass::of(&error).http_status(), status, "{category}");
        }
    }
}

#[test]
fn error_bodies_keep_the_documented_envelope() {
    let error = QueryError::runtime("ResourceLimit", "MemoryLimit", "over budget")
        .at(Span { start: 7, end: 12 });
    assert_eq!(
        serde_json::to_value(api::ErrorBody::from(&error)).unwrap(),
        json!({
            "error": "resource_limit",
            "msg": "over budget",
            "details": {"detail": "memory_limit", "phase": "runtime", "span": {"start": 7, "end": 12}},
        })
    );

    let error = helix_cypher::compile("RETURN (").unwrap_err();
    let body = serde_json::to_value(api::ErrorBody::from(&error)).unwrap();
    let code = api::ErrorCode::from(&error);
    assert_eq!(body["error"], code.category);
    assert_eq!(body["msg"], error.message);
    assert_eq!(body["details"]["detail"], code.detail);
    assert_eq!(body["details"]["phase"], "compile");
    assert_eq!(
        body["details"]["span"],
        serde_json::to_value(error.span).unwrap()
    );
}

#[test]
fn error_codes_use_the_lower_snake_case_of_native_codes() {
    for (category, code) in [
        ("ResourceLimit", "resource_limit"),
        ("AccessModeError", "access_mode_error"),
        ("InternalPlannerError", "internal_planner_error"),
        ("SyntaxError", "syntax_error"),
        ("TypeError", "type_error"),
        ("UnsupportedFeature", "unsupported_feature"),
        ("ParameterMissing", "parameter_missing"),
        ("ArgumentError", "argument_error"),
        ("EntityNotFound", "entity_not_found"),
        ("ArithmeticError", "arithmetic_error"),
        (
            "ConstraintVerificationFailed",
            "constraint_verification_failed",
        ),
        ("IDOverflow", "id_overflow"),
        ("Utf8Literal", "utf8_literal"),
        ("already_snake", "already_snake"),
    ] {
        let error = QueryError::compile(category, category, "message");
        let error_code = api::ErrorCode::from(&error);
        assert_eq!(error_code.category, code);
        assert_eq!(error_code.detail, code);
        assert!(
            code.split('_').all(|word| !word.is_empty()
                && word
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())),
            "{code}"
        );
    }
}

#[test]
fn single_string_codes_join_category_phase_and_detail() {
    let compile = QueryError::compile("SyntaxError", "UndefinedVariable", "message");
    assert_eq!(
        api::ErrorCode::from(&compile).to_string(),
        "syntax_error:compile:undefined_variable"
    );
    let runtime = QueryError::runtime(
        "ConstraintVerificationFailed",
        "DeleteConnectedNode",
        "message",
    );
    assert_eq!(
        api::ErrorCode::from(&runtime).to_string(),
        "constraint_verification_failed:runtime:delete_connected_node"
    );
}
