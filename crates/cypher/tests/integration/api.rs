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
            "error": "ResourceLimit",
            "msg": "over budget",
            "details": {"detail": "MemoryLimit", "phase": "runtime", "span": {"start": 7, "end": 12}},
        })
    );

    let error = helix_cypher::compile("RETURN (").unwrap_err();
    let body = serde_json::to_value(api::ErrorBody::from(&error)).unwrap();
    assert_eq!(body["error"], error.category);
    assert_eq!(body["msg"], error.message);
    assert_eq!(body["details"]["detail"], error.detail);
    assert_eq!(body["details"]["phase"], "compile");
    assert_eq!(
        body["details"]["span"],
        serde_json::to_value(error.span).unwrap()
    );
}
