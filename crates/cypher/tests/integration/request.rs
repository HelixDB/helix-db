use crate::allocations;
use helix_ast::query::{QueryRequestType, QueryValue as Q};
use helix_cypher::request::Request;
use serde_json::json;
use std::collections::BTreeMap;

#[test]
fn routing_reuses_validated_effects_and_moves_parameter_ownership() {
    for (text, expected) in [
        ("RETURN $value", QueryRequestType::Read),
        ("CREATE (:N {key:$value})", QueryRequestType::Write),
    ] {
        let mut source = Request::new(text);
        let payload = "owned".repeat(16 * 1024);
        let address = payload.as_ptr();
        source.parameters.insert("value".into(), Q::String(payload));
        let (kind, original) = allocations::observe(|| source.request_type());
        assert_eq!(kind.unwrap(), expected);
        assert!(original.allocations > 0);
        let compiled = source.compile().unwrap();
        let (kind, reused) = allocations::observe(|| compiled.request_type());
        assert_eq!(kind, expected);
        assert_eq!(reused.allocations, 0);
        let (query, parameters) = compiled.into_parts();
        assert_eq!(
            query.effect(),
            helix_cypher::compile(text).unwrap().effect()
        );
        let Q::String(payload) = &parameters["value"] else {
            panic!("original string parameter")
        };
        assert_eq!(payload.as_ptr(), address);
    }
    for text in ["RETURN missing", "RETURN (", "MERGE (:N)"] {
        let original = Request::new(text).request_type().unwrap_err();
        let compiled = Request::new(text).compile().err().unwrap();
        assert_eq!(
            serde_json::to_value(original).unwrap(),
            serde_json::to_value(compiled).unwrap()
        );
    }
}

#[test]
fn lossless_parameters_decode_every_envelope_and_plain_json_shape() {
    let decode = |value: serde_json::Value| {
        serde_json::from_value::<Request>(json!({"query":"RETURN $p","parameters":{"p":value}}))
            .map(|mut request| request.parameters.remove("p").expect("decoded parameter"))
    };
    let map = |entries: &[(&str, Q)]| {
        Q::Object(
            entries
                .iter()
                .map(|(name, value)| (name.to_string(), value.clone()))
                .collect(),
        )
    };
    for (value, expected) in [
        (json!(null), Q::Null),
        (json!(true), Q::Bool(true)),
        (json!(-7), Q::I64(-7)),
        (json!(1.5), Q::F64(1.5)),
        (json!("text"), Q::String("text".into())),
        (
            json!({"$type":"integer","value":"9223372036854775807"}),
            Q::I64(i64::MAX),
        ),
        (
            json!({"$type":"float","value":"Infinity"}),
            Q::F64(f64::INFINITY),
        ),
        (
            json!({"$type":"float","value":"-Infinity"}),
            Q::F64(f64::NEG_INFINITY),
        ),
        (
            json!({"$type":"map","value":{"$type":"user","value":[1,true]}}),
            map(&[
                ("$type", Q::String("user".into())),
                ("value", Q::Array(vec![Q::I64(1), Q::Bool(true)])),
            ]),
        ),
        (
            json!({"$type":1,"nested":[{"$type":"integer","value":"2"},[null]]}),
            map(&[
                ("$type", Q::I64(1)),
                ("nested", Q::Array(vec![Q::I64(2), Q::Array(vec![Q::Null])])),
            ]),
        ),
    ] {
        assert_eq!(decode(value.clone()).unwrap(), expected, "{value}");
    }
    let Q::F64(nan) = decode(json!({"$type":"float","value":"NaN"})).unwrap() else {
        panic!("NaN float envelope")
    };
    assert!(nan.is_nan());

    for (value, message) in [
        (
            json!({"$type":"integer","value":7}),
            "integer envelope requires a decimal string",
        ),
        (
            json!({"$type":"integer","value":"9223372036854775808"}),
            "integer envelope exceeds signed 64-bit range",
        ),
        (
            json!({"$type":"float","value":"1.5"}),
            "invalid float envelope",
        ),
        (
            json!({"$type":"map","value":[1]}),
            "map envelope requires an object",
        ),
        (
            json!({"$type":"map","value":{"big":18446744073709551615u64}}),
            "integer parameter exceeds signed 64-bit range",
        ),
        (
            json!({"$type":"date","value":"2024-01-01"}),
            "unknown lossless parameter envelope",
        ),
        (
            json!({"$type":"integer","value":"1","extra":true}),
            "unknown lossless parameter envelope",
        ),
        (
            json!({"plain":{"$type":"float","value":"1"}}),
            "invalid float envelope",
        ),
        (
            json!([1, 18446744073709551615u64]),
            "integer parameter exceeds signed 64-bit range",
        ),
        (
            json!(18446744073709551615u64),
            "integer parameter exceeds signed 64-bit range",
        ),
    ] {
        let error = decode(value.clone()).unwrap_err().to_string();
        assert!(error.contains(message), "{value}: {error}");
    }
}

#[test]
fn request_bodies_reject_unknown_fields_and_default_optional_ones() {
    let request: Request = serde_json::from_value(json!({"query":"RETURN 1"})).unwrap();
    assert_eq!(request.query, "RETURN 1");
    assert_eq!(request.parameters, BTreeMap::new());
    assert_eq!(request.query_name, None);
    assert_eq!(
        serde_json::to_value(&request).unwrap(),
        json!({"query":"RETURN 1","parameters":{}})
    );

    let named: Request =
        serde_json::from_value(json!({"query":"RETURN 1","query_name":"one"})).unwrap();
    assert_eq!(named.query_name.as_deref(), Some("one"));
    assert_eq!(
        serde_json::to_value(&named).unwrap(),
        json!({"query":"RETURN 1","parameters":{},"query_name":"one"})
    );

    for (body, message) in [
        (
            json!({"query":"RETURN 1","limit":1}),
            "unknown field `limit`",
        ),
        (json!({"parameters":{}}), "missing field `query`"),
        (json!({"query":"RETURN 1","parameters":[]}), "invalid type"),
    ] {
        let error = serde_json::from_value::<Request>(body.clone())
            .unwrap_err()
            .to_string();
        assert!(error.contains(message), "{body}: {error}");
    }
}
