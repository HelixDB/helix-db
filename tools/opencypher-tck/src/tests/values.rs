use super::*;

#[cfg(test)]
mod scenarios {
    use super::*;
    #[test]
    fn nested_values_graphs_and_paths() {
        for text in [
            "{x:[1, 'a\\'b', null, {}]}",
            "(:A:B {x:2})",
            "[:R {x:true}]",
            "<(:A)-[:R]->(:B)<-[:S]-()>",
        ] {
            parse(text).unwrap();
        }
        assert!(parse("[1,2").is_err());
        assert!(parse("null garbage").is_err());
    }
    #[test]
    fn duplicates_and_order_are_independent() {
        assert!(!rows_equal(&[1, 1], &[1, 2], true, |a, b| a == b));
        assert!(rows_equal(&[1, 2], &[2, 1], true, |a, b| a == b));
        assert!(!rows_equal(&[1, 2], &[2, 1], false, |a, b| a == b));
        assert!(!equal(
            &parse("[1,2]").unwrap(),
            &parse("[2,1]").unwrap(),
            false
        ));
        assert!(equal(
            &parse("[1,2]").unwrap(),
            &parse("[2,1]").unwrap(),
            true
        ));
    }
    #[test]
    fn integer_precision_is_preserved() {
        let value =
            wire(&serde_json::json!({"$type":"integer","value":"9223372036854775807"})).unwrap();
        assert_eq!(value, Value::Integer(i64::MAX));
        assert!(!equal(&value, &Value::Float(i64::MAX as f64), false));
    }
}

#[cfg(test)]
mod boundary_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn escapes_nesting_and_malformed_values_are_checked() {
        assert_eq!(
            parse(r#"'\n\r\t\b\f\u0041'"#).unwrap(),
            Value::String("\n\r\t\u{8}\u{c}A".into())
        );
        assert_eq!(
            parse("{`a b`: 1}").unwrap(),
            Value::Map(BTreeMap::from([("a b".into(), Value::Integer(1))]))
        );
        for text in [
            "'unfinished",
            "'unfinished\\",
            r#"'\u00'"#,
            r#"'\uZZZZ'"#,
            r#"'\uD800'"#,
            "{a:1,a:2}",
            "{:1}",
            "(:N",
            "[:R",
            "<(:N)-[:R](:N)>",
            "[1 2]",
            "1 garbage",
        ] {
            assert!(parse(text).is_err(), "{text}");
        }
        let nested = format!("{}0{}", "[".repeat(140), "]".repeat(140));
        assert!(parse(&nested).is_err());
        assert!(Parser {
            text: "",
            position: 0,
            depth: 0
        }
        .string()
        .is_err());
        for text in ["NaN", "Inf", "-Inf"] {
            assert!(matches!(parse(text).unwrap(), Value::Float(_)));
        }
    }
    #[test]
    fn lossless_wire_validation_rejects_ambiguous_and_disconnected_graphs() {
        for wire_value in [
            json!({"$type":"integer","value":"bad"}),
            json!({"$type":"integer"}),
            json!({"$type":"float","value":"bad"}),
            json!({"$type":"map","value":[]}),
            json!({"$type":"node","id":"1","labels":1}),
            json!({"$type":"node","id":"1","labels":[false]}),
            json!({"$type":"node","id":"1","labels":[],"properties":[]}),
            json!({"$type":"relationship","id":"1","start":"1","end":"2","type":null}),
            json!({"$type":"path","nodes":[]}),
            json!({"$type":"path","nodes":[],"relationships":[]}),
            json!({"$type":"unknown"}),
        ] {
            assert!(wire(&wire_value).is_err(), "{wire_value}");
        }
        for (wire_value, expected) in [
            (json!({"$type":"float","value":"NaN"}), f64::NAN),
            (json!({"$type":"float","value":"Infinity"}), f64::INFINITY),
            (
                json!({"$type":"float","value":"-Infinity"}),
                f64::NEG_INFINITY,
            ),
        ] {
            assert!(equal(
                &wire(&wire_value).unwrap(),
                &Value::Float(expected),
                false
            ));
        }
        assert_eq!(
            wire(&json!({"$type":"map","value":{"$type":"literal","a":[1,2]}})).unwrap(),
            parse("{'$type':'literal',a:[1,2]}").unwrap()
        );
        assert!(parse("(:N)").unwrap().parameter().is_err());
        assert!(parse("{a:[1,true,null]}").unwrap().parameter().is_ok());
        assert!(!equal(
            &Value::Integer(i64::MAX),
            &Value::Float(i64::MAX as f64),
            false
        ));
    }
}

#[test]
fn wire_paths_require_lossless_ids_and_connected_graph_elements() {
    use serde_json::json;
    let first = json!({"$type":"node","id":"18446744073709551615","labels":["A"],"properties":{}});
    let second = json!({"$type":"node","id":"2","labels":["B"],"properties":{}});
    let edge = json!({"$type":"relationship","id":"3","start":"18446744073709551615","end":"2","type":"R","properties":{}});
    let path = json!({"$type":"path","nodes":[first,second],"relationships":[edge]});
    assert!(equal(
        &wire(&path).unwrap(),
        &parse("<(:A)-[:R]->(:B)>").unwrap(),
        false
    ));
    let mut disconnected = path.clone();
    disconnected["relationships"][0]["end"] = json!("4");
    assert!(wire(&disconnected).is_err());
    let mut wrong_kind = path.clone();
    wrong_kind["nodes"][0]["$type"] = json!("map");
    assert!(wire(&wrong_kind).is_err());
    for bad in [
        json!(1),
        json!("01"),
        json!("-1"),
        json!("18446744073709551616"),
        json!(null),
    ] {
        let mut invalid = path.clone();
        invalid["nodes"][0]["id"] = bad;
        assert!(wire(&invalid).is_err());
    }
    for text in ["<1>", "<(:A)-[1]->(:B)>"] {
        assert!(parse(text).is_err());
    }
}
