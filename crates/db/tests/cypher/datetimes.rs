//! Datetime properties end to end through Cypher and SQL: typed parameters,
//! stored values, comparisons, ordering, aggregation, functions, index access,
//! the native response encoding and the bad paths.
use super::database;
use super::membership::create_index;
use db::{cypher, HelixDB};
use helix_ast::query::QueryValue as Q;
use helix_ast::{batch, index, query, traversal};
use helix_planner::relational as r;
use serde_json::{json, Value};

/// 2023-11-14T22:13:20.000Z.
const BASE: i64 = 1_700_000_000_000;
const HOUR: i64 = 3_600_000;

fn text(millis: i64) -> Value {
    json!(r::format_datetime(millis).unwrap())
}

fn request<const N: usize>(query: &str, parameters: [(&str, Q); N]) -> cypher::Request {
    let mut request = cypher::Request::new(query);
    request.parameters = parameters
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect();
    request
}

async fn rows(db: &HelixDB, request: cypher::Request) -> Vec<Vec<Value>> {
    let query = request.query.clone();
    db.cypher(request)
        .await
        .unwrap_or_else(|error| panic!("{query}: {error:?}"))
        .rows
}

async fn read(db: &HelixDB, query: &str) -> Vec<Vec<Value>> {
    rows(db, cypher::Request::new(query)).await
}

async fn query_error(db: &HelixDB, request: cypher::Request) -> r::QueryError {
    let query = request.query.clone();
    match db.cypher(request).await {
        Err(cypher::Error::Query(error)) => error,
        other => panic!("{query}: expected a query error, got {other:?}"),
    }
}

/// Ten events an hour apart, written through typed `date_time` parameters,
/// plus one whose `at` is a string that only looks like a datetime.
async fn events(db: &HelixDB) {
    for seq in 0..10_i64 {
        db.cypher(request(
            "CREATE (:Event {seq: $seq, at: $at, tag: $tag})",
            [
                ("seq", Q::I64(seq)),
                ("at", Q::DateTime(BASE + seq * HOUR)),
                (
                    "tag",
                    Q::String(if seq % 2 == 0 { "even" } else { "odd" }.into()),
                ),
            ],
        ))
        .await
        .unwrap();
    }
    read(
        db,
        "CREATE (:Event {seq: 99, at: '2023-11-15T03:30:00.000Z', tag: 'text'})",
    )
    .await;
}

/// The node access a statement's first MATCH starts from.
async fn access(db: &HelixDB, request: cypher::Request) -> String {
    let explanation = db.explain_cypher(request).await.unwrap();
    let [step, ..] = explanation.plan().matches()[&0].sources[0].access.steps() else {
        panic!("an access step");
    };
    format!("{:?}", step.op)
        .trim_start_matches("Access { plan: Node(")
        .split(|c: char| !c.is_alphanumeric())
        .next()
        .unwrap()
        .to_owned()
}

fn seqs(values: impl IntoIterator<Item = i64>) -> Vec<Vec<Value>> {
    values.into_iter().map(|seq| vec![json!(seq)]).collect()
}

/// A datetime written through a typed parameter is stored as a datetime, and
/// Cypher, Cypher JSON and native responses all encode it as the same text.
#[tokio::test]
async fn typed_parameters_store_datetimes_every_response_encodes_alike() {
    let db = database().await;
    events(&db).await;
    let at = BASE + 3 * HOUR;
    let returned = read(
        &db,
        "MATCH (e:Event {seq: 3}) RETURN e.at, e, [e.at], {at: e.at}",
    )
    .await;
    assert_eq!(returned[0][0], text(at));
    assert_eq!(returned[0][0], json!("2023-11-15T01:13:20.000Z"));
    assert_eq!(returned[0][1]["properties"]["at"], text(at));
    assert_eq!(returned[0][2], json!([text(at)]));
    assert_eq!(returned[0][3], json!({"at": text(at)}));
    // Only a stored datetime equals a datetime; the look-alike string does not.
    assert_eq!(
        read(
            &db,
            "MATCH (e:Event) WHERE e.at = datetime('2023-11-15T01:13:20Z') RETURN e.seq"
        )
        .await,
        seqs([3])
    );
    let native = db
        .query(query::QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "rows",
                    traversal::g()
                        .n_with_label("Event")
                        .value_map(None::<Vec<String>>),
                )
                .returning(["rows"]),
        ))
        .await
        .unwrap();
    let native = native["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["seq"] == json!(3))
        .unwrap()["at"]
        .clone();
    assert_eq!(native, returned[0][0]);
    let encoded = db
        .cypher_json(cypher::Request::new("MATCH (e:Event {seq: 3}) RETURN e.at"))
        .await
        .unwrap();
    let body = std::str::from_utf8(encoded.body()).unwrap();
    assert!(
        body.contains(&format!("[[{}]]", serde_json::to_string(&native).unwrap())),
        "{body}"
    );
    db.close().await.unwrap();
}

/// Datetimes filter, order, group and aggregate chronologically; against any
/// other type, equality is false and ordering is null, never an error.
#[tokio::test]
async fn datetimes_filter_order_and_aggregate_chronologically() {
    let db = database().await;
    events(&db).await;
    assert_eq!(
        rows(
            &db,
            request(
                "MATCH (e:Event) WHERE e.at >= $from AND e.at < $to RETURN e.seq ORDER BY e.seq",
                [
                    ("from", Q::DateTime(BASE + 2 * HOUR)),
                    ("to", Q::DateTime(BASE + 5 * HOUR)),
                ],
            ),
        )
        .await,
        seqs([2, 3, 4])
    );
    for (query, expected) in [
        (
            "MATCH (e:Event) WHERE e.at > datetime('2023-11-15T03:00:00Z') RETURN e.seq ORDER BY e.seq",
            seqs(5..10),
        ),
        (
            "MATCH (e:Event) WHERE e.at <= datetime('2023-11-14 23:13:20') RETURN e.seq ORDER BY e.seq",
            seqs([0, 1]),
        ),
        (
            "MATCH (e:Event) WHERE e.tag <> 'text' RETURN e.seq ORDER BY e.at DESC LIMIT 3",
            seqs([9, 8, 7]),
        ),
        // The string sorts after every datetime, as strings rank above them.
        (
            "MATCH (e:Event) RETURN e.seq ORDER BY e.at DESC LIMIT 2",
            seqs([99, 9]),
        ),
        (
            "MATCH (e:Event) RETURN e.seq ORDER BY e.at SKIP 8",
            seqs([8, 9, 99]),
        ),
        (
            "MATCH (e:Event) WHERE e.tag <> 'text' \
             RETURN min(e.at), max(e.at), count(DISTINCT e.at), collect(e.at)[0] = min(e.at)",
            vec![vec![text(BASE), text(BASE + 9 * HOUR), json!(10), json!(true)]],
        ),
        (
            "MATCH (e:Event) WHERE e.tag <> 'text' RETURN e.tag, max(e.at), min(e.at) ORDER BY e.tag",
            vec![
                vec![json!("even"), text(BASE + 8 * HOUR), text(BASE)],
                vec![json!("odd"), text(BASE + 9 * HOUR), text(BASE + HOUR)],
            ],
        ),
        (
            "UNWIND [datetime({epochMillis: 1}), datetime({epochMillis: 1}), 1, '1970-01-01T00:00:00.001Z'] AS x \
             RETURN DISTINCT x ORDER BY x",
            // The datetime and the string render alike but stay distinct.
            vec![vec![text(1)], vec![json!("1970-01-01T00:00:00.001Z")], vec![json!(1)]],
        ),
        (
            "UNWIND [datetime({epochMillis: 2}), datetime({epochMillis: 1}), 2, 1] AS x \
             RETURN x, count(*) ORDER BY x",
            vec![
                vec![text(1), json!(1)],
                vec![text(2), json!(1)],
                vec![json!(1), json!(1)],
                vec![json!(2), json!(1)],
            ],
        ),
        // Mixed domains, as a string against an integer.
        (
            "MATCH (e:Event) WHERE e.tag <> 'text' AND (e.at = toString(e.at) OR e.at < 'z' OR e.at >= 0) \
             RETURN count(*)",
            vec![vec![json!(0)]],
        ),
        (
            "MATCH (e:Event) WHERE e.at <> 1 AND e.at <> toString(e.at) RETURN count(*)",
            vec![vec![json!(10)]],
        ),
        (
            "RETURN datetime() > '2000-01-01', datetime() = 1, datetime() IN ['x', datetime()]",
            vec![vec![Value::Null, json!(false), json!(true)]],
        ),
        (
            "MATCH (e:Event {seq: 0}) \
             RETURN toString(e.at), datetime(toString(e.at)) = e.at, datetime(e.at) = e.at",
            vec![vec![text(BASE), json!(true), json!(true)]],
        ),
    ] {
        assert_eq!(read(&db, query).await, expected, "{query}");
    }
    db.close().await.unwrap();
}

/// CREATE and SET store datetimes, lists of them, and the statement clock,
/// which every call in one statement reads alike.
#[tokio::test]
async fn writes_store_datetimes_and_one_statement_reads_one_clock() {
    let db = database().await;
    assert_eq!(
        read(
            &db,
            "CREATE (n:W {a: datetime('2024-02-29T12:00:00+01:00'), \
             list: [datetime({epochMillis: 0}), datetime({epochMillis: 1})]}) RETURN n.a, n.list"
        )
        .await,
        vec![vec![
            json!("2024-02-29T11:00:00.000Z"),
            json!(["1970-01-01T00:00:00.000Z", "1970-01-01T00:00:00.001Z"])
        ]]
    );
    rows(
        &db,
        request(
            "MATCH (n:W) SET n.b = $b, n += {c: datetime({epochMillis: 5})}",
            [("b", Q::DateTime(-1))],
        ),
    )
    .await;
    assert_eq!(
        read(
            &db,
            "MATCH (n:W) RETURN n.b, n.c, n.list[1] > n.list[0], n.a > n.b"
        )
        .await,
        vec![vec![
            json!("1969-12-31T23:59:59.999Z"),
            text(5),
            json!(true),
            json!(true)
        ]]
    );
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let clock = read(
        &db,
        "UNWIND range(1, 50) AS i CREATE (c:Clock {t: datetime(), ms: timestamp()}) \
         RETURN count(DISTINCT c.t), count(DISTINCT c.ms), min(c.ms)",
    )
    .await;
    let after = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    assert_eq!(clock[0][..2], [json!(1), json!(1)]);
    let millis = clock[0][2].as_i64().unwrap();
    assert!(
        (before..=after).contains(&millis),
        "{before} {millis} {after}"
    );
    assert_eq!(
        read(
            &db,
            "MATCH (c:Clock) WITH c LIMIT 1 RETURN c.t = datetime({epochMillis: c.ms})"
        )
        .await,
        vec![vec![json!(true)]]
    );
    // A later statement reads a later clock.
    assert_eq!(
        read(
            &db,
            "MATCH (c:Clock) WITH DISTINCT c.t AS t RETURN t <= datetime(), count(*)"
        )
        .await,
        vec![vec![json!(true), json!(1)]]
    );
    db.close().await.unwrap();
}

/// Range and equality indexes answer datetime predicates from literals,
/// typed parameters and `datetime($text)`, exactly as a scan does; datetime
/// bounds never return the look-alike string.
#[tokio::test]
async fn datetime_predicates_read_range_and_equality_indexes() {
    let range = database().await;
    let equality = database().await;
    let scanned = database().await;
    for db in [&range, &equality, &scanned] {
        events(db).await;
    }
    create_index(&range, index::IndexSpec::node_range("Event", "at")).await;
    create_index(&equality, index::IndexSpec::node_equality("Event", "at")).await;
    let parameters = || {
        [
            ("from", Q::DateTime(BASE + 4 * HOUR)),
            ("at", Q::DateTime(BASE + 7 * HOUR)),
            ("other", Q::DateTime(BASE + 2 * HOUR)),
            ("text", Q::String("2023-11-15T03:00:00Z".into())),
        ]
    };
    for (db, query, expected, ranks) in [
        (
            &range,
            "MATCH (e:Event) WHERE e.at >= $from RETURN e.seq ORDER BY e.seq",
            "RangeIndex",
            seqs(4..10),
        ),
        (
            &range,
            "MATCH (e:Event) WHERE e.at > datetime('2023-11-15T04:00:00Z') AND e.at < $at \
             RETURN e.seq ORDER BY e.seq",
            "RangeIndex",
            seqs([6]),
        ),
        (
            &range,
            "MATCH (e:Event) WHERE e.at < datetime($text) RETURN e.seq ORDER BY e.seq",
            "RangeIndex",
            seqs(0..5),
        ),
        (
            &range,
            "MATCH (e:Event {at: $at}) RETURN e.seq",
            "RangeIndex",
            seqs([7]),
        ),
        (
            &equality,
            "MATCH (e:Event {at: $at}) RETURN e.seq",
            "Bitmap",
            seqs([7]),
        ),
        (
            &equality,
            "MATCH (e:Event) WHERE e.at = datetime('2023-11-15T05:13:20.000Z') RETURN e.seq",
            "Bitmap",
            seqs([7]),
        ),
        (
            &equality,
            "MATCH (e:Event) WHERE e.at IN [$at, $other] RETURN e.seq ORDER BY e.seq",
            "SecondarySet",
            seqs([2, 7]),
        ),
        // Text finds only the look-alike string, never the datetimes.
        (
            &equality,
            "MATCH (e:Event) WHERE e.at = '2023-11-15T03:30:00.000Z' RETURN e.seq",
            "Bitmap",
            seqs([99]),
        ),
    ] {
        assert_eq!(
            access(db, request(query, parameters())).await,
            expected,
            "{query}"
        );
        let direct = rows(db, request(query, parameters())).await;
        assert_eq!(direct, ranks, "{query}");
        assert_eq!(
            rows(&scanned, request(query, parameters())).await,
            direct,
            "{query}"
        );
    }
    // The ordered range index reads only the window.
    let response = range
        .cypher(request(
            "MATCH (e:Event) WHERE e.at >= $from RETURN e.seq ORDER BY e.at LIMIT 2",
            parameters(),
        ))
        .await
        .unwrap();
    assert_eq!(response.rows, seqs([4, 5]));
    assert!(
        response.resources.reads.scan_rows <= 8,
        "{:?}",
        response.resources.reads
    );
    // Bound text that does not read as a datetime keeps the scan, which fails.
    let mut unreadable = request(
        "MATCH (e:Event) WHERE e.at < datetime($text) RETURN e.seq",
        parameters(),
    );
    unreadable
        .parameters
        .insert("text".into(), Q::String("soon".into()));
    let error = query_error(&range, unreadable).await;
    assert_eq!(error.detail, r::detail::INVALID_DATETIME);
    for db in [range, equality, scanned] {
        db.close().await.unwrap();
    }
}

/// A datetime list is stored in one typed form however it is written —
/// Cypher, a typed Cypher or SQL parameter, or a native typed parameter — so
/// an equality index holds it, equality finds it through the index exactly as
/// a scan does, and every response reads it back as a list of datetimes.
#[tokio::test]
async fn datetime_lists_are_held_and_found_by_equality_indexes() {
    use cypher::sql::request::Request;
    use helix_ast::query::QueryParamType as T;
    let indexed = database().await;
    let scanned = database().await;
    create_index(&indexed, index::IndexSpec::node_equality("Slot", "at")).await;
    let list = |millis: &[i64]| Q::Array(millis.iter().copied().map(Q::DateTime).collect());
    let texts = |millis: &[i64]| json!(millis.iter().map(|&at| text(at)).collect::<Vec<_>>());
    for db in [&indexed, &scanned] {
        read(
            db,
            "CREATE (:Slot {seq: 1, at: [datetime({epochMillis: 0}), \
             datetime('1970-01-01T00:00:00.001Z')]})",
        )
        .await;
        rows(
            db,
            request(
                "CREATE (:Slot {seq: 2, at: $at})",
                [("at", list(&[BASE, BASE + HOUR]))],
            ),
        )
        .await;
        let mut insert = Request::new("INSERT INTO Slot (seq, at) VALUES (3, ?)");
        insert
            .parameters
            .insert("1".into(), list(&[BASE + 2 * HOUR]));
        db.sql(insert).await.unwrap();
        db.query(
            query::QueryRequest::write(batch::write_batch().var_as(
                "slot",
                traversal::g().add_n(
                    "Slot",
                    vec![
                        ("seq", helix_ast::value::PropertyInput::from(4_i64)),
                        ("at", helix_ast::value::PropertyInput::param("at")),
                    ],
                ),
            ))
            .with_typed_parameter(
                "at",
                T::Array(Box::new(T::DateTime)),
                Q::Array(vec![
                    Q::String("1970-01-01T00:00:00Z".into()),
                    Q::String("1970-01-01T01:00:00+01:00".into()),
                ]),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    }
    for (query, expected, access_op) in [
        (
            "MATCH (s:Slot) WHERE s.at = [datetime('1970-01-01'), \
             datetime('1970-01-01T00:00:00.001Z')] RETURN s.seq",
            seqs([1]),
            "Bitmap",
        ),
        // A member the planner does not read before running keeps the scan.
        (
            "MATCH (s:Slot) WHERE s.at = [datetime({epochMillis: 0}), datetime({epochMillis: 1})] \
             RETURN s.seq",
            seqs([1]),
            "LabelScan",
        ),
        (
            "MATCH (s:Slot {at: [datetime('2023-11-14T22:13:20Z'), \
             datetime('2023-11-14T23:13:20Z')]}) RETURN s.seq",
            seqs([2]),
            "Bitmap",
        ),
        (
            "MATCH (s:Slot) WHERE s.at = [datetime('1970-01-01'), datetime('1970-01-01')] \
             RETURN s.seq",
            seqs([4]),
            "Bitmap",
        ),
        // Order, length and member type all count.
        (
            "MATCH (s:Slot) WHERE s.at = [datetime('1970-01-01T00:00:00.001Z'), \
             datetime('1970-01-01')] RETURN s.seq",
            seqs([]),
            "Bitmap",
        ),
        (
            "MATCH (s:Slot) WHERE s.at = [datetime('1970-01-01')] RETURN s.seq",
            seqs([]),
            "Bitmap",
        ),
        (
            "MATCH (s:Slot) WHERE s.at = [0, 1] RETURN s.seq",
            seqs([]),
            "LabelScan",
        ),
        (
            "MATCH (s:Slot) WHERE s.at IN [[datetime('1970-01-01'), \
             datetime('1970-01-01T00:00:00.001Z')], [datetime('2023-11-15T00:13:20Z')]] \
             RETURN s.seq ORDER BY s.seq",
            seqs([1, 3]),
            "SecondarySet",
        ),
    ] {
        assert_eq!(
            access(&indexed, cypher::Request::new(query)).await,
            access_op,
            "{query}"
        );
        assert_eq!(read(&indexed, query).await, expected, "{query}");
        assert_eq!(read(&scanned, query).await, expected, "{query}");
    }
    for db in [&indexed, &scanned] {
        assert_eq!(
            rows(
                db,
                request(
                    "MATCH (s:Slot) WHERE s.at = $at RETURN s.seq",
                    [("at", list(&[BASE, BASE + HOUR]))],
                ),
            )
            .await,
            seqs([2])
        );
        let returned = read(
            db,
            "MATCH (s:Slot {seq: 1}) RETURN s.at, s.at[1] > s.at[0], s.at[0].epochMillis, \
             s.at = [datetime({epochMillis: 0}), datetime({epochMillis: 1})], s",
        )
        .await;
        assert_eq!(
            returned[0][..4],
            [texts(&[0, 1]), json!(true), json!(0), json!(true)]
        );
        assert_eq!(
            returned[0][4]["properties"],
            json!({"seq": 1, "at": texts(&[0, 1])})
        );
        let sql = db
            .sql(Request::new("SELECT s.at FROM Slot s WHERE s.seq = 3"))
            .await
            .unwrap();
        assert_eq!(sql.rows, vec![vec![texts(&[BASE + 2 * HOUR])]]);
        let native = db
            .query(query::QueryRequest::read(
                batch::read_batch()
                    .var_as(
                        "rows",
                        traversal::g()
                            .n_with_label("Slot")
                            .value_map(None::<Vec<String>>),
                    )
                    .returning(["rows"]),
            ))
            .await
            .unwrap();
        let mut native = native["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| (row["seq"].as_i64().unwrap(), row["at"].clone()))
            .collect::<Vec<_>>();
        native.sort_by_key(|(seq, _)| *seq);
        assert_eq!(
            native,
            vec![
                (1, texts(&[0, 1])),
                (2, texts(&[BASE, BASE + HOUR])),
                (3, texts(&[BASE + 2 * HOUR])),
                (4, texts(&[0, 0])),
            ]
        );
    }
    for db in [indexed, scanned] {
        db.close().await.unwrap();
    }
}

/// Unreadable text, unsupported forms, out-of-range instants and arithmetic
/// fail with typed errors; comparing an out-of-range instant still works.
#[tokio::test]
async fn datetime_bad_paths_fail_with_typed_errors() {
    let db = database().await;
    let phase = |error: &r::QueryError| (error.category.clone(), error.detail.clone(), error.phase);
    let compile = |category: &str, detail: &str| {
        (
            category.to_owned(),
            detail.to_owned(),
            r::ErrorPhase::Compile,
        )
    };
    let runtime = |category: &str, detail: &str| {
        (
            category.to_owned(),
            detail.to_owned(),
            r::ErrorPhase::Runtime,
        )
    };
    let invalid = runtime(r::category::ARGUMENT_ERROR, r::detail::INVALID_DATETIME);
    let out_of_range = runtime(
        r::category::ARGUMENT_ERROR,
        r::detail::DATETIME_OUT_OF_RANGE,
    );
    for (query, expected) in [
        (
            "RETURN datetime('not a date')",
            compile(r::category::UNSUPPORTED_FEATURE, r::detail::FUNCTION),
        ),
        (
            "RETURN datetime('2024-1-5')",
            compile(r::category::UNSUPPORTED_FEATURE, r::detail::FUNCTION),
        ),
        (
            "RETURN datetime('2024-02-30')",
            compile(r::category::ARGUMENT_ERROR, r::detail::INVALID_DATETIME),
        ),
        (
            "RETURN datetime().Year",
            compile(r::category::TYPE_ERROR, r::detail::INVALID_ARGUMENT_TYPE),
        ),
        (
            "WITH [datetime()] AS xs RETURN xs[0].Year",
            runtime(r::category::TYPE_ERROR, r::detail::INVALID_ARGUMENT_TYPE),
        ),
        ("RETURN $far.year", out_of_range.clone()),
        (
            "RETURN datetime({year: 2024})",
            compile(r::category::UNSUPPORTED_FEATURE, r::detail::FUNCTION),
        ),
        (
            "RETURN datetime({epochMillis: 1, timezone: 'Z'})",
            compile(r::category::UNSUPPORTED_FEATURE, r::detail::FUNCTION),
        ),
        (
            "RETURN date('2024-01-01')",
            compile(r::category::UNSUPPORTED_FEATURE, r::detail::FUNCTION),
        ),
        (
            "RETURN localdatetime()",
            compile(r::category::UNSUPPORTED_FEATURE, r::detail::FUNCTION),
        ),
        (
            "RETURN duration('P1D')",
            compile(r::category::UNSUPPORTED_FEATURE, r::detail::FUNCTION),
        ),
        (
            "RETURN timestamp(1)",
            compile(
                r::category::SYNTAX_ERROR,
                r::detail::INVALID_NUMBER_OF_ARGUMENTS,
            ),
        ),
        (
            "RETURN datetime('a', 'b')",
            compile(
                r::category::SYNTAX_ERROR,
                r::detail::INVALID_NUMBER_OF_ARGUMENTS,
            ),
        ),
        (
            "RETURN datetime(1)",
            compile(r::category::TYPE_ERROR, r::detail::INVALID_ARGUMENT_TYPE),
        ),
        ("RETURN datetime($text)", invalid.clone()),
        (
            "WITH {epochMillis: 1.5} AS m RETURN datetime(m)",
            invalid.clone(),
        ),
        (
            "WITH 9223372036854775807 AS ms RETURN datetime({epochMillis: ms})",
            out_of_range.clone(),
        ),
        ("RETURN $far", out_of_range.clone()),
        ("RETURN toString($far)", out_of_range.clone()),
        (
            "RETURN datetime() + 1",
            compile(r::category::SYNTAX_ERROR, r::detail::INVALID_ARGUMENT_TYPE),
        ),
        (
            "WITH [datetime()] AS xs RETURN xs[0] - 1",
            runtime(r::category::TYPE_ERROR, r::detail::INVALID_ARGUMENT_TYPE),
        ),
        (
            "RETURN sum(datetime())",
            runtime(r::category::TYPE_ERROR, r::detail::INVALID_ARGUMENT_TYPE),
        ),
    ] {
        let error = query_error(
            &db,
            request(
                query,
                [
                    ("text", Q::String("2024-01-01T00:00:00+0100".into())),
                    ("far", Q::DateTime(i64::MAX)),
                ],
            ),
        )
        .await;
        assert_eq!(phase(&error), expected, "{query}: {error}");
    }
    // An instant past RFC 3339's years still stores and compares; only
    // rendering it fails, as a native response does.
    rows(
        &db,
        request("CREATE (:Far {at: $far})", [("far", Q::DateTime(i64::MAX))]),
    )
    .await;
    assert_eq!(
        read(&db, "MATCH (f:Far) RETURN f.at > datetime(), f.at = f.at").await,
        vec![vec![json!(true), json!(true)]]
    );
    for query in ["MATCH (f:Far) RETURN f.at", "MATCH (f:Far) RETURN f"] {
        let error = query_error(&db, cypher::Request::new(query)).await;
        assert_eq!(phase(&error), out_of_range, "{query}");
        let Err(cypher::Error::Query(error)) = db.cypher_json(cypher::Request::new(query)).await
        else {
            panic!("{query}: JSON encoding should fail");
        };
        assert_eq!(phase(&error), out_of_range, "{query}");
    }
    db.close().await.unwrap();
}

/// SQL writes typed datetimes, reads them through TIMESTAMP literals, casts
/// and the statement clock, and encodes them as Cypher does.
#[tokio::test]
async fn sql_reads_and_writes_datetimes() {
    use cypher::sql::request::Request;
    let db = database().await;
    let sql = |query: &str, parameters: Vec<Q>| {
        let mut request = Request::new(query);
        request.parameters = parameters
            .into_iter()
            .enumerate()
            .map(|(index, value)| ((index + 1).to_string(), value))
            .collect();
        request
    };
    for seq in 0..4_i64 {
        db.sql(sql(
            "INSERT INTO Event (seq, at) VALUES (?, ?)",
            vec![Q::I64(seq), Q::DateTime(BASE + seq * HOUR)],
        ))
        .await
        .unwrap();
    }
    db.sql(sql(
        "UPDATE Event SET at = ? WHERE seq = 3",
        vec![Q::DateTime(BASE - HOUR)],
    ))
    .await
    .unwrap();
    for (query, expected) in [
        (
            "SELECT e.seq FROM Event e WHERE e.at >= TIMESTAMP '2023-11-14 23:00:00' ORDER BY e.at",
            seqs([1, 2]),
        ),
        (
            "SELECT e.seq FROM Event e ORDER BY e.at",
            seqs([3, 0, 1, 2]),
        ),
        (
            "SELECT MIN(e.at), MAX(e.at) FROM Event e",
            vec![vec![text(BASE - HOUR), text(BASE + 2 * HOUR)]],
        ),
        (
            "SELECT e.seq FROM Event e WHERE e.at BETWEEN TIMESTAMP '2023-11-14T22:00:00Z' \
             AND CAST('2023-11-14T23:30:00Z' AS TIMESTAMP) ORDER BY e.seq",
            seqs([0, 1]),
        ),
        (
            "SELECT CURRENT_TIMESTAMP > TIMESTAMP '2020-01-01', NOW() = CURRENT_TIMESTAMP, \
             CAST('2024-01-01' AS TIMESTAMP)",
            vec![vec![
                json!(true),
                json!(true),
                json!("2024-01-01T00:00:00.000Z"),
            ]],
        ),
    ] {
        let response = db.sql(Request::new(query)).await.unwrap();
        assert_eq!(response.rows, expected, "{query}");
        let encoded = db.sql_json(Request::new(query)).await.unwrap();
        let body: Value = serde_json::from_slice(encoded.body()).unwrap();
        assert_eq!(body["rows"], json!(expected), "{query}");
    }
    for (query, category, detail) in [
        (
            "SELECT TIMESTAMP 'nope'",
            r::category::UNSUPPORTED_FEATURE,
            r::detail::SQL_CONSTRUCT,
        ),
        (
            "SELECT CAST('nope' AS TIMESTAMP)",
            r::category::UNSUPPORTED_FEATURE,
            r::detail::FUNCTION,
        ),
        (
            "SELECT TIMESTAMP '2024-02-30'",
            r::category::ARGUMENT_ERROR,
            r::detail::INVALID_DATETIME,
        ),
        (
            "SELECT EXTRACT(SECOND FROM e.at) FROM Event e",
            r::category::UNSUPPORTED_FEATURE,
            r::detail::SQL_CONSTRUCT,
        ),
    ] {
        let Err(cypher::Error::Query(error)) = db.sql(Request::new(query)).await else {
            panic!("{query}: unreadable timestamp text fails to compile");
        };
        assert_eq!(
            (error.category.as_str(), error.detail.as_str(), error.phase),
            (category, detail, r::ErrorPhase::Compile),
            "{query}"
        );
    }
    // Text that arrives at run time fails as an invalid argument.
    let mut cast = Request::new("SELECT CAST(? AS TIMESTAMP)");
    cast.parameters.insert("1".into(), Q::String("nope".into()));
    let Err(cypher::Error::Query(error)) = db.sql(cast).await else {
        panic!("unreadable parameter text fails");
    };
    assert_eq!(
        (error.category.as_str(), error.detail.as_str(), error.phase),
        (
            r::category::ARGUMENT_ERROR,
            r::detail::INVALID_DATETIME,
            r::ErrorPhase::Runtime
        )
    );
    let explanation = db
        .explain_sql(Request::new(
            "SELECT e.seq FROM Event e WHERE e.at > TIMESTAMP '2023-11-15 00:00:00'",
        ))
        .await
        .unwrap();
    let [step, ..] = explanation.plan().matches()[&0].sources[0].access.steps() else {
        panic!("an access step");
    };
    assert!(format!("{:?}", step.op).contains("LabelScan"));
    create_index(&db, index::IndexSpec::node_range("Event", "at")).await;
    let explanation = db
        .explain_sql(Request::new(
            "SELECT e.seq FROM Event e WHERE e.at > TIMESTAMP '2023-11-15 00:00:00'",
        ))
        .await
        .unwrap();
    let [step, ..] = explanation.plan().matches()[&0].sources[0].access.steps() else {
        panic!("an access step");
    };
    assert!(format!("{:?}", step.op).contains("RangeIndex"));
    db.close().await.unwrap();
}

/// Datetime fields bucket and compute stored datetimes in Cypher and, through
/// `EXTRACT`, in SQL; a string that only looks like a datetime has none.
#[tokio::test]
async fn datetime_fields_bucket_and_compute_stored_datetimes() {
    use cypher::sql::request::Request;
    let db = database().await;
    events(&db).await;
    // BASE is 22:13:20 on November 14, so seq 0 and 1 fall on the 14th.
    let by_day = vec![vec![json!(14), json!(2)], vec![json!(15), json!(8)]];
    assert_eq!(
        read(
            &db,
            "MATCH (e:Event) WHERE e.seq < 10 \
             RETURN e.at.day AS day, count(*) AS events ORDER BY day",
        )
        .await,
        by_day
    );
    assert_eq!(
        read(
            &db,
            "MATCH (e:Event) WHERE e.seq < 10 \
             RETURN max(e.at.epochMillis) - min(e.at.epochMillis) AS span, \
             min(e.at.hour) AS earliest, max(e.at.hour) AS latest",
        )
        .await,
        vec![vec![json!(9 * HOUR), json!(0), json!(23)]]
    );
    assert_eq!(
        read(
            &db,
            "MATCH (e:Event {seq: 0}) RETURN e.at.year, e.at.month, e.at.minute, \
             e.at.second, e.at.millisecond, e.at.epochSeconds, e.at.dayOfWeek",
        )
        .await,
        vec![vec![
            json!(2023),
            json!(11),
            json!(13),
            json!(20),
            json!(0),
            json!(BASE / 1000),
            json!(2),
        ]]
    );
    for (query, expected) in [
        (
            "SELECT EXTRACT(DAY FROM e.at) AS day, COUNT(*) AS events FROM Event e \
             WHERE e.seq < 10 GROUP BY EXTRACT(DAY FROM e.at) ORDER BY day",
            by_day.clone(),
        ),
        (
            "SELECT EXTRACT(YEAR FROM e.at), EXTRACT(DOY FROM e.at), EXTRACT(EPOCH FROM e.at) \
             FROM Event e WHERE e.seq = 0",
            vec![vec![json!(2023), json!(318), json!(1_700_000_000.0)]],
        ),
    ] {
        let response = db.sql(Request::new(query)).await.unwrap();
        assert_eq!(response.rows, expected, "{query}");
        let encoded = db.sql_json(Request::new(query)).await.unwrap();
        let body: Value = serde_json::from_slice(encoded.body()).unwrap();
        assert_eq!(body["rows"], json!(expected), "{query}");
    }
    // The string holds no datetime, so it has no fields.
    let error = query_error(
        &db,
        cypher::Request::new("MATCH (e:Event {seq: 99}) RETURN e.at.year"),
    )
    .await;
    assert_eq!(
        (error.category.as_str(), error.phase),
        (r::category::TYPE_ERROR, r::ErrorPhase::Runtime)
    );
    db.close().await.unwrap();
}
