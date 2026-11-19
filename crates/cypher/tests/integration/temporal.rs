//! Binding and planning of datetimes: which temporal calls the profile
//! accepts, literal folding, and the index bounds datetime predicates give.
use helix_ast::value::PropertyValue as P;
use helix_planner::{catalog, context, ir, relational as r};

fn error(text: &str) -> r::QueryError {
    helix_cypher::compile(text).expect_err(text)
}

/// The value of `RETURN <expression> AS v` as bound, before planning.
fn bound(expression: &str) -> r::Expression {
    let query = helix_cypher::compile(&format!("RETURN {expression} AS v")).unwrap();
    let [r::Operator::Project { items, .. }] = query.operators() else {
        panic!("one projection: {query:?}");
    };
    items[0].expression.clone()
}

#[test]
fn datetime_and_timestamp_bind_with_literal_text_folded() {
    use r::{Expression as E, Function as F, Value as V};
    assert_eq!(
        bound("datetime('2024-01-01T00:00:00+01:00')"),
        E::Literal(V::DateTime(1_704_063_600_000))
    );
    assert_eq!(bound("DateTime('1970-01-01')"), E::Literal(V::DateTime(0)));
    assert_eq!(bound("datetime()"), E::Function(F::DateTime, vec![]));
    assert_eq!(bound("timestamp()"), E::Function(F::Timestamp, vec![]));
    assert_eq!(
        bound("datetime($p)"),
        E::Function(F::DateTime, vec![E::Parameter("p".into())])
    );
    assert!(matches!(
        bound("datetime({epochMillis: 5})"),
        E::Function(F::DateTime, arguments) if matches!(arguments.as_slice(), [E::Map(_)])
    ));
    // Null text is not read at bind time; the call returns null.
    assert!(matches!(
        bound("datetime(null)"),
        E::Function(F::DateTime, _)
    ));
}

#[test]
fn temporal_calls_outside_the_profile_report_their_capability() {
    for (text, reason) in [
        ("RETURN datetime({year: 2024}) AS v", "function:datetime"),
        ("RETURN datetime({}) AS v", "function:datetime"),
        (
            "RETURN datetime({epochMillis: 1, timezone: 'Z'}) AS v",
            "function:datetime",
        ),
        ("RETURN date() AS v", "function:date"),
        ("RETURN localdatetime() AS v", "function:localdatetime"),
        ("RETURN localtime() AS v", "function:localtime"),
        ("RETURN time() AS v", "function:time"),
        ("RETURN duration('P1D') AS v", "function:duration"),
        // Literal text outside the grammar may be ISO 8601 the profile does
        // not read, so it reports the capability rather than bad input.
        (
            "RETURN datetime('2015-W30-2T214032.142Z') AS v",
            "function:datetime",
        ),
        (
            "RETURN datetime('2015-07-21T21:40:32.142+02:00[Europe/Stockholm]') AS v",
            "function:datetime",
        ),
        ("RETURN datetime('not a date') AS v", "function:datetime"),
        // Fields that are not zero-padded and years outside RFC 3339's are
        // outside the grammar too.
        ("RETURN datetime('2024-1-5') AS v", "function:datetime"),
        (
            "RETURN datetime('2024-01-05T1:02:03') AS v",
            "function:datetime",
        ),
        (
            "RETURN datetime('+10000-01-01T00:00:00.000Z') AS v",
            "function:datetime",
        ),
        ("RETURN datetime('-0001-01-01') AS v", "function:datetime"),
    ] {
        let error = error(text);
        assert_eq!(error.category, r::category::UNSUPPORTED_FEATURE, "{text}");
        assert_eq!(error.detail, r::detail::FUNCTION, "{text}");
        let span = error.span.expect("a located call");
        let name = text[span.start..span.end].split('(').next().unwrap();
        assert_eq!(
            format!("function:{}", name.to_lowercase()),
            reason,
            "{text}"
        );
    }
    for (text, category, detail) in [
        (
            "RETURN timestamp(1) AS v",
            r::category::SYNTAX_ERROR,
            r::detail::INVALID_NUMBER_OF_ARGUMENTS,
        ),
        (
            "RETURN datetime(1, 2) AS v",
            r::category::SYNTAX_ERROR,
            r::detail::INVALID_NUMBER_OF_ARGUMENTS,
        ),
        (
            "RETURN datetime(true) AS v",
            r::category::TYPE_ERROR,
            r::detail::INVALID_ARGUMENT_TYPE,
        ),
        (
            "RETURN -datetime() AS v",
            r::category::SYNTAX_ERROR,
            r::detail::INVALID_ARGUMENT_TYPE,
        ),
        // Text in the grammar's form that names no instant is bad input.
        (
            "RETURN datetime('2024-13-01') AS v",
            r::category::ARGUMENT_ERROR,
            r::detail::INVALID_DATETIME,
        ),
        (
            "RETURN datetime('2024-02-30T00:00:00Z') AS v",
            r::category::ARGUMENT_ERROR,
            r::detail::INVALID_DATETIME,
        ),
        (
            "RETURN datetime('2016-12-31T23:59:60Z') AS v",
            r::category::ARGUMENT_ERROR,
            r::detail::INVALID_DATETIME,
        ),
        // An offset that moves the instant outside the four-digit UTC years
        // names an instant whose text this grammar cannot read back.
        (
            "RETURN datetime('9999-12-31T23:59:59-00:01') AS v",
            r::category::ARGUMENT_ERROR,
            r::detail::INVALID_DATETIME,
        ),
        (
            "RETURN datetime('0000-01-01T00:00:00+00:01') AS v",
            r::category::ARGUMENT_ERROR,
            r::detail::INVALID_DATETIME,
        ),
    ] {
        let error = error(text);
        assert_eq!(
            (error.category.as_str(), error.detail.as_str(), error.phase),
            (category, detail, r::ErrorPhase::Compile),
            "{text}: {error}"
        );
    }
}

/// Datetime literals, bound datetime parameters and `datetime()` of bound
/// text bound range and equality index access; anything the planner cannot
/// read keeps the label scan, where evaluation decides.
#[test]
fn datetime_predicates_bound_index_access() {
    let indexes = catalog::IndexCatalogSnapshot::default()
        .with_node_eq(catalog::ScopedPropertyKey::try_new("E", "day").unwrap())
        .with_node_range(
            catalog::ScopedPropertyDirectionKey::try_new(
                "E",
                "at",
                helix_ast::index::RangeIndexDirection::Asc,
            )
            .unwrap(),
        );
    let mut context = context::PlannerContext {
        indexes,
        ..context::PlannerContext::default()
    };
    for (name, value) in [
        ("at", P::DateTime(1_700_000_000_000)),
        ("text", P::String("2023-11-14T22:13:20Z".into())),
        ("bad", P::String("tomorrow".into())),
        ("number", P::I64(1)),
        ("missing", P::Null),
    ] {
        context
            .params
            .make_mut()
            .values
            .insert(ir::NonEmptyString::new(name).unwrap(), value);
    }
    let source = |text: &str| {
        let plan = r::plan(helix_cypher::compile(text).unwrap(), &context).unwrap();
        let [step] = plan.matches()[&0].sources[0].access.steps() else {
            panic!("single access step: {text}");
        };
        format!("{:?}", step.op)
            .trim_start_matches("Access { plan: Node(")
            .split(|c: char| !c.is_alphanumeric())
            .next()
            .unwrap()
            .to_owned()
    };
    for (text, expected) in [
        ("MATCH (n:E) WHERE n.at > $at RETURN n", "RangeIndex"),
        ("MATCH (n:E) WHERE $at <= n.at RETURN n", "RangeIndex"),
        (
            "MATCH (n:E) WHERE n.at >= datetime('2023-11-14T22:13:20Z') RETURN n",
            "RangeIndex",
        ),
        (
            "MATCH (n:E) WHERE n.at < datetime($text) AND n.at > $at RETURN n",
            "RangeIndex",
        ),
        ("MATCH (n:E {at: $at}) RETURN n", "RangeIndex"),
        ("MATCH (n:E {day: $at}) RETURN n", "Bitmap"),
        (
            "MATCH (n:E) WHERE n.day = datetime('2023-11-14') RETURN n",
            "Bitmap",
        ),
        (
            "MATCH (n:E) WHERE n.day = datetime($text) RETURN n",
            "Bitmap",
        ),
        (
            "MATCH (n:E) WHERE n.day = datetime($missing) RETURN n",
            "Empty",
        ),
        // Text or numbers the planner cannot read keep the scan.
        (
            "MATCH (n:E) WHERE n.at < datetime($bad) RETURN n",
            "LabelScan",
        ),
        (
            "MATCH (n:E) WHERE n.at < datetime($number) RETURN n",
            "LabelScan",
        ),
    ] {
        assert_eq!(source(text), expected, "{text}");
    }
}

/// 2023-11-14T22:13:20.000Z, the statement clock of the plans below.
const NOW: i64 = 1_700_000_000_000;
const WEEK: i64 = 604_800_000;

/// The planning context's statement clock is known before planning, so a
/// comparison with `datetime()`, `timestamp()`, integer arithmetic over them
/// and `datetime({epochMillis: …})` of such an integer bounds range and
/// equality index access at the instant execution evaluates. An operand that
/// would fail, such as an overflow or datetime arithmetic, keeps the scan,
/// whose evaluation raises the error.
#[test]
fn clock_relative_predicates_bound_index_access() {
    let indexes = catalog::IndexCatalogSnapshot::default()
        .with_node_eq(catalog::ScopedPropertyKey::try_new("E", "day").unwrap())
        .with_node_range(
            catalog::ScopedPropertyDirectionKey::try_new(
                "E",
                "at",
                helix_ast::index::RangeIndexDirection::Asc,
            )
            .unwrap(),
        )
        .with_node_range(
            catalog::ScopedPropertyDirectionKey::try_new(
                "E",
                "ms",
                helix_ast::index::RangeIndexDirection::Asc,
            )
            .unwrap(),
        );
    let mut context = context::PlannerContext {
        indexes,
        statement_time: NOW,
        ..context::PlannerContext::default()
    };
    for (name, value) in [
        ("window", P::I64(WEEK)),
        ("text", P::String("a week".into())),
    ] {
        context
            .params
            .make_mut()
            .values
            .insert(ir::NonEmptyString::new(name).unwrap(), value);
    }
    let plan = |query: r::Query| {
        let plan = r::plan(query, &context).unwrap();
        assert_eq!(plan.statement_time(), NOW);
        let first = plan.matches().values().next().expect("a MATCH");
        let [step, ..] = first.sources[0].access.steps() else {
            panic!("an access step");
        };
        format!("{:?}", step.op)
    };
    let cypher = |text: &str| (text.to_owned(), plan(helix_cypher::compile(text).unwrap()));
    let sql = |text: &str| {
        let query =
            helix_cypher::sql::compile(text, &helix_cypher::sql::Mappings::default()).unwrap();
        (text.to_owned(), plan(query))
    };
    let source = |op: &str| {
        op.trim_start_matches("Access { plan: Node(")
            .split(|c: char| !c.is_alphanumeric())
            .next()
            .unwrap()
            .to_owned()
    };
    let datetime = |millis: i64| format!("DateTime({millis})");
    let integer = |millis: i64| format!("I64({millis})");
    for ((text, op), expected, bound) in [
        (
            cypher("MATCH (n:E) WHERE n.at < datetime() RETURN n"),
            "RangeIndex",
            datetime(NOW),
        ),
        (
            cypher("MATCH (n:E) WHERE n.day = datetime() RETURN n"),
            "Bitmap",
            datetime(NOW),
        ),
        (
            cypher("MATCH (n:E {at: datetime()}) RETURN n"),
            "RangeIndex",
            datetime(NOW),
        ),
        (
            cypher("MATCH (n:E) WHERE n.at > datetime({epochMillis: timestamp() - 604800000}) RETURN n"),
            "RangeIndex",
            datetime(NOW - WEEK),
        ),
        (
            cypher("MATCH (n:E) WHERE datetime({epochMillis: timestamp() - $window}) <= n.at RETURN n"),
            "RangeIndex",
            datetime(NOW - WEEK),
        ),
        (
            cypher("MATCH (n:E) WHERE n.day = datetime({epochMillis: timestamp() / 86400000 * 86400000}) RETURN n"),
            "Bitmap",
            datetime(NOW / 86_400_000 * 86_400_000),
        ),
        (
            cypher("MATCH (n:E) WHERE n.ms > timestamp() - $window RETURN n"),
            "RangeIndex",
            integer(NOW - WEEK),
        ),
        (
            cypher("MATCH (n:E) WHERE -1000 + timestamp() < n.ms AND n.ms <= timestamp() % 1000 + 3 * 1000 RETURN n"),
            "RangeIndex",
            integer(NOW - 1000),
        ),
        (
            cypher(
                "MATCH (n:E) WITH n, timestamp() - $window AS since WHERE n.ms >= since RETURN n",
            ),
            "RangeIndex",
            integer(NOW - WEEK),
        ),
        (
            sql("SELECT e.seq FROM E e WHERE e.at <= NOW()"),
            "RangeIndex",
            datetime(NOW),
        ),
        (
            sql("SELECT e.seq FROM E e WHERE e.at > CURRENT_TIMESTAMP(0)"),
            "RangeIndex",
            datetime((NOW + 500) / 1000 * 1000),
        ),
        (
            sql("SELECT e.seq FROM E e WHERE e.day = CURRENT_TIMESTAMP"),
            "Bitmap",
            datetime(NOW),
        ),
        (
            sql("SELECT e.seq FROM E e WHERE e.ms >= TIMESTAMP() - 604800000"),
            "RangeIndex",
            integer(NOW - WEEK),
        ),
    ] {
        assert_eq!(source(&op), expected, "{text}: {op}");
        assert!(op.contains(&bound), "{text}: {bound} in {op}");
    }
    // Operands that fail, or whose value the planner cannot read, keep the
    // scan, so evaluation decides and raises the error.
    for text in [
        "MATCH (n:E) WHERE n.ms > timestamp() * 9223372036854775807 RETURN n",
        "MATCH (n:E) WHERE n.ms > timestamp() / 0 RETURN n",
        "MATCH (n:E) WHERE n.ms > timestamp() % 0 RETURN n",
        "MATCH (n:E) WHERE n.ms > -(timestamp() - timestamp() - 9223372036854775807 - 1) RETURN n",
        "MATCH (n:E) WHERE n.ms > timestamp() - $text RETURN n",
        "MATCH (n:E) WHERE n.ms > timestamp() - 1.5 RETURN n",
        "MATCH (n:E) WHERE n.at > datetime({epochMillis: timestamp() * 100000}) RETURN n",
        "MATCH (n:E) WHERE n.at > datetime({epochMillis: $text}) RETURN n",
        "MATCH (n:E) WHERE n.at > datetime({epochMillis: n.ms}) RETURN n",
    ] {
        assert_eq!(source(&cypher(text).1), "LabelScan", "{text}");
    }
}
