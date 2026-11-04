use super::request::Request;
use super::tables::relationship_table;
use super::{operators, token_estimate, translate, Mappings, MAX_OPERATORS, MAX_TOKENS};
use helix_ast::query::QueryValue;
use helix_planner::relational::{category, detail, Span};
use proptest::prelude::*;
use sqlparser::{dialect::GenericDialect, tokenizer::Tokenizer};

fn cypher(sql: &str) -> String {
    translate(sql, &Mappings::default())
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        .to_string()
}

fn error(sql: &str) -> (String, String) {
    let error = translate(sql, &Mappings::default()).expect_err(sql);
    (error.category, error.detail)
}

fn assert_error(sql: &str, expected_category: &str, expected_detail: &str) {
    assert_eq!(
        error(sql),
        (expected_category.to_owned(), expected_detail.to_owned()),
        "{sql}"
    );
}

#[test]
fn relationship_tables_need_typed_middles_and_named_ends() {
    for (label, expected) in [
        (
            "Person_ACTED_IN_Movie",
            Some(("Person", "ACTED_IN", "Movie")),
        ),
        ("A_b_REL_c_d", Some(("A_b", "REL", "c_d"))),
        ("User_LIKES_Music", Some(("User", "LIKES", "Music"))),
        ("Person_KNOWS_Person", Some(("Person", "KNOWS", "Person"))),
        ("My_Table", None),
        ("ACTED_IN", None),
        ("ORDER_LINE_ITEMS", None),
        ("A_RELATED_TO_B", None),
        ("flyway_schema_history", None),
        ("test_write__c", None),
        ("a___b", None),
        ("_x_T_y", Some(("_x", "T", "y"))),
        ("x_T_", None),
        ("Person_ACTED_IN_", None),
    ] {
        assert_eq!(relationship_table(label), expected, "{label}");
    }
}

#[test]
fn mappings_match_case_insensitively() {
    let mappings = Mappings::default()
        .table("PEOPLE", "Person")
        .join_column("People.Movie_ID", "DIRECTED");
    assert_eq!(
        translate(
            "SELECT p.name FROM people p JOIN movies m ON m.id = p.movie_id",
            &mappings
        )
        .unwrap()
        .to_string(),
        "MATCH (p:Person)-[directed:DIRECTED]->(m:movies) RETURN p.name"
    );
}

#[test]
fn guards_bound_text_tokens_operators_and_nesting() {
    let large = format!("SELECT '{}'", "x".repeat(16 * 1024 * 1024));
    assert_error(&large, category::RESOURCE_LIMIT, detail::QUERY_TOO_LARGE);
    let tokens = format!("SELECT {}1", "1,".repeat(MAX_TOKENS / 2));
    assert_error(&tokens, category::RESOURCE_LIMIT, detail::TOO_MANY_TOKENS);
    let chain = |operator: &str, terms: usize| {
        format!(
            "SELECT 1 FROM t WHERE {}",
            vec!["t.a = 1"; terms].join(operator)
        )
    };
    // Each comparison is one operator and each OR another.
    assert_error(
        &chain(" OR ", MAX_OPERATORS / 2 + 1),
        category::RESOURCE_LIMIT,
        detail::EXPRESSION_DEPTH,
    );
    let flat = chain(" OR ", MAX_OPERATORS / 2);
    crate::resolve(&translate(&flat, &Mappings::default()).unwrap()).unwrap();
    // Arithmetic nests, so a long sum stops at the expression depth limit.
    let sum = format!("SELECT {}", vec!["1"; 200].join(" + "));
    assert_error(&sum, category::RESOURCE_LIMIT, detail::EXPRESSION_DEPTH);
    let nested = format!("SELECT {}1{}", "(".repeat(200), ")".repeat(200));
    assert_error(&nested, category::RESOURCE_LIMIT, detail::EXPRESSION_DEPTH);
    // Unary signs, commas and VALUES rows are not operators.
    let rows = format!(
        "INSERT INTO t (a, b) VALUES {}",
        vec!["(-1, -2)"; 5_000].join(", ")
    );
    assert!(cypher(&rows).starts_with("UNWIND [{a: -1, b: -2}"));
}

/// The largest accepted inputs translate on a thread with tokio's default
/// 2 MiB stack, so no request can overflow a server worker's stack.
#[test]
fn accepted_inputs_fit_a_worker_stack() {
    let inputs = [
        format!(
            "SELECT 1 FROM t WHERE {}",
            vec!["t.a = 1"; MAX_OPERATORS / 2].join(" OR ")
        ),
        format!(
            "SELECT 1 FROM t WHERE {}",
            vec!["t.a"; MAX_OPERATORS].join(" AND ")
        ),
        format!("SELECT {}", vec!["1"; MAX_OPERATORS].join(" + ")),
        format!("SELECT {}", vec!["1"; MAX_OPERATORS].join(" || ")),
        format!("SELECT 1 {}", "IS NULL ".repeat(MAX_OPERATORS)),
        format!("SELECT {}1{}", "(".repeat(48), ")".repeat(48)),
        format!("SELECT {}1", "-".repeat(48)),
        format!("SELECT {}TRUE", "NOT ".repeat(48)),
        format!("SELECT x IN ({})", vec!["1"; 100_000].join(", ")),
    ];
    std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(move || {
            for input in inputs {
                let _ = translate(&input, &Mappings::default()).map(|s| crate::resolve(&s));
            }
        })
        .unwrap()
        .join()
        .unwrap();
}

proptest! {
    /// The estimate that guards tokenizing never undercounts sqlparser.
    #[test]
    fn token_estimate_bounds_sqlparser(text in "[a-zEXNR0-9_ ,.()'\"`*+\\-/=<>!|\n\t#@$&é²\\\\]{0,64}") {
        if let Ok(tokens) = Tokenizer::new(&GenericDialect {}, &text).tokenize() {
            prop_assert!(token_estimate(&text) >= tokens.len(), "{text}");
        }
    }
}

#[test]
fn token_estimate_bounds_quotes_and_comments() {
    for text in [
        "''``--\n",
        "--\nx",
        "a--b\n\n",
        "/* a */\n",
        "\"\"\"\"\"\"@@`<0a \t",
        "$$'$$, 1, 2, 3, ''",
        "E'a\\'b', 1, 2",
    ] {
        let tokens = Tokenizer::new(&GenericDialect {}, text).tokenize().unwrap();
        assert!(token_estimate(text) >= tokens.len(), "{text:?}");
    }
}

#[test]
fn operators_count_binary_positions_only() {
    let count = |text: &str| {
        operators(
            &Tokenizer::new(&GenericDialect {}, text)
                .tokenize_with_location()
                .unwrap(),
        )
    };
    assert_eq!(count("SELECT -1, +2, (-3)"), 0);
    assert_eq!(count("SELECT 1 - -1"), 1);
    assert_eq!(count("SELECT a AND b OR c XOR d"), 3);
    assert_eq!(
        count("SELECT x IS NULL, y LIKE 'a', z IN (1), w BETWEEN 1 AND 2"),
        5
    );
    assert_eq!(count("SELECT `and`, \"or\" FROM t"), 0);
}

#[test]
fn placeholders_are_numbered_or_named() {
    assert_eq!(
        cypher("INSERT INTO t (a, b) VALUES (?, ?), (?, ?)"),
        "UNWIND [{a: $1, b: $2}, {a: $3, b: $4}] AS properties CREATE (t:t) SET t = properties"
    );
    assert_eq!(
        cypher("SELECT $1, $name, :other"),
        "RETURN $1, $name, $other"
    );
    assert_eq!(
        cypher("SELECT t.a FROM t WHERE t.a IN (?) AND t.b IN (?, ?)"),
        "MATCH (t:t) WHERE t.a IN $1 AND t.b IN [$2, $3] RETURN t.a"
    );
}

#[test]
fn numbers_keep_their_range() {
    assert_eq!(
        cypher("SELECT 9223372036854775807, -9223372036854775808, 1.5, -2.5e3, 0.0"),
        "RETURN 9223372036854775807, -9223372036854775808, 1.5, -2500.0, 0.0"
    );
    assert_error(
        "SELECT 9223372036854775808",
        category::SYNTAX_ERROR,
        detail::INTEGER_OVERFLOW,
    );
    assert_error(
        "SELECT -9223372036854775809",
        category::SYNTAX_ERROR,
        detail::INTEGER_OVERFLOW,
    );
    assert_error(
        "SELECT 1e400",
        category::SYNTAX_ERROR,
        detail::FLOATING_POINT_OVERFLOW,
    );
    assert_eq!(cypher("SELECT -t.a FROM t"), "MATCH (t:t) RETURN -t.a");
    assert_eq!(
        cypher("SELECT +t.a, - 'x' FROM t"),
        "MATCH (t:t) RETURN t.a, -'x'"
    );
}

#[test]
fn errors_point_into_the_sql() {
    let span = |sql: &str| translate(sql, &Mappings::default()).unwrap_err().span;
    assert_eq!(span("SELECT q.x FROM t"), Some(Span { start: 7, end: 10 }));
    assert_eq!(
        span("SELECT 1;\nSELECT 2"),
        Some(Span { start: 10, end: 18 })
    );
    assert_eq!(
        span("SELECT * FROM t u v"),
        Some(Span { start: 18, end: 18 })
    );
    assert_eq!(span("SELECT * FROM"), None);
    assert_eq!(
        span("SELECT 'é', x FROM a, b"),
        Some(Span { start: 13, end: 14 })
    );
    let compiled = super::compile("SELECT toLower(1, 2)", &Mappings::default()).unwrap_err();
    assert_eq!(compiled.detail, detail::INVALID_NUMBER_OF_ARGUMENTS);
}

#[test]
fn relationship_tables_resolve_endpoints() {
    assert_eq!(
        cypher("SELECT _start.name, _end.name, v$id, v$person_id FROM Person_KNOWS_Person"),
        "MATCH (_start:Person)-[person_knows_person:KNOWS]->(_end:Person) \
         RETURN _start.name, _end.name, id(person_knows_person), id(_start)"
    );
    assert_error(
        "SELECT Person.name FROM Person_KNOWS_Person",
        category::SYNTAX_ERROR,
        detail::AMBIGUOUS_COLUMN,
    );
    assert_error(
        "SELECT v$nobody_id FROM Person_KNOWS_Person",
        category::SYNTAX_ERROR,
        detail::UNDEFINED_VARIABLE,
    );
    assert_eq!(
        cypher("SELECT r.* FROM Person_ACTED_IN_Movie r"),
        "MATCH (_start:Person)-[r:ACTED_IN]->(_end:Movie) RETURN properties(r) AS r"
    );
    assert_error(
        "SELECT 1 FROM Person_ACTED_IN_Movie r JOIN Movie m ON m.id = r.movie",
        category::UNSUPPORTED_FEATURE,
        detail::SQL_CONSTRUCT,
    );
    assert_error(
        "SELECT 1 FROM Person_ACTED_IN_Movie, Person_LIKES_Movie",
        category::SYNTAX_ERROR,
        detail::VARIABLE_ALREADY_BOUND,
    );
}

#[test]
fn joins_split_and_reject_by_rule() {
    assert_eq!(
        cypher("SELECT a.x FROM A a CROSS JOIN B b JOIN C c ON c.id = b.c_id"),
        "MATCH (a:A), (b:B)-[c_id:C_ID]->(c:C) RETURN a.x"
    );
    assert_eq!(
        cypher("SELECT a.x FROM A a JOIN B ON B.id = A.b_id"),
        "MATCH (a:A)-[b_id:B_ID]->(b:B) RETURN a.x"
    );
    assert_eq!(
        cypher("SELECT a.x FROM A a JOIN B b ON id = b_id"),
        "MATCH (a:A)-[b_id:B_ID]->(b:B) RETURN a.x"
    );
    for sql in [
        "SELECT 1 FROM A a JOIN B b USING (x, y)",
        "SELECT 1 FROM A a JOIN B b ON a.x > b.y",
        "SELECT 1 FROM A a JOIN B b ON a.x = 1",
        "SELECT 1 FROM A a RIGHT JOIN B b ON b.id = a.b",
        "SELECT 1 FROM A a FULL OUTER JOIN B b ON b.id = a.b",
        "SELECT 1 FROM (A a JOIN B b ON b.id = a.b)",
        "SELECT 1 FROM generate_series(1, 2)",
        "SELECT 1 FROM A a (x, y)",
    ] {
        assert_error(sql, category::UNSUPPORTED_FEATURE, detail::SQL_CONSTRUCT);
    }
    assert_error(
        "SELECT 1 FROM A a JOIN B b ON q.id = a.b",
        category::SYNTAX_ERROR,
        detail::UNDEFINED_VARIABLE,
    );
}

#[test]
fn expressions_translate_with_sql_semantics() {
    for (sql, expected) in [
        (
            "SELECT t.a IS TRUE, t.a IS NOT TRUE, t.a IS FALSE, t.a IS NOT FALSE FROM t",
            "MATCH (t:t) RETURN coalesce(t.a = TRUE, FALSE), NOT coalesce(t.a = TRUE, FALSE), \
             coalesce(t.a = FALSE, FALSE), NOT coalesce(t.a = FALSE, FALSE)",
        ),
        (
            "SELECT t.a IS DISTINCT FROM t.b, t.a IS NOT DISTINCT FROM 1 FROM t",
            "MATCH (t:t) RETURN NOT coalesce(t.a = t.b, t.a IS NULL AND t.b IS NULL), \
             coalesce(t.a = 1, t.a IS NULL AND 1 IS NULL)",
        ),
        (
            "SELECT ARRAY[1, 2], t.a NOT IN (1), NOT t.a IN (2) FROM t",
            "MATCH (t:t) RETURN [1, 2], NOT t.a IN [1], NOT t.a IN [2]",
        ),
        (
            "SELECT TRIM(LEADING ' ' FROM ' a'), TRIM(TRAILING ' ' FROM 'a '), TRIM(BOTH ' ' FROM ' a '), TRIM(' a ')",
            "RETURN ltrim(' a'), rtrim('a '), trim(' a '), trim(' a ')",
        ),
        (
            "SELECT SUBSTRING('abc' FROM 2), SUBSTRING('abc' FOR 2), substr('abc', 1)",
            "RETURN substring('abc', 1), substring('abc', 0, 2), substring('abc', 0)",
        ),
        (
            "SELECT CEIL(1.5), FLOOR(1.5), power(2, 3), mod(5, 2), COUNT(*), Sum(1)",
            "RETURN ceil(1.5), floor(1.5), 2 ^ 3, 5 % 2, count(*), sum(1)",
        ),
        (
            "SELECT 1::varchar, TRY_CAST('1' AS INT), 1 % 2, 'a' || 1",
            "RETURN toString(1), toInteger('1'), 1 % 2, 'a' + 1",
        ),
        (
            "SELECT array_agg(t.a), stddev(t.a) FROM t",
            "MATCH (t:t) RETURN collect(t.a), stDev(t.a)",
        ),
        ("SELECT elementId(t) FROM t", "MATCH (t:t) RETURN id(t)"),
        (
            "SELECT t.a FROM t WHERE (t.a AND t.b) XOR t.c OR t.d XOR t.e",
            "MATCH (t:t) WHERE t.a AND t.b XOR t.c OR t.d XOR t.e RETURN t.a",
        ),
    ] {
        assert_eq!(cypher(sql), expected, "{sql}");
    }
    for sql in [
        "SELECT EXTRACT(YEAR FROM t.d) FROM t",
        "SELECT INTERVAL '1' DAY",
        "SELECT DATE '2020-01-01'",
        "SELECT CAST(1 AS DATE)",
        "SELECT CAST('a' AS VARCHAR FORMAT 'x')",
        "SELECT x'00'",
        "SELECT t.a SIMILAR TO 'x' FROM t",
        "SELECT t.a LIKE t.b FROM t",
        "SELECT t.a LIKE 'a\\%' FROM t",
        "SELECT t.a LIKE '%a' ESCAPE '!' FROM t",
        "SELECT row_number() OVER (ORDER BY t.a) FROM t",
        "SELECT count(*) FILTER (WHERE t.a) FROM t",
        "SELECT count(t.*) FROM t",
        "SELECT CEIL(1.5 TO DAY)",
        "SELECT TRIM('x' FROM 'xax')",
        "SELECT (1, 2)",
        "SELECT (1, 2) + (3, 4)",
        "SELECT t.a AND t.b XOR t.c FROM t",
        "SELECT NOT t.a XOR t.b FROM t",
        "SELECT t.a = t.b XOR t.c FROM t",
        "SELECT t.a ~ 'x' FROM t",
        "SELECT POSITION('a' IN 'abc')",
    ] {
        assert_error(sql, category::UNSUPPORTED_FEATURE, detail::SQL_CONSTRUCT);
    }
    assert_error(
        "SELECT pg_catalog.lower('A')",
        category::UNSUPPORTED_FEATURE,
        detail::NAMESPACED_FUNCTION,
    );
    for sql in [
        "SELECT EXISTS (SELECT 1)",
        "SELECT (SELECT 1)",
        "SELECT t.a FROM t WHERE t.a IN (SELECT 1)",
        "SELECT count((SELECT 1))",
    ] {
        assert_error(
            sql,
            category::UNSUPPORTED_FEATURE,
            detail::PROCEDURES_AND_SUBQUERIES,
        );
    }
    for sql in [
        "SELECT concat()",
        "SELECT nullif(1)",
        "SELECT nvl2(1, 2)",
        "SELECT square(1, 2)",
        "SELECT substring('a')",
        "SELECT (1, 2) = (1, 2, 3)",
    ] {
        assert_eq!(error(sql).0, category::SYNTAX_ERROR, "{sql}");
    }
}

#[test]
fn select_clauses_follow_sql() {
    for (sql, expected) in [
        (
            "SELECT t.a FROM t LIMIT 2, 5",
            "MATCH (t:t) RETURN t.a SKIP 2 LIMIT 5",
        ),
        (
            "SELECT t.a FROM t OFFSET 3 ROWS FETCH FIRST 4 ROWS ONLY",
            "MATCH (t:t) RETURN t.a SKIP 3 LIMIT 4",
        ),
        (
            "SELECT t.a FROM t LIMIT ? OFFSET ?",
            "MATCH (t:t) RETURN t.a SKIP $2 LIMIT $1",
        ),
        (
            "SELECT t.a AS x FROM t ORDER BY x DESC NULLS FIRST, t.b ASC NULLS LAST",
            "MATCH (t:t) RETURN t.a AS x ORDER BY x DESC, t.b",
        ),
        (
            "SELECT ALL t.a FROM t",
            "MATCH (t:t) RETURN t.a",
        ),
        (
            "SELECT t.a, count(*) FROM t GROUP BY t.a HAVING t.a > 1 ORDER BY count(*) + 1",
            "MATCH (t:t) WITH t.a AS `t.a`, count(*) AS `count(*)` WHERE `t.a` > 1 \
             RETURN `t.a`, `count(*)` ORDER BY `count(*)` + 1",
        ),
        (
            "SELECT t.a FROM t GROUP BY t.a ORDER BY max(t.b)",
            "MATCH (t:t) WITH t.a AS `t.a`, max(t.b) AS __having_col_0 RETURN `t.a` ORDER BY __having_col_0",
        ),
        (
            "SELECT 1 AS one, count(*) FROM t GROUP BY t.a",
            "MATCH (t:t) WITH 1 AS one, count(*) AS `count(*)`, t.a AS __group_col_0 RETURN one, `count(*)`",
        ),
    ] {
        assert_eq!(cypher(sql), expected, "{sql}");
    }
    for (sql, expected_category, expected_detail) in [
        (
            "SELECT * FROM t GROUP BY t.a",
            category::SYNTAX_ERROR,
            detail::INVALID_AGGREGATION,
        ),
        (
            "SELECT *",
            category::SYNTAX_ERROR,
            detail::NO_VARIABLES_IN_SCOPE,
        ),
        (
            "SELECT 1 WHERE 1 = 1",
            category::UNSUPPORTED_FEATURE,
            detail::SQL_CONSTRUCT,
        ),
        (
            "SELECT t.a FROM t ORDER BY 3",
            category::SYNTAX_ERROR,
            detail::INVALID_STATEMENT,
        ),
        (
            "SELECT t.a FROM t GROUP BY 0",
            category::SYNTAX_ERROR,
            detail::INVALID_STATEMENT,
        ),
        (
            "SELECT * FROM t ORDER BY 1",
            category::SYNTAX_ERROR,
            detail::INVALID_STATEMENT,
        ),
        (
            "SELECT count(*) FROM t GROUP BY ROLLUP (t.a)",
            category::UNSUPPORTED_FEATURE,
            detail::SQL_CONSTRUCT,
        ),
        (
            "SELECT DISTINCT ON (t.a) t.a FROM t",
            category::UNSUPPORTED_FEATURE,
            detail::SQL_CONSTRUCT,
        ),
        (
            "SELECT t.a FROM t GROUP BY t.a HAVING t.b > 1",
            category::SYNTAX_ERROR,
            detail::INVALID_AGGREGATION,
        ),
        (
            "SELECT t.a FROM t GROUP BY t.a ORDER BY t.b",
            category::SYNTAX_ERROR,
            detail::INVALID_AGGREGATION,
        ),
        (
            "SELECT t.a FROM t ORDER BY t.a ASC NULLS FIRST",
            category::UNSUPPORTED_FEATURE,
            detail::SQL_CONSTRUCT,
        ),
        (
            "SELECT t.a FROM t ORDER BY t.a DESC NULLS LAST",
            category::UNSUPPORTED_FEATURE,
            detail::SQL_CONSTRUCT,
        ),
        (
            "SELECT t.a INTO u FROM t",
            category::UNSUPPORTED_FEATURE,
            detail::SQL_CONSTRUCT,
        ),
        (
            "SELECT t.a FROM t LIMIT 1 FETCH FIRST 2 ROWS ONLY",
            category::UNSUPPORTED_FEATURE,
            detail::SQL_CONSTRUCT,
        ),
        (
            "SELECT t.a FROM t FETCH FIRST 50 PERCENT ROWS ONLY",
            category::UNSUPPORTED_FEATURE,
            detail::SQL_CONSTRUCT,
        ),
        (
            "SELECT t.a FROM t FOR UPDATE",
            category::UNSUPPORTED_FEATURE,
            detail::SQL_CONSTRUCT,
        ),
        (
            "VALUES (1)",
            category::UNSUPPORTED_FEATURE,
            detail::SQL_CONSTRUCT,
        ),
        (
            "(SELECT 1)",
            category::UNSUPPORTED_FEATURE,
            detail::SQL_CONSTRUCT,
        ),
        (
            "SELECT 1 INTERSECT SELECT 1",
            category::UNSUPPORTED_FEATURE,
            detail::UNION,
        ),
        ("", category::SYNTAX_ERROR, detail::UNEXPECTED_END),
        (
            "SELECT FROM t",
            category::SYNTAX_ERROR,
            detail::INVALID_STATEMENT,
        ),
        (
            "SELECT a.b.c.d.e FROM t",
            category::UNSUPPORTED_FEATURE,
            detail::SQL_CONSTRUCT,
        ),
        (
            "SELECT x FROM t, u",
            category::SYNTAX_ERROR,
            detail::AMBIGUOUS_COLUMN,
        ),
        (
            "SELECT x",
            category::SYNTAX_ERROR,
            detail::UNDEFINED_VARIABLE,
        ),
    ] {
        assert_error(sql, expected_category, expected_detail);
    }
    assert_eq!(cypher("SELECT x.y.z FROM y"), "MATCH (y:y) RETURN y.z");
}

#[test]
fn dml_validates_targets_and_columns() {
    assert_eq!(
        cypher("UPDATE Person p NATURAL JOIN Movie m SET m.seen = TRUE WHERE p.name = 'a' RETURNING m.title"),
        "MATCH (p:Person)-->(m:Movie) WHERE p.name = 'a' SET m.seen = TRUE RETURN m.title"
    );
    assert_eq!(
        cypher("INSERT INTO properties (a, b) VALUES (1, 2), (3, 4)"),
        "UNWIND [{a: 1, b: 2}, {a: 3, b: 4}] AS row CREATE (properties:properties) SET properties = row"
    );
    assert_eq!(
        cypher("INSERT INTO Aa_KNOWS_b (Aa.x, b.y) VALUES (1, 2)"),
        "MATCH (_start:Aa {x: 1}), (_end:b {y: 2}) CREATE (_start)-[aa_knows_b:KNOWS]->(_end)"
    );
    assert_eq!(
        cypher("INSERT INTO Person_KNOWS_Person (_start.v$id, _end.v$id) VALUES (1, 2), (3, 4)"),
        "UNWIND [{start: {}, rel: {}, `end`: {}, start_id: 1, end_id: 2}, \
         {start: {}, rel: {}, `end`: {}, start_id: 3, end_id: 4}] AS properties \
         MATCH (_start:Person), (_end:Person) \
         WHERE id(_start) = properties.start_id AND id(_end) = properties.end_id \
         CREATE (_start)-[person_knows_person:KNOWS]->(_end)"
    );
    for (sql, expected_category, expected_detail) in [
        (
            "INSERT INTO t (a, a) VALUES (1, 2)",
            category::SYNTAX_ERROR,
            detail::INVALID_STATEMENT,
        ),
        (
            "INSERT INTO t (a, b) VALUES (1)",
            category::SYNTAX_ERROR,
            detail::INVALID_STATEMENT,
        ),
        (
            "INSERT INTO t (a) SELECT 1",
            category::UNSUPPORTED_FEATURE,
            detail::SQL_CONSTRUCT,
        ),
        (
            "INSERT INTO t (u.a) VALUES (1)",
            category::SYNTAX_ERROR,
            detail::UNDEFINED_VARIABLE,
        ),
        (
            "INSERT INTO t (a) VALUES (t.b)",
            category::SYNTAX_ERROR,
            detail::UNDEFINED_VARIABLE,
        ),
        (
            "INSERT OR REPLACE INTO t (a) VALUES (1)",
            category::UNSUPPORTED_FEATURE,
            detail::MERGE,
        ),
        (
            "REPLACE INTO t (a) VALUES (1)",
            category::UNSUPPORTED_FEATURE,
            detail::MERGE,
        ),
        (
            "INSERT INTO Aa_KNOWS_b (Aa.x, Aa_KNOWS_b.v$id, b.y) VALUES (1, 2, 3)",
            category::SYNTAX_ERROR,
            detail::INVALID_STATEMENT,
        ),
        (
            "INSERT INTO Aa_KNOWS_b (Aa.x, v$nobody_id) VALUES (1, 2)",
            category::SYNTAX_ERROR,
            detail::UNDEFINED_VARIABLE,
        ),
        (
            "INSERT INTO Aa_KNOWS_b (Aa.x, c.y) VALUES (1, 2)",
            category::SYNTAX_ERROR,
            detail::UNDEFINED_VARIABLE,
        ),
        (
            "INSERT INTO Person_KNOWS_Person (Person.x) VALUES (1)",
            category::SYNTAX_ERROR,
            detail::AMBIGUOUS_COLUMN,
        ),
        (
            "INSERT INTO Aa_KNOWS_b (Aa.x) VALUES (1)",
            category::SYNTAX_ERROR,
            detail::INVALID_STATEMENT,
        ),
        (
            "UPDATE t SET v$id = 1",
            category::SYNTAX_ERROR,
            detail::INVALID_STATEMENT,
        ),
        (
            "UPDATE t SET (a, b) = (1, 2)",
            category::UNSUPPORTED_FEATURE,
            detail::SQL_CONSTRUCT,
        ),
        (
            "UPDATE t SET a = 1 FROM u",
            category::UNSUPPORTED_FEATURE,
            detail::SQL_CONSTRUCT,
        ),
        (
            "DELETE FROM t USING u",
            category::UNSUPPORTED_FEATURE,
            detail::SQL_CONSTRUCT,
        ),
        (
            "DELETE FROM t RETURNING t.a",
            category::UNSUPPORTED_FEATURE,
            detail::SQL_CONSTRUCT,
        ),
        (
            "DELETE FROM t, u",
            category::UNSUPPORTED_FEATURE,
            detail::SQL_CONSTRUCT,
        ),
        (
            "TRUNCATE TABLE t, u",
            category::UNSUPPORTED_FEATURE,
            detail::SQL_CONSTRUCT,
        ),
        (
            "TRUNCATE TABLE t CASCADE",
            category::UNSUPPORTED_FEATURE,
            detail::SQL_CONSTRUCT,
        ),
        (
            "DROP TABLE t",
            category::UNSUPPORTED_FEATURE,
            detail::SQL_CONSTRUCT,
        ),
    ] {
        assert_error(sql, expected_category, expected_detail);
    }
}

#[test]
fn requests_decode_named_positional_and_lossless_parameters() {
    let request: Request = serde_json::from_value(serde_json::json!({
        "query": "SELECT ?, ?",
        "parameters": [1, {"$type": "integer", "value": "9223372036854775807"}],
        "query_name": "pair",
    }))
    .unwrap();
    assert_eq!(request.parameters["1"], QueryValue::I64(1));
    assert_eq!(request.parameters["2"], QueryValue::I64(i64::MAX));
    let compiled = request.compile().unwrap();
    assert_eq!(compiled.text(), "RETURN $1, $2");
    assert_eq!(compiled.query_name(), Some("pair"));
    let request: Request =
        serde_json::from_str(r#"{"query":"SELECT :a","parameters":{"a":"x"}}"#).unwrap();
    assert_eq!(request.parameters["a"], QueryValue::String("x".into()));
    for invalid in [
        r#"{"query":"SELECT 1","extra":1}"#,
        r#"{"query":"SELECT 1","parameters":5}"#,
        r#"{"query":"SELECT 1","parameters":[{"$type":"bogus","value":1}]}"#,
        r#"{"query":"SELECT 1","mappings":{"labels":{}}}"#,
    ] {
        assert!(
            serde_json::from_str::<Request>(invalid).is_err(),
            "{invalid}"
        );
    }
    assert_eq!(
        Request::new("SELECT 1 UNION SELECT 2")
            .compile()
            .unwrap_err()
            .detail,
        detail::UNION
    );
    assert_eq!(
        Request::new("SELECT sqrt(1)").compile().unwrap_err().detail,
        detail::FUNCTION
    );
}

proptest! {
    /// Constant-time location lookups agree with walking the text.
    #[test]
    fn locations_convert_to_byte_offsets(text in "[a-cé€😀\n]{0,300}") {
        let lines = super::Lines::new(&text);
        for (number, line) in text.split('\n').enumerate() {
            let start = text.split('\n').take(number).map(|l| l.len() + 1).sum::<usize>();
            for (column, (offset, _)) in line.char_indices().chain([(line.len(), ' ')]).enumerate() {
                let location = sqlparser::tokenizer::Location {
                    line: number as u64 + 1,
                    column: column as u64 + 1,
                };
                prop_assert_eq!(lines.offset(location), Some(start + offset));
            }
        }
    }
}

/// Wide and long statements translate in time linear in their size:
/// lookups, grouping checks and span conversion are indexed.
#[test]
fn wide_statements_translate_in_linear_time() {
    let started = std::time::Instant::now();
    let columns: Vec<String> = (0..4_000).map(|i| format!("t.c{i}")).collect();
    let grouped = format!(
        "SELECT {}, count(*) FROM t GROUP BY {}",
        columns.join(", "),
        columns.join(", ")
    );
    assert!(cypher(&grouped).starts_with("MATCH (t:t) RETURN t.c0, t.c1"));
    let hidden = format!(
        "SELECT count(*) FROM t GROUP BY {} HAVING count(*) > 1 ORDER BY 1",
        columns.join(", ")
    );
    assert!(cypher(&hidden).contains("__group_col_3999"));
    let tables: Vec<String> = (0..4_000).map(|i| format!("t{i}")).collect();
    let joined = format!(
        "SELECT {} FROM {}",
        tables
            .iter()
            .map(|t| format!("{t}.x"))
            .collect::<Vec<_>>()
            .join(", "),
        tables.join(", ")
    );
    assert!(cypher(&joined).starts_with("MATCH (t0:t0), (t1:t1)"));
    // One long line with multibyte text and many nodes.
    let long = format!("SELECT 'é€😀', {}", vec!["1"; 150_000].join(", "));
    assert!(translate(&long, &Mappings::default()).is_ok());
    assert!(
        started.elapsed() < std::time::Duration::from_secs(20),
        "took {:?}",
        started.elapsed()
    );
}
