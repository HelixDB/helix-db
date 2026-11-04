//! SQL statements executed end to end through the query service, as
//! `POST /v2/sql` runs them: translation, routing type, parameters, results,
//! writes and errors.
use super::database;
use db::cypher;
use db::encoding::v2::keys::scope::DataScope;
use db::execution_control::ExecutionControl;
use db::query_service::{HelixQueryService, QueryMode};
use helix_ast::query::QueryRequestType;
use helix_cypher::sql::request::Request;
use helix_planner::relational::{ErrorPhase, Span};
use serde_json::{json, Value};
use std::sync::Arc;

async fn execute(service: &HelixQueryService, body: Value) -> cypher::Result<Value> {
    let request: Request = serde_json::from_value(body).expect("a valid SQL request");
    let compiled = request.compile()?;
    let response = service
        .execute_compiled_cypher_json_scoped_controlled(
            compiled,
            QueryMode::Execute,
            DataScope::LegacyUnscoped,
            ExecutionControl::unlimited(),
            cypher::Limits::default(),
            None,
        )
        .await?;
    Ok(serde_json::from_slice(response.body()).expect("a JSON body"))
}

async fn sql(service: &HelixQueryService, query: &str, parameters: Value) -> Value {
    execute(service, json!({"query": query, "parameters": parameters}))
        .await
        .unwrap_or_else(|error| panic!("{query}: {error:?}"))
}

async fn rows(service: &HelixQueryService, query: &str, parameters: Value) -> Value {
    sql(service, query, parameters).await["rows"].clone()
}

async fn query_error(service: &HelixQueryService, query: &str) -> helix_cypher::QueryError {
    match execute(service, json!({"query": query})).await {
        Err(cypher::Error::Query(error)) => error,
        other => panic!("{query}: expected a query error, got {other:?}"),
    }
}

async fn movies() -> HelixQueryService {
    let service = HelixQueryService::new(Arc::new(database().await));
    for (query, parameters) in [
        (
            "INSERT INTO Person (name, born, dept) VALUES ('Ada', 1815, 'math'), \
             ('Grace', 1906, 'navy'), ('Alan', 1912, 'math')",
            json!({}),
        ),
        (
            "INSERT INTO Movie (title, released) VALUES (?, ?)",
            json!(["Hidden", 2016]),
        ),
        (
            "INSERT INTO Movie (title, released) VALUES ('Enigma', 2014)",
            json!({}),
        ),
        (
            "INSERT INTO Person_ACTED_IN_Movie (Person.name, Movie.title, role) VALUES \
             ('Ada', 'Hidden', 'lead'), ('Alan', 'Enigma', 'self'), ('Grace', 'Hidden', 'cameo')",
            json!({}),
        ),
    ] {
        sql(&service, query, parameters).await;
    }
    service
}

/// Reads see SQL's rows and columns: unqualified columns keep their names,
/// other entries are named as written, and `?` binds in written order.
#[tokio::test]
async fn selects_return_sql_shaped_results() {
    let service = movies().await;
    assert_eq!(
        sql(
            &service,
            "SELECT name, born FROM Person p WHERE born > ? ORDER BY born DESC",
            json!([1900]),
        )
        .await,
        json!({"columns": ["name", "born"], "rows": [["Alan", 1912], ["Grace", 1906]]})
    );
    assert_eq!(
        sql(
            &service,
            "SELECT ?, p.name, COUNT(*) FROM Person p WHERE p.name = ? GROUP BY p.name",
            json!(["x", "Grace"]),
        )
        .await,
        json!({"columns": ["?", "p.name", "COUNT(*)"], "rows": [["x", "Grace", 1]]})
    );
    assert_eq!(
        rows(
            &service,
            "SELECT p.* FROM Person p WHERE p.name = :who",
            json!({"who": "Ada"})
        )
        .await,
        json!([[{"name": "Ada", "born": 1815, "dept": "math"}]])
    );
    let node = rows(
        &service,
        "SELECT p FROM Person p WHERE p.name = 'Ada'",
        json!({}),
    )
    .await;
    assert_eq!(node[0][0]["$type"], "node");
    assert_eq!(node[0][0]["labels"], json!(["Person"]));
    assert_eq!(
        rows(
            &service,
            "SELECT name, CASE WHEN born < 1900 THEN 'old' ELSE 'new' END AS era, \
             coalesce(missing, 'none') AS fallback, CAST(born AS varchar) AS text, \
             (born, name) < (1906, 'Z') AS earlier, substring(name, 2, 2) AS middle \
             FROM Person p \
             WHERE name LIKE 'A%' AND born BETWEEN 1800 AND 1950 AND dept IN ('math') \
             ORDER BY name",
            json!({}),
        )
        .await,
        json!([
            ["Ada", "old", "none", "1815", true, "da"],
            ["Alan", "new", "none", "1912", false, "la"]
        ])
    );
    assert_eq!(
        rows(&service, "SELECT 1, 1, 'a' || 'b' AS ab", json!({})).await,
        json!([[1, 1, "ab"]])
    );
    // Concatenation joins strings, as in SQL, and CONCAT skips nulls.
    assert_eq!(
        rows(
            &service,
            "SELECT p.born || p.name, CONCAT(p.born, p.missing, '!'), p.missing || 'x' \
             FROM Person p WHERE p.name = 'Ada'",
            json!({}),
        )
        .await,
        json!([["1815Ada", "1815!", null]])
    );
    assert_eq!(
        sql(&service, "SELECT 1, 1", json!({})).await["columns"],
        json!(["1", "1_2"])
    );
}

/// Joins read relationships, through a relationship table in the middle of
/// a chain or a relationship table on its own.
#[tokio::test]
async fn joins_read_relationships() {
    let service = movies().await;
    let expected = json!([
        ["Ada", "Hidden", "lead"],
        ["Alan", "Enigma", "self"],
        ["Grace", "Hidden", "cameo"]
    ]);
    assert_eq!(
        rows(
            &service,
            "SELECT p.name, m.title, r.role FROM Person p \
             JOIN Person_ACTED_IN_Movie r ON r.person = p.id \
             JOIN Movie m ON m.id = r.movie ORDER BY p.name",
            json!({}),
        )
        .await,
        expected
    );
    assert_eq!(
        rows(
            &service,
            "SELECT p.name, m.title, r.role FROM Person p \
             NATURAL JOIN ACTED_IN r NATURAL JOIN Movie m ORDER BY p.name",
            json!({}),
        )
        .await,
        expected
    );
    assert_eq!(
        rows(
            &service,
            "SELECT person_name, movie_title, role FROM Person_ACTED_IN_Movie \
             WHERE role <> 'self' ORDER BY person_name",
            json!({}),
        )
        .await,
        json!([["Ada", "Hidden", "lead"], ["Grace", "Hidden", "cameo"]])
    );
    // A comma join is a cartesian product.
    assert_eq!(
        rows(
            &service,
            "SELECT count(*) FROM Person p, Movie m",
            json!({})
        )
        .await,
        json!([[6]])
    );
}

/// Grouping matches SQL, including keys that are not selected, HAVING,
/// functions of keys and positional references.
#[tokio::test]
async fn grouping_matches_sql() {
    let service = movies().await;
    for (query, expected) in [
        (
            "SELECT m.title, count(*) AS actors FROM Person p NATURAL JOIN ACTED_IN r \
             NATURAL JOIN Movie m GROUP BY m.title ORDER BY actors DESC, m.title",
            json!([["Hidden", 2], ["Enigma", 1]]),
        ),
        (
            "SELECT dept, count(*) AS n FROM Person p GROUP BY dept HAVING count(*) > 1",
            json!([["math", 2]]),
        ),
        (
            "SELECT count(*) FROM Person p GROUP BY dept ORDER BY 1",
            json!([[1], [2]]),
        ),
        (
            "SELECT upper(dept), count(*), min(born) FROM Person p GROUP BY dept ORDER BY 1",
            json!([["MATH", 2, 1815], ["NAVY", 1, 1906]]),
        ),
        (
            "SELECT DISTINCT dept FROM Person p ORDER BY dept DESC",
            json!([["navy"], ["math"]]),
        ),
        // GROUP BY without an aggregate returns each group once.
        (
            "SELECT dept FROM Person p GROUP BY dept ORDER BY dept",
            json!([["math"], ["navy"]]),
        ),
        (
            "SELECT p.dept FROM Person p GROUP BY p.dept, p.born HAVING p.born > 1900 ORDER BY 1",
            json!([["math"], ["navy"]]),
        ),
        // An aggregate without GROUP BY returns one row, even for no input.
        (
            "SELECT 'total', count(*), sum(n.x) FROM Nobody n",
            json!([["total", 0, null]]),
        ),
        (
            "SELECT count(DISTINCT p.dept), sum(p.born), avg(m.released) FROM Person p, Movie m",
            json!([[2, 11266, 2015.0]]),
        ),
    ] {
        assert_eq!(rows(&service, query, json!({})).await, expected, "{query}");
    }
}

/// Writes run as one transaction each: UPDATE and DELETE on nodes and
/// relationships, connected-node deletes and TRUNCATE fail atomically, and
/// TRUNCATE .. CASCADE detaches.
#[tokio::test]
async fn writes_update_and_delete_nodes_and_relationships() {
    let service = movies().await;
    sql(
        &service,
        "UPDATE Person SET born = born + 1 WHERE name = :who",
        json!({"who": "Ada"}),
    )
    .await;
    sql(
        &service,
        "UPDATE Person_ACTED_IN_Movie SET role = 'star' WHERE Person.name = 'Ada'",
        json!({}),
    )
    .await;
    // Every new value comes from the row before the update.
    sql(
        &service,
        "UPDATE Person p SET born = p.dept, dept = p.born WHERE p.name = 'Grace'",
        json!({}),
    )
    .await;
    assert_eq!(
        rows(
            &service,
            "SELECT born, dept FROM Person p WHERE name = 'Grace'",
            json!({})
        )
        .await,
        json!([["navy", 1906]])
    );
    assert_eq!(
        rows(
            &service,
            "SELECT p.born, r.role FROM Person p NATURAL JOIN ACTED_IN r NATURAL JOIN Movie m \
             WHERE p.name = 'Ada'",
            json!({}),
        )
        .await,
        json!([[1816, "star"]])
    );
    let error = execute(
        &service,
        json!({"query": "DELETE FROM Person WHERE name = 'Ada'"}),
    )
    .await
    .unwrap_err();
    let cypher::Error::Query(error) = error else {
        panic!("expected a query error: {error:?}");
    };
    assert_eq!(
        (error.category.as_str(), error.detail.as_str(), error.phase),
        (
            "constraint_verification_failed",
            "delete_connected_node",
            ErrorPhase::Runtime
        )
    );
    sql(
        &service,
        "DELETE FROM Person_ACTED_IN_Movie WHERE Person.name = 'Ada'",
        json!({}),
    )
    .await;
    sql(&service, "DELETE FROM Person WHERE name = 'Ada'", json!({})).await;
    assert_eq!(
        rows(&service, "SELECT count(*) FROM Person p", json!({})).await,
        json!([[2]])
    );
    let error = execute(&service, json!({"query": "TRUNCATE TABLE Movie"}))
        .await
        .unwrap_err();
    let cypher::Error::Query(error) = error else {
        panic!("expected a query error: {error:?}");
    };
    assert_eq!(error.detail, "delete_connected_node");
    assert_eq!(
        rows(&service, "SELECT count(*) FROM Movie m", json!({})).await,
        json!([[2]])
    );
    sql(&service, "TRUNCATE TABLE Movie CASCADE", json!({})).await;
    for query in [
        "SELECT count(*) FROM Movie m",
        "SELECT count(*) FROM Person_ACTED_IN_Movie",
    ] {
        assert_eq!(
            rows(&service, query, json!({})).await,
            json!([[0]]),
            "{query}"
        );
    }
    assert_eq!(
        rows(&service, "SELECT count(*) FROM Person p", json!({})).await,
        json!([[2]])
    );
}

/// A relationship insert finds its endpoints by id, connects every pair it
/// matches, and creates nothing when an endpoint is missing.
#[tokio::test]
async fn relationship_inserts_connect_existing_nodes() {
    let service = movies().await;
    let ids = rows(
        &service,
        "SELECT name, v$id FROM Person p WHERE name IN ('Grace', 'Alan') ORDER BY name",
        json!({}),
    )
    .await;
    let (alan, grace) = (ids[0][1].clone(), ids[1][1].clone());
    let inserted = sql(
        &service,
        "INSERT INTO Person_KNOWS_Person (_start.v$id, _end.v$id, since) VALUES (?, ?, 1950) \
         RETURNING person_knows_person.since",
        json!([grace, alan]),
    )
    .await;
    assert_eq!(inserted["rows"], json!([[1950]]));
    sql(
        &service,
        "INSERT INTO Person_KNOWS_Person (_start.name, _end.name) VALUES ('Nobody', 'Alan')",
        json!({}),
    )
    .await;
    assert_eq!(
        rows(
            &service,
            "SELECT a.name, b.name, k.since FROM Person a \
             JOIN Person_KNOWS_Person k ON k.a = a.id JOIN Person b ON b.id = k.b",
            json!({}),
        )
        .await,
        json!([["Grace", "Alan", 1950]])
    );
    let error = query_error(
        &service,
        "INSERT INTO Person_KNOWS_Person (v$person_id, since) VALUES (1, 2)",
    )
    .await;
    assert_eq!(error.detail, "ambiguous_column");
}

/// Requests carry their routing type, and errors use the Cypher contract
/// with spans in the SQL text.
#[tokio::test]
async fn requests_route_and_report_like_cypher() {
    let service = movies().await;
    let compile = |query: &str| Request::new(query).compile().unwrap();
    assert_eq!(compile("SELECT 1").request_type(), QueryRequestType::Read);
    for write in [
        "INSERT INTO A (x) VALUES (1)",
        "UPDATE A SET x = 1",
        "DELETE FROM A",
        "TRUNCATE TABLE A",
    ] {
        assert_eq!(
            compile(write).request_type(),
            QueryRequestType::Write,
            "{write}"
        );
    }
    assert_eq!(
        compile("SELECT p.name FROM Person p WHERE p.born > ?").text(),
        "MATCH (p:Person) WHERE p.born > $1 RETURN p.name"
    );

    // sqlparser spans a call from its name to its last argument.
    let error = query_error(&service, "SELECT sqrt(born) FROM Person p").await;
    assert_eq!(
        (error.category.as_str(), error.detail.as_str(), error.span),
        (
            "unsupported_feature",
            "function",
            Some(Span { start: 7, end: 16 })
        )
    );
    let error = query_error(&service, "SELECT name FROM Person p WHERE name = ?").await;
    assert_eq!(
        (error.category.as_str(), error.detail.as_str()),
        ("parameter_missing", "missing_parameter")
    );
    let error = query_error(&service, "SELECT name FROM Person p, Movie m").await;
    assert_eq!(
        (error.category.as_str(), error.detail.as_str(), error.span),
        (
            "syntax_error",
            "ambiguous_column",
            Some(Span { start: 7, end: 11 })
        )
    );
    let error = query_error(&service, "SELECT FROM WHERE").await;
    assert_eq!(error.category, "syntax_error");
}
