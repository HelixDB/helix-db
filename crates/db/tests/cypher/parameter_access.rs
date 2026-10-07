use super::membership::create_index;
use super::{database, run};
use db::cypher;
use helix_ast::index;
use serde_json::json;

fn request(text: &str, parameters: &serde_json::Value) -> cypher::Request {
    serde_json::from_value(json!({"query": text, "parameters": parameters})).unwrap()
}

/// A property or element of a bound parameter, such as `$p.uid` or `$ids[1]`,
/// reads an index as the parameter itself does. Each result and error equals
/// the same query on a database without indexes.
#[tokio::test]
async fn parameter_properties_and_elements_read_indexes_with_scan_semantics() {
    let indexed = database().await;
    let scanned = database().await;
    for db in [&indexed, &scanned] {
        run(
            db,
            "UNWIND range(0, 199) AS i CREATE (:User {uid: i, tier: i % 3, age: i % 50})",
        )
        .await;
    }
    create_index(
        &indexed,
        index::IndexSpec::node_unique_equality("User", "uid"),
    )
    .await;
    create_index(&indexed, index::IndexSpec::node_equality("User", "tier")).await;
    create_index(&indexed, index::IndexSpec::node_range("User", "age")).await;
    let parameters = json!({
        "p": {"uid": 42, "uids": [3, 4, null], "min": 47, "flag": true, "none": null},
        "ids": [5, 6, 7],
        "s": 5,
    });
    for query in [
        "MATCH (n:User {uid: $p.uid}) RETURN n.uid",
        "MATCH (n:User {uid: $p['uid']}) RETURN n.uid",
        "MATCH (n:User) WHERE n.uid = $ids[1] RETURN n.uid",
        "MATCH (n:User) WHERE n.uid = $ids[-1] RETURN n.uid",
        "MATCH (n:User) WHERE n.uid = $ids[9] RETURN n.uid",
        "MATCH (n:User) WHERE n.uid = $p.missing RETURN n.uid",
        "MATCH (n:User) WHERE n.uid = $p.none.x RETURN n.uid",
        "MATCH (n:User) WHERE n.uid IN $p.uids RETURN n.uid ORDER BY n.uid",
        "MATCH (n:User) WHERE n.uid = $p.uid OR n.uid = $ids[0] RETURN n.uid ORDER BY n.uid",
        "MATCH (n:User) WHERE n.age > $p.min AND n.tier = 0 RETURN n.uid ORDER BY n.uid",
        "MATCH (n:User) WHERE $p.flag AND n.uid = 9 RETURN n.uid",
    ] {
        let direct = indexed.cypher(request(query, &parameters)).await.unwrap();
        let reference = scanned.cypher(request(query, &parameters)).await.unwrap();
        assert_eq!(direct.rows, reference.rows, "{query}");
        assert!(
            direct.resources.reads.point_gets + direct.resources.reads.multi_get_keys < 100,
            "{query}: {:?}",
            direct.resources.reads
        );
    }
    // Access that fails for a non-map or non-list value still fails.
    for query in [
        "MATCH (n:User {uid: $s.uid}) RETURN n.uid",
        "MATCH (n:User) WHERE n.uid = $p[0] RETURN n.uid",
    ] {
        let direct = indexed.cypher(request(query, &parameters)).await;
        let reference = scanned.cypher(request(query, &parameters)).await;
        assert_eq!(
            direct
                .map(|response| response.rows)
                .map_err(|error| error.to_string()),
            reference
                .map(|response| response.rows)
                .map_err(|error| error.to_string()),
            "{query}"
        );
    }
    indexed.close().await.unwrap();
    scanned.close().await.unwrap();
}
