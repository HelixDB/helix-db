//! Graph values, property storage shapes and mutation errors as observed
//! through the public Cypher API.
use super::{database, run};
use db::cypher;
use helix_planner::relational::ErrorPhase;
use serde_json::json;

fn request(text: &str, parameters: serde_json::Value) -> cypher::Request {
    serde_json::from_value(json!({"query": text, "parameters": parameters})).unwrap()
}

/// Run each request and require the given runtime error category and detail.
async fn assert_runtime_errors(
    db: &db::HelixDB,
    cases: impl IntoIterator<Item = (cypher::Request, &str, &str)>,
) {
    for (request, category, detail) in cases {
        let text = request.query.clone();
        let Err(cypher::Error::Query(error)) = db.cypher(request).await else {
            panic!("{text}: expected a query error");
        };
        assert_eq!(
            (error.category.as_str(), error.detail.as_str(), error.phase),
            (category, detail, ErrorPhase::Runtime),
            "{text}: {error:?}"
        );
    }
}

/// Returned relationships and paths carry their type, endpoints and stored
/// properties, and a path created by CREATE names the entities it created.
#[tokio::test]
async fn relationships_and_paths_serialize_endpoints_and_properties() {
    let db = database().await;
    run(
        &db,
        "CREATE (:Wire {k: 1})-[:LINK {w: 1, tags: ['x']}]->(:Wire {k: 2})",
    )
    .await;
    let ids = run(
        &db,
        "MATCH (a:Wire {k: 1})-[r:LINK]->(b:Wire) RETURN id(a), id(b), id(r)",
    )
    .await;
    let [a, b, r] = [0, 1, 2].map(|column| ids.rows[0][column].as_i64().unwrap().to_string());
    let relationship = json!({
        "$type": "relationship", "id": r, "type": "LINK", "start": a, "end": b,
        "properties": {"w": 1, "tags": ["x"]},
    });
    assert_eq!(
        run(&db, "MATCH ()-[r:LINK]->() RETURN r").await.rows,
        vec![vec![relationship.clone()]]
    );
    assert_eq!(
        run(&db, "MATCH p=(:Wire {k: 1})-[:LINK]->(:Wire) RETURN p")
            .await
            .rows,
        vec![vec![json!({
            "$type": "path",
            "nodes": [
                {"$type": "node", "id": a, "labels": ["Wire"], "properties": {"k": 1}},
                {"$type": "node", "id": b, "labels": ["Wire"], "properties": {"k": 2}},
            ],
            "relationships": [relationship],
        })]]
    );
    let created = run(
        &db,
        "CREATE p=(:Made {k: 1})-[:MADE {w: 2}]->(:Made {k: 2}) RETURN p, length(p)",
    )
    .await;
    let [path, length] = [&created.rows[0][0], &created.rows[0][1]];
    assert_eq!(length, &json!(1));
    assert_eq!(path["$type"], "path");
    let nodes = path["nodes"].as_array().unwrap();
    let relationships = path["relationships"].as_array().unwrap();
    assert_eq!(
        nodes
            .iter()
            .map(|node| &node["properties"])
            .collect::<Vec<_>>(),
        [&json!({"k": 1}), &json!({"k": 2})]
    );
    assert_eq!(relationships.len(), 1);
    assert_eq!(relationships[0]["type"], "MADE");
    assert_eq!(relationships[0]["properties"], json!({"w": 2}));
    assert_eq!(relationships[0]["start"], nodes[0]["id"]);
    assert_eq!(relationships[0]["end"], nodes[1]["id"]);
    // An incoming CREATE pattern stores the relationship from its right node.
    run(&db, "CREATE (:In {k: 1})<-[:BACK]-(:In {k: 2})").await;
    assert_eq!(
        run(&db, "MATCH (x:In)-[:BACK]->(y:In) RETURN x.k, y.k")
            .await
            .rows,
        vec![vec![json!(2), json!(1)]]
    );
    db.close().await.unwrap();
}

/// Infinite and NaN floats cannot be JSON numbers, so they use tagged values,
/// including inside lists.
#[tokio::test]
async fn non_finite_floats_are_tagged_on_the_wire() {
    let db = database().await;
    assert_eq!(
        run(&db, "RETURN 1.0/0.0, -1.0/0.0, [0.0/0.0]").await.rows,
        vec![vec![
            json!({"$type": "float", "value": "Infinity"}),
            json!({"$type": "float", "value": "-Infinity"}),
            json!([{"$type": "float", "value": "NaN"}]),
        ]]
    );
    db.close().await.unwrap();
}

/// Homogeneous float, string, boolean and empty lists are stored and read
/// back unchanged, whether written by literals or by parameters.
#[tokio::test]
async fn homogeneous_lists_round_trip_through_typed_storage() {
    let db = database().await;
    run(
        &db,
        "CREATE (:Lists {floats: [1.5, 2.5], strings: ['a', 'b'], flags: [true, false], empty: []})",
    )
    .await;
    assert_eq!(
        run(
            &db,
            "MATCH (n:Lists) RETURN n.floats, n.strings, n.flags, n.empty, size(n.empty)"
        )
        .await
        .rows,
        vec![vec![
            json!([1.5, 2.5]),
            json!(["a", "b"]),
            json!([true, false]),
            json!([]),
            json!(0)
        ]]
    );
    db.cypher(request(
        "MATCH (n:Lists) SET n.more = $floats, n.names = $names, n.bits = $bits",
        json!({"floats": [0.5], "names": ["c"], "bits": [false]}),
    ))
    .await
    .unwrap();
    assert_eq!(
        run(&db, "MATCH (n:Lists) RETURN n.more, n.names, n.bits")
            .await
            .rows,
        vec![vec![json!([0.5]), json!(["c"]), json!([false])]]
    );
    db.close().await.unwrap();
}

/// Property values outside the storable domain and invalid property names are
/// runtime errors that leave no partial write behind.
#[tokio::test]
async fn invalid_property_values_and_names_fail_without_writing() {
    let db = database().await;
    run(&db, "CREATE (:Target {k: 1})").await;
    let invalid = ("type_error", "invalid_property_type");
    let cases = [
        ("CREATE (:Bad {xs: [1, null]})", json!({}), invalid),
        ("CREATE (:Bad {xs: [1, 'a']})", json!({}), invalid),
        ("CREATE (:Bad {m: {a: 1}})", json!({}), invalid),
        (
            "CREATE (:Bad)-[:BAD {m: {k: 1}}]->(:Bad)",
            json!({}),
            invalid,
        ),
        (
            "CREATE (:Bad {``: 1})",
            json!({}),
            ("unsupported_feature", "empty_property_name"),
        ),
        (
            "MATCH (n:Target) SET n.x = $p",
            json!({"p": [1, "a"]}),
            invalid,
        ),
        (
            "MATCH (n:Target) SET n.x = $p",
            json!({"p": {"a": 1}}),
            invalid,
        ),
        (
            "MATCH (n:Target) SET n += $p",
            json!({"p": {"$x": 1}}),
            ("unsupported_feature", "reserved_property_name"),
        ),
        (
            "MATCH (n:Target) SET n += $p",
            json!({"p": {"": 1}}),
            ("unsupported_feature", "empty_property_name"),
        ),
        (
            "MATCH (n:Target) SET n = $p",
            json!({"p": {"": 1}}),
            ("unsupported_feature", "empty_property_name"),
        ),
    ];
    assert_runtime_errors(
        &db,
        cases.map(|(text, parameters, (category, detail))| {
            (request(text, parameters), category, detail)
        }),
    )
    .await;
    assert_eq!(
        run(&db, "MATCH (n:Bad) RETURN count(n)").await.rows,
        vec![vec![json!(0)]]
    );
    assert_eq!(
        run(&db, "MATCH (n:Target) RETURN properties(n)").await.rows,
        vec![vec![json!({"k": 1})]]
    );
    db.close().await.unwrap();
}

/// CREATE, SET and DELETE reject values that are not the graph entities or
/// maps they require, and roll back without writing.
#[tokio::test]
async fn mutation_targets_of_the_wrong_type_are_runtime_type_errors() {
    let db = database().await;
    run(&db, "CREATE (:T {k: 1})").await;
    let cases = [
        (
            "MATCH (b:T) OPTIONAL MATCH (a:Missing) CREATE (a)-[:R]->(b)",
            json!({}),
            "expected_node",
        ),
        (
            "MATCH (a:T) OPTIONAL MATCH (b:Missing) CREATE (a)-[:R]->(b)",
            json!({}),
            "expected_node",
        ),
        ("MATCH (n:T) SET n += 1", json!({}), "expected_map"),
        ("MATCH (n:T) SET n = $p", json!({"p": [1]}), "expected_map"),
        (
            "WITH $p AS x SET x.a = 1",
            json!({"p": 1}),
            "expected_entity",
        ),
        (
            "WITH $p AS x SET x += {a: 1}",
            json!({"p": "text"}),
            "expected_entity",
        ),
        (
            "WITH $p AS x DELETE x",
            json!({"p": 1}),
            "invalid_argument_type",
        ),
        (
            "UNWIND [1] AS x DELETE x",
            json!({}),
            "invalid_argument_type",
        ),
    ];
    assert_runtime_errors(
        &db,
        cases.map(|(text, parameters, detail)| (request(text, parameters), "type_error", detail)),
    )
    .await;
    assert_eq!(
        run(&db, "MATCH ()-[r]->() RETURN count(r)").await.rows,
        vec![vec![json!(0)]]
    );
    assert_eq!(
        run(&db, "MATCH (n:T) RETURN properties(n)").await.rows,
        vec![vec![json!({"k": 1})]]
    );
    db.close().await.unwrap();
}

/// `SET x += e` and `SET x = e` accept another entity as the property map,
/// while a null or empty map leaves the target unchanged.
#[tokio::test]
async fn map_updates_copy_entity_properties_and_ignore_empty_maps() {
    let db = database().await;
    run(
        &db,
        "CREATE (:Src {k: 1, name: 'src', tags: ['a']}), (:Dst {k: 2, old: true})",
    )
    .await;
    assert_eq!(
        run(
            &db,
            "MATCH (s:Src), (d:Dst) SET d += s RETURN properties(d)"
        )
        .await
        .rows,
        vec![vec![
            json!({"k": 1, "name": "src", "tags": ["a"], "old": true})
        ]]
    );
    assert_eq!(
        run(&db, "MATCH (s:Src), (d:Dst) SET d = s RETURN properties(d)")
            .await
            .rows,
        vec![vec![json!({"k": 1, "name": "src", "tags": ["a"]})]]
    );
    for query in [
        "MATCH (d:Dst) SET d += null RETURN properties(d)",
        "MATCH (d:Dst) SET d += {} RETURN properties(d)",
    ] {
        assert_eq!(
            run(&db, query).await.rows,
            vec![vec![json!({"k": 1, "name": "src", "tags": ["a"]})]],
            "{query}"
        );
    }
    // One value may read labels or a single property and the whole property map.
    for (query, expected) in [
        (
            "MATCH (d:Dst) SET d.width = size(labels(d)) + size(keys(d)) RETURN d.width",
            1 + 3,
        ),
        (
            "MATCH (d:Dst) SET d.depth = d.k + size(keys(d)) RETURN d.depth",
            1 + 4,
        ),
    ] {
        assert_eq!(
            run(&db, query).await.rows,
            vec![vec![json!(expected)]],
            "{query}"
        );
    }
    db.close().await.unwrap();
}

/// A deleted relationship keeps its immutable type for the rest of the
/// statement, but reading a deleted entity's properties or labels fails and
/// rolls back the deletion. Null and unmatched DELETE targets are ignored.
#[tokio::test]
async fn deleted_entities_keep_relationship_types_but_hide_properties() {
    let db = database().await;
    run(
        &db,
        "CREATE (:Del {k: 1})-[:A {w: 1}]->(:Del {k: 2})-[:B {w: 2}]->(:Del {k: 3})",
    )
    .await;
    assert_eq!(
        run(
            &db,
            "MATCH ()-[r]->() DELETE r RETURN type(r) AS t ORDER BY t"
        )
        .await
        .rows,
        vec![vec![json!("A")], vec![json!("B")]]
    );
    assert_eq!(
        run(&db, "MATCH ()-[r]->() RETURN count(r)").await.rows,
        vec![vec![json!(0)]]
    );
    run(
        &db,
        "MATCH (a:Del {k: 1}), (b:Del {k: 2}) CREATE (a)-[:C {w: 3}]->(b)",
    )
    .await;
    let unavailable = ("entity_not_found", "deleted_entity_access");
    assert_runtime_errors(
        &db,
        [
            ("MATCH ()-[r:C]->() DELETE r RETURN r.w", unavailable),
            ("MATCH ()-[r:C]->() DELETE r RETURN r", unavailable),
            (
                "MATCH (n:Del {k: 3}) DELETE n RETURN labels(n)",
                unavailable,
            ),
        ]
        .map(|(text, (category, detail))| (cypher::Request::new(text), category, detail)),
    )
    .await;
    assert_eq!(
        run(
            &db,
            "MATCH (n:Del) OPTIONAL MATCH (n)-[r:C]->() RETURN n.k, r.w ORDER BY n.k"
        )
        .await
        .rows,
        vec![
            vec![json!(1), json!(3)],
            vec![json!(2), json!(null)],
            vec![json!(3), json!(null)]
        ]
    );
    assert_eq!(
        run(
            &db,
            "MATCH (a:Del {k: 1})-[r:C]->() DETACH DELETE a RETURN type(r)"
        )
        .await
        .rows,
        vec![vec![json!("C")]]
    );
    for (query, rows) in [
        ("OPTIONAL MATCH (n:Missing) DELETE n RETURN count(*)", 1),
        (
            "MATCH (n:Del) DELETE CASE WHEN n.k = 99 THEN n END RETURN count(*)",
            2,
        ),
    ] {
        assert_eq!(
            run(&db, query).await.rows,
            vec![vec![json!(rows)]],
            "{query}"
        );
    }
    assert_eq!(
        run(&db, "MATCH (n:Del) RETURN n.k ORDER BY n.k").await.rows,
        vec![vec![json!(2)], vec![json!(3)]]
    );
    db.close().await.unwrap();
}

/// An undirected match sees every relationship twice. With one-row batches
/// the second sighting arrives in a later batch, and each relationship is
/// still deleted once with its type available afterwards.
#[tokio::test]
async fn undirected_relationship_deletes_span_batches() {
    let db = database().await;
    run(
        &db,
        "CREATE (:U {k: 1})-[:UR]->(:U {k: 2})-[:UR]->(:U {k: 3})",
    )
    .await;
    let response = cypher::execute(
        &db,
        cypher::Request::new("MATCH (:U)-[r:UR]-(:U) DELETE r RETURN type(r)"),
        db::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
        db::query_service::QueryMode::Execute,
        db::execution_control::ExecutionControl::unlimited(),
        cypher::Limits {
            batch_rows: 1,
            ..cypher::Limits::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(response.rows, vec![vec![json!("UR")]; 4]);
    assert_eq!(
        run(&db, "MATCH ()-[r]->() RETURN count(r)").await.rows,
        vec![vec![json!(0)]]
    );
    db.close().await.unwrap();
}

/// Natively written collections, maps and 32-bit floats read as Cypher values.
/// Null-valued native properties are absent. Binary and over-nested values
/// stay dormant until a projection observes them.
#[tokio::test]
async fn stored_native_collections_convert_to_cypher_values() {
    use helix_ast::{batch, query, traversal, value};
    use value::PropertyValue as V;
    let db = database().await;
    // A nonempty typed list at the deepest writable level needs one more level
    // of Cypher value nesting than the query value limit allows.
    let deep = V::Object(
        [(
            "inner".to_owned(),
            (0..46).fold(V::I64Array(vec![1]), |inner, _| V::Array(vec![inner])),
        )]
        .into_iter()
        .collect(),
    );
    let object = V::Object(
        [
            ("a".to_owned(), V::I64(1)),
            ("b".to_owned(), V::Array(vec![V::Bool(true), V::Null])),
        ]
        .into_iter()
        .collect(),
    );
    for (label, extra) in [
        ("NativeOk", Vec::new()),
        (
            "NativeBad",
            vec![("bytes", V::Bytes(vec![1, 2])), ("deep", deep)],
        ),
    ] {
        let properties = [
            ("f32", V::F32(1.5)),
            ("f32s", V::F32Array(vec![0.5, 2.0])),
            ("f64s", V::F64Array(vec![0.25])),
            ("strs", V::StringArray(vec!["x".into(), "y".into()])),
            (
                "mixed",
                V::Array(vec![V::I64(1), V::String("s".into()), V::Null]),
            ),
            ("obj", object.clone()),
            ("nothing", V::Null),
        ]
        .into_iter()
        .chain(extra)
        .map(|(name, value)| (name, value::PropertyInput::Value(value)))
        .collect::<Vec<_>>();
        db.query(query::QueryRequest::write(
            batch::write_batch()
                .var_as("n", traversal::g().add_n(label, properties))
                .returning(Vec::<String>::new()),
        ))
        .await
        .unwrap();
    }
    let values = json!({
        "f32": 1.5, "f32s": [0.5, 2.0], "f64s": [0.25], "strs": ["x", "y"],
        "mixed": [1, "s", null], "obj": {"a": 1, "b": [true, null]},
    });
    assert_eq!(
        run(
            &db,
            "MATCH (n:NativeBad) RETURN n.f32, n.f32s, n.f64s, n.strs, n.mixed, n.obj, keys(n)"
        )
        .await
        .rows,
        vec![vec![
            values["f32"].clone(),
            values["f32s"].clone(),
            values["f64s"].clone(),
            values["strs"].clone(),
            values["mixed"].clone(),
            values["obj"].clone(),
            json!(["bytes", "deep", "f32", "f32s", "f64s", "mixed", "obj", "strs"]),
        ]]
    );
    let node = &run(&db, "MATCH (n:NativeOk) RETURN n").await.rows[0][0];
    assert_eq!(node["labels"], json!(["NativeOk"]));
    assert_eq!(node["properties"], values);
    let stored_type = ("unsupported_feature", "stored_value_type");
    assert_runtime_errors(
        &db,
        [
            ("MATCH (n:NativeBad) RETURN n.bytes", stored_type),
            (
                "MATCH (n:NativeBad) RETURN n.deep",
                ("resource_limit", "stored_value_nesting_limit"),
            ),
            ("MATCH (n:NativeBad) RETURN n", stored_type),
            ("MATCH (n:NativeBad) RETURN [n]", stored_type),
            ("MATCH (n:NativeBad) RETURN properties(n)", stored_type),
        ]
        .map(|(text, (category, detail))| (cypher::Request::new(text), category, detail)),
    )
    .await;
    db.close().await.unwrap();
}

/// Unordered DISTINCT windows deterministically keep the smallest equality
/// classes, and SKIP and LIMIT values are validated when the query runs.
#[tokio::test]
async fn distinct_and_ordered_windows_bound_and_validate_rows() {
    let db = database().await;
    for (query, expected) in [
        (
            "UNWIND [5, 4, 3, 2, 1, 5] AS x RETURN DISTINCT x LIMIT 2",
            vec![vec![json!(1)], vec![json!(2)]],
        ),
        (
            "UNWIND [5, 4, 3, 2, 1, 5] AS x RETURN DISTINCT x SKIP 2",
            vec![vec![json!(3)], vec![json!(4)], vec![json!(5)]],
        ),
        (
            "UNWIND range(1, 10) AS x WITH x ORDER BY x DESC WHERE x < 3 RETURN x",
            vec![vec![json!(2)], vec![json!(1)]],
        ),
        // A windowed WITH's WHERE reads its own projected binding.
        (
            "UNWIND range(1, 10) AS x WITH x AS y ORDER BY y LIMIT 3 WHERE y < 5 RETURN y",
            vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]],
        ),
    ] {
        assert_eq!(run(&db, query).await.rows, expected, "{query}");
    }
    let negative = ("syntax_error", "negative_integer_argument");
    assert_runtime_errors(
        &db,
        [
            (
                request(
                    "UNWIND [1, 2] AS x RETURN DISTINCT x LIMIT $n",
                    json!({"n": -1}),
                ),
                negative,
            ),
            (
                request(
                    "UNWIND [1, 2] AS x RETURN x ORDER BY x SKIP $n",
                    json!({"n": -1}),
                ),
                negative,
            ),
            (
                cypher::Request::new(
                    "UNWIND range(1, 5) AS x WITH x ORDER BY x WHERE x / (x - x) > 0 RETURN x",
                ),
                ("arithmetic_error", "division_by_zero"),
            ),
            (
                cypher::Request::new(
                    "UNWIND range(1, 5) AS x WITH x AS y ORDER BY y LIMIT 2 \
                     WHERE y / (y - y) > 0 RETURN y",
                ),
                ("arithmetic_error", "division_by_zero"),
            ),
        ]
        .map(|(request, (category, detail))| (request, category, detail)),
    )
    .await;
    db.close().await.unwrap();
}

/// One projection may read a whole entity, its labels and single properties
/// together, in any order, and every column sees the same stored values.
#[tokio::test]
async fn projections_mix_whole_entities_labels_and_properties() {
    let db = database().await;
    run(&db, "CREATE (:H {k: 1, name: 'a'}), (:H {k: 2, name: 'b'})").await;
    let rows = run(
        &db,
        "MATCH (n:H) RETURN n, n.k, labels(n), properties(n), n.name ORDER BY n.k",
    )
    .await
    .rows;
    let reordered = run(
        &db,
        "MATCH (n:H) RETURN n.name, properties(n), labels(n), n.k, n ORDER BY n.k",
    )
    .await
    .rows;
    assert_eq!(
        rows.iter()
            .map(|row| row.iter().rev().cloned().collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        reordered
    );
    assert_eq!(
        rows.iter().map(|row| row[1..].to_vec()).collect::<Vec<_>>(),
        vec![
            vec![
                json!(1),
                json!(["H"]),
                json!({"k": 1, "name": "a"}),
                json!("a")
            ],
            vec![
                json!(2),
                json!(["H"]),
                json!({"k": 2, "name": "b"}),
                json!("b")
            ],
        ]
    );
    assert!(rows.iter().all(|row| row[0]["properties"] == row[3]));
    // Each node is returned in several rows of one output batch.
    let pairs = run(&db, "MATCH (a:H), (b:H) RETURN a.k, b ORDER BY a.k, b.k")
        .await
        .rows;
    assert_eq!(
        pairs
            .iter()
            .map(|row| (row[0].clone(), row[1]["properties"]["k"].clone()))
            .collect::<Vec<_>>(),
        [(1, 1), (1, 2), (2, 1), (2, 2)].map(|(a, b)| (json!(a), json!(b)))
    );
    assert_eq!(pairs[0][1], rows[0][0]);
    assert_eq!(pairs[3][1], rows[1][0]);
    db.close().await.unwrap();
}

/// A second UNWIND streams into an aggregate while expanding every input row,
/// including rows whose list reads stored properties.
#[tokio::test]
async fn nested_unwinds_expand_every_input_row() {
    let db = database().await;
    run(&db, "CREATE (:Nest {k: 1}), (:Nest {k: 2}), (:Nest {k: 3})").await;
    for (query, expected) in [
        (
            "UNWIND [1, 2] AS a UNWIND [a, a * 10] AS b RETURN count(*), sum(b)",
            vec![json!(4), json!(33)],
        ),
        (
            "MATCH (n:Nest) UNWIND [n.k, n.k * 10] AS b RETURN count(*), sum(b)",
            vec![json!(6), json!(66)],
        ),
    ] {
        assert_eq!(run(&db, query).await.rows, vec![expected], "{query}");
    }
    db.close().await.unwrap();
}

/// Aggregation orders groups by grouping expressions written in terms of the
/// input, and grouping by a list beyond the structural value limit is a
/// runtime resource error.
#[tokio::test]
async fn aggregation_keys_order_by_inputs_and_reject_oversized_lists() {
    let db = database().await;
    assert_eq!(
        run(
            &db,
            "UNWIND [1, 2, 2, 3] AS x RETURN x % 2 AS parity, count(*) AS c ORDER BY x % 2 DESC"
        )
        .await
        .rows,
        vec![vec![json!(1), json!(2)], vec![json!(0), json!(2)]]
    );
    assert_runtime_errors(
        &db,
        [(
            cypher::Request::new("WITH range(1, 200000) AS xs RETURN xs, count(*)"),
            "resource_limit",
            "value_depth",
        )],
    )
    .await;
    db.close().await.unwrap();
}
