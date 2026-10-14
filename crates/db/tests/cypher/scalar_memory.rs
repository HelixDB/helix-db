use super::{database, run};
use db::{
    cypher, encoding::v2::keys::scope::DataScope, execution_control::ExecutionControl,
    query_service::QueryMode,
};
use helix_ast::query;
use serde_json::json;
use std::collections::BTreeMap;

#[tokio::test]
async fn scalar_temporary_peaks_cover_lists_parameters_and_properties() {
    let db = database().await;
    let execute = |text: &str, parameters: BTreeMap<String, query::QueryValue>, memory_bytes| {
        let mut request = cypher::Request::new(text);
        request.parameters = parameters;
        cypher::execute(
            &db,
            request,
            DataScope::LegacyUnscoped,
            QueryMode::Execute,
            ExecutionControl::unlimited(),
            cypher::Limits {
                memory_bytes,
                ..Default::default()
            },
        )
    };
    for text in [
        "RETURN size(range(1,10000)) AS size",
        "RETURN size(reverse(range(1,10000))) AS size",
    ] {
        let result = execute(text, BTreeMap::new(), 16 * 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(result.rows, vec![vec![json!(10000)]]);
        // Replaying a deterministic scalar query must fit its reported bound.
        // Do not require a particular materialization strategy or byte count.
        assert_eq!(
            execute(text, BTreeMap::new(), result.resources.peak_memory_bytes)
                .await
                .unwrap()
                .rows,
            result.rows
        );
    }
    let parameters =
        BTreeMap::from([("text".into(), query::QueryValue::String("x".repeat(10000)))]);
    let simple = execute(
        "RETURN size($text) AS size",
        parameters.clone(),
        16 * 1024 * 1024,
    )
    .await
    .unwrap();
    let converted = execute(
        "RETURN size(toUpper($text)) AS size",
        parameters.clone(),
        16 * 1024 * 1024,
    )
    .await
    .unwrap();
    assert_eq!(converted.rows, simple.rows);
    assert_eq!(
        execute(
            "RETURN size(toUpper($text)) AS size",
            parameters.clone(),
            converted.resources.peak_memory_bytes
        )
        .await
        .unwrap()
        .rows,
        simple.rows
    );
    execute(
        "CREATE (:PeakValue {text:$text})",
        parameters,
        16 * 1024 * 1024,
    )
    .await
    .unwrap();
    let simple = execute(
        "MATCH (n:PeakValue) RETURN size(n.text) AS size",
        BTreeMap::new(),
        16 * 1024 * 1024,
    )
    .await
    .unwrap();
    let converted = execute(
        "MATCH (n:PeakValue) RETURN size(toUpper(n.text)) AS size",
        BTreeMap::new(),
        16 * 1024 * 1024,
    )
    .await
    .unwrap();
    assert_eq!(converted.rows, simple.rows);
    assert_eq!(
        execute(
            "MATCH (n:PeakValue) RETURN size(toUpper(n.text)) AS size",
            BTreeMap::new(),
            converted.resources.peak_memory_bytes
        )
        .await
        .unwrap()
        .rows,
        simple.rows
    );
    assert_eq!(
        run(&db, "MATCH (n:PeakValue) RETURN size(n.text)")
            .await
            .rows,
        vec![vec![json!(10000)]]
    );
    db.close().await.unwrap();
}
