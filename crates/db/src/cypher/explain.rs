//! Planning-only diagnostics. This path never opens an execution transaction,
//! mutates graph data, flushes durability, or returns query-result rows.
use super::*;

#[derive(Debug)]
pub struct Explanation {
    plan: r::RowPlan,
}
impl Explanation {
    pub fn plan(&self) -> &r::RowPlan {
        &self.plan
    }
}
impl Serialize for Explanation {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        self.plan.explain().serialize(serializer)
    }
}

impl HelixDB {
    /// Plan a Cypher statement without executing it, including write statements.
    pub async fn explain_cypher(&self, request: Request) -> Result<Explanation> {
        explain(
            self,
            request,
            DataScope::LegacyUnscoped,
            ExecutionControl::from_timeout(std::time::Duration::from_secs(30)),
            Limits::default(),
        )
        .await
    }
}

/// Use the same scoped catalog, parameter validation, and planning limits as
/// execution. Explaining a write is safe on a reader because it does not run.
pub async fn explain(
    db: &HelixDB,
    request: Request,
    scope: DataScope,
    control: ExecutionControl,
    limits: Limits,
) -> Result<Explanation> {
    control.check()?;
    let request = prepare_request(request, limits)?;
    let prepared = control
        .run(db.planner_context_scoped_prepared(request.params, scope))
        .await?;
    control.check()?;
    let plan = r::plan(request.query, prepared.context())?;
    control.check()?;
    let explanation = Explanation { plan };
    // Count encoded bytes without allocating the response buffer. A plan can
    // repeat literals in several selected contracts, so query length alone is
    // insufficient to bound diagnostics.
    struct SizeLimit {
        bytes: usize,
        limit: usize,
    }
    impl std::io::Write for SizeLimit {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.bytes = self.bytes.saturating_add(bytes.len());
            if self.bytes > self.limit {
                return Err(std::io::Error::other(
                    "explain response exceeds its byte budget",
                ));
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut size = SizeLimit {
        bytes: 0,
        limit: limits.result_bytes.min(limits.memory_bytes),
    };
    if let Err(error) = serde_json::to_writer(&mut size, &explanation) {
        if size.bytes > size.limit {
            return Err(r::QueryError::runtime(
                "ResourceLimit",
                "ResultLimit",
                "explain response exceeds its byte budget",
            )
            .into());
        }
        return Err(error.into());
    }
    Ok(explanation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn explanation_does_not_execute_and_shows_only_selected_accesses() {
        let source = crate::HelixDbSource::InMemory {
            database: "cypher-explanation".into(),
        };
        let config = source
            .embedded_default_config()
            .with_query_telemetry(crate::config::QueryTelemetry::Disabled);
        let db = HelixDB::open_with_config(source, config).await.unwrap();
        db.install_index_for_tests(
            crate::config::SecondaryIndexDefinition::node_equality("N", "key")
                .unwrap()
                .try_into()
                .unwrap(),
        )
        .await
        .unwrap();
        let write = db
            .explain_cypher(Request::new("CREATE (:N {key:7})"))
            .await
            .unwrap();
        assert_eq!(write.plan().query().effect(), r::Effect::Write);
        let serialized = serde_json::to_value(&write).unwrap();
        assert!(serialized.get("columns").is_none());
        assert!(serialized.get("rows").is_none());
        assert_eq!(
            serialized["operators"][0]["blocking"],
            json!(["input_before_mutation"])
        );
        assert_eq!(
            db.cypher(Request::new("MATCH (n:N) RETURN count(*)"))
                .await
                .unwrap()
                .rows,
            vec![vec![json!(0)]]
        );
        db.cypher(Request::new(
            "UNWIND range(0,63) AS key CREATE (:N {key:key})",
        ))
        .await
        .unwrap();
        // Planning must not evaluate runtime arithmetic, even when it will fail.
        db.explain_cypher(Request::new("RETURN 1/0")).await.unwrap();
        let connected = db
            .explain_cypher(Request::new("MATCH (a:N)-[:R]->(b:N) RETURN count(*)"))
            .await
            .unwrap();
        let report = serde_json::to_value(&connected).unwrap();
        assert_eq!(
            report["operators"][0]["graph"]["sources"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(report["operators"][1]["blocking"], json!(["aggregation"]));
        let indexed = db
            .explain_cypher(Request::new(
                "WITH 7 AS key MATCH (n:N {key:key}) RETURN n.key",
            ))
            .await
            .unwrap();
        let report = serde_json::to_value(&indexed).unwrap();
        let access = &report["operators"][1]["graph"];
        assert!(access["steps"][0].get("IndexLookup").is_some());
        assert!(access["sources"].as_array().unwrap().is_empty());
        let joined = db
            .explain_cypher(Request::new(
                "MATCH (a:N),(b:N) WHERE a.key=b.key RETURN a.key",
            ))
            .await
            .unwrap();
        let report = serde_json::to_value(&joined).unwrap();
        assert_eq!(
            report["operators"][0]["blocking"],
            json!(["hash_build", "materialized_relation"])
        );
        let product = db
            .explain_cypher(Request::new(
                "MATCH (a:N),(b:N) RETURN DISTINCT a,b ORDER BY a",
            ))
            .await
            .unwrap();
        let report = serde_json::to_value(&product).unwrap();
        assert!(report["notices"]
            .as_array()
            .unwrap()
            .iter()
            .any(|notice| notice["kind"] == "cartesian_product"));
        assert_eq!(
            report["operators"][1]["blocking"],
            json!(["distinct", "ordering", "materialized_relation"])
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn explanation_preserves_parameter_deadline_and_response_limits() {
        let source = crate::HelixDbSource::InMemory {
            database: "cypher-explanation-limits".into(),
        };
        let config = source
            .embedded_default_config()
            .with_query_telemetry(crate::config::QueryTelemetry::Disabled);
        let db = HelixDB::open_with_config(source, config).await.unwrap();
        let error = db
            .explain_cypher(Request::new("RETURN $missing"))
            .await
            .unwrap_err();
        assert!(matches!(error,Error::Query(error) if error.detail=="MissingParameter"));
        let request: Request =
            serde_json::from_value(json!({"query":"RETURN $n","parameters":{"n":7}})).unwrap();
        db.explain_cypher(request.clone()).await.unwrap();
        let error = explain(
            &db,
            request.clone(),
            DataScope::LegacyUnscoped,
            ExecutionControl::unlimited(),
            Limits {
                result_bytes: 16,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error,Error::Query(error) if error.detail=="ResultLimit"));
        let error = explain(
            &db,
            request.clone(),
            DataScope::LegacyUnscoped,
            ExecutionControl::unlimited(),
            Limits {
                batch_rows: 0,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error,Error::Query(error) if error.detail=="InvalidLimits"));
        let error = explain(
            &db,
            request,
            DataScope::LegacyUnscoped,
            ExecutionControl::from_timeout(std::time::Duration::ZERO),
            Limits::default(),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            Error::Storage(HelixDbError::QueryDeadlineExceeded)
        ));
        db.close().await.unwrap();
    }
}
