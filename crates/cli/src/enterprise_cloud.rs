use eyre::{eyre, Result};
use reqwest::Client;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::BTreeMap;

pub fn cloud_base_url() -> String {
    crate::service_endpoints::url(crate::service_endpoints::ServiceEndpoint::Cloud)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliWorkspace {
    pub id: String,
    pub name: String,
    pub url_slug: String,
    #[serde(default = "default_workspace_type")]
    pub workspace_type: String,
}

fn default_workspace_type() -> String {
    "organization".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliProject {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliProjectDetails {
    pub id: String,
    pub name: String,
    pub workspace_id: String,
    pub workspace_name: String,
    pub workspace_slug: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliProjectClusters {
    pub project_id: String,
    pub project_name: String,
    #[serde(default)]
    pub enterprise: Vec<CliEnterpriseCluster>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliWorkspaceClusters {
    #[serde(default)]
    pub enterprise: Vec<CliEnterpriseCluster>,
}

/// A per-backend index snapshot. Each index entry is a `[label, property]`
/// tuple. Unknown sibling keys are captured in `extra` so future schema
/// additions are not silently dropped.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CliIndexSnapshot {
    #[serde(default)]
    pub node_label_index: bool,
    #[serde(default)]
    pub edge_label_index: bool,
    #[serde(default)]
    pub node_equality_indexes: Vec<(String, String)>,
    #[serde(default)]
    pub node_range_indexes: Vec<(String, String)>,
    #[serde(default)]
    pub node_range_desc_indexes: Vec<(String, String)>,
    #[serde(default)]
    pub node_text_indexes: Vec<(String, String)>,
    #[serde(default)]
    pub node_vector_indexes: Vec<(String, String)>,
    #[serde(default)]
    pub edge_equality_indexes: Vec<(String, String)>,
    #[serde(default)]
    pub edge_range_indexes: Vec<(String, String)>,
    #[serde(default)]
    pub edge_range_desc_indexes: Vec<(String, String)>,
    #[serde(default)]
    pub edge_text_indexes: Vec<(String, String)>,
    #[serde(default)]
    pub edge_vector_indexes: Vec<(String, String)>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliClusterBackend {
    #[serde(default)]
    pub pod: String,
    #[serde(default)]
    pub snapshot: CliIndexSnapshot,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CliClusterIndexes {
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub backends: Vec<CliClusterBackend>,
    #[serde(default)]
    pub readable_backends: Vec<String>,
    /// Authoritative backend; only its `pod` field is needed for display.
    #[serde(default)]
    pub writer_backend: Option<serde_json::Value>,
    #[serde(default)]
    pub errors: Vec<serde_json::Value>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

impl CliClusterIndexes {
    /// The pod id of the writer backend, if present in the response.
    pub fn writer_pod(&self) -> Option<&str> {
        self.writer_backend
            .as_ref()?
            .get("pod")
            .and_then(|v| v.as_str())
    }

    /// The writer backend's snapshot, falling back to the first backend.
    pub fn canonical_backend(&self) -> Option<&CliClusterBackend> {
        if let Some(pod) = self.writer_pod()
            && let Some(b) = self.backends.iter().find(|b| b.pod == pod)
        {
            return Some(b);
        }
        self.backends.first()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliEnterpriseCluster {
    pub cluster_id: String,
    #[serde(alias = "cluster_name")]
    pub name: String,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub project_name: Option<String>,
    #[serde(default)]
    pub availability_mode: Option<String>,
    #[serde(default)]
    pub gateway_node_type: Option<String>,
    #[serde(default)]
    pub db_node_type: Option<String>,
    #[serde(default)]
    pub gateway_url: Option<String>,
    #[serde(default)]
    pub query_auth_header: Option<String>,
    #[serde(default)]
    pub query_auth_env: Option<String>,
    #[serde(default)]
    pub query_auth_scheme: Option<crate::config::QueryAuthScheme>,
    #[serde(default)]
    pub min_gateway_count: Option<u64>,
    #[serde(default)]
    pub max_gateway_count: Option<u64>,
    #[serde(default)]
    pub min_hyperscale_count: Option<u64>,
    #[serde(default)]
    pub max_hyperscale_count: Option<u64>,
    #[serde(default)]
    pub gateway_count: Option<u64>,
    #[serde(default)]
    pub hyperscale_count: Option<u64>,
    #[serde(default)]
    pub min_instances: Option<u64>,
    #[serde(default)]
    pub max_instances: Option<u64>,
}

impl CliEnterpriseCluster {
    pub fn resolved_gateway_min_count(&self) -> Option<u64> {
        self.min_gateway_count
            .or(self.gateway_count)
            .or(self.max_gateway_count)
            .or(self.min_instances)
    }

    pub fn resolved_gateway_max_count(&self) -> Option<u64> {
        self.max_gateway_count
            .or(self.gateway_count)
            .or(self.min_gateway_count)
            .or(self.min_instances)
    }

    pub fn resolved_hyperscale_min_count(&self) -> Option<u64> {
        self.min_hyperscale_count
            .or(self.hyperscale_count)
            .or(self.max_hyperscale_count)
            .or(self.max_instances)
    }

    pub fn resolved_hyperscale_max_count(&self) -> Option<u64> {
        self.max_hyperscale_count
            .or(self.hyperscale_count)
            .or(self.min_hyperscale_count)
            .or(self.max_instances)
    }

    pub fn resolved_gateway_count(&self) -> Option<u64> {
        self.resolved_gateway_min_count()
    }

    pub fn resolved_hyperscale_count(&self) -> Option<u64> {
        self.resolved_hyperscale_min_count()
    }

    pub fn compatibility_min_instances(&self) -> Option<u64> {
        if let (Some(gateway_count), Some(hyperscale_count)) = (
            self.resolved_gateway_min_count(),
            self.resolved_hyperscale_min_count(),
        ) {
            Some(gateway_count.min(hyperscale_count))
        } else {
            self.min_instances
        }
    }

    pub fn compatibility_max_instances(&self) -> Option<u64> {
        if let (Some(gateway_count), Some(hyperscale_count)) = (
            self.resolved_gateway_max_count(),
            self.resolved_hyperscale_max_count(),
        ) {
            Some(gateway_count.max(hyperscale_count))
        } else {
            self.max_instances
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliClusterProject {
    pub cluster_id: String,
    pub project_id: String,
    pub project_name: String,
    pub workspace_id: String,
}

async fn get_json<T: DeserializeOwned>(
    client: &Client,
    url: String,
    api_key: &str,
    action: &str,
) -> Result<T> {
    let response = client.get(&url).header("x-api-key", api_key).send().await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(eyre!("Failed to {action}: HTTP {status} {body}"));
    }
    Ok(response.json::<T>().await?)
}

pub async fn fetch_workspaces(
    client: &Client,
    base_url: &str,
    api_key: &str,
) -> Result<Vec<CliWorkspace>> {
    get_json(
        client,
        format!("{base_url}/api/cli/workspaces"),
        api_key,
        "fetch workspaces",
    )
    .await
}

pub async fn fetch_projects(
    client: &Client,
    base_url: &str,
    api_key: &str,
    workspace_id: &str,
) -> Result<Vec<CliProject>> {
    get_json(
        client,
        format!("{base_url}/api/cli/workspaces/{workspace_id}/projects"),
        api_key,
        "fetch projects",
    )
    .await
}

pub async fn fetch_project_details(
    client: &Client,
    base_url: &str,
    api_key: &str,
    project_id: &str,
) -> Result<CliProjectDetails> {
    get_json(
        client,
        format!("{base_url}/api/cli/projects/{project_id}"),
        api_key,
        "fetch project details",
    )
    .await
}

pub async fn fetch_project_clusters(
    client: &Client,
    base_url: &str,
    api_key: &str,
    project_id: &str,
) -> Result<CliProjectClusters> {
    get_json(
        client,
        format!("{base_url}/api/cli/projects/{project_id}/clusters"),
        api_key,
        "fetch project clusters",
    )
    .await
}

pub async fn fetch_workspace_clusters(
    client: &Client,
    base_url: &str,
    api_key: &str,
    workspace_id: &str,
) -> Result<CliWorkspaceClusters> {
    get_json(
        client,
        format!("{base_url}/api/cli/workspaces/{workspace_id}/clusters"),
        api_key,
        "fetch workspace clusters",
    )
    .await
}

/// Fetches the cluster index snapshot. Returns the typed view alongside the raw
/// JSON body so callers can render the typed summary while still emitting the
/// exact API response for `--format json` (and as a fallback when the shape is
/// unrecognized).
pub async fn fetch_indexes_for_cluster(
    client: &Client,
    base_url: &str,
    api_key: &str,
    cluster_id: &str,
) -> Result<(CliClusterIndexes, serde_json::Value)> {
    let raw: serde_json::Value = get_json(
        client,
        format!("{base_url}/api/cli/enterprise-clusters/{cluster_id}/indexes"),
        api_key,
        "fetch cluster indexes",
    )
    .await?;
    let typed: CliClusterIndexes = serde_json::from_value(raw.clone())?;
    Ok((typed, raw))
}

pub async fn fetch_enterprise_cluster_project(
    client: &Client,
    base_url: &str,
    api_key: &str,
    cluster_id: &str,
) -> Result<CliClusterProject> {
    get_json(
        client,
        format!("{base_url}/api/cli/enterprise-clusters/{cluster_id}/project"),
        api_key,
        "fetch enterprise cluster project",
    )
    .await
}

pub fn find_workspace_by_id<'a>(
    workspaces: &'a [CliWorkspace],
    id: &str,
) -> Option<&'a CliWorkspace> {
    workspaces.iter().find(|workspace| workspace.id == id)
}

pub fn find_workspace_by_slug<'a>(
    workspaces: &'a [CliWorkspace],
    slug: &str,
) -> Option<&'a CliWorkspace> {
    workspaces
        .iter()
        .find(|workspace| workspace.url_slug == slug)
}

pub fn find_project_by_id<'a>(projects: &'a [CliProject], id: &str) -> Option<&'a CliProject> {
    projects.iter().find(|project| project.id == id)
}

pub fn find_project_by_name<'a>(projects: &'a [CliProject], name: &str) -> Option<&'a CliProject> {
    projects.iter().find(|project| project.name == name)
}

pub fn find_enterprise_cluster_by_id<'a>(
    clusters: &'a [CliEnterpriseCluster],
    id: &str,
) -> Option<&'a CliEnterpriseCluster> {
    clusters.iter().find(|cluster| cluster.cluster_id == id)
}

/// List the Enterprise clusters available for a project (preferred) or workspace.
pub async fn list_clusters_for_context(
    client: &Client,
    base_url: &str,
    api_key: &str,
    project_id: Option<&str>,
    workspace_id: Option<&str>,
) -> Result<Vec<CliEnterpriseCluster>> {
    if let Some(project_id) = project_id {
        Ok(
            fetch_project_clusters(client, base_url, api_key, project_id)
                .await?
                .enterprise,
        )
    } else if let Some(workspace_id) = workspace_id {
        Ok(
            fetch_workspace_clusters(client, base_url, api_key, workspace_id)
                .await?
                .enterprise,
        )
    } else {
        Err(eyre!(
            "No workspace selected. Run 'helix workspace switch <workspace>'."
        ))
    }
}

/// A cluster resolved to its full metadata plus the project/workspace it belongs to.
pub struct ResolvedEnterpriseCluster {
    pub cluster: CliEnterpriseCluster,
    pub project_id: String,
    pub project_name: String,
    pub workspace_id: Option<String>,
}

/// Resolve a cluster ID to its full record and owning project/workspace.
///
/// Prefers the caller-supplied IDs; otherwise looks up the cluster's project via
/// `fetch_enterprise_cluster_project`. The full cluster record is then pulled from
/// the project's cluster list.
pub async fn resolve_enterprise_cluster(
    client: &Client,
    base_url: &str,
    api_key: &str,
    cluster_id: &str,
    known_project_id: Option<&str>,
    known_workspace_id: Option<&str>,
) -> Result<ResolvedEnterpriseCluster> {
    let (project_id, project_name, workspace_id) = if let Some(project_id) = known_project_id {
        (
            project_id.to_string(),
            None,
            known_workspace_id.map(str::to_string),
        )
    } else {
        let cluster_project =
            fetch_enterprise_cluster_project(client, base_url, api_key, cluster_id).await?;
        (
            cluster_project.project_id,
            Some(cluster_project.project_name),
            Some(cluster_project.workspace_id),
        )
    };
    let project_clusters = fetch_project_clusters(client, base_url, api_key, &project_id).await?;
    let cluster = find_enterprise_cluster_by_id(&project_clusters.enterprise, cluster_id)
        .cloned()
        .ok_or_else(|| {
            eyre!("Enterprise cluster '{cluster_id}' was not found in project '{project_id}'")
        })?;

    Ok(ResolvedEnterpriseCluster {
        project_id: project_clusters.project_id,
        project_name: project_name.unwrap_or(project_clusters.project_name),
        workspace_id,
        cluster,
    })
}

#[cfg(test)]
mod index_tests {
    use super::*;

    // A trimmed but structurally-faithful sample of the real
    // `/api/cli/enterprise-clusters/{id}/indexes` response (pod_pool, 2 replicas).
    const REAL_RESPONSE: &str = r#"{
        "phase": { "phase": "stable", "writer": { "pod": "pod-a", "term": 15 } },
        "mode": "pod_pool",
        "errors": [],
        "readable_backends": ["pod-a", "pod-b"],
        "writer_backend": { "pod": "pod-a", "healthy": true, "accepting_writes": true },
        "backends": [
            { "pod": "pod-a", "snapshot": {
                "node_label_index": true,
                "edge_label_index": true,
                "node_equality_indexes": [["User","externalId"], ["Project","firmId"]],
                "node_text_indexes": [["User","firstName"]],
                "node_vector_indexes": [["DocumentChunk","embedding"]],
                "node_range_indexes": [],
                "node_range_desc_indexes": [],
                "edge_equality_indexes": [["ForClient","firmId"]],
                "edge_text_indexes": [],
                "edge_vector_indexes": [],
                "edge_range_indexes": [],
                "edge_range_desc_indexes": []
            }},
            { "pod": "pod-b", "snapshot": {
                "node_label_index": true,
                "edge_label_index": true,
                "node_equality_indexes": [["User","externalId"], ["Project","firmId"]],
                "node_text_indexes": [["User","firstName"]],
                "node_vector_indexes": [["DocumentChunk","embedding"]],
                "node_range_indexes": [],
                "node_range_desc_indexes": [],
                "edge_equality_indexes": [["ForClient","firmId"]],
                "edge_text_indexes": [],
                "edge_vector_indexes": [],
                "edge_range_indexes": [],
                "edge_range_desc_indexes": []
            }}
        ]
    }"#;

    #[test]
    fn deserializes_real_multi_backend_response() {
        let parsed: CliClusterIndexes = serde_json::from_str(REAL_RESPONSE).unwrap();

        assert_eq!(parsed.mode.as_deref(), Some("pod_pool"));
        assert_eq!(parsed.readable_backends.len(), 2);
        assert_eq!(parsed.writer_pod(), Some("pod-a"));

        let canonical = parsed.canonical_backend().expect("writer backend resolved");
        assert_eq!(canonical.pod, "pod-a");

        let snap = &canonical.snapshot;
        assert!(snap.node_label_index);
        assert!(snap.edge_label_index);
        assert_eq!(
            snap.node_equality_indexes,
            vec![
                ("User".to_string(), "externalId".to_string()),
                ("Project".to_string(), "firmId".to_string())
            ]
        );
        assert_eq!(
            snap.node_vector_indexes,
            vec![("DocumentChunk".to_string(), "embedding".to_string())]
        );
        assert_eq!(
            snap.node_text_indexes,
            vec![("User".to_string(), "firstName".to_string())]
        );
        assert_eq!(
            snap.edge_equality_indexes,
            vec![("ForClient".to_string(), "firmId".to_string())]
        );
        assert!(snap.node_range_indexes.is_empty());
    }

    #[test]
    fn canonical_falls_back_to_first_backend_without_writer() {
        // No writer_backend field -> fall back to the first backend.
        let body = r#"{
            "mode": "pod_pool",
            "backends": [ { "pod": "only-pod", "snapshot": { "node_label_index": true } } ]
        }"#;
        let parsed: CliClusterIndexes = serde_json::from_str(body).unwrap();
        assert_eq!(parsed.writer_pod(), None);
        assert_eq!(
            parsed.canonical_backend().map(|b| b.pod.as_str()),
            Some("only-pod")
        );
    }

    #[test]
    fn unrecognized_shape_yields_no_backend() {
        // The old/wrong shape (or any body without `backends`) must not pretend
        // success: there is no canonical backend, so the caller prints raw JSON.
        let old_shape = r#"{ "vector_indexes": [], "equality_indexes": [], "range_indexes": [] }"#;
        let parsed: CliClusterIndexes = serde_json::from_str(old_shape).unwrap();
        assert!(parsed.backends.is_empty());
        assert!(parsed.canonical_backend().is_none());
        // The unknown keys are captured in `extra`, not silently dropped.
        assert!(parsed.extra.contains_key("vector_indexes"));
    }

    #[test]
    fn enterprise_cluster_count_precedence_covers_all_remote_shapes() {
        struct Case {
            body: serde_json::Value,
            gateway_min: Option<u64>,
            gateway_max: Option<u64>,
            hyperscale_min: Option<u64>,
            hyperscale_max: Option<u64>,
            compatibility_min: Option<u64>,
            compatibility_max: Option<u64>,
        }

        let cases = [
            Case {
                body: serde_json::json!({
                    "cluster_id":"new",
                    "name":"new",
                    "min_gateway_count":2,
                    "max_gateway_count":4,
                    "min_hyperscale_count":3,
                    "max_hyperscale_count":5
                }),
                gateway_min: Some(2),
                gateway_max: Some(4),
                hyperscale_min: Some(3),
                hyperscale_max: Some(5),
                compatibility_min: Some(2),
                compatibility_max: Some(5),
            },
            Case {
                body: serde_json::json!({
                    "cluster_id":"role",
                    "cluster_name":"role",
                    "gateway_count":6,
                    "hyperscale_count":3
                }),
                gateway_min: Some(6),
                gateway_max: Some(6),
                hyperscale_min: Some(3),
                hyperscale_max: Some(3),
                compatibility_min: Some(3),
                compatibility_max: Some(6),
            },
            Case {
                body: serde_json::json!({
                    "cluster_id":"legacy",
                    "name":"legacy",
                    "min_instances":1,
                    "max_instances":8
                }),
                gateway_min: Some(1),
                gateway_max: Some(1),
                hyperscale_min: Some(8),
                hyperscale_max: Some(8),
                compatibility_min: Some(1),
                compatibility_max: Some(8),
            },
            Case {
                body: serde_json::json!({"cluster_id":"empty","name":"empty"}),
                gateway_min: None,
                gateway_max: None,
                hyperscale_min: None,
                hyperscale_max: None,
                compatibility_min: None,
                compatibility_max: None,
            },
        ];

        for case in cases {
            let cluster: CliEnterpriseCluster = serde_json::from_value(case.body).unwrap();
            assert_eq!(cluster.resolved_gateway_min_count(), case.gateway_min);
            assert_eq!(cluster.resolved_gateway_max_count(), case.gateway_max);
            assert_eq!(cluster.resolved_hyperscale_min_count(), case.hyperscale_min);
            assert_eq!(cluster.resolved_hyperscale_max_count(), case.hyperscale_max);
            assert_eq!(cluster.resolved_gateway_count(), case.gateway_min);
            assert_eq!(cluster.resolved_hyperscale_count(), case.hyperscale_min);
            assert_eq!(
                cluster.compatibility_min_instances(),
                case.compatibility_min
            );
            assert_eq!(
                cluster.compatibility_max_instances(),
                case.compatibility_max
            );
        }
    }

    #[test]
    fn writer_backend_missing_or_unknown_falls_back_deterministically() {
        for writer_backend in [
            None,
            Some(serde_json::json!({"pod":"missing"})),
            Some(serde_json::json!({"healthy":true})),
            Some(serde_json::Value::String("writer-a".to_string())),
        ] {
            let indexes = CliClusterIndexes {
                writer_backend,
                backends: vec![CliClusterBackend {
                    pod: "first".to_string(),
                    snapshot: CliIndexSnapshot::default(),
                }],
                ..CliClusterIndexes::default()
            };
            assert_eq!(
                indexes
                    .canonical_backend()
                    .map(|backend| backend.pod.as_str()),
                Some("first")
            );
        }
    }
}
