use helix_cli::enterprise_cloud::{
    fetch_enterprise_cluster_project, fetch_indexes_for_cluster, fetch_project_clusters,
    fetch_project_details, fetch_projects, fetch_workspace_clusters, fetch_workspaces,
    list_clusters_for_context,
};
use reqwest::Client;
use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn json_route(server: &MockServer, route: &str, body: serde_json::Value) {
    Mock::given(method("GET"))
        .and(path(route))
        .and(header("x-api-key", "admin-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .expect(1)
        .mount(server)
        .await;
}

async fn error_route(server: &MockServer, route: &str) {
    Mock::given(method("GET"))
        .and(path(route))
        .respond_with(ResponseTemplate::new(503).set_body_string("unavailable"))
        .expect(1)
        .mount(server)
        .await;
}

#[tokio::test]
async fn every_enterprise_get_endpoint_decodes_its_success_contract() {
    let server = MockServer::start().await;
    let client = Client::new();
    let base = server.uri();
    json_route(
        &server,
        "/api/cli/workspaces",
        json!([{"id":"ws-1","name":"Acme","url_slug":"acme"}]),
    )
    .await;
    json_route(
        &server,
        "/api/cli/workspaces/ws-1/projects",
        json!([{"id":"project-1","name":"Graph App"}]),
    )
    .await;
    json_route(
        &server,
        "/api/cli/projects/project-1",
        json!({
            "id":"project-1","name":"Graph App","workspace_id":"ws-1",
            "workspace_name":"Acme","workspace_slug":"acme"
        }),
    )
    .await;
    json_route(
        &server,
        "/api/cli/projects/project-1/clusters",
        json!({
            "project_id":"project-1","project_name":"Graph App",
            "enterprise":[{"cluster_id":"cluster-1","name":"Primary"}]
        }),
    )
    .await;
    json_route(
        &server,
        "/api/cli/workspaces/ws-1/clusters",
        json!({"enterprise":[{"cluster_id":"cluster-2","name":"Secondary"}]}),
    )
    .await;
    json_route(
        &server,
        "/api/cli/enterprise-clusters/cluster-1/indexes",
        json!({
            "mode":"standard",
            "backends":[{"pod":"writer-0","snapshot":{"node_label_index":true}}],
            "writer_backend":{"pod":"writer-0"}
        }),
    )
    .await;
    json_route(
        &server,
        "/api/cli/enterprise-clusters/cluster-1/project",
        json!({
            "cluster_id":"cluster-1","project_id":"project-1",
            "project_name":"Graph App","workspace_id":"ws-1"
        }),
    )
    .await;

    assert_eq!(
        fetch_workspaces(&client, &base, "admin-key").await.unwrap()[0].id,
        "ws-1"
    );
    assert_eq!(
        fetch_projects(&client, &base, "admin-key", "ws-1")
            .await
            .unwrap()[0]
            .id,
        "project-1"
    );
    assert_eq!(
        fetch_project_details(&client, &base, "admin-key", "project-1")
            .await
            .unwrap()
            .workspace_id,
        "ws-1"
    );
    assert_eq!(
        fetch_project_clusters(&client, &base, "admin-key", "project-1")
            .await
            .unwrap()
            .enterprise[0]
            .cluster_id,
        "cluster-1"
    );
    assert_eq!(
        fetch_workspace_clusters(&client, &base, "admin-key", "ws-1")
            .await
            .unwrap()
            .enterprise[0]
            .cluster_id,
        "cluster-2"
    );
    let (indexes, raw) = fetch_indexes_for_cluster(&client, &base, "admin-key", "cluster-1")
        .await
        .unwrap();
    assert_eq!(indexes.writer_pod(), Some("writer-0"));
    assert_eq!(raw["mode"], "standard");
    assert_eq!(
        fetch_enterprise_cluster_project(&client, &base, "admin-key", "cluster-1")
            .await
            .unwrap()
            .project_id,
        "project-1"
    );
}

#[tokio::test]
async fn every_enterprise_get_endpoint_surfaces_non_success_status() {
    let server = MockServer::start().await;
    let client = Client::new();
    let base = server.uri();
    for route in [
        "/api/cli/workspaces",
        "/api/cli/workspaces/ws-1/projects",
        "/api/cli/projects/project-1",
        "/api/cli/projects/project-1/clusters",
        "/api/cli/workspaces/ws-1/clusters",
        "/api/cli/enterprise-clusters/cluster-1/indexes",
        "/api/cli/enterprise-clusters/cluster-1/project",
    ] {
        error_route(&server, route).await;
    }

    let errors = [
        fetch_workspaces(&client, &base, "admin-key")
            .await
            .unwrap_err(),
        fetch_projects(&client, &base, "admin-key", "ws-1")
            .await
            .unwrap_err(),
        fetch_project_details(&client, &base, "admin-key", "project-1")
            .await
            .unwrap_err(),
        fetch_project_clusters(&client, &base, "admin-key", "project-1")
            .await
            .unwrap_err(),
        fetch_workspace_clusters(&client, &base, "admin-key", "ws-1")
            .await
            .unwrap_err(),
        fetch_indexes_for_cluster(&client, &base, "admin-key", "cluster-1")
            .await
            .unwrap_err(),
        fetch_enterprise_cluster_project(&client, &base, "admin-key", "cluster-1")
            .await
            .unwrap_err(),
    ];
    for error in errors {
        let message = error.to_string();
        assert!(message.contains("HTTP 503"));
        assert!(message.contains("unavailable"));
    }
}

#[tokio::test]
async fn cluster_context_prefers_project_and_rejects_missing_context() {
    let server = MockServer::start().await;
    let client = Client::new();
    json_route(
        &server,
        "/api/cli/projects/project-1/clusters",
        json!({
            "project_id":"project-1","project_name":"Graph App",
            "enterprise":[{"cluster_id":"cluster-1","name":"Primary"}]
        }),
    )
    .await;
    let clusters = list_clusters_for_context(
        &client,
        &server.uri(),
        "admin-key",
        Some("project-1"),
        Some("ws-ignored"),
    )
    .await
    .unwrap();
    assert_eq!(clusters[0].cluster_id, "cluster-1");

    let error = list_clusters_for_context(&client, &server.uri(), "admin-key", None, None)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("No workspace selected"));
}
