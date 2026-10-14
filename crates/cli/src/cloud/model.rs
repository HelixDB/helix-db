//! Typed Helix Cloud resources as WFE returns them (proto3 JSON, camelCase).
//!
//! Only `id` is required; every other field is optional and is omitted again
//! on output when the server omitted it. Fields the CLI does not model are
//! kept in `extra`, so `--json` output neither drops nor invents anything.

use crate::config::DatabaseReference;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A resource that can be matched by ID, slug, or display name.
pub trait Named {
    fn id(&self) -> &str;
    fn slug(&self) -> &str;
    fn display_name(&self) -> &str;

    /// The value to pass on the command line to select exactly this resource.
    fn argument(&self) -> String {
        self.id().to_owned()
    }

    /// The friendliest non-empty identifier: display name, then slug, then ID.
    fn label(&self) -> &str {
        [self.display_name(), self.slug()]
            .into_iter()
            .find(|value| !value.is_empty())
            .unwrap_or_else(|| self.id())
    }
}

/// Human form of a `RESOURCE_STATUS_*` value: `RESOURCE_STATUS_ACTIVE` → `active`.
///
/// ```
/// use helix_cli::cloud::model::status_label;
///
/// assert_eq!(status_label(Some("RESOURCE_STATUS_ACTIVE")), "active");
/// assert_eq!(status_label(Some("ready")), "ready");
/// assert_eq!(status_label(None), "unknown");
/// ```
pub fn status_label(status: Option<&str>) -> String {
    let status = status.unwrap_or_default();
    match status.strip_prefix("RESOURCE_STATUS_").unwrap_or(status) {
        "" | "UNSPECIFIED" => "unknown".to_owned(),
        status => status.to_lowercase(),
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Workspace {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slug: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Project {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slug: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Whether a cluster is dedicated to one project (and so is itself a
/// database target) or shared between tenant databases. A view over the raw
/// `access` string, which is kept verbatim for output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterAccess {
    Dedicated,
    Shared,
    Unknown,
}

impl ClusterAccess {
    pub fn label(self) -> &'static str {
        match self {
            Self::Dedicated => "dedicated",
            Self::Shared => "shared",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Cluster {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slug: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl Cluster {
    /// `CLUSTER_ACCESS_DEDICATED` and the short `dedicated` form both count.
    pub fn access(&self) -> ClusterAccess {
        match self.access.as_deref() {
            Some("CLUSTER_ACCESS_DEDICATED" | "dedicated") => ClusterAccess::Dedicated,
            Some("CLUSTER_ACCESS_SHARED" | "shared") => ClusterAccess::Shared,
            _ => ClusterAccess::Unknown,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Tenant {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slug: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// A queryable database: a dedicated cluster or a tenant. Serialized as the
/// resource plus `"kind": "dedicated" | "tenant"`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Database {
    Dedicated(Cluster),
    Tenant(Tenant),
}

impl Database {
    pub fn reference(&self) -> DatabaseReference {
        match self {
            Self::Dedicated(cluster) => DatabaseReference::Cluster(cluster.id.clone()),
            Self::Tenant(tenant) => DatabaseReference::Tenant(tenant.id.clone()),
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Dedicated(_) => "dedicated",
            Self::Tenant(_) => "tenant",
        }
    }

    pub fn status(&self) -> Option<&str> {
        match self {
            Self::Dedicated(cluster) => cluster.status.as_deref(),
            Self::Tenant(tenant) => tenant.status.as_deref(),
        }
    }

    pub fn project_id(&self) -> Option<&str> {
        match self {
            Self::Dedicated(cluster) => cluster.project_id.as_deref(),
            Self::Tenant(tenant) => tenant.project_id.as_deref(),
        }
    }

    pub fn workspace_id(&self) -> Option<&str> {
        match self {
            Self::Dedicated(cluster) => cluster.workspace_id.as_deref(),
            Self::Tenant(tenant) => tenant.workspace_id.as_deref(),
        }
    }
}

/// An application key for a database. Its token is only ever returned once,
/// at creation, and is never part of this model.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseKey {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permissions: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl DatabaseKey {
    /// `read-write` / `read-only` from the access enum, else the raw access
    /// value, else the permissions.
    pub fn access_label(&self) -> String {
        match self.access.as_deref() {
            Some(access) => access.strip_prefix("DATABASE_KEY_ACCESS_").map_or_else(
                || access.to_owned(),
                |access| access.to_lowercase().replace('_', "-"),
            ),
            None => self.permissions.as_deref().unwrap_or_default().join(","),
        }
    }
}

/// A workspace-owned headless credential. Its secret is never returned after
/// creation and is never part of this model.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceCredential {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grants: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl Named for Workspace {
    fn id(&self) -> &str {
        &self.id
    }
    fn slug(&self) -> &str {
        self.slug.as_deref().unwrap_or_default()
    }
    fn display_name(&self) -> &str {
        self.display_name.as_deref().unwrap_or_default()
    }
}

impl Named for Project {
    fn id(&self) -> &str {
        &self.id
    }
    fn slug(&self) -> &str {
        self.slug.as_deref().unwrap_or_default()
    }
    fn display_name(&self) -> &str {
        self.display_name.as_deref().unwrap_or_default()
    }
}

impl Named for Cluster {
    fn id(&self) -> &str {
        &self.id
    }
    fn slug(&self) -> &str {
        self.slug.as_deref().unwrap_or_default()
    }
    fn display_name(&self) -> &str {
        self.display_name.as_deref().unwrap_or_default()
    }
}

impl Named for Tenant {
    fn id(&self) -> &str {
        &self.id
    }
    fn slug(&self) -> &str {
        self.slug.as_deref().unwrap_or_default()
    }
    /// Tenants carry a `name`; older responses use `displayName`.
    fn display_name(&self) -> &str {
        self.name
            .as_deref()
            .or(self.display_name.as_deref())
            .unwrap_or_default()
    }
}

impl Named for Database {
    /// The typed `tenant:<id>` / `cluster:<id>` reference, which resolves
    /// without any project context.
    fn argument(&self) -> String {
        self.reference().to_string()
    }
    fn id(&self) -> &str {
        match self {
            Self::Dedicated(cluster) => cluster.id(),
            Self::Tenant(tenant) => tenant.id(),
        }
    }
    fn slug(&self) -> &str {
        match self {
            Self::Dedicated(cluster) => cluster.slug(),
            Self::Tenant(tenant) => tenant.slug(),
        }
    }
    fn display_name(&self) -> &str {
        match self {
            Self::Dedicated(cluster) => cluster.display_name(),
            Self::Tenant(tenant) => tenant.display_name(),
        }
    }
}

impl Named for DatabaseKey {
    fn id(&self) -> &str {
        &self.id
    }
    fn slug(&self) -> &str {
        ""
    }
    fn display_name(&self) -> &str {
        self.name.as_deref().unwrap_or_default()
    }
}

impl Named for ServiceCredential {
    fn id(&self) -> &str {
        &self.id
    }
    fn slug(&self) -> &str {
        ""
    }
    fn display_name(&self) -> &str {
        self.name.as_deref().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn output_matches_input_field_for_field() {
        let raw = json!({"id": "ws-1", "displayName": "Acme", "iconUrl": "https://x"});
        let workspace: Workspace = serde_json::from_value(raw.clone()).unwrap();
        assert_eq!(workspace.display_name.as_deref(), Some("Acme"));
        assert_eq!(workspace.extra["iconUrl"], "https://x");
        assert_eq!(
            serde_json::to_value(&workspace).unwrap(),
            raw,
            "nothing dropped or invented"
        );

        let raw = json!({"id": "c", "access": "dedicated", "status": "RESOURCE_STATUS_ACTIVE"});
        let cluster: Cluster = serde_json::from_value(raw.clone()).unwrap();
        assert_eq!(
            serde_json::to_value(&cluster).unwrap(),
            raw,
            "access is kept verbatim"
        );
    }

    #[test]
    fn only_the_id_is_required() {
        assert!(serde_json::from_value::<Project>(json!({"slug": "x"})).is_err());
        let project: Project = serde_json::from_value(json!({"id": "p"})).unwrap();
        assert_eq!(project.label(), "p");
    }

    #[test]
    fn cluster_access_accepts_proto_and_short_forms() {
        for (raw, expected) in [
            (json!("CLUSTER_ACCESS_DEDICATED"), ClusterAccess::Dedicated),
            (json!("dedicated"), ClusterAccess::Dedicated),
            (json!("shared"), ClusterAccess::Shared),
            (json!("CLUSTER_ACCESS_SHARED"), ClusterAccess::Shared),
            (json!("something-new"), ClusterAccess::Unknown),
            (Value::Null, ClusterAccess::Unknown),
        ] {
            let cluster: Cluster =
                serde_json::from_value(json!({"id": "c", "access": raw})).unwrap();
            assert_eq!(cluster.access(), expected, "{raw}");
        }
    }

    #[test]
    fn labels_prefer_names_over_slugs_over_ids() {
        let tenant: Tenant =
            serde_json::from_value(json!({"id": "t", "slug": "s", "displayName": "D"})).unwrap();
        assert_eq!(tenant.label(), "D");
        let tenant: Tenant = serde_json::from_value(
            json!({"id": "t", "slug": "s", "displayName": "D", "name": "N"}),
        )
        .unwrap();
        assert_eq!(tenant.label(), "N");
        let tenant: Tenant = serde_json::from_value(json!({"id": "t", "slug": "s"})).unwrap();
        assert_eq!(tenant.label(), "s");
    }

    #[test]
    fn databases_serialize_with_their_kind_and_expose_references() {
        let tenant: Tenant = serde_json::from_value(
            json!({"id": "t-1", "projectId": "p", "status": "RESOURCE_STATUS_ACTIVE"}),
        )
        .unwrap();
        let database = Database::Tenant(tenant);
        assert_eq!(
            database.reference(),
            DatabaseReference::Tenant("t-1".into())
        );
        assert_eq!(database.kind(), "tenant");
        assert_eq!(database.project_id(), Some("p"));
        assert_eq!(status_label(database.status()), "active");
        let value = serde_json::to_value(&database).unwrap();
        assert_eq!(value["kind"], "tenant");
        assert_eq!(value["id"], "t-1");
        assert!(value.get("workspaceId").is_none(), "absent stays absent");

        let cluster: Cluster =
            serde_json::from_value(json!({"id": "c-1", "workspaceId": "w"})).unwrap();
        let database = Database::Dedicated(cluster);
        assert_eq!(
            database.reference(),
            DatabaseReference::Cluster("c-1".into())
        );
        assert_eq!(database.workspace_id(), Some("w"));
        assert_eq!(
            serde_json::to_value(&database).unwrap()["kind"],
            "dedicated"
        );
    }

    #[test]
    fn key_access_labels_cover_enum_raw_and_permission_forms() {
        let key = |value: Value| serde_json::from_value::<DatabaseKey>(value).unwrap();
        assert_eq!(
            key(json!({"id": "k", "access": "DATABASE_KEY_ACCESS_READ_WRITE"})).access_label(),
            "read-write"
        );
        assert_eq!(
            key(json!({"id": "k", "access": "custom"})).access_label(),
            "custom"
        );
        assert_eq!(
            key(json!({"id": "k", "permissions": ["read", "write"]})).access_label(),
            "read,write"
        );
        assert_eq!(key(json!({"id": "k"})).access_label(), "");
    }
}
