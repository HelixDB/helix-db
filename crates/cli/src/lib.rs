use clap::{Args, Subcommand, ValueEnum};

pub mod cloud;
pub mod commands;
pub mod config;
pub mod errors;
pub(crate) mod external_tools;
pub(crate) mod host_actions;
pub mod image;
pub mod local_runtime;
pub use helix_metrics::cli as metrics_sender;
pub mod output;
pub(crate) mod paths;
pub mod port;
pub mod project;
pub mod prompts;
pub(crate) mod service_endpoints;
pub mod setup;
pub mod ts_query;
pub mod update;
pub mod utils;

#[derive(Subcommand)]
pub enum AuthAction {
    /// Login to Helix Cloud
    Login,
    /// Show the active WorkOS session
    Status,
    /// Logout from Helix Cloud
    Logout,
}

#[derive(Subcommand)]
pub enum InitTarget {
    /// Initialize a project with a local development instance
    Local {
        /// Local instance name
        #[arg(short, long, default_value = "dev")]
        name: String,
        /// Local gateway port
        #[arg(long, default_value_t = crate::config::DEFAULT_LOCAL_PORT)]
        port: u16,
        /// Use on-disk storage backed by a local SeaweedFS container
        #[arg(long, conflicts_with = "storage_uri")]
        disk: bool,
        #[command(flatten)]
        s3: S3StorageArgs,
        /// Install the Helix agent skills + docs MCP (prompted when interactive)
        #[arg(long, conflicts_with = "no_skills")]
        skills: bool,
        /// Skip installing the Helix agent skills + docs MCP
        #[arg(long = "no-skills", conflicts_with = "skills")]
        no_skills: bool,
    },
    /// Initialize a Helix Cloud project
    #[command(name = "cloud", alias = "enterprise")]
    Enterprise {
        /// Cloud instance name
        #[arg(short, long, default_value = "production")]
        name: String,
        /// Database ID, slug, name, or cluster:<id> / tenant:<id>; prompts when omitted
        #[arg(long)]
        database: Option<String>,
        /// Project ID, slug, or name to pick the database from
        #[arg(long)]
        project: Option<String>,
        /// Workspace ID, slug, or name
        #[arg(long)]
        workspace: Option<String>,
        /// Install the Helix agent skills + docs MCP (prompted when interactive)
        #[arg(long, conflicts_with = "no_skills")]
        skills: bool,
        /// Skip installing the Helix agent skills + docs MCP
        #[arg(long = "no-skills", conflicts_with = "skills")]
        no_skills: bool,
    },
}

impl InitTarget {
    /// Resolve the `--skills`/`--no-skills` flags supplied *after* the
    /// subcommand (e.g. `helix init local --no-skills`) into the same
    /// `Option<bool>` shape used for the top-level flags. Returns `None` when
    /// neither was set, so the caller can fall back to the parent-level flag.
    pub fn skills_override(&self) -> Option<bool> {
        let (skills, no_skills) = match self {
            InitTarget::Local {
                skills, no_skills, ..
            }
            | InitTarget::Enterprise {
                skills, no_skills, ..
            } => (*skills, *no_skills),
        };
        if skills {
            Some(true)
        } else if no_skills {
            Some(false)
        } else {
            None
        }
    }
}

#[derive(Subcommand)]
pub enum AddTarget {
    /// Add a local development instance
    Local {
        /// Local instance name
        #[arg(short, long)]
        name: String,
        /// Local gateway port
        #[arg(long, default_value_t = crate::config::DEFAULT_LOCAL_PORT)]
        port: u16,
        /// Use on-disk storage backed by a local SeaweedFS container
        #[arg(long, conflicts_with = "storage_uri")]
        disk: bool,
        #[command(flatten)]
        s3: S3StorageArgs,
    },
    /// Add a Helix Cloud instance
    #[command(name = "cloud", alias = "enterprise")]
    Enterprise {
        /// Cloud instance name
        #[arg(short, long)]
        name: String,
        /// Database ID, slug, name, or cluster:<id> / tenant:<id>; prompts when omitted
        #[arg(long)]
        database: Option<String>,
        /// Project ID, slug, or name; defaults to the linked project
        #[arg(long)]
        project: Option<String>,
        /// Workspace ID, slug, or name
        #[arg(long)]
        workspace: Option<String>,
    },
}

/// Where a Cloud command operates. Both are optional: they default to the
/// project linked in helix.toml, then to the only candidate, then to a picker.
#[derive(Args, Debug, Clone, Default)]
pub struct ScopeArgs {
    /// Workspace ID, slug, or name
    #[arg(long, value_name = "WORKSPACE")]
    pub workspace: Option<String>,
    /// Project ID, slug, or name
    #[arg(long, value_name = "PROJECT")]
    pub project: Option<String>,
}

#[derive(Args, Debug, Clone, Default)]
pub struct S3StorageArgs {
    /// Use an S3 or S3-compatible bucket/prefix, e.g. s3://bucket/prefix/
    #[arg(long)]
    pub storage_uri: Option<String>,
    /// Region for S3 storage
    #[arg(long)]
    pub s3_region: Option<String>,
    /// Custom S3-compatible endpoint URL
    #[arg(long)]
    pub s3_endpoint_url: Option<String>,
    /// Allow plain HTTP for the S3 endpoint
    #[arg(long)]
    pub s3_allow_http: bool,
}

impl S3StorageArgs {
    pub fn has_any(&self) -> bool {
        self.storage_uri.is_some()
            || self.s3_region.is_some()
            || self.s3_endpoint_url.is_some()
            || self.s3_allow_http
    }
}

#[derive(Subcommand)]
pub enum SkillsAction {
    /// Install the Helix agent skills (npx skills add HelixDB/skills)
    Install {
        /// Install into the current project (.<agent>/skills) instead of globally
        #[arg(long)]
        project: bool,
    },
    /// Refresh installed Helix agent skills to the latest version
    Update {
        /// Operate on the current project instead of globally
        #[arg(long)]
        project: bool,
    },
    /// List installed agent skills
    List {
        /// List project skills instead of global skills
        #[arg(long)]
        project: bool,
    },
}

#[derive(Subcommand)]
pub enum MetricsAction {
    /// Enable full metrics collection
    Full,
    /// Enable basic metrics collection
    Basic,
    /// Disable metrics collection
    Off,
    /// Show metrics status
    Status,
}

#[derive(Subcommand)]
pub enum WorkspaceAction {
    /// List the workspaces you can access
    List,
    /// Show a workspace; defaults to the linked or only workspace
    Get {
        /// Workspace ID, slug, or name
        workspace: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum ProjectAction {
    /// List projects in a workspace
    List {
        /// Workspace ID, slug, or name
        #[arg(long)]
        workspace: Option<String>,
    },
    /// Show a project; defaults to the project linked in helix.toml
    Get {
        /// Project ID, slug, or name
        project: Option<String>,
        /// Workspace ID, slug, or name
        #[arg(long)]
        workspace: Option<String>,
    },
    /// Create a project
    Create {
        /// Display name
        name: String,
        /// URL-safe slug; derived from the name when omitted
        #[arg(long)]
        slug: Option<String>,
        /// Workspace ID, slug, or name
        #[arg(long)]
        workspace: Option<String>,
        /// Also link this directory's helix.toml to the new project
        #[arg(long)]
        link: bool,
    },
    /// Delete a project; defaults to the project linked in helix.toml
    Delete {
        /// Project ID, slug, or name
        project: Option<String>,
        /// Workspace ID, slug, or name
        #[arg(long)]
        workspace: Option<String>,
        /// Skip the confirmation prompt
        #[arg(short = 'y', long)]
        yes: bool,
    },
    /// Link this directory's helix.toml to a project
    Link {
        /// Project ID, slug, or name; prompts when omitted
        project: Option<String>,
        /// Workspace ID, slug, or name
        #[arg(long)]
        workspace: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum ClusterAction {
    /// List clusters in the linked project, or in a workspace
    List {
        #[command(flatten)]
        scope: ScopeArgs,
    },
    /// Show a cluster
    Get {
        /// Cluster ID, slug, or name
        cluster: Option<String>,
        #[command(flatten)]
        scope: ScopeArgs,
    },
    /// List a cluster's active indexes
    #[command(alias = "indices")]
    Indexes {
        /// Cluster ID, slug, or name
        cluster: Option<String>,
        #[command(flatten)]
        scope: ScopeArgs,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum DatabaseKeyAccess {
    ReadOnly,
    ReadWrite,
}

impl DatabaseKeyAccess {
    pub const fn protobuf_name(self) -> &'static str {
        match self {
            Self::ReadOnly => "DATABASE_KEY_ACCESS_READ_ONLY",
            Self::ReadWrite => "DATABASE_KEY_ACCESS_READ_WRITE",
        }
    }
}

#[derive(Subcommand)]
pub enum DatabaseAction {
    /// List a project's databases
    List {
        #[command(flatten)]
        scope: ScopeArgs,
    },
    /// Show a database
    Get {
        /// Database ID, slug, name, or tenant:<id> / cluster:<id>
        database: Option<String>,
        #[command(flatten)]
        scope: ScopeArgs,
    },
    /// Create a tenant database and show its default read-write key once
    Create {
        /// Display name
        name: String,
        /// URL-safe slug; derived from the name when omitted
        #[arg(long)]
        slug: Option<String>,
        /// Dedicated cluster ID, slug, or name to create the tenant on
        #[arg(long)]
        cluster: Option<String>,
        /// Plan code for a shared tenant; prompted when omitted
        #[arg(long)]
        plan: Option<String>,
        #[command(flatten)]
        scope: ScopeArgs,
    },
    /// Delete a tenant database
    Delete {
        /// Database ID, slug, name, or tenant:<id>
        database: Option<String>,
        /// Skip the confirmation prompt
        #[arg(short = 'y', long)]
        yes: bool,
        #[command(flatten)]
        scope: ScopeArgs,
    },
    /// List a database's active indexes
    #[command(alias = "indices")]
    Indexes {
        /// Database ID, slug, name, or tenant:<id> / cluster:<id>
        database: Option<String>,
        #[command(flatten)]
        scope: ScopeArgs,
    },
    /// Manage application database keys
    Key {
        #[command(subcommand)]
        action: DatabaseKeyAction,
    },
}

#[derive(Subcommand)]
pub enum DatabaseKeyAction {
    /// Create an application key and show its token once
    Create {
        /// Access the key grants
        #[arg(long, value_enum)]
        access: DatabaseKeyAccess,
        /// Key name
        #[arg(long)]
        name: Option<String>,
        /// Database ID, slug, name, or tenant:<id> / cluster:<id>
        #[arg(long)]
        database: Option<String>,
        #[command(flatten)]
        scope: ScopeArgs,
    },
    /// List application keys; tokens are never shown again
    List {
        /// Database ID, slug, name, or tenant:<id> / cluster:<id>
        #[arg(long)]
        database: Option<String>,
        #[command(flatten)]
        scope: ScopeArgs,
    },
    /// Revoke an application key
    Revoke {
        /// Key ID or name
        key: String,
        /// Database ID, slug, name, or tenant:<id> / cluster:<id>
        #[arg(long)]
        database: Option<String>,
        /// Skip the confirmation prompt
        #[arg(short = 'y', long)]
        yes: bool,
        #[command(flatten)]
        scope: ScopeArgs,
    },
}

#[derive(Subcommand)]
pub enum ServiceCredentialAction {
    /// Create a workspace-owned headless credential and show its token once
    Create {
        /// Credential name
        #[arg(long)]
        name: String,
        /// Project grant: PROJECT_ID=project-read,query-read (repeatable)
        #[arg(long = "grant", required = true)]
        grants: Vec<String>,
        /// Expiry as RFC 3339, e.g. 2030-01-01T00:00:00Z
        #[arg(long)]
        expires_at: Option<String>,
        /// Workspace ID, slug, or name
        #[arg(long)]
        workspace: Option<String>,
    },
    /// List a workspace's credentials
    List {
        /// Workspace ID, slug, or name
        #[arg(long)]
        workspace: Option<String>,
    },
    /// Show a credential; its secret is never shown again
    Get {
        /// Credential ID or name
        credential: String,
        /// Workspace ID, slug, or name
        #[arg(long)]
        workspace: Option<String>,
    },
    /// Update name, expiry, or project grants without rotating the secret
    Update {
        /// Credential ID or name
        credential: String,
        /// New name
        #[arg(long)]
        name: Option<String>,
        /// Replacement project grants (repeatable)
        #[arg(long = "grant")]
        grants: Vec<String>,
        /// New expiry as RFC 3339
        #[arg(long, conflicts_with = "clear_expiry")]
        expires_at: Option<String>,
        /// Remove the expiry
        #[arg(long)]
        clear_expiry: bool,
        /// Workspace ID, slug, or name
        #[arg(long)]
        workspace: Option<String>,
    },
    /// Revoke a credential
    Revoke {
        /// Credential ID or name
        credential: String,
        /// Skip the confirmation prompt
        #[arg(short = 'y', long)]
        yes: bool,
        /// Workspace ID, slug, or name
        #[arg(long)]
        workspace: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum CloudApiAction {
    /// GET a /v1/... path
    Get { path: String },
    /// POST a JSON body to a /v1/... path
    Post {
        path: String,
        /// JSON request body
        #[arg(long, default_value = "{}")]
        body: String,
    },
    /// PATCH a /v1/... path with a JSON body
    Patch {
        path: String,
        /// JSON request body
        #[arg(long, default_value = "{}")]
        body: String,
    },
    /// DELETE a /v1/... path
    Delete { path: String },
}
