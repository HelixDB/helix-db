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
    /// Initialize a local v2 development project
    Local {
        /// Local instance name
        #[arg(short, long, default_value = "dev")]
        name: String,
        /// Local gateway port
        #[arg(long, default_value_t = crate::config::DEFAULT_LOCAL_PORT)]
        port: u16,
        /// Use on-disk storage backed by a local MinIO container
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
        /// Cloud database as cluster:<id> or tenant:<id>
        #[arg(long)]
        database: Option<String>,
        /// Owning project ID; required when the database cannot be derived
        #[arg(long)]
        project: Option<String>,
        /// Owning workspace ID
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
    /// Add a local v2 development instance
    Local {
        /// Local instance name
        #[arg(short, long)]
        name: String,
        /// Local gateway port
        #[arg(long, default_value_t = crate::config::DEFAULT_LOCAL_PORT)]
        port: u16,
        /// Use on-disk storage backed by a local MinIO container
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
        /// Cloud database as cluster:<id> or tenant:<id>
        #[arg(long)]
        database: Option<String>,
        /// Owning project ID; defaults to the linked project
        #[arg(long)]
        project: Option<String>,
        /// Owning workspace ID
        #[arg(long)]
        workspace: Option<String>,
    },
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
pub enum ConfigOutputFormat {
    #[default]
    Human,
    Json,
}

#[derive(Subcommand)]
pub enum ConfigAction {
    /// Discover accessible workspaces
    Workspace {
        #[command(subcommand)]
        action: WorkspaceAction,
    },
    /// Manage linked project selection
    Project {
        #[command(subcommand)]
        action: ProjectConfigAction,
    },
    /// List Enterprise clusters
    Cluster {
        #[command(subcommand)]
        action: ClusterConfigAction,
    },
}

#[derive(Subcommand)]
pub enum WorkspaceAction {
    /// List accessible workspaces
    List {
        #[arg(long, value_enum, default_value_t = ConfigOutputFormat::Human)]
        format: ConfigOutputFormat,
    },
    /// Get a workspace by ID
    Get {
        workspace: String,
        #[arg(long, value_enum, default_value_t = ConfigOutputFormat::Human)]
        format: ConfigOutputFormat,
    },
}

#[derive(Subcommand)]
pub enum ProjectConfigAction {
    /// List projects in an explicit or linked workspace
    List {
        #[arg(long)]
        workspace_id: Option<String>,
        #[arg(long, value_enum, default_value_t = ConfigOutputFormat::Human)]
        format: ConfigOutputFormat,
    },
    /// Get a project; defaults to the project linked in helix.toml
    Get {
        project: Option<String>,
        #[arg(long, value_enum, default_value_t = ConfigOutputFormat::Human)]
        format: ConfigOutputFormat,
    },
    /// Create a project in an explicit workspace
    Create {
        #[arg(long)]
        workspace: String,
        #[arg(long)]
        slug: String,
        #[arg(long)]
        name: String,
        #[arg(long, value_enum, default_value_t = ConfigOutputFormat::Human)]
        format: ConfigOutputFormat,
    },
    /// Delete a project; defaults to the project linked in helix.toml
    Delete {
        project: Option<String>,
        #[arg(short = 'y', long)]
        yes: bool,
    },
    /// Link this helix.toml to a Cloud project by ID
    Link {
        project: String,
        #[arg(long)]
        workspace: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum ClusterConfigAction {
    /// List Enterprise clusters
    List {
        #[arg(long)]
        workspace_id: Option<String>,
        #[arg(long)]
        project_id: Option<String>,
        #[arg(long, value_enum, default_value_t = ConfigOutputFormat::Human)]
        format: ConfigOutputFormat,
    },

    /// Get a cluster by ID
    Get {
        cluster_id: String,
        #[arg(long, value_enum, default_value_t = ConfigOutputFormat::Human)]
        format: ConfigOutputFormat,
    },

    /// List indexes in an Enterprise cluster
    #[command(alias = "indices")]
    Indexes {
        /// Enterprise cluster ID; defaults to the current project's Enterprise instance
        #[arg(long, value_name = "CLUSTER_ID")]
        cluster_id: Option<String>,
        #[arg(long, value_enum, default_value_t = ConfigOutputFormat::Human)]
        format: ConfigOutputFormat,
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
    /// List databases in an explicit or linked project
    List {
        #[arg(long)]
        project: Option<String>,
        #[arg(long, value_enum, default_value_t = ConfigOutputFormat::Human)]
        format: ConfigOutputFormat,
    },
    /// Get a database by cluster:<id> or tenant:<id>
    Get {
        database: Option<String>,
        #[arg(long, value_enum, default_value_t = ConfigOutputFormat::Human)]
        format: ConfigOutputFormat,
    },
    /// Create a tenant database and display its default read-write key once
    Create {
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        cluster: Option<String>,
        #[arg(long)]
        name: String,
        #[arg(long)]
        slug: String,
        #[arg(long)]
        plan: Option<String>,
        #[arg(long, value_enum, default_value_t = ConfigOutputFormat::Human)]
        format: ConfigOutputFormat,
    },
    /// Delete a database by cluster:<id> or tenant:<id>
    Delete {
        database: Option<String>,
        #[arg(short = 'y', long)]
        yes: bool,
    },
    /// List active indexes
    #[command(alias = "indices")]
    Indexes {
        database: Option<String>,
        #[arg(long, value_enum, default_value_t = ConfigOutputFormat::Human)]
        format: ConfigOutputFormat,
    },
    /// Manage application database keys
    Key {
        #[command(subcommand)]
        action: DatabaseKeyAction,
    },
}

#[derive(Subcommand)]
pub enum DatabaseKeyAction {
    /// Create an application key and display its token once
    Create {
        database: Option<String>,
        #[arg(long)]
        name: Option<String>,
        #[arg(long, value_enum)]
        access: DatabaseKeyAccess,
    },
    /// List application keys; tokens are never returned
    List {
        database: Option<String>,
        #[arg(long, value_enum, default_value_t = ConfigOutputFormat::Human)]
        format: ConfigOutputFormat,
    },
    /// Revoke an application key
    Revoke {
        database: Option<String>,
        #[arg(long)]
        key: String,
        #[arg(short = 'y', long)]
        yes: bool,
    },
}

#[derive(Subcommand)]
pub enum ServiceCredentialAction {
    /// Create a workspace-owned headless credential; displays its token once
    Create {
        #[arg(long)]
        workspace: String,
        #[arg(long)]
        name: String,
        /// Project grant: PROJECT_ID=project-read,query-read (repeatable)
        #[arg(long = "grant")]
        grants: Vec<String>,
        #[arg(long)]
        expires_at: Option<String>,
    },
    /// List credentials owned by a workspace
    List {
        #[arg(long)]
        workspace: String,
        #[arg(long, value_enum, default_value_t = ConfigOutputFormat::Human)]
        format: ConfigOutputFormat,
    },
    /// Get a credential; its secret is never returned
    Get {
        #[arg(long)]
        workspace: String,
        credential: String,
        #[arg(long, value_enum, default_value_t = ConfigOutputFormat::Human)]
        format: ConfigOutputFormat,
    },
    /// Update name, expiry, or project grants without rotating the secret
    Update {
        #[arg(long)]
        workspace: String,
        credential: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long = "grant")]
        grants: Vec<String>,
        #[arg(long)]
        expires_at: Option<String>,
        #[arg(long)]
        clear_expiry: bool,
    },
    /// Revoke a service credential
    Revoke {
        #[arg(long)]
        workspace: String,
        credential: String,
        #[arg(short = 'y', long)]
        yes: bool,
    },
}

#[derive(Subcommand)]
pub enum CloudApiAction {
    Get {
        path: String,
    },
    Post {
        path: String,
        #[arg(long, default_value = "{}")]
        json: String,
    },
    Patch {
        path: String,
        #[arg(long, default_value = "{}")]
        json: String,
    },
    Delete {
        path: String,
    },
}
