use clap::{Args, Subcommand, ValueEnum};

pub mod commands;
pub mod config;
pub mod enterprise_cloud;
pub mod errors;
pub(crate) mod external_tools;
pub(crate) mod host_actions;
pub mod local_runtime;
pub mod metrics_sender;
pub mod output;
pub mod port;
pub mod project;
pub mod prompts;
pub(crate) mod service_endpoints;
pub mod setup;
pub mod sse_client;
pub mod ts_query;
pub mod update;
pub mod utils;

#[derive(Subcommand)]
pub enum AuthAction {
    /// Login to Helix Cloud
    Login,
    /// Logout from Helix Cloud
    Logout,
    /// Rotate an Enterprise cluster API key
    CreateKey {
        /// Cluster ID
        cluster: String,
    },
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
        /// Cloud cluster ID; omit to pick one from the cluster list interactively
        #[arg(long)]
        cluster_id: Option<String>,
        /// Runtime gateway URL for queries
        #[arg(long)]
        gateway_url: Option<String>,
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
        /// Cloud cluster ID; omit to pick one from the cluster list interactively
        #[arg(long)]
        cluster_id: Option<String>,
        /// Runtime gateway URL for queries
        #[arg(long)]
        gateway_url: Option<String>,
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
    /// Manage active workspace selection
    Workspace {
        #[command(subcommand)]
        action: WorkspaceConfigAction,
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
pub enum WorkspaceConfigAction {
    /// List accessible workspaces
    List {
        #[arg(long, value_enum, default_value_t = ConfigOutputFormat::Human)]
        format: ConfigOutputFormat,
    },
    /// Show selected workspace
    Show {
        #[arg(long, value_enum, default_value_t = ConfigOutputFormat::Human)]
        format: ConfigOutputFormat,
    },
    /// Select workspace by slug or ID
    Switch {
        workspace: String,
        #[arg(long)]
        id: bool,
    },
}

#[derive(Subcommand)]
pub enum ProjectConfigAction {
    /// List projects in the selected workspace
    List {
        #[arg(long)]
        workspace_id: Option<String>,
        #[arg(long, value_enum, default_value_t = ConfigOutputFormat::Human)]
        format: ConfigOutputFormat,
    },
    /// Show linked project
    Show {
        #[arg(long, value_enum, default_value_t = ConfigOutputFormat::Human)]
        format: ConfigOutputFormat,
    },
    /// Link this project to a cloud project by name or ID
    Switch {
        project: String,
        #[arg(long)]
        id: bool,
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
