use crate::CloudDeploymentTypeCommand;
use crate::commands::integrations::ecr::{EcrAuthType, EcrManager};
use crate::commands::integrations::fly::{FlyAuthType, FlyManager, Privacy, VmSize};
use crate::commands::integrations::helix::HelixManager;
use crate::config::{CloudConfig, HelixConfig};
use crate::docker::DockerManager;
use crate::errors::project_error;
use crate::project::ProjectContext;
use crate::utils::{print_instructions, print_status, print_success, Template};
use eyre::Result;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

pub async fn run(
    path: Option<String>,
    template: Option<String>,
    queries_path: String,
    deployment_type: Option<CloudDeploymentTypeCommand>,
) -> Result<()> {
    let project_dir = match path {
        Some(p) => std::path::PathBuf::from(p),
        None => env::current_dir()?,
    };

    let project_name = project_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("helix-project");

    let config_path = project_dir.join("helix.toml");

    if config_path.exists() {
        return Err(project_error(format!(
            "helix.toml already exists in {}",
            project_dir.display()
        ))
        .with_hint("use 'helix add <instance_name>' to add a new instance to the existing project")
        .into());
    }

    print_status(
        "INIT",
        &format!("Initializing Helix project: {project_name}"),
    );

    // Create project directory if it doesn't exist
    fs::create_dir_all(&project_dir)?;

    // Create default helix.toml with custom queries path
    let mut config = HelixConfig::default_config(project_name);
    config.project.queries = std::path::PathBuf::from(&queries_path);
    config.save_to_file(&config_path)?;
    // Create project structure
    create_project_structure(&project_dir, &queries_path)?;

    // Create template-specific files
    let template_type = Template::from(template)?;
    match template_type {
        Template::Python => {
            let output_path = project_dir.join("helix-python");
            create_python_project(&output_path, &queries_path, project_name)?;
            // Enable Python codegen in config with correct output path
            config.project.codegen.python.enabled = true;
            config.project.codegen.python.output = PathBuf::from("./helix-python/");
            config.save_to_file(&config_path)?;
        },
        _ => {
            // Other templates not yet implemented
        }
    }

    // Initialize deployment type based on flags

    match deployment_type {
        Some(deployment) => {
            match deployment {
                CloudDeploymentTypeCommand::Helix { region, .. } => {
                    // Initialize Helix deployment
                    let cwd = env::current_dir()?;
                    let project_context = ProjectContext::find_and_load(Some(&cwd))?;

                    // Create Helix manager
                    let helix_manager = HelixManager::new(&project_context);

                    // Create cloud instance configuration
                    let cloud_config = helix_manager
                        .create_instance_config(project_name, region)
                        .await?;

                    // Initialize the cloud cluster
                    helix_manager
                        .init_cluster(project_name, &cloud_config)
                        .await?;

                    // Insert into config
                    config.cloud.insert(
                        project_name.to_string(),
                        CloudConfig::Helix(cloud_config.clone()),
                    );

                    // save config
                    config.save_to_file(&config_path)?;
                }
                CloudDeploymentTypeCommand::Ecr { .. } => {
                    let cwd = env::current_dir()?;
                    let project_context = ProjectContext::find_and_load(Some(&cwd))?;

                    // Create ECR manager
                    let ecr_manager =
                        EcrManager::new(&project_context, EcrAuthType::AwsCli).await?;

                    // Create ECR configuration
                    let ecr_config = ecr_manager
                        .create_ecr_config(
                            project_name,
                            None, // Use default region
                            EcrAuthType::AwsCli,
                        )
                        .await?;

                    // Initialize the ECR repository
                    ecr_manager
                        .init_repository(project_name, &ecr_config)
                        .await?;

                    // Save configuration to ecr.toml
                    ecr_manager.save_config(project_name, &ecr_config).await?;

                    // Update helix.toml with cloud config
                    config.cloud.insert(
                        project_name.to_string(),
                        CloudConfig::Ecr(ecr_config.clone()),
                    );
                    config.save_to_file(&config_path)?;

                    print_status("ECR", "AWS ECR repository initialized successfully");
                }
                CloudDeploymentTypeCommand::Fly {
                    auth,
                    volume_size,
                    vm_size,
                    public,
                    ..
                } => {
                    let cwd = env::current_dir()?;
                    let project_context = ProjectContext::find_and_load(Some(&cwd))?;
                    let docker = DockerManager::new(&project_context);

                    // Parse configuration with proper error handling
                    let auth_type = FlyAuthType::try_from(auth)?;

                    // Parse vm_size directly using match statement to avoid trait conflicts
                    let vm_size_parsed = VmSize::try_from(vm_size)?;
                    let privacy = Privacy::from(!public); // public=true means privacy=false (Public)

                    // Create Fly.io manager
                    let fly_manager = FlyManager::new(&project_context, auth_type.clone()).await?;
                    // Create instance configuration
                    let instance_config = fly_manager.create_instance_config(
                        &docker,
                        project_name, // Use "default" as the instance name for init
                        volume_size,
                        vm_size_parsed,
                        privacy,
                        auth_type,
                    );

                    // Initialize the Fly.io app
                    fly_manager.init_app(project_name, &instance_config).await?;

                    config.cloud.insert(
                        project_name.to_string(),
                        CloudConfig::FlyIo(instance_config.clone()),
                    );
                    config.save_to_file(&config_path)?;
                }
                _ => {}
            }
        }
        None => {
            // Local instance is the default, config already saved above
        }
    }

    print_success(&format!(
        "Helix project initialized in {}",
        project_dir.display()
    ));
    let queries_path_clean = queries_path.trim_end_matches('/');
    print_instructions(
        "Next steps:",
        &[
            &format!("Edit {queries_path_clean}/schema.hx to define your data model"),
            &format!("Add queries to {queries_path_clean}/queries.hx"),
            "Run 'helix build dev' to compile your project",
            "Run 'helix push dev' to start your development instance",
        ],
    );

    Ok(())
}

fn create_project_structure(project_dir: &Path, queries_path: &str) -> Result<()> {
    // Create directories
    fs::create_dir_all(project_dir.join(".helix"))?;
    fs::create_dir_all(project_dir.join(queries_path))?;

    // Create default schema.hx with proper Helix syntax
    let default_schema = r#"// Start building your schema here.
//
// The schema is used to to ensure a level of type safety in your queries.
//
// The schema is made up of Node types, denoted by N::,
// and Edge types, denoted by E::
//
// Under the Node types you can define fields that
// will be stored in the database.
//
// Under the Edge types you can define what type of node
// the edge will connect to and from, and also the
// properties that you want to store on the edge.
//
// Example:
//
// N::User {
//     Name: String,
//     Label: String,
//     Age: I64,
//     IsAdmin: Boolean,
// }
//
// E::Knows {
//     From: User,
//     To: User,
//     Properties: {
//         Since: I64,
//     }
// }
"#;
    fs::write(
        project_dir.join(queries_path).join("schema.hx"),
        default_schema,
    )?;

    // Create default queries.hx with proper Helix query syntax in the queries directory
    let default_queries = r#"// Start writing your queries here.
//
// You can use the schema to help you write your queries.
//
// Queries take the form:
//     QUERY {query name}({input name}: {input type}) =>
//         {variable} <- {traversal}
//         RETURN {variable}
//
// Example:
//     QUERY GetUserFriends(user_id: String) =>
//         friends <- N<User>(user_id)::Out<Knows>
//         RETURN friends
//
//
// For more information on how to write queries,
// see the documentation at https://docs.helix-db.com
// or checkout our GitHub at https://github.com/HelixDB/helix-db
"#;
    fs::write(
        project_dir.join(queries_path).join("queries.hx"),
        default_queries,
    )?;

    // Create .gitignore
    let gitignore = r#".helix/
target/
*.log
"#;
    fs::write(project_dir.join(".gitignore"), gitignore)?;

    Ok(())
}

fn create_python_project(project_dir: &Path, queries_path: &str, project_name: &str) -> Result<()> {
    print_status("PYTHON", "Creating Python project structure");

    // Create Python source directory
    let src_dir = project_dir.join("src");
    fs::create_dir_all(&src_dir)?;

    // Create __init__.py files
    fs::write(src_dir.join("__init__.py"),
        format!(r#""""{}
Auto-generated HelixDB Python client
"""
from .client import HelixDBClient

__version__ = "0.1.0"
__all__ = ["HelixDBClient"]
"#, project_name))?;

    // Create client.py
    let client_content = r#""""HelixDB client wrapper"""
from helix import Client
from typing import Optional, Dict, Any
import os


class HelixDBClient:
    """Wrapper for HelixDB client with project-specific configuration"""

    def __init__(
        self,
        local: bool = True,
        port: int = 6969,
        api_endpoint: Optional[str] = None,
        api_key: Optional[str] = None,
        verbose: bool = False,
        max_workers: int = 1
    ):
        """Initialize HelixDB client

        Args:
            local: Whether to connect to local instance
            port: Port for local connection (default: 6969)
            api_endpoint: Remote API endpoint URL
            api_key: API key for remote connection
            verbose: Enable verbose logging
            max_workers: Number of concurrent workers
        """
        self._client = Client(
            local=local,
            port=port,
            api_endpoint=api_endpoint or os.getenv("HELIX_API_ENDPOINT"),
            api_key=api_key or os.getenv("HELIX_API_KEY"),
            verbose=verbose,
            max_workers=max_workers
        )

    @property
    def client(self) -> Client:
        """Get the underlying helix-py client"""
        return self._client

    def query(self, name: str, payload: Optional[Dict] = None) -> Any:
        """Execute a query

        Args:
            name: Query name
            payload: Query parameters

        Returns:
            Query results
        """
        return self._client.query(name, payload)
"#;
    fs::write(src_dir.join("client.py"), client_content)?;

    // Create placeholder files for generated code
    fs::write(src_dir.join("models.py"),
        "\"\"\"Generated models will appear here after running 'helix build'\"\"\"\n")?;

    fs::write(src_dir.join("queries.py"),
        "\"\"\"Generated query functions will appear here after running 'helix build'\"\"\"\n")?;

    // Create requirements.txt
    let requirements = r#"helix-py>=0.2.30
pydantic>=2.11.9
python-dotenv>=1.1.1
"#;
    fs::write(project_dir.join("requirements.txt"), requirements)?;

    // Create pyproject.toml
    let pyproject = format!(r#"[build-system]
requires = ["setuptools>=45", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "{}"
version = "0.1.0"
description = "HelixDB Python client for {}"
requires-python = ">=3.13"
dependencies = [
    "helix-py>=0.2.30",
    "pydantic>=2.11.9",
    "python-dotenv>=1.1.1",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.4.2",
    "pytest-asyncio>=1.2.0",
    "mypy>=1.18.2",
    "black>=25.9.0",
    "ruff>=0.1.0",
]
"#, project_name, project_name);
    fs::write(project_dir.join("pyproject.toml"), pyproject)?;

    // Create .env.example
    let env_example = r#"# HelixDB Configuration
HELIX_API_ENDPOINT=https://your-api-endpoint.com
HELIX_API_KEY=your-api-key-here
"#;
    fs::write(project_dir.join(".env.example"), env_example)?;

    // Create README.md
    let readme = format!(r#"# {}

A HelixDB Python client project.

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure environment (for remote connections):
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

## Development

1. Define your schema in `{}/schema.hx`
2. Write queries in `{}/queries.hx`
3. Build the project to generate Python code:
   ```bash
   helix build dev
   ```
4. Start your local instance:
   ```bash
   helix push dev
   ```

## Usage

```python
from {}.client import HelixDBClient

# Connect to local instance
client = HelixDBClient(local=True, port=6969)

# Or connect to remote instance
client = HelixDBClient(
    local=False,
    api_endpoint="https://your-api.com",
    api_key="your-key"
)
```

## Testing

```bash
pytest tests/
```
"#, project_name, queries_path, queries_path, project_name.replace("-", "_"));
    fs::write(project_dir.join("README.md"), readme)?;

    print_success("Python project structure created");

    Ok(())
}
