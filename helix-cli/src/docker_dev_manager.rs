use chrono::Local;
use helix_db::utils::styled_string::StyledString;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::PathBuf;
use std::process::Command;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DockerDevInstance {
    pub container_name: String,
    pub port: u16,
    pub started_at: String,
    pub status: DockerDevStatus,
    pub log_file: PathBuf,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DockerDevStatus {
    Running,
    Stopped,
    NotFound,
}

pub struct DockerDevManager {
    dockerdev_dir: PathBuf,
    logs_dir: PathBuf,
    instance_file: PathBuf,
}

const CONTAINER_NAME: &str = "helix-dockerdev";
const DEFAULT_PORT: u16 = 6969;

#[derive(Debug, Clone)]
enum ComposeCommand {
    DockerCompose,
    DockerCompose2,
}

impl DockerDevManager {
    pub fn new() -> io::Result<Self> {
        let home_dir = dirs::home_dir().expect("Could not find home directory");
        let helix_dir = home_dir.join(".helix");
        let dockerdev_dir = helix_dir.join("dockerdev");
        let logs_dir = dockerdev_dir.join("logs");

        // Create necessary directories
        fs::create_dir_all(&helix_dir)?;
        fs::create_dir_all(&dockerdev_dir)?;
        fs::create_dir_all(&logs_dir)?;

        Ok(Self {
            dockerdev_dir: dockerdev_dir.clone(),
            logs_dir,
            instance_file: dockerdev_dir.join("dockerdev_instance.json"),
        })
    }

    fn get_compose_command(&self) -> Result<ComposeCommand, String> {
        // Try docker-compose first
        let compose_check = Command::new("docker-compose").args(["--version"]).output();
        if let Ok(output) = compose_check
            && output.status.success()
        {
            return Ok(ComposeCommand::DockerCompose);
        }

        // Try newer docker compose syntax
        let compose_check = Command::new("docker")
            .args(["compose", "--version"])
            .output();
        if let Ok(output) = compose_check
            && output.status.success()
        {
            return Ok(ComposeCommand::DockerCompose2);
        }

        Err("Neither 'docker-compose' nor 'docker compose' command is available".to_string())
    }

    fn create_compose_command(&self, args: &[&str]) -> Result<Command, String> {
        let compose_cmd = self.get_compose_command()?;

        let mut command = match compose_cmd {
            ComposeCommand::DockerCompose => {
                let mut cmd = Command::new("docker-compose");
                cmd.args(args);
                cmd
            }
            ComposeCommand::DockerCompose2 => {
                let mut cmd = Command::new("docker");
                cmd.arg("compose");
                cmd.args(args);
                cmd
            }
        };

        // Set environment variables and working directory
        if let Some(home) = dirs::home_dir() {
            command.env("HOME", home);
        }
        command.current_dir("helix-container");

        Ok(command)
    }

    pub fn run(&self, background: bool, port: Option<u16>) -> Result<(), String> {
        // Check if Docker is available
        self.check_docker_available()?;

        // Check if already running
        if self.is_running()? {
            return Err(
                "Docker development instance is already running. Use 'helix dockerdev stop' first."
                    .to_string(),
            );
        }

        let port = port.unwrap_or(DEFAULT_PORT);

        // Validate port range (u16 max is 65535, so only check lower bound)
        if port < 1024 {
            return Err("Port must be 1024 or higher".to_string());
        }

        // Check if port is available
        if self.is_port_in_use(port)? {
            return Err(format!(
                "Port {port} is already in use. Please choose a different port with --port"
            ));
        }

        // Verify docker-compose.yml exists
        if !PathBuf::from("helix-container/docker-compose.yml").exists() {
            return Err("docker-compose.yml not found in helix-container directory. Please ensure you're running from the project root.".to_string());
        }

        // Create log file with timestamp in the mounted directory
        let timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();
        let log_file = self.logs_dir.join(format!("dockerdev-{timestamp}.log"));

        println!(
            "{}",
            format!("Starting Helix development container on port {port}...")
                .blue()
                .bold()
        );

        if background {
            // Run in background mode
            let mut command = self.create_compose_command(&["up", "--build", "-d"])?;
            command.env("HELIX_PORT", port.to_string());

            let output = command
                .output()
                .map_err(|e| format!("Failed to start container: {e}"))?;

            if !output.status.success() {
                let error = String::from_utf8_lossy(&output.stderr);
                return Err(format!("Failed to start container: {error}"));
            }

            println!("{}", "Container started in background mode".green().bold());
            println!(
                "{}",
                format!(
                    "Container logs accessible in: {}",
                    self.dockerdev_dir.display()
                )
                .normal()
            );
            println!("{}", "View logs with: helix dockerdev logs".normal());
            println!(
                "{}",
                format!("Access at: http://localhost:{port}").blue().bold()
            );

            // Wait a moment for container to start and create initial logs
            std::thread::sleep(std::time::Duration::from_secs(2));
        } else {
            // Run in foreground mode
            println!(
                "{}",
                "Container started in foreground mode. Press Ctrl+C to stop."
                    .green()
                    .bold()
            );
            println!(
                "{}",
                format!(
                    "Container logs accessible in: {}",
                    self.dockerdev_dir.display()
                )
                .normal()
            );
            println!(
                "{}",
                format!("Access at: http://localhost:{port}").blue().bold()
            );

            let mut command = self.create_compose_command(&["up", "--build"])?;
            command.env("HELIX_PORT", port.to_string());

            let mut child = command
                .spawn()
                .map_err(|e| format!("Failed to start container: {e}"))?;

            let _ = child.wait();
        }

        // Save instance info
        let instance = DockerDevInstance {
            container_name: CONTAINER_NAME.to_string(),
            port,
            started_at: Local::now().to_rfc3339(),
            status: DockerDevStatus::Running,
            log_file,
        };

        self.save_instance(&instance)?;
        Ok(())
    }

    pub fn stop(&self) -> Result<(), String> {
        // Check if Docker is available
        self.check_docker_available()?;

        if !self.is_running()? {
            println!(
                "{}",
                "No Docker development instance is currently running".yellow()
            );
            return Ok(());
        }

        println!(
            "{}",
            "Stopping Docker development instance...".blue().bold()
        );

        let output = self
            .create_compose_command(&["stop"])?
            .output()
            .map_err(|e| format!("Failed to stop container: {e}"))?;

        if !output.status.success() {
            let error = String::from_utf8_lossy(&output.stderr);
            return Err(format!("Failed to stop container: {error}"));
        }

        // Update instance status
        if let Ok(mut instance) = self.load_instance() {
            instance.status = DockerDevStatus::Stopped;
            self.save_instance(&instance)?;
        }

        println!("{}", "Docker development instance stopped".green().bold());
        println!(
            "{}",
            "Data and logs are preserved. Use 'helix dockerdev run' to start again.".normal()
        );
        Ok(())
    }

    pub fn delete(&self) -> Result<(), String> {
        // Check if Docker is available
        self.check_docker_available()?;

        println!(
            "{}",
            "Stopping and removing Docker development instance and data..."
                .red()
                .bold()
        );

        // Stop and remove containers, networks, and volumes
        let output = self
            .create_compose_command(&["down", "-v", "--remove-orphans"])?
            .output()
            .map_err(|e| format!("Failed to remove container: {e}"))?;

        if !output.status.success() {
            let error = String::from_utf8_lossy(&output.stderr);
            return Err(format!("Failed to remove container: {error}"));
        }

        // Remove instance file
        if self.instance_file.exists() {
            std::fs::remove_file(&self.instance_file)
                .map_err(|e| format!("Failed to remove instance file: {e}"))?;
        }

        // Optionally clean up the dockerdev directory (but preserve logs)
        println!(
            "{}",
            "Docker development instance and Docker volumes removed"
                .green()
                .bold()
        );
        println!(
            "{}",
            format!("Host data in {} is preserved", self.dockerdev_dir.display()).normal()
        );
        Ok(())
    }

    pub fn status(&self) -> Result<(), String> {
        // Check if Docker is available
        self.check_docker_available()?;

        match self.get_container_status()? {
            DockerDevStatus::Running => {
                if let Ok(instance) = self.load_instance() {
                    println!("{}", "Docker Development Instance Status".bold());
                    println!("Status: {}", "Running".green().bold());
                    println!("Container: {}", instance.container_name);
                    println!(
                        "URL: {}",
                        format!("http://localhost:{}", instance.port).blue().bold()
                    );
                    println!("Started: {}", instance.started_at);
                    println!("Host mount: {}", self.dockerdev_dir.display());
                    println!("Container logs: {}", self.logs_dir.display());
                } else {
                    println!(
                        "{}",
                        "Status: Running (no instance data found)".green().bold()
                    );
                }
            }
            DockerDevStatus::Stopped => {
                println!("{}", "Docker Development Instance Status".bold());
                println!("Status: {}", "Stopped".yellow().bold());
                println!("Data preserved in: {}", self.dockerdev_dir.display());
                println!("{}", "Use 'helix dockerdev run' to start".normal());
            }
            DockerDevStatus::NotFound => {
                println!("{}", "Docker Development Instance Status".bold());
                println!("Status: {}", "Not Found".red());
                println!("{}", "No Docker development instance exists".normal());
                println!(
                    "{}",
                    "Use 'helix dockerdev run' to create and start".normal()
                );
            }
        }
        Ok(())
    }

    pub fn logs(&self, follow: bool, lines: Option<u32>) -> Result<(), String> {
        // Check if Docker is available
        self.check_docker_available()?;

        if !self.is_running()? {
            return Err("No Docker development instance is currently running".to_string());
        }

        let mut command = Command::new("docker");
        command.args(["logs"]);

        if follow {
            command.arg("-f");
        }

        if let Some(n) = lines {
            command.arg("--tail");
            command.arg(n.to_string());
        } else if !follow {
            // Default to last 100 lines if not following and no lines specified
            command.arg("--tail");
            command.arg("100");
        }

        command.arg(CONTAINER_NAME);

        if follow {
            println!("{}", "Following logs (Press Ctrl+C to exit)...".normal());
        }

        let mut child = command
            .spawn()
            .map_err(|e| format!("Failed to get logs: {e}"))?;

        let _ = child.wait();
        Ok(())
    }

    /// Get the URL where the Helix instance is accessible
    pub fn get_instance_url(&self) -> Result<String, String> {
        if let Ok(instance) = self.load_instance() {
            Ok(format!("http://localhost:{}", instance.port))
        } else {
            Err("No instance information available".to_string())
        }
    }

    /// Execute a command inside the running container
    pub fn exec_command(&self, command: &[&str]) -> Result<(), String> {
        if !self.is_running()? {
            return Err("Container is not running".to_string());
        }

        let mut docker_command = Command::new("docker");
        docker_command.args(["exec", "-it", CONTAINER_NAME]);
        docker_command.args(command);

        let mut child = docker_command
            .spawn()
            .map_err(|e| format!("Failed to execute command: {e}"))?;

        let _ = child.wait();
        Ok(())
    }

    fn is_port_in_use(&self, port: u16) -> Result<bool, String> {
        use std::net::TcpListener;
        match TcpListener::bind(("127.0.0.1", port)) {
            Ok(_) => Ok(false),
            Err(_) => Ok(true),
        }
    }

    fn is_running(&self) -> Result<bool, String> {
        let output = Command::new("docker")
            .args(["ps", "-q", "-f", &format!("name={CONTAINER_NAME}")])
            .output()
            .map_err(|e| format!("Failed to check container status: {e}"))?;

        Ok(!output.stdout.is_empty())
    }

    fn get_container_status(&self) -> Result<DockerDevStatus, String> {
        // Check if running
        if self.is_running()? {
            return Ok(DockerDevStatus::Running);
        }

        // Check if stopped (exists but not running)
        let output = Command::new("docker")
            .args(["ps", "-aq", "-f", &format!("name={CONTAINER_NAME}")])
            .output()
            .map_err(|e| format!("Failed to check container status: {e}"))?;

        if !output.stdout.is_empty() {
            Ok(DockerDevStatus::Stopped)
        } else {
            Ok(DockerDevStatus::NotFound)
        }
    }

    fn save_instance(&self, instance: &DockerDevInstance) -> Result<(), String> {
        let json = serde_json::to_string(&instance)
            .map_err(|e| format!("Failed to serialize instance: {e}"))?;
        std::fs::write(&self.instance_file, json)
            .map_err(|e| format!("Failed to save instance: {e}"))?;

        Ok(())
    }

    fn load_instance(&self) -> Result<DockerDevInstance, String> {
        if !self.instance_file.exists() {
            return Err("No instance file found".to_string());
        }

        let contents = std::fs::read_to_string(&self.instance_file)
            .map_err(|e| format!("Failed to read instance file: {e}"))?;

        serde_json::from_str(&contents).map_err(|e| format!("Failed to parse instance file: {e}"))
    }

    fn check_docker_available(&self) -> Result<(), String> {
        // Check if docker command is available
        let docker_check = Command::new("docker").args(["--version"]).output();

        match docker_check {
            Ok(output) if output.status.success() => {
                // Docker is available, now check if daemon is running
                let daemon_check = Command::new("docker")
                    .args(["info"])
                    .output();

                match daemon_check {
                    Ok(output) if output.status.success() => {
                        // Docker daemon is running, now check docker-compose
                        self.check_docker_compose_available()
                    },
                    _ => Err("Docker daemon is not running. Please start Docker and try again.".to_string()),
                }
            }
            _ => Err("Docker is not installed or not available in PATH. Please install Docker and try again.".to_string()),
        }
    }

    fn check_docker_compose_available(&self) -> Result<(), String> {
        match self.get_compose_command() {
            Ok(ComposeCommand::DockerCompose2) => {
                println!(
                    "{}",
                    "Note: Using 'docker compose' syntax (newer Docker versions)".yellow()
                );
                Ok(())
            }
            Ok(ComposeCommand::DockerCompose) => Ok(()),
            Err(e) => Err(e),
        }
    }
}
