use crate::{
    commands::auth::require_auth,
    commands::integrations::helix::CLOUD_AUTHORITY,
    config::{CloudInstanceConfig, DbConfig},
    project::ProjectContext,
    sse_client::SseEvent,
    utils::{print_error, print_info, print_status, print_success},
};
use eyre::{OptionExt, Result, eyre};

/// Create a new cluster in Helix Cloud
pub async fn run(instance_name: &str) -> Result<()> {
    print_status("CREATE", &format!("Creating cluster: {}", instance_name));

    // Load project context
    let project = ProjectContext::find_and_load(None)?;

    // Check if this instance already exists and has a real cluster
    if let Some(existing_config) = project.config.cloud.get(instance_name) {
        if let crate::config::CloudConfig::Helix(config) = existing_config {
            // If cluster already has a real ID (not placeholder), error out
            if config.cluster_id != "YOUR_CLUSTER_ID" {
                return Err(eyre!(
                    "Instance '{}' already has a cluster (ID: {}). Cannot create a new cluster for this instance.",
                    instance_name,
                    config.cluster_id
                ));
            }
            // Otherwise, proceed to create the cluster and update the config
        } else {
            return Err(eyre!(
                "Instance '{}' exists but is not a Helix Cloud instance.",
                instance_name
            ));
        }
    }

    // Check authentication
    let credentials = require_auth().await?;

    // Connect to SSE stream for cluster creation
    // The server will send CheckoutRequired, PaymentConfirmed, CreatingProject, ProjectCreated events
    print_status("INITIATING", "Starting cluster creation...");

    let create_url = format!("https://{}/create-cluster", *CLOUD_AUTHORITY);
    let client = reqwest::Client::new();

    use reqwest_eventsource::RequestBuilderExt;
    let mut event_source = client
        .post(&create_url)
        .header("x-api-key", &credentials.helix_admin_key)
        .header("Content-Type", "application/json")
        .eventsource()?;

    let mut final_cluster_id: Option<String> = None;
    let mut checkout_opened = false;

    use futures_util::StreamExt;
    while let Some(event) = event_source.next().await {
        match event {
            Ok(reqwest_eventsource::Event::Open) => {
                // Connection opened
            }
            Ok(reqwest_eventsource::Event::Message(message)) => {
                let sse_event: SseEvent = match serde_json::from_str(&message.data) {
                    Ok(event) => event,
                    Err(e) => {
                        print_error(&format!(
                            "Failed to parse event: {} | Raw data: {}",
                            e, message.data
                        ));
                        continue;
                    }
                };

                match sse_event {
                    SseEvent::CheckoutRequired { url } => {
                        if !checkout_opened {
                            print_info("Opening Stripe checkout in your browser...");
                            print_info(&format!("If the browser doesn't open, visit: {}", url));

                            if let Err(e) = webbrowser::open(&url) {
                                print_error(&format!("Failed to open browser: {}", e));
                                print_info(&format!("Please manually open: {}", url));
                            }

                            checkout_opened = true;
                            print_status("WAITING", "Waiting for payment confirmation...");
                        }
                    }
                    SseEvent::PaymentConfirmed => {
                        print_success("Payment confirmed!");
                    }
                    SseEvent::CreatingProject => {
                        print_status("CREATING", "Creating cluster...");
                    }
                    SseEvent::ProjectCreated { cluster_id } => {
                        final_cluster_id = Some(cluster_id);
                        print_success("Cluster created successfully!");
                        event_source.close();
                        break;
                    }
                    SseEvent::Error { error } => {
                        print_error(&format!("Error: {}", error));
                        event_source.close();
                        return Err(eyre!("Cluster creation failed: {}", error));
                    }
                    _ => {
                        // Ignore other event types
                    }
                }
            }
            Err(err) => {
                print_error(&format!("Stream error: {}", err));
                return Err(eyre!("Cluster creation stream error: {}", err));
            }
        }
    }

    let cluster_id =
        final_cluster_id.ok_or_eyre("Cluster creation completed but no cluster_id received")?;

    // Save cluster configuration to helix.toml
    // If instance already exists, preserve its existing settings and just update cluster_id
    let config = if let Some(crate::config::CloudConfig::Helix(existing)) =
        project.config.cloud.get(instance_name)
    {
        CloudInstanceConfig {
            cluster_id: cluster_id.clone(),
            region: existing.region.clone(),
            build_mode: existing.build_mode,
            env_vars: existing.env_vars.clone(),
            db_config: existing.db_config.clone(),
        }
    } else {
        CloudInstanceConfig {
            cluster_id: cluster_id.clone(),
            region: None,
            build_mode: crate::config::BuildMode::Release,
            env_vars: std::collections::HashMap::new(),
            db_config: DbConfig::default(),
        }
    };

    // Update helix.toml
    let mut helix_config = project.config.clone();
    helix_config.cloud.insert(
        instance_name.to_string(),
        crate::config::CloudConfig::Helix(config),
    );

    let config_path = project.root.join("helix.toml");
    let toml_string = toml::to_string_pretty(&helix_config)?;
    std::fs::write(&config_path, toml_string)?;

    print_success(&format!(
        "Cluster '{}' created successfully! (ID: {})",
        instance_name, cluster_id
    ));
    print_info("Configuration saved to helix.toml");
    print_info(&format!(
        "You can now deploy with: helix push {}",
        instance_name
    ));

    Ok(())
}
