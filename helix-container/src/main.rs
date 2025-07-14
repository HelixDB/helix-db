use helixdb::helix_engine::graph_core::config::Config;
use helixdb::helix_engine::graph_core::graph_core::{HelixGraphEngine, HelixGraphEngineOpts};
use helixdb::helix_gateway::mcp::mcp::{MCPHandlerFn, MCPHandlerSubmission};
use helixdb::helix_gateway::router::dynamic::Plugin;
use helixdb::helix_gateway::{
    gateway::{GatewayOpts, HelixGateway},
    router::router::{HandlerFn, HandlerSubmission},
};
use inventory;
use std::{collections::HashMap, sync::Arc};

mod queries;

#[tokio::main]
async fn main() {
    let home = dirs::home_dir().expect("Could not retrieve home directory");
    let config_path = home.join(".helix/repo/helix-db/helix-container/src/config.hx.json");
    let schema_path = home.join(".helix/repo/helix-db/helix-container/src/schema.hx");
    let config = match Config::from_files(config_path, schema_path) {
        Ok(config) => config,
        Err(e) => {
            println!("Error loading config: {}", e);
            Config::default()
        }
    };

    let path = match std::env::var("HELIX_DATA_DIR") {
        Ok(val) => std::path::PathBuf::from(val).join("user"),
        Err(_) => {
            println!("HELIX_DATA_DIR not set, using default");
            let home = dirs::home_dir().expect("Could not retrieve home directory");
            home.join(".helix/user")
        }
    };

    let port = match std::env::var("HELIX_PORT") {
        Ok(val) => val.parse::<u16>().unwrap(),
        Err(_) => 6969,
    };

    println!("Running with the following setup:");
    println!("\tconfig: {:?}", config);
    println!("\tpath: {}", path.display());
    println!("\tport: {}", port);

    let path_str = path.to_str().expect("Could not convert path to string");
    let opts = HelixGraphEngineOpts {
        path: path_str.to_string(),
        config,
    };
    let graph = Arc::new(HelixGraphEngine::new(opts).unwrap());

    // generates routes from handler proc macro
    println!("Starting route collection...");
    let submissions: Vec<_> = inventory::iter::<HandlerSubmission>.into_iter().collect();
    println!("Found {} submissions", submissions.len());

    // let routes = unsafe { Plugin::open("../target/release/libquery_container.dylib").unwrap() }
    //     .get_queries()
    //     .unwrap();

    let routes = HashMap::from_iter(
        submissions
            .into_iter()
            .map(|submission| {
                println!("Processing submission for handler: {}", submission.0.name);
                let handler = &submission.0;
                let func: HandlerFn = Arc::new(handler.func);
                (
                    (
                        "post".to_ascii_uppercase().to_string(),
                        format!("/{}", handler.name.to_string()),
                    ),
                    func,
                )
            })
            .collect::<Vec<((String, String), HandlerFn)>>(),
    );

    let mcp_submissions: Vec<_> = inventory::iter::<MCPHandlerSubmission>
        .into_iter()
        .collect();
    let mcp_routes = HashMap::from_iter(
        mcp_submissions
            .into_iter()
            .map(|submission| {
                println!("Processing submission for handler: {}", submission.0.name);
                let handler = &submission.0;
                let func: MCPHandlerFn =
                    Arc::new(move |input, response| (handler.func)(input, response));
                (
                    (
                        "post".to_ascii_uppercase().to_string(),
                        format!("/mcp/{}", handler.name.to_string()),
                    ),
                    func,
                )
            })
            .collect::<Vec<((String, String), MCPHandlerFn)>>(),
    );

    println!("Routes: {:?}", routes.keys());
    // create gateway
    let gateway = HelixGateway::new(
        &format!("0.0.0.0:{}", port),
        graph,
        GatewayOpts::DEFAULT_POOL_SIZE,
        Some(routes),
        Some(mcp_routes),
    )
    .await;

    // start server
    println!("Starting server...");
    let a = gateway.connection_handler.accept_conns().await.unwrap();
    let _b = a.await.unwrap();
}
