use helix_db::helix_engine::graph_core::config::Config;
use helix_db::helix_engine::graph_core::graph_core::{HelixGraphEngine, HelixGraphEngineOpts};
use helix_db::helix_gateway::mcp::mcp::{MCPHandlerFn, MCPHandlerSubmission};
use helix_db::helix_gateway::{
    gateway::{GatewayOpts, HelixGateway},
    router::router::{HandlerFn, HandlerSubmission},
};
use inventory;
use std::{collections::HashMap, sync::Arc};

mod graphvis;
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
    let submissions: Vec<_> = inventory::iter::<HandlerSubmission>.into_iter().collect();
    println!("Found {} route submissions", submissions.len());

    let post_routes: HashMap<(String, String), HandlerFn> = inventory::iter::<HandlerSubmission>
        .into_iter()
        .map(|submission| {
            println!(
                "Processing POST submission for handler: {}",
                submission.0.name
            );
            let handler = &submission.0;
            let func: HandlerFn = Arc::new(handler.func);
            (
                ("POST".to_string(), format!("/{}", handler.name.to_string())),
                func,
            )
        })
        .collect();

    // collect GET routes
    // let get_routes: HashMap<(String, String), HandlerFn> = inventory::iter::<HandlerSubmission>
    //     .into_iter()
    //     .map(|submission| {
    //         println!("Processing GET submission for handler: {}", submission.0.name);
    //         let handler = &submission.0;
    //         let func: HandlerFn = Arc::new(move |input, response| (handler.func)(input, response));
    //         (
    //             (
    //                 "GET".to_string(),
    //                 format!("/get/{}", handler.name.to_string()),
    //             ),
    //             func,
    //         )
    //     })
    // .collect();

    let routes: HashMap<(String, String), HandlerFn> = post_routes;

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
    let gateway = HelixGateway::new(
        &format!("0.0.0.0:{}", port),
        graph,
        GatewayOpts::DEFAULT_POOL_SIZE,
        Some(routes),
        Some(mcp_routes),
    )
    .await;

    println!("Starting server...");
    let a = gateway.connection_handler.accept_conns().await.unwrap();
    let _b = a.await.unwrap();
}
