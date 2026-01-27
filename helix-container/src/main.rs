use helix_db::helix_engine::{
    storage_core::version_info::{
        ItemInfo, Transition, TransitionFn, TransitionSubmission, VersionInfo,
    },
    traversal_core::{HelixGraphEngine, HelixGraphEngineOpts},
};
use helix_db::helix_gateway::mcp::mcp::{MCPHandlerFn, MCPHandlerSubmission};
use helix_db::helix_gateway::{
    gateway::{GatewayOpts, HelixGateway},
    router::router::{HandlerFn, HandlerSubmission},
};
use std::{collections::HashMap, sync::Arc};
use tracing::info;
use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};

mod queries;

fn main() {
    let env_res = dotenvy::dotenv();
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer().with_filter(tracing_subscriber::filter::filter_fn(
                |metadata| {
                    let target = metadata.target();
                    !target.starts_with("axum")
                        && !target.starts_with("hyper")
                        && !target.starts_with("tower")
                        && !target.starts_with("h2")
                        && !target.starts_with("reqwest")
                },
            )),
        )
        .init();

    match env_res {
        Ok(_) => info!("Loaded .env file"),
        Err(e) => info!(?e, "Didn't load .env file"),
    }

    let config = queries::config().unwrap_or_default();

    let path = match std::env::var("HELIX_DATA_DIR") {
        Ok(val) => std::path::PathBuf::from(val).join("user"),
        Err(_) => {
            println!("HELIX_DATA_DIR not set, using default");
            let home = dirs::home_dir().expect("Could not retrieve home directory");
            home.join(".helix/user")
        }
    };

    let port = match std::env::var("HELIX_PORT") {
        Ok(val) => val
            .parse::<u16>()
            .expect("HELIX_PORT must be a valid port number"),
        Err(_) => 6969,
    };

    println!("Running with the following setup:");
    println!("\tconfig: {config:#?}");
    println!("\tpath: {}", path.display());
    println!("\tport: {port}");

    let transition_fns: HashMap<&'static str, ItemInfo> =
        inventory::iter::<TransitionSubmission>.into_iter().fold(
            HashMap::new(),
            |mut acc,
             TransitionSubmission(Transition {
                 item_label,
                 func,
                 from_version,
                 to_version,
             })| {
                acc.entry(item_label)
                    .and_modify(|item_info: &mut ItemInfo| {
                        item_info.latest = item_info.latest.max(*to_version);

                        // asserts for versions
                        assert!(
                            *from_version < *to_version,
                            "from_version must be less than to_version"
                        );
                        assert!(*from_version > 0, "from_version must be greater than 0");
                        assert!(*to_version > 0, "to_version must be greater than 0");
                        assert!(
                            *to_version - *from_version == 1,
                            "to_version must be exactly 1 greater than from_version"
                        );

                        item_info.transition_fns.push(TransitionFn {
                            from_version: *from_version,
                            to_version: *to_version,
                            func: *func,
                        });
                        item_info.transition_fns.sort_by_key(|f| f.from_version);
                    });
                acc
            },
        );

    let path_str = path.to_str().expect("Could not convert path to string");
    let opts = HelixGraphEngineOpts {
        path: path_str.to_string(),
        config,
        version_info: VersionInfo(transition_fns),
    };

    let graph = Arc::new(
        HelixGraphEngine::new(opts.clone())
            .unwrap_or_else(|e| panic!("Failed to create graph engine: {e}")),
    );

    // generates routes from handler proc macro
    let submissions: Vec<_> = inventory::iter::<HandlerSubmission>.into_iter().collect();
    println!("Found {} route submissions", submissions.len());

    let (query_routes, write_routes): (
        HashMap<String, HandlerFn>,
        std::collections::HashSet<String>,
    ) = inventory::iter::<HandlerSubmission>.into_iter().fold(
        (HashMap::new(), std::collections::HashSet::new()),
        |(mut routes, mut writes), submission| {
            println!(
                "Processing POST submission for handler: {} (is_write: {})",
                submission.0.name, submission.0.is_write
            );
            let handler = &submission.0;
            let func: HandlerFn = Arc::new(handler.func);
            routes.insert(handler.name.to_string(), func);
            if handler.is_write {
                writes.insert(handler.name.to_string());
            }
            (routes, writes)
        },
    );

    let mcp_routes = inventory::iter::<MCPHandlerSubmission>
        .into_iter()
        .map(|submission| {
            println!("Processing submission for handler: {}", submission.0.name);
            let handler = &submission.0;
            let func: MCPHandlerFn = Arc::new(handler.func);
            (handler.name.to_string(), func)
        })
        .collect::<HashMap<String, MCPHandlerFn>>();

    println!("Routes: {:?}", query_routes.keys());
    println!("Write routes: {:?}", write_routes);
    let gateway = HelixGateway::new(
        &format!("0.0.0.0:{port}"),
        graph,
        GatewayOpts::DEFAULT_WORKERS_PER_CORE,
        Some(query_routes),
        Some(mcp_routes),
        Some(write_routes),
        Some(opts),
    );

    gateway.run().expect("Failed to run gateway")
}
