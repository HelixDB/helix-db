use helix_db::helix_engine::{
    storage_core::version_info::{
        ItemInfo, Transition, TransitionFn, TransitionSubmission, VersionInfo,
    },
    traversal_core::{HelixGraphEngine, HelixGraphEngineOpts},
};
use helix_db::helix_gateway::mcp::mcp::{MCPHandlerFn, MCPHandlerSubmission};
use helix_db::helix_gateway::{
    gateway::HelixGateway,
    router::router::{HandlerFn, HandlerSubmission},
};
use std::{collections::HashMap, sync::Arc};
use tracing::{info, Level};
use tracing_subscriber::util::SubscriberInitExt;

mod queries;

fn main() {
    let env_res = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .finish()
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
        Ok(val) => val.parse::<u16>().unwrap(),
        Err(_) => 6969,
    };

    println!("Running with the following setup:");
    println!("\tconfig: {config:?}");
    println!("\tpath: {}", path.display());
    println!("\tport: {port}");

    let transition_fns = inventory::iter::<TransitionSubmission>.into_iter().fold(
        HashMap::new(),
        |mut acc,
         TransitionSubmission(Transition {
             item_label,
             func,
             from_version,
             to_version,
         })| {
            acc.entry(item_label.to_string())
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

    let graph = Arc::new(HelixGraphEngine::new(opts.clone()).unwrap());

    // generates routes from handler proc macro
    let submissions: Vec<_> = inventory::iter::<HandlerSubmission>.into_iter().collect();
    println!("Found {} route submissions", submissions.len());

    let query_routes: HashMap<String, HandlerFn> = inventory::iter::<HandlerSubmission>
        .into_iter()
        .map(|submission| {
            println!(
                "Processing POST submission for handler: {}",
                submission.0.name
            );
            let handler = &submission.0;
            let func: HandlerFn = Arc::new(handler.func);
            (handler.name.to_string(), func)
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
    
    // Get configurable thread pool sizes from environment
    let worker_threads = std::env::var("HELIX_WORKER_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(num_cpus::get()); // Default to number of CPUs
        
    let io_threads = std::env::var("HELIX_IO_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(num_cpus::get() / 4) // Default to num_cpus / 4
        .max(2); // At least 2 IO threads
        
    info!("Starting gateway with worker_threads={}, io_threads={}", worker_threads, io_threads);
    
    let gateway = HelixGateway::new(
        &format!("0.0.0.0:{port}"),
        graph,
        worker_threads,
        io_threads,
        Some(query_routes),
        Some(mcp_routes),
        Some(opts),
    );

    gateway.run().unwrap()
}
