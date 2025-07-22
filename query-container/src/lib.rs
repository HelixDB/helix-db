use helixdb::{
    helix_engine::types::GraphError,
    helix_gateway::router::router::{HandlerInput, HandlerSubmission},
    protocol::response::Response,
};

mod query;

type DynQueryFn = fn(&HandlerInput, &mut Response) -> Result<(), GraphError>;

#[unsafe(no_mangle)]
pub extern "Rust" fn get_queries() -> Vec<(String, DynQueryFn)> {
    println!("get_queries called!!!!\n\n\n");
    let submissions = HandlerSubmission::collect_linked_handlers()
        .into_iter()
        .collect::<Vec<_>>();

    println!("got {} submissions", submissions.len());

    let ret = submissions
        .into_iter()
        .map(|hs| (hs.0.name.to_owned(), hs.0.func))
        .collect();

    ret
}
