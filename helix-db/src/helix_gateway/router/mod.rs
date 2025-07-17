use crate::{
    helix_engine::types::GraphError,
    helix_gateway::router::router::{BasicHandlerFn, HandlerInput},
    protocol::response::Response,
};

pub mod dynamic;
pub mod router;

pub trait QueryHandler: Send + Sync {
    fn handle(&self, input: &HandlerInput, response: &mut Response) -> Result<(), GraphError>;
}

impl QueryHandler for BasicHandlerFn {
    fn handle(&self, input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
        self(input, response)
    }
}
