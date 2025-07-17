use libloading::{self, Library, Symbol};
use std::{
    collections::HashMap,
    error::Error,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    helix_engine::types::GraphError,
    helix_gateway::router::{
        router::{HandlerFn, HandlerInput, HelixRouter},
        QueryHandler,
    },
    protocol::response::Response,
};

#[derive(Clone)]
pub struct DynHandler {
    // holding this guarentees that the Symbol is still valid
    _source: Arc<Library>,
    func: extern "Rust" fn(&HandlerInput, &mut Response) -> Result<(), GraphError>,
}

impl QueryHandler for DynHandler {
    fn handle(&self, input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
        (self.func)(input, response)
    }
}

type DynQueryFn = extern "Rust" fn(&HandlerInput, &mut Response) -> Result<(), GraphError>;
type GetQueryFn = extern "Rust" fn() -> Vec<(String, DynQueryFn)>;

pub struct Plugin {
    lib: Arc<Library>,
}

impl Plugin {
    /// SAFETY: This must be called with a path to Helix query dynamic library, compiled with the same version of Rust as the main database
    pub unsafe fn open(lib_path: impl AsRef<Path>) -> Result<Self, Box<dyn Error>> {
        let lib = Library::new(lib_path.as_ref())?;
        Ok(Plugin { lib: Arc::new(lib) })
    }

    pub fn get_queries(&self) -> Result<HashMap<(String, String), HandlerFn>, Box<dyn Error>> {
        // SAFETY: If a valid file was opened it will have a get_queries function of this type
        let get_fn: Symbol<GetQueryFn> = unsafe { self.lib.get(b"get_queries")? };

        let queries = get_fn();

        let mut acc: HashMap<(String, String), HandlerFn> = HashMap::new();

        for (n, func) in queries.into_iter() {
            let handler = DynHandler {
                _source: self.lib.clone(),
                func,
            };

            acc.insert(("POST".to_string(), format!("/{n}")), Arc::new(handler));
        }
        Ok(acc)
    }
}
