//! Fuzzes arena parsing against owned parsing of public query JSON: for any
//! input, the arena and owned parses agree on the verdict, the error message
//! and the request.

#![no_main]

use helix_ast::arena::{Bump, IntoOwned};
use helix_ast::query::{ArenaQueryRequest, QueryRequest};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let bump = Bump::new();
    let owned = QueryRequest::from_json_slice(data).map_err(|error| error.to_string());
    let arena = ArenaQueryRequest::from_json_slice(&bump, data)
        .map(IntoOwned::into_owned)
        .map_err(|error| error.to_string());
    assert_eq!(arena, owned);
});
