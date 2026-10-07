//! Fuzzes arena parsing against owned parsing of public query JSON: for any
//! input, each JSON backend's arena and owned parses agree on the verdict,
//! the error message and the request.

#![no_main]

use helix_ast::arena::{Bump, IntoOwned};
use helix_ast::query::{ArenaQueryRequest, QueryRequest};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // The arena entry point checks UTF-8 before parsing and words that error
    // differently; every other outcome must match exactly.
    let utf8 = std::str::from_utf8(data).is_ok();
    let bump = Bump::new();
    let owned = QueryRequest::from_json_slice(data).map_err(|error| error.to_string());
    let arena = ArenaQueryRequest::from_json_slice(&bump, data)
        .map(IntoOwned::into_owned)
        .map_err(|error| error.to_string());
    match utf8 {
        true => assert_eq!(arena, owned),
        false => assert!(arena.is_err() && owned.is_err()),
    }

    let simd_owned =
        QueryRequest::from_json_slice_mut(&mut data.to_vec()).map_err(|error| error.to_string());
    let simd_arena = ArenaQueryRequest::from_json_slice_mut(&bump, &mut data.to_vec())
        .map(IntoOwned::into_owned)
        .map_err(|error| error.to_string());
    assert_eq!(simd_arena, simd_owned);
});
