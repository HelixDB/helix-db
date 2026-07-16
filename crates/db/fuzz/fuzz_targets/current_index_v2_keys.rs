//! Fuzzes scoped and database-global V2 physical key framing.

#![no_main]

use libfuzzer_sys::fuzz_target;

mod seed;

fuzz_target!(|data: &[u8]| {
    let data = seed::bytes(data);
    let Some((&selector, payload)) = data.split_first() else {
        return;
    };
    db::fuzzing::decode_current_index_v2_key(selector, payload);
});
