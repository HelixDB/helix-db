//! Fuzzes canonical V2 catalog, operation, and global-control values.

#![no_main]

use libfuzzer_sys::fuzz_target;

mod seed;

fuzz_target!(|data: &[u8]| {
    let data = seed::bytes(data);
    let Some((&selector, payload)) = data.split_first() else {
        return;
    };
    db::fuzzing::decode_current_index_v2_record(selector, payload);
});
