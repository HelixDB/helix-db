//! Fuzzes V2 physical-work, upload, proof, reachability, and GC values.

#![no_main]

use libfuzzer_sys::fuzz_target;

mod seed;

fuzz_target!(|data: &[u8]| {
    let data = seed::bytes(data);
    db::fuzzing::decode_current_index_v2_work(&data);
});
