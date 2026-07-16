//! Fuzzes deployed text/vector rows and the additive vector descriptor.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Some((&selector, payload)) = data.split_first() else {
        return;
    };
    let payload = payload.strip_suffix(b"\n").unwrap_or(payload);
    let selector = selector.wrapping_sub(b'0');
    db::fuzzing::decode_current_search_record(selector, payload);
});
