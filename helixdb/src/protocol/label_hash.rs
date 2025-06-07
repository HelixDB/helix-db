use std::hash::Hasher;

#[inline(always)]
pub fn hash_label(label: &str, seed: Option<u32>) -> [u8; 4] {
    let mut hash = twox_hash::XxHash32::with_seed(seed.unwrap_or(0));
    hash.write(label.as_bytes());
    hash.finish_32().to_be_bytes()
}
