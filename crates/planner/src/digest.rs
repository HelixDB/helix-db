use serde::{Deserialize, Serialize};

/// Stable planner digest used for memo identity and deterministic tie-breaking.
///
/// The digest is computed from canonical JSON emitted by `serde` and fed into
/// an explicit FNV-1a implementation. This keeps optimizer ordering stable
/// across process runs without relying on Rust's randomized hash maps.
///
/// ```
/// use helix_planner::digest::PlanDigest;
///
/// let first = PlanDigest::for_tagged_value("example:v1", &("node", 7_u64));
/// let second = PlanDigest::for_tagged_value("example:v1", &("node", 7_u64));
/// let different = PlanDigest::for_tagged_value("example:v1", &("edge", 7_u64));
///
/// assert_eq!(first, second);
/// assert_ne!(first, different);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PlanDigest(u64);

impl PlanDigest {
    /// Build a digest from an already-computed stable value.
    pub const fn from_u64(value: u64) -> Self {
        Self(value)
    }

    /// Return the raw digest value.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Compute a stable digest for a serializable planner contract.
    ///
    /// Values containing unordered maps are not appropriate digest inputs
    /// unless the map type serializes keys deterministically.
    pub fn for_value<T>(value: &T) -> Self
    where
        T: Serialize,
    {
        let mut writer = DigestWriter::default();
        serde_json::to_writer(&mut writer, value)
            .expect("planner digest serialization writes into an infallible sink");
        Self(writer.finish())
    }

    /// Compute a stable digest with an explicit schema/version tag.
    pub fn for_tagged_value<T>(tag: &'static str, value: &T) -> Self
    where
        T: Serialize,
    {
        Self::for_value(&(tag, value))
    }
}

#[derive(Debug)]
struct StableFnv64 {
    state: u64,
}

impl Default for StableFnv64 {
    fn default() -> Self {
        Self {
            state: 0xcbf2_9ce4_8422_2325,
        }
    }
}

impl StableFnv64 {
    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.state ^= u64::from(*byte);
            self.state = self.state.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    const fn finish(&self) -> u64 {
        self.state
    }
}

#[derive(Debug, Default)]
struct DigestWriter {
    hasher: StableFnv64,
}

impl DigestWriter {
    const fn finish(&self) -> u64 {
        self.hasher.finish()
    }
}

impl std::io::Write for DigestWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hasher.write(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_is_stable_for_same_serialized_contract_and_tagged_by_schema() {
        let first = PlanDigest::for_tagged_value("memo_expr:v1", &("node", 1_u64));
        let second = PlanDigest::for_tagged_value("memo_expr:v1", &("node", 1_u64));
        let different_tag = PlanDigest::for_tagged_value("memo_expr:v2", &("node", 1_u64));
        let different_value = PlanDigest::for_tagged_value("memo_expr:v1", &("node", 2_u64));

        assert_eq!(first, second);
        assert_ne!(first, different_tag);
        assert_ne!(first, different_value);
        assert_eq!(PlanDigest::from_u64(first.get()), first);
    }
}
