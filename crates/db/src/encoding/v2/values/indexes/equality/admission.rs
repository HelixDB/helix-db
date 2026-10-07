//! Allocation-free structural preflight for the portable Roaring layout read
//! by the locked roaring 0.11.3 dependency. The ordinary decoder still validates
//! values, cardinalities, ordering, and delta disjointness. No stored bytes change.
use super::{EncodingError, BITMAP_MEMBERSHIP_DELTA_MAGIC};

const NO_RUN_COOKIE: u32 = 12346;
const RUN_COOKIE: u16 = 12347;
const OFFSET_THRESHOLD: usize = 4;
const ARRAY_CARDINALITY_LIMIT: usize = 4096;
const BITSET_BYTES: usize = 65536 / u8::BITS as usize;

struct Input<'a>(&'a [u8]);
impl<'a> Input<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8], EncodingError> {
        let Some((head, tail)) = self.0.split_at_checked(length) else {
            return Err(EncodingError::BufferTooShort {
                expected: length,
                actual: self.0.len(),
            });
        };
        self.0 = tail;
        Ok(head)
    }
    fn number<const N: usize>(&mut self) -> Result<[u8; N], EncodingError> {
        Ok(self
            .take(N)?
            .try_into()
            .expect("preflight checked fixed-width field"))
    }
}

/// Conservative simultaneous decoded/scratch allocation bound. Payload is
/// doubled for array-capacity accounting and run conversion; fixed per-container
/// and per-treemap-entry allowances include descriptions, offsets, vector and
/// tree-node overhead. This bound is admitted before calling the real decoder.
pub(super) fn allocation_bound(bytes: &[u8]) -> Result<usize, EncodingError> {
    let mut input = Input(bytes);
    let mut overhead = size_of::<roaring::RoaringTreemap>();
    if bytes.starts_with(BITMAP_MEMBERSHIP_DELTA_MAGIC) {
        input.take(BITMAP_MEMBERSHIP_DELTA_MAGIC.len())?;
        for name in ["additions", "removals"] {
            let length = u32::from_be_bytes(input.number()?) as usize;
            let (allocation, consumed) = portable_prefix(input.take(length)?)?;
            if consumed != length {
                return Err(EncodingError::Custom(format!(
                    "bitmap membership delta {name} has {} trailing bytes",
                    length - consumed
                )));
            }
            overhead = overhead.saturating_add(allocation);
        }
        if !input.0.is_empty() {
            return Err(EncodingError::Custom(format!(
                "bitmap membership delta has {} trailing bytes",
                input.0.len()
            )));
        }
    } else {
        let (allocation, consumed) = portable_prefix(bytes)?;
        if consumed != bytes.len() {
            return Err(EncodingError::Custom(format!(
                "secondary equality bitmap has {} trailing bytes",
                bytes.len() - consumed
            )));
        }
        overhead = overhead.saturating_add(allocation);
    }
    Ok(overhead.saturating_add(bytes.len().saturating_mul(2)))
}

/// Portable prefix boundary and container overhead. Built-in index values
/// historically ignore trailing bytes; managed values check this boundary.
pub(super) fn portable_prefix(bytes: &[u8]) -> Result<(usize, usize), EncodingError> {
    treemap(bytes).map_err(|error| {
        EncodingError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("failed to decode secondary equality bitmap: {error}"),
        ))
    })
}

fn treemap(bytes: &[u8]) -> Result<(usize, usize), EncodingError> {
    let mut input = Input(bytes);
    let count = u64::from_le_bytes(input.number()?);
    let mut overhead = 0_usize;
    for _ in 0..count {
        input.take(size_of::<u32>())?; // high ID word
        let cookie = u32::from_le_bytes(input.number()?);
        let (containers, runs, offsets) = if cookie == NO_RUN_COOKIE {
            (u32::from_le_bytes(input.number()?) as usize, false, true)
        } else if cookie as u16 == RUN_COOKIE {
            let containers = (cookie >> u16::BITS) as usize + 1;
            (containers, true, containers >= OFFSET_THRESHOLD)
        } else {
            return Err(EncodingError::Custom(
                "invalid roaring bitmap cookie".into(),
            ));
        };
        if containers > u16::MAX as usize + 1 {
            return Err(EncodingError::Custom("too many roaring containers".into()));
        }
        let run_bitmap = if runs {
            input.take(containers.div_ceil(8))?
        } else {
            &[]
        };
        let descriptions = input.take(containers * (2 * size_of::<u16>()))?;
        if offsets {
            input.take(containers * size_of::<u32>())?;
        }
        for (index, description) in descriptions.chunks_exact(2 * size_of::<u16>()).enumerate() {
            let cardinality = u16::from_le_bytes(
                description[size_of::<u16>()..2 * size_of::<u16>()]
                    .try_into()
                    .expect("checked roaring container description"),
            ) as usize
                + 1;
            let payload = if runs && run_bitmap[index / 8] & (1 << (index % 8)) != 0 {
                let intervals = u16::from_le_bytes(input.number()?) as usize;
                intervals * (2 * size_of::<u16>())
            } else if cardinality <= ARRAY_CARDINALITY_LIMIT {
                cardinality * size_of::<u16>()
            } else {
                BITSET_BYTES
            };
            input.take(payload)?;
        }
        overhead = overhead
            .saturating_add(512)
            .saturating_add(containers.saturating_mul(256));
    }
    Ok((overhead, bytes.len() - input.0.len()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::v2::values::indexes::equality::{
        BitmapMembershipDelta, SecondaryEqualityBitmapValue,
    };

    #[test]
    fn prepared_decoders_preserve_deployed_prefix_and_managed_framing_contracts() {
        use super::super::SecondaryEqualityValue;
        let ids = [1, 3, u64::MAX]
            .into_iter()
            .collect::<roaring::RoaringTreemap>();
        let mut padded = SecondaryEqualityValue::encode_ids(&ids).to_vec();
        let length = padded.len();
        padded.extend_from_slice(b"deployed trailing bytes");
        assert_eq!(portable_prefix(&padded).unwrap().1, length);
        assert_eq!(
            SecondaryEqualityValue::decode(&padded).unwrap().into_ids(),
            ids
        );
        assert_eq!(
            SecondaryEqualityValue::prepare(&padded)
                .unwrap()
                .decode()
                .unwrap()
                .into_ids(),
            ids
        );
        assert!(
            matches!(SecondaryEqualityBitmapValue::prepare(&padded), Err(EncodingError::Custom(message)) if message.contains("trailing bytes"))
        );
        assert!(matches!(
            SecondaryEqualityBitmapValue::prepare(b"malformed"),
            Err(EncodingError::Io(_))
        ));
        assert!(SecondaryEqualityValue::prepare(b"malformed").is_err());
        let delta = BitmapMembershipDelta::from_additions(ids.clone()).encode();
        assert_eq!(
            SecondaryEqualityValue::prepare(&delta)
                .unwrap()
                .decode()
                .unwrap()
                .into_ids(),
            ids
        );
        for length in 8..delta.len() {
            assert!(SecondaryEqualityValue::prepare(&delta[..length]).is_err());
        }
        let mut trailing = delta.to_vec();
        trailing.push(0);
        assert!(matches!(
            SecondaryEqualityValue::prepare(&trailing),
            Err(EncodingError::Custom(_))
        ));
    }

    #[test]
    fn preflight_bounds_sparse_dense_run_high_word_and_delta_decodes() {
        let mut fixtures = vec![
            roaring::RoaringTreemap::new(),
            [0, u64::MAX, 1 << 32, 1 << 48].into_iter().collect(),
            (0..20000).map(|id| id * 2).collect(),
            (0..100000).collect(),
        ];
        for length in [100, 300000] {
            let mut runs = (0..length).collect::<roaring::RoaringBitmap>();
            runs.optimize();
            assert!(runs.statistics().n_run_containers > 0);
            fixtures.push(roaring::RoaringTreemap::from_bitmaps([(0, runs)]));
        }
        for ids in fixtures {
            let value = SecondaryEqualityBitmapValue::new(ids.clone());
            for bytes in [
                value.encode(),
                BitmapMembershipDelta::from_additions(ids.clone()).encode(),
            ] {
                let bound = allocation_bound(&bytes).unwrap();
                let decoded = SecondaryEqualityBitmapValue::decode(&bytes).unwrap();
                assert_eq!(decoded.ids(), &ids);
                assert!(super::super::retained_allocation_estimate(decoded.ids()) <= bound);
                for length in 0..bytes.len() {
                    assert!(allocation_bound(&bytes[..length]).is_err());
                }
                let mut trailing = bytes.to_vec();
                trailing.push(0);
                assert!(allocation_bound(&trailing).is_err());
            }
        }
    }

    #[test]
    fn malformed_headers_are_rejected_before_container_allocation() {
        for cookie in [0, u32::MAX] {
            let mut bytes = 1_u64.to_le_bytes().to_vec();
            bytes.extend_from_slice(&0_u32.to_le_bytes());
            bytes.extend_from_slice(&cookie.to_le_bytes());
            assert!(allocation_bound(&bytes).is_err());
        }
        let mut bytes = 1_u64.to_le_bytes().to_vec();
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes.extend_from_slice(&NO_RUN_COOKIE.to_le_bytes());
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(allocation_bound(&bytes).is_err());
    }
}
