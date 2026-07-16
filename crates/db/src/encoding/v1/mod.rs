pub mod indexes;
pub mod keys;
pub mod property;
pub mod values;

use crate::encoding::error::EncodingError;

#[inline]
pub(crate) fn read_u64(slice: &[u8], start: usize) -> Result<u64, EncodingError> {
    let end =
        start
            .checked_add(core::mem::size_of::<u64>())
            .ok_or(EncodingError::BufferTooShort {
                expected: usize::MAX,
                actual: slice.len(),
            })?;
    let bytes: [u8; core::mem::size_of::<u64>()] = slice
        .get(start..end)
        .ok_or(EncodingError::BufferTooShort {
            expected: end,
            actual: slice.len(),
        })?
        .try_into()
        .expect("u64 slice is exactly 8 bytes");
    Ok(u64::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_u64_decodes_big_endian_at_offset() {
        let data = [0xAA, 0, 0, 0, 0, 0, 0, 0, 42, 0xBB];

        assert_eq!(read_u64(&data, 1).unwrap(), 42);
    }

    #[test]
    fn read_u64_reports_short_buffer() {
        assert!(matches!(
            read_u64(&[1, 2, 3], 1),
            Err(EncodingError::BufferTooShort {
                expected: 9,
                actual: 3
            })
        ));
    }

    #[test]
    fn read_u64_reports_offset_overflow() {
        assert!(matches!(
            read_u64(&[], usize::MAX),
            Err(EncodingError::BufferTooShort {
                expected: usize::MAX,
                actual: 0
            })
        ));
    }
}
