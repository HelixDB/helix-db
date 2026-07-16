//! Version-1 database value construction and parsing boundaries.
//!
//! Persisted values are encoded here so storage and search call sites do not
//! assemble byte layouts independently. Existing physical codecs preserve
//! deployed bytes; canonical V2 lifecycle codecs are added here in Phase 2.

use crate::encoding::error::EncodingError;

pub(crate) mod edge_endpoints;
pub mod edges;
pub(crate) mod id_allocation;
pub(crate) mod index_v2;
pub(crate) mod secondary;
#[cfg(any(test, feature = "fuzzing"))]
pub(crate) mod text_index;
pub(crate) mod vector_generation;
pub mod vectors;

const ENCODING_TYPE_LEN: usize = core::mem::size_of::<u8>();
const U32_LEN: usize = core::mem::size_of::<u32>();
const U64_LEN: usize = core::mem::size_of::<u64>();

#[inline]
fn ensure_min_len(data: &[u8], expected: usize) -> Result<(), EncodingError> {
    if data.len() < expected {
        return Err(EncodingError::BufferTooShort {
            expected,
            actual: data.len(),
        });
    }

    Ok(())
}

#[inline]
fn ensure_exact_len(data: &[u8], expected: usize) -> Result<(), EncodingError> {
    if data.len() != expected {
        return Err(EncodingError::BufferTooShort {
            expected,
            actual: data.len(),
        });
    }

    Ok(())
}

#[inline]
fn checked_len_with_element_count(
    prefix_len: usize,
    count: usize,
    element_len: usize,
    overflow_message: &str,
) -> Result<usize, EncodingError> {
    let payload_len = count
        .checked_mul(element_len)
        .ok_or_else(|| EncodingError::Custom(overflow_message.to_string()))?;
    prefix_len
        .checked_add(payload_len)
        .ok_or_else(|| EncodingError::Custom(overflow_message.to_string()))?;
    Ok(payload_len + prefix_len)
}

#[inline]
fn take_slice<'a>(
    data: &'a [u8],
    offset: &mut usize,
    len: usize,
) -> Result<&'a [u8], EncodingError> {
    let start = *offset;
    let end = start
        .checked_add(len)
        .ok_or(EncodingError::BufferTooShort {
            expected: usize::MAX,
            actual: data.len(),
        })?;
    let slice = data.get(start..end).ok_or(EncodingError::BufferTooShort {
        expected: end,
        actual: data.len(),
    })?;
    *offset = end;
    Ok(slice)
}

#[inline]
fn take_u8(data: &[u8], offset: &mut usize) -> Result<u8, EncodingError> {
    Ok(take_slice(data, offset, ENCODING_TYPE_LEN)?[0])
}

#[inline]
fn take_u32_le(data: &[u8], offset: &mut usize) -> Result<usize, EncodingError> {
    let bytes = take_slice(data, offset, U32_LEN)?;
    Ok(u32::from_le_bytes(bytes.try_into().expect("u32 field is 4 bytes")) as usize)
}

#[inline]
fn take_u32_be(data: &[u8], offset: &mut usize) -> Result<usize, EncodingError> {
    let bytes = take_slice(data, offset, U32_LEN)?;
    Ok(u32::from_be_bytes(bytes.try_into().expect("u32 field is 4 bytes")) as usize)
}

#[inline]
fn take_u64_le(data: &[u8], offset: &mut usize) -> Result<u64, EncodingError> {
    let bytes = take_slice(data, offset, U64_LEN)?;
    Ok(u64::from_le_bytes(
        bytes.try_into().expect("u64 field is 8 bytes"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_len_helpers_report_exact_contracts() {
        assert!(ensure_min_len(&[1, 2], 2).is_ok());
        assert!(matches!(
            ensure_min_len(&[1], 2),
            Err(EncodingError::BufferTooShort {
                expected: 2,
                actual: 1
            })
        ));
        assert!(ensure_exact_len(&[1, 2], 2).is_ok());
        assert!(matches!(
            ensure_exact_len(&[1, 2, 3], 2),
            Err(EncodingError::BufferTooShort {
                expected: 2,
                actual: 3
            })
        ));
    }

    #[test]
    fn checked_len_reports_multiplication_and_addition_overflow() {
        assert_eq!(
            checked_len_with_element_count(2, 3, 4, "overflow").unwrap(),
            14
        );
        assert!(matches!(
            checked_len_with_element_count(0, usize::MAX, 2, "overflow"),
            Err(EncodingError::Custom(message)) if message == "overflow"
        ));
        assert!(matches!(
            checked_len_with_element_count(usize::MAX, 1, 1, "overflow"),
            Err(EncodingError::Custom(message)) if message == "overflow"
        ));
    }

    #[test]
    fn take_helpers_advance_offsets_and_report_bounds() {
        let data = [0xAB, 1, 2, 3, 4, 8, 7, 6, 5, 4, 3, 2, 1];
        let mut offset = 0;

        assert_eq!(take_u8(&data, &mut offset).unwrap(), 0xAB);
        assert_eq!(offset, 1);
        assert_eq!(take_u32_be(&data, &mut offset).unwrap(), 0x0102_0304);
        assert_eq!(take_u32_le(&data, &mut offset).unwrap(), 0x0506_0708);
        assert_eq!(take_slice(&data, &mut offset, 2).unwrap(), &[4, 3]);

        let mut overflow_offset = usize::MAX;
        assert!(matches!(
            take_slice(&data, &mut overflow_offset, 1),
            Err(EncodingError::BufferTooShort {
                expected: usize::MAX,
                actual: 13
            })
        ));

        let mut short_offset = data.len();
        assert!(matches!(
            take_u64_le(&data, &mut short_offset),
            Err(EncodingError::BufferTooShort {
                expected: 21,
                actual: 13
            })
        ));
    }
}
