//! Allocation-free equality classification followed by exactly sized encoding.
//! A prepared value borrows immutable input, so its validated lengths and number
//! semantics cannot change between admission and construction.
use super::*;

const TAG_LEN: usize = size_of::<u8>();
const COUNT_LEN: usize = size_of::<u32>();

#[derive(Debug, Clone, Copy)]
enum Kind<'a> {
    Bool(bool),
    Number(CanonicalNumber),
    DateTime(i64),
    String(&'a str),
    Bytes(&'a [u8]),
    Integers(&'a [i64]),
    Doubles(&'a [f64]),
    Floats(&'a [f32]),
    Strings(&'a [String]),
}

/// A valid indexed value and its exact canonical length, without allocated bytes.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PreparedEqualityValue<'a> {
    kind: Kind<'a>,
    encoded_len: usize,
}

impl PreparedEqualityValue<'_> {
    pub(crate) const fn encoded_len(&self) -> usize {
        self.encoded_len
    }

    pub(crate) fn encode(self) -> CanonicalEqualityValue {
        let mut bytes = Vec::with_capacity(self.encoded_len());
        match self.kind {
            Kind::Bool(value) => {
                bytes.put_u8(BOOL_TAG);
                bytes.put_u8(u8::from(value));
            }
            Kind::Number(value) => {
                bytes.put_u8(NUMBER_TAG);
                put_number(&mut bytes, value);
            }
            Kind::DateTime(value) => {
                bytes.put_u8(DATETIME_TAG);
                bytes.put_i64(value);
            }
            Kind::String(value) => {
                bytes.put_u8(STRING_TAG);
                put_length_delimited(&mut bytes, value.as_bytes()).expect("prepared string length");
            }
            Kind::Bytes(value) => {
                bytes.put_u8(BYTES_TAG);
                put_length_delimited(&mut bytes, value).expect("prepared byte length");
            }
            Kind::Integers(values) => {
                bytes.put_u8(I64_ARRAY_TAG);
                put_count(&mut bytes, values.len()).expect("prepared integer count");
                values.iter().for_each(|value| bytes.put_i64(*value));
            }
            Kind::Doubles(values) => {
                bytes.put_u8(F64_ARRAY_TAG);
                put_count(&mut bytes, values.len()).expect("prepared double count");
                values.iter().for_each(|value| {
                    put_number(
                        &mut bytes,
                        CanonicalNumber::from_f64(*value).expect("prepared non-NaN double"),
                    );
                });
            }
            Kind::Floats(values) => {
                bytes.put_u8(F32_ARRAY_TAG);
                put_count(&mut bytes, values.len()).expect("prepared float count");
                values.iter().for_each(|value| {
                    put_number(
                        &mut bytes,
                        CanonicalNumber::from_f32(*value).expect("prepared non-NaN float"),
                    );
                });
            }
            Kind::Strings(values) => {
                bytes.put_u8(STRING_ARRAY_TAG);
                put_count(&mut bytes, values.len()).expect("prepared string count");
                values.iter().for_each(|value| {
                    put_length_delimited(&mut bytes, value.as_bytes())
                        .expect("prepared member length");
                });
            }
        }
        assert_eq!(
            bytes.len(),
            self.encoded_len,
            "prepared equality length matches its encoding"
        );
        assert_eq!(
            bytes.capacity(),
            self.encoded_len,
            "prepared encoding never grows its allocation"
        );
        CanonicalEqualityValue::new(bytes)
    }
}

/// Inspect the complete value without building canonical bytes. In particular,
/// NaN in an otherwise oversized float array retains non-reflexive semantics.
pub(crate) fn prepare_equality_value(
    value: &PropertyValue,
) -> EqualityValueProjection<PreparedEqualityValue<'_>> {
    let (kind, encoded_len) = match value {
        PropertyValue::Null => return EqualityValueProjection::AuthoritativeNull,
        PropertyValue::Array(_) => return EqualityValueProjection::Unsupported("Array"),
        PropertyValue::Object(_) => return EqualityValueProjection::Unsupported("Object"),
        PropertyValue::Bool(value) => (Kind::Bool(*value), TAG_LEN + size_of::<u8>()),
        PropertyValue::I64(_) | PropertyValue::F64(_) | PropertyValue::F32(_) => {
            let Some(number) = canonical_number::from_property(value) else {
                return EqualityValueProjection::NonReflexive;
            };
            (Kind::Number(number), TAG_LEN + number_len(number))
        }
        PropertyValue::DateTime(value) => (Kind::DateTime(*value), TAG_LEN + size_of::<i64>()),
        PropertyValue::String(value) => {
            if u32::try_from(value.len()).is_err() {
                return oversized(TAG_LEN);
            }
            (Kind::String(value), TAG_LEN + COUNT_LEN + value.len())
        }
        PropertyValue::Bytes(value) => {
            if u32::try_from(value.len()).is_err() {
                return oversized(TAG_LEN);
            }
            (Kind::Bytes(value), TAG_LEN + COUNT_LEN + value.len())
        }
        PropertyValue::I64Array(values) => {
            if u32::try_from(values.len()).is_err() {
                return oversized(TAG_LEN);
            }
            (
                Kind::Integers(values),
                values
                    .len()
                    .saturating_mul(size_of::<i64>())
                    .saturating_add(TAG_LEN + COUNT_LEN),
            )
        }
        PropertyValue::F64Array(values) => {
            if u32::try_from(values.len()).is_err() {
                return oversized(values.len());
            }
            let Some(length) = values
                .iter()
                .try_fold(TAG_LEN + COUNT_LEN, |length, value| {
                    CanonicalNumber::from_f64(*value)
                        .map(|number| length.saturating_add(number_len(number)))
                })
            else {
                return EqualityValueProjection::NonReflexive;
            };
            (Kind::Doubles(values), length)
        }
        PropertyValue::F32Array(values) => {
            if u32::try_from(values.len()).is_err() {
                return oversized(values.len());
            }
            let Some(length) = values
                .iter()
                .try_fold(TAG_LEN + COUNT_LEN, |length, value| {
                    CanonicalNumber::from_f32(*value)
                        .map(|number| length.saturating_add(number_len(number)))
                })
            else {
                return EqualityValueProjection::NonReflexive;
            };
            (Kind::Floats(values), length)
        }
        PropertyValue::StringArray(values) => {
            if u32::try_from(values.len()).is_err() {
                return oversized(values.len());
            }
            let mut length = TAG_LEN + COUNT_LEN;
            for value in values {
                if u32::try_from(value.len()).is_err() {
                    return oversized(value.len());
                }
                length = length.saturating_add(COUNT_LEN).saturating_add(value.len());
            }
            (Kind::Strings(values), length)
        }
    };
    if encoded_len > MAX_EQUALITY_CANONICAL_LEN {
        return oversized(encoded_len);
    }
    EqualityValueProjection::Indexed(PreparedEqualityValue { kind, encoded_len })
}

fn oversized<T>(encoded_len: usize) -> EqualityValueProjection<T> {
    EqualityValueProjection::Oversized {
        encoded_len,
        maximum: MAX_EQUALITY_CANONICAL_LEN,
    }
}

fn number_len(number: CanonicalNumber) -> usize {
    match number {
        CanonicalNumber::NegativeFinite(_) | CanonicalNumber::PositiveFinite(_) => {
            TAG_LEN + size_of::<i16>() + size_of::<u64>()
        }
        CanonicalNumber::NegativeInfinity
        | CanonicalNumber::Zero
        | CanonicalNumber::PositiveInfinity => TAG_LEN,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepared_lengths_preserve_independent_golden_frames() {
        let fixtures = [
            (PropertyValue::Bool(true), vec![1, 1]),
            (PropertyValue::I64(0), vec![2, 3]),
            (
                PropertyValue::I64(1),
                vec![2, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
            ),
            (
                PropertyValue::I64(-2),
                vec![2, 2, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1],
            ),
            (PropertyValue::DateTime(7), vec![3, 0, 0, 0, 0, 0, 0, 0, 7]),
            (
                PropertyValue::String("é".into()),
                vec![4, 0, 0, 0, 2, 0xc3, 0xa9],
            ),
            (
                PropertyValue::Bytes(vec![0, 255]),
                vec![5, 0, 0, 0, 2, 0, 255],
            ),
            (
                PropertyValue::I64Array(vec![0, 1]),
                vec![
                    6, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
                ],
            ),
            (
                PropertyValue::F64Array(vec![0.0, f64::INFINITY, f64::NEG_INFINITY, 1.0]),
                vec![7, 0, 0, 0, 4, 3, 5, 1, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
            ),
            (
                PropertyValue::F32Array(vec![-0.0, f32::NEG_INFINITY]),
                vec![8, 0, 0, 0, 2, 3, 1],
            ),
            (
                PropertyValue::StringArray(vec!["".into(), "é".into()]),
                vec![9, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0xc3, 0xa9],
            ),
        ];
        for (value, expected) in fixtures {
            let (prepared, allocation) =
                crate::allocation_testing::observe(|| prepare_equality_value(&value));
            assert_eq!(allocation.allocations, 0, "preparation does not allocate");
            let EqualityValueProjection::Indexed(prepared) = prepared else {
                panic!("golden frame must be indexed");
            };
            assert_eq!(prepared.encoded_len(), expected.len());
            let (encoded, allocation) = crate::allocation_testing::observe(|| prepared.encode());
            assert_eq!(
                allocation.allocations, 1,
                "canonical encoding uses one allocation"
            );
            assert_eq!(allocation.bytes, expected.len());
            assert_eq!(encoded.canonical(), expected);
            assert_eq!(
                project_equality_value(&value),
                EqualityValueProjection::Indexed(encoded)
            );
        }
    }

    #[test]
    fn size_boundaries_and_nonreflexive_arrays_are_classified_before_encoding() {
        let largest = "x".repeat(MAX_EQUALITY_CANONICAL_LEN - TAG_LEN - COUNT_LEN);
        let value = PropertyValue::String(largest.clone());
        let EqualityValueProjection::Indexed(prepared) = prepare_equality_value(&value) else {
            panic!("largest string is indexed");
        };
        assert_eq!(prepared.encoded_len(), MAX_EQUALITY_CANONICAL_LEN);
        assert_eq!(
            prepared.encode().canonical().len(),
            MAX_EQUALITY_CANONICAL_LEN
        );
        for value in [
            PropertyValue::String(format!("{largest}x")),
            PropertyValue::Bytes(vec![
                0;
                MAX_EQUALITY_CANONICAL_LEN - TAG_LEN - COUNT_LEN + 1
            ]),
        ] {
            assert!(
                matches!(prepare_equality_value(&value), EqualityValueProjection::Oversized { encoded_len, maximum }
                if encoded_len == MAX_EQUALITY_CANONICAL_LEN + 1 && maximum == MAX_EQUALITY_CANONICAL_LEN)
            );
        }
        for value in [
            PropertyValue::I64Array(vec![1; MAX_EQUALITY_CANONICAL_LEN / size_of::<i64>()]),
            PropertyValue::F64Array(vec![1.0; MAX_EQUALITY_CANONICAL_LEN / 11]),
            PropertyValue::F32Array(vec![1.0; MAX_EQUALITY_CANONICAL_LEN / 11]),
            PropertyValue::StringArray(vec![largest, "".into()]),
        ] {
            let (projected, allocation) =
                crate::allocation_testing::observe(|| project_equality_value(&value));
            assert_eq!(
                allocation.allocations, 0,
                "oversized values do not allocate canonical buffers"
            );
            assert!(matches!(
                projected,
                EqualityValueProjection::Oversized { .. }
            ));
        }
        let mut doubles = vec![1.0; MAX_EQUALITY_CANONICAL_LEN / 11 + 1];
        doubles.push(f64::NAN);
        let mut floats = vec![1.0; MAX_EQUALITY_CANONICAL_LEN / 11 + 1];
        floats.push(f32::NAN);
        for value in [
            PropertyValue::F64Array(doubles),
            PropertyValue::F32Array(floats),
        ] {
            assert!(matches!(
                prepare_equality_value(&value),
                EqualityValueProjection::NonReflexive
            ));
            assert_eq!(
                project_equality_value(&value),
                EqualityValueProjection::NonReflexive
            );
        }
    }
}
