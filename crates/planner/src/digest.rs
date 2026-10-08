use serde::{ser, Deserialize, Serialize};

/// Stable planner digest used for memo identity and deterministic tie-breaking.
///
/// A digest hashes a compact encoding of the value's `serde` serialization:
/// every value is tagged with its kind, strings and byte arrays carry their
/// length, enum variants their index, and compound values are bracketed, so
/// different shapes of the same bytes digest differently. Struct fields are
/// hashed by position, with a marker for each skipped field, so two struct
/// types with the same field values digest alike; digests only compare values
/// of one type. Nothing depends on addresses or randomized hashers, so
/// optimizer ordering is the same in every process run. Values that serialize
/// equally digest equally, and callers confirm a digest match with `==`
/// wherever identity matters. Floats hash by their bits, so `-0.0` and `+0.0`,
/// which `==` treats as equal, digest differently;
/// [`Self::for_equality_screen`] hashes them alike for callers that rule out
/// equality by digest.
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
    pub fn for_value<T>(value: &T) -> Self
    where
        T: Serialize,
    {
        Self::digest(value, Zero::Signed)
    }

    /// A digest that values equal under a derived `==` always share, so
    /// values whose screens differ cannot be equal. Unlike
    /// [`Self::for_value`], it hashes `-0.0` and `+0.0` alike, as `==`
    /// compares them. It is not an identity: values that share it may still
    /// differ.
    ///
    /// ```
    /// use helix_planner::digest::PlanDigest;
    ///
    /// assert_ne!(PlanDigest::for_value(&-0.0_f64), PlanDigest::for_value(&0.0_f64));
    /// assert_eq!(
    ///     PlanDigest::for_equality_screen(&-0.0_f64),
    ///     PlanDigest::for_equality_screen(&0.0_f64)
    /// );
    /// ```
    pub fn for_equality_screen<T>(value: &T) -> Self
    where
        T: Serialize,
    {
        Self::digest(value, Zero::Unsigned)
    }

    fn digest<T>(value: &T, zero: Zero) -> Self
    where
        T: Serialize,
    {
        let mut hasher = StableHasher::default();
        value
            .serialize(&mut DigestSerializer {
                hasher: &mut hasher,
                zero,
            })
            .expect("planner digest serialization cannot fail");
        Self(hasher.finish())
    }

    /// Compute a stable digest with an explicit schema/version tag.
    pub fn for_tagged_value<T>(tag: &'static str, value: &T) -> Self
    where
        T: Serialize,
    {
        Self::for_value(&(tag, value))
    }

    /// The digest physical alternatives break cost ties with: FNV-1a over
    /// the tagged value's compact JSON.
    ///
    /// It stays apart from [`Self::for_tagged_value`] because it decides
    /// which of two equal-cost plans is chosen, and reviewed plan baselines
    /// record those choices; any other function would pick differently among
    /// ties. Only tie-breaking needs it; identity digests use the faster
    /// encoding.
    ///
    /// ```
    /// use helix_planner::digest::PlanDigest;
    ///
    /// let first = PlanDigest::for_tie_break("example:v1", &("node", 7_u64));
    /// assert_eq!(first, PlanDigest::for_tie_break("example:v1", &("node", 7_u64)));
    /// assert_ne!(first, PlanDigest::for_tie_break("example:v1", &("edge", 7_u64)));
    /// ```
    pub fn for_tie_break<T>(tag: &'static str, value: &T) -> Self
    where
        T: Serialize,
    {
        let mut writer = JsonFnv64::default();
        serde_json::to_writer(&mut writer, &(tag, value))
            .expect("tie-break digest serialization writes into an infallible sink");
        Self(writer.state)
    }
}

/// FNV-1a over everything written to it.
struct JsonFnv64 {
    state: u64,
}

impl Default for JsonFnv64 {
    fn default() -> Self {
        Self {
            state: 0xcbf2_9ce4_8422_2325,
        }
    }
}

impl std::io::Write for JsonFnv64 {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        for byte in buf {
            self.state ^= u64::from(*byte);
            self.state = self.state.wrapping_mul(0x0000_0100_0000_01b3);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A 64-bit hash that absorbs one 64-bit word per multiply: each word is
/// folded into the state with a full 128-bit product, as wyhash does, so
/// hashing costs a multiply per eight bytes rather than one per byte.
/// Words are read little-endian, so digests match across platforms.
#[derive(Debug)]
struct StableHasher {
    state: u64,
    words: u64,
}

impl Default for StableHasher {
    fn default() -> Self {
        Self {
            state: 0x243f_6a88_85a3_08d3,
            words: 0,
        }
    }
}

const STATE_KEY: u64 = 0xa076_1d64_78bd_642f;
const WORD_KEY: u64 = 0xe703_7ed1_a0b4_28db;

/// The high and low halves of `a * b`, xored.
const fn fold(a: u64, b: u64) -> u64 {
    let product = (a as u128) * (b as u128);
    (product as u64) ^ ((product >> 64) as u64)
}

impl StableHasher {
    fn word(&mut self, word: u64) {
        self.state = fold(self.state ^ STATE_KEY, word ^ WORD_KEY);
        self.words += 1;
    }

    /// Bytes with their length, so adjacent strings cannot run together.
    fn bytes(&mut self, bytes: &[u8]) {
        const WORD: usize = size_of::<u64>();
        self.word(bytes.len() as u64);
        let mut chunks = bytes.chunks_exact(WORD);
        for chunk in &mut chunks {
            self.word(u64::from_le_bytes(
                chunk.try_into().expect("exact chunks are one word"),
            ));
        }
        let rest = chunks.remainder();
        if !rest.is_empty() {
            let mut last = [0; WORD];
            last[..rest.len()].copy_from_slice(rest);
            self.word(u64::from_le_bytes(last));
        }
    }

    const fn finish(&self) -> u64 {
        fold(self.state ^ self.words, STATE_KEY ^ WORD_KEY)
    }
}

/// Kinds of serialized value, each opening its encoding.
#[derive(Clone, Copy)]
enum Tag {
    Bool = 1,
    Signed,
    Unsigned,
    Signed128,
    Unsigned128,
    Float32,
    Float64,
    Char,
    Str,
    DisplayStr,
    Bytes,
    None,
    Some,
    Unit,
    UnitVariant,
    NewtypeVariant,
    Seq,
    TupleVariant,
    Map,
    Struct,
    StructVariant,
    /// Stands for a struct field its `Serialize` implementation skipped.
    Skipped,
    /// Closes a sequence, map or struct.
    End,
}

/// How a digest hashes floating-point zero.
#[derive(Clone, Copy)]
enum Zero {
    /// By its bits, as it serializes: `-0.0` and `+0.0` differ.
    Signed,
    /// As `==` compares it: `-0.0` and `+0.0` hash alike.
    Unsigned,
}

struct DigestSerializer<'h> {
    hasher: &'h mut StableHasher,
    zero: Zero,
}

impl DigestSerializer<'_> {
    fn tag(&mut self, tag: Tag) {
        self.hasher.word(tag as u64);
    }
}

/// The digest serializer never fails; this only carries a `Serialize`
/// implementation's own error.
#[derive(Debug)]
struct DigestError(String);

impl std::fmt::Display for DigestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for DigestError {}

impl ser::Error for DigestError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Self(msg.to_string())
    }
}

impl<'h> ser::Serializer for &mut DigestSerializer<'h> {
    type Ok = ();
    type Error = DigestError;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, v: bool) -> Result<(), DigestError> {
        self.tag(Tag::Bool);
        self.hasher.word(u64::from(v));
        Ok(())
    }

    fn serialize_i8(self, v: i8) -> Result<(), DigestError> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i16(self, v: i16) -> Result<(), DigestError> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i32(self, v: i32) -> Result<(), DigestError> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i64(self, v: i64) -> Result<(), DigestError> {
        self.tag(Tag::Signed);
        self.hasher.word(v as u64);
        Ok(())
    }

    fn serialize_i128(self, v: i128) -> Result<(), DigestError> {
        self.tag(Tag::Signed128);
        self.hasher.bytes(&v.to_le_bytes());
        Ok(())
    }

    fn serialize_u8(self, v: u8) -> Result<(), DigestError> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u16(self, v: u16) -> Result<(), DigestError> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u32(self, v: u32) -> Result<(), DigestError> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u64(self, v: u64) -> Result<(), DigestError> {
        self.tag(Tag::Unsigned);
        self.hasher.word(v);
        Ok(())
    }

    fn serialize_u128(self, v: u128) -> Result<(), DigestError> {
        self.tag(Tag::Unsigned128);
        self.hasher.bytes(&v.to_le_bytes());
        Ok(())
    }

    fn serialize_f32(self, v: f32) -> Result<(), DigestError> {
        self.tag(Tag::Float32);
        let v = match self.zero {
            Zero::Unsigned if v == 0.0 => 0.0,
            Zero::Signed | Zero::Unsigned => v,
        };
        self.hasher.word(u64::from(v.to_bits()));
        Ok(())
    }

    fn serialize_f64(self, v: f64) -> Result<(), DigestError> {
        self.tag(Tag::Float64);
        let v = match self.zero {
            Zero::Unsigned if v == 0.0 => 0.0,
            Zero::Signed | Zero::Unsigned => v,
        };
        self.hasher.word(v.to_bits());
        Ok(())
    }

    fn serialize_char(self, v: char) -> Result<(), DigestError> {
        self.tag(Tag::Char);
        self.hasher.word(u64::from(v));
        Ok(())
    }

    fn serialize_str(self, v: &str) -> Result<(), DigestError> {
        self.tag(Tag::Str);
        self.hasher.bytes(v.as_bytes());
        Ok(())
    }

    /// Streams the text in words instead of formatting it into a `String`.
    fn collect_str<T>(self, value: &T) -> Result<(), DigestError>
    where
        T: ?Sized + std::fmt::Display,
    {
        struct Words<'a> {
            hasher: &'a mut StableHasher,
            pending: [u8; 8],
            filled: usize,
            len: u64,
        }
        impl std::fmt::Write for Words<'_> {
            fn write_str(&mut self, text: &str) -> std::fmt::Result {
                for byte in text.bytes() {
                    self.pending[self.filled] = byte;
                    self.filled += 1;
                    if self.filled == self.pending.len() {
                        self.hasher.word(u64::from_le_bytes(self.pending));
                        self.filled = 0;
                    }
                }
                self.len += text.len() as u64;
                Ok(())
            }
        }
        self.tag(Tag::DisplayStr);
        let mut words = Words {
            hasher: &mut *self.hasher,
            pending: [0; 8],
            filled: 0,
            len: 0,
        };
        std::fmt::Write::write_fmt(&mut words, format_args!("{value}"))
            .map_err(|_| <DigestError as ser::Error>::custom("Display implementation failed"))?;
        let (pending, filled, len) = (words.pending, words.filled, words.len);
        if filled > 0 {
            let mut last = [0; 8];
            last[..filled].copy_from_slice(&pending[..filled]);
            self.hasher.word(u64::from_le_bytes(last));
        }
        self.hasher.word(len);
        Ok(())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<(), DigestError> {
        self.tag(Tag::Bytes);
        self.hasher.bytes(v);
        Ok(())
    }

    fn serialize_none(self) -> Result<(), DigestError> {
        self.tag(Tag::None);
        Ok(())
    }

    fn serialize_some<T>(self, value: &T) -> Result<(), DigestError>
    where
        T: ?Sized + Serialize,
    {
        self.tag(Tag::Some);
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<(), DigestError> {
        self.tag(Tag::Unit);
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<(), DigestError> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
    ) -> Result<(), DigestError> {
        self.tag(Tag::UnitVariant);
        self.hasher.word(u64::from(variant_index));
        Ok(())
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<(), DigestError>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        value: &T,
    ) -> Result<(), DigestError>
    where
        T: ?Sized + Serialize,
    {
        self.tag(Tag::NewtypeVariant);
        self.hasher.word(u64::from(variant_index));
        value.serialize(self)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self, DigestError> {
        self.tag(Tag::Seq);
        Ok(self)
    }

    fn serialize_tuple(self, len: usize) -> Result<Self, DigestError> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(self, _name: &'static str, len: usize) -> Result<Self, DigestError> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self, DigestError> {
        self.tag(Tag::TupleVariant);
        self.hasher.word(u64::from(variant_index));
        Ok(self)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self, DigestError> {
        self.tag(Tag::Map);
        Ok(self)
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self, DigestError> {
        self.tag(Tag::Struct);
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self, DigestError> {
        self.tag(Tag::StructVariant);
        self.hasher.word(u64::from(variant_index));
        Ok(self)
    }
}

impl ser::SerializeSeq for &mut DigestSerializer<'_> {
    type Ok = ();
    type Error = DigestError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), DigestError>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), DigestError> {
        self.tag(Tag::End);
        Ok(())
    }
}

impl ser::SerializeTuple for &mut DigestSerializer<'_> {
    type Ok = ();
    type Error = DigestError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), DigestError>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), DigestError> {
        self.tag(Tag::End);
        Ok(())
    }
}

impl ser::SerializeTupleStruct for &mut DigestSerializer<'_> {
    type Ok = ();
    type Error = DigestError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), DigestError>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), DigestError> {
        self.tag(Tag::End);
        Ok(())
    }
}

impl ser::SerializeTupleVariant for &mut DigestSerializer<'_> {
    type Ok = ();
    type Error = DigestError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), DigestError>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), DigestError> {
        self.tag(Tag::End);
        Ok(())
    }
}

impl ser::SerializeMap for &mut DigestSerializer<'_> {
    type Ok = ();
    type Error = DigestError;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), DigestError>
    where
        T: ?Sized + Serialize,
    {
        key.serialize(&mut **self)
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), DigestError>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), DigestError> {
        self.tag(Tag::End);
        Ok(())
    }
}

impl ser::SerializeStruct for &mut DigestSerializer<'_> {
    type Ok = ();
    type Error = DigestError;

    fn serialize_field<T>(&mut self, _key: &'static str, value: &T) -> Result<(), DigestError>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn skip_field(&mut self, _key: &'static str) -> Result<(), DigestError> {
        self.tag(Tag::Skipped);
        Ok(())
    }

    fn end(self) -> Result<(), DigestError> {
        self.tag(Tag::End);
        Ok(())
    }
}

impl ser::SerializeStructVariant for &mut DigestSerializer<'_> {
    type Ok = ();
    type Error = DigestError;

    fn serialize_field<T>(&mut self, _key: &'static str, value: &T) -> Result<(), DigestError>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn skip_field(&mut self, _key: &'static str) -> Result<(), DigestError> {
        self.tag(Tag::Skipped);
        Ok(())
    }

    fn end(self) -> Result<(), DigestError> {
        self.tag(Tag::End);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

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

    #[derive(Serialize)]
    struct Pair {
        left: u8,
        right: u8,
    }

    #[derive(Serialize)]
    struct Sparse {
        #[serde(skip_serializing_if = "Option::is_none")]
        left: Option<u8>,
        #[serde(skip_serializing_if = "Option::is_none")]
        right: Option<u8>,
    }

    #[derive(Serialize)]
    enum Shape {
        Unit,
        Other,
        Newtype(u8),
        Tuple(u8, u8),
        Struct { value: u8 },
    }

    #[test]
    fn different_shapes_of_the_same_content_digest_differently() {
        let digests = [
            PlanDigest::for_value(&("ab", "c")),
            PlanDigest::for_value(&("a", "bc")),
            PlanDigest::for_value(&("abc",)),
            PlanDigest::for_value(&Some(())),
            PlanDigest::for_value(&None::<()>),
            PlanDigest::for_value(&()),
            PlanDigest::for_value(&vec![1_u8]),
            PlanDigest::for_value(&vec![vec![1_u8]]),
            PlanDigest::for_value(&Vec::<u8>::new()),
            PlanDigest::for_value(&vec![Vec::<u8>::new()]),
            PlanDigest::for_value(&1_u8),
            PlanDigest::for_value(&1_i8),
            PlanDigest::for_value(&1.0_f64),
            PlanDigest::for_value(&-0.0_f64),
            PlanDigest::for_value(&0.0_f64),
            PlanDigest::for_value(&'a'),
            PlanDigest::for_value(&"a"),
            PlanDigest::for_value(&Pair { left: 1, right: 2 }),
            PlanDigest::for_value(&Pair { left: 2, right: 1 }),
            PlanDigest::for_value(&Sparse {
                left: Some(1),
                right: None,
            }),
            PlanDigest::for_value(&Sparse {
                left: None,
                right: Some(1),
            }),
            PlanDigest::for_value(&Sparse {
                left: None,
                right: None,
            }),
            PlanDigest::for_value(&BTreeMap::from([("left", 1_u8), ("right", 2)])),
            PlanDigest::for_value(&Shape::Unit),
            PlanDigest::for_value(&Shape::Other),
            PlanDigest::for_value(&Shape::Newtype(1)),
            PlanDigest::for_value(&Shape::Tuple(1, 2)),
            PlanDigest::for_value(&Shape::Struct { value: 1 }),
            PlanDigest::for_value(&u128::MAX),
            PlanDigest::for_value(&i128::MIN),
        ];
        for (index, digest) in digests.iter().enumerate() {
            for (other_index, other) in digests.iter().enumerate().skip(index + 1) {
                assert_ne!(digest, other, "{index} and {other_index}");
            }
        }
    }

    #[test]
    fn equality_screens_hash_signed_zeros_alike_and_identities_do_not() {
        #[derive(Serialize)]
        struct Floats {
            single: f32,
            double: Vec<f64>,
        }
        let floats = |zero: f64| Floats {
            single: zero as f32,
            double: vec![1.5, zero],
        };
        assert_eq!(
            PlanDigest::for_equality_screen(&floats(-0.0)),
            PlanDigest::for_equality_screen(&floats(0.0))
        );
        assert_ne!(
            PlanDigest::for_value(&floats(-0.0)),
            PlanDigest::for_value(&floats(0.0))
        );
        // Away from zero, the screen keeps the identity's distinctions.
        assert_ne!(
            PlanDigest::for_equality_screen(&floats(0.0)),
            PlanDigest::for_equality_screen(&floats(f64::MIN_POSITIVE))
        );
        assert_eq!(
            PlanDigest::for_equality_screen(&("a", 1_u8)),
            PlanDigest::for_value(&("a", 1_u8)),
            "values without zeros digest the same either way"
        );
    }

    #[test]
    fn strings_at_every_word_boundary_digest_by_content_and_length() {
        let text = "abcdefghijklmnopq";
        let digests = (0..=text.len())
            .map(|len| PlanDigest::for_value(&&text[..len]))
            .collect::<Vec<_>>();
        for (len, digest) in digests.iter().enumerate() {
            assert_eq!(*digest, PlanDigest::for_value(&text[..len].to_owned()));
            assert!(
                digests[len + 1..].iter().all(|longer| longer != digest),
                "{len}"
            );
        }
        // Trailing zero bytes pad the last word, so the length separates them.
        assert_ne!(PlanDigest::for_value(&"a\0"), PlanDigest::for_value(&"a"));
    }

    #[test]
    fn displayed_text_digests_by_content() {
        struct Shown(&'static str);
        impl Serialize for Shown {
            fn serialize<S: ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(self.0)
            }
        }
        assert_eq!(
            PlanDigest::for_value(&Shown("a longer displayed value")),
            PlanDigest::for_value(&Shown("a longer displayed value"))
        );
        assert_ne!(
            PlanDigest::for_value(&Shown("a longer displayed value")),
            PlanDigest::for_value(&Shown("a longer displayed valuf"))
        );
        assert_ne!(
            PlanDigest::for_value(&Shown("12345678")),
            PlanDigest::for_value(&Shown("1234567"))
        );
    }
}
