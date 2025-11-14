use crate::{
    helix_engine::{
        types::VectorError,
        vector_core::{vector_distance::DistanceCalc, vector_without_data::VectorWithoutData},
    },
    protocol::{custom_serde::vector_serde::VectorDeSeed, value::Value},
    utils::{
        id::{uuid_str_from_buf, v6_uuid},
        properties::ImmutablePropertiesMap,
    },
};
use bincode::Options;
use core::fmt;
use serde::{Serialize, Serializer, ser::SerializeMap};
use std::{alloc, cmp::Ordering, fmt::Debug, mem, ptr, slice};

// TODO: make this generic over the type of encoding (f32, f64, etc)
// TODO: use const param to set dimension
// TODO: set level as u8

#[repr(C, align(16))] // TODO: see performance impact of repr(C) and align(16)
#[derive(Clone, Copy)]
pub struct HVector<'arena> {
    /// The id of the HVector
    pub id: u128,
    /// The label of the HVector
    pub label: &'arena str,
    /// the version of the vector
    pub version: u8,
    /// whether the vector is deleted
    pub deleted: bool,
    /// The level of the HVector
    pub level: usize,
    /// The distance of the HVector
    pub distance: Option<f64>,
    /// The actual vector
    pub data: &'arena [f64],
    /// The properties of the HVector
    pub properties: Option<ImmutablePropertiesMap<'arena>>,
}

impl<'arena> Serialize for HVector<'arena> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;

        // Check if this is a human-readable format (like JSON)
        if serializer.is_human_readable() {
            // Include id for JSON serialization
            let mut buffer = [0u8; 36];
            let mut state = serializer.serialize_map(Some(
                5 + self.properties.as_ref().map(|p| p.len()).unwrap_or(0),
            ))?;
            state.serialize_entry("id", uuid_str_from_buf(self.id, &mut buffer))?;
            state.serialize_entry("label", &self.label)?;
            state.serialize_entry("version", &self.version)?;
            state.serialize_entry("deleted", &self.deleted)?;
            if let Some(properties) = &self.properties {
                for (key, value) in properties.iter() {
                    state.serialize_entry(key, value)?;
                }
            }
            state.end()
        } else {
            // Skip id, level, distance, and data for bincode serialization
            let mut state = serializer.serialize_struct("HVector", 4)?;
            state.serialize_field("label", &self.label)?;
            state.serialize_field("version", &self.version)?;
            state.serialize_field("deleted", &self.deleted)?;
            state.serialize_field("properties", &self.properties)?;
            state.end()
        }
    }
}

impl PartialEq for HVector<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for HVector<'_> {}
impl PartialOrd for HVector<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HVector<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .distance
            .partial_cmp(&self.distance)
            .unwrap_or(Ordering::Equal)
    }
}

impl Debug for HVector<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{ \nid: {},\nlevel: {},\ndistance: {:?},\ndata: {:?}, }}",
            uuid::Uuid::from_u128(self.id),
            // self.is_deleted,
            self.level,
            self.distance,
            self.data,
        )
    }
}

impl<'arena> HVector<'arena> {
    #[inline(always)]
    pub fn from_slice(label: &'arena str, level: usize, data: &'arena [f64]) -> Self {
        let id = v6_uuid();
        HVector {
            id,
            // is_deleted: false,
            version: 1,
            level,
            label,
            data,
            distance: None,
            properties: None,
            deleted: false,
        }
    }

    /// Converts the HVector to an vec of bytes by accessing the data field directly
    /// and converting each f64 to a byte slice
    #[inline(always)]
    pub fn vector_data_to_bytes(&self) -> Result<&[u8], VectorError> {
        bytemuck::try_cast_slice(self.data).map_err(|_| {
            VectorError::ConversionError("Invalid vector data: vector data".to_string())
        })
    }

    /// Deserializes bytes into an vector using a custom deserializer that allocates into the provided arena
    ///
    /// Both the properties bytes (if present) and the raw vector data are combined to generate the final vector struct
    ///
    /// NOTE: in this method, fixint encoding is used
    #[inline]
    pub fn from_bincode_bytes<'txn>(
        arena: &'arena bumpalo::Bump,
        properties: Option<&'txn [u8]>,
        raw_vector_data: &'txn [u8],
        id: u128,
    ) -> Result<Self, VectorError> {
        bincode::options()
            .with_fixint_encoding()
            .allow_trailing_bytes()
            .deserialize_seed(
                VectorDeSeed {
                    arena,
                    id,
                    raw_vector_data,
                },
                properties.unwrap_or(&[]),
            )
            .map_err(|e| VectorError::ConversionError(format!("Error deserializing vector: {e}")))
    }

    #[inline(always)]
    pub fn to_bincode_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    /// Casts the raw bytes to a f64 slice by copying them once into the arena
    #[inline]
    pub fn cast_raw_vector_data<'txn>(
        arena: &'arena bumpalo::Bump,
        raw_vector_data: &'txn [u8],
    ) -> &'arena [f64] {
        assert!(!raw_vector_data.is_empty(), "raw_vector_data.len() == 0");
        assert!(
            raw_vector_data.len().is_multiple_of(mem::size_of::<f64>()),
            "raw_vector_data bytes len is not a multiple of size_of::<f64>()"
        );
        let dimensions = raw_vector_data.len() / mem::size_of::<f64>();

        assert!(
            raw_vector_data.len().is_multiple_of(dimensions),
            "raw_vector_data does not have the exact required number of dimensions"
        );

        let layout = alloc::Layout::array::<f64>(dimensions)
            .expect("vector_data array arithmetic overflow or total size exceeds isize::MAX");

        let vector_data: ptr::NonNull<u8> = arena.alloc_layout(layout);

        // 'arena because the destination pointer is allocated in the arena
        let data: &'arena [f64] = unsafe {
            // SAFETY:
            // - We assert data is present and that we are within bounds in asserts above
            ptr::copy_nonoverlapping(
                raw_vector_data.as_ptr(),
                vector_data.as_ptr(),
                raw_vector_data.len(),
            );

            // We allocated with the layout of an f64 array
            let vector_data: ptr::NonNull<f64> = vector_data.cast();

            // SAFETY:
            // - `vector_data`` is guaranteed to be valid by being NonNull
            // - the asserts above guarantee that there are enough valid bytes to be read
            slice::from_raw_parts(vector_data.as_ptr(), dimensions)
        };

        data
    }

    /// Uses just the vector data to generate a HVector struct
    pub fn from_raw_vector_data<'txn>(
        arena: &'arena bumpalo::Bump,
        raw_vector_data: &'txn [u8],
        label: &'arena str,
        id: u128,
    ) -> Result<Self, VectorError> {
        let data = Self::cast_raw_vector_data(arena, raw_vector_data);
        Ok(HVector {
            id,
            label,
            data,
            version: 1,
            level: 0,
            distance: None,
            properties: None,
            deleted: false,
        })
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    #[inline(always)]
    pub fn distance_to(&self, other: &HVector) -> Result<f64, VectorError> {
        HVector::<'arena>::distance(self, other)
    }

    #[inline(always)]
    pub fn set_distance(&mut self, distance: f64) {
        self.distance = Some(distance);
    }

    #[inline(always)]
    pub fn get_distance(&self) -> f64 {
        self.distance.unwrap_or(2.0)
    }

    #[inline(always)]
    pub fn get_label(&self) -> Option<&Value> {
        match &self.properties {
            Some(p) => p.get("label"),
            None => None,
        }
    }

    #[inline(always)]
    pub fn get_property(&self, key: &str) -> Option<&'arena Value> {
        self.properties.as_ref().and_then(|value| value.get(key))
    }

    pub fn id(&self) -> &u128 {
        &self.id
    }

    pub fn label(&self) -> &'arena str {
        self.label
    }

    pub fn score(&self) -> f64 {
        self.distance.unwrap_or(2.0)
    }

    pub fn expand_from_vector_without_data(&mut self, vector: VectorWithoutData<'arena>) {
        self.label = vector.label;
        self.version = vector.version;
        self.level = vector.level;
        self.properties = vector.properties;
    }
}

impl<'arena> From<VectorWithoutData<'arena>> for HVector<'arena> {
    fn from(value: VectorWithoutData<'arena>) -> Self {
        HVector {
            id: value.id,
            label: value.label,
            version: value.version,
            level: value.level,
            distance: None,
            data: &[],
            properties: value.properties,
            deleted: value.deleted,
        }
    }
}
