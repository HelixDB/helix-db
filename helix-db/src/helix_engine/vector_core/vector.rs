use crate::{
    helix_engine::{
        types::{GraphError, VectorError},
        vector_core::{vector_data::VectorData, vector_distance::DistanceCalc},
    },
    protocol::{return_values::ReturnValue, value::Value},
    utils::{
        filterable::{Filterable, FilterableType},
        id::v6_uuid,
    },
};
use core::fmt;
use half::f16;
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, cmp::Ordering, collections::HashMap, fmt::Debug};

// Type markers for serialization
const F64_MARKER: u8 = 0x01;
const F32_MARKER: u8 = 0x02;
const F16_MARKER: u8 = 0x03;

// TODO: use const param to set dimension

#[repr(C, align(16))] // TODO: see performance impact of repr(C) and align(16)
#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct HVector {
    /// The id of the HVector
    pub id: u128,
    /// Whether the HVector is deleted (will be used for soft deletes)
    // pub is_deleted: bool,
    /// The level of the HVector
    #[serde(default)]
    pub level: usize,
    /// The distance of the HVector
    #[serde(default)]
    pub distance: Option<f64>,
    /// The actual vector data with precision support
    #[serde(default)]
    pub data: VectorData,
    /// The properties of the HVector
    #[serde(default)]
    pub properties: Option<HashMap<String, Value>>,

    /// the version of the vector
    #[serde(default)]
    pub version: u8,
}

impl Default for VectorData {
    fn default() -> Self {
        VectorData::F64(Vec::new())
    }
}

impl Eq for HVector {}
impl PartialOrd for HVector {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HVector {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .distance
            .partial_cmp(&self.distance)
            .unwrap_or(Ordering::Equal)
    }
}

impl Debug for HVector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{ \nid: {},\nlevel: {},\ndistance: {:?},\nprecision: {},\ndata: {:?},\nproperties: {:#?} }}",
            uuid::Uuid::from_u128(self.id),
            // self.is_deleted,
            self.level,
            self.distance,
            self.data.precision(),
            self.data,
            self.properties
        )
    }
}

impl HVector {
    #[inline(always)]
    pub fn new(data: VectorData) -> Self {
        let id = v6_uuid();
        HVector {
            id,
            // is_deleted: false,
            version: 1,
            level: 0,
            data,
            distance: None,
            properties: None,
        }
    }

    #[inline(always)]
    pub fn from_slice(level: usize, data: VectorData) -> Self {
        let id = v6_uuid();
        HVector {
            id,
            // is_deleted: false,
            version: 1,
            level,
            data,
            distance: None,
            properties: None,
        }
    }

    /// Create HVector from f64 slice (backwards compatibility)
    #[inline(always)]
    pub fn from_f64_slice(level: usize, data: Vec<f64>) -> Self {
        Self::from_slice(level, VectorData::F64(data))
    }

    #[inline(always)]
    pub fn decode_vector(
        raw_vector_bytes: &[u8],
        properties: Option<HashMap<String, Value>>,
        id: u128,
    ) -> Result<Self, VectorError> {
        let mut vector = HVector::from_bytes(id, 0, raw_vector_bytes)?;
        vector.properties = properties;
        Ok(vector)
    }

    /// Returns the data of the HVector as f64 vec (for calculations)
    #[inline(always)]
    pub fn get_data_f64(&self) -> Vec<f64> {
        match &self.data {
            VectorData::F64(v) => v.clone(),
            VectorData::F32(v) => v.iter().map(|&f| f as f64).collect(),
            VectorData::F16(v) => v.iter().map(|&f| f.to_f32() as f64).collect(),
        }
    }

    /// Returns a reference to the VectorData
    #[inline(always)]
    pub fn get_data(&self) -> &VectorData {
        &self.data
    }

    /// Returns the id of the HVector
    #[inline(always)]
    pub fn get_id(&self) -> u128 {
        self.id
    }

    /// Returns the level of the HVector
    #[inline(always)]
    pub fn get_level(&self) -> usize {
        self.level
    }

    /// Converts the HVector to a vec of bytes with type marker
    /// Format: [type_marker: 1 byte][data: n bytes]
    pub fn to_bytes(&self) -> Vec<u8> {
        let (marker, element_size) = match &self.data {
            VectorData::F64(_) => (F64_MARKER, std::mem::size_of::<f64>()),
            VectorData::F32(_) => (F32_MARKER, std::mem::size_of::<f32>()),
            VectorData::F16(_) => (F16_MARKER, std::mem::size_of::<u16>()),
        };

        let size = 1 + self.data.len() * element_size;
        let mut bytes = Vec::with_capacity(size);
        bytes.push(marker);

        match &self.data {
            VectorData::F64(vec) => {
                for &value in vec {
                    bytes.extend_from_slice(&value.to_be_bytes());
                }
            }
            VectorData::F32(vec) => {
                for &value in vec {
                    bytes.extend_from_slice(&value.to_be_bytes());
                }
            }
            VectorData::F16(vec) => {
                for &value in vec {
                    bytes.extend_from_slice(&value.to_bits().to_be_bytes());
                }
            }
        }

        bytes
    }

    /// Converts a byte array into a HVector, detecting precision from type marker
    /// Format: [type_marker: 1 byte][data: n bytes]
    /// For backwards compatibility, assumes F64 if no marker present
    pub fn from_bytes(id: u128, level: usize, bytes: &[u8]) -> Result<Self, VectorError> {
        if bytes.is_empty() {
            return Err(VectorError::InvalidVectorData);
        }

        // Check if this is a new format with type marker
        let (type_marker, data_bytes) =
            if bytes[0] == F64_MARKER || bytes[0] == F32_MARKER || bytes[0] == F16_MARKER {
                (bytes[0], &bytes[1..])
            } else {
                // Backwards compatibility: no marker means F64
                (F64_MARKER, bytes)
            };

        let data = match type_marker {
            F64_MARKER => {
                if !data_bytes.len().is_multiple_of(std::mem::size_of::<f64>()) {
                    return Err(VectorError::InvalidVectorData);
                }
                let mut vec = Vec::with_capacity(data_bytes.len() / std::mem::size_of::<f64>());
                for chunk in data_bytes.chunks_exact(std::mem::size_of::<f64>()) {
                    let value = f64::from_be_bytes(chunk.try_into().expect("Invalid chunk"));
                    vec.push(value);
                }
                VectorData::F64(vec)
            }
            F32_MARKER => {
                if !data_bytes.len().is_multiple_of(std::mem::size_of::<f32>()) {
                    return Err(VectorError::InvalidVectorData);
                }
                let mut vec = Vec::with_capacity(data_bytes.len() / std::mem::size_of::<f32>());
                for chunk in data_bytes.chunks_exact(std::mem::size_of::<f32>()) {
                    let value = f32::from_be_bytes(chunk.try_into().expect("Invalid chunk"));
                    vec.push(value);
                }
                VectorData::F32(vec)
            }
            F16_MARKER => {
                if !data_bytes.len().is_multiple_of(std::mem::size_of::<u16>()) {
                    return Err(VectorError::InvalidVectorData);
                }
                let mut vec = Vec::with_capacity(data_bytes.len() / std::mem::size_of::<u16>());
                for chunk in data_bytes.chunks_exact(std::mem::size_of::<u16>()) {
                    let bits = u16::from_be_bytes(chunk.try_into().expect("Invalid chunk"));
                    vec.push(f16::from_bits(bits));
                }
                VectorData::F16(vec)
            }
            _ => return Err(VectorError::InvalidVectorData),
        };

        Ok(HVector {
            id,
            level,
            version: 1,
            data,
            distance: None,
            properties: None,
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

    /// Returns the precision of the vector data
    pub fn precision(&self) -> &'static str {
        self.data.precision()
    }

    #[inline(always)]
    pub fn distance_to(&self, other: &HVector) -> Result<f64, VectorError> {
        HVector::distance(self, other)
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
}

/// Filterable implementation for HVector
///
/// see helix_db/src/protocol/filterable.rs
///
/// NOTE: This could be moved to the protocol module with the node and edges in `helix_db/protocol/items.rs``
impl Filterable for HVector {
    fn type_name(&self) -> FilterableType {
        FilterableType::Vector
    }

    fn id(&self) -> &u128 {
        &self.id
    }

    fn uuid(&self) -> String {
        uuid::Uuid::from_u128(self.id).to_string()
    }

    fn label(&self) -> &str {
        match &self.properties {
            Some(properties) => match properties.get("label") {
                Some(label) => label.as_str(),
                None => "vector",
            },
            None => "vector",
        }
    }

    fn from_node(&self) -> u128 {
        unreachable!()
    }

    fn from_node_uuid(&self) -> String {
        unreachable!()
    }

    fn to_node(&self) -> u128 {
        unreachable!()
    }

    fn to_node_uuid(&self) -> String {
        unreachable!()
    }

    fn properties(self) -> Option<HashMap<String, Value>> {
        let mut properties = self.properties.unwrap_or_default();
        let vec_values = match &self.data {
            VectorData::F64(v) => v.iter().map(|&f| Value::F64(f)).collect(),
            VectorData::F32(v) => v.iter().map(|&f| Value::F32(f)).collect(),
            VectorData::F16(v) => v.iter().map(|&f| Value::F32(f.to_f32())).collect(),
        };
        properties.insert("data".to_string(), Value::Array(vec_values));
        Some(properties)
    }

    fn vector_data(&self) -> &[f64] {
        // For backwards compatibility, we need to return &[f64]
        // This is a temporary workaround - ideally this method should be updated
        // For now, we'll need to convert on the fly
        // Note: This is inefficient and should be refactored later
        match &self.data {
            VectorData::F64(v) => v.as_slice(),
            _ => &[], // Return empty slice for non-f64 data
        }
    }

    fn score(&self) -> f64 {
        self.get_distance()
    }

    fn properties_mut(&mut self) -> &mut Option<HashMap<String, Value>> {
        &mut self.properties
    }

    fn properties_ref(&self) -> &Option<HashMap<String, Value>> {
        &self.properties
    }

    fn check_property(&self, key: &str) -> Result<Cow<'_, Value>, GraphError> {
        match key {
            "id" => Ok(Cow::Owned(Value::from(self.uuid()))),
            "label" => Ok(Cow::Owned(Value::from(self.label().to_string()))),
            "data" => {
                let vec_values = match &self.data {
                    VectorData::F64(v) => v.iter().map(|&f| Value::F64(f)).collect(),
                    VectorData::F32(v) => v.iter().map(|&f| Value::F32(f)).collect(),
                    VectorData::F16(v) => v.iter().map(|&f| Value::F32(f.to_f32())).collect(),
                };
                Ok(Cow::Owned(Value::Array(vec_values)))
            }
            "score" => Ok(Cow::Owned(Value::F64(self.score()))),
            _ => match &self.properties {
                Some(properties) => properties
                    .get(key)
                    .ok_or(GraphError::ConversionError(format!(
                        "Property {key} not found"
                    )))
                    .map(Cow::Borrowed),
                None => Err(GraphError::ConversionError(format!(
                    "Property {key} not found"
                ))),
            },
        }
    }

    fn find_property(
        &self,
        _key: &str,
        _secondary_properties: &HashMap<String, ReturnValue>,
        _property: &mut ReturnValue,
    ) -> Option<&ReturnValue> {
        unreachable!()
    }
}
