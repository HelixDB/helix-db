//! Database-global lifecycle markers, watermarks, reservations, and pointers.

use bytes::{BufMut, Bytes};

use crate::encoding::error::EncodingError;
use crate::encoding::v2::keys::scope::{DataScope, TenantId};
use crate::index_lifecycle::{
    IndexGenerationId, IndexId, IndexIdentity, IndexIdentityFamily, IndexOperationId,
    VectorPhysicalIndexId,
};

use super::{
    decode_generation, decode_identity, decode_index_id, decode_operation_id, encode_identity,
    identity_encoded_len, model_key_error, KeyDecoder, PartitionFingerprint, HASH_LEN, KIND_LEN,
    U32_LEN, U64_LEN, UUID_LEN,
};

const TENANT_ID_LEN: usize = core::mem::size_of::<u128>();
const GLOBAL_SENTINEL_LEN: usize = TENANT_ID_LEN + core::mem::size_of::<u8>();

/// Exact V2-only database-global envelope.
pub(crate) const GLOBAL_SENTINEL: [u8; GLOBAL_SENTINEL_LEN] = [0xFE; GLOBAL_SENTINEL_LEN];

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum GlobalKind {
    StorageVersion = 0x01,
    LogicalIndexIdWatermark = 0x02,
    VectorPhysicalIdWatermark = 0x03,
    OperationPointer = 0x04,
    LegacyVectorPhysicalReservation = 0x0A,
    TextCompactionPointer = 0x0B,
}

impl GlobalKind {
    const fn as_u8(self) -> u8 {
        self as u8
    }

    fn try_from_u8(value: u8) -> Result<Self, EncodingError> {
        match value {
            0x01 => Ok(Self::StorageVersion),
            0x02 => Ok(Self::LogicalIndexIdWatermark),
            0x03 => Ok(Self::VectorPhysicalIdWatermark),
            0x04 => Ok(Self::OperationPointer),
            0x0A => Ok(Self::LegacyVectorPhysicalReservation),
            0x0B => Ok(Self::TextCompactionPointer),
            unknown => Err(EncodingError::InvalidKey(format!(
                "unknown global V2 key kind {unknown:#04x}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TextCompactionTarget {
    scope: DataScope,
    identity: IndexIdentity,
    index_id: IndexId,
    generation: IndexGenerationId,
    partition: PartitionFingerprint,
    page: u32,
}

impl TextCompactionTarget {
    pub(crate) fn try_new(
        scope: DataScope,
        identity: IndexIdentity,
        index_id: IndexId,
        generation: IndexGenerationId,
        partition: PartitionFingerprint,
        page: u32,
    ) -> Result<Self, EncodingError> {
        if identity.family() != IndexIdentityFamily::Text {
            return Err(EncodingError::InvalidKey(
                "text compaction target requires a text identity".to_string(),
            ));
        }
        Ok(Self {
            scope,
            identity,
            index_id,
            generation,
            partition,
            page,
        })
    }

    pub(crate) const fn scope(&self) -> DataScope {
        self.scope
    }

    pub(crate) const fn identity(&self) -> &IndexIdentity {
        &self.identity
    }

    pub(crate) const fn index_id(&self) -> IndexId {
        self.index_id
    }

    pub(crate) const fn generation(&self) -> IndexGenerationId {
        self.generation
    }

    pub(crate) const fn partition(&self) -> PartitionFingerprint {
        self.partition
    }

    pub(crate) const fn page(&self) -> u32 {
        self.page
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum GlobalKey {
    StorageVersion,
    LogicalIndexIdWatermark,
    VectorPhysicalIdWatermark,
    OperationPointer(IndexOperationId),
    LegacyVectorPhysicalReservation(VectorPhysicalIndexId),
    TextCompactionPointer(TextCompactionTarget),
}

impl GlobalKey {
    pub(crate) fn logical_prefix(kind: GlobalKind) -> Bytes {
        let mut bytes = Vec::with_capacity(GLOBAL_SENTINEL_LEN + KIND_LEN);
        bytes.put_slice(&GLOBAL_SENTINEL);
        bytes.put_u8(kind.as_u8());
        Bytes::from(bytes)
    }

    pub(crate) const fn kind(&self) -> GlobalKind {
        match self {
            Self::StorageVersion => GlobalKind::StorageVersion,
            Self::LogicalIndexIdWatermark => GlobalKind::LogicalIndexIdWatermark,
            Self::VectorPhysicalIdWatermark => GlobalKind::VectorPhysicalIdWatermark,
            Self::OperationPointer(_) => GlobalKind::OperationPointer,
            Self::LegacyVectorPhysicalReservation(_) => GlobalKind::LegacyVectorPhysicalReservation,
            Self::TextCompactionPointer(_) => GlobalKind::TextCompactionPointer,
        }
    }

    pub(crate) fn encoded_len(&self) -> usize {
        let suffix = match self {
            Self::StorageVersion
            | Self::LogicalIndexIdWatermark
            | Self::VectorPhysicalIdWatermark => 0,
            Self::OperationPointer(_) => UUID_LEN,
            Self::LegacyVectorPhysicalReservation(_) => U64_LEN,
            Self::TextCompactionPointer(target) => {
                KIND_LEN
                    + target.scope.encoded_len()
                    + identity_encoded_len(&target.identity)
                    + U64_LEN
                    + U64_LEN
                    + HASH_LEN
                    + U32_LEN
            }
        };
        GLOBAL_SENTINEL_LEN + KIND_LEN + suffix
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buffer: &mut B) {
        buffer.put_slice(&GLOBAL_SENTINEL);
        buffer.put_u8(self.kind().as_u8());
        match self {
            Self::StorageVersion
            | Self::LogicalIndexIdWatermark
            | Self::VectorPhysicalIdWatermark => {}
            Self::OperationPointer(id) => buffer.put_slice(id.as_bytes()),
            Self::LegacyVectorPhysicalReservation(physical_index_id) => {
                buffer.put_u64(physical_index_id.get());
            }
            Self::TextCompactionPointer(target) => {
                match target.scope {
                    DataScope::LegacyUnscoped => buffer.put_u8(0x00),
                    DataScope::Tenant(tenant_id) => {
                        buffer.put_u8(0x01);
                        buffer.put_slice(&tenant_id.as_u128().to_be_bytes());
                    }
                }
                encode_identity(&target.identity, buffer);
                buffer.put_u64(target.index_id.get());
                buffer.put_u64(target.generation.get());
                buffer.put_slice(target.partition.as_bytes());
                buffer.put_u32(target.page);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn to_bytes(&self) -> Bytes {
        let mut bytes = Vec::with_capacity(self.encoded_len());
        self.encode_into(&mut bytes);
        Bytes::from(bytes)
    }

    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        const SENTINEL_OFFSET: usize = 0;
        const KIND_OFFSET: usize = SENTINEL_OFFSET + GLOBAL_SENTINEL_LEN;
        const SUFFIX_OFFSET: usize = KIND_OFFSET + KIND_LEN;
        if slice.len() < GLOBAL_SENTINEL_LEN + KIND_LEN {
            return Err(EncodingError::BufferTooShort {
                expected: GLOBAL_SENTINEL_LEN + KIND_LEN,
                actual: slice.len(),
            });
        }
        if slice[SENTINEL_OFFSET..SENTINEL_OFFSET + GLOBAL_SENTINEL_LEN] != GLOBAL_SENTINEL {
            return Err(EncodingError::InvalidKey(
                "global V2 sentinel mismatch".to_string(),
            ));
        }
        let kind = GlobalKind::try_from_u8(slice[KIND_OFFSET])?;
        let mut decoder =
            KeyDecoder::new(&slice[SUFFIX_OFFSET..SUFFIX_OFFSET + slice.len() - SUFFIX_OFFSET]);
        let key = match kind {
            GlobalKind::StorageVersion => Self::StorageVersion,
            GlobalKind::LogicalIndexIdWatermark => Self::LogicalIndexIdWatermark,
            GlobalKind::VectorPhysicalIdWatermark => Self::VectorPhysicalIdWatermark,
            GlobalKind::OperationPointer => {
                Self::OperationPointer(decode_operation_id(&mut decoder)?)
            }
            GlobalKind::LegacyVectorPhysicalReservation => Self::LegacyVectorPhysicalReservation(
                VectorPhysicalIndexId::new(decoder.take_u64()?).map_err(model_key_error)?,
            ),
            GlobalKind::TextCompactionPointer => {
                let scope = match decoder.take_u8()? {
                    0x00 => DataScope::LegacyUnscoped,
                    0x01 => DataScope::Tenant(TenantId::from_u128(u128::from_be_bytes(
                        decoder.take_array::<TENANT_ID_LEN>()?,
                    ))),
                    unknown => {
                        return Err(EncodingError::InvalidKey(format!(
                            "unknown text compaction scope {unknown:#04x}"
                        )));
                    }
                };
                Self::TextCompactionPointer(TextCompactionTarget::try_new(
                    scope,
                    decode_identity(&mut decoder)?,
                    decode_index_id(&mut decoder)?,
                    decode_generation(&mut decoder)?,
                    PartitionFingerprint::new(decoder.take_array::<HASH_LEN>()?),
                    decoder.take_u32()?,
                )?)
            }
        };
        decoder.finish()?;
        Ok(key)
    }
}
