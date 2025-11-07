use heed3::{Database, RoTxn, RwTxn, WithTls, types::Bytes};

use crate::helix_engine::types::GraphError;

pub const STORAGE_VERSION_KEY: &[u8] = b"storage_version";
pub const VECTOR_ENDIANNESS_KEY: &[u8] = b"vector_endianness";

/// Each version that needs a migration is a variant in this enum.
/// Since different versions will have different metadata keys they are
/// fields of the variants.
pub enum StorageMetadata {
    /// Data is stored in a version before the metadata table existed.
    PreMetadata,
    /// The first version that introduced storing vectors in native-endian.
    /// Stores VectorEndianness so the vectors can be migrated to native-endian
    /// when the database is copied to a machine with a different endianness.
    VectorNativeEndianness { vector_endianness: VectorEndianness },
}

mod storage_version_tag {
    pub const VECTOR_NATIVE_ENDIANNESS: u64 = 1;
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum VectorEndianness {
    BigEndian,
    LittleEndian,
}

pub const NATIVE_VECTOR_ENDIANNESS: VectorEndianness = if cfg!(target_endian = "little") {
    VectorEndianness::LittleEndian
} else {
    VectorEndianness::BigEndian
};

mod vector_endianness_value {
    pub const BIG_ENDIAN: &[u8] = b"big";
    pub const LITTLE_ENDIAN: &[u8] = b"lil";
}

impl StorageMetadata {
    pub fn read(
        txn: &RoTxn<WithTls>,
        metadata_db: &Database<Bytes, Bytes>,
    ) -> Result<Self, GraphError> {
        match metadata_db.get(txn, STORAGE_VERSION_KEY)? {
            None => Ok(Self::PreMetadata),
            Some(version_bytes) => {
                let version_byte_array: [u8; std::mem::size_of::<u64>()] =
                    version_bytes.try_into().map_err(|e| {
                        GraphError::New(format!("storage metadata version tag is not a u64: {e:?}"))
                    })?;

                let version = u64::from_le_bytes(version_byte_array);

                Self::parse(version, txn, metadata_db)
            }
        }
    }

    pub fn save(
        &self,
        txn: &mut RwTxn,
        metadata_db: &Database<Bytes, Bytes>,
    ) -> Result<(), GraphError> {
        match self {
            Self::PreMetadata => {
                panic!("can't save metadata that represents a version before metadata table")
            }
            Self::VectorNativeEndianness { vector_endianness } => {
                Self::save_version(
                    storage_version_tag::VECTOR_NATIVE_ENDIANNESS,
                    txn,
                    metadata_db,
                )?;
                vector_endianness.save(txn, metadata_db)?;
            }
        }

        Ok(())
    }

    fn parse(
        version: u64,
        txn: &RoTxn<WithTls>,
        metadata_db: &Database<Bytes, Bytes>,
    ) -> Result<Self, GraphError> {
        match version {
            storage_version_tag::VECTOR_NATIVE_ENDIANNESS => {
                Self::parse_vector_native_endianness(txn, metadata_db)
            }
            _ => Err(GraphError::New(format!(
                "storage metadata version tag unknown: {version}"
            ))),
        }
    }

    fn parse_vector_native_endianness(
        txn: &RoTxn<WithTls>,
        metadata_db: &Database<Bytes, Bytes>,
    ) -> Result<Self, GraphError> {
        Ok(Self::VectorNativeEndianness {
            vector_endianness: VectorEndianness::read(txn, metadata_db)?,
        })
    }

    fn save_version(
        version: u64,
        txn: &mut RwTxn,
        metadata_db: &Database<Bytes, Bytes>,
    ) -> Result<(), GraphError> {
        metadata_db.put(txn, STORAGE_VERSION_KEY, &version.to_le_bytes())?;

        Ok(())
    }
}

impl VectorEndianness {
    fn read(
        txn: &RoTxn<WithTls>,
        metadata_db: &Database<Bytes, Bytes>,
    ) -> Result<Self, GraphError> {
        let endianness_bytes = metadata_db
            .get(txn, VECTOR_ENDIANNESS_KEY)?
            .ok_or_else(|| {
                GraphError::New("missing vector endianness key in metadata db".into())
            })?;

        match endianness_bytes {
            vector_endianness_value::BIG_ENDIAN => Ok(Self::BigEndian),
            vector_endianness_value::LITTLE_ENDIAN => Ok(Self::LittleEndian),
            _ => Err(GraphError::New(
                "unknown vector endianness value in metadata db".into(),
            )),
        }
    }

    fn save(
        &self,
        txn: &mut RwTxn,
        metadata_db: &Database<Bytes, Bytes>,
    ) -> Result<(), GraphError> {
        let endianness_bytes = match self {
            Self::BigEndian => vector_endianness_value::BIG_ENDIAN,
            Self::LittleEndian => vector_endianness_value::LITTLE_ENDIAN,
        };

        metadata_db.put(txn, VECTOR_ENDIANNESS_KEY, endianness_bytes)?;

        Ok(())
    }
}
