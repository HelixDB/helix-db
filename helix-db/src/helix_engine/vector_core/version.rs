use std::mem::size_of;
use std::{borrow::Cow, fmt};

use byteorder::{BigEndian, ByteOrder};
use heed3::BoxedError;

#[derive(Debug, Clone, Copy)]
pub struct Version {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl Version {
    pub fn current() -> Self {
        Version {
            major: env!("CARGO_PKG_VERSION_MAJOR").parse().unwrap(),
            minor: env!("CARGO_PKG_VERSION_MINOR").parse().unwrap(),
            patch: env!("CARGO_PKG_VERSION_PATCH").parse().unwrap(),
        }
    }
}

pub enum VersionCodec {}

impl<'a> heed3::BytesEncode<'a> for VersionCodec {
    type EItem = Version;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, BoxedError> {
        let Version {
            major,
            minor,
            patch,
        } = item;

        let mut output = Vec::with_capacity(size_of::<u32>() * 3);
        output.extend_from_slice(&major.to_be_bytes());
        output.extend_from_slice(&minor.to_be_bytes());
        output.extend_from_slice(&patch.to_be_bytes());

        Ok(Cow::Owned(output))
    }
}

impl heed3::BytesDecode<'_> for VersionCodec {
    type DItem = Version;

    fn bytes_decode(bytes: &'_ [u8]) -> Result<Self::DItem, BoxedError> {
        let major = BigEndian::read_u32(bytes);
        let bytes = &bytes[size_of_val(&major)..];
        let minor = BigEndian::read_u32(bytes);
        let bytes = &bytes[size_of_val(&minor)..];
        let patch = BigEndian::read_u32(bytes);

        Ok(Version {
            major,
            minor,
            patch,
        })
    }
}

#[cfg(test)]
mod test {
    use heed3::{BytesDecode, BytesEncode};

    use super::*;

    #[test]
    fn version_codec() {
        let version = Version {
            major: 0,
            minor: 10,
            patch: 100,
        };

        let encoded = VersionCodec::bytes_encode(&version).unwrap();
        let decoded = VersionCodec::bytes_decode(&encoded).unwrap();

        assert_eq!(version.major, decoded.major);
        assert_eq!(version.minor, decoded.minor);
        assert_eq!(version.patch, decoded.patch);
    }
}
