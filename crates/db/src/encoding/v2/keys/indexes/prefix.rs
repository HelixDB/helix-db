//! Shared property-index prefix parsing and exclusive scan bounds.

use bytes::Bytes;

use crate::encoding::{
    error::EncodingError,
    indexes::{
        range::{EdgeRangeIndexDirection, RangeIndexDirection},
        EdgeDirection,
    },
    keys::{KeyPrefix, PREFIX_LEN},
};

use super::property::INDEX_PREFIX_LEN;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IndexPrefix {
    Equality,
    Range(RangeIndexDirection),
    EdgeEquality,
    EdgeLabel,
    EdgeLabelNeighbor(EdgeDirection),
    EdgeRange(EdgeRangeIndexDirection, EdgeDirection),
    GlobalEdgeEquality,
    GlobalEdgeRange(RangeIndexDirection),
}

impl IndexPrefix {
    pub const fn as_slice(&self) -> &[u8] {
        match self {
            IndexPrefix::Equality => &[0x00],
            IndexPrefix::Range(direction) => match direction {
                RangeIndexDirection::Asc => &[0x01],
                RangeIndexDirection::Desc => &[0x05],
            },
            IndexPrefix::EdgeEquality => &[0x02],
            IndexPrefix::EdgeLabel => &[0x04],
            IndexPrefix::EdgeLabelNeighbor(direction) => match direction {
                EdgeDirection::Out => &[0x10, 0x00],
                EdgeDirection::In => &[0x10, 0x01],
            },
            IndexPrefix::GlobalEdgeEquality => &[0x08],
            IndexPrefix::GlobalEdgeRange(direction) => match direction {
                RangeIndexDirection::Asc => &[0x09],
                RangeIndexDirection::Desc => &[0x0a],
            },
            IndexPrefix::EdgeRange(range_direction, edge_direction) => match range_direction {
                EdgeRangeIndexDirection::Asc => match edge_direction {
                    EdgeDirection::Out => {
                        &[EdgeRangeIndexDirection::Asc as u8, EdgeDirection::Out as u8]
                    }
                    EdgeDirection::In => {
                        &[EdgeRangeIndexDirection::Asc as u8, EdgeDirection::In as u8]
                    }
                },
                EdgeRangeIndexDirection::Desc => match edge_direction {
                    EdgeDirection::Out => &[
                        EdgeRangeIndexDirection::Desc as u8,
                        EdgeDirection::Out as u8,
                    ],
                    EdgeDirection::In => {
                        &[EdgeRangeIndexDirection::Desc as u8, EdgeDirection::In as u8]
                    }
                },
            },
        }
    }

    pub fn from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        if slice.len() < PREFIX_LEN + INDEX_PREFIX_LEN {
            return Err(EncodingError::BufferTooShort {
                expected: PREFIX_LEN + INDEX_PREFIX_LEN,
                actual: slice.len(),
            });
        }

        if KeyPrefix::from_u8(slice[0])? != KeyPrefix::PropertyIndex {
            return Err(EncodingError::InvalidKey(format!(
                "expected PropertyIndex key prefix ({:#04x}), got {:#04x}",
                KeyPrefix::PropertyIndex.as_u8(),
                slice[0]
            )));
        }

        match slice[PREFIX_LEN] {
            0x00 => Ok(IndexPrefix::Equality),
            0x01 => Ok(IndexPrefix::Range(RangeIndexDirection::Asc)),
            0x05 => Ok(IndexPrefix::Range(RangeIndexDirection::Desc)),
            0x02 => Ok(IndexPrefix::EdgeEquality),
            0x04 => Ok(IndexPrefix::EdgeLabel),
            0x08 => Ok(IndexPrefix::GlobalEdgeEquality),
            0x09 => Ok(IndexPrefix::GlobalEdgeRange(RangeIndexDirection::Asc)),
            0x0a => Ok(IndexPrefix::GlobalEdgeRange(RangeIndexDirection::Desc)),
            0x10 => {
                if slice.len() < PREFIX_LEN + INDEX_PREFIX_LEN + size_of::<EdgeDirection>() {
                    return Err(EncodingError::BufferTooShort {
                        expected: PREFIX_LEN + INDEX_PREFIX_LEN + size_of::<EdgeDirection>(),
                        actual: slice.len(),
                    });
                }

                let edge_direction = EdgeDirection::from_u8(slice[PREFIX_LEN + INDEX_PREFIX_LEN])?;
                Ok(IndexPrefix::EdgeLabelNeighbor(edge_direction))
            }
            0x03 | 0x06 => {
                if slice.len() < PREFIX_LEN + INDEX_PREFIX_LEN + size_of::<EdgeDirection>() {
                    return Err(EncodingError::BufferTooShort {
                        expected: PREFIX_LEN + INDEX_PREFIX_LEN + size_of::<EdgeDirection>(),
                        actual: slice.len(),
                    });
                }

                let edge_direction = EdgeDirection::from_u8(slice[PREFIX_LEN + INDEX_PREFIX_LEN])?;
                match slice[PREFIX_LEN] {
                    0x03 => Ok(IndexPrefix::EdgeRange(
                        EdgeRangeIndexDirection::Asc,
                        edge_direction,
                    )),
                    0x06 => Ok(IndexPrefix::EdgeRange(
                        EdgeRangeIndexDirection::Desc,
                        edge_direction,
                    )),
                    _ => unreachable!("edge range index prefix was checked above"),
                }
            }
            invalid => Err(EncodingError::InvalidIndexPrefix(invalid)),
        }
    }
}

pub(crate) fn exclusive_prefix_end_bound(prefix: &Bytes) -> Option<Bytes> {
    let mut end = prefix.to_vec();
    let offset = end.iter().rposition(|byte| *byte != u8::MAX)?;
    end[offset] += 1;
    end.truncate(offset + core::mem::size_of::<u8>());
    Some(Bytes::from(end))
}

#[cfg(test)]
mod tests {
    use super::super::direction::EdgeDirection as RangeEdgeDirection;
    use super::super::equality::{scans::*, EdgeDirection as EqualityEdgeDirection};
    use super::super::label::{EdgeLabelNeighborScanPrefix, EdgeLabelScanPrefix};
    use super::super::range::scans::*;
    use super::*;
    use crate::encoding::indexes::{PropertyHash, ValueHash};

    const PROP: PropertyHash = [1, 2, 3, 4];
    const VALUE: ValueHash = [5, 6, 7, 8, 9, 10, 11, 12];

    #[test]
    fn equality_prefixes_encode_only_valid_segments() {
        assert_eq!(EqualityScanPrefix::Index.to_bytes().as_ref(), &[0x03, 0x00]);
        assert_eq!(
            EqualityScanPrefix::Property {
                property_hash: PROP,
            }
            .to_bytes()
            .as_ref(),
            &[0x03, 0x00, 1, 2, 3, 4]
        );
        assert_eq!(
            EqualityScanPrefix::PropertyValue {
                property_hash: PROP,
                value_hash: VALUE,
            }
            .to_bytes()
            .as_ref(),
            &[0x03, 0x00, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        );
    }

    #[test]
    fn exclusive_prefix_end_bound_increments_and_truncates() {
        let prefix = Bytes::from_static(&[0x03, 0x01, 0xAA]);
        assert_eq!(
            exclusive_prefix_end_bound(&prefix).unwrap().as_ref(),
            &[0x03, 0x01, 0xAB]
        );
        assert_eq!(
            exclusive_prefix_end_bound(&Bytes::from_static(&[0x03, 0xFF]))
                .unwrap()
                .as_ref(),
            &[0x04]
        );
        assert!(exclusive_prefix_end_bound(&Bytes::from_static(&[0xFF])).is_none());
    }

    #[test]
    fn regression_exclusive_prefix_end_includes_every_key_with_the_prefix() {
        let prefix = Bytes::from_static(&[0x03, 0x01, 0xAA]);
        let end = exclusive_prefix_end_bound(&prefix).unwrap();
        let prefixed_keys = [
            vec![0x03, 0x01, 0xAA],
            vec![0x03, 0x01, 0xAA, 0xFE],
            vec![0x03, 0x01, 0xAA, 0xFF],
            vec![0x03, 0x01, 0xAA, 0xFF, 0x00],
            vec![0x03, 0x01, 0xAA, 0xFF, 0x7A, 0xFE],
        ];

        for key in prefixed_keys {
            assert!(
                key.as_slice() < end.as_ref(),
                "{key:?} must remain inside the prefix scan"
            );
        }
        assert!(
            [0x03, 0x01, 0xAB].as_slice() >= end.as_ref(),
            "the first key outside the prefix must not be scanned"
        );
    }

    #[test]
    fn edge_equality_prefixes_encode_source_before_optional_segments() {
        let source = 0x0102_0304_0506_0708u64;
        assert_eq!(
            EdgeEqualityScanPrefix::Index.to_bytes().as_ref(),
            &[0x03, 0x02]
        );
        assert_eq!(
            EdgeEqualityScanPrefix::Direction {
                direction: EqualityEdgeDirection::Out,
            }
            .to_bytes()
            .as_ref(),
            &[0x03, 0x02, 0x00]
        );

        let mut expected = vec![0x03, 0x02, EqualityEdgeDirection::Out.as_u8()];
        expected.extend_from_slice(&source.to_be_bytes());
        assert_eq!(
            EdgeEqualityScanPrefix::Source {
                direction: EqualityEdgeDirection::Out,
                source,
            }
            .to_bytes()
            .as_ref(),
            expected.as_slice()
        );

        expected.extend_from_slice(&PROP);
        assert_eq!(
            EdgeEqualityScanPrefix::Property {
                direction: EqualityEdgeDirection::Out,
                source,
                property_hash: PROP,
            }
            .to_bytes()
            .as_ref(),
            expected.as_slice()
        );

        expected.extend_from_slice(&VALUE);
        assert_eq!(
            EdgeEqualityScanPrefix::PropertyValue {
                direction: EqualityEdgeDirection::Out,
                source,
                property_hash: PROP,
                value_hash: VALUE,
            }
            .to_bytes()
            .as_ref(),
            expected.as_slice()
        );
    }

    #[test]
    fn global_edge_equality_prefixes_encode_only_valid_segments() {
        assert_eq!(
            GlobalEdgeEqualityScanPrefix::Index.to_bytes().as_ref(),
            &[0x03, 0x08]
        );
        assert_eq!(
            GlobalEdgeEqualityScanPrefix::Property {
                property_hash: PROP,
            }
            .to_bytes()
            .as_ref(),
            &[0x03, 0x08, 1, 2, 3, 4]
        );
        assert_eq!(
            GlobalEdgeEqualityScanPrefix::PropertyValue {
                property_hash: PROP,
                value_hash: VALUE,
            }
            .to_bytes()
            .as_ref(),
            &[0x03, 0x08, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        );
    }

    #[test]
    fn edge_label_prefixes_encode_label_hash_layouts() {
        let node_id = 0x0102_0304_0506_0708u64;
        assert_eq!(
            EdgeLabelScanPrefix::Index.to_bytes().as_ref(),
            &[0x03, 0x04]
        );
        assert_eq!(
            EdgeLabelScanPrefix::Label { label_hash: VALUE }
                .to_bytes()
                .as_ref(),
            &[0x03, 0x04, 5, 6, 7, 8, 9, 10, 11, 12]
        );
        assert_eq!(
            EdgeLabelNeighborScanPrefix::Index.to_bytes().as_ref(),
            &[0x03, 0x10]
        );
        assert_eq!(
            EdgeLabelNeighborScanPrefix::Direction {
                direction: RangeEdgeDirection::Out,
            }
            .to_bytes()
            .as_ref(),
            &[0x03, 0x10, 0x00]
        );

        let mut endpoint = vec![0x03, 0x10, 0x01];
        endpoint.extend_from_slice(&node_id.to_be_bytes());
        assert_eq!(
            EdgeLabelNeighborScanPrefix::Endpoint {
                direction: RangeEdgeDirection::In,
                node_id,
            }
            .to_bytes()
            .as_ref(),
            endpoint.as_slice()
        );

        endpoint.extend_from_slice(&VALUE);
        assert_eq!(
            EdgeLabelNeighborScanPrefix::Label {
                direction: RangeEdgeDirection::In,
                node_id,
                label_hash: VALUE,
            }
            .to_bytes()
            .as_ref(),
            endpoint.as_slice()
        );
    }

    #[test]
    fn range_prefixes_and_bounds_encode_existing_layout() {
        assert_eq!(
            RangeScanPrefix::Direction {
                direction: RangeIndexDirection::Asc,
            }
            .to_bytes()
            .as_ref(),
            &[0x03, 0x01]
        );
        assert_eq!(
            RangeScanPrefix::Property {
                direction: RangeIndexDirection::Desc,
                property_hash: PROP,
            }
            .to_bytes()
            .as_ref(),
            &[0x03, 0x05, 1, 2, 3, 4]
        );

        let value_prefix = RangeScanValuePrefix::new(RangeIndexDirection::Desc, PROP, "a\0");
        assert_eq!(
            value_prefix.to_bytes().as_ref(),
            &[0x03, 0x05, 1, 2, 3, 4, 0x9E, 0xFF, 0x00, 0xFF, 0xFE]
        );
        assert_eq!(
            value_prefix.exclusive_end_bound().as_ref(),
            &[0x03, 0x05, 1, 2, 3, 4, 0x9E, 0xFF, 0x00, 0xFF, 0xFE, 0xFF,]
        );

        let inclusive_end =
            RangeScanValuePrefix::new(RangeIndexDirection::Asc, PROP, "a").inclusive_end_bound();
        let mut expected = vec![0x03, 0x01, 1, 2, 3, 4, b'a'];
        expected.extend_from_slice(&u64::MAX.to_be_bytes());
        expected.push(0);
        assert_eq!(inclusive_end.as_ref(), expected.as_slice());
    }

    #[test]
    fn global_edge_range_prefixes_and_bounds_encode_existing_layout() {
        assert_eq!(
            GlobalEdgeRangeScanPrefix::Direction {
                direction: RangeIndexDirection::Asc,
            }
            .to_bytes()
            .as_ref(),
            &[0x03, 0x09]
        );
        assert_eq!(
            GlobalEdgeRangeScanPrefix::Property {
                direction: RangeIndexDirection::Desc,
                property_hash: PROP,
            }
            .to_bytes()
            .as_ref(),
            &[0x03, 0x0a, 1, 2, 3, 4]
        );

        let value_prefix =
            GlobalEdgeRangeScanValuePrefix::new(RangeIndexDirection::Desc, PROP, "a\0");
        assert_eq!(
            value_prefix.to_bytes().as_ref(),
            &[0x03, 0x0a, 1, 2, 3, 4, 0x9E, 0xFF, 0x00, 0xFF, 0xFE]
        );
        assert_eq!(
            GlobalEdgeRangeScanPrefix::PropertyValue(value_prefix)
                .exclusive_end_bound()
                .as_ref(),
            &[0x03, 0x0a, 1, 2, 3, 4, 0x9E, 0xFF, 0x00, 0xFF, 0xFE, 0xFF,]
        );

        let inclusive_end =
            GlobalEdgeRangeScanValuePrefix::new(RangeIndexDirection::Asc, PROP, "a")
                .inclusive_end_bound();
        let mut expected = vec![0x03, 0x09, 1, 2, 3, 4, b'a'];
        expected.extend_from_slice(&u64::MAX.to_be_bytes());
        expected.push(0);
        assert_eq!(inclusive_end.as_ref(), expected.as_slice());
    }

    #[test]
    fn edge_range_prefixes_include_endpoint_before_property() {
        let endpoint = 0x0102_0304_0506_0708u64;
        assert_eq!(
            EdgeRangeScanPrefix::Direction {
                edge_direction: RangeEdgeDirection::Out,
                range_direction: EdgeRangeIndexDirection::Asc,
            }
            .to_bytes()
            .as_ref(),
            &[0x03, 0x03, 0x00]
        );

        let mut endpoint_expected = vec![0x03, 0x03, 0x00];
        endpoint_expected.extend_from_slice(&endpoint.to_be_bytes());
        assert_eq!(
            EdgeRangeScanPrefix::Endpoint {
                edge_direction: RangeEdgeDirection::Out,
                range_direction: EdgeRangeIndexDirection::Asc,
                endpoint,
            }
            .to_bytes()
            .as_ref(),
            endpoint_expected.as_slice()
        );

        let mut property_expected = vec![0x03, 0x06, 0x00];
        property_expected.extend_from_slice(&endpoint.to_be_bytes());
        property_expected.extend_from_slice(&PROP);
        assert_eq!(
            EdgeRangeScanPrefix::Property {
                edge_direction: RangeEdgeDirection::Out,
                range_direction: EdgeRangeIndexDirection::Desc,
                endpoint,
                property_hash: PROP,
            }
            .to_bytes()
            .as_ref(),
            property_expected.as_slice()
        );

        let mut value_expected = vec![0x03, 0x03, 0x01];
        value_expected.extend_from_slice(&endpoint.to_be_bytes());
        value_expected.extend_from_slice(&PROP);
        value_expected.push(b'a');
        let value_prefix = EdgeRangeScanValuePrefix::new(
            RangeEdgeDirection::In,
            EdgeRangeIndexDirection::Asc,
            endpoint,
            PROP,
            "a",
        );
        assert_eq!(value_prefix.to_bytes().as_ref(), value_expected.as_slice());
        value_expected.push(0xFF);
        assert_eq!(
            value_prefix.exclusive_end_bound().as_ref(),
            value_expected.as_slice()
        );

        let mut desc_value_expected = vec![0x03, 0x06, 0x00];
        desc_value_expected.extend_from_slice(&endpoint.to_be_bytes());
        desc_value_expected.extend_from_slice(&PROP);
        desc_value_expected.extend_from_slice(&[0x9E, 0xFF, 0x00, 0xFF, 0xFE]);
        assert_eq!(
            EdgeRangeScanValuePrefix::new(
                RangeEdgeDirection::Out,
                EdgeRangeIndexDirection::Desc,
                endpoint,
                PROP,
                "a\0",
            )
            .to_bytes()
            .as_ref(),
            desc_value_expected.as_slice()
        );
    }
}
