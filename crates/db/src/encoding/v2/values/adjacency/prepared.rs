//! Bounded adjacency decoding. Preflight borrows encoded slices and allocates
//! no containers. The validating bitmap decoder runs only after admission.
use super::super::indexes::equality::{BitmapMembershipDelta, PreparedBitmap};
use super::*;

pub(crate) struct PreparedEdges<'a> {
    directions: Option<(PreparedBitmap<'a>, PreparedBitmap<'a>)>,
}

impl PreparedEdges<'_> {
    pub(crate) fn allocation_bound(&self) -> usize {
        self.directions
            .as_ref()
            .map_or(size_of::<Edges>(), |(out, incoming)| {
                size_of::<Edges>()
                    .saturating_add(out.allocation_bound())
                    .saturating_add(incoming.allocation_bound())
            })
    }

    pub(crate) fn decode(self) -> Result<Edges, EncodingError> {
        let Some((out, incoming)) = self.directions else {
            return Ok(Edges::new());
        };
        // A standalone delta applies to an empty adjacency row. Its additions
        // are exactly the resulting members; the decoder still validates the
        // removal set and disjointness. Moving them avoids cloning containers.
        Ok(Edges {
            nxts_out: out.decode()?.into_ids(),
            nxts_in: incoming.decode()?.into_ids(),
        })
    }
}

pub(crate) fn prepare_edges(data: &[u8]) -> Result<PreparedEdges<'_>, EncodingError> {
    if data.is_empty() {
        return Ok(PreparedEdges { directions: None });
    }
    let delta = data.starts_with(ADJACENCY_MEMBERSHIP_DELTA_MAGIC);
    let mut offset = if delta {
        ADJACENCY_MEMBERSHIP_DELTA_MAGIC.len()
    } else {
        0
    };
    let marker = take_u8(data, &mut offset)?;
    match (delta, marker) {
        (true, 0 | 1) | (false, ENCODING_TYPE_NONE) => {}
        (true, value) => {
            return Err(EncodingError::Custom(format!(
                "invalid adjacency reset flag {value}"
            )))
        }
        (false, value) => return Err(EncodingError::InvalidEncodingType(value)),
    }
    let mut direction = |name: &str| {
        let length = take_u32_le(data, &mut offset)?;
        let bytes = take_slice(data, &mut offset, length)?;
        if delta {
            BitmapMembershipDelta::prepare_if_delta(bytes)?.ok_or_else(|| {
                EncodingError::Custom(format!("adjacency {name} membership delta is malformed"))
            })
        } else {
            PreparedBitmap::portable(bytes)
        }
    };
    let outgoing = direction("outgoing")?;
    let incoming = direction("incoming")?;
    if delta && offset != data.len() {
        return Err(EncodingError::Custom(format!(
            "adjacency membership delta has {} trailing bytes",
            data.len() - offset
        )));
    }
    // Plain adjacency rows and their nested portable bitmaps historically
    // ignore suffix bytes. Delta framing is strict, as in the existing codec.
    Ok(PreparedEdges {
        directions: Some((outgoing, incoming)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_and_delta_adjacency_match_the_existing_decoder_and_membership_oracle() {
        let mut edges = Edges::new();
        for id in (0..100_000).chain([1 << 32, u64::MAX]) {
            edges.add_out(id);
            edges.add_in(id / 2);
        }
        edges.nxts_out =
            RoaringTreemap::from_bitmaps(edges.nxts_out.bitmaps().map(|(id, bitmap)| {
                let mut bitmap = bitmap.clone();
                bitmap.optimize();
                (id, bitmap)
            }));
        let mut delta = AdjacencyMembershipDelta::from_edges(&edges);
        delta.remove_out(17);
        delta.remove_in(31);
        for bytes in [
            encode_edges(&edges),
            delta.encode(),
            empty_edges_bytes(),
            Bytes::new(),
        ] {
            let prepared = prepare_edges(&bytes).unwrap();
            let bound = prepared.allocation_bound();
            let decoded = prepared.decode().unwrap();
            let ordinary = decode_edges(&bytes).unwrap();
            assert_eq!(
                decoded.iter_out().collect::<Vec<_>>(),
                ordinary.iter_out().collect::<Vec<_>>()
            );
            assert_eq!(
                decoded.iter_in().collect::<Vec<_>>(),
                ordinary.iter_in().collect::<Vec<_>>()
            );
            assert!(
                super::super::super::indexes::equality::retained_allocation_estimate(
                    &decoded.nxts_out
                ) + super::super::super::indexes::equality::retained_allocation_estimate(
                    &decoded.nxts_in
                ) <= bound
            );
        }
        assert_eq!(
            prepare_edges(&delta.encode())
                .unwrap()
                .decode()
                .unwrap()
                .iter_out()
                .collect::<Vec<_>>(),
            (0..100_000)
                .chain([1 << 32, u64::MAX])
                .filter(|id| *id != 17)
                .collect::<Vec<_>>()
        );
        delta.reset_out();
        delta.add_out(42);
        assert_eq!(
            prepare_edges(&delta.encode())
                .unwrap()
                .decode()
                .unwrap()
                .iter_out()
                .collect::<Vec<_>>(),
            vec![42]
        );
    }

    #[test]
    fn preflight_preserves_plain_suffix_leniency_and_rejects_malformed_framing() {
        let portable = super::super::super::indexes::equality::SecondaryEqualityValue::encode_ids(
            &[7].into_iter().collect(),
        );
        let mut bytes = vec![ENCODING_TYPE_NONE];
        for _ in 0..2 {
            bytes.put_u32_le((portable.len() + 1) as u32);
            bytes.extend_from_slice(&portable);
            bytes.push(255);
        }
        bytes.push(254);
        let decoded = prepare_edges(&bytes).unwrap().decode().unwrap();
        assert_eq!(decoded.iter_out().collect::<Vec<_>>(), vec![7]);
        assert_eq!(decoded.iter_in().collect::<Vec<_>>(), vec![7]);
        let delta = AdjacencyMembershipDelta::from_edges(&decoded).encode();
        for fixture in [encode_edges(&decoded), delta.clone()] {
            for end in 1..fixture.len() {
                assert!(
                    prepare_edges(&fixture[..end]).is_err(),
                    "accepted truncated adjacency at {end}"
                );
            }
        }
        let mut malformed = delta.to_vec();
        malformed.push(255);
        assert!(prepare_edges(&malformed).is_err());
        malformed = delta.to_vec();
        malformed[ADJACENCY_MEMBERSHIP_DELTA_MAGIC.len()] = 2;
        assert!(prepare_edges(&malformed).is_err());
        let outgoing_length = BitmapMembershipDelta::from_additions(decoded.nxts_out.clone())
            .encode()
            .len();
        for offset in [
            ADJACENCY_MEMBERSHIP_DELTA_MAGIC.len()
                + ADJACENCY_RESET_OUT_LEN
                + BITMAP_LEN_PREFIX_LEN,
            ADJACENCY_MEMBERSHIP_DELTA_MAGIC.len()
                + ADJACENCY_RESET_OUT_LEN
                + BITMAP_LEN_PREFIX_LEN
                + outgoing_length
                + BITMAP_LEN_PREFIX_LEN,
        ] {
            // The two direction headers must themselves be membership deltas.
            let mut malformed = delta.to_vec();
            malformed[offset] ^= 255;
            assert!(prepare_edges(&malformed).is_err());
        }
        assert!(matches!(
            prepare_edges(&[ENCODING_TYPE_EFP]),
            Err(EncodingError::InvalidEncodingType(ENCODING_TYPE_EFP))
        ));
    }
}
