//! Batched unique owners retain admission through authoritative verification.
use super::*;

#[cfg(any(test, feature = "production-coverage"))]
pub(crate) async fn lookup_active_unique_equality_batch(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    values: &[PropertyValue],
) -> Result<roaring::RoaringTreemap> {
    lookup_active_unique_equality_batch_admitted(reader, handle, values, None)
        .await
        .map(bitmap::Bitmap::into_unbudgeted)
}

pub(crate) async fn lookup_active_unique_equality_batch_admitted(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    values: &[PropertyValue],
    budget: Option<&query_resources::Budget>,
) -> Result<bitmap::Bitmap> {
    let Some(definition @ ValidatedSecondaryIndexDefinition::NodeEquality { unique: true, .. }) =
        handle.secondary_definition()
    else {
        return Err(corruption(
            "unique equality batch requires an Active unique node index",
        ));
    };
    if values.len() < 2 {
        return Err(corruption(
            "unique equality batch requires at least two values",
        ));
    }
    // Both vectors remain live during the batch. Each encoded key is admitted
    // before construction; all raw values remain admitted until verification ends.
    let mut memory = budget
        .map(|budget| {
            budget.reserve(
                values
                    .len()
                    .saturating_mul(size_of::<Bytes>() + size_of::<Option<Bytes>>()),
            )
        })
        .transpose()?;
    let mut keys = Vec::with_capacity(values.len());
    for value in values {
        let prepared = match equality::prepare_equality_value(value) {
            EqualityValueProjection::Indexed(value) => value,
            EqualityValueProjection::Oversized {
                encoded_len,
                maximum,
            } => {
                return Err(SecondaryIndexValueError::EncodedKeyTooLarge {
                    encoded_len,
                    maximum,
                }
                .into());
            }
            EqualityValueProjection::AuthoritativeNull
            | EqualityValueProjection::NonReflexive
            | EqualityValueProjection::Unsupported(_) => {
                return Err(corruption(
                    "unique equality batch requires indexed literals",
                ));
            }
        };
        let _canonical = budget
            .map(|budget| {
                budget.reserve(
                    prepared
                        .encoded_len()
                        .saturating_add(2 * size_of::<equality::CanonicalEqualityValue>()),
                )
            })
            .transpose()?;
        let key = prepare_secondary_entry_key(
            handle.scope(),
            handle.index_id(),
            handle.generation(),
            definition,
            CanonicalSecondaryValue::equality(prepared.encode()),
            IndexEntityId::initial(),
        )
        .expect("validated unique equality key");
        if let (Some(budget), Some(memory)) = (budget, memory.as_mut()) {
            memory.absorb(budget.reserve(key.encoded_len())?);
        }
        keys.push(key.to_bytes());
    }
    keys.iter().for_each(|_| record_equality_point_read());
    #[cfg(any(test, feature = "production-coverage"))]
    record(ReadKind::MultiGet);
    if let Some(budget) = budget {
        budget.record_reads(query_resources::StorageReadUsage {
            multi_get_batches: 1,
            multi_get_keys: keys.len(),
            ..Default::default()
        });
    }
    let entries = reader.multi_get(&keys).await?;
    if entries.len() != values.len() {
        return Err(corruption(
            "unique equality multi-get returned the wrong number of entries",
        ));
    }
    if let (Some(budget), Some(memory)) = (budget, memory.as_mut()) {
        let bytes = entries
            .iter()
            .flatten()
            .fold(0_usize, |bytes, value| bytes.saturating_add(value.len()));
        memory.absorb(budget.reserve(bytes)?);
    }
    let mut owners = bitmap::Builder::new(budget)?;
    for (entry, value) in entries.into_iter().zip(values) {
        let Some(bytes) = entry else {
            continue;
        };
        let owner = decode_secondary_entry_value(
            handle.index_id(),
            handle.generation(),
            definition_lane(definition),
            &bytes,
        )?;
        record_equality_graph_read();
        if !authoritative_equality_matches(reader, handle.scope(), definition, owner, value, budget)
            .await?
        {
            return Err(corruption(
                "unique equality owner disagrees with its authoritative node",
            ));
        }
        owners.insert(owner.get())?;
    }
    Ok(owners.finish())
}
