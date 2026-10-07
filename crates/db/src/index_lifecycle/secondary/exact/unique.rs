//! Batched unique owners retain admission through authoritative verification.
use super::*;

/// Owners of `values` in one Active unique node equality generation.
///
/// Owner keys are read in `multi_get`s of at most
/// [`helix_planner::cost::RECORD_BATCH_ROWS`] keys, then each batch's owners
/// are verified against their authoritative rows with one more `multi_get`
/// from the same reader, so a list of any length costs two reads per batch,
/// never one per owner. With a `budget`, every key, raw row and decoded record
/// is admitted before it is built or read, and the owners are admitted as
/// they are collected.
pub(crate) async fn lookup_active_unique_equality_batch_admitted(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    values: &[impl std::borrow::Borrow<PropertyValue>],
    budget: Option<&query_resources::Budget>,
) -> Result<bitmap::Bitmap> {
    const BATCH: usize = helix_planner::cost::RECORD_BATCH_ROWS as usize;
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
    // The key vector and one batch's result vectors remain live together.
    // Each encoded key is admitted before construction.
    let mut memory = budget
        .map(|budget| {
            budget.reserve(
                values
                    .len()
                    .saturating_mul(size_of::<Bytes>())
                    .saturating_add(BATCH.saturating_mul(2 * size_of::<Option<Bytes>>())),
            )
        })
        .transpose()?;
    let mut keys = Vec::with_capacity(values.len());
    for value in values {
        let prepared = match equality::prepare_equality_value(value.borrow()) {
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
    let mut owners = bitmap::Builder::new(budget)?;
    for (keys, values) in keys.chunks(BATCH).zip(values.chunks(BATCH)) {
        #[cfg(any(test, feature = "production-coverage"))]
        record(ReadKind::MultiGet);
        if let Some(budget) = budget {
            budget.record_reads(query_resources::StorageReadUsage {
                multi_get_batches: 1,
                multi_get_keys: keys.len(),
                ..Default::default()
            });
        }
        let entries = reader.multi_get(keys).await?;
        if entries.len() != values.len() {
            return Err(corruption(
                "unique equality multi-get returned the wrong number of entries",
            ));
        }
        let _entries_memory = budget
            .map(|budget| {
                budget.reserve(
                    entries
                        .iter()
                        .flatten()
                        .fold(0_usize, |bytes, entry| bytes.saturating_add(entry.len())),
                )
            })
            .transpose()?;
        let found = entries
            .into_iter()
            .zip(values)
            .filter_map(|(entry, value)| entry.map(|bytes| (bytes, value.borrow())))
            .map(|(bytes, value)| {
                decode_secondary_entry_value(
                    handle.index_id(),
                    handle.generation(),
                    definition_lane(definition),
                    &bytes,
                )
                .map(|owner| (owner, value))
            })
            .collect::<Result<Vec<_>>>()?;
        if found.is_empty() {
            continue;
        }
        found.iter().for_each(|_| record_equality_graph_read());
        let _record_keys_memory =
            budget
                .map(|budget| {
                    budget.reserve(found.len().saturating_mul(
                        handle.scope().encoded_len().saturating_add(
                            size_of::<u8>() + size_of::<u64>() + size_of::<Bytes>(),
                        ),
                    ))
                })
                .transpose()?;
        let records = found
            .iter()
            .map(|(owner, _)| {
                authoritative_property_key(
                    handle.scope(),
                    IndexEntity {
                        kind: definition.element_kind(),
                        id: *owner,
                    },
                )
            })
            .collect::<Vec<_>>();
        #[cfg(any(test, feature = "production-coverage"))]
        record(ReadKind::MultiGet);
        if let Some(budget) = budget {
            budget.record_reads(query_resources::StorageReadUsage {
                multi_get_batches: 1,
                multi_get_keys: records.len(),
                ..Default::default()
            });
        }
        let records = reader.multi_get(&records).await?;
        if records.len() != found.len() {
            return Err(corruption(
                "unique equality verification multi-get returned the wrong number of records",
            ));
        }
        // Raw records stay admitted while each is decoded, at the common
        // hydration decoder bound.
        let _records_memory = budget
            .map(|budget| {
                budget.reserve(records.iter().flatten().fold(0_usize, |bytes, record| {
                    bytes.saturating_add(record.len().saturating_mul(33))
                }))
            })
            .transpose()?;
        let mut scratch = view::Scratch::new();
        for ((owner, value), record) in found.into_iter().zip(records) {
            let matches = record
                .map(|bytes| {
                    view::decode_selected(&bytes, &mut scratch, |name| {
                        name == "$label" || name == definition.property().as_str()
                    })
                })
                .transpose()?
                .is_some_and(|properties| {
                    properties_match_definition(definition, &properties)
                        && properties
                            .iter()
                            .find(|property| property.name == definition.property().as_str())
                            .is_some_and(|property| property.value.eq_value(value))
                });
            if !matches {
                return Err(corruption(
                    "unique equality owner disagrees with its authoritative node",
                ));
            }
            owners.insert(owner.get())?;
        }
    }
    Ok(owners.finish())
}
