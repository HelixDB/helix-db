//! Runtime equality domains retain native null and unique-owner semantics while
//! sharing admitted physical key construction and bitmap ownership with literals.
use super::*;

pub(crate) async fn lookup_active_equality_generation_admitted(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    value: &PropertyValue,
    compatibility: ReaderStorageCompatibility,
    budget: Option<&query_resources::Budget>,
) -> Result<bitmap::Bitmap> {
    let Some(definition) = handle.secondary_definition() else {
        return Err(corruption(
            "secondary equality serving received a non-secondary Active handle",
        ));
    };
    if !matches!(
        definition,
        ValidatedSecondaryIndexDefinition::NodeEquality { .. }
            | ValidatedSecondaryIndexDefinition::EdgeEquality { .. }
    ) {
        return Err(corruption(
            "secondary equality serving received a range definition",
        ));
    }
    match equality::prepare_equality_value(value) {
        EqualityValueProjection::Indexed(_) => {}
        EqualityValueProjection::AuthoritativeNull => {
            return scan_authoritative_null_equality(reader, handle, definition, budget).await;
        }
        EqualityValueProjection::NonReflexive => return bitmap::Bitmap::empty(budget),
        EqualityValueProjection::Unsupported(value_type) => {
            return Err(SecondaryIndexValueError::UnsupportedEqualityValue { value_type }.into());
        }
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
    }
    let owners =
        lookup_active_equality_point_admitted(reader, handle, value, compatibility, budget).await?;
    if definition_lane(definition).is_unique() {
        for owner in owners.iter() {
            // A unique physical hit is not an authoritative result until its
            // current graph value and label have been checked in this snapshot.
            record_equality_graph_read();
            let matches = authoritative_equality_matches(
                reader,
                handle.scope(),
                definition,
                IndexEntityId::new(owner),
                value,
                budget,
            )
            .await?;
            if !matches {
                return Err(corruption(
                    "unique secondary equality owner differs from authoritative graph state",
                ));
            }
        }
    }
    Ok(owners)
}

/// Runtime domains fold equal physical keys. A singleton uses `get`; larger
/// domains use one `multi_get`. Literal executable batches deliberately keep
/// duplicates and always use their selected batch primitive.
pub(crate) async fn lookup_active_equality_generations_admitted(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    values: &[PropertyValue],
    compatibility: ReaderStorageCompatibility,
    budget: Option<&query_resources::Budget>,
) -> Result<bitmap::Bitmap> {
    if values.is_empty() {
        return bitmap::Bitmap::empty(budget);
    }
    let Some(definition) = handle.secondary_definition() else {
        return Err(corruption(
            "secondary equality batch serving received a non-secondary Active handle",
        ));
    };
    if definition_uses_equality_bitmap(definition)
        && compatibility == ReaderStorageCompatibility::Current
        && values.iter().all(|value| {
            matches!(
                equality::prepare_equality_value(value),
                EqualityValueProjection::Indexed(_)
            )
        })
    {
        return lookup_equality_keys_admitted(
            reader,
            handle,
            definition,
            values,
            budget,
            EqualityRead::DistinctSet,
        )
        .await;
    }
    let mut owners = bitmap::Bitmap::empty(budget)?;
    for value in values {
        owners = owners.union(
            lookup_active_equality_generation_admitted(
                reader,
                handle,
                value,
                compatibility,
                budget,
            )
            .await?,
        )?;
    }
    Ok(owners)
}

async fn scan_authoritative_null_equality(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    definition: &ValidatedSecondaryIndexDefinition,
    budget: Option<&query_resources::Budget>,
) -> Result<bitmap::Bitmap> {
    #[cfg(any(test, feature = "production-coverage"))]
    record(ReadKind::Scan);
    let _prefix_memory = budget
        .map(|budget| {
            budget.reserve(
                handle
                    .scope()
                    .encoded_len()
                    .saturating_add(2 * size_of::<u8>()),
            )
        })
        .transpose()?;
    let prefix = source_prefix(handle.scope(), definition.element_kind());
    if let Some(budget) = budget {
        budget.record_reads(query_resources::StorageReadUsage {
            scans: 1,
            ..Default::default()
        });
    }
    let mut rows = reader.scan_prefix(&prefix, ..).await?;
    let mut owners = bitmap::SortedBuilder::new(budget)?;
    let mut batch_rows = 0;
    loop {
        // Cached storage iterators can stay ready for the entire graph. Yield
        // before reading another row so cancellation retains no unadmitted data.
        if batch_rows == 512 {
            batch_rows = 0;
            tokio::task::yield_now().await;
        }
        let Some(row) = rows.next().await? else {
            break;
        };
        batch_rows += 1;
        record_equality_graph_read();
        let _properties_memory = budget
            .map(|budget| {
                budget.record_reads(query_resources::StorageReadUsage {
                    scan_rows: 1,
                    ..Default::default()
                });
                budget.reserve(
                    row.key
                        .len()
                        .saturating_add(row.value.len().saturating_mul(33)),
                )
            })
            .transpose()?;
        let Some(entity_id) = source_entity(handle.scope(), definition.element_kind(), &row.key)?
        else {
            continue;
        };
        let properties = decode_properties(&row.value)?;
        if properties_match_definition(definition, &properties)
            && properties
                .iter()
                .find(|property| property.name == definition.property().as_str())
                .is_none_or(|property| matches!(property.value, PropertyValue::Null))
        {
            owners.push(entity_id.get())?;
        }
    }
    Ok(owners.finish())
}

#[cfg(test)]
mod tests;
