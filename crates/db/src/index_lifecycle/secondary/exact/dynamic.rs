//! Runtime equality domains, read unbudgeted or admitted against a request's
//! memory budget. Both read the same way: indexed values through their lane in
//! batches, and null or values no lane can encode through the verified label
//! rows outside the lane.
use super::*;

/// One value's owners in one exact Active equality generation.
///
/// An indexed value reads its lane: a unique owner is verified against its
/// authoritative row, and a reader that still unions deployed V3 entries reads
/// them too. Null and values no lane can encode are answered by the verified
/// label rows outside the lane (see [`unindexed_label_rows`]), checking
/// `deadline` between those reads.
pub(crate) async fn lookup_active_equality_generation_admitted(
    reader: &(impl DbReadOps + Send + Sync),
    handle: &ActiveIndexHandle,
    value: &PropertyValue,
    compatibility: ReaderStorageCompatibility,
    budget: Option<&query_resources::Budget>,
    deadline: &(impl Fn() -> Result<()> + Sync),
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
        EqualityValueProjection::NonReflexive => return bitmap::Bitmap::empty(budget),
        // No lane entry holds null, and writes reject values a lane cannot
        // encode, so only label rows outside the lane can equal these.
        EqualityValueProjection::AuthoritativeNull
        | EqualityValueProjection::Unsupported(_)
        | EqualityValueProjection::Oversized { .. } => {
            let label = UnindexedLabel {
                scope: handle.scope(),
                kind: definition.element_kind(),
                label: definition.label().as_str(),
                property: definition.property().as_str(),
            };
            // Callers without a request's read budget keep one read in
            // flight.
            let candidates = unindexed_label_rows(
                reader,
                label,
                Some((handle, compatibility)),
                None,
                LabelLaneReads::Sequential,
                deadline,
            )
            .await?;
            return verified_unindexed_rows(
                reader,
                label,
                candidates,
                |stored| stored.unwrap_or(&PropertyValue::Null).eq_value(value),
                deadline,
                budget,
            )
            .await;
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

/// Owners of any of `values` in one exact Active equality generation.
///
/// Non-unique indexed values read their distinct V4 bitmap rows in
/// `multi_get`s of at most [`helix_planner::cost::RECORD_BATCH_ROWS`] keys,
/// and unique indexed values their owners in verified batches of the same
/// size. Null, non-reflexive and unencodable values, and every value of a
/// reader that still unions deployed V3 entries, take the single-value path,
/// which checks `deadline` between its reads of label rows outside the lane.
pub(crate) async fn lookup_active_equality_generations_admitted(
    reader: &(impl DbReadOps + Send + Sync),
    handle: &ActiveIndexHandle,
    values: &[PropertyValue],
    compatibility: ReaderStorageCompatibility,
    budget: Option<&query_resources::Budget>,
    deadline: &(impl Fn() -> Result<()> + Sync),
) -> Result<bitmap::Bitmap> {
    let Some(
        definition @ (ValidatedSecondaryIndexDefinition::NodeEquality { .. }
        | ValidatedSecondaryIndexDefinition::EdgeEquality { .. }),
    ) = handle.secondary_definition()
    else {
        return Err(corruption(
            "secondary equality batch serving received a non-equality Active handle",
        ));
    };
    let takes_lane = |value: &PropertyValue| {
        compatibility != ReaderStorageCompatibility::LegacyEqualityUnion
            && matches!(
                equality::prepare_equality_value(value),
                EqualityValueProjection::Indexed(_)
            )
    };
    // A list of only indexed values, the common case, is read in place.
    if values.iter().all(takes_lane) {
        return lookup_lane_values(
            reader,
            handle,
            definition,
            values,
            compatibility,
            budget,
            deadline,
        )
        .await;
    }
    let mut owners = bitmap::Bitmap::empty(budget)?;
    for value in values.iter().filter(|value| !takes_lane(value)) {
        owners = owners.union(
            lookup_active_equality_generation_admitted(
                reader,
                handle,
                value,
                compatibility,
                budget,
                deadline,
            )
            .await?,
        )?;
    }
    let _indexed_memory = budget
        .map(|budget| budget.reserve(values.len().saturating_mul(size_of::<&PropertyValue>())))
        .transpose()?;
    let indexed = values
        .iter()
        .filter(|value| takes_lane(value))
        .collect::<Vec<_>>();
    if indexed.is_empty() {
        return Ok(owners);
    }
    owners.union(
        lookup_lane_values(
            reader,
            handle,
            definition,
            &indexed,
            compatibility,
            budget,
            deadline,
        )
        .await?,
    )
}

/// Owners of indexed `values`, which their lane holds: distinct V4 bitmap
/// rows, or verified unique owners in batches.
async fn lookup_lane_values(
    reader: &(impl DbReadOps + Send + Sync),
    handle: &ActiveIndexHandle,
    definition: &ValidatedSecondaryIndexDefinition,
    values: &[impl std::borrow::Borrow<PropertyValue> + Sync],
    compatibility: ReaderStorageCompatibility,
    budget: Option<&query_resources::Budget>,
    deadline: &(impl Fn() -> Result<()> + Sync),
) -> Result<bitmap::Bitmap> {
    const BATCH: usize = helix_planner::cost::RECORD_BATCH_ROWS as usize;
    if definition_uses_equality_bitmap(definition) {
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
    for batch in values.chunks(BATCH) {
        let batch_owners = match batch {
            [value] => {
                lookup_active_equality_generation_admitted(
                    reader,
                    handle,
                    value.borrow(),
                    compatibility,
                    budget,
                    deadline,
                )
                .await?
            }
            batch => {
                Box::pin(lookup_active_unique_equality_batch_admitted(
                    reader, handle, batch, budget,
                ))
                .await?
            }
        };
        owners = owners.union(batch_owners)?;
    }
    Ok(owners)
}

#[cfg(test)]
mod tests;
