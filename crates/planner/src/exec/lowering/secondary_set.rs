//! Pure secondary-index access lowering.
//!
//! Eligible logical access trees become one executable ID-set operation. Mixed
//! trees return `None` and retain the legacy row-stream lowering path.

use crate::{exec, ir};

pub(crate) fn node_secondary_set(
    plan: &ir::NodeAccessPlan,
) -> Option<exec::ExecNodeSecondarySetPlan> {
    match plan {
        ir::NodeAccessPlan::Empty => Some(exec::ExecNodeSecondarySetPlan::Empty),
        ir::NodeAccessPlan::EqualityIndex { index, key, value } => {
            match exec::exact_node_equality(index.clone(), key.clone(), value.clone()) {
                exec::ExecNodeEqualityAccessPlan::Empty => {
                    Some(exec::ExecNodeSecondarySetPlan::Empty)
                }
                exec::ExecNodeEqualityAccessPlan::Bitmap(bitmap) => {
                    Some(exec::ExecNodeSecondarySetPlan::Bitmap(bitmap))
                }
                exec::ExecNodeEqualityAccessPlan::Unique {
                    lookup,
                    verification,
                } => Some(exec::ExecNodeSecondarySetPlan::Unique {
                    lookup,
                    verification,
                }),
                exec::ExecNodeEqualityAccessPlan::AuthoritativeScan(predicate) => {
                    Some(exec::ExecNodeSecondarySetPlan::AuthoritativeScan(predicate))
                }
                exec::ExecNodeEqualityAccessPlan::DynamicEquality { index, key, param } => {
                    Some(exec::ExecNodeSecondarySetPlan::DynamicEquality { index, key, param })
                }
            }
        }
        ir::NodeAccessPlan::RangeIndex { index, key, range } => Some(
            exec::ExecNodeSecondarySetPlan::Range(exec::ExecNodeSecondaryRangePlan {
                index: index.clone(),
                key: key.clone(),
                range: range.clone(),
            }),
        ),
        ir::NodeAccessPlan::Union(children) => {
            let children = children
                .iter()
                .map(|child| node_secondary_set(child.as_ref()))
                .collect::<Option<Vec<_>>>()?;
            node_union(children)
        }
        ir::NodeAccessPlan::Intersect(children) => {
            let children = children
                .iter()
                .map(|child| node_secondary_set(child.as_ref()))
                .collect::<Option<Vec<_>>>()?;
            node_intersection(children)
        }
        ir::NodeAccessPlan::PointIds { .. }
        | ir::NodeAccessPlan::FromParam { .. }
        | ir::NodeAccessPlan::FromVar { .. }
        | ir::NodeAccessPlan::AllScan
        | ir::NodeAccessPlan::LabelScan { .. }
        | ir::NodeAccessPlan::VectorSearch { .. }
        | ir::NodeAccessPlan::TextSearch { .. }
        | ir::NodeAccessPlan::ScanThenFilter { .. } => None,
    }
}

pub(crate) fn edge_secondary_set(
    plan: &ir::EdgeAccessPlan,
) -> Option<exec::ExecEdgeSecondarySetPlan> {
    match plan {
        ir::EdgeAccessPlan::Empty => Some(exec::ExecEdgeSecondarySetPlan::Empty),
        ir::EdgeAccessPlan::EqualityIndex { index, key, value } => {
            match exec::exact_edge_equality(index.clone(), key.clone(), value.clone()) {
                exec::ExecEdgeEqualityAccessPlan::Empty => {
                    Some(exec::ExecEdgeSecondarySetPlan::Empty)
                }
                exec::ExecEdgeEqualityAccessPlan::Bitmap(bitmap) => {
                    Some(exec::ExecEdgeSecondarySetPlan::Bitmap(bitmap))
                }
                exec::ExecEdgeEqualityAccessPlan::AuthoritativeScan(predicate) => {
                    Some(exec::ExecEdgeSecondarySetPlan::AuthoritativeScan(predicate))
                }
                exec::ExecEdgeEqualityAccessPlan::DynamicEquality { index, key, param } => {
                    Some(exec::ExecEdgeSecondarySetPlan::DynamicEquality { index, key, param })
                }
            }
        }
        ir::EdgeAccessPlan::RangeIndex { index, key, range } => Some(
            exec::ExecEdgeSecondarySetPlan::Range(exec::ExecEdgeSecondaryRangePlan {
                index: index.clone(),
                key: key.clone(),
                range: range.clone(),
            }),
        ),
        ir::EdgeAccessPlan::Union(children) => {
            let children = children
                .iter()
                .map(|child| edge_secondary_set(child.as_ref()))
                .collect::<Option<Vec<_>>>()?;
            edge_union(children)
        }
        ir::EdgeAccessPlan::Intersect(children) => {
            let children = children
                .iter()
                .map(|child| edge_secondary_set(child.as_ref()))
                .collect::<Option<Vec<_>>>()?;
            edge_intersection(children)
        }
        ir::EdgeAccessPlan::PointIds { .. }
        | ir::EdgeAccessPlan::FromParam { .. }
        | ir::EdgeAccessPlan::FromVar { .. }
        | ir::EdgeAccessPlan::AllScan
        | ir::EdgeAccessPlan::LabelScan { .. }
        | ir::EdgeAccessPlan::VectorSearch { .. }
        | ir::EdgeAccessPlan::TextSearch { .. }
        | ir::EdgeAccessPlan::ScanThenFilter { .. } => None,
    }
}

fn node_union(
    children: Vec<exec::ExecNodeSecondarySetPlan>,
) -> Option<exec::ExecNodeSecondarySetPlan> {
    let mut flattened = children
        .into_iter()
        .flat_map(|child| match child {
            exec::ExecNodeSecondarySetPlan::Union { driver, rest } => {
                core::iter::once(*driver).chain(rest).collect()
            }
            child => vec![child],
        })
        .filter(|child| !matches!(child, exec::ExecNodeSecondarySetPlan::Empty))
        .collect::<Vec<_>>();
    match flattened.len() {
        0 => Some(exec::ExecNodeSecondarySetPlan::Empty),
        1 => flattened.pop(),
        _ => {
            let batch = flattened
                .iter()
                .map(|child| match child {
                    exec::ExecNodeSecondarySetPlan::Bitmap(
                        exec::ExecNodeBitmapExpr::PointRead { index, key, value },
                    ) => Some((index.clone(), key.clone(), value.clone())),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>();
            match batch {
                Some(batch)
                    if batch
                        .iter()
                        .all(|(index, key, _)| index == &batch[0].0 && key == &batch[0].1) =>
                {
                    let (index, key, _) = batch[0].clone();
                    let values = batch.into_iter().map(|(_, _, value)| value).collect();
                    return Some(exec::ExecNodeSecondarySetPlan::Bitmap(
                        exec::ExecNodeBitmapExpr::BatchedUnionRead {
                            index,
                            key,
                            values: ir::AtLeast::try_from_vec(values)
                                .expect("batched equality union has at least two values"),
                        },
                    ));
                }
                Some(_) | None => {}
            }
            let driver = flattened.remove(0);
            Some(exec::ExecNodeSecondarySetPlan::Union {
                driver: Box::new(driver),
                rest: ir::AtLeast::try_from_vec(flattened)
                    .expect("multi-child node union leaves at least one child"),
            })
        }
    }
}

fn edge_union(
    children: Vec<exec::ExecEdgeSecondarySetPlan>,
) -> Option<exec::ExecEdgeSecondarySetPlan> {
    let mut flattened = children
        .into_iter()
        .flat_map(|child| match child {
            exec::ExecEdgeSecondarySetPlan::Union { driver, rest } => {
                core::iter::once(*driver).chain(rest).collect()
            }
            child => vec![child],
        })
        .filter(|child| !matches!(child, exec::ExecEdgeSecondarySetPlan::Empty))
        .collect::<Vec<_>>();
    match flattened.len() {
        0 => Some(exec::ExecEdgeSecondarySetPlan::Empty),
        1 => flattened.pop(),
        _ => {
            let batch = flattened
                .iter()
                .map(|child| match child {
                    exec::ExecEdgeSecondarySetPlan::Bitmap(
                        exec::ExecEdgeBitmapExpr::PointRead { index, key, value },
                    ) => Some((index.clone(), key.clone(), value.clone())),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>();
            match batch {
                Some(batch)
                    if batch
                        .iter()
                        .all(|(index, key, _)| index == &batch[0].0 && key == &batch[0].1) =>
                {
                    let (index, key, _) = batch[0].clone();
                    let values = batch.into_iter().map(|(_, _, value)| value).collect();
                    return Some(exec::ExecEdgeSecondarySetPlan::Bitmap(
                        exec::ExecEdgeBitmapExpr::BatchedUnionRead {
                            index,
                            key,
                            values: ir::AtLeast::try_from_vec(values)
                                .expect("batched equality union has at least two values"),
                        },
                    ));
                }
                Some(_) | None => {}
            }
            let driver = flattened.remove(0);
            Some(exec::ExecEdgeSecondarySetPlan::Union {
                driver: Box::new(driver),
                rest: ir::AtLeast::try_from_vec(flattened)
                    .expect("multi-child edge union leaves at least one child"),
            })
        }
    }
}

fn node_intersection(
    mut children: Vec<exec::ExecNodeSecondarySetPlan>,
) -> Option<exec::ExecNodeSecondarySetPlan> {
    if children
        .iter()
        .any(|child| matches!(child, exec::ExecNodeSecondarySetPlan::Empty))
    {
        return Some(exec::ExecNodeSecondarySetPlan::Empty);
    }
    let driver = children
        .iter()
        .enumerate()
        .find_map(|(position, child)| match child {
            exec::ExecNodeSecondarySetPlan::Range(driver) => Some((position, driver.clone())),
            _ => None,
        });
    match driver {
        Some((position, driver)) => {
            children.remove(position);
            Some(exec::ExecNodeSecondarySetPlan::OrderedIntersect {
                driver,
                filters: ir::AtLeast::try_from_vec(children)
                    .expect("logical intersection leaves at least one filter"),
            })
        }
        None => {
            let driver = children.remove(0);
            Some(exec::ExecNodeSecondarySetPlan::Intersect {
                driver: Box::new(driver),
                rest: ir::AtLeast::try_from_vec(children)
                    .expect("logical node intersection leaves at least one child"),
            })
        }
    }
}

fn edge_intersection(
    mut children: Vec<exec::ExecEdgeSecondarySetPlan>,
) -> Option<exec::ExecEdgeSecondarySetPlan> {
    if children
        .iter()
        .any(|child| matches!(child, exec::ExecEdgeSecondarySetPlan::Empty))
    {
        return Some(exec::ExecEdgeSecondarySetPlan::Empty);
    }
    let driver = children
        .iter()
        .enumerate()
        .find_map(|(position, child)| match child {
            exec::ExecEdgeSecondarySetPlan::Range(driver) => Some((position, driver.clone())),
            _ => None,
        });
    match driver {
        Some((position, driver)) => {
            children.remove(position);
            Some(exec::ExecEdgeSecondarySetPlan::OrderedIntersect {
                driver,
                filters: ir::AtLeast::try_from_vec(children)
                    .expect("logical intersection leaves at least one filter"),
            })
        }
        None => {
            let driver = children.remove(0);
            Some(exec::ExecEdgeSecondarySetPlan::Intersect {
                driver: Box::new(driver),
                rest: ir::AtLeast::try_from_vec(children)
                    .expect("logical edge intersection leaves at least one child"),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use helix_ast::index::RangeIndexDirection;
    use helix_ast::value::PropertyValue;

    use super::*;

    fn equality_value(value: PropertyValue) -> ir::IndexValue {
        ir::IndexValue::Literal(
            ir::SecondaryIndexLiteral::new(value).expect("test value is index compatible"),
        )
    }

    fn node_equality(property: &str, value: PropertyValue) -> ir::NodeAccessPlan {
        ir::NodeAccessPlan::EqualityIndex {
            index: crate::catalog::NodeEqualityIndexMeta::try_new(format!("user_{property}"))
                .expect("test index ID is non-empty"),
            key: crate::catalog::ScopedPropertyKey::try_new("User", property)
                .expect("test key is valid"),
            value: equality_value(value),
        }
    }

    fn node_source(plan: ir::NodeAccessPlan) -> ir::NodeAccessSourcePlan {
        ir::NodeAccessSourcePlan::new(plan).expect("test source is valid")
    }

    fn edge_equality(property: &str, value: PropertyValue) -> ir::EdgeAccessPlan {
        ir::EdgeAccessPlan::EqualityIndex {
            index: crate::catalog::EdgeEqualityIndexMeta::try_new(format!("likes_{property}"))
                .expect("test index ID is non-empty"),
            key: crate::catalog::ScopedPropertyKey::try_new("LIKES", property)
                .expect("test key is valid"),
            value: equality_value(value),
        }
    }

    fn edge_source(plan: ir::EdgeAccessPlan) -> ir::EdgeAccessSourcePlan {
        ir::EdgeAccessSourcePlan::new(plan).expect("test source is valid")
    }

    fn node_range(property: &str) -> ir::NodeAccessPlan {
        ir::NodeAccessPlan::RangeIndex {
            index: crate::catalog::NodeRangeIndexMeta::try_new(format!("user_{property}"))
                .expect("test index ID is non-empty"),
            key: crate::catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                property,
                RangeIndexDirection::Asc,
            )
            .expect("test range key is valid"),
            range: ir::IndexRange::All,
        }
    }

    fn edge_range(property: &str) -> ir::EdgeAccessPlan {
        ir::EdgeAccessPlan::RangeIndex {
            index: crate::catalog::EdgeRangeIndexMeta::try_new(format!("follows_{property}"))
                .expect("test index ID is non-empty"),
            key: crate::catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                property,
                RangeIndexDirection::Desc,
            )
            .expect("test range key is valid"),
            range: ir::IndexRange::All,
        }
    }

    #[test]
    fn same_index_union_batches_values_into_one_equality_leaf() {
        let plan = ir::NodeAccessPlan::Union(ir::AtLeast::from_pair(
            node_source(node_equality("name", PropertyValue::from("alice"))),
            node_source(node_equality("name", PropertyValue::from("bob"))),
        ));

        assert!(matches!(
            node_secondary_set(&plan),
            Some(exec::ExecNodeSecondarySetPlan::Bitmap(
                exec::ExecNodeBitmapExpr::BatchedUnionRead { key, values, .. }
            ))
                if key.property == "name" && values.len() == 2
        ));
    }

    #[test]
    fn mixed_union_retains_legacy_row_stream_lowering() {
        let ids = ir::ElementIds::new(ir::AtLeast::from_one(7)).expect("test ID is valid");
        let plan = ir::NodeAccessPlan::Union(ir::AtLeast::from_pair(
            node_source(node_equality("name", PropertyValue::from("alice"))),
            node_source(ir::NodeAccessPlan::PointIds { ids: ids.clone() }),
        ));

        assert_eq!(node_secondary_set(&plan), None);

        let node_intersection = ir::NodeAccessPlan::Intersect(ir::AtLeast::from_pair(
            node_source(node_equality("name", PropertyValue::from("alice"))),
            node_source(ir::NodeAccessPlan::PointIds { ids: ids.clone() }),
        ));
        assert_eq!(node_secondary_set(&node_intersection), None);

        let edge_union = ir::EdgeAccessPlan::Union(ir::AtLeast::from_pair(
            edge_source(edge_equality("status", PropertyValue::from("active"))),
            edge_source(ir::EdgeAccessPlan::PointIds { ids }),
        ));
        assert_eq!(edge_secondary_set(&edge_union), None);
    }

    #[test]
    fn range_intersection_becomes_an_ordered_driver_with_filters() {
        let plan = ir::NodeAccessPlan::Intersect(ir::AtLeast::from_pair(
            node_source(node_equality("status", PropertyValue::from("active"))),
            node_source(node_range("age")),
        ));

        assert!(matches!(
            node_secondary_set(&plan),
            Some(exec::ExecNodeSecondarySetPlan::OrderedIntersect { driver, filters })
                if driver.key.property == "age" && filters.len() == 1
        ));
    }

    #[test]
    fn ordered_intersection_lowering_uses_the_first_direct_range_for_nodes_and_edges() {
        let node = ir::NodeAccessPlan::Intersect(ir::AtLeast::from_pair(
            node_source(node_range("score")),
            node_source(node_range("age")),
        ));
        let edge = ir::EdgeAccessPlan::Intersect(ir::AtLeast::from_pair(
            edge_source(edge_range("score")),
            edge_source(edge_range("age")),
        ));

        assert!(matches!(
            node_secondary_set(&node),
            Some(exec::ExecNodeSecondarySetPlan::OrderedIntersect { driver, filters })
                if driver.key.property == "score"
                    && matches!(&filters[0], exec::ExecNodeSecondarySetPlan::Range(range)
                        if range.key.property == "age")
        ));
        assert!(matches!(
            edge_secondary_set(&edge),
            Some(exec::ExecEdgeSecondarySetPlan::OrderedIntersect { driver, filters })
                if driver.key.property == "score"
                    && matches!(&filters[0], exec::ExecEdgeSecondarySetPlan::Range(range)
                        if range.key.property == "age")
        ));
    }

    #[test]
    fn ordered_intersection_lowering_never_promotes_a_nested_range() {
        let nested = ir::NodeAccessPlan::Intersect(ir::AtLeast::from_pair(
            node_source(node_range("score")),
            node_source(node_equality("status", PropertyValue::from("active"))),
        ));
        let outer = ir::NodeAccessPlan::Intersect(ir::AtLeast::from_pair(
            node_source(nested),
            node_source(node_range("age")),
        ));

        assert!(matches!(
            node_secondary_set(&outer),
            Some(exec::ExecNodeSecondarySetPlan::OrderedIntersect { driver, filters })
                if driver.key.property == "age"
                    && matches!(&filters[0], exec::ExecNodeSecondarySetPlan::OrderedIntersect {
                        driver: nested_driver,
                        ..
                    } if nested_driver.key.property == "score")
        ));
    }

    #[test]
    fn nan_equality_lowers_to_a_static_empty_set() {
        assert_eq!(
            node_secondary_set(&node_equality("score", PropertyValue::F64(f64::NAN))),
            Some(exec::ExecNodeSecondarySetPlan::Empty)
        );
    }

    #[test]
    fn equality_leaf_matrix_preserves_every_exact_node_and_edge_primitive() {
        let unique = ir::NodeAccessPlan::EqualityIndex {
            index: crate::catalog::NodeEqualityIndexMeta::try_new("user_email")
                .unwrap()
                .with_uniqueness(crate::catalog::IndexUniqueness::Unique),
            key: crate::catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
            value: equality_value(PropertyValue::from("alice@example.test")),
        };
        assert!(matches!(
            node_secondary_set(&unique),
            Some(exec::ExecNodeSecondarySetPlan::Unique { .. })
        ));
        assert!(matches!(
            node_secondary_set(&node_equality("email", PropertyValue::Null)),
            Some(exec::ExecNodeSecondarySetPlan::AuthoritativeScan(_))
        ));
        let node_dynamic = ir::NodeAccessPlan::EqualityIndex {
            index: crate::catalog::NodeEqualityIndexMeta::try_new("user_email").unwrap(),
            key: crate::catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
            value: ir::IndexValue::Param(ir::NonEmptyString::new("email").unwrap()),
        };
        assert!(matches!(
            node_secondary_set(&node_dynamic),
            Some(exec::ExecNodeSecondarySetPlan::DynamicEquality { .. })
        ));

        assert!(matches!(
            edge_secondary_set(&edge_equality("status", PropertyValue::Null)),
            Some(exec::ExecEdgeSecondarySetPlan::AuthoritativeScan(_))
        ));
        assert_eq!(
            edge_secondary_set(&edge_equality("score", PropertyValue::F32(f32::NAN))),
            Some(exec::ExecEdgeSecondarySetPlan::Empty)
        );
        let edge_dynamic = ir::EdgeAccessPlan::EqualityIndex {
            index: crate::catalog::EdgeEqualityIndexMeta::try_new("likes_status").unwrap(),
            key: crate::catalog::ScopedPropertyKey::try_new("LIKES", "status").unwrap(),
            value: ir::IndexValue::Param(ir::NonEmptyString::new("status").unwrap()),
        };
        assert!(matches!(
            edge_secondary_set(&edge_dynamic),
            Some(exec::ExecEdgeSecondarySetPlan::DynamicEquality { .. })
        ));
    }

    #[test]
    fn empty_and_nested_unions_flatten_without_reordering_live_children() {
        let live = node_source(node_equality("name", PropertyValue::from("alice")));
        let nested = node_source(ir::NodeAccessPlan::Union(ir::AtLeast::from_pair(
            node_source(ir::NodeAccessPlan::Empty),
            live.clone(),
        )));
        let plan = ir::NodeAccessPlan::Union(ir::AtLeast::from_pair(
            node_source(ir::NodeAccessPlan::Empty),
            nested,
        ));
        assert!(matches!(
            node_secondary_set(&plan),
            Some(exec::ExecNodeSecondarySetPlan::Bitmap(
                exec::ExecNodeBitmapExpr::PointRead { .. }
            ))
        ));

        let empty = ir::NodeAccessPlan::Union(ir::AtLeast::from_pair(
            node_source(ir::NodeAccessPlan::Empty),
            node_source(ir::NodeAccessPlan::Empty),
        ));
        assert_eq!(
            node_secondary_set(&empty),
            Some(exec::ExecNodeSecondarySetPlan::Empty)
        );
    }

    #[test]
    fn node_intersections_cover_empty_bitmap_and_range_driver_shapes() {
        let live = node_source(node_equality("name", PropertyValue::from("alice")));
        let empty = ir::NodeAccessPlan::Intersect(ir::AtLeast::from_pair(
            live.clone(),
            node_source(ir::NodeAccessPlan::Empty),
        ));
        assert_eq!(
            node_secondary_set(&empty),
            Some(exec::ExecNodeSecondarySetPlan::Empty)
        );

        let bitmap = ir::NodeAccessPlan::Intersect(ir::AtLeast::from_pair(
            live.clone(),
            node_source(node_equality("status", PropertyValue::from("active"))),
        ));
        assert!(matches!(
            node_secondary_set(&bitmap),
            Some(exec::ExecNodeSecondarySetPlan::Intersect { .. })
        ));

        let range = ir::NodeAccessPlan::RangeIndex {
            index: crate::catalog::NodeRangeIndexMeta::try_new("user_age").unwrap(),
            key: crate::catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "age",
                RangeIndexDirection::Asc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        };
        let ordered =
            ir::NodeAccessPlan::Intersect(ir::AtLeast::from_pair(node_source(range), live));
        assert!(matches!(
            node_secondary_set(&ordered),
            Some(exec::ExecNodeSecondarySetPlan::OrderedIntersect { .. })
        ));
    }

    #[test]
    fn edge_secondary_sets_cover_batch_flatten_empty_intersection_and_range_driver() {
        let active = edge_source(edge_equality("status", PropertyValue::from("active")));
        let batch = ir::EdgeAccessPlan::Union(ir::AtLeast::from_pair(
            active.clone(),
            edge_source(edge_equality("status", PropertyValue::from("inactive"))),
        ));
        assert!(matches!(
            edge_secondary_set(&batch),
            Some(exec::ExecEdgeSecondarySetPlan::Bitmap(
                exec::ExecEdgeBitmapExpr::BatchedUnionRead { .. }
            ))
        ));

        let nested = edge_source(ir::EdgeAccessPlan::Union(ir::AtLeast::from_pair(
            edge_source(ir::EdgeAccessPlan::Empty),
            active.clone(),
        )));
        let flattened = ir::EdgeAccessPlan::Union(ir::AtLeast::from_pair(
            edge_source(ir::EdgeAccessPlan::Empty),
            nested,
        ));
        assert!(matches!(
            edge_secondary_set(&flattened),
            Some(exec::ExecEdgeSecondarySetPlan::Bitmap(
                exec::ExecEdgeBitmapExpr::PointRead { .. }
            ))
        ));

        let empty = ir::EdgeAccessPlan::Intersect(ir::AtLeast::from_pair(
            active.clone(),
            edge_source(ir::EdgeAccessPlan::Empty),
        ));
        assert_eq!(
            edge_secondary_set(&empty),
            Some(exec::ExecEdgeSecondarySetPlan::Empty)
        );

        let bitmap = ir::EdgeAccessPlan::Intersect(ir::AtLeast::from_pair(
            active.clone(),
            edge_source(edge_equality("kind", PropertyValue::from("primary"))),
        ));
        assert!(matches!(
            edge_secondary_set(&bitmap),
            Some(exec::ExecEdgeSecondarySetPlan::Intersect { .. })
        ));

        let range = ir::EdgeAccessPlan::RangeIndex {
            index: crate::catalog::EdgeRangeIndexMeta::try_new("likes_weight").unwrap(),
            key: crate::catalog::ScopedPropertyDirectionKey::try_new(
                "LIKES",
                "weight",
                RangeIndexDirection::Desc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        };
        let ordered =
            ir::EdgeAccessPlan::Intersect(ir::AtLeast::from_pair(edge_source(range), active));
        assert!(matches!(
            edge_secondary_set(&ordered),
            Some(exec::ExecEdgeSecondarySetPlan::OrderedIntersect { .. })
        ));
    }

    #[test]
    fn unions_flatten_explicit_sets_and_decline_batching_for_mixed_primitives() {
        let node_nested = node_source(ir::NodeAccessPlan::Union(ir::AtLeast::from_pair(
            node_source(node_equality("name", PropertyValue::from("alice"))),
            node_source(node_equality("status", PropertyValue::from("active"))),
        )));
        let node_plan = ir::NodeAccessPlan::Union(ir::AtLeast::from_pair(
            node_nested,
            node_source(node_equality("role", PropertyValue::from("admin"))),
        ));
        assert!(matches!(
            node_secondary_set(&node_plan),
            Some(exec::ExecNodeSecondarySetPlan::Union { rest, .. }) if rest.len() == 2
        ));

        let node_range = ir::NodeAccessPlan::RangeIndex {
            index: crate::catalog::NodeRangeIndexMeta::try_new("user_age").unwrap(),
            key: crate::catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "age",
                RangeIndexDirection::Asc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        };
        let mixed_node = ir::NodeAccessPlan::Union(ir::AtLeast::from_pair(
            node_source(node_equality("name", PropertyValue::from("alice"))),
            node_source(node_range.clone()),
        ));
        assert!(matches!(
            node_secondary_set(&mixed_node),
            Some(exec::ExecNodeSecondarySetPlan::Union { .. })
        ));
        assert!(matches!(
            node_secondary_set(&node_range),
            Some(exec::ExecNodeSecondarySetPlan::Range(_))
        ));

        let edge_nested = edge_source(ir::EdgeAccessPlan::Union(ir::AtLeast::from_pair(
            edge_source(edge_equality("status", PropertyValue::from("active"))),
            edge_source(edge_equality("kind", PropertyValue::from("primary"))),
        )));
        let edge_plan = ir::EdgeAccessPlan::Union(ir::AtLeast::from_pair(
            edge_nested,
            edge_source(edge_equality("source", PropertyValue::from("api"))),
        ));
        assert!(matches!(
            edge_secondary_set(&edge_plan),
            Some(exec::ExecEdgeSecondarySetPlan::Union { rest, .. }) if rest.len() == 2
        ));
        let edge_range = ir::EdgeAccessPlan::RangeIndex {
            index: crate::catalog::EdgeRangeIndexMeta::try_new("likes_weight").unwrap(),
            key: crate::catalog::ScopedPropertyDirectionKey::try_new(
                "LIKES",
                "weight",
                RangeIndexDirection::Asc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        };
        let mixed_edge = ir::EdgeAccessPlan::Union(ir::AtLeast::from_pair(
            edge_source(edge_equality("status", PropertyValue::from("active"))),
            edge_source(edge_range.clone()),
        ));
        assert!(matches!(
            edge_secondary_set(&mixed_edge),
            Some(exec::ExecEdgeSecondarySetPlan::Union { .. })
        ));
        assert!(matches!(
            edge_secondary_set(&edge_range),
            Some(exec::ExecEdgeSecondarySetPlan::Range(_))
        ));
        let all_empty = ir::EdgeAccessPlan::Union(ir::AtLeast::from_pair(
            edge_source(ir::EdgeAccessPlan::Empty),
            edge_source(ir::EdgeAccessPlan::Empty),
        ));
        assert_eq!(
            edge_secondary_set(&all_empty),
            Some(exec::ExecEdgeSecondarySetPlan::Empty)
        );
    }

    #[test]
    fn every_non_secondary_source_declines_set_lowering() {
        let ids = ir::ElementIds::new(ir::AtLeast::from_one(1)).unwrap();
        for source in [
            ir::NodeAccessPlan::PointIds { ids: ids.clone() },
            ir::NodeAccessPlan::FromParam {
                param: ir::NonEmptyString::new("nodes").unwrap(),
            },
            ir::NodeAccessPlan::FromVar {
                variable: ir::NonEmptyString::new("nodes").unwrap(),
            },
            ir::NodeAccessPlan::AllScan,
            ir::NodeAccessPlan::LabelScan {
                label: ir::NonEmptyString::new("User").unwrap(),
            },
        ] {
            assert_eq!(node_secondary_set(&source), None);
        }
        for source in [
            ir::EdgeAccessPlan::PointIds { ids },
            ir::EdgeAccessPlan::FromParam {
                param: ir::NonEmptyString::new("edges").unwrap(),
            },
            ir::EdgeAccessPlan::FromVar {
                variable: ir::NonEmptyString::new("edges").unwrap(),
            },
            ir::EdgeAccessPlan::AllScan,
            ir::EdgeAccessPlan::LabelScan {
                label: ir::NonEmptyString::new("LIKES").unwrap(),
            },
        ] {
            assert_eq!(edge_secondary_set(&source), None);
        }
    }
}
