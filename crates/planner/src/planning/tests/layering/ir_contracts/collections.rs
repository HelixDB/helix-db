use super::*;

#[test]
fn at_least_two_collection_preserves_cardinality_contract() {
    assert!(AtLeast::<i32, 2>::try_from_vec(Vec::new()).is_none());
    assert!(AtLeast::<_, 2>::try_from_vec(vec![1]).is_none());

    let mut values = AtLeast::<_, 2>::try_from_vec(vec![3, 1, 2]).expect("three items are valid");
    assert_eq!(values.as_ref(), &[3, 1, 2]);
    assert_eq!(values.as_ref(), &[3, 1, 2]);
    assert_eq!(values.iter().copied().sum::<i32>(), 6);

    values.sort_by_key(|value| *value);
    assert_eq!(&values[..], &[1, 2, 3]);

    let borrowed = (&values).into_iter().copied().collect::<Vec<_>>();
    assert_eq!(borrowed, vec![1, 2, 3]);
    let consumed = values.into_iter().collect::<Vec<_>>();
    assert_eq!(consumed, vec![1, 2, 3]);

    let from_pair = AtLeast::<_, 2>::from_pair("left", "right");
    assert_eq!(
        from_pair.into_iter().collect::<Vec<_>>(),
        vec!["left", "right"]
    );

    let from_pair_and_rest = AtLeast::<_, 2>::from_pair_and_rest(1, 2, vec![3, 4]);
    assert_eq!(
        from_pair_and_rest.into_iter().collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
}

#[test]
fn at_least_two_serializes_as_array_and_rejects_invalid_arrays() {
    let values = AtLeast::<_, 2>::from_pair_and_rest(1, 2, vec![3]);
    assert_eq!(serde_json::to_string(&values).unwrap(), "[1,2,3]");

    let parsed: AtLeast<i32, 2> = serde_json::from_str("[7,8]").unwrap();
    assert_eq!(parsed.as_ref(), &[7, 8]);
    assert!(serde_json::from_str::<AtLeast<i32, 2>>("[]").is_err());
    assert!(serde_json::from_str::<AtLeast<i32, 2>>("[1]").is_err());
    assert!(serde_json::from_value::<AtLeast<PhysicalOp, 2>>(serde_json::json!([])).is_err());
    assert!(
        AtLeast::<_, 2>::try_from_vec(vec![PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)])
            .is_none()
    );
    assert!(AtLeast::<_, 2>::try_from_vec(vec![
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan),
        PhysicalOp::EdgeAccess(EdgeAccessPlan::AllScan),
    ])
    .is_some());
    assert!(
        serde_json::from_value::<AtLeast<NodeAccessSourcePlan, 2>>(serde_json::json!([])).is_err()
    );
    assert!(AtLeast::<_, 2>::try_from_vec(vec![
        NodeAccessSourcePlan::new(NodeAccessPlan::AllScan).unwrap(),
        NodeAccessSourcePlan::new(NodeAccessPlan::Empty).unwrap(),
    ])
    .is_some());
    assert!(
        serde_json::from_value::<AtLeast<EdgeAccessSourcePlan, 2>>(serde_json::json!([])).is_err()
    );
    assert!(AtLeast::<_, 2>::try_from_vec(vec![
        EdgeAccessSourcePlan::new(EdgeAccessPlan::AllScan).unwrap(),
        EdgeAccessSourcePlan::new(EdgeAccessPlan::Empty).unwrap(),
    ])
    .is_some());
}

#[test]
fn non_empty_collection_preserves_cardinality_contract() {
    assert!(AtLeast::<i32, 1>::try_from_vec(Vec::new()).is_none());

    let single = AtLeast::<_, 1>::from_one(9);
    assert_eq!(single.as_ref(), &[9]);
    assert_eq!(single.iter().copied().sum::<i32>(), 9);
    assert_eq!((&single).into_iter().copied().collect::<Vec<_>>(), vec![9]);
    assert_eq!(single.into_iter().collect::<Vec<_>>(), vec![9]);

    let many = AtLeast::<_, 1>::from_one_and_rest("first", vec!["second", "third"]);
    assert_eq!(many.as_ref(), ["first", "second", "third"]);
    assert_eq!(
        many.into_iter().collect::<Vec<_>>(),
        vec!["first", "second", "third"]
    );

    let canonical = AtLeast::<_, 1>::from_one_and_rest(3, vec![1, 3, 2]).sorted_dedup();
    assert_eq!(canonical.as_ref(), &[1, 2, 3]);
}

#[test]
fn non_empty_serializes_as_array_and_rejects_empty_arrays() {
    let values = AtLeast::<_, 1>::from_one_and_rest(1, vec![2, 3]);
    assert_eq!(serde_json::to_string(&values).unwrap(), "[1,2,3]");

    let parsed: AtLeast<i32, 1> = serde_json::from_str("[7]").unwrap();
    assert_eq!(parsed.as_ref(), &[7]);
    let parsed_names: AtLeast<NonEmptyString, 1> =
        serde_json::from_value(serde_json::json!(["users"])).unwrap();
    assert_eq!(
        parsed_names.as_ref(),
        &[NonEmptyString::new("users").unwrap()]
    );
    assert!(serde_json::from_str::<AtLeast<i32, 1>>("[]").is_err());
    assert!(serde_json::from_value::<AtLeast<NonEmptyString, 1>>(serde_json::json!([])).is_err());
    assert!(serde_json::from_value::<AtLeast<PhysicalOp, 1>>(serde_json::json!([])).is_err());
    assert!(serde_json::from_value::<AtLeast<ProjectionItem, 1>>(serde_json::json!([])).is_err());
    assert!(
        serde_json::from_value::<AtLeast<BindingValueRefPlan, 1>>(serde_json::json!([])).is_err()
    );
    assert!(
        serde_json::from_value::<AtLeast<BindingProjectionPlan, 1>>(serde_json::json!([])).is_err()
    );
    assert!(serde_json::from_value::<AtLeast<OrderKey, 1>>(serde_json::json!([])).is_err());
    assert!(serde_json::from_value::<AtLeast<u64, 1>>(serde_json::json!([])).is_err());
}

#[test]
fn element_ids_preserve_non_empty_unique_id_contract() {
    let ids = ElementIds::new(AtLeast::<_, 1>::from_one_and_rest(7, vec![9])).unwrap();
    assert_eq!(ids.as_ref(), &[7, 9]);
    assert_eq!(serde_json::to_string(&ids).unwrap(), "[7,9]");

    let parsed: ElementIds = serde_json::from_str("[3,4]").unwrap();
    assert_eq!(parsed.as_ref(), &[3, 4]);
    assert_eq!(
        ElementIds::new(AtLeast::<_, 1>::from_one_and_rest(7, vec![7])).unwrap_err(),
        ElementIdsError::DuplicateId { id: 7 }
    );
    assert!(serde_json::from_str::<ElementIds>("[]").is_err());
    assert!(serde_json::from_str::<ElementIds>("[7,7]").is_err());
}

#[test]
fn non_empty_string_serializes_as_string_and_rejects_empty_strings() {
    assert!(NonEmptyString::new("").is_none());

    let name = NonEmptyString::new("users").unwrap();
    assert_eq!(name.as_ref(), "users");
    assert_eq!(name.as_ref(), "users");
    assert_eq!(&*name, "users");
    assert_eq!(name.to_string(), "users");
    assert_eq!(name.clone().to_string(), "users");
    assert_eq!(serde_json::to_string(&name).unwrap(), "\"users\"");

    let parsed: NonEmptyString = serde_json::from_str("\"accounts\"").unwrap();
    assert_eq!(parsed.as_ref(), "accounts");
    assert!(serde_json::from_str::<NonEmptyString>("\"\"").is_err());
    assert!(serde_json::from_str::<NonEmptyString>("[]").is_err());
}

#[test]
fn non_zero_usize_serializes_as_integer_and_rejects_zero() {
    assert!(NonZeroUsize::new(0).is_none());

    let value = NonZeroUsize::new(7).unwrap();
    assert_eq!(value.get(), 7);
    assert_eq!(serde_json::to_string(&value).unwrap(), "7");

    let parsed: NonZeroUsize = serde_json::from_str("9").unwrap();
    assert_eq!(parsed.get(), 9);
    assert!(serde_json::from_str::<NonZeroUsize>("0").is_err());
}
