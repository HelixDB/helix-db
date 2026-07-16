use super::*;

#[test]
fn index_ranges_prove_secondary_literal_containment_for_static_ordered_bounds() {
    let secondary = |value| SecondaryIndexLiteral::new(value).unwrap();
    let range_value = |value| RangeIndexValue::literal(value).unwrap();

    let lower_inclusive = IndexRange::Lower {
        lower: IndexBound::Inclusive(range_value(PropertyValue::from(18))),
    };
    let lower_exclusive = IndexRange::Lower {
        lower: IndexBound::Exclusive(range_value(PropertyValue::from(18))),
    };
    assert!(lower_inclusive.contains_secondary_literal(&secondary(PropertyValue::from(18))));
    assert!(lower_inclusive.contains_secondary_literal(&secondary(PropertyValue::from(21))));
    assert!(!lower_inclusive.contains_secondary_literal(&secondary(PropertyValue::from(17))));
    assert!(lower_inclusive.excludes_secondary_literal(&secondary(PropertyValue::from(17))));
    assert!(!lower_inclusive.excludes_secondary_literal(&secondary(PropertyValue::from(18))));
    assert!(!lower_exclusive.contains_secondary_literal(&secondary(PropertyValue::from(18))));
    assert!(lower_exclusive.excludes_secondary_literal(&secondary(PropertyValue::from(18))));

    let upper_inclusive = IndexRange::Upper {
        upper: IndexBound::Inclusive(range_value(PropertyValue::from(30))),
    };
    let upper_exclusive = IndexRange::Upper {
        upper: IndexBound::Exclusive(range_value(PropertyValue::from(30))),
    };
    assert!(upper_inclusive.contains_secondary_literal(&secondary(PropertyValue::from(30))));
    assert!(upper_inclusive.contains_secondary_literal(&secondary(PropertyValue::from(21))));
    assert!(!upper_inclusive.contains_secondary_literal(&secondary(PropertyValue::from(31))));
    assert!(upper_inclusive.excludes_secondary_literal(&secondary(PropertyValue::from(31))));
    assert!(!upper_inclusive.excludes_secondary_literal(&secondary(PropertyValue::from(30))));
    assert!(!upper_inclusive.excludes_secondary_literal(&secondary(PropertyValue::from(21))));
    assert!(!upper_exclusive.contains_secondary_literal(&secondary(PropertyValue::from(30))));
    assert!(upper_exclusive.excludes_secondary_literal(&secondary(PropertyValue::from(30))));

    let between = IndexRange::Between(
        IndexBetweenRange::new(
            IndexBound::Inclusive(range_value(PropertyValue::from(18))),
            IndexBound::Exclusive(range_value(PropertyValue::from(30))),
        )
        .unwrap(),
    );
    assert!(between.contains_secondary_literal(&secondary(PropertyValue::from(21))));
    assert!(!between.contains_secondary_literal(&secondary(PropertyValue::from(30))));
    assert!(between.excludes_secondary_literal(&secondary(PropertyValue::from(17))));
    assert!(between.excludes_secondary_literal(&secondary(PropertyValue::from(30))));

    for (range, literal) in [
        (
            IndexRange::Lower {
                lower: IndexBound::Inclusive(range_value(PropertyValue::datetime_millis(1_000))),
            },
            PropertyValue::datetime_millis(2_000),
        ),
        (
            IndexRange::Lower {
                lower: IndexBound::Inclusive(range_value(PropertyValue::from(1.5_f64))),
            },
            PropertyValue::from(2.5_f64),
        ),
        (
            IndexRange::Lower {
                lower: IndexBound::Inclusive(range_value(PropertyValue::from(1.5_f32))),
            },
            PropertyValue::from(2.5_f32),
        ),
        (
            IndexRange::Lower {
                lower: IndexBound::Inclusive(range_value(PropertyValue::from("alice"))),
            },
            PropertyValue::from("bob"),
        ),
    ] {
        assert!(range.contains_secondary_literal(&secondary(literal)));
    }

    let dynamic_lower = IndexRange::Lower {
        lower: IndexBound::Inclusive(RangeIndexValue::param("min").unwrap()),
    };
    let dynamic_upper = IndexRange::Upper {
        upper: IndexBound::Inclusive(RangeIndexValue::param("max").unwrap()),
    };
    assert!(!dynamic_lower.contains_secondary_literal(&secondary(PropertyValue::from(21))));
    assert!(!dynamic_upper.contains_secondary_literal(&secondary(PropertyValue::from(21))));
    assert!(!dynamic_lower.excludes_secondary_literal(&secondary(PropertyValue::from(21))));
    assert!(!dynamic_upper.excludes_secondary_literal(&secondary(PropertyValue::from(21))));
    assert!(!lower_inclusive.contains_secondary_literal(&secondary(PropertyValue::from(true))));
    assert!(!lower_inclusive.contains_secondary_literal(&secondary(PropertyValue::from("21"))));
    assert!(!lower_inclusive.excludes_secondary_literal(&secondary(PropertyValue::from(true))));
    assert!(!lower_inclusive.excludes_secondary_literal(&secondary(PropertyValue::from("21"))));

    assert!(IndexRange::All.contains_secondary_literal(&secondary(PropertyValue::from(21))));
    assert!(!IndexRange::All.contains_secondary_literal(&secondary(PropertyValue::from(true))));
    assert!(!IndexRange::All.excludes_secondary_literal(&secondary(PropertyValue::from(21))));
}

#[test]
fn index_ranges_prove_static_range_containment() {
    let range_value = |value| RangeIndexValue::literal(value).unwrap();
    let lower = |value| IndexBound::Inclusive(range_value(PropertyValue::from(value)));
    let lower_exclusive = |value| IndexBound::Exclusive(range_value(PropertyValue::from(value)));
    let upper = |value| IndexBound::Inclusive(range_value(PropertyValue::from(value)));
    let upper_exclusive = |value| IndexBound::Exclusive(range_value(PropertyValue::from(value)));
    let lower_range = |value| IndexRange::Lower {
        lower: lower(value),
    };
    let upper_range = |value| IndexRange::Upper {
        upper: upper(value),
    };
    let between =
        |min, max| IndexRange::Between(IndexBetweenRange::new(lower(min), upper(max)).unwrap());

    assert!(lower_range(18).contains_range(&lower_range(21)));
    assert!(!lower_range(21).contains_range(&lower_range(18)));
    assert!(upper_range(30).contains_range(&upper_range(21)));
    assert!(!upper_range(21).contains_range(&upper_range(30)));
    assert!(lower_range(18).contains_range(&between(21, 30)));
    assert!(!between(21, 30).contains_range(&lower_range(21)));
    assert!(between(18, 65).contains_range(&between(21, 30)));
    assert!(!between(21, 30).contains_range(&between(18, 65)));
    assert!(
        IndexRange::Lower { lower: lower(18) }.contains_range(&IndexRange::Lower {
            lower: lower_exclusive(18),
        })
    );
    assert!(!IndexRange::Lower {
        lower: lower_exclusive(18),
    }
    .contains_range(&IndexRange::Lower { lower: lower(18) }));
    assert!(
        IndexRange::Upper { upper: upper(30) }.contains_range(&IndexRange::Upper {
            upper: upper_exclusive(30),
        })
    );
    assert!(!IndexRange::Upper {
        upper: upper_exclusive(30),
    }
    .contains_range(&IndexRange::Upper { upper: upper(30) }));

    let dynamic_lower = IndexRange::Lower {
        lower: IndexBound::Inclusive(RangeIndexValue::param("min").unwrap()),
    };
    let dynamic_upper = IndexRange::Upper {
        upper: IndexBound::Inclusive(RangeIndexValue::param("max").unwrap()),
    };
    assert!(dynamic_lower.contains_range(&dynamic_lower));
    assert!(!dynamic_lower.contains_range(&lower_range(18)));
    assert!(!lower_range(18).contains_range(&dynamic_lower));
    assert!(dynamic_upper.contains_range(&dynamic_upper));
    assert!(!dynamic_upper.contains_range(&upper_range(30)));
    assert!(!upper_range(30).contains_range(&dynamic_upper));
    assert!(!lower_range(18).contains_range(&IndexRange::Lower {
        lower: IndexBound::Inclusive(range_value(PropertyValue::from("21"))),
    }));
    assert!(IndexRange::All.contains_range(&lower_range(18)));
    assert!(IndexRange::All.contains_range(&IndexRange::All));
    assert!(!lower_range(18).contains_range(&IndexRange::All));
}

#[test]
fn index_ranges_intersect_when_tighter_bounds_are_proven() {
    let range_value = |value| RangeIndexValue::literal(value).unwrap();
    let lower = |value| IndexBound::Inclusive(range_value(PropertyValue::from(value)));
    let lower_exclusive = |value| IndexBound::Exclusive(range_value(PropertyValue::from(value)));
    let upper = |value| IndexBound::Inclusive(range_value(PropertyValue::from(value)));
    let upper_exclusive = |value| IndexBound::Exclusive(range_value(PropertyValue::from(value)));
    let lower_range = |value| IndexRange::Lower {
        lower: lower(value),
    };
    let upper_range = |value| IndexRange::Upper {
        upper: upper(value),
    };
    let between =
        |min, max| IndexRange::Between(IndexBetweenRange::new(lower(min), upper(max)).unwrap());

    assert_eq!(
        lower_range(18).intersect(&upper_range(65)),
        IndexBetweenRange::new(lower(18), upper(65)).map(IndexRange::Between)
    );
    assert_eq!(
        IndexRange::All.intersect(&lower_range(18)),
        Some(lower_range(18))
    );
    assert_eq!(
        upper_range(65).intersect(&IndexRange::All),
        Some(upper_range(65))
    );
    assert_eq!(
        lower_range(18).intersect(&lower_range(21)),
        Some(lower_range(21))
    );
    assert_eq!(
        lower_range(18).intersect(&IndexRange::Lower {
            lower: lower_exclusive(18),
        }),
        Some(IndexRange::Lower {
            lower: lower_exclusive(18),
        })
    );
    assert_eq!(
        IndexRange::Lower {
            lower: lower_exclusive(18),
        }
        .intersect(&lower_range(18)),
        Some(IndexRange::Lower {
            lower: lower_exclusive(18),
        })
    );
    assert_eq!(
        upper_range(30).intersect(&upper_range(21)),
        Some(upper_range(21))
    );
    assert_eq!(
        upper_range(30).intersect(&IndexRange::Upper {
            upper: upper_exclusive(30),
        }),
        Some(IndexRange::Upper {
            upper: upper_exclusive(30),
        })
    );
    assert_eq!(
        IndexRange::Upper {
            upper: upper_exclusive(30),
        }
        .intersect(&upper_range(30)),
        Some(IndexRange::Upper {
            upper: upper_exclusive(30),
        })
    );
    assert_eq!(
        between(18, 65).intersect(&between(18, 30)),
        Some(between(18, 30))
    );
    assert_eq!(
        between(18, 65).intersect(&between(21, 65)),
        Some(between(21, 65))
    );
    assert_eq!(
        between(18, 65).intersect(&IndexRange::Lower {
            lower: lower_exclusive(21),
        }),
        IndexBetweenRange::new(lower_exclusive(21), upper(65)).map(IndexRange::Between)
    );
    assert_eq!(
        between(18, 65).intersect(&IndexRange::Upper {
            upper: upper_exclusive(65),
        }),
        IndexBetweenRange::new(lower(18), upper_exclusive(65)).map(IndexRange::Between)
    );

    let dynamic_lower = IndexRange::Lower {
        lower: IndexBound::Inclusive(RangeIndexValue::param("min").unwrap()),
    };
    let dynamic_upper = IndexRange::Upper {
        upper: IndexBound::Exclusive(RangeIndexValue::param("max").unwrap()),
    };
    assert_eq!(
        dynamic_lower.intersect(&dynamic_lower),
        Some(dynamic_lower.clone())
    );
    assert_eq!(
        dynamic_lower.intersect(&dynamic_upper),
        IndexBetweenRange::new(
            IndexBound::Inclusive(RangeIndexValue::param("min").unwrap()),
            IndexBound::Exclusive(RangeIndexValue::param("max").unwrap()),
        )
        .map(IndexRange::Between)
    );
    assert_eq!(
        dynamic_lower.intersect(&IndexRange::Lower {
            lower: IndexBound::Inclusive(RangeIndexValue::param("other_min").unwrap()),
        }),
        None
    );
    assert_eq!(lower_range(18).intersect(&dynamic_lower), None);
    assert_eq!(
        lower_range(18).intersect(&IndexRange::Lower {
            lower: IndexBound::Inclusive(range_value(PropertyValue::from("bob"))),
        }),
        None
    );
    assert_eq!(
        dynamic_upper.intersect(&IndexRange::Upper {
            upper: IndexBound::Inclusive(RangeIndexValue::param("other_max").unwrap()),
        }),
        None
    );
    assert_eq!(dynamic_upper.intersect(&upper_range(30)), None);
    assert_eq!(upper_range(30).intersect(&dynamic_upper), None);
    assert_eq!(
        upper_range(30).intersect(&IndexRange::Upper {
            upper: IndexBound::Inclusive(range_value(PropertyValue::from("bob"))),
        }),
        None
    );
    assert_eq!(
        lower_range(30).intersect(&IndexRange::Upper { upper: upper(18) }),
        None
    );
    assert_eq!(
        lower_range(18).intersect(&IndexRange::Upper {
            upper: IndexBound::Inclusive(range_value(PropertyValue::from("bob"))),
        }),
        None
    );
}
