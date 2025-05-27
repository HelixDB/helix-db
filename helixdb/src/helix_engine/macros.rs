pub mod macros {
    #[macro_export]
    /// Creates array of pairs which each represent the property key and corresponding value.
    /// If a value is None, it will be excluded from the final vector.
    /// The vector is preallocated with capacity for all potential items.
    ///
    /// ## Example Use
    /// ```rust
    /// use helixdb::optional_props;
    /// use helixdb::protocol::value::Value;
    ///
    /// let properties: Vec<(String, Value)> = optional_props! {
    ///     "name" => Some("Will"),
    ///     "age" => Some(21),
    ///     "title" => None::<String>,
    /// };
    ///
    /// assert_eq!(properties.len(), 2); // "title" is excluded
    /// ```
    macro_rules! optional_props {
    () => {
        vec![]
    };
    ($($key:expr => $value:expr),* $(,)?) => {{
        let mut vec = Vec::with_capacity($crate::count!($($key),*));
        $(
            if let Some(value) = $value {
                vec.push((String::from($key), value.into()));
            }
        )*
        vec
    }};
}

    // Helper macro to count the number of expressions
    #[macro_export]
    #[doc(hidden)]
    macro_rules! count {
    () => (0);
    ($head:expr $(, $tail:expr)*) => (1 + $crate::count!($($tail),*));
    }

    #[macro_export]
    /// Creates array of pairs which each represent the property key and corresponding value.
    ///
    /// ## Example Use
    /// ```rust
    /// use helixdb::props;
    /// use helixdb::protocol::value::Value;
    ///
    /// let properties: Vec<(String, Value)> = props! {
    ///     "name" => "Will",
    ///     "age" => 21,
    /// };
    ///
    /// assert_eq!(properties.len(), 2);
    macro_rules! props {
    () => {
    vec![]
    };
    ($($key:expr => $value:expr),* $(,)?) => {
        vec![
            $(
            (String::from($key), $value.into()),
            )*
        ]
    };
 }

    #[macro_export]
    /// Creates a closeure that takes a node and checks a property of the node against a value.
    /// The closure returns true if the property matches the value, otherwise false.
    ///
    /// ## Example Use
    ///
    /// ```rust
    /// use helixdb::node_matches;
    /// use helixdb::protocol::value::Value;
    /// use helixdb::protocol::items::Node;
    /// use helixdb::protocol::filterable::Filterable;
    /// let pred = node_matches!("name", "Will");
    ///
    /// let node = Node::new("person", vec![
    ///    ("name".to_string(), Value::String("Will".to_string())),
    ///   ("age".to_string(), Value::Integer(21)),
    /// ]);
    ///
    ///
    /// assert_eq!(pred(&node).unwrap(), true);
    /// ```
    macro_rules! node_matches {
        ($key:expr, $value:expr) => {
            |node: &helixdb::protocol::items::Node| {
                if let Some(val) = node.check_property($key) {
                    if let helixdb::protocol::value::Value::String(val) = &val {
                        Ok(*val == $value)
                    } else {
                        Err(helixdb::helix_engine::types::GraphError::from(
                            "Invalid node".to_string(),
                        ))
                    }
                } else {
                    Err(helixdb::helix_engine::types::GraphError::from(
                        "Invalid node".to_string(),
                    ))
                }
            }
        };
    }

    #[macro_export]
    macro_rules! edge_matches {
        ($key:expr, $value:expr) => {
            |edge: &helixdb::protocol::items::Edge| {
                if let Some(val) = edge.check_property($key) {
                    if let helixdb::protocol::value::Value::String(val) = &val {
                        Ok(*val == $value)
                    } else {
                        Err(helixdb::helix_engine::types::GraphError::from(
                            "Invalid edge".to_string(),
                        ))
                    }
                } else {
                    Err(helixdb::helix_engine::types::GraphError::from(
                        "Invalid edge".to_string(),
                    ))
                }
            }
        };
    }

    #[macro_export]
    macro_rules! field_remapping {
        ($remapping_vals:expr, $var_name:expr, $old_name:expr => $new_name:expr) => {{
            let old_value = match $var_name.check_property($old_name) {
                Ok(val) => val,
                Err(e) => {
                    return Err(GraphError::ConversionError(format!(
                        "Error Decoding: {:?}",
                        "Invalid node".to_string()
                    )))
                }
            };
            let old_value_remapping =
                Remapping::new(false, Some($new_name), Some(ReturnValue::from(old_value)));
            $remapping_vals.borrow_mut().insert(
                $var_name.id(),
                ResponseRemapping::new(
                    HashMap::from([($old_name.to_string(), old_value_remapping)]),
                    false,
                ),
            );
            Ok::<TraversalVal, GraphError>($var_name) // Return the Ok value
        }};
    }

    #[macro_export]
    macro_rules! traversal_remapping {
        ($remapping_vals:expr, $var_name:expr, $new_name:expr => $traversal:expr) => {{
            // TODO: ref?
            let traversal_result: Vec<TraversalVal> = $traversal;
            let new_remapping = Remapping::new(
                false,
                Some($new_name.to_string()),
                Some(ReturnValue::from(traversal_result)),
            );
            $remapping_vals.borrow_mut().insert(
                $var_name.id(),
                ResponseRemapping::new(
                    HashMap::from([($new_name.to_string(), new_remapping)]),
                    false,
                ),
            );
            Ok::<TraversalVal, GraphError>($var_name)
        }};
    }

    #[macro_export]
    macro_rules! exclude_field {
        ($remapping_vals:expr, $($field_to_exclude:expr),* $(,)?) => {{

                    $(
                    let $field_to_exclude_remapping = Remapping::new(
                        true,
                        Some($field_to_exclude),
                        None,
                    );
                    $remapping_vals.borrow_mut().insert(
                        item.id(),
                        ResponseRemapping::new(
                            HashMap::from([($field_to_exclude.to_string(), $field_to_exclude_remapping)]),
                            false,
                        ),
                    );
                    )*
        }};
    }

    #[macro_export]
    macro_rules! identifier_remapping {
        ($remapping_vals:expr, $var_name:expr, $field_name:expr =>  $identifier_value:expr) => {{
            let value = match $var_name.check_property($field_name) {
                Ok(val) => val.clone(), // TODO: try and remove clone
                Err(e) => {
                    return Err(GraphError::ConversionError(format!(
                        "Error Decoding: {:?}",
                        "Invalid node".to_string()
                    )))
                }
            };
            let value_remapping = Remapping::new(
                false,
                Some($identifier_value.to_string()),
                Some(ReturnValue::from(value)),
            );
            $remapping_vals.borrow_mut().insert(
                $var_name.id(),
                ResponseRemapping::new(
                    HashMap::from([($field_name.to_string(), value_remapping)]),
                    false,
                ),
            );
            Ok::<TraversalVal, GraphError>($var_name)
        }};
    }

    #[macro_export]
    macro_rules! value_remapping {
        ($remapping_vals:expr, $var_name:expr, $field_name:expr =>  $value:expr) => {{
            let old_value = match $var_name.check_property($field_name) {
                Ok(val) => val,
                Err(e) => {
                    return Err(GraphError::ConversionError(format!(
                        "Error Decoding: {:?}",
                        "Invalid node".to_string()
                    )))
                }
            };
            let old_value_remapping =
                Remapping::new(false, Some(value), Some(ReturnValue::from(old_value)));
            $remapping_vals.borrow_mut().insert(
                $var_name.id(),
                ResponseRemapping::new(
                    HashMap::from([($field_name.to_string(), old_value_remapping)]),
                    false,
                ),
            );
            Ok(()) // Return the Ok value
        }};
    }
}
