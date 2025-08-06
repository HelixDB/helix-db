pub mod macros {
    #[macro_export]
    /// Creates array of pairs which each represent the property key and corresponding value.
    /// If a value is None, it will be excluded from the final vector.
    /// The vector is preallocated with capacity for all potential items.
    ///
    /// ## Example Use
    /// ```rust
    /// use helix_db::optional_props;
    /// use helix_db::protocol::value::Value;
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
    /// use helix_db::props;
    /// use helix_db::protocol::value::Value;
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
    /// use helix_db::node_matches;
    /// use helix_db::protocol::value::Value;
    /// use helix_db::protocol::items::Node;
    /// use helix_db::protocol::filterable::Filterable;
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
            |node: &helix_db::protocol::items::Node| {
                if let Some(val) = node.check_property($key) {
                    if let helix_db::protocol::value::Value::String(val) = &val {
                        Ok(*val == $value)
                    } else {
                        Err(helix_db::helix_engine::types::GraphError::from(
                            "Invalid node".to_string(),
                        ))
                    }
                } else {
                    Err(helix_db::helix_engine::types::GraphError::from(
                        "Invalid node".to_string(),
                    ))
                }
            }
        };
    }

    #[macro_export]
    macro_rules! edge_matches {
        ($key:expr, $value:expr) => {
            |edge: &helix_db::protocol::items::Edge| {
                if let Some(val) = edge.check_property($key) {
                    if let helix_db::protocol::value::Value::String(val) = &val {
                        Ok(*val == $value)
                    } else {
                        Err(helix_db::helix_engine::types::GraphError::from(
                            "Invalid edge".to_string(),
                        ))
                    }
                } else {
                    Err(helix_db::helix_engine::types::GraphError::from(
                        "Invalid edge".to_string(),
                    ))
                }
            }
        };
    }

    #[macro_export]
    macro_rules! field_remapping {
        ($remapping_vals:expr, $var_name:expr, $should_spread:expr, $old_name:expr => $new_name:expr) => {{
            let old_value = match $var_name.check_property($old_name) {
                Ok(val) => val,
                Err(e) => {
                    return Err(GraphError::ConversionError(format!(
                        "Error Decoding: {:?}",
                        "Invalid node".to_string()
                    )));
                }
            };
            let old_value_remapping =
                Remapping::new(false, Some($new_name), Some(ReturnValue::from(old_value)));
            $remapping_vals.insert(
                $var_name.id(),
                ResponseRemapping::new(
                    HashMap::from([($old_name.to_string(), old_value_remapping)]),
                    $should_spread,
                ),
            );
            Ok::<TraversalVal, GraphError>($var_name) // Return the Ok value
        }};
    }

    #[macro_export]
    macro_rules! traversal_remapping {
        ($remapping_vals:expr, $var_name:expr, $should_spread:expr, $new_name:expr => $traversal:expr) => {{
            // TODO: ref?
            let new_remapping = Remapping::new(
                false,
                Some($new_name.to_string()),
                Some(ReturnValue::from($traversal)),
            );
            $remapping_vals.insert(
                $var_name.id(),
                ResponseRemapping::new(
                    HashMap::from([($new_name.to_string(), new_remapping)]),
                    $should_spread,
                ),
            );
            Ok::<TraversalVal, GraphError>($var_name)
        }};
    }

    #[macro_export]
    macro_rules! exclude_field {
        ($remapping_vals:expr, $var_name:expr, $($field_to_exclude:expr),* $(,)?) => {{

            $(
                let field_to_exclude_remapping = Remapping::new(
                    true,
                    None,
                    None,
                );
                $remapping_vals.insert(
                    $var_name.id(),
                    ResponseRemapping::new(
                        HashMap::from([($field_to_exclude.to_string(), field_to_exclude_remapping)]),
                        true,
                    ),
                );
                println!("inserting remapping: {:?}", $remapping_vals.borrow_mut());
            )*
                Ok::<TraversalVal, GraphError>($var_name)
        }};
    }

    #[macro_export]
    macro_rules! identifier_remapping {
        ($remapping_vals:expr, $var_name:expr, $should_spread:expr, $field_name:expr =>  $identifier_value:expr) => {{
            let value = match $var_name.check_property($field_name) {
                Ok(val) => val.clone(), // TODO: try and remove clone
                Err(e) => {
                    return Err(GraphError::ConversionError(format!(
                        "Error Decoding: {:?}",
                        "Invalid node".to_string()
                    )));
                }
            };
            let value_remapping = Remapping::new(
                false,
                Some($identifier_value.to_string()),
                Some(ReturnValue::from(value)),
            );
            -$remapping_vals.insert(
                $var_name.id(),
                ResponseRemapping::new(
                    HashMap::from([($field_name.to_string(), value_remapping)]),
                    $should_spread,
                ),
            );
            Ok::<TraversalVal, GraphError>($var_name)
        }};
    }

    #[macro_export]
    macro_rules! value_remapping {
        ($remapping_vals:expr, $var_name:expr, $should_spread:expr, $field_name:expr =>  $value:expr) => {{
            let value = match $var_name.check_property($field_name) {
                Ok(val) => val.clone(),
                Err(e) => {
                    return Err(GraphError::ConversionError(format!(
                        "Error Decoding: {:?}",
                        "Invalid node".to_string()
                    )));
                }
            };
            let old_value_remapping = Remapping::new(
                false,
                Some($field_name.to_string()),
                Some(ReturnValue::from(value)),
            );
            $remapping_vals.insert(
                $var_name.id(),
                ResponseRemapping::new(
                    HashMap::from([($field_name.to_string(), old_value_remapping)]),
                    $should_spread,
                ),
            );
            Ok::<TraversalVal, GraphError>($var_name) // Return the Ok value
        }};
    }

    #[macro_export]
    /// simply just a debug logging function
    macro_rules! debug_println {
        ($($arg:tt)*) => {
            #[cfg(feature = "debug-output")]
            {
                let caller = std::any::type_name_of_val(&|| {});
                let caller = caller.strip_suffix("::{{closure}}").unwrap_or(caller);
                println!("{}:{} =>\n\t{}", caller, line!(), format_args!($($arg)*));
            }
        };
    }

    /// Time a block of code
    /// time_block!("my label" {
    ///     let x = 1 + 2;
    /// });
    #[macro_export]
    macro_rules! time_block {
        // params: label, code block
        ($label:expr, $($block:tt)*) => {{
            use std::time::Instant;
            let start_time = Instant::now();
            $($block)*
                let time = start_time.elapsed();
            println!("{}: time elapsed: {:?}", $label, time);
            time
        }};
    }

    /// Time a block of code and be able to return something out of the block
    /// time_block_result!("my label" {
    ///     let x = 1 + 2;
    ///     x
    /// });
    #[macro_export]
    macro_rules! time_block_result {
        // params: label, code block
        ($label:expr, $($block:tt)*) => {{
            use std::time::Instant;
            let start_time = Instant::now();
            let result = { $($block)* };
            let time = start_time.elapsed();
            println!("{}: time elapsed: {:?}", $label, time);
            result
        }};
    }

    #[macro_export]
    macro_rules! err_bubble {
        ($ret_chan:ident, $e:expr) => {
            match e {
                Ok(v) => v,
                Err(e) => {
                    let _ = $ret_chan.send(e.into());
                    return;
                }
            }
        };
    }
}
