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
    macro_rules! decode_str {
        ($value:expr) => {
            match $value.decode() {
                Ok(v) => std::str::from_utf8(v)?,
                Err(e) => {
                    return Err(GraphError::ConversionError(format!(
                        "Error Decoding: {:?}",
                        e
                    )))
                }
            }
        };
    }

    #[macro_export]
    macro_rules! decode_string {
        ($value:expr) => {
            match $value.decode() {
                Ok(v) => String::from_utf8(v.to_vec())?,
                Err(e) => {
                    return Err(GraphError::ConversionError(format!(
                        "Error Decoding: {:?}",
                        e
                    )))
                }
            }
        };
    }
}
