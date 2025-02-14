pub mod macros {
    #[macro_export]
    /// Creates array of pairs which each represent the property key and corresponding value.
    ///
    /// ## Example Use
    /// ```rust
    /// use helix_engine::props;
    /// use protocol::value::Value;
    ///
    /// let properties: Vec<(String, Value)> = props! {
    ///     "name" => "Will",
    ///     "age" => 21,
    /// };
    ///
    /// assert_eq!(properties.len(), 2);
    /// ```
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
    /// use helix_engine::node_matches;
    /// use protocol::value::Value;
    /// use protocol::node::Node;
    ///
    /// let pred = node_matches!("name", "Will");
    ///
    /// let node = Node::new("person", vec![
    ///    ("name".to_string(), Value::String("Will".to_string())),
    ///   ("age".to_string(), Value::Number(21.0)),
    /// ]);
    ///
    ///
    /// assert_eq!(pred(&node), true);
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
                Err(e) => return Err(GraphError::ConversionError(format!("Error Decoding: {:?}", e))),
            }
        };
    }
}
