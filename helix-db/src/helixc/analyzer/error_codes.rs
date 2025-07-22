use paste::paste;
use std::fmt::Debug;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum ErrorCode {
    /// `E101` – `unknown node type`
    E101,
    /// `E102` – `unknown edge type`
    E102,
    /// `E103` – `unknown vector type`
    E103,
    /// `E104` – `cannot access properties on this type`
    E104,
    /// `E105` – `invalid identifier`
    E105,
    /// `E106` – `use of undeclared node type in schema`
    E106,

    // TYPE ERRORS
    /// `E201` – `item type not in schema`
    E201,
    /// `E202` – `given field is not a valid field for a given item type`
    E202,
    /// `E203` – `cannot access properties on the type`
    E203,
    /// `E204` – `field is a reserved field name`
    E204,
    /// `E205` – `type of value does not match field type in schema for a given item type`
    E205,
    /// `E206` – `invalid value type`
    E206,
    /// `E207` – `edge type exists but it is not a valid edge type for the given item type`
    E207,
    /// `E208` – `field has not been indexed for given type`
    E208,
    /// `E209` – `unknown type for parameter`
    E209,

    // QUERY ERRORS
    /// `E301` – `variable not in scope`
    E301,
    /// `E302` – `variable previously declared`
    E302,
    /// `E303` – `invalid primitive value`
    E303,
    /// `E304` – `missing item type`
    E304,
    /// `E305` – `missing parameter`
    E305,

    // MCP ERRORS
    /// `E401` – `MCP query must return a single value`
    E401,

    // CONVERSION ERRORS
    /// `E501` - `invalid date`
    E501,

    // TRAVERSAL ERRORS
    /// `E601` - `invalid traversal`
    E601,
    /// `E602` - `invalid step`
    E602,
    /// `E603` - `SearchVector must be used on a vector type`
    E603,
    /// `E604` - `update is only valid on nodes or edges`
    E604,

    /// `E611` - `edge creation must have a to id`
    E611,
    /// `E612` - `edge creation must have a from id`
    E612,

    /// `E621` - `boolean comparison operation cannot be applied to given type`
    E621,
    /// `E622` - `type of property of given item does not match type of compared value`
    E622,
    /// `E623` - `edge type does not have a node type as its From source`
    E623,
    /// `E624` - `edge type does not have a node type as its To source`
    E624,
    /// `E625` - `edge type does not have a vector type as its From source`
    E625,
    /// `E626` - `edge type does not have a vector type as its To source`
    E626,

    /// `E631` - `range must have a start and end`
    E631,
    /// `E632` - `range start must be less than range end`
    E632,
    /// `E633` - `index of range must be an integer`
    E633,

    /// `E641` - `closure is only valid as the last step in a traversal`
    E641,
    /// `E642` - `object remapping is only valid as the last step in a traversal`
    E642,
    /// `E643` – `field previously excluded`
    E643,
    /// `E644` – `exclude is only valid as the last step in a traversal, or as the step before an object remapping or closure`
    E644,
    /// `E645` - `object remapping must have at least one field`
    E645,
    /// `E646` - `field value is empty`
    E646,

    /// `E651` - `in variable is not iterable`
    E651,
    /// `E652` - `variable is not a field of the inner type of the in variable`
    E652,
    /// `E653` - `inner type of in variable is not an object`
    E653,


    /// `W101` - `query has no return`
    W101,
}

#[macro_export]
macro_rules! implement_error_code {
    ($error_code:ident, $message:expr => { $($message_args:ident),* }, $hint:expr => { $($hint_args:ident),* }) => {
        paste! {
            impl ErrorCode {
                #[allow(unused)]
                #[allow(non_snake_case)]
                pub fn [<$error_code _message>]($($message_args: &str),*) -> String {
                    format!($message, $($message_args),*)
                }

                #[allow(unused)]
                #[allow(non_snake_case)]
                pub fn [<$error_code _hint>]($($hint_args: &str),*) -> String {
                    format!($hint, $($hint_args),*)
                }
            }
        }
    };
}

implement_error_code!(E101, "unknown node type `{}`" => { node_type }, "check the schema field names or declare the node type" => {});
implement_error_code!(E102, "unknown edge type `{} `" => { edge_type }, "check the schema field names or declare the edge type" => {});
implement_error_code!(E103, "unknown vector type `{}`" => { vector_type }, "check the schema field names or declare the vector type" => {});
implement_error_code!(E105, "invalid identifier `{}`" => { identifier }, "check the identifier" => {});
implement_error_code!(E106, "use of undeclared node or vector type `{}` in schema" => { item_type_name }, "declare `{}` in the schema before using it in an edge" => { item_type_name });


implement_error_code!(E201, "item type not in schema `{}`" => { item_type }, "check the schema field names" => {});
implement_error_code!(E202, 
    "given field `{}` is not a valid field for a given {} type `{}`" => { field_name, item_type, item_type_name  }, 
    "check the schema field names" => {});
implement_error_code!(E203, "cannot access properties on the type `{}`" => { type_name }, "ensure the type is a node, edge, or vector" => {});
implement_error_code!(E204, "field `{}` is a reserved field name" => { field_name }, "rename the field" => {});
implement_error_code!(E205, 
    "type of value `{}` does not match field type `{}` for {} type `{}`" => { value, field_type, item_type, item_type_name }, 
    "change the value type to match the field type defined in the schema" => {});
implement_error_code!(E206, "invalid value type `{}`" => { value_type }, "use a literal or an identifier" => {});
implement_error_code!(E207, "edge type `{}` exists but it is not a valid edge type for the given {} type `{}`" => { edge_type, item_type, item_type_name }, "check the schema field names" => {});
implement_error_code!(E208, "field `{}` has not been indexed for node type `{}`" => { field_name, node_type }, "use a field that has been indexed with `INDEX` in the schema for node type `{}`" => { node_type });
implement_error_code!(E209, "unknown type `{}` for parameter `{}`" => { parameter_type, parameter_name }, "declare or use a matching schema object or use a primitive type" => {});

implement_error_code!(E301, "variable `{}` not in scope" => { variable }, "check the variable" => {});
implement_error_code!(E302, "variable `{}` previously declared" => { variable }, "check the variable" => {});
implement_error_code!(E304, "missing {} type" => { item_type }, "add an {} type" => { item_type });
implement_error_code!(E305, "missing parameter `{}` for method `{}`" => { parameter_name, method_name }, "add the parameter `{}`" => { parameter_name });

implement_error_code!(E401, "MCP query must return a single value, but got `{}`" => { number_of_values }, "return a single value" => {});

implement_error_code!(E501, "invalid date `{}`" => { date }, "ensure the date conforms to the ISO 8601 or RFC 3339 formats" => {});

implement_error_code!(E601, "invalid traversal `{}`" => { traversal }, "ensure the traversal is valid" => {});
implement_error_code!(E602, "step `{}` is not valid given the previous step `{}`" => { step, previous_step }, "{}" => { reason });
implement_error_code!(E603, "`SearchV` must be used on a vector type, got `{}`, which is a `{}`" => { cur_ty, cur_ty_name }, "ensure the result of the previous step is a vector type" => {});
implement_error_code!(E604, "`UPDATE` step is only valid on nodes or edges, but got `{}`" => { step }, "use `UPDATE` on a node or edge or remove the `UPDATE` step" => {});
implement_error_code!(E611, "edge creation must have a to id" => {}, "add a `::To(target_node_id)` step to your edge creation" => {});
implement_error_code!(E612, "edge creation must have a from id" => {}, "add a `::From(source_node_id)` step to your edge creation" => {});

implement_error_code!(E621, "boolean comparison operation cannot be applied to given {} type `{}`" => { item_type, item_type_name }, "use a valid boolean comparison operation" => {});
implement_error_code!(E622, 
    "property `{}` of {} `{}` is of type `{}`, which does not match type of compared value which is of type `{}`" => { property_name, item_type, item_type_name, property_type, compared_value_type }, 
    "change the property type to match the compared value type" => {});
implement_error_code!(E623, "edge type `{}` does not have a node type as its `From` source" => { edge_type }, "set the `From` type of the edge to a node type" => {});
implement_error_code!(E624, "edge type `{}` does not have a node type as its `To` source" => { edge_type }, "set the `To` type of the edge to a node type" => {});
implement_error_code!(E625, "edge type `{}` does not have a vector type as its `From` source" => { edge_type }, "set the `From` type of the edge to a vector type" => {});
implement_error_code!(E626, "edge type `{}` does not have a vector type as its `To` source" => { edge_type }, "set the `To` type of the edge to a vector type" => {});

implement_error_code!(E631, "range must have a start and end, missing the `{}` value" => { start_or_end }, "add a `{}` value to the range" => { start_or_end });
implement_error_code!(E632, "range start must be less than range end, got `{}` which is larger than `{}`" => { start, end }, "change the range start to be less than the range end" => {});
implement_error_code!(E633, "index of range must be an integer, got `{}` which is of type `{}`" => { index, index_type }, "change {} to be an integer" => { index_type });

implement_error_code!(E641, "closure is only valid as the last step in a traversal" => {}, "move the closure to the end of the traversal" => {});
implement_error_code!(E642, "object remapping is only valid as the last step in a traversal" => {}, "move the object remapping to the end of the traversal" => {});
implement_error_code!(E643, "field `{}` previously excluded" => { field_name }, "remove the `exclude` step for this field" => {});
implement_error_code!(E644, "`exclude` is only valid as the last step in a traversal, or as the step before an object remapping or closure" => {}, "move the `exclude` step to the end of the traversal or before the object remapping or closure" => {});
implement_error_code!(E645, "object remapping must have at least one field" => {}, "add at least one field to the object remapping" => {});
implement_error_code!(E646, "field value is empty" => {}, "field value must be a literal, identifier, traversal,or object" => {});

implement_error_code!(E651, "`IN` variable `{}` is not iterable" => { in_variable }, "ensure the `in` variable is iterable" => {});
implement_error_code!(E652, "variable `{}` is not a field of the inner object of the `IN` variable `{}`" => { variable, in_variable }, "ensure `{}` is a field of `{}`" => { variable, in_variable });
implement_error_code!(E653, "inner object of `IN` variable `{}` is not an object" => { in_variable }, "ensure the inner type of `{}` is an object" => { in_variable });

#[macro_export]
macro_rules! generate_error {
    ($ctx:ident, $original_query:ident, $loc:expr, $error_code:ident, [$($message_args:expr),*], [$($hint_args:expr),*]) => {
        paste! {
            let msg = ErrorCode::[<$error_code _message>]($($message_args),*);
            let hint = ErrorCode::[<$error_code _hint>]($($hint_args),*);
            push_query_err($ctx, $original_query, $loc, ErrorCode::$error_code, msg, hint);
        }
    };
    ($ctx:ident, $original_query:ident, $loc:expr, $error_code:ident, $($message_args:expr),*) => {{
        paste! {
            let msg = ErrorCode::[<$error_code _message>]($($message_args),*);
            let hint = ErrorCode::[<$error_code _hint>]();
            push_query_err($ctx, $original_query, $loc, ErrorCode::$error_code, msg, hint);
        }
    }};
    ($ctx:ident, $original_query:ident, $loc:expr, $error_code:ident) => {{
        paste! {
            let msg = ErrorCode::[<$error_code _message>]();
            let hint = ErrorCode::[<$error_code _hint>]();
            push_query_err($ctx, $original_query, $loc, ErrorCode::$error_code, msg, hint);
        }
    }};
}
