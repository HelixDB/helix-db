use paste::paste;
use std::fmt::Debug;

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
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
    /// `E107` – `duplicate schema definition`
    E107,
    /// `E108` – `invalid schema version`
    E108,
    /// `E109` – `duplicate field name in schema`
    E109,
    /// `E110` – `schema item name is a reserved type name`
    E110,

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
    /// `E210` – `identifier was expected to be of type ID, but got {}`
    E210,
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
    /// `E306` – `expression is not a boolean`
    E306,

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
    /// `E627` - `shortest path requires from or to parameter`
    E627,
    /// `E628` - `DROP can only be applied to traversals`
    E628,

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
    /// `E654` - `object access in for loop not supported`
    E654,
    /// `E655` - `internal analyzer error`
    E655,
    /// `E656` - `unsupported type conversion`
    E656,
    /// `E657` - `step requires a previous step`
    E657,
    /// `E658` - `field not found in object type`
    E658,

    /// `W101` - `query has no return`
    W101,
}
impl ErrorCode {
    /// Returns a short human-readable description of the error (e.g. "unknown edge type").
    pub fn description(&self) -> &'static str {
        match self {
            // Schema errors
            ErrorCode::E101 => "unknown node type",
            ErrorCode::E102 => "unknown edge type",
            ErrorCode::E103 => "unknown vector type",
            ErrorCode::E104 => "cannot access properties on this type",
            ErrorCode::E105 => "invalid identifier",
            ErrorCode::E106 => "use of undeclared node type in schema",
            ErrorCode::E107 => "duplicate schema definition",
            ErrorCode::E108 => "invalid schema version",
            ErrorCode::E109 => "duplicate field name in schema",
            ErrorCode::E110 => "schema item name is a reserved type name",
            // Type errors
            ErrorCode::E201 => "item type not in schema",
            ErrorCode::E202 => "invalid field for item type",
            ErrorCode::E203 => "cannot access properties on the type",
            ErrorCode::E204 => "field is a reserved field name",
            ErrorCode::E205 => "value type does not match field type",
            ErrorCode::E206 => "invalid value type",
            ErrorCode::E207 => "invalid edge type for item type",
            ErrorCode::E208 => "field has not been indexed",
            ErrorCode::E209 => "unknown type for parameter",
            ErrorCode::E210 => "expected ID type",
            // Query errors
            ErrorCode::E301 => "variable not in scope",
            ErrorCode::E302 => "variable previously declared",
            ErrorCode::E303 => "invalid primitive value",
            ErrorCode::E304 => "missing item type",
            ErrorCode::E305 => "missing parameter",
            ErrorCode::E306 => "expression is not a boolean",
            // MCP errors
            ErrorCode::E401 => "MCP query must return a single value",
            // Conversion errors
            ErrorCode::E501 => "invalid date",
            // Traversal errors
            ErrorCode::E601 => "invalid traversal",
            ErrorCode::E602 => "invalid step",
            ErrorCode::E603 => "SearchVector must be used on a vector type",
            ErrorCode::E604 => "update is only valid on nodes or edges",
            ErrorCode::E611 => "edge creation must have a to id",
            ErrorCode::E612 => "edge creation must have a from id",
            ErrorCode::E621 => "boolean comparison cannot be applied to type",
            ErrorCode::E622 => "property type does not match compared value",
            ErrorCode::E623 => "edge type does not have a node type as its From source",
            ErrorCode::E624 => "edge type does not have a node type as its To source",
            ErrorCode::E625 => "edge type does not have a vector type as its From source",
            ErrorCode::E626 => "edge type does not have a vector type as its To source",
            ErrorCode::E627 => "shortest path requires from or to parameter",
            ErrorCode::E628 => "DROP can only be applied to traversals",
            // Range errors
            ErrorCode::E631 => "range must have a start and end",
            ErrorCode::E632 => "range start must be less than range end",
            ErrorCode::E633 => "index of range must be an integer",
            // Object remapping errors
            ErrorCode::E641 => "closure is only valid as the last step in a traversal",
            ErrorCode::E642 => "object remapping is only valid as the last step in a traversal",
            ErrorCode::E643 => "field previously excluded",
            ErrorCode::E644 => "exclude is only valid as the last step in a traversal",
            ErrorCode::E645 => "object remapping must have at least one field",
            ErrorCode::E646 => "field value is empty",
            // For loop errors
            ErrorCode::E651 => "in variable is not iterable",
            ErrorCode::E652 => "variable is not a field of the inner type",
            ErrorCode::E653 => "inner type of in variable is not an object",
            ErrorCode::E654 => "object access in for loop not supported",
            ErrorCode::E655 => "internal analyzer error",
            ErrorCode::E656 => "unsupported type conversion",
            ErrorCode::E657 => "step requires a previous step",
            ErrorCode::E658 => "field not found in object type",
            // Warnings
            ErrorCode::W101 => "query has no return",
        }
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorCode::E101 => write!(f, "E101"),
            ErrorCode::E102 => write!(f, "E102"),
            ErrorCode::E103 => write!(f, "E103"),
            ErrorCode::E104 => write!(f, "E104"),
            ErrorCode::E105 => write!(f, "E105"),
            ErrorCode::E106 => write!(f, "E106"),
            ErrorCode::E107 => write!(f, "E107"),
            ErrorCode::E108 => write!(f, "E108"),
            ErrorCode::E109 => write!(f, "E109"),
            ErrorCode::E110 => write!(f, "E110"),
            ErrorCode::E201 => write!(f, "E201"),
            ErrorCode::E202 => write!(f, "E202"),
            ErrorCode::E203 => write!(f, "E203"),
            ErrorCode::E204 => write!(f, "E204"),
            ErrorCode::E205 => write!(f, "E205"),
            ErrorCode::E206 => write!(f, "E206"),
            ErrorCode::E207 => write!(f, "E207"),
            ErrorCode::E208 => write!(f, "E208"),
            ErrorCode::E209 => write!(f, "E209"),
            ErrorCode::E210 => write!(f, "E210"),
            ErrorCode::E301 => write!(f, "E301"),
            ErrorCode::E302 => write!(f, "E302"),
            ErrorCode::E303 => write!(f, "E303"),
            ErrorCode::E304 => write!(f, "E304"),
            ErrorCode::E305 => write!(f, "E305"),
            ErrorCode::E306 => write!(f, "E306"),
            ErrorCode::E401 => write!(f, "E401"),
            ErrorCode::E501 => write!(f, "E501"),
            ErrorCode::E601 => write!(f, "E601"),
            ErrorCode::E602 => write!(f, "E602"),
            ErrorCode::E603 => write!(f, "E603"),
            ErrorCode::E604 => write!(f, "E604"),
            ErrorCode::E611 => write!(f, "E611"),
            ErrorCode::E612 => write!(f, "E612"),
            ErrorCode::E621 => write!(f, "E621"),
            ErrorCode::E622 => write!(f, "E622"),
            ErrorCode::E623 => write!(f, "E623"),
            ErrorCode::E624 => write!(f, "E624"),
            ErrorCode::E625 => write!(f, "E625"),
            ErrorCode::E626 => write!(f, "E626"),
            ErrorCode::E627 => write!(f, "E627"),
            ErrorCode::E628 => write!(f, "E628"),
            ErrorCode::E631 => write!(f, "E631"),
            ErrorCode::E632 => write!(f, "E632"),
            ErrorCode::E633 => write!(f, "E633"),
            ErrorCode::E641 => write!(f, "E641"),
            ErrorCode::E642 => write!(f, "E642"),
            ErrorCode::E643 => write!(f, "E643"),
            ErrorCode::E644 => write!(f, "E644"),
            ErrorCode::E645 => write!(f, "E645"),
            ErrorCode::E646 => write!(f, "E646"),
            ErrorCode::E651 => write!(f, "E651"),
            ErrorCode::E652 => write!(f, "E652"),
            ErrorCode::E653 => write!(f, "E653"),
            ErrorCode::E654 => write!(f, "E654"),
            ErrorCode::E655 => write!(f, "E655"),
            ErrorCode::E656 => write!(f, "E656"),
            ErrorCode::E657 => write!(f, "E657"),
            ErrorCode::E658 => write!(f, "E658"),
            ErrorCode::W101 => write!(f, "W101"),
        }
    }
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

// Schema errors
implement_error_code!(E101, "unknown node type `{}`" => { node_type }, "check the schema field names or declare the node type" => {});
implement_error_code!(E102, "unknown edge type `{}`" => { edge_type }, "check the schema field names or declare the edge type" => {});
implement_error_code!(E103, "unknown vector type `{}`" => { vector_type }, "check the schema field names or declare the vector type" => {});
implement_error_code!(E105, "invalid identifier `{}`" => { identifier }, "check the identifier" => {});
implement_error_code!(E106, "use of undeclared node or vector type `{}` in schema" => { item_type_name }, "declare `{}` in the schema before using it in an edge" => { item_type_name });
implement_error_code!(E107, "duplicate {} definition `{}`" => { schema_type, name }, "rename the {} or remove the duplicate definition" => { schema_type });
implement_error_code!(E109, "duplicate field `{}` in {} `{}`" => { field_name, schema_type, schema_name }, "rename the field or remove the duplicate" => {});
implement_error_code!(E110, "`{}` is a reserved type name and cannot be used as a {} name" => { name, schema_type }, "rename the {} to something else" => { schema_type });

// Type errors
implement_error_code!(E201, "item type not in schema `{}`" => { item_type }, "check the schema field names" => {});
implement_error_code!(E202, 
    "given field `{}` is not a valid field for a given {} type `{}`" => { field_name, item_type, item_type_name  }, 
    "check the schema field names" => {});
implement_error_code!(E203, "cannot access properties on the type `{}`" => { type_name }, "ensure the type is a node, edge, or vector" => {});
implement_error_code!(E204, "field `{}` is a reserved field name" => { field_name }, "rename the field" => {});
implement_error_code!(E205, 
    "type of value `{}` is `{}`, which does not match field type `{}` for {} type `{}`" => { value, value_type, field_type, item_type, item_type_name }, 
    "change the value type to match the field type defined in the schema" => {});
implement_error_code!(E206, "invalid value type `{}`" => { value_type }, "use a literal or an identifier" => {});
implement_error_code!(E207, "edge type `{}` exists but it is not a valid edge type for the given {} type `{}`" => { edge_type, item_type, item_type_name }, "check the schema field names" => {});
implement_error_code!(E208, "field `{}` has not been indexed for node type `{}`" => { field_name, node_type }, "use a field that has been indexed with `INDEX` in the schema for node type `{}`" => { node_type });
implement_error_code!(E209, "unknown type `{}` for parameter `{}`" => { parameter_type, parameter_name }, "declare or use a matching schema object or use a primitive type" => {});
implement_error_code!(E210, "identifier `{}` was expected to be of type ID, but got {}" => { identifier, value_type_name }, "ensure the identifier is of type ID" => {});

// Query errors
implement_error_code!(E301, "variable `{}` not in scope" => { variable }, "check the variable" => {});
implement_error_code!(E302, "variable `{}` previously declared" => { variable }, "check the variable" => {});
implement_error_code!(E304, "missing {} type" => { item_type }, "add an {} type" => { item_type });
implement_error_code!(E305, "missing parameter `{}` for method `{}`" => { parameter_name, method_name }, "add the parameter `{}`" => { parameter_name });
implement_error_code!(E306, "expression should result in a boolean, instead got `{}`" => { expression_type }, "ensure the expression is a boolean" => {});

// MCP errors
implement_error_code!(E401, "MCP query must return a single value, but got `{}`" => { number_of_values }, "return a single value" => {});

// Conversion errors
implement_error_code!(E501, "invalid date `{}`" => { date }, "ensure the date conforms to the ISO 8601 or RFC 3339 formats" => {});

// Traversal errors
implement_error_code!(E601, "invalid traversal `{}`" => { traversal }, "ensure the traversal is valid" => {});
implement_error_code!(E602, "step `{}` is not valid given the previous step `{}`" => { step, previous_step }, "{}" => { reason });
implement_error_code!(E603, "`SearchV` must be used on a vector type, got `{}`, which is a `{}`" => { cur_ty, cur_ty_name }, "ensure the result of the previous step is a vector type" => {});
implement_error_code!(E604, "`UPDATE` step is only valid on nodes or edges, but got `{}`" => { step }, "use `UPDATE` on a node or edge or remove the `UPDATE` step" => {});
implement_error_code!(E611, "edge creation must have a to id" => {}, "add a `::To(target_node_id)` step to your edge creation" => {});
implement_error_code!(E612, "edge creation must have a from id" => {}, "add a `::From(source_node_id)` step to your edge creation" => {});

// Edge type errors
implement_error_code!(E621, "boolean comparison operation cannot be applied to given {} type `{}`" => { item_type, item_type_name }, "use a valid boolean comparison operation" => {});
implement_error_code!(E622, 
    "property `{}` of {} `{}` is of type `{}`, which does not match type of compared value which is of type `{}`" => { property_name, item_type, item_type_name, property_type, compared_value_type }, 
    "change the property type to match the compared value type" => {});
implement_error_code!(E623, "edge type `{}` does not have a node type as its `From` source" => { edge_type }, "set the `From` type of the edge to a node type" => {});
implement_error_code!(E624, "edge type `{}` does not have a node type as its `To` source" => { edge_type }, "set the `To` type of the edge to a node type" => {});
implement_error_code!(E625, "edge type `{}` does not have a vector type as its `From` source" => { edge_type }, "set the `From` type of the edge to a vector type" => {});
implement_error_code!(E626, "edge type `{}` does not have a vector type as its `To` source" => { edge_type }, "set the `To` type of the edge to a vector type" => {});
implement_error_code!(E627, "`{}` requires either a `from` or `to` parameter" => { step_name }, "add a `from` or `to` parameter to the step" => {});
implement_error_code!(E628, "`DROP` can only be applied to traversals, but got `{}`" => { expression_type }, "ensure the expression is a traversal" => {});

// Range errors
implement_error_code!(E631, "range must have a start and end, missing the `{}` value" => { start_or_end }, "add a `{}` value to the range" => { start_or_end });
implement_error_code!(E632, "range start must be less than range end, got `{}` which is larger than `{}`" => { start, end }, "change the range start to be less than the range end" => {});
implement_error_code!(E633, "index of range must be an integer, got `{}` which is of type `{}`" => { index, index_type }, "change {} to be an integer" => { index_type });

// Object remapping errors
implement_error_code!(E641, "closure is only valid as the last step in a traversal" => {}, "move the closure to the end of the traversal" => {});
implement_error_code!(E642, "object remapping is only valid as the last step in a traversal" => {}, "move the object remapping to the end of the traversal" => {});
implement_error_code!(E643, "field `{}` previously excluded" => { field_name }, "remove the `exclude` step for this field" => {});
implement_error_code!(E644, "`exclude` is only valid as the last step in a traversal, or as the step before an object remapping or closure" => {}, "move the `exclude` step to the end of the traversal or before the object remapping or closure" => {});
implement_error_code!(E645, "object remapping must have at least one field" => {}, "add at least one field to the object remapping" => {});
implement_error_code!(E646, "field value is empty" => {}, "field value must be a literal, identifier, traversal,or object" => {});

// For loop errors
implement_error_code!(E651, "`IN` variable `{}` is not iterable" => { in_variable }, "ensure the `in` variable is iterable" => {});
implement_error_code!(E652, "variable `{}` is not a field of the inner object of the `IN` variable `{}`" => { variable, in_variable }, "ensure `{}` is a field of `{}`" => { variable, in_variable });
implement_error_code!(E653, "inner object of `IN` variable `{}` is not an object" => { in_variable }, "ensure the inner type of `{}` is an object" => { in_variable });
implement_error_code!(E654, "object access syntax `{}.{}` in for loop is not yet supported" => { object_name, field_name }, "use object destructuring syntax `{{{}}}` instead" => { field_name });
implement_error_code!(E655, "internal analyzer error: {}" => { message }, "this is a bug in the analyzer, please report it" => {});
implement_error_code!(E656, "unsupported type conversion from `{}`" => { type_name }, "use a supported type" => {});
implement_error_code!(E657, "step `{}` requires a previous step but none was found" => { step_name }, "ensure this step follows a property access" => {});
implement_error_code!(E658, "field `{}` not found in object type" => { field_name }, "check the field name or use a valid field" => {});

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
