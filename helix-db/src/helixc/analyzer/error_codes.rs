#[allow(dead_code)]
pub(crate) enum ErrorCode {
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

    // TYPE ERRORS
    /// `E202` – `field not in schema`
    E202,
    /// `E204` – `field is a reserved field name`
    E204,
    /// `E205` – `type of value does not match field type in schema for a given item type`
    E205,
    /// `E207` – `edge type exists but it is not a valid edge type for the given item type`
    E207,
    /// `E208` - `SearchV must be used on a vector type`
    E208,

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
    /// `E603` - `invalid step`
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

    /// `E651` - `in variable is not iterable`
    E651,
    /// `E652` - `variable is not a field of the inner type of the in variable`
    E652,
    /// `E653` - `inner type of in variable is not an object`
    E653,
}

#[allow(dead_code)]
pub(crate) enum WarningCode {
    /// `W101` - `query has no return`
    W101,
}

#[allow(dead_code)]
pub(crate) enum InfoCode {}
