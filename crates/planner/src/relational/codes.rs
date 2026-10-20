//! Cypher error codes.
//!
//! A [`super::QueryError`] reports one category and one detail from these
//! lists, in the lower snake case of every HelixDB error code (native codes
//! include `index_not_found`). Transports send them unchanged.
//!
//! Codes are strings, not enums, so a transport passes on a code it does not
//! know, such as one that a newer database node added.
//!
//! ```
//! use helix_planner::relational::{category, detail, QueryError};
//! let error = QueryError::runtime(category::RESOURCE_LIMIT, detail::MEMORY_LIMIT, "over budget");
//! assert_eq!(error.code(), "resource_limit:runtime:memory_limit");
//! ```

/// Declares one constant per code and `ALL`, every code of the group.
macro_rules! codes {
    ($($(#[$meta:meta])* $name:ident = $code:literal,)+) => {
        $($(#[$meta])* pub const $name: &str = $code;)+

        /// Every code in this group, in declaration order.
        pub const ALL: &[&str] = &[$($code,)+];
    };
}

/// What kind of failure a query error reports. Transports classify an error by
/// its category; see `helix_cypher::api::ErrorClass`.
pub mod category {
    codes! {
        /// The statement is not valid Cypher. Like the openCypher TCK, this
        /// includes semantic errors such as an undefined variable.
        SYNTAX_ERROR = "syntax_error",
        /// A value has the wrong type for its use.
        TYPE_ERROR = "type_error",
        /// A function argument is invalid.
        ARGUMENT_ERROR = "argument_error",
        /// Arithmetic failed, such as a division by zero.
        ARITHMETIC_ERROR = "arithmetic_error",
        /// A statement referenced a parameter the request did not supply.
        PARAMETER_MISSING = "parameter_missing",
        /// A statement read a node or relationship that it had deleted.
        ENTITY_NOT_FOUND = "entity_not_found",
        /// A statement broke a graph constraint, such as deleting a node that
        /// still has relationships.
        CONSTRAINT_VERIFICATION_FAILED = "constraint_verification_failed",
        /// Valid openCypher outside the supported profile.
        UNSUPPORTED_FEATURE = "unsupported_feature",
        /// A per-query budget was exceeded. A modifying statement committed nothing.
        RESOURCE_LIMIT = "resource_limit",
        /// A modifying statement reached a reader or a warm-only request.
        ACCESS_MODE_ERROR = "access_mode_error",
        /// The planner broke one of its own invariants.
        INTERNAL_PLANNER_ERROR = "internal_planner_error",
    }
}

/// The specific condition within a category. A detail can occur in more than
/// one category, such as `invalid_argument_type`.
pub mod detail {
    codes! {
        AGGREGATE_ARITY = "aggregate_arity",
        AGGREGATE_IDENTITY_BUDGET = "aggregate_identity_budget",
        AMBIGUOUS_AGGREGATION_EXPRESSION = "ambiguous_aggregation_expression",
        COLLECTION_LIMIT = "collection_limit",
        COLUMN_NAME_CONFLICT = "column_name_conflict",
        DELETE_CONNECTED_NODE = "delete_connected_node",
        DELETED_ENTITY_ACCESS = "deleted_entity_access",
        DIVISION_BY_ZERO = "division_by_zero",
        EMPTY_LABEL = "empty_label",
        EMPTY_PROPERTY_NAME = "empty_property_name",
        EMPTY_RELATIONSHIP_TYPE = "empty_relationship_type",
        ENTITY_KIND_MISMATCH = "entity_kind_mismatch",
        EXPECTED_ENTITY = "expected_entity",
        EXPECTED_MAP = "expected_map",
        EXPECTED_NODE = "expected_node",
        EXPECTED_RELATIONSHIP = "expected_relationship",
        EXPRESSION_DEPTH = "expression_depth",
        FLOATING_POINT_OVERFLOW = "floating_point_overflow",
        FOREACH = "foreach",
        /// A known openCypher function outside the supported profile. The
        /// message names it.
        FUNCTION = "function",
        FUNCTION_ARITY = "function_arity",
        INTEGER_OVERFLOW = "integer_overflow",
        INVALID_ACCESS_PLAN = "invalid_access_plan",
        INVALID_ACCESS_RESULT = "invalid_access_result",
        INVALID_AGGREGATION = "invalid_aggregation",
        INVALID_ARGUMENT_TYPE = "invalid_argument_type",
        INVALID_CASE = "invalid_case",
        INVALID_CLAUSE_COMPOSITION = "invalid_clause_composition",
        INVALID_DELETE = "invalid_delete",
        INVALID_LIMITS = "invalid_limits",
        INVALID_NODE_SLOT = "invalid_node_slot",
        INVALID_NUMBER_LITERAL = "invalid_number_literal",
        INVALID_NUMBER_OF_ARGUMENTS = "invalid_number_of_arguments",
        INVALID_PARAMETER = "invalid_parameter",
        INVALID_PARAMETER_USE = "invalid_parameter_use",
        INVALID_PATH = "invalid_path",
        INVALID_PATTERN_EQUALITY = "invalid_pattern_equality",
        INVALID_PATTERN_LOOKUP = "invalid_pattern_lookup",
        INVALID_PATTERN_ORDER = "invalid_pattern_order",
        INVALID_PROPERTY_TYPE = "invalid_property_type",
        INVALID_RELATIONSHIP_PATTERN = "invalid_relationship_pattern",
        INVALID_RELATIONSHIP_SLOT = "invalid_relationship_slot",
        INVALID_SCHEMA = "invalid_schema",
        INVALID_SLOT = "invalid_slot",
        INVALID_STATEMENT = "invalid_statement",
        INVALID_UNICODE_CHARACTER = "invalid_unicode_character",
        INVALID_UNICODE_LITERAL = "invalid_unicode_literal",
        LABEL_MUTATION = "label_mutation",
        LIST_COMPREHENSION = "list_comprehension",
        LIST_ELEMENT_ACCESS_BY_NON_INTEGER = "list_element_access_by_non_integer",
        LOAD_CSV = "load_csv",
        MAP_ELEMENT_ACCESS_BY_NON_STRING = "map_element_access_by_non_string",
        MAP_PROJECTION_OR_SUBQUERY = "map_projection_or_subquery",
        MEMORY_LIMIT = "memory_limit",
        MERGE = "merge",
        MISSING_PARAMETER = "missing_parameter",
        MULTIPLE_NODE_LABELS = "multiple_node_labels",
        MULTIPLE_STATEMENTS = "multiple_statements",
        NAMESPACED_FUNCTION = "namespaced_function",
        NEGATIVE_INTEGER_ARGUMENT = "negative_integer_argument",
        NESTED_AGGREGATION = "nested_aggregation",
        NO_EXPRESSION_ALIAS = "no_expression_alias",
        NO_RELATIONSHIP_TYPE = "no_relationship_type",
        NO_SINGLE_RELATIONSHIP_TYPE = "no_single_relationship_type",
        NO_VARIABLES_IN_SCOPE = "no_variables_in_scope",
        NODE_LABEL_REQUIRED = "node_label_required",
        NON_CONSTANT_EXPRESSION = "non_constant_expression",
        NUMBER_OUT_OF_RANGE = "number_out_of_range",
        PATTERN_COMPREHENSION = "pattern_comprehension",
        PATTERN_EXPRESSION = "pattern_expression",
        PATTERN_PARAMETER_MAP = "pattern_parameter_map",
        PHYSICAL_PLANNING = "physical_planning",
        PLAN_SIZE = "plan_size",
        PROCEDURES_AND_SUBQUERIES = "procedures_and_subqueries",
        QUANTIFIED_LIST_EXPRESSION = "quantified_list_expression",
        QUERY_TOO_LARGE = "query_too_large",
        RELATIONSHIP_UNIQUENESS_VIOLATION = "relationship_uniqueness_violation",
        REQUIRES_DIRECTED_RELATIONSHIP = "requires_directed_relationship",
        RESERVED_PROPERTY_NAME = "reserved_property_name",
        RESULT_LIMIT = "result_limit",
        SCHEMA_DDL = "schema_ddl",
        SHORTEST_PATH = "shortest_path",
        STORED_VALUE_NESTING_LIMIT = "stored_value_nesting_limit",
        STORED_VALUE_TYPE = "stored_value_type",
        TOO_MANY_BINDINGS = "too_many_bindings",
        TOO_MANY_TOKENS = "too_many_tokens",
        UNBOUND_SLOT = "unbound_slot",
        UNDEFINED_VARIABLE = "undefined_variable",
        UNEXPECTED_END = "unexpected_end",
        UNEXPECTED_SYNTAX = "unexpected_syntax",
        UNION = "union",
        UNKNOWN_FUNCTION = "unknown_function",
        UNTYPED_STORED_RELATIONSHIP = "untyped_stored_relationship",
        VALUE_DEPTH = "value_depth",
        VARIABLE_ALREADY_BOUND = "variable_already_bound",
        VARIABLE_LENGTH_PATTERN = "variable_length_pattern",
        VARIABLE_TYPE_CONFLICT = "variable_type_conflict",
        WRITER_REQUIRED = "writer_required",
    }
}
