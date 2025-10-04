// Test queries for all data types

// String parameter (baseline)
QUERY testString(value: String) =>
    node <- N<TestNode>({str_field: value})
    RETURN node

// Integer types with parameters
QUERY testI8(value: I8) =>
    node <- N<TestNode>({i8_field: value})
    RETURN node
    
QUERY testI32(value: I32) =>
    node <- N<TestNode>({i32_field: value})
    RETURN node
    
QUERY testI64(value: I64) =>
    node <- N<TestNode>({i64_field: value})
    RETURN node

// Unsigned integer types with parameters
QUERY testU8(value: U8) =>
    node <- N<TestNode>({u8_field: value})
    RETURN node
    
QUERY testU32(value: U32) =>
    node <- N<TestNode>({u32_field: value})
    RETURN node
    
QUERY testU64(value: U64) =>
    node <- N<TestNode>({u64_field: value})
    RETURN node

// Float types with parameters
QUERY testF32(value: F32) =>
    node <- N<TestNode>({f32_field: value})
    RETURN node
    
QUERY testF64(value: F64) =>
    node <- N<TestNode>({f64_field: value})
    RETURN node

// Boolean type with parameter
QUERY testBoolean(value: Boolean) =>
    node <- N<TestNode>({bool_field: value})
    RETURN node

// Test with literal values
QUERY testLiterals() =>
    str_node <- N<TestNode>({str_field: "test"})
    i32_node <- N<TestNode>({i32_field: 42})
    u32_node <- N<TestNode>({u32_field: 100})
    f64_node <- N<TestNode>({f64_field: 3.14})
    bool_true <- N<TestNode>({bool_field: true})
    bool_false <- N<TestNode>({bool_field: false})
    RETURN str_node, i32_node, u32_node, f64_node, bool_true, bool_false

// Test multiple conditions
QUERY testMultipleConditions(name: String, age: U32, active: Boolean) =>
    // Note: Current HQL doesn't support multiple field filters in a single N<> call
    // This would need to be implemented as separate lookups with intersection
    nodes_by_name <- N<TestNode>({str_field: name})
    nodes_by_age <- N<TestNode>({u32_field: age})
    nodes_by_active <- N<TestNode>({bool_field: active})
    RETURN nodes_by_name, nodes_by_age, nodes_by_active