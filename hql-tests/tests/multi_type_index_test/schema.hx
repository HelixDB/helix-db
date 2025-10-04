// Comprehensive test for multi-type secondary indexing

N::TestNode {
    // String indexing (baseline - already works)
    INDEX str_field: String,
    
    // Integer types
    INDEX i8_field: I8,
    INDEX i16_field: I16,
    INDEX i32_field: I32,
    INDEX i64_field: I64,
    
    // Unsigned integer types  
    INDEX u8_field: U8,
    INDEX u16_field: U16,
    INDEX u32_field: U32,
    INDEX u64_field: U64,
    INDEX u128_field: U128,
    
    // Float types
    INDEX f32_field: F32,
    INDEX f64_field: F64,
    
    // Boolean type
    INDEX bool_field: Boolean,
    
    // Non-indexed fields for comparison
    extra_string: String,
    extra_number: I32,
}