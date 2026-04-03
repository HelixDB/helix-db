## **Helix Engine Tests**

### **Traversal Tests** (`helix-db/src/helix_engine/tests/traversal_tests/`)

#### **Util Traversal Tests** (`util_tests.rs`)
- `test_order_by_asc` - Tests ascending order functionality
- `test_order_by_desc` - Tests descending order functionality

#### **Remapping Tests** (`remapping_tests.rs`)
- `test_exclude_field_remapping` - Tests field exclusion in remapping
- `test_field_remapping` - Tests basic field remapping
- `test_identifier_remapping` - Tests identifier remapping
- `test_traversal_remapping` - Tests traversal remapping
- `test_value_remapping` - Tests value remapping
- `test_exists_remapping` - Tests exists clause remapping
- `test_one_of_each_remapping` - Tests combined remapping scenarios
- `test_nested_remapping` - Tests single-level nested remapping
- `test_double_nested_remapping` - Tests double-nested remapping
- `test_nested_with_other_remapping` - Tests nested remapping with other operations

#### **Vector Traversal Tests** (`vector_traversal_tests.rs`)
- `test_from_v` - Tests vector source traversal
- `test_to_v` - Tests vector destination traversal
- `test_brute_force_vector_search` - Tests brute force vector search
- `test_order_by_desc` - Tests vector ordering by descending
- `test_vector_search` - Tests general vector search functionality
- `test_delete_vector` - Tests vector deletion
- `test_drop_vectors_then_add_them_back` - Tests vector drop and re-add operations

#### **Update Tests** (`update_tests.rs`)
- `test_update_node` - Tests node update operations

#### **Shortest Path Tests** (`shortest_path_tests.rs`)
- `test_shortest_path` - Tests shortest path finding algorithm

#### **Secondary Index Tests** (`secondary_index_tests.rs`)
- `test_delete_node_with_secondary_index` - Tests node deletion with secondary indices
- `test_update_of_secondary_indices` - Tests updating secondary indices

#### **Range Tests** (`range_tests.rs`)
- `test_range_subset` - Tests range subset operations
- `test_range_chaining` - Tests chaining range operations
- `test_range_empty` - Tests empty range handling

#### **Drop Tests** (`drop_tests.rs`)
- `test_drop_edge` - Tests edge dropping
- `test_drop_node` - Tests node dropping
- `test_drop_traversal` - Tests traversal dropping
- `test_node_deletion_in_existing_graph` - Tests node deletion in populated graphs
- `test_edge_deletion_in_existing_graph` - Tests edge deletion in populated graphs
- `test_vector_deletion_in_existing_graph` - Tests vector deletion in populated graphs

#### **Edge Traversal Tests** (`edge_traversal_tests.rs`)
- `test_add_e` - Tests edge addition
- `test_out_e` - Tests outgoing edge traversal
- `test_in_e` - Tests incoming edge traversal
- `test_in_n` - Tests incoming node traversal
- `test_out_n` - Tests outgoing node traversal
- `test_edge_properties` - Tests edge property handling
- `test_e_from_id` - Tests edge retrieval by ID
- `test_e_from_id_nonexistent` - Tests non-existent edge retrieval
- `test_e_from_id_chain_operations` - Tests chained operations on edge retrieval
- `test_add_e_between_node_and_vector` - Tests edge creation between nodes and vectors

#### **Node Traversal Tests** (`node_traversal_tests.rs`)
- `test_add_n` - Tests node addition
- `test_out` - Tests outgoing traversal from nodes
- `test_in` - Tests incoming traversal to nodes
- `test_complex_traversal` - Tests complex multi-step traversals
- `test_n_from_id` - Tests node retrieval by ID
- `test_n_from_id_with_traversal` - Tests node retrieval with traversal
- `test_n_from_id_nonexistent` - Tests non-existent node retrieval
- `test_n_from_id_chain_operations` - Tests chained operations on node retrieval
- `test_with_id_type` - Tests node operations with ID types
- `test_double_add_and_double_fetch` - Tests duplicate additions and fetches

#### **Count Tests** (`count_tests.rs`)
- `test_count_single_node` - Tests counting single nodes
- `test_count_node_array` - Tests counting node arrays
- `test_count_mixed_steps` - Tests counting with mixed traversal steps
- `test_count_empty` - Tests counting empty results

#### **Filter Tests** (`filter_tests.rs`)
- `test_filter_nodes` - Tests node filtering
- `test_filter_macro_single_argument` - Tests filter macro with single argument
- `test_filter_macro_multiple_arguments` - Tests filter macro with multiple arguments
- `test_filter_edges` - Tests edge filtering
- `test_filter_empty_result` - Tests filtering with empty results
- `test_filter_chain` - Tests chained filter operations

---

### **Core Vector Tests** (`helix-db/src/helix_engine/tests/vector_tests.rs`)
- `test_hvector_new` - Tests vector creation
- `test_hvector_from_slice` - Tests vector creation from slice
- `test_hvector_distance_orthogonal` - Tests orthogonal vector distance
- `test_hvector_distance_min` - Tests minimum distance calculation
- `test_hvector_distance_max` - Tests maximum distance calculation
- `test_bytes_roundtrip` - Tests byte serialization roundtrip
- `test_hvector_len` - Tests vector length
- `test_hvector_is_empty` - Tests empty vector check
- `test_hvector_mismatched_dimensions` - Tests mismatched dimension handling (panic test)
- `test_hvector_large_values` - Tests vectors with large values
- `test_hvector_negative_values` - Tests vectors with negative values
- `test_hvector_cosine_similarity` - Tests cosine similarity calculation

### **HNSW Tests** (`helix-db/src/helix_engine/tests/hnsw_tests.rs`)
- `tests_hnsw_config_build` - Tests HNSW configuration building
- `test_hnsw_insert` - Tests HNSW insertion operations
- `test_get_vector` - Tests vector retrieval from HNSW
- `test_hnsw_search` - Tests HNSW search functionality
- `test_hnsw_search_property_ordering` - Tests search with property ordering
- `test_hnsw_search_filter_ordering` - Tests search with filter ordering
- `test_hnsw_delete` - Tests HNSW deletion operations

### **BM25 Tests** (`helix-db/src/helix_engine/bm25/bm25_tests.rs`)
- `test_tokenize_with_filter` - Tests tokenization with filtering
- `test_tokenize_without_filter` - Tests tokenization without filtering
- `test_tokenize_edge_cases_punctuation_only` - Tests edge case tokenization
- `test_insert_document` - Tests document insertion
- `test_insert_multiple_documents` - Tests multiple document insertion
- `test_search_single_term` - Tests single term search
- `test_search_multiple_terms` - Tests multiple term search
- `test_search_many_terms` - Tests search with many terms
- `test_bm25_score_calculation` - Tests BM25 score calculation
- `test_update_document` - Tests document updates
- `test_delete_document` - Tests document deletion
- `test_search_with_limit` - Tests search with result limits
- `test_search_no_results` - Tests search with no results
- `test_edge_cases_empty_document` - Tests empty document handling
- `test_hybrid_search` - Tests hybrid search functionality (async)
- `test_hybrid_search_alpha_vectors` - Tests hybrid search with vector emphasis (async)
- `test_hybrid_search_alpha_bm25` - Tests hybrid search with BM25 emphasis (async)
- `test_bm25_score_properties` - Tests BM25 score properties
- `test_metadata_consistency` - Tests metadata consistency

### **Storage Core Tests** (`helix-db/src/helix_engine/storage_core/version_info.rs`)
- `test_field_renaming` - Tests field renaming in storage
- `test_field_type_cast` - Tests field type casting
- `test_field_addition_from_value` - Tests field addition from values

### **Embedding Provider Tests** (`helix-db/src/helix_gateway/embedding_providers/embedding_providers.rs`)
- `test_openai_embedding_success` - Tests OpenAI embedding success
- `test_gemini_embedding_success` - Tests Gemini embedding success
- `test_gemini_embedding_with_task_type` - Tests Gemini embedding with task types
- `test_minimax_embedding_success` - Tests MiniMax embedding success
- `test_minimax_embedding_with_query_type` - Tests MiniMax embedding with query type
- `test_local_embedding_success` - Tests local embedding success
- `test_local_embedding_invalid_url` - Tests invalid URL handling for local embeddings

## **Util Tests** (`helix-db/src/protocol/date.rs`)
- Date tests:
    - `test_naive_date_serialization` - Tests naive date serialization
    - `test_naive_date_deserialization` - Tests naive date deserialization
    - `test_timestamp_serialization` - Tests timestamp serialization
    - `test_timestamp_deserialization` - Tests timestamp deserialization
    - `test_rfc3339_serialization` - Tests RFC3339 date serialization
    - `test_rfc3339_deserialization` - Tests RFC3339 date deserialization
- ID tests:
    - `test_uuid_deserialization` - Tests UUID deserialization
    - `test_uuid_serialization` - Tests UUID serialization

