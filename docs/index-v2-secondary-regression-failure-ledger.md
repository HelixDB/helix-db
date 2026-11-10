# Index V2 secondary correctness resolution ledger

The red checkpoint was committed before production changes in `089baba1` and
`4b91906f`. Every retained regression now passes without an ignore, panic
expectation, incorrect snapshot, or production workaround.

This remediation changes the storage-version-2 secondary encoding in place.
Databases written by the previous V2 secondary encoding are incompatible and
must be deleted and recreated. There is intentionally no V3, version bump,
V2-to-V3 migration, dual read, compatibility decoder, or shadow keyspace.

## Equality

| Test | Defect captured |
| --- | --- |
| `regression_secondary_equality_canonicalization_distinguishes_bool_and_string` | `Bool(true)` and `String("true")` collapse to one identity. |
| `regression_secondary_equality_canonicalization_distinguishes_integer_and_string` | `I64(42)` and its padded string representation collapse. |
| `regression_secondary_equality_canonicalization_distinguishes_same_length_arrays` | Array contents are discarded; only type and length remain. |
| `regression_secondary_equality_canonicalization_distinguishes_bytes_and_string` | Bytes and their debug-format string collapse. |
| `regression_secondary_equality_canonicalization_distinguishes_null_and_string` | Null and `String("null")` collapse. |
| `regression_secondary_equality_canonicalization_matches_semantic_numeric_equality` | Cross-numeric equality and signed zero do not share an index identity. |
| `regression_equality_lookup_exactly_filters_a_shared_digest_bucket` | A node digest collision returns a false positive without exact-value filtering. |
| `regression_edge_equality_lookup_exactly_filters_a_shared_digest_bucket` | An edge digest collision returns a false positive without exact-value filtering. |
| `regression_equality_lookup_matches_cross_numeric_full_scan_semantics` | Indexed `I64(42) = F64(42.0)` disagrees with the full-scan predicate. |
| `regression_equality_lookup_treats_positive_and_negative_zero_as_equal` | Indexed `-0.0 = +0.0` disagrees with the full-scan predicate. |
| `regression_equality_lookup_keeps_nan_non_reflexive_like_a_full_scan` | An indexed NaN equals its own digest although the full-scan predicate is false. |
| `regression_equality_distinguishes_distinct_same_length_arrays` | Distinct arrays in one length bucket are returned together. |
| `regression_unique_equality_allows_distinct_typed_values_with_the_same_text` | Distinct typed values produce a false uniqueness violation. |
| `regression_unique_equality_uses_exact_cross_numeric_semantics_above_two_to_the_53` | Unique enforcement fails to conflict for the exactly equal integer/float pair at `2^53`. |

## Range

| Test | Defect captured |
| --- | --- |
| `regression_secondary_range_encoding_handles_integer_extremes_and_sign_transitions` | Text encoding does not preserve signed integer order. |
| `regression_secondary_range_encoding_handles_float_boundaries` | Text encoding does not preserve negative, exponent-transition, adjacent, or infinite float order. |
| `regression_secondary_range_i64_encoding_is_monotonic` | Property testing shrinks the integer defect to `-1` versus `-2`. |
| `regression_secondary_range_f64_encoding_is_monotonic` | Property testing finds finite/non-NaN float order inversions. |
| `regression_ascending_node_range_matches_typed_oracle_across_signed_extremes` | Ascending node results disagree with typed `i64` order. |
| `regression_descending_node_range_matches_typed_oracle_across_signed_extremes` | Descending node results do not reverse typed `i64` order. |
| `regression_signed_node_range_bounds_and_limit_match_typed_oracle` | Lexical bounds admit/order the wrong rows before `LIMIT`. |
| `regression_edge_range_matches_typed_oracle_for_negative_values` | Edge range ordering has the same signed-value defect. |
| `regression_node_range_remains_correct_after_reopen` | Persisted range rows remain semantically misordered after reopen. |
| `regression_secondary_range_ascending_values_are_prefix_framed_before_entity_ids` | Ascending raw values are not prefix-framed, so the entity ID of `""`, `"a"`, and other prefixes changes value ordering. |
| `regression_exclusive_prefix_end_includes_every_key_with_the_prefix` | Appending `0xFF` excludes `P + FF` and every longer key beginning with it. |

## Property identity and exact numeric semantics

| Test | Defect captured |
| --- | --- |
| `regression_property_hash_collision_fixture_is_exact` | Passing fixture guard for the known `User␟property_16755` / `User␟property_36911` 32-bit collision. |
| `regression_colliding_property_names_keep_independent_managed_node_and_edge_indexes` | Passing guard that full managed identities isolate node and edge indexes despite the legacy collision. |
| `regression_node_dynamic_equality_never_falls_back_to_colliding_legacy_property_rows` | Absent canonical node metadata incorrectly falls back to the colliding 32-bit legacy row. |
| `regression_edge_dynamic_equality_never_falls_back_to_colliding_legacy_property_rows` | Absent canonical edge metadata incorrectly falls back to the colliding 32-bit legacy row. |
| `regression_dynamic_range_never_falls_back_to_colliding_legacy_property_rows` | Node and edge range execution incorrectly serves colliding legacy rows when canonical metadata is absent. |
| `regression_exact_numeric_semantics_do_not_round_i64_through_f64` | `9_007_199_254_740_993i64` is rounded to `2^53` for equality and comparison. |

## Disabled lifecycle

| Test | Defect captured |
| --- | --- |
| `disabled_secondary_worker_never_hangs_writer_open` | Legacy definition convergence previously waited forever after accepting work while the secondary worker was disabled; the regression enforces a strict timeout. |

`process_secondary_index_lifecycle_once` now uses the installed production
driver, normal observation/claim/fencing/recovery/commit path, configured
limits, a shared checked claim-sequence allocator, and serialized explicit
steps. The lifecycle contract matrix covers CREATE, DROP, blocked work, retry,
abort, delayed work, close, crash/reopen recovery, and concurrent claims.

## Independent oracle

`secondary_oracle_exact_numeric_and_typed_equality_are_independent`,
`secondary_oracle_uses_typed_equality_bounds_direction_and_limit`, and
`deterministic_secondary_workload_spans_lifecycle_mutation_drop_and_reopen`
pass. The oracle owns its numeric decomposition and typed comparison logic and
does not import production comparison or encoding code.

## Resolution

| Defect | Resolution |
| --- | --- |
| E1 | Closed typed equality projections and exact canonical bytes make type and array contents part of identity. Digests only narrow scans; canonical bytes and authoritative graph values decide matches and uniqueness. Null uses an authoritative scan and NaN is non-reflexive. |
| E2 | `CanonicalNumber` provides exact cross-variant numeric order without integer-to-float conversion. Range values use one typed total order: numeric, datetime, string. Bounds are applied before `LIMIT`, and stale candidates do not consume the limit. |
| E3 | Range payloads are self-delimiting before the entity ID. Prefix-related strings remain ordered and equal values use the entity ID as the deterministic tie-breaker. |
| E4 | Prefix scans use a true lexicographic successor that increments the final non-`0xFF` byte and truncates, with all-`0xFF` represented as unbounded. |
| E5 | Dynamic secondary access resolves the full canonical `IndexIdentity`; equality and range serving never fall back to 32-bit property-hash rows. |
| E6 | Integer, `f32`, and `f64` values are decomposed from their bits into exact sign, exponent, and odd significand components. `2^53 + 1` remains distinct from `2^53`, while exactly equal integer/float pairs normalize together. |

Unsupported or oversized equality/range values now produce typed errors.
Builds block durably; active graph mutations fail atomically. Planner null
literals remain typed while missing-index diagnostics suppress null-only index
recommendations.

## Validation

```bash
cargo test --workspace
cargo clippy --workspace -- -D warnings
cargo fmt --all -- --check
rustfmt --edition 2024 --check <every changed Rust file>
git diff --check
```

The full workspace test run, focused production lifecycle contracts,
independent oracle tests, and all doctests pass. Storage version remains
exactly `0x0002`.
