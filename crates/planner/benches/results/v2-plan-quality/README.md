# V2 planner benchmark comparison

Report-only results from ten interleaved baseline/candidate runs on Darwin arm64. Each binary run performs three warmups and ten measured plans per case.

| Population | Case | Baseline shape | Candidate shape | p50 planning delta | Baseline costed I/O | Candidate costed I/O | Allocations/plan |
|---:|---|---|---|---:|---|---|---:|
| 1000 | edge_equality | legacy_equality_range | bitmap_equality_point | +4.2% | get=0, multi=0, seek=1, next=100 | get=1, multi=0, seek=0, next=0, auth=0 | 369 → 377 |
| 1000 | edge_equality_ordered_range | legacy_row_merge_sort | ordered_range_bitmap_filter | +8.2% | get=0, multi=0, seek=2, next=600 | get=501, multi=0, seek=1, next=500, auth=500 | 580 → 593 |
| 1000 | edge_multi_index_intersection | legacy_equality_range_intersection | bitmap_intersection | -2.0% | get=0, multi=0, seek=2, next=300 | get=2, multi=0, seek=0, next=0, auth=0 | 544 → 542 |
| 1000 | edge_range_ascending_limit | legacy_ordered_range | ordered_range_verified_scan | +1.5% | get=0, multi=0, seek=1, next=500 | get=500, multi=0, seek=1, next=500, auth=500 | 411 → 414 |
| 1000 | edge_range_descending_limit | legacy_ordered_range | ordered_range_verified_scan | +1.6% | get=0, multi=0, seek=1, next=500 | get=500, multi=0, seek=1, next=500, auth=500 | 413 → 416 |
| 1000 | edge_same_index_union | legacy_equality_range_union | batched_bitmap_equality | +1.1% | get=0, multi=0, seek=2, next=200 | get=2, multi=1, seek=0, next=0, auth=0 | 542 → 546 |
| 1000 | node_equality | legacy_equality_range | bitmap_equality_point | +7.5% | get=0, multi=0, seek=1, next=100 | get=1, multi=0, seek=0, next=0, auth=0 | 369 → 377 |
| 1000 | node_equality_ordered_range | legacy_row_merge_sort | ordered_range_bitmap_filter | +8.0% | get=0, multi=0, seek=2, next=600 | get=501, multi=0, seek=1, next=500, auth=500 | 580 → 593 |
| 1000 | node_multi_index_intersection | legacy_equality_range_intersection | bitmap_intersection | -2.8% | get=0, multi=0, seek=2, next=300 | get=2, multi=0, seek=0, next=0, auth=0 | 544 → 542 |
| 1000 | node_range_ascending_limit | legacy_ordered_range | ordered_range_verified_scan | +2.0% | get=0, multi=0, seek=1, next=500 | get=500, multi=0, seek=1, next=500, auth=500 | 411 → 414 |
| 1000 | node_range_descending_limit | legacy_ordered_range | ordered_range_verified_scan | +2.0% | get=0, multi=0, seek=1, next=500 | get=500, multi=0, seek=1, next=500, auth=500 | 413 → 416 |
| 1000 | node_same_index_union | legacy_equality_range_union | batched_bitmap_equality | +1.9% | get=0, multi=0, seek=2, next=200 | get=2, multi=1, seek=0, next=0, auth=0 | 542 → 546 |
| 1000 | node_unique_equality | legacy_unique_equality_range | unique_equality_verified_point | +2.1% | get=0, multi=0, seek=1, next=1 | get=2, multi=0, seek=0, next=0, auth=1 | 369 → 374 |
| 10000 | edge_equality | legacy_equality_range | bitmap_equality_point | +3.1% | get=0, multi=0, seek=1, next=1000 | get=1, multi=0, seek=0, next=0, auth=0 | 369 → 377 |
| 10000 | edge_equality_ordered_range | legacy_row_merge_sort | ordered_range_bitmap_filter | +6.5% | get=0, multi=0, seek=2, next=6000 | get=5001, multi=0, seek=1, next=5000, auth=5000 | 580 → 593 |
| 10000 | edge_multi_index_intersection | legacy_equality_range_intersection | bitmap_intersection | -1.0% | get=0, multi=0, seek=2, next=3000 | get=2, multi=0, seek=0, next=0, auth=0 | 544 → 542 |
| 10000 | edge_range_ascending_limit | legacy_ordered_range | ordered_range_verified_scan | +2.5% | get=0, multi=0, seek=1, next=5000 | get=5000, multi=0, seek=1, next=5000, auth=5000 | 411 → 414 |
| 10000 | edge_range_descending_limit | legacy_ordered_range | ordered_range_verified_scan | +1.5% | get=0, multi=0, seek=1, next=5000 | get=5000, multi=0, seek=1, next=5000, auth=5000 | 413 → 416 |
| 10000 | edge_same_index_union | legacy_equality_range_union | batched_bitmap_equality | +1.4% | get=0, multi=0, seek=2, next=2000 | get=2, multi=1, seek=0, next=0, auth=0 | 542 → 546 |
| 10000 | node_equality | legacy_equality_range | bitmap_equality_point | +3.5% | get=0, multi=0, seek=1, next=1000 | get=1, multi=0, seek=0, next=0, auth=0 | 369 → 377 |
| 10000 | node_equality_ordered_range | legacy_row_merge_sort | ordered_range_bitmap_filter | +9.0% | get=0, multi=0, seek=2, next=6000 | get=5001, multi=0, seek=1, next=5000, auth=5000 | 580 → 593 |
| 10000 | node_multi_index_intersection | legacy_equality_range_intersection | bitmap_intersection | -1.4% | get=0, multi=0, seek=2, next=3000 | get=2, multi=0, seek=0, next=0, auth=0 | 544 → 542 |
| 10000 | node_range_ascending_limit | legacy_ordered_range | ordered_range_verified_scan | +1.9% | get=0, multi=0, seek=1, next=5000 | get=5000, multi=0, seek=1, next=5000, auth=5000 | 411 → 414 |
| 10000 | node_range_descending_limit | legacy_ordered_range | ordered_range_verified_scan | +2.8% | get=0, multi=0, seek=1, next=5000 | get=5000, multi=0, seek=1, next=5000, auth=5000 | 413 → 416 |
| 10000 | node_same_index_union | legacy_equality_range_union | batched_bitmap_equality | +0.1% | get=0, multi=0, seek=2, next=2000 | get=2, multi=1, seek=0, next=0, auth=0 | 542 → 546 |
| 10000 | node_unique_equality | legacy_unique_equality_range | unique_equality_verified_point | +3.4% | get=0, multi=0, seek=1, next=1 | get=2, multi=0, seek=0, next=0, auth=1 | 369 → 374 |

## Interpretation

- Non-unique equality and equality sets move from range iteration to V4 bitmap point reads, including one multi-get for same-index unions.
- Equality intersections combine verified IDs before the final row materialization.
- Mixed equality/range cases use the ordered range as the driver, filter verified bitmap IDs, and no longer require an explicit sort.
- Unique equality and range candidates retain authoritative graph verification; non-unique bitmap IDs do not add a graph existence read.
- I/O columns are the selected plan's storage-cost components; functional executor tests separately assert observed point, multi-get, and graph-read counts.
- Latency deltas are observations, not pass/fail gates.
