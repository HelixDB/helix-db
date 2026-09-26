# Selective equality costing validation

With empty statistics, three selective equality predicates can lose to a label
scan after intersection children are costed serially. Both candidates survive
exploration. The comparison must include the label bitmap, graph-row hydration,
row construction, and residual evaluation.

## Complete cost comparison

The synthetic fixture uses three indexed equalities, 1,000 estimated scan rows,
and 10 estimated rows per equality. Values below are estimated microseconds,
not measured query latency.

| Cost | Previous parallel costing | Serial costing before fix | Fixed |
| --- | ---: | ---: | ---: |
| Equality child reads/decode | 5,135 | 15,180 | 15,180 |
| Intersection membership work | 10 | 10 | 30 |
| Final access-row construction | 10 | 10 | 10 |
| Complete indexed access | 5,155 | 15,200 | 15,220 |
| Label access plus residual | 13,000 | 13,000 | 18,050 |
| Projection, either candidate | 1,000 | 1,000 | 1,000 |
| Selected full plan | 6,155 indexed | 14,000 scan | 16,220 indexed |

The fixed label candidate includes 5,050 bitmap setup, 1,000 bitmap decode,
10,000 graph-row read/decode, 1,000 row construction, and 1,000 residual
evaluation. Including projection, the scan alternative costs 19,050.

For a shared equality intersected with a two-value equality union, factoring
out the shared lookup costs 6,020. Distributing and charging that lookup in
every branch costs 20,320, incorrectly favoring the 18,050 scan alternative.

## Preserved contracts

Serial child execution costing remains in place. Unordered set membership
work uses all input cardinalities, including when estimated output is zero;
final row construction uses output cardinality once. Saturating sums preserve
the cost domain. Physical candidate costing and executable lowering agree.
Populated statistics can still make a small label scan the cheaper choice.

Ordered range drivers retain visited-entry costing, membership-first
verification, reverse iteration, dynamic limits, and tie semantics.

## Validation

Synthetic regression coverage includes node and edge equality indexes, empty
and populated statistics, parameter bindings, nested conjunctions, shared
predicates with disjunctions, unindexed residuals, tenant catalog isolation,
retained snapshots, and concurrent storage changes. The benchmark oracle
checks complete responses and rejects missing, duplicate, or foreign rows.
Existing ordered-storage tests and paired built-image checks cover both wide
and narrow ordered projections.

Local checks completed for planner unit tests and doctests, query-service and
database contracts, ordered storage, Python benchmark oracles, Clippy,
formatting, coverage, and built-image smoke and persistence behavior.

Focused reproduction commands:

```sh
cargo test -p helix-planner --lib selective_equality
cargo test -p db --lib selective_equality
python3 -m unittest discover -s docker-image/tests -p 'test_equality_benchmark.py'
```
