# V1-to-current index migration failure ledger

This ledger records confirmed migration defects and their resolution state.

## H1: migration parity does not validate current typed secondary identity

Status: resolved. The standalone parity harness and `migration-parity` feature
were later removed; the observer now builds under `production-coverage`.

Pre-fix evidence:

- `cargo check -p db --features migration-parity` fails because
  `CanonicalSecondaryValue` is not imported by the feature-gated observer.
- The standalone parity oracle still derives current equality membership from
  legacy lossy value hashes and discards canonical equality bytes.
- The range expectation passes a legacy string to the current byte-valued
  parity DTO and therefore does not compile after the DB feature is repaired.
- `CanonicalEqualityValue::try_from_parts` accepts a valid canonical frame
  paired with an unrelated digest. Both key and value decoders inherit that
  invalid state.

Resolution:

- Expected current memberships come only from authoritative typed graph values.
- Equality parity compares both digest and complete canonical bytes.
- Range parity compares independently generated typed payload bytes.
- Persisted digest/canonical mismatches now return
  `EncodingError::CanonicalEqualityDigestMismatch` from both key and value
  decoders.
- The feature-gated observer exposes complete equality and range payloads.
- The standalone tool owns exact integer, binary32, and binary64 decomposition,
  typed framing, SHA-256 digesting, domain ordering, descending complementation,
  and closed projection states without importing production comparison or
  canonical-encoding code.
- A private lane ADT replaces raw lane dispatch. Legacy property and value
  hashes no longer contribute to expected current memberships.

Post-fix evidence:

- Focused property, key, and value codec regressions reject mismatched digests
  with the typed error.
- All standalone oracle regressions pass, including typed identity, signed
  zero, infinities, `2^53`, `2^53 + 1`, datetime, NUL, and prefix strings.
- `cargo check -p db --features migration-parity` passes and is enforced by the
  normal PR quality job.
- The full dev parity scenario reports zero graph or index differences across
  16,034 current index memberships. Its first post-fix run reached
  `smoke_passed_release_blocked` solely because the release gate observed the
  expected uncommitted implementation under test.

## F1: legacy global edge secondary rows survive retirement

Status: resolved on this branch.

Test:
`migrated_v1_property_hash_collision_rebuilds_from_graph_truth`

Fixture:

- Exact deployed collision between `User\x1fproperty_16755` and
  `User\x1fproperty_36911`.
- Eight V1 definitions: node/edge equality and ascending range for both
  properties.
- Node and edge definitions both use label `User`; an earlier fixture revision
  incorrectly used edge label `REL` while seeding `User\x1f...` physical rows.
  The corrected fixture was independently run against commit `14f65479`.
- Authoritative node and edge rows assign each property to a different entity.
- Legacy physical rows contain swapped, stale, extra, colliding, and misordered
  memberships. Stale IDs are `99000` and `99001`.
- Migration uses one-row batches, normal writer bootstrap, and a cold reopen.

Expected:

- Current full-string identities rebuild from graph truth.
- Every legacy node, directional edge, and global edge physical row is deleted
  only after the corresponding current generation becomes Active.
- Cold reopen exposes no legacy membership.

Pre-fix observation:

- All eight current generations are Active and return only their authoritative
  entity.
- Authoritative graph rows and edge endpoint ownership remain exact.
- The V1 definition catalog is empty.
- Graph, index, and storage readiness are all published.
- Cold reopen preserves those results and IDs.
- Legacy node equality rows: `[]`.
- Legacy node range rows: `[]`.
- Legacy global edge equality rows: `[60000, 60001, 99001]`.
- Legacy global edge range rows: `[60000, 60001, 99001]`.

The focused test fails with:

```text
legacy physical rows survived retirement: node equality=[], node range=[],
global edge equality=[60000, 60001, 99001],
global edge range=[60000, 60001, 99001]
```

Read-only localization:

- Secondary retirement calls
  `delete_edge_equality_index_entries_for_property` and
  `delete_edge_range_index_entries_for_property_with_direction`.
- Those helpers delete directional edge rows but do not scan or delete the
  `GlobalEdgeEquality` or `GlobalEdgeRange` lanes written by the corresponding
  legacy maintenance helpers.

Resolution:

- Added typed-prefix cleanup for `GlobalEdgeEquality` by property hash.
- Added typed-prefix cleanup for `GlobalEdgeRange` by property hash and
  direction.
- Retirement now deletes directional and global edge rows in the same
  transaction as the exact legacy catalog row.
- Existing directional-only cleanup helpers retain their previous behavior.

Post-fix verification:

- The corrected fixture still fails against pre-fix commit `14f65479` with the
  exact remaining IDs above.
- The focused cleanup unit contract passes for `0x00`, `0x41`, and `0xFF`
  leading entity IDs, direction isolation, and unrelated properties.
- The complete collision migration contract passes with all four legacy
  physical lanes empty after cold reopen.

## F2: vector adoption validation failpoint strands writer-open

Status: resolved on this branch.

Test:
`legacy_vector_adoption_failpoints_preserve_physical_ownership`

Fixture:

- One populated V1 cosine-vector definition and one graph-authoritative node.
- The deployed HNSW namespace contains one searchable vector.
- Migration uses one-row batches and enters through normal writer bootstrap.
- Each validation, metadata-publication, reservation-transition, and
  definition-retirement boundary is injected independently.
- Every writer-open is bounded by a strict five-second timeout.

Expected:

- The failpoint fires and writer-open returns typed `MigrationRequired`.
- The legacy catalog, graph row, HNSW bytes, and unpublished readiness remain
  inspectable.
- A clean cold reopen resumes the same index ID, generation, and physical ID.

Pre-fix observation:

- The same diagnostic matrix successfully interrupted and recovered both
  `LegacyVectorReservationBefore` and `LegacyVectorReservationAfter`, proving
  the populated adoption fixture reaches the production migration path.
- `LegacyVectorValidationCheckpointBefore` fires inside the vector driver, but
  writer-open does not return within five seconds.
- The shared outbox converts the driver error into a delayed transient retry,
  while bootstrap treated every queued/running status as indefinitely
  waitable.
- The comparable green adoption contract takes roughly 7.5 seconds end to end
  in the debug profile, so the timeout alone was not evidence of a deadlocked
  worker.

Resolution:

- Queued operation persistence now distinguishes immediate work, delayed
  progress, and transient failure, with the failed writer epoch retained.
- Blocking bootstrap returns `MigrationRequired` for a transient failure from
  its current worker epoch.
- A new writer epoch ignores the failed epoch's remaining backoff and retries
  immediately; same-epoch automatic work retains bounded backoff.
- Stage-time driver failures release their exact claim into the typed transient
  state. Preparation-time errors retain their existing typed error contract and
  automatic-worker behavior.

Post-fix verification:

- All eight before/after adoption boundaries return the typed error within five
  seconds.
- Failure preserves the graph row, legacy catalog, HNSW digest, unpublished
  readiness, index ID, generation, operation, and physical ID.
- Cold reopen converges with identical ownership and searchable HNSW rows.
- Operation codec goldens and malformed-schedule tests cover every queue cause.
- Storage remains version 2; no compatibility decoder or V2/V3 migration was
  added.
