# Unique equality membership regression

The serial cost of a unique equality union exceeds the default label-scan cost
at four values. Production has no label cardinality statistics, so selective
membership queries scan large labels.

## Scope and contract

- Lower same-index, finite, indexed unique literals into one typed owner batch.
- Resolve the active generation and tenant scope from the request snapshot.
- Read owner keys with `multi_get`; verify every returned owner against the
  authoritative graph in that same snapshot. Missing owners are misses; stale
  or malformed owners fail closed. Do not alter persisted formats or writes.
- Charge batch reads and authoritative verification in both cost paths.
- Preserve null/NaN classification, numeric equality, set deduplication, range
  ordering, and configured union limits. Keep non-unique batching unchanged.

## Validation

1. Reproduce the failing four-value plan before implementation.
2. Test empty/populated statistics, literal/bound parameters, duplicate values,
   nested labels, union bounds, and small labels where a scan is appropriate.
3. Test batch results, missing/corrupt owners, snapshot/tenant boundaries,
   canonical numeric equality, and cancellation using local fixtures.
4. Run workspace tests, doc tests, coverage, formatting, and strict Clippy.
5. Build and test the full local image, compare synthetic membership queries
   with the released image, and inspect plans as well as timings.

No production access, push, deployment, or image publication is part of this work.

## Implemented

`UniqueUnion` holds one typed unique index, one scoped key, and at least two
indexed literals. Lowering emits it only when all children share that identity.
Both cost paths charge a real owner `multi_get`, authoritative graph checks,
and the set operation. The executor uses the existing request catalog/read view;
it does not reload a newer generation or bypass verification.

The configured union limit remains unchanged (64 distinct values by default).
Null/non-reflexive classification and mixed-index unions retain their existing
execution paths. No stored bytes, index formats, writes, CRDs, or cluster settings
change. No rebuild of existing indexes is required.

## Local validation results

- The pre-fix regression failed at four values with empty statistics.
- 1,265 planner tests passed, including literals/bound parameters, duplicates,
  nested label predicates, missing/populated statistics, small labels, and limits.
- Full workspace tests and doc tests passed: 3,931 reported passes, 11 existing
  ignored tests. All 23 final focused unique-index tests also passed.
- Strict workspace/all-target Clippy and formatting checks passed.
- LLVM coverage exercised planner and database suites. The set cost and equality
  leaf contract files reached 100% line coverage in the planner run. The new
  unique-owner batch helper reached 64/64 covered executable lines, including
  failed/truncated reads, malformed/stale owners, and invalid value/lane checks.
- Native Linux ARM64 baseline and candidate server images built locally. Image
  metadata/secret checks, memory/disk restart tests, concurrent membership,
  configuration rejection, and graceful shutdown passed.
- The stock Compose S3 fixture could not pull its pinned MinIO images. A separate
  disposable cached-MinIO test passed seed/read, stop/restart, and replacement by
  the final image, preserving all indexed results. This is not a pass of the
  blocked Compose command itself.
- Matched 100,000-row images: affected five-hit membership was about 300 ms before
  and 1.2 ms after; 64 distinct values were about 334 ms before and 2 ms after.
  Every response was checked against the fixture oracle. These are local synthetic
  measurements, not a production latency promise. Lists above the configured
  union bound still use the existing fallback.

The image benchmark is reproducible with
`docker-image/tests/unique_membership_benchmark.py` against two empty disposable
localhost servers. It also supports `--no-seed` and `--workers 4`.

## Release boundary

This is a local HelixDB source fix, not a production rollout. After review and
explicit push/merge approval, Hyperscale must pin the resulting reachable HelixDB
revision, build new images, and follow the rollout runbook. This task does not
change those dependency or deployment pins. Hosted CI and production acceptance
remain separate release gates.
