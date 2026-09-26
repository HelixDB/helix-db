# Idle vector-cache refresh validation

The refresh guard retains a published cache only when its snapshot sequence,
committed dirty generation, and admission budget match. Empty and budget-limited
stores retain the same checks. No persisted data format or public API changed.

- `cargo test --workspace`: 3,972 tests and doc tests passed; 12 existing tests ignored.
- Workspace Clippy, production-contract Clippy with the production coverage feature
  set, formatting, and 24 Docker tooling tests passed.
- Focused LLVM coverage exercised all 25 regions in `retain_if_current` (44 calls),
  with no uncovered regions.
- All 187 production coverage tests passed. Vector coverage exceeds unchanged
  thresholds: functions 97.35%, source lines 97.16%, regions 94.56%.
- The overall coverage gate failed: secondary lifecycle lines are 76.97% against
  77.81%, and the non-vector uncovered-line fingerprint differs from its baseline
  (10,566 lines; SHA-256
  `cb63a9aa38bfad1bffccdc63c61ff4eb7bedcab9357c7b7fafab9c323b8427a5`).
  This change does not edit those source areas. An equivalent baseline coverage
  run was not performed, so the cause remains unverified. No thresholds or
  non-vector baselines were lowered or replaced.
- The counted-SST contract verifies unchanged store identity and no additional
  vector-data reads across three refreshes, including empty and bounded stores.
- The ARM64 server image passed the full packaging/runtime suite. The final MinIO
  regression uses a metadata-only query to identify catalog ranges, then observes
  17 seconds without queries: six catalog GETs and zero vector-data GETs. Search
  results remain correct before and after a write.
- The same regression fails against a rebuilt image from unmodified `main`
  (`2eaa844a96046808b887914679fbe4a49b7adc05`), detecting repeated non-catalog ranges.

Catalog polling remains enabled. Unrelated committed writes can still trigger
rehydration because cache reads require an exact snapshot sequence.
