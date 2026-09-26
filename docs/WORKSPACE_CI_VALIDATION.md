# Workspace CI validation

The workspace failure on `main` came from process-global equality read counters:
parallel unit tests could increment or reset another test's measurement.
Unit-test counters now belong to the test thread and its current-thread Tokio
runtime. Production-coverage builds retain their global atomics.

The barrier regression fails with the original counters and passes with isolated
counters. Workspace tests, doctests, strict workspace and DB test-target Clippy,
formatting, and the multithreaded production equality contract passed locally.
Focused LLVM coverage reached 100% of lines, regions, and functions in the new
counter module.

## Production coverage fingerprint

The final coverage gate also failed before this PR. Compare the complete JSON
summaries emitted by these two independent Linux ARM64 CI jobs:

- [Merged PR #1090, commit c834d51e](https://github.com/HelixDB/helix-db/actions/runs/34374965998/job/102545436242)
- [PR #1092, commit 1b382834](https://github.com/HelixDB/helix-db/actions/runs/34398741639/job/102625359451)

The DB sources and coverage scripts at `c834d51e` match its merge into `main`
at `1ffadba6`. The two parsed CI summaries are identical: all 187 integration
tests passed, every scope threshold passed, and vector coverage was 97.3473%
functions, 97.1564% source lines, and 94.5472% regions.

Both runs report 10,497 uncovered non-vector source lines with SHA-256
`6d29b946f78eab3f28136bca75e6d0e9fa6bc3b23fc925e8fe0f4329425a5143`.
The stored fingerprint still described 10,424 lines from an earlier revision.
Refreshing only its count and digest records the existing `main` backlog;
those lines retain their `test-required` classification. Scope thresholds,
vector thresholds, exclusions, dispositions, and exact fingerprint enforcement
remain unchanged. This refresh does not claim those uncovered lines are tested.
