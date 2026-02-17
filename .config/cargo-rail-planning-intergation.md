# cargo-rail Planning Integration

## Intent
This repo uses `cargo rail run` with workflow/profile mapping.
The `.config/rail.toml` keeps DB/HQL lanes explicit via workflow + custom surface mapping.

## Local developer flow
```bash
cargo rail config validate --strict
cargo rail plan --merge-base --explain
cargo rail run --merge-base --profile ci
```

## GitHub Actions integration (cargo-rail-action)
Use planner outputs for core and HQL lanes.

```yaml
- uses: loadingalias/cargo-rail-action@v3
  id: rail

- name: Core lane
  if: steps.rail.outputs.build == 'true' || steps.rail.outputs.test == 'true'
  run: cargo rail run --since "${{ steps.rail.outputs.base-ref }}" --profile ci

- name: HQL lane
  if: steps.rail.outputs.hql == 'true'
  run: cargo rail run --since "${{ steps.rail.outputs.base-ref }}" --profile hql
```

## UI output that teams should read
- action summary surface table + reasons
- `steps.rail.outputs.plan-json` for custom branching
- `cargo rail plan --explain` for local reproducibility

## Measured impact (last 20 commits)
- Could skip build: 10%
- Could skip tests: 10%
- Targeted (not full run): 75%
