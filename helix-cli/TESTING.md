# Helix CLI Testing Guide

For each flow, run commands from the `helix-cli` directory using:

```bash
cargo run -- <command>
```

## Local flows (non-cloud)

- `helix init` with defaults; verify `helix.toml` and `./db/` created.
- `helix init --path /custom/path` with custom directory.
- `helix init --queries-path ./custom-queries/` with custom queries dir.
- `helix add` (interactive local); ensure instance added to `helix.toml`.
- `helix check` and `helix check <instance>`.
- `helix compile` and `helix compile --path ./project --output ./out`.
- `helix build <instance>` and `helix build --bin ./dist`.
- `helix push <instance>` and `helix push <instance> --dev`.
- `helix start <instance>` / `helix stop <instance>` / `helix status`.
- `helix logs <instance>` and `helix logs <instance> --live`.
- `helix prune` / `helix prune <instance>` / `helix prune --all`.
- `helix metrics full|basic|off|status`.
- `helix update` / `helix update --force`.

## Cloud/remote flows

### Project initialization

- `helix init cloud --region us-east-1`.
- `helix init ecr`.
- `helix init fly --auth cli --volume-size 50 --vm-size performance-4x --private`.

### Instance management

- `helix add cloud --name my-instance --region us-east-1`.
- `helix add ecr --name my-ecr`.
- `helix add fly --name my-fly --volume-size 30`.
- `helix delete <instance>` to remove instance.

### Syncing and logs

- `helix sync <instance>` to pull `.hx` and config from Helix Cloud.
- `helix logs <instance> --range --start <iso> --end <iso>`.

### Authentication

- `helix auth login` and `helix auth logout`.
- `helix auth create-key <cluster-id>`.

### Error scenarios

- `helix init` in a directory with an existing `helix.toml`.
- `helix build <missing-instance>` (missing instance validation).
- `helix start <instance>` without prior build.
- Docker/Podman not installed or daemon not running.
- Commands requiring auth without login.
