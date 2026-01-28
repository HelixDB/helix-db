# Helix CLI

Command-line interface for managing Helix projects and deployments.

## Commands

- `init`: initialize a new project with `helix.toml`.
- `add`: add an instance to an existing project.
- `check`: validate config and queries.
- `compile`: compile queries into the workspace.
- `build`: build an instance (local or remote prep).
- `push`: deploy/start an instance.
- `sync`: sync source/config from Helix Cloud.
- `start` / `stop` / `status`: manage running instances.
- `logs`: view or stream logs.
- `auth`: login/logout/create-key.
- `prune`: clean containers/images/workspaces.
- `delete`: remove an instance.
- `metrics`: manage telemetry level.
- `dashboard`: manage the Helix Dashboard.
- `update`: update the CLI.
- `migrate`: migrate v1 projects to v2.
- `backup`: back up an instance.
- `feedback`: send feedback to the Helix team.

Run `helix <command> --help` for command-specific flags and options.

## Error handling

- Recoverable/library errors use `thiserror::Error` (config, project, port).
- CLI commands return `eyre::Result` and render `CliError` for consistent output.
