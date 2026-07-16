# Helix CLI

Command-line interface for managing v2 Helix projects, local development instances, and Enterprise Cloud deployments.

## Commands

- `init`: initialize a v2 project with `helix.toml` and a query example.
- `chef`: bootstrap a first Helix app with skills, docs MCP, local runtime, starter queries, seed data, and a launched coding agent.
- `add`: add a local v2 or Enterprise Cloud instance to an existing project.
- `run`: run a local v2 instance in the background by default, attached with `--foreground`, with persistent local storage using `--disk`, or against a user-managed S3/S3-compatible prefix using `--storage-uri`.
- `stop` / `restart` / `status`: manage local v2 instances and inspect Enterprise Cloud config.
- `logs`: view local container logs or query Enterprise Cloud historical logs.
- `query`: send a query request JSON file to `POST /v1/query`.
- `push`: compile and deploy an Enterprise query project to an Enterprise Cloud cluster.
- `auth`: login, logout, or create an Enterprise Cloud API key.
- `workspace`: manage active Enterprise Cloud workspace selection.
- `project`: manage linked Enterprise Cloud project selection.
- `cluster`: list and inspect Enterprise Cloud clusters.
- `sync`: reconcile Enterprise query project source and sync Enterprise Cloud metadata into `helix.toml`.
- `prune`: clean Helix-owned local containers, disk-mode volumes, and workspaces.
- `delete`: remove an instance from `helix.toml` and clean local runtime state.
- `metrics`: manage telemetry level.
- `update`: update the CLI.
- `feedback`: send feedback to the Helix team.

Run `helix <command> --help` for command-specific flags and options.

When run in a terminal, commands with missing choices use interactive prompts powered by `cliclack`. When run in non-interactive contexts, commands do not prompt and require explicit arguments or flags.

## Error handling

- Recoverable/library errors use `thiserror::Error` (config, project, port).
- CLI commands return `eyre::Result` and render `CliError` for consistent output.

## Testing

See [TESTING.md](TESTING.md) for the unit, mocked service, actual-binary, Docker runtime, coverage, and cross-platform CI contracts.
