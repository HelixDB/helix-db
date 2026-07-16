# helix-cli

The Helix CLI — binary `helix`, crate `helix-cli` (v3.0.1). It is a **runtime orchestrator**, not a compiler.

## v2 → v3 shift (read this first)

This CLI has **no `helix compile` and no `helix check`**, and there is **no `.hx` query workflow** in it. (Older notes/memory that mention those commands describe the v2 CLI and are stale.) In v3:

- **Queries are JSON requests** sent to a *running* instance via `POST /v1/query` (`helix query`). Validation happens server-side, in the instance.
- **Local instances are Docker/Podman containers** (image `ghcr.io/helixdb/enterprise-dev:latest`), managed by `LocalRuntime`. `helix start` starts one; in-memory by default, on-disk (MinIO-backed) with `--disk`.
- **Enterprise instances deploy to Helix Cloud** via `helix push`, with auth/metadata managed through `helix auth`, `helix sync`, and the `workspace`/`project`/`cluster` commands.

The Rust DSL builder lives in `sdks/rust/` (a client library), not in this CLI.

## Entry point & dispatch

`src/main.rs` defines the clap `Cli` struct with two **global flags** — `--quiet` (errors + final result only) and `-v/--verbose` (timing detail) — wired into `output::Verbosity`. The `Commands` enum lists every subcommand; `main()` matches each to `commands::<name>::run(...)`. With no subcommand it prints the welcome banner (`display_welcome`). Errors are downcast to `CliError`/`ConfigError`/`ProjectError`/`PortError` for pretty rendering before `exit(1)`. Metrics are bootstrapped (`MetricsSender::new`) and an update check runs on every invocation.

## Module map (`src/`)

- `commands/` — one module per subcommand (the handlers below).
- `lib.rs` — the public crate root; defines the subcommand enums (`InitTarget`, `AddTarget`, `AuthAction`, `MetricsAction`, `Workspace/Project/ClusterConfigAction`, `ConfigOutputFormat`) and re-exports modules.
- `config.rs` — `helix.toml` (`HelixConfig`) and user-level `~/.helix/config` (`WorkspaceConfig`) formats; load/save/validate; key constants.
- `project.rs` — `ProjectContext::find_and_load()` walks up the tree to find `helix.toml`; resolves `.helix/<instance>` state dirs. `get_helix_cache_dir()` honors `HELIX_CACHE_DIR`.
- `local_runtime.rs` — `LocalRuntime`: Docker/Podman container lifecycle (`check_available`, `container_name` = `helix-<project>-<instance>`, pull/run/stop/restart/status/prune). Disk mode also spins up MinIO container + volume + network; memory mode is the Helix container alone. Health-checks via TCP probe.
- `enterprise_cloud.rs` — cloud REST types (`CliWorkspace`, `CliProject`, `CliEnterpriseCluster`, …) and fetchers (`fetch_workspaces`, `fetch_projects`, `fetch_workspace_clusters`). `cloud_base_url()` reads `CLOUD_AUTHORITY` (default `cloud.helix-db.com`).
- `sse_client.rs` — `SseClient` / `SseEvent` for server-sent-event streams: auth device-code flow, deploy progress, and enterprise log ranges.
- `metrics_sender.rs` — `MetricsSender` + `MetricsConfig` (level Full/Basic/Off, user_id, email). Async event sender to the logs endpoint; created in `main`, `shutdown()` on exit.
- `port.rs` — `is_port_available`, `find_available_port`, `ensure_port_available` (scans up to 100 ports).
- `update.rs` — `check_for_updates` (cached 24h in `~/.helix/`), `current_version`; `commands::update` does the actual `self_update` binary swap.
- `output.rs` — terminal helpers. `Verbosity` (Silent/Quiet/Normal/Verbose, global atomic), `Operation`, `Step` (spinner: `start`/`done`/`fail`, plus `println`/`set_message`/`set_completion`), `LiveSpinner` (wraps indicatif).
- `prompts.rs` — `is_interactive()` and cliclack prompts (`confirm`, `input_instance_name`, `input_port`, `select_local_disk_mode`, …).
- `utils.rs` — `command_exists` (via `which`), print helpers, `add_env_var_to_file`.
- `errors.rs` — `CliError` (severity/message/context/hint/file_path/caused_by) with boxed colored `render()`; typed `ConfigError`/`ProjectError`/`PortError` that convert into it.

## Command reference

All instance args default to `dev` (or prompt interactively when ambiguous). Enterprise/cloud commands call `require_auth()` (credentials from `helix auth login`).

**Project setup**
- `init [--path <dir>] (local|enterprise)` — scaffold a project: writes `helix.toml`, `.helix/`, `examples/request.json`, `AGENTS.md` (local only; never overwrites an existing one), and `.gitignore` entries. `--path` is a clap global arg, so it works before or after the subcommand. `local`: `--name dev --port 8080 [--disk]`. `enterprise`: `--name production --cluster-id <id> [--gateway-url <url>]`.
- `chef` (alias `cook`) — interactive one-shot bootstrapper that hands off to an AI agent. **See dedicated section below.**
- `add [--path <dir>] (local|enterprise)` — add an instance to an existing `helix.toml` without clobbering others. Same flags as `init` targets (`--name` required here); `--path` is global (either position) and points at the project directory.

**Local lifecycle**
- `start [instance] [--foreground] [--port <p>] [--disk] [--persist]` (alias `run`) — start a local container (background by default; `--detach` is a hidden alias). `--disk` forces on-disk/MinIO storage for this run; `--persist` writes the resolved port/storage back to `helix.toml`. The in-memory data-loss warning is shown only once per instance (tracked by a `.warned-memory` marker in the instance workspace).
- `stop [instance]` / `restart [instance]` — stop/restart a background container.
- `status [instance]` — project + per-instance details (URL, cluster id, storage mode, container state). Omit instance for all.
- `logs [instance] [-f] [-r --start <iso> --end <iso>]` — local: Docker/Podman logs (`-f` follows). Enterprise: time-range fetch from cloud (`-r`, ISO-8601, defaults to last hour).
- `prune [instance] [-a/--all] [-y/--yes]` — delete local containers + `.helix/workspace/` dirs. Non-interactive needs an instance or `--all`.
- `delete <instance> [-y/--yes]` — remove instance from `helix.toml` **and** its local runtime state (instance arg required).

**Queries**
- `query [instance] (--file <req.json> | --json '<body>' | -e/--ts '<ts>' | --ts-file <query.ts>) [--warm] [--host <h>] [--port <p>] [--compact]` — send a query to `POST /v1/query`. The four input flags are mutually exclusive and exactly one is required (enforced by a clap `ArgGroup`). `--file`/`--json` pass a raw query JSON body (`request_type` + `query`). `-e`/`--ts`/`--ts-file` accept a **raw TypeScript DSL expression** (like `mysql -e`): `src/ts_query.rs` evaluates it in Node with the published `@helix-db/helix-db` SDK in scope (auto-imports `g`/`readBatch`/`writeBatch`/`defineParams`/`param`), calls `.toQueryJson()`, and feeds the result into the same send path — `request_type` is inferred from read-vs-write batch. The SDK is `npm install`ed once into `<helix cache>/ts-runtime/` (spec `^3.0.0`, re-installed only when `SDK_SPEC` changes); needs Node 20+ on PATH. `--warm` adds `X-Helix-Warm` (read-only). Pretty JSON unless `--compact`. Enterprise targets use the configured gateway URL + auth header; the auth value is read from `query_auth_env`, now also loaded from a project-root `.env` (via `dotenvy`).

**Cloud**
- `auth (login | logout | create-key <cluster>)` — `login`: GitHub OAuth via SSE, stores `~/.helix/credentials`. `logout`: clears it. `create-key`: rotate a cluster API key (shown once).
- `push [instance]` — deploy an Enterprise instance to Helix Cloud (`deploy_enterprise`); errors on local instances; emits deploy metrics.
- `sync [instance] [-y/--yes] [--dry-run]` — reconcile enterprise metadata + source between local and cloud (SHA256/mtime diff, conflict prompts unless `--yes`); updates `helix.toml`. `--dry-run` (conflicts with `--yes`) fetches remote state and prints the would-be plan via `print_dry_run_summary`, then returns before any mutation.
- `workspace (list|show|switch) [--format human|json]` — manage active cloud workspace (selection in `~/.helix/config`).
- `project (list|show|switch) [--format human|json]` — manage linked cloud project.
- `cluster list [--workspace-id] [--project-id] [--format human|json]` — list Enterprise clusters.
- `config (workspace|project|cluster) …` — hidden parent grouping the three above.

**Misc**
- `metrics (full|basic|off|status)` — manage telemetry level (`~/.helix/metrics.toml`); `full` prompts for email.
- `update [--force] [--v1]` — self-update to latest release; `--v1` pins the last v1-compatible CLI.
- `feedback [message]` — opens a pre-filled GitHub issue in the browser.

## `helix chef` (alias: `helix cook`)

Defined in `src/commands/chef.rs`, dispatched from `src/main.rs` (`Commands::Chef {}` → `commands::chef::run()`). The command takes **no flags** — it is fully interactive. (Earlier `--auto`, `--intent`, and `--agent` flags were removed in favor of the interactive flow.)

### End-to-end flow (`run()` → `collect_options()`)

1. **Ensure Helix Cloud auth** — `chef` requires Cloud credentials before setup. If `~/.helix/credentials` is missing/invalid, it runs the existing GitHub device login flow inline. Login failure exits `chef` without user-facing error and emits a Chef auth-failure metric.
2. **Ask the build intent** — "What do you want to build?" (free text; blank → Personal CRM default).
3. **Ask the setup mode** — Manual vs Automatic (recommended). Manual adds per-step confirm prompts; Automatic runs everything with defaults. Both still ask the intent question.
4. **Setup pipeline:**
   - `install_skills` — `npx skills add HelixDB/skills` (the HelixDB query skills). **Global (`-g`) by default**; Manual mode asks global-vs-project.
   - `install_mcp` — `npx add-mcp <docs MCP>` scoped to `MCP_HTTP_COMPATIBLE_AGENTS` (add-mcp errors non-zero if it hits an http-incompatible agent like Claude Desktop, so the agent list is pinned).
   - `init_project` — reuses `helix init local`.
   - `write_agent_prompt` + `write_example_queries` — writes `HELIX_CHEF_PROMPT.md` (the system prompt) and `examples/{seed,read_users}.json`.
   - `run_database` — `helix start dev` (port 8080, in-memory).
   - `seed_starter_data` — runs `examples/seed.json`.
5. **Agent detection** (`detect_agent`) — first available of `AGENT_PRIORITY`: Claude Code (`claude`) → OpenAI Codex (`codex`) → OpenCode (`opencode`), via `utils::command_exists`.
6. **Permission prompt** (`select_permission_mode`) — "Give the agent full autonomy?": Yes (full auto) / Scoped (ask per command) / Don't launch. Non-interactive → `None` (skip launch).
7. **Launch** (`launch_agent`, async) — Claude goes through `launch_claude_streaming`; codex/opencode through a captured stdout/stderr path so `chef` can include a transcript in the snapshot.
8. **Post-run** — on success, print the agent's structured summary and `try_open_frontend` (open `http://localhost:3000` if `web/package.json` exists and the server responds). On failure / abort / no-agent → `print_paste_prompt_hint` points the user at `HELIX_CHEF_PROMPT.md`.
9. **Snapshot upload** — always attempts to build a sanitized snapshot and upload it to Helix Cloud using presigned S3 URLs from `/api/cli/chef-snapshots/upload-urls`. Upload failures are silent and recorded through Chef metrics.

### The system prompt (`AGENT_PROMPT_TEMPLATE` + `DEFAULT_PROJECT_SPEC`)

`starter_prompt()` substitutes `{intent}` into `AGENT_PROMPT_TEMPLATE`; blank intent falls back to `DEFAULT_PROJECT_SPEC` (a Personal CRM: Contact / Company / Interaction with WORKS_AT and LOGGED edges). Written verbatim to `HELIX_CHEF_PROMPT.md`.

Prompt sections: `<role>`, `<environment>`, `<user_intent>`, `<workflow>` (14 steps), `<install_more_skills>`, `<json_dsl_quickref>`, `<patterns>`, `<frontend>`, `<cli_commands>`, `<antipatterns>`, `<deploy_imperative>`.

**Mandated tech stack** (the agent must use this — not optional):
- Queries: **JSON query files only** (no Rust `.hx` files). One JSON file per query under `examples/`, run with `helix query dev --file ...`.
- Frontend: **Next.js (App Router) + React + Tailwind, all TypeScript**, scaffolded with `npx create-next-app@latest web --typescript --tailwind --app --eslint --src-dir --import-alias '@/*' --use-npm --yes`.
- Backend: **TypeScript only**, via Next.js API routes (`web/src/app/api/<name>/route.ts`) that read the sibling `examples/*.json` and proxy to `http://localhost:8080/v1/query`. **The browser never calls Helix directly.**
- Extra skills: the agent installs `vercel-labs/agent-skills` (Next.js/React/Tailwind/TS) itself via `npx skills add ... -g -y --all`.

**Lifecycle requirements baked into the prompt:**
- Leave the Next.js dev server (and any extra backend processes, tracked in `processes.md`) **running** after finishing — `cd web && nohup npm run dev > .next-dev.log 2>&1 & disown`.
- Open the frontend in the browser (`open`/`xdg-open`/`start`); chef retries as a safety net.
- End with a 7-section summary: What you built / Files created / Files modified / Services running / Commands run / How to try it / Known gaps.

### Claude streaming (`launch_claude_streaming`)

Claude is run headless: `claude --append-system-prompt-file HELIX_CHEF_PROMPT.md <permission flag> --output-format stream-json --verbose -p "<AGENT_USER_PROMPT>"`. Permission flag is `--dangerously-skip-permissions` (full auto) or `--permission-mode acceptEdits` (scoped). Codex/OpenCode use their own `exec`/`run` subcommands with equivalent flags (`build_agent_argv`).

stdout is piped and parsed line-by-line as NDJSON into `ClaudeEvent` (System / Assistant / User / Other), `ContentBlock` (Text / ToolUse / ToolResult / Other), and `ResultEvent`. `format_tool_use` maps each tool to a one-line status (`✎ Editing …`, `💻 …`, `📋 Updating tasks (N)`, etc.). The status updates the chef spinner **in place** (two-line message via `Step::set_message`) — one line, no scroll spam. The terminal `result` event yields stats (`format_result_stats` → `(37.2s, $0.412)`, baked into the completion line via `Step::set_completion`) and the final summary text (printed via `Step::println`).

**Robustness:** stdin is `Stdio::null()`; the read loop races `tokio::signal::ctrl_c()` (Ctrl-C kills the child + prints the paste hint); `child.wait()` is wrapped in a 5s `timeout` then force-kill so chef never hangs.

### Chef snapshots and metrics

`chef` emits a `chef` metrics event for `started`, `completed`, `auth_failed`, and `upload_failed`. Metrics include only metadata (run id, phase, setup mode, agent, duration, success, sizes, and short error strings); prompt text, transcripts, and source code are never sent through metrics.

After agent completion, `chef` creates two in-memory upload objects:
- `overview.json` — run metadata, original intent, rendered `HELIX_CHEF_PROMPT.md`, final summary, transcript, file inventory, and skipped-file list.
- `project.txt.gz` — a gzipped inert text rendering of selected UTF-8 project files.

Snapshot safety excludes `.git`, `.helix`, `node_modules`, `.next`, `target`, build/cache/coverage directories, `.env*`, keys/certs, logs, DB dumps, binary/non-UTF8 files, oversized files, and files matching common secret patterns. The CLI uploads via presigned PUT URLs returned by Helix Cloud; upload failures do not fail the Chef build.

### Output helpers (`src/output.rs`)

`Step` (spinner with `start`/`done`/`fail`) gained `println` (print above the spinner), `set_message` (rewrite the spinner line in place), and `set_completion` (override the ✓ line after the fact). `LiveSpinner` wraps indicatif's `ProgressBar`.

## Config & state

**Project config — `helix.toml`** (`HelixConfig` in `config.rs`, found via `ProjectContext::find_and_load`):

- `[project]` — `name` (required), optional `id` / `workspace_id`, `queries` (default `db/`), `container_runtime` (`docker` | `podman`, default docker).
- `[local.<name>]` — `port` (default `8080`), `image` (default `ghcr.io/helixdb/enterprise-dev`), `tag` (default `latest`), `storage` (`memory` | `disk`, default memory).
- `[enterprise.<name>]` — `cluster_id` (required), optional `workspace_id`/`project_id`/`gateway_url`, `query_auth_header` (default `Authorization`), `query_auth_env` (default `HELIX_API_KEY`), `availability_mode`, `gateway_node_type`, `db_node_type`, `min_instances`/`max_instances` (default 1), plus a **flattened `DbConfig`**: `vector_config` (m=16, ef_construction=128, ef_search=768, db_max_size_gb=20), `graph_config.secondary_indices`, `mcp`/`bm25` (default true), `schema`, `embedding_model` (default `text-embedding-ada-002`), `graphvis_node_label`.

`HelixConfig::validate` requires a non-empty project name, ≥1 instance, non-empty instance names, and a non-empty `cluster_id` for each enterprise instance. `default_config()` seeds a single in-memory `local.dev`.

**User-level state — `~/.helix/`:** `config` (`WorkspaceConfig` = active `workspace_id`), `credentials` (auth), `metrics.toml` (telemetry level/user). Constants: `DEFAULT_LOCAL_PORT = 8080`, `DEFAULT_ENTERPRISE_DEV_IMAGE/TAG`, `DEFAULT_QUERY_AUTH_HEADER = Authorization`, `DEFAULT_QUERY_AUTH_ENV = HELIX_API_KEY`.

## Testing

`cargo fmt -p helix-cli && cargo test -p helix-cli` — ~66 lib + ~21 binary tests. clap parsing tests live in `src/main.rs` `#[cfg(test)]` (every command/flag combo). Config (de)serialization + backward-compat defaults are tested in `src/config.rs`.

Chef tests cover: prompt rendering (intent substitution, CRM fallback, Next.js stack keywords, summary sections, browser-open commands, dev-server persistence), agent-priority order, `build_agent_argv` per (agent, permission) combo, install-arg construction, tool-use formatting, and stream-json event parsing. The actual agent spawn / browser open are not unit-tested (require external processes) — verify those manually with `cargo run -p helix-cli -- chef`.

Doc-tests in `output.rs` are `ignore`d (illustrative only). The `helix-cli/CHEF_COMMAND_PLAN.md` file is the original design doc and is now largely superseded by the implementation described above.
