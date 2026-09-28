# helix-cli

The Helix CLI — binary `helix`, crate `helix-cli` (v3.0.1). It is a **runtime orchestrator**, not a compiler.

## v2 → v3 shift (read this first)

This CLI has **no `helix compile` and no `helix check`**, and there is **no `.hx` query workflow** in it. (Older notes/memory that mention those commands describe the v2 CLI and are stale.) In v3:

- **Queries are JSON requests** sent to a *running* instance via `POST /v2/query` (`helix query`). Validation happens server-side, in the instance.
- **Local instances are Docker/Podman containers** (image `ghcr.io/helixdb/helixdb:v0.0.6`), managed by `LocalRuntime`. `helix start` starts one; in-memory by default, on-disk (SeaweedFS-backed) with `--disk`.
- **Cloud instances are linked resources.** The CLI authenticates only with a rotating WorkOS session and sends Cloud queries through WFE's backend broker. It does not deploy query bundles or call Cloud gateways directly.

The Rust DSL builder lives in `sdks/rust/` (a client library), not in this CLI.

## Entry point & dispatch

`src/main.rs` defines the clap `Cli` struct with two **global flags** — `--quiet` (errors + final result only) and `-v/--verbose` (timing detail) — wired into `output::Verbosity`. The `Commands` enum lists every subcommand; `main()` matches each to `commands::<name>::run(...)`. With no subcommand it prints the welcome banner (`display_welcome`). Errors are downcast to `CliError`/`ConfigError`/`ProjectError`/`PortError` for pretty rendering before `exit(1)`. Metrics are bootstrapped (`MetricsSender::new`) and an update check runs on every invocation.

## Module map (`src/`)

- `commands/` — one module per subcommand (the handlers below).
- `lib.rs` — the public crate root; defines the subcommand enums (`InitTarget`, `AddTarget`, `AuthAction`, `MetricsAction`, `Workspace/Project/ClusterConfigAction`, `ConfigOutputFormat`) and re-exports modules.
- `cloud.rs` — strict WorkOS session loading, locked refresh rotation, and WFE requests.
- `config.rs` — `helix.toml` (`HelixConfig`) load/save/validation; stable project and typed database linkage only.
- `project.rs` — `ProjectContext::find_and_load()` walks up the tree to find `helix.toml`; resolves `.helix/<instance>` state dirs. `get_helix_cache_dir()` honors `HELIX_CACHE_DIR`.
- `local_runtime.rs` — `LocalRuntime`: Docker/Podman container lifecycle (`check_available`, `container_name` = `helix-<project>-<instance>`, pull/run/stop/restart/status/prune). Disk mode also spins up a SeaweedFS S3 container + volume + network (and removes MinIO sidecars left by older releases); memory mode is the Helix container alone. Health-checks via TCP probe.
- `service_endpoints.rs` — resolves the WFE base URL from `CLOUD_AUTHORITY` or the production default.
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
- `init [--path <dir>] (local|cloud)` — scaffold a project. Cloud uses `--database cluster:<id>|tenant:<id>` plus owner IDs when they cannot be derived; it stores no gateway URL or query credential.
- `chef` (alias `cook`) — interactive one-shot bootstrapper that hands off to an AI agent. **See dedicated section below.**
- `add [--path <dir>] (local|cloud)` — add an instance to an existing `helix.toml` without clobbering others. Cloud resolves and stores a typed database link.

**Local lifecycle**
- `start [instance] [--foreground] [--port <p>] [--disk] [--persist]` (alias `run`) — start a local container (background by default; `--detach` is a hidden alias). `--disk` forces on-disk/SeaweedFS storage for this run; `--persist` writes the resolved port/storage back to `helix.toml`. The in-memory data-loss warning is shown only once per instance (tracked by a `.warned-memory` marker in the instance workspace).
- `stop [instance]` / `restart [instance]` — stop/restart a background container.
- `status [instance]` — project + per-instance details (URL, cluster id, storage mode, container state). Omit instance for all.
- `logs [instance] [-f] [-r --start <iso> --end <iso>]` — local: Docker/Podman logs (`-f` follows). Enterprise: time-range fetch from cloud (`-r`, ISO-8601, defaults to last hour).
- `prune [instance] [-a/--all] [-y/--yes]` — delete local containers + `.helix/workspace/` dirs. Non-interactive needs an instance or `--all`.
- `delete <instance> [-y/--yes]` — remove instance from `helix.toml` **and** its local runtime state (instance arg required).

**Queries**
- `query [instance|cluster:<id>|tenant:<id>] (--file <req.json> | --json '<body>' | -e/--ts '<ts>' | --ts-file <query.ts>) [--warm] [--host <h>] [--port <p>] [--compact]` — local targets send to `POST /v2/query` with auth disabled. Cloud targets use the active WorkOS session and WFE's separate read/write broker endpoints. They never load an application key. The four input flags are mutually exclusive and exactly one is required. TypeScript input is evaluated through the cached `@helix-db/helix-db` SDK.

**Cloud**
- `auth (login|status|logout)` — WorkOS PKCE login and rotating session credentials. Refresh is serialized and persisted atomically. There is no API-key or service-credential login.
- `workspace (list|get)` — discover workspaces without global selection state.
- `project (list|get|create|delete|link)` — discover/manage projects and link only the current `helix.toml`.
- `cluster (list|get|indexes)` — discover and inspect existing clusters.
- `database (list|get|create|delete|indexes|key)` — manage tenant databases. Creation returns a default read-write application key once; the CLI displays but never stores it.
- `service-credential (create|list|get|update|revoke)` — manage workspace-owned headless automation credentials through the user session. These credentials cannot log the CLI in.
- `api (get|post|patch|delete)` — call WFE with the active WorkOS session.
- `push`, `sync`, `workspace switch`, `project switch`, and `auth create-key` do not exist.

**Misc**
- `metrics (full|basic|off|status)` — manage telemetry level (`~/.helix/metrics.toml`); `full` prompts for email.
- `update [--force] [--v1]` — self-update to latest release; `--v1` pins the last v1-compatible CLI.
- `feedback [message]` — opens a pre-filled GitHub issue in the browser.

## `helix chef` (alias: `helix cook`)

Defined in `src/commands/chef.rs`, dispatched from `src/main.rs` (`Commands::Chef {}` → `commands::chef::run()`). The command takes **no flags** — it is fully interactive. (Earlier `--auto`, `--intent`, and `--agent` flags were removed in favor of the interactive flow.)

### End-to-end flow (`run()` → `collect_options()`)

1. **Ask the build intent** — "What do you want to build?" (free text; blank → Personal CRM default).
2. **Ask the setup mode** — Manual vs Automatic (recommended). Manual adds per-step confirm prompts; Automatic runs everything with defaults. Both still ask the intent question.
3. **Setup pipeline:**
   - `install_skills` — `npx skills add HelixDB/skills` (the HelixDB query skills). **Global (`-g`) by default**; Manual mode asks global-vs-project.
   - `install_mcp` — `npx add-mcp <docs MCP>` scoped to `MCP_HTTP_COMPATIBLE_AGENTS` (add-mcp errors non-zero if it hits an http-incompatible agent like Claude Desktop, so the agent list is pinned).
   - `init_project` — reuses `helix init local`.
   - `write_agent_prompt` + `write_example_queries` — writes `HELIX_CHEF_PROMPT.md` (the system prompt) and `examples/{seed,read_users}.json`.
   - `run_database` — `helix start dev` (port 8080, in-memory).
   - `seed_starter_data` — runs `examples/seed.json`.
4. **Agent detection** (`detect_agent`) — first available of `AGENT_PRIORITY`: Claude Code → OpenAI Codex → OpenCode → Cursor Agent (`claude` → `codex` → `opencode` → `cursor-agent`), via `external_tools::available`.
5. **Permission prompt** (`select_permission_mode`) — "Give the agent full autonomy?": Yes (full auto) / Scoped (ask per command) / Don't launch. Non-interactive → `None` (skip launch).
6. **Launch** (`launch_agent`, async) — Claude goes through `launch_claude_streaming`; Codex and OpenCode use captured stdout/stderr.
7. **Post-run** — on success, print the agent's structured summary and `try_open_frontend` (open `http://localhost:3000` if `web/package.json` exists and the server responds). On failure / abort / no-agent → `print_paste_prompt_hint` points the user at `HELIX_CHEF_PROMPT.md`.

### The system prompt (`AGENT_PROMPT_TEMPLATE` + `DEFAULT_PROJECT_SPEC`)

`starter_prompt()` substitutes `{intent}` into `AGENT_PROMPT_TEMPLATE`; blank intent falls back to `DEFAULT_PROJECT_SPEC` (a Personal CRM: Contact / Company / Interaction with WORKS_AT and LOGGED edges). Written verbatim to `HELIX_CHEF_PROMPT.md`.

Prompt sections: `<role>`, `<environment>`, `<user_intent>`, `<workflow>` (14 steps), `<install_more_skills>`, `<json_dsl_quickref>`, `<patterns>`, `<frontend>`, `<cli_commands>`, `<antipatterns>`, `<deploy_imperative>`.

**Mandated tech stack** (the agent must use this — not optional):
- Queries: **JSON query files only** (no Rust `.hx` files). One JSON file per query under `examples/`, run with `helix query dev --file ...`.
- Frontend: **Next.js (App Router) + React + Tailwind, all TypeScript**, scaffolded with `npx create-next-app@latest web --typescript --tailwind --app --eslint --src-dir --import-alias '@/*' --use-npm --yes`.
- Backend: **TypeScript only**, via Next.js API routes (`web/src/app/api/<name>/route.ts`) that read the sibling `examples/*.json` and proxy to `http://localhost:8080/v2/query`. **The browser never calls Helix directly.**
- Extra skills: the agent installs `vercel-labs/agent-skills` (Next.js/React/Tailwind/TS) itself via `npx skills add ... -g -y --all`.

**Lifecycle requirements baked into the prompt:**
- Leave the Next.js dev server (and any extra backend processes, tracked in `processes.md`) **running** after finishing — `cd web && nohup npm run dev > .next-dev.log 2>&1 & disown`.
- Open the frontend in the browser (`open`/`xdg-open`/`start`); chef retries as a safety net.
- End with a 7-section summary: What you built / Files created / Files modified / Services running / Commands run / How to try it / Known gaps.

### Claude streaming (`launch_claude_streaming`)

Claude is run headless: `claude --append-system-prompt-file HELIX_CHEF_PROMPT.md <permission flag> --output-format stream-json --verbose -p "<AGENT_USER_PROMPT>"`. Permission flag is `--dangerously-skip-permissions` (full auto) or `--permission-mode acceptEdits` (scoped). Codex/OpenCode use their own `exec`/`run` subcommands with equivalent flags (`build_agent_argv`).

stdout is piped and parsed line-by-line as NDJSON into `ClaudeEvent` (System / Assistant / User / Other), `ContentBlock` (Text / ToolUse / ToolResult / Other), and `ResultEvent`. `format_tool_use` maps each tool to a one-line status (`✎ Editing …`, `💻 …`, `📋 Updating tasks (N)`, etc.). The status updates the chef spinner **in place** (two-line message via `Step::set_message`) — one line, no scroll spam. The terminal `result` event yields stats (`format_result_stats` → `(37.2s, $0.412)`, baked into the completion line via `Step::set_completion`) and the final summary text (printed via `Step::println`).

**Robustness:** stdin is `Stdio::null()`; the read loop races `tokio::signal::ctrl_c()` (Ctrl-C kills the child + prints the paste hint); `child.wait()` is wrapped in a 5s `timeout` then force-kill so chef never hangs.

### Chef metrics

`chef` emits `started` and `completed` metrics with run metadata, setup mode, agent, duration, and outcome. It does not require Cloud authentication or upload prompts, transcripts, source code, or project snapshots.

### Output helpers (`src/output.rs`)

`Step` (spinner with `start`/`done`/`fail`) gained `println` (print above the spinner), `set_message` (rewrite the spinner line in place), and `set_completion` (override the ✓ line after the fact). `LiveSpinner` wraps indicatif's `ProgressBar`.

## Config & state

**Project config — `helix.toml`** (`HelixConfig` in `config.rs`, found via `ProjectContext::find_and_load`):

- `[project]` — `name` (required), optional `id` / `workspace_id`, `queries` (default `db/`), `container_runtime` (`docker` | `podman`, default docker).
- `[local.<name>]` — `port` (default `6969`), `image` (default `ghcr.io/helixdb/helixdb`), `tag` (default `v0.0.6`), `storage` (`memory` | `disk`, default memory).
- `[enterprise.<name>]` — typed `database = "cluster:<id>"|"tenant:<id>"`, plus optional stable `workspace_id` and `project_id` linkage. Gateway URLs, query keys, query bundles, and sync snapshots are rejected as obsolete.

`HelixConfig::validate` requires a non-empty project name, at least one instance, non-empty instance names, and a valid typed database for each Cloud instance. `default_config()` seeds a single in-memory `local.dev`.

**User-level state — `~/.helix/`:** `credentials` contains only the rotating WorkOS session, and `metrics.toml` contains telemetry preferences. There is no global workspace/project selection file and no Cloud query key.

## Testing

`cargo fmt -p helix-cli && cargo test -p helix-cli` — ~66 lib + ~21 binary tests. clap parsing tests live in `src/main.rs` `#[cfg(test)]` (every command/flag combo). Config (de)serialization + backward-compat defaults are tested in `src/config.rs`.

Chef tests cover: prompt rendering (intent substitution, CRM fallback, Next.js stack keywords, summary sections, browser-open commands, dev-server persistence), agent-priority order, `build_agent_argv` per (agent, permission) combo, install-arg construction, tool-use formatting, and stream-json event parsing. The actual agent spawn / browser open are not unit-tested (require external processes) — verify those manually with `cargo run -p helix-cli -- chef`.

Doc-tests in `output.rs` run as part of `cargo test -p helix-cli --doc`.
