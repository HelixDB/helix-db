# helix-cli

The Helix CLI — binary `helix`, crate `helix-cli` (v3.0.1). It is a **runtime orchestrator**, not a compiler.

## v2 → v3 shift (read this first)

This CLI has **no `helix compile` and no `helix check`**, and there is **no `.hx` query workflow** in it. (Older notes/memory that mention those commands describe the v2 CLI and are stale.) In v3:

- **Queries are JSON requests** sent to a *running* instance via `POST /v2/query` (`helix query`). Validation happens server-side, in the instance.
- **Local instances are Docker/Podman containers** (image `ghcr.io/helixdb/helixdb:v0.0.11`), managed by `LocalRuntime`. `helix start` starts one; in-memory by default, on-disk (SeaweedFS-backed) with `--disk`.
- **Cloud instances are linked resources.** The CLI authenticates only with a rotating WorkOS session and sends Cloud queries through WFE's backend broker. It does not deploy query bundles or call Cloud gateways directly.

The Rust DSL builder lives in `sdks/rust/` (a client library), not in this CLI.

## Entry point & dispatch

`src/main.rs` defines the clap `Cli` struct with three **global flags** — `--json` (machine output, never prompts), `--quiet` (errors + final result only), and `-v/--verbose` (timing detail) — mapped onto `output::OutputMode` (`Human(Verbosity)` | `Json`; `--json` conflicts with the other two). The `Commands` enum lists every subcommand; `main()` matches each to `commands::<name>::run(...)`. With no subcommand it prints the welcome banner (`display_welcome`). A failed command goes through `output::report_error` (cliclack-styled block, or `{"error":{…}}` under `--json`) before `exit(1)`; clap usage errors under `--json` become the same JSON with exit 2. Metrics are bootstrapped (`MetricsSender::new`) and an update check runs on every non-JSON invocation.

## Output contract

- **stdout** carries only a command's result: tables (`output::table::Table`), detail views (`table::key_values`), query results (`output::json::pretty`, syntax-highlighted), one-time tokens, or the `--json` payload. Everything goes through `output::emit(&value, |value| human render)`.
- **stderr** carries all chrome in the cliclack style (`HelixTheme`): `intro`/`outro` sessions, `step`/`success`/`info`/`warning`/`remark`, `note`/`next_steps`, `Step` spinners, and errors.
- `--json` prints no chrome, never prompts (`prompts::is_interactive()` is false), and emits compact JSON. Prompts need an interactive stdin **and stderr**.
- Colours come from `console` (honours `NO_COLOR`/`CLICOLOR`, per stream); never use owo directly.

## Module map (`src/`)

- `commands/` — one module per subcommand (the handlers below).
- `lib.rs` — the public crate root; defines the subcommand enums (`InitTarget`, `AddTarget`, `AuthAction`, `MetricsAction`, `WorkspaceAction`, `ProjectAction`, `ClusterAction`, `DatabaseAction`, `ServiceCredentialAction`, `CloudApiAction`), the shared `ScopeArgs` (`--workspace`/`--project`), and re-exports modules.
- `cloud/` — `client.rs` (strict WorkOS session loading, locked refresh rotation, WFE requests, typed `fetch`, paginated `list`), `model.rs` (typed `Workspace`/`Project`/`Cluster`/`Tenant`/`Database`/`DatabaseKey`/`ServiceCredential`, unknown fields kept in `extra`), and `resolve.rs` (`Scope`: resolves optional ID/slug/name arguments via explicit arg → helix.toml `Link` → sole candidate → cliclack picker → error listing candidates).
- `config.rs` — `helix.toml` (`HelixConfig`) load/save/validation; stable project and typed database linkage only.
- `project.rs` — `ProjectContext::find_and_load()` walks up the tree to find `helix.toml`; resolves `.helix/<instance>` state dirs; `resolve_local_instance` picks the instance for start/stop/restart. `get_helix_cache_dir()` honors `HELIX_CACHE_DIR`.
- `local_runtime.rs` — `LocalRuntime`: Docker/Podman container lifecycle (`check_available`, `container_name` = `helix-<project>-<instance>`, pull/run/stop/restart/status/prune). Disk mode also spins up a SeaweedFS S3 container + volume + network (and removes MinIO sidecars left by older releases); memory mode is the Helix container alone. Health-checks via TCP probe.
- `service_endpoints.rs` — resolves the WFE base URL from `CLOUD_AUTHORITY` or the production default.
- `metrics_sender.rs` — `MetricsSender` + `MetricsConfig` (level Full/Basic/Off, user_id, email). Async event sender to the logs endpoint; created in `main`, `shutdown()` on exit.
- `port.rs` — `is_port_available`, `find_available_port`, `ensure_port_available` (scans up to 100 ports).
- `update.rs` — `check_for_updates` (cached 24h in `~/.helix/`), `current_version`; `commands::update` does the actual `self_update` binary swap.
- `output/` — the output contract above: `OutputMode`/`Verbosity` (global atomic), `emit`, chrome helpers, `Operation` (an intro/outro session), `Step` (live spinner on a TTY, plain lines off one, hidden when quiet/JSON; `println`/`set_message`/`set_completion`), `report_error`/`print_error`, `table` (tables, key/values, state colouring), `json::pretty`, and `HelixTheme`.
- `prompts.rs` — `is_interactive()` and cliclack prompts (`confirm`, `input_instance_name`, `input_port`, `select_local_disk_mode`, …).
- `utils.rs` — `command_exists` (via `which`), `add_env_var_to_file`.
- `errors.rs` — `CliError` (message/context/caused_by/hint/candidates; `Serialize`) with cliclack-style `render()` and `from_report`; typed `ConfigError`/`ProjectError`/`PortError` that convert into it.

## Command reference

Instance args default to `dev`, else the only instance, else a prompt (TTY), else an error listing candidates. Cloud commands build a `Scope` (which calls `require_auth()`); every Cloud resource argument is optional and accepts an ID, slug, or name. A Cloud group with no subcommand lists it.

**Project setup**
- `init [--path <dir>] (local|cloud)` — scaffold a project. Cloud resolves `[--database] [--project] [--workspace]` through `Scope` (no flags needed) and writes the typed database plus the `[project] id/workspace_id` link; it stores no gateway URL or query credential.
- `chef` (alias `cook`) — interactive one-shot bootstrapper that hands off to an AI agent. **See dedicated section below.**
- `add [--path <dir>] (local|cloud)` — add an instance to an existing `helix.toml` without clobbering others. Cloud defaults to the linked project (never an already-added database) and refuses a database from another project.

**Local lifecycle**
- `start [instance] [--foreground] [--port <p>] [--disk] [--persist]` (alias `run`) — start a local container (background by default; `--detach` is a hidden alias). `--disk` forces on-disk/SeaweedFS storage for this run; `--persist` writes the resolved port/storage back to `helix.toml`. The in-memory data-loss warning is shown only once per instance (tracked by a `.warned-memory` marker in the instance workspace).
- `stop [instance]` / `restart [instance]` — stop/restart a background container.
- `status [instance]` — project header plus an INSTANCE/KIND/STATUS/ENDPOINT table; an unreachable Cloud database is an `unreachable` row plus a warning, not a failure. Omit instance for all.
- `doctor [instance|cluster:<id>|tenant:<id>] [--url <URL>]` — read-only setup checkup (`commands/doctor.rs`). CLI checks: `cli.version` (fresh release check, honours `HELIX_NO_UPDATE_CHECK`), and for local instances `runtime.daemon` (bounded `info`, never auto-starts), `runtime.container`, `runtime.image`; then `server.reachable` (`/healthz`) and the server's own checks from `GET /v2/diagnostics` (storage, S3 region/latency, cache device/space/warm, memory, index publication, missing indexes, unbounded scans). A stopped container skips server checks; Cloud databases report `cloud.managed` (skip). `<instance>` in server fixes is replaced by the instance name. Exits 1 when any check fails; `--json` prints `{target, uptime_secs?, checks, summary}`.
- `logs [instance] [-f] [--start <rfc3339>] [--end <rfc3339>]` — local: Docker/Podman logs (`-f` follows; no `--json`). Cloud: query errors as a table (default last hour, explicit empty state).
- `prune [instance] [-a/--all] [-y/--yes]` — delete local containers + `.helix/workspace/` dirs. Non-interactive needs an instance or `--all`.
- `delete <instance> [-y/--yes]` — remove instance from `helix.toml` **and** its local runtime state (instance arg required).

**Queries**
- `query [instance|cluster:<id>|tenant:<id>] (--file <req.json> | --body '<json>' | -e/--ts '<ts>' | --ts-file <query.ts>) [--warm] [--host <h>] [--port <p>]` — local targets send to `POST /v2/query` with auth disabled. Cloud targets use the active WorkOS session and WFE's separate read/write broker endpoints; a typed target needs no helix.toml. They never load an application key. The four input flags are mutually exclusive and exactly one is required. TypeScript input is evaluated through the cached `@helix-db/helix-db` SDK. Results print as highlighted JSON with a `status · latency · target` footer on stderr, or compact JSON under `--json`.

**Cloud**
- `auth (login|status|logout)` — WorkOS PKCE login and rotating session credentials. Refresh is serialized and persisted atomically. There is no API-key or service-credential login.
- `workspace (list|get)` — discover workspaces without global selection state.
- `project (list|get|create <name>|delete|link)` — discover/manage projects; `link` (picker when no argument) writes only this `helix.toml` and keeps the local project name.
- `cluster (list|get|indexes)` — discover and inspect existing clusters.
- `database (list|get|create <name>|delete|indexes|key)` — manage tenant databases. Creation returns a default read-write application key once (stdout); the CLI displays but never stores it. `key create|list|revoke <key>` take `--database`.
- `service-credential (create|list|get|update|revoke)` — manage workspace-owned headless automation credentials through the user session (`--workspace` optional). These credentials cannot log the CLI in.
- `api (get|post|patch|delete) <path> [--body <json>]` — call WFE with the active WorkOS session.
- Destructive commands need `-y` when no prompt is possible and check that before any request.
- `push`, `sync`, `config`, `workspace switch`, `project switch`, and `auth create-key` do not exist; nor do `--format`, `--compact`, `--workspace-id`, `--project-id`, or `--cluster-id`.

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

### Output helpers (`src/output/`)

Chef is framed as one `intro`/`outro` session. `Step` provides `println` (print above the spinner), `set_message` (rewrite the spinner line in place), and `set_completion` (override the completion line after the fact). Chef wraps `init`/`start`/`query` in `OutputMode::Human(Verbosity::Silent)` so only its own steps show.

## Config & state

**Project config — `helix.toml`** (`HelixConfig` in `config.rs`, found via `ProjectContext::find_and_load`):

- `[project]` — `name` (required), optional `id` / `workspace_id`, `queries` (default `db/`), `container_runtime` (`docker` | `podman`, default docker).
- `[local.<name>]` — `port` (default `6969`), `image` (default `ghcr.io/helixdb/helixdb`), `tag` (default `v0.0.11`), `storage` (`memory` | `disk`, default memory).
- `[enterprise.<name>]` — typed `database = "cluster:<id>"|"tenant:<id>"`, plus optional stable `workspace_id` and `project_id` linkage. Gateway URLs, query keys, query bundles, and sync snapshots are rejected as obsolete.

`HelixConfig::validate` requires a non-empty project name, at least one instance, non-empty instance names, and a valid typed database for each Cloud instance. `default_config()` seeds a single in-memory `local.dev`.

**User-level state — `~/.helix/`:** `credentials` contains only the rotating WorkOS session, and `metrics.toml` contains telemetry preferences. There is no global workspace/project selection file and no Cloud query key.

## Testing

`cargo fmt -p helix-cli && cargo test -p helix-cli`. clap parsing tests live in `src/main.rs` `#[cfg(test)]` (every command/flag combo). Config (de)serialization is tested in `src/config.rs`. Cloud resolution (ID/slug/name, links, sole candidate, candidates on error, pagination, `--json` purity, init/add cloud linking) is covered end to end with wiremock in `tests/cloud_resolution.rs`; command contracts in `tests/configuration_commands.rs`. `tests/typescript_runtime.rs` needs `npm ci` in `sdks/typescript`; `tests/e2e_runtime.rs` is `#[ignore]`d and needs Docker.

Chef tests cover: prompt rendering (intent substitution, CRM fallback, Next.js stack keywords, summary sections, browser-open commands, dev-server persistence), agent-priority order, `build_agent_argv` per (agent, permission) combo, install-arg construction, tool-use formatting, and stream-json event parsing. The actual agent spawn / browser open are not unit-tested (require external processes) — verify those manually with `cargo run -p helix-cli -- chef`.

Doc-tests in `output/`, `cloud/`, and `errors.rs` run as part of `cargo test -p helix-cli --doc`.
