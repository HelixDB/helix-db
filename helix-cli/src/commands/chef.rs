use crate::InitTarget;
use crate::commands::auth::{Credentials, ensure_auth_or_login};
use crate::config::DEFAULT_LOCAL_PORT;
use crate::enterprise_cloud::cloud_base_url;
use crate::metrics_sender::MetricsSender;
use crate::output::{Step, Verbosity};
use crate::prompts;
use eyre::{Result, eyre};
use flate2::Compression;
use flate2::write::GzEncoder;
use helix_metrics::events::ChefEvent;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::env;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_PROJECT_DIR: &str = "my-first-helix-project";
const INSTANCE_NAME: &str = "dev";
const CHEF_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
const CHEF_SNAPSHOT_MAX_FILES: usize = 2_000;
const CHEF_SNAPSHOT_MAX_FILE_BYTES: u64 = 1024 * 1024;
const CHEF_SNAPSHOT_MAX_TOTAL_BYTES: u64 = 25 * 1024 * 1024;

const DEFAULT_PROJECT_SPEC: &str = r#"You are building a **Personal CRM** as your default MVP because the user did not specify their own intent. Build exactly this — no extra features.

**Entities and edges:**
- `Contact` — properties: `name` (String), `email` (String), `phone` (String, optional), `createdAt` (Timestamp).
- `Company` — properties: `name` (String), `domain` (String, optional), `createdAt` (Timestamp).
- `Interaction` — properties: `kind` (String, one of `"call" | "email" | "note"`), `note` (String), `loggedAt` (Timestamp).
- `Contact -[WORKS_AT]-> Company` with property `since` (I64, year).
- `Contact -[LOGGED]-> Interaction`.

**Queries to write (TypeScript DSL builder functions, grouped under `web/src/lib/queries/`):**
1. `seed` — replace the starter `User` data with 3 Companies, 5 Contacts (each linked to a Company via WORKS_AT), and 6 Interactions (each linked to a Contact via LOGGED). Use `writeBatch()` + `g().addN(...)`, and `forEachParam` for bulk inserts.
2. `addContact` — write, params `name`, `email`, optional `phone`. Returns the created contact id.
3. `addInteraction` — write, params `contactId` (array of i64), `kind` (String), `note` (String). Creates the Interaction and the LOGGED edge from contact to interaction.
4. `listContacts` — read, no params. Returns up to 50 contacts as `{$id, name, email, phone}`.
5. `contactsAtCompany` — read, param `company` (String). Returns contacts at that company (label scan → `.where(eqParam)` → `.in('WORKS_AT')`).
6. `interactionsForContact` — read, param `contactId` (array of i64). The contact's interactions ordered by `loggedAt` desc, limited to 10.
7. `searchContacts` — read, param `q` (String). Up to 25 contacts whose `name` starts with `q` (label scan → `.where(Predicate.startsWith(...))`).

**Frontend (Next.js, App Router, Tailwind, all TypeScript) under `web/`:**
- `web/src/lib/helix.ts` — the `runQuery` helper (see `<frontend>`).
- `web/src/lib/queries/*.ts` — the builder functions above.
- `web/src/app/page.tsx` (Server Component) — renders the contact list (server-fetches `/api/list_contacts`) and embeds `AddContactForm` and `ContactSearch`.
- `web/src/app/_components/AddContactForm.tsx` (Client Component) — form (name, email, phone) → POST `/api/add_contact`.
- `web/src/app/_components/ContactSearch.tsx` (Client Component) — debounced input → GET `/api/search_contacts?q=...`.
- `web/src/app/contact/[id]/page.tsx` (Server Component) — detail view: contact + WORKS_AT company + LOGGED interactions. Embeds `AddInteractionForm`.
- `web/src/app/_components/AddInteractionForm.tsx` (Client Component) — kind dropdown, note textarea → POST `/api/add_interaction`.
- API routes (one per query): `web/src/app/api/{list_contacts,add_contact,contacts_at_company,interactions_for_contact,add_interaction,search_contacts}/route.ts`. Each imports its builder from `@/lib/queries/...` and `runQuery` from `@/lib/helix`, maps client params to typed values, and returns the JSON.
- Styling: Tailwind utility classes throughout. No global CSS beyond `globals.css` from the scaffold.

**Demo flow the user should be able to click through end to end:**
1. Add a Contact.
2. Search for that contact by partial name.
3. Open the contact, see their interactions (empty initially).
4. Add an interaction (`kind: "call"`, `note: "discussed Q3 roadmap"`).
5. Refresh the contact's detail panel; the new interaction appears."#;

const AGENT_PROMPT_TEMPLATE: &str = r#"# HelixDB MVP Builder

<role>
You are a HelixDB expert. The user just ran `helix chef` to bootstrap a new project. Your job: take the build intent below and ship a working MVP — a small set of HelixDB queries authored with the TypeScript DSL (`@helix-db/helix-db`) plus a Next.js + React + Tailwind frontend (all TypeScript) that demonstrates them. Be persistent. Don't stop until every query you wrote returns valid JSON when run against the local DB and the demo flow works in a browser.
</role>

<environment>
`helix chef` already did all of this — do NOT redo any of it:

- Created `helix.toml` with a local instance named `dev` on port `6969`.
- Started the local DB (`helix start dev`). It is running in the background, in-memory.
{seed_state}
- Installed the HelixDB query skills. This project is **TypeScript**, so you author queries with the **TypeScript DSL** (`@helix-db/helix-db`): `helix-query-typescript` is the skill you reach for first — it is the default and authoritative for the builder API. `helix-query-json-dynamic` documents the raw `/v1/query` JSON the DSL emits — your fallback for dynamic-shaped queries or debugging. Also installed: `helix-query-rust`, `helix-query-optimize`, `helix-query-from-gremlin`, `helix-query-from-cypher`, `helix-query-from-hql`, `helix-memory-system`. Invoke them when authoring queries — they are authoritative. Do not guess query syntax.
- If the user wants anything involving **memory or retrieval** — long-term memory, recall/"remember", personalization, agent memory, RAG, semantic/full-text search over their own data, or recommendations — invoke the `helix-memory-system` skill. It is authoritative for the memory/retrieval data model and the generation, updating, deletion, and categorisation lifecycle.
- Installed the Helix docs MCP (`helixdb-docs`). Query it when you need syntax details this prompt does not cover.

Additional skills (Next.js, React, Tailwind, TypeScript) are NOT pre-installed. You install them yourself as part of the workflow — see `<install_more_skills>`.

Existing files you must read before touching:
- `helix.toml` — project config. Do not edit.
- `DESIGN.md` — the Helix brand + styling guide. **Apply it to every component you build.** It defines the color palette, typography, the signature tactical corner brackets, and copy-paste component recipes. Do not edit it; treat it as read-only input.

This project authors HelixDB queries with the **TypeScript DSL** (`@helix-db/helix-db`) — typed builder functions, not hand-written JSON. Never write Rust `.hx` files; there is no compile step. Queries live as builder modules under `web/src/lib/queries/*.ts`; a small server-only runner (`web/src/lib/helix.ts`) serializes them and POSTs to the local DB at `http://localhost:6969/v1/query`. Raw dynamic JSON (`helix query dev --file/--json`) stays available as a fallback for debugging or dynamic-shaped queries. The frontend that consumes these queries is a Next.js app under `web/` (App Router, TypeScript, Tailwind) — see `<frontend>`.
</environment>

<user_intent>
{intent}
</user_intent>

<workflow>
1. **Sketch entities and edges.** Helix has no schema file; labels and properties come into existence the first time you write them. Pick singular labels (`Contact`, not `contacts`). Pick edge labels that read as verbs (`WORKS_AT`, `LOGGED`). Write the sketch as a comment block at the top of `SCHEMA.md`.
2. **Install the Next.js / React / Tailwind / TypeScript skills** before writing any code. See `<install_more_skills>`.
3. **Scaffold the Next.js app** by running this exact command from the project root: `npx create-next-app@latest web --typescript --tailwind --app --eslint --src-dir --import-alias '@/*' --use-npm --yes`. If `web/` already exists from a previous run, delete it first. The scaffold creates `web/package.json`, `web/src/app/`, `web/tailwind.config.ts`, `web/tsconfig.json`, etc.
4. **Install the query DSL:** `cd web && npm i @helix-db/helix-db`. This is how you author every query — typed builders, not hand-written JSON. Also `npm i tsx` (dev) if you want to run query scripts directly.
5. **Add the query runner** at `web/src/lib/helix.ts` — a tiny server-only helper that POSTs a built request to the local DB and returns the parsed JSON. Copy it verbatim from `<frontend>`.
6. **Author your queries as TypeScript builder modules** under `web/src/lib/queries/` (group by entity, e.g. `contacts.ts`, `interactions.ts`). Each query is a function returning a `readBatch()` / `writeBatch()` built with `g()` and the builder steps; use `defineParams` + `param.*` for parameters. See `<typescript_dsl_quickref>` and `<patterns>`. For anything beyond them — `repeat`, `union`, `choose`, vector/text search, aggregations — invoke the `helix-query-typescript` skill. Do not guess.
{seed_step}
8. **Test each query against the running DB** before wiring the UI. Run a one-off script: `web/scripts/<name>.ts` that imports the builder + `runQuery` and prints the result — `cd web && npx tsx scripts/<name>.ts`. (Quick raw-JSON spot check: `console.log(myQuery().toDynamicJson(params, values))` and pipe into `helix query dev --json "$(...)"`.) Fix and retry until each returns the expected shape.
9. **Add one Next.js API route per query** at `web/src/app/api/<query_name>/route.ts`. Each handler imports its builder from `@/lib/queries/...` and `runQuery` from `@/lib/helix`, maps any client-supplied parameters to typed values, runs the query, and returns the JSON. **The browser must NEVER hit `:6969` directly.**
10. **Build the UI** under `web/src/app/`, following `DESIGN.md`. Server Components (the default — no `'use client'`) for read-only views; they `fetch('/api/<name>')` at render time. Client Components (`'use client'`) only for forms or anything that needs state / handlers. Style with Tailwind utility classes per the `DESIGN.md` recipes; the only global CSS you add is the one-time `DESIGN.md` setup block (base dark background + tactical-corner utilities) pasted into `web/src/app/globals.css`. See `<frontend>` for concrete examples.
11. **Start the Next.js dev server in the background.** The dev server is **frontend and backend in one process** — it serves React on `/` and the TypeScript API routes on `/api/*`. Detach it so it survives your bash invocation. Use the shell-portable pattern (works for every agent CLI):

       cd web && nohup npm run dev > .next-dev.log 2>&1 & disown

   If you're Claude Code, you can equivalently use the `Bash` tool's `run_in_background: true` flag.

12. **Verify both layers are up before continuing.** Poll with a small retry loop (up to ~15 attempts, 1s sleeps; Next.js usually warms up in 3–5s):

    - `curl -fsS http://localhost:3000` returns 200 (frontend).
    - `curl -fsS http://localhost:3000/api/<one-of-your-routes>` returns valid JSON (backend).

    If neither responds, read `web/.next-dev.log` for the error and fix it.

13. **Curl every API route** under `web/src/app/api/` against the running server. Each must return the expected JSON shape. Then click through the demo flow in a browser via `http://localhost:3000`.

14. **Open the frontend in the user's default browser.** Try whichever of these matches the platform:

        open http://localhost:3000        # macOS
        xdg-open http://localhost:3000    # Linux
        start http://localhost:3000       # Windows

    If none works (headless box, ssh session, missing utility), skip silently — `helix chef` retries the open after you exit as a safety net.

15. **If you stood up any separate backend processes** (workers, queue consumers, additional Node services — uncommon, but if the MVP required it), background each one the same way: `nohup … > <name>.log 2>&1 & disown`. List every process you started in a `processes.md` file at the project root: name, start command, log path, stop command.

16. **Do NOT stop anything you started.** Leave the Next.js dev server (and any extra backend services) running when you finish. The user opens `http://localhost:3000` immediately after `helix chef` exits — everything must be live.

If a query returns an error: read the error body (`runQuery` surfaces it), check it against `<typescript_dsl_quickref>` / the `helix-query-typescript` skill, fix the builder, retry. Tail the DB with `helix logs dev --follow` in another shell if the Helix-side error is opaque. If in-memory state gets corrupted, `helix restart dev` wipes everything and you can re-run your seed script.
</workflow>

<install_more_skills>
The HelixDB skills (`helix-query-typescript`, `helix-query-json-dynamic`, etc.) are already installed by `helix chef`. You install everything else yourself as you go.

**Install the Next.js / React / Tailwind / TypeScript skill pack first** (Vercel's curated set):

    npx skills add vercel-labs/agent-skills -g -y --all

`-g` puts them in `~/.claude/skills` so they're available across projects; `-y` skips prompts; `--all` installs every skill in the pack to every detected agent. After installing, the new skills become available the next time you invoke a skill — you may need to re-read the skill list to discover them.

For any other tooling you decide is useful (e.g. shadcn, drizzle, prisma, react-hook-form), use the same pattern:

    npx skills add <github-org/repo> -g -y

Check what's already installed before adding more:

    npx skills ls -g

Don't kitchen-sink it. Install only what this project actually needs. One project, one Next.js skill pack — anything more requires a concrete reason.
</install_more_skills>

<typescript_dsl_quickref>
Author every query with `@helix-db/helix-db`. A query is a function returning a `readBatch()` or `writeBatch()`. Import what you need:

```ts
import {
  g, sub, readBatch, writeBatch, NodeRef, EdgeRef,
  Predicate, SourcePredicate, PropertyInput, Expr, Order,
  PropertyProjection, ExprProjection, defineParams, param,
} from '@helix-db/helix-db';
```

**Batch shape** — name each query with `varAs`, list what to return:

```ts
readBatch()
  .varAs('contacts', g().nWithLabel('Contact').limit(50).valueMap(['$id', 'name', 'email']))
  .returning(['contacts']);
```

**Parameters** — declare a schema once, reference by name; pass values when you run it (see `<frontend>`):

```ts
const params = defineParams({ company: param.string(), contactId: param.array(param.i64()) });
```

**Sources** (first step):
- `g().nWithLabel('Contact')` — label scan (indexed). `g().nWithLabelWhere('Contact', SourcePredicate.eq('email', 'a@b.com'))` — label + indexed predicate.
- `g().n(NodeRef.param('contactId'))` / `g().n(NodeRef.var('stored'))` / `g().n([42n])` — by id (params are arrays of i64).
- `g().vectorSearchNodesWith('Doc', 'embedding', PropertyInput.param('vec'), 10, null)` — vector search.
- `g().textSearchNodesWith('Doc', 'body', PropertyInput.param('q'), 10, null)` — BM25 text search.

`SourcePredicate` (index-eligible, for `nWhere`/`nWithLabelWhere`): `eq, neq, gt, gte, lt, lte, between, hasKey, startsWith, and, or`. Anything else (`contains`, `isNull`, `endsWith`, etc.) goes in a `.where(Predicate...)` after the source.

**Traversal:** `.out('WORKS_AT')` / `.in('WORKS_AT')` / `.both('WORKS_AT')` (omit the label for any); `.outE/.inE/.bothE` to edges; `.outN/.inN/.otherN` back to nodes.

**Filters:** `.where(Predicate.eq('kind', 'call'))`, `.has('phone', '...')`, `.hasLabel('Contact')`, `.hasKey('phone')`, `.dedup()`, `.limit(25)`, `.skip(10)`, `.range(0, 25)`.

**Ordering:** `.orderBy('loggedAt', Order.Desc)`, `.orderByMultiple([['priority', Order.Desc], ['name', Order.Asc]])`.

**Mutations** (`writeBatch()` only): `g().addN('Contact', { name: PropertyInput.param('name'), createdAt: PropertyInput.expr(Expr.timestamp()) })`; `.addE('WORKS_AT', NodeRef.param('companyId'), { since: PropertyInput.param('since') })`; `.setProperty('name', PropertyInput.param('newName'))`; `.drop()`; `.dropEdge(NodeRef.param('targetId'))`.

**Parameterized comparisons:** prefer the typed helpers — `.where(Predicate.eqParam('email', 'email'))` (also `gteParam`, `ltParam`, …), or pass an explicit expression like `Predicate.eq('email', Expr.param('email'))`. `addN`/`addE` property values accept `PropertyInput.param('x')` or a `ParamRef` directly.

**Terminals:** `.count()`, `.exists()`, `.values(['name', 'email'])`, `.valueMap(['$id', 'name'])` (or `.valueMap(null)` for all), `.project([PropertyProjection.renamed('$id', 'id'), PropertyProjection.new('name')])`.

**Virtual fields:** `$id`, `$label`, `$from`/`$to` (edges), `$distance` (vector/BM25). **Project `$distance` immediately after the search step**, before any `.out/.in/.both` — traversal drops it.

**Serialize / run:** `myQuery().toDynamicRequest(params, values)` (object) or `.toDynamicJson(params, values)` (string body). No-param queries take no args: `myQuery().toDynamicJson()`. Use `bigint` (`25n`) or `i64(...)` for large integers; serialize with the SDK helpers, never raw `JSON.stringify`.

For anything beyond this cheat sheet (`repeat`, `union`, `choose`, `coalesce`, `optional`, `aggregateBy`, `groupCount`, `forEachParam`, expression math, the full predicate/projection surface) — invoke the `helix-query-typescript` skill. Do not guess.

**Fallback — raw JSON.** Dynamic JSON sent to `helix query dev --json/--file` is the fallback for quick debugging or dynamic-shaped queries you can't express in the builder. The DSL emits exactly this JSON; its encoding rules (tagged `PropertyValue`, lowercase `request_type`, untagged `Project`, etc.) live in the `helix-query-json-dynamic` skill — invoke it if you drop down to raw JSON.
</typescript_dsl_quickref>

<patterns>
All patterns are TypeScript DSL builder functions. Define a `params` schema with `defineParams` for parameterized queries; pass the matching values when you run the query (see `<frontend>`).

**1. Create one node (write):**
```ts
const params = defineParams({ name: param.string(), email: param.string() });
const addContact = (p = params) =>
  writeBatch()
    .varAs('created',
      g().addN('Contact', {
        name: PropertyInput.param('name'),
        email: PropertyInput.param('email'),
        createdAt: PropertyInput.expr(Expr.timestamp()),
      }).valueMap(['$id', 'name', 'email', 'createdAt']))
    .returning(['created']);
```

**2. Bulk seed via `forEachParam` (write):** runs the body once per object in the `data` array.
```ts
const params = defineParams({ data: param.array(param.object(param.value())) });
const seed = (p = params) =>
  writeBatch()
    .forEachParam('data',
      writeBatch().varAs('created',
        g().addN('Contact', { name: PropertyInput.param('name'), email: PropertyInput.param('email') })))
    .returning(['created']);
// values: { data: [{ name: 'Ada', email: 'ada@example.com' }, { name: 'Grace', email: 'grace@example.com' }] }
```
Inside the body, each object's fields (`name`, `email`) are scoped as params for the inner query.

**3. Create an edge between two existing nodes by id (write):**
```ts
const params = defineParams({ contactId: param.array(param.i64()), companyId: param.array(param.i64()), since: param.i64() });
const linkEmployer = (p = params) =>
  writeBatch()
    .varAs('linked',
      g().n(NodeRef.param('contactId'))
        .addE('WORKS_AT', NodeRef.param('companyId'), { since: PropertyInput.param('since') }))
    .returning(['linked']);
// node-id params are arrays of i64: { contactId: [1n], companyId: [2n], since: 2024n }
```

**4. Indexed lookup by a property (read):** prefer the parameterized form.
```ts
const params = defineParams({ email: param.string() });
const contactByEmail = (p = params) =>
  readBatch()
    .varAs('contact',
      g().nWithLabel('Contact')
        .where(Predicate.eqParam('email', 'email'))
        .valueMap(['$id', 'name', 'email', 'createdAt']))
    .returning(['contact']);
```
For a literal (non-parameterized) value, fold it into the source: `g().nWithLabelWhere('Contact', SourcePredicate.eq('email', 'ada@example.com'))` — that uses the index.

**5. Multi-hop traversal — contacts at a company (read):**
```ts
const params = defineParams({ company: param.string() });
const contactsAtCompany = (p = params) =>
  readBatch()
    .varAs('contacts',
      g().nWithLabel('Company')
        .where(Predicate.eqParam('name', 'company'))
        .in('WORKS_AT')
        .dedup()
        .valueMap(['$id', 'name', 'email']))
    .returning(['contacts']);
```

**6. Ordered, limited traversal — recent interactions for a contact (read):**
```ts
const params = defineParams({ contactId: param.array(param.i64()) });
const interactionsForContact = (p = params) =>
  readBatch()
    .varAs('interactions',
      g().n(NodeRef.param('contactId'))
        .out('LOGGED')
        .orderBy('loggedAt', Order.Desc)
        .limit(10)
        .valueMap(['$id', 'kind', 'note', 'loggedAt']))
    .returning(['interactions']);
```

**7. Prefix search (read):** `startsWith` is not allowed at the source — use `.where(...)` after the label scan.
```ts
const params = defineParams({ q: param.string() });
const searchContacts = (p = params) =>
  readBatch()
    .varAs('matches',
      g().nWithLabel('Contact')
        .where(Predicate.startsWith('name', 'Ad'))   // literal prefix
        .limit(25)
        .valueMap(['$id', 'name', 'email']))
    .returning(['matches']);
```
For a parameterized prefix, invoke the `helix-query-typescript` skill (parameterized `startsWith` variant).

**8. Semantic search over embeddings (read).** Generate the query embedding server-side with OpenAI's latest model, then pass the vector to the DSL. Store an `embedding` (F64 array) on each node at write time with the same model + dimensions.
```ts
const params = defineParams({ vector: param.array(param.f64()), k: param.i64() });
const semanticSearch = (p = params) =>
  readBatch()
    .varAs('hits',
      g().vectorSearchNodesWith('Doc', 'embedding', PropertyInput.param('vector'), Expr.param('k'), null)
        .project([
          PropertyProjection.renamed('$id', 'id'),
          PropertyProjection.new('title'),
          PropertyProjection.renamed('$distance', 'distance'),   // project distance BEFORE any traversal
        ]))
    .returning(['hits']);
```
See `<embeddings>` for the OpenAI call that produces `vector`.
</patterns>

<frontend>
The frontend is a Next.js 15 app (App Router, TypeScript, Tailwind) under `web/`. Scaffolded by `npx create-next-app@latest web --typescript --tailwind --app --eslint --src-dir --import-alias '@/*' --use-npm --yes` (see step 6 of `<workflow>`).

**All UI must follow `DESIGN.md` — the Helix brand guide at the project root. Read it before writing any component.** It is the source of truth for colors (forced dark theme, orange `#FF5C01` accent), typography (Geist sans + mono), shape (`rounded-none` everywhere), the signature tactical corner brackets, and copy-paste component recipes (buttons, cards, inputs, lists, badges). Right after scaffolding, paste the `globals.css` setup block from `DESIGN.md` (base dark background + the `.tactical-corners` / `.btn-tactical` / `.thin-scrollbar` utilities) into `web/src/app/globals.css` **once**, then build every component from its recipes. Install `lucide-react` (`cd web && npm i lucide-react`) for icons. The examples below show the *data flow*; their Tailwind classes already match `DESIGN.md` — keep that styling, don't revert to generic/light Tailwind.

Four concrete file shapes you write. **Every Helix call is server-side; the browser never talks to `:6969`.**

**0. The query runner — `web/src/lib/helix.ts`** (write once; both API routes and seed/test scripts import it). It takes a built request and POSTs its JSON to the local DB:

```typescript
import type { DynamicQueryRequest } from '@helix-db/helix-db';

const HELIX_URL = process.env.HELIX_URL ?? 'http://localhost:6969/v1/query';

export async function runQuery(request: DynamicQueryRequest): Promise<unknown> {
  const res = await fetch(HELIX_URL, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: request.toJsonString(),
    cache: 'no-store',
  });
  if (!res.ok) throw new Error(`Helix ${res.status}: ${await res.text()}`);
  return res.json();
}
```

**1. A query module — `web/src/lib/queries/contacts.ts`** (group queries by entity; one exported builder per query). See `<typescript_dsl_quickref>` and `<patterns>`:

```typescript
import { defineParams, Expr, g, param, PropertyInput, readBatch, writeBatch } from '@helix-db/helix-db';

export const listContacts = () =>
  readBatch()
    .varAs('contacts', g().nWithLabel('Contact').limit(50).valueMap(['$id', 'name', 'email', 'phone']))
    .returning(['contacts']);

export const addContactParams = defineParams({ name: param.string(), email: param.string() });
export const addContact = (p = addContactParams) =>
  writeBatch()
    .varAs('created',
      g().addN('Contact', {
        name: PropertyInput.param('name'),
        email: PropertyInput.param('email'),
        createdAt: PropertyInput.expr(Expr.timestamp()),
      }).valueMap(['$id', 'name', 'email']))
    .returning(['created']);
```

**2. An API route — `web/src/app/api/list_contacts/route.ts`** (one per query; imports the builder + runner, maps client params to typed values):

```typescript
import { NextRequest, NextResponse } from 'next/server';
import { runQuery } from '@/lib/helix';
import { addContact, addContactParams, listContacts } from '@/lib/queries/contacts';

export async function GET() {
  return NextResponse.json(await runQuery(listContacts().toDynamicRequest()));
}

export async function POST(req: NextRequest) {
  const { parameters } = await req.json().catch(() => ({ parameters: {} }));
  const request = addContact().toDynamicRequest(addContactParams, {
    name: String(parameters.name ?? ''),
    email: String(parameters.email ?? ''),
  });
  return NextResponse.json(await runQuery(request));
}
```

**3. Server Component — `web/src/app/page.tsx`** (default; no `'use client'`):

```typescript
import AddContactForm from './_components/AddContactForm';

async function getContacts() {
  const res = await fetch('http://localhost:3000/api/list_contacts', { cache: 'no-store' });
  return res.json();
}

export default async function Home() {
  const data = await getContacts();
  return (
    <main className="min-h-screen bg-[#1E1715] text-[#EBD9AF] font-sans">
      <div className="mx-auto max-w-2xl space-y-6 px-6 py-10">
        <h1 className="text-2xl font-semibold tracking-tight text-[#FDFBF0]">Contacts</h1>
        <AddContactForm />
        <ul className="divide-y divide-white/10 border border-white/10 bg-[#070504]">
          {data.contacts.map((c: { $id: number; name: string; email: string }) => (
            <li key={c.$id} className="px-4 py-3 transition-colors hover:bg-[#FF5C01]/10">
              <a href={`/contact/${c.$id}`} className="text-[#FDFBF0]">{c.name}</a>
              <span className="ml-2 text-sm text-[#EBD9AF]/60">{c.email}</span>
            </li>
          ))}
        </ul>
      </div>
    </main>
  );
}
```

**4. Client Component — `web/src/app/_components/AddContactForm.tsx`** (only because of state + handlers):

```typescript
'use client';
import { useState, useTransition } from 'react';
import { useRouter } from 'next/navigation';

export default function AddContactForm() {
  const router = useRouter();
  const [name, setName] = useState('');
  const [email, setEmail] = useState('');
  const [pending, start] = useTransition();

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    await fetch('/api/add_contact', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ parameters: { name, email } }),
    });
    setName(''); setEmail('');
    start(() => router.refresh());
  }

  return (
    <form onSubmit={submit} className="flex flex-wrap gap-2">
      <input
        className="flex-1 rounded-none border border-white/10 bg-transparent px-3 py-2 text-[#FDFBF0] placeholder:text-[#EBD9AF]/50 transition-colors focus:border-[#FF5C01]/50 focus:outline-none"
        placeholder="Name"
        value={name}
        onChange={(e) => setName(e.target.value)}
        required
      />
      <input
        className="flex-1 rounded-none border border-white/10 bg-transparent px-3 py-2 text-[#FDFBF0] placeholder:text-[#EBD9AF]/50 transition-colors focus:border-[#FF5C01]/50 focus:outline-none"
        placeholder="Email"
        type="email"
        value={email}
        onChange={(e) => setEmail(e.target.value)}
        required
      />
      <button
        type="submit"
        disabled={pending}
        className="tactical-corners rounded-none bg-[#FF5C01]/15 px-4 py-2 font-mono text-sm uppercase tracking-wider text-[#FF5C01] transition-all hover:bg-[#FF5C01] hover:text-[#1E1715] disabled:opacity-50"
      >
        {pending ? 'Adding...' : 'Add'}
      </button>
    </form>
  );
}
```

**Rules of thumb:**

- One API route per query. The route imports the builder from `@/lib/queries/...` and `runQuery` from `@/lib/helix`; it never reads files from disk.
- Parameterized queries: read the client `parameters` from the request body, coerce them to typed values (`String(...)`, `BigInt(...)`, arrays of ids as `bigint[]`), and pass them to `builder().toDynamicRequest(params, values)`.
- Server Components (the default) for read-only views — they `fetch` from the API routes at render time. After mutations, call `router.refresh()` to re-run the server fetch.
- Client Components (`'use client'` at the top of the file) only when you need `useState`, `onClick`, etc.
- Place Client Components under `web/src/app/_components/`. The leading underscore opts the directory out of routing.
- Tailwind utility classes per `DESIGN.md`. The only global CSS is the `DESIGN.md` setup block in `globals.css` (base dark theme + tactical-corner / scrollbar utilities) — no other custom CSS files.
- App Router (`web/src/app/`) only. Do not create `web/src/pages/` — that's the legacy router.
</frontend>

<embeddings>
Only if the app needs embeddings — semantic/similarity search, RAG over the user's documents, or recommendations. Skip this entirely for plain CRUD apps.

- **Model:** use OpenAI's latest embedding model — `text-embedding-3-small` by default (1536 dims, cheap, fast), or `text-embedding-3-large` (3072 dims) when the user wants higher accuracy. Do not use the legacy `text-embedding-ada-002`.
- **Server-side only.** Compute embeddings in an API route or a server module, never in the browser. Read the key from `process.env.OPENAI_API_KEY` (the agent adds it to `web/.env.local`; never hardcode it, never ship it to the client). If no key is set, surface a clear error and tell the user to add `OPENAI_API_KEY` to `web/.env.local`.
- **Consistency:** embed with the **same model and dimensions** at write time (when you store the vector) and at query time (the search vector). Store the vector as an F64-array property (e.g. `embedding`) on the node via `addN`/`setProperty`.
- **Query:** pass the query vector to `g().vectorSearchNodesWith('Label', 'embedding', PropertyInput.param('vector'), k, null)` (pattern 8). Project `$distance` before any traversal.

Minimal embedding helper (`web/src/lib/embed.ts`):

```typescript
export async function embed(text: string): Promise<number[]> {
  const key = process.env.OPENAI_API_KEY;
  if (!key) throw new Error('Set OPENAI_API_KEY in web/.env.local to use embeddings.');
  const res = await fetch('https://api.openai.com/v1/embeddings', {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: `Bearer ${key}` },
    body: JSON.stringify({ model: 'text-embedding-3-small', input: text }),
  });
  if (!res.ok) throw new Error(`OpenAI ${res.status}: ${await res.text()}`);
  const json = await res.json();
  return json.data[0].embedding as number[];
}
```

For memory / long-term-recall / RAG apps specifically, invoke the `helix-memory-system` skill — it is authoritative for the data model and the write/maintain lifecycle, and it assumes this same embedding approach.
</embeddings>

<cli_commands>
The commands you should run while building:

**Queries (TypeScript DSL — the default):**
- `cd web && npm i @helix-db/helix-db` — install the query builder (once per project).
- `cd web && npx tsx scripts/<name>.ts` — run a query/seed script that imports a builder + `runQuery` (see `<frontend>`).

**HelixDB CLI (raw-JSON fallback + DB control):**
- `helix query dev --json '<inline json>'` — run a one-off raw-JSON query (fallback / quick check; pipe `myQuery().toDynamicJson(...)` into it).
- `helix query dev --file <name>.json --compact | jq` — run a saved raw-JSON request and inspect the shape.
- `helix logs dev --follow` — tail DB logs in another shell; ctrl-C when done.
- `helix restart dev` — wipe in-memory state. Re-run your seed script afterwards.
- `helix status dev` — sanity check that the DB is up.

**Skills / scaffolding:**
- `npx skills add vercel-labs/agent-skills -g -y --all` — install Next.js / React / Tailwind / TypeScript skills (run once per machine).
- `npx skills ls -g` — list installed skills before deciding what else to add.
- `npx create-next-app@latest web --typescript --tailwind --app --eslint --src-dir --import-alias '@/*' --use-npm --yes` — scaffold the frontend (run once per project).

**Next.js:**
- `cd web && nohup npm run dev > .next-dev.log 2>&1 & disown` — start the Next.js dev server in the background (port 3000). Survives your bash invocation; the user opens the URL after `helix chef` exits.
- `tail -f web/.next-dev.log` — watch the dev server's output when debugging.
- `pkill -f 'next dev'` — stop the dev server (the user runs this when they're done).
- `cd web && npm run build && npm run start` — production-ish check; not required for the MVP.

Do NOT run:
- `helix init`, `helix chef`, `helix start dev` — already done. Re-running can fail or duplicate state.
- `helix push`, `helix sync`, `helix deploy` — V2 Cloud commands; the user is on a local DB.
- `helix prune`, `helix delete` — destructive. Only the user runs these.

When `helix query` fails, the response body (or stderr) contains the error. Common causes are in `<antipatterns>`.
</cli_commands>

<antipatterns>
- DO NOT hand-write tagged-JSON query ASTs — author every query with the `@helix-db/helix-db` builder. Raw JSON is only a debugging fallback; its encoding rules live in the `helix-query-json-dynamic` skill.
- DO NOT guess builder method names — check `<typescript_dsl_quickref>` / `<patterns>` or invoke the `helix-query-typescript` skill.
- DO NOT put non-indexed predicates (`contains`, `isNull`, `isNotNull`, `endsWith`, `not`) at the source (`nWhere` / `nWithLabelWhere`) — use a `.where(Predicate...)` step after the source.
- DO NOT pass a plain runtime value to `Predicate.eq('prop', value)` expecting a parameter lookup — use `Predicate.eqParam('prop', 'paramName')` (and `gteParam`, etc.) or `Predicate.eq('prop', Expr.param('paramName'))`; in mutations use `PropertyInput.param('x')`.
- DO NOT mix mutations into a `readBatch()` — use `writeBatch()` for anything that adds/updates/deletes.
- DO NOT project `$distance` after `.out` / `.in` / `.both` — traversal drops it. Project it immediately after the search step.
- DO NOT pass a single id as a scalar to `g().n(...)` — use an array (`[42n]`), `NodeRef.param('ids')` (param typed as an array of i64), or `NodeRef.var('stored')`.
- DO NOT call `JSON.stringify` on a request — use `.toJsonString()` / `.toDynamicJson(...)` (bigint-safe).
- DO NOT write `.hx` files or invoke `helix compile` — there is no compile step.
- DO NOT re-run `helix init` / `helix start dev` — already running.
- DO NOT use plural label names (`Contacts`). Convention is singular (`Contact`). Edge labels are `SCREAMING_SNAKE` verbs (`WORKS_AT`).
- DO NOT write static `.html` files or hand-rolled CSS / JS for the frontend. The frontend is a Next.js app under `web/`; everything goes through the App Router and Tailwind.
- DO NOT have the browser fetch `http://localhost:6969/v1/query`. Every Helix call goes through a Next.js API route handler in `web/src/app/api/<name>/route.ts`. Server-only.
- DO NOT write any server / glue code in JavaScript or in any language other than TypeScript. Helix itself is the DB; everything you add is TypeScript.
- DO NOT use the legacy `pages/` router. The scaffold uses the App Router (`web/src/app/`) — keep it.
- DO NOT omit the `--src-dir` flag when running `create-next-app`. Routes, examples, and paths throughout this prompt all assume `web/src/app/...`.
- DO NOT install random npm packages directly. Install skill packs first (`npx skills add ...`) and let the skill guide what gets added.
- DO NOT ignore `DESIGN.md`. The frontend must use the Helix dark theme — no `bg-white` / light surfaces, no `rounded-lg` / `rounded-xl` on cards or buttons (use `rounded-none`), no blue / slate / indigo accents (the accent is orange `#FF5C01`). Don't pull in a UI kit (shadcn / MUI / Chakra) — the `DESIGN.md` recipes are the system.
- DO NOT add features the user did not ask for. Build the MVP, then stop.
</antipatterns>

<deploy_imperative>
Before you end your turn, all three of these must be true:

1. Every query you wrote (the TypeScript builders under `web/src/lib/queries/`) runs against the local DB and returns a JSON body, not an error — verified via its API route or a `tsx` script.
2. The Next.js dev server is running in the background on `http://localhost:3000` and is **still running when you finish.** Both the frontend (the App Router pages) and the backend (every API route under `web/src/app/api/`) are responsive — the API routes return valid JSON when called via `curl`, and every form / link in the UI works (adding data, listing it, navigating to detail views). Any additional backend processes you spun up are likewise still running and listed in `processes.md`.
3. The user can click through the demo flow described in `<user_intent>` end to end.

If any is not true: read the error, fix the query / route / component, retry. Tail `helix logs dev --follow` if the Helix-side error is opaque, or read the Next.js dev server output for SSR / route errors. Be persistent. Do not stop until the demo works.

**Final summary — print this and then stop.** The user reads only this; make it scannable. Use exactly these seven sections, in this order:

### What you built
One or two sentences naming the entities, edges, and what the Next.js frontend demonstrates. No marketing language.

### Files created
Bullet list of every new file (`SCHEMA.md`, `web/src/lib/helix.ts`, `web/src/lib/queries/*.ts`, `web/scripts/*.ts`, `web/src/app/api/*/route.ts`, `web/src/app/page.tsx`, `web/src/app/_components/*.tsx`, `web/src/app/<route>/page.tsx`, anything else). One line per file with a 3–8-word description of its purpose. You can group the `web/` files generated by `create-next-app` under a single line like "web/ — Next.js scaffold (package.json, tsconfig.json, tailwind.config.ts, etc.)". Do NOT list `DESIGN.md`, `HELIX_CHEF_PROMPT.md`, or `helix.toml` — those are pre-existing inputs created by `helix chef`, not by you (note `web/src/app/globals.css` under "Files modified" if you pasted the design setup block into it).

### Files modified
Bullet list of files that already existed and were changed (e.g. `web/src/app/globals.css` if you pasted the design block, or the starter `examples/*.json` if you touched them). One line per file describing what changed. Empty list if you didn't modify anything.

### Services running
Every long-lived process you left running, one bullet each, in the format `name · URL or PID · log file · stop command`. Example:
- `Next.js dev server · http://localhost:3000 · web/.next-dev.log · pkill -f 'next dev'`
- `(extra service, if any) · http://localhost:4000 · workers.log · pkill -f 'worker'`

### Commands run
Significant commands you executed during this run, in chronological order, one per line. Include skill installs, the `create-next-app` scaffold, `npm i @helix-db/helix-db`, the dev server start, every query/seed script run (`npx tsx ...`), every `curl http://localhost:3000/api/...`, and the browser-open. Skip filler like `ls`, `cat`, `pwd`. The user must be able to replay any one of these by copy-paste.

### How to try it
- The Next.js dev server is already running. The browser should be open at `http://localhost:3000` (chef will open it if you couldn't).
- Brief click-through walkthrough of the demo flow (2–4 bullets covering the main UI actions).

### Known gaps
Anything you couldn't finish or that's flaky. Empty list if everything works. Be honest — do not paper over broken behavior.

Nothing else after these seven sections. No closing pleasantries, no offer of next steps.
</deploy_imperative>
"#;

/// The Helix brand + styling guide. Written verbatim to `DESIGN.md` in every chef
/// project and referenced from `AGENT_PROMPT_TEMPLATE` (`<environment>` + `<frontend>`).
/// It is fully self-contained — the generated project has no access to the
/// helix-website repo, so every token, CSS utility, and component recipe is inlined
/// here. Mirrors the real product (website / dashboard / explorer): forced dark
/// theme, `#FF5C01` orange accent, `rounded-none`, tactical corner brackets.
const DESIGN_GUIDE: &str = r##"# Helix Design Guide

This is the visual identity for HelixDB. **Every UI you build for this project must follow it.**
The goal: a frontend that looks like it belongs inside the Helix product (the dashboard and graph
explorer), not a generic create-next-app demo.

The aesthetic in three words: **dark, sharp, tactical.** Near-black backgrounds, a single hot
orange accent, square corners (no rounding), thin hairline borders, and small monospaced labels in
uppercase. Decorative orange "corner brackets" frame primary actions and key cards.

---

## 1. Color tokens

Forced dark theme — there is no light mode. Use these exact values (as Tailwind arbitrary classes,
e.g. `bg-[#1E1715]`, `text-[#EBD9AF]`, `border-[#FF5C01]`):

| Role                     | Value                      | Usage                                                        |
|--------------------------|----------------------------|--------------------------------------------------------------|
| Page background          | `#1E1715`                  | `<body>` / outermost shell. Dark warm brown-black.           |
| Panel / card background  | `#070504`                  | Cards, panels, sidebars, popovers. Almost pure black.        |
| Brand accent (orange)    | `#FF5C01`                  | The ONLY accent. CTAs, active states, focus, links, corners. |
| Primary text             | `#FDFBF0`                  | Headings, active labels, important values. Near-white.       |
| Secondary / muted text   | `#EBD9AF`                  | Body text, labels. Tan/cream. Use opacities below for dimming.|
| Borders / hairlines      | `rgba(255,255,255,0.10)`   | `border-white/10` — the default border everywhere.           |
| Card border (alt)        | `#EBD9AF` @ 10%            | `border-[#EBD9AF]/10` — borders on `#070504` cards.          |
| Destructive / error      | `hsl(0 62.8% 30.6%)`       | Delete buttons, error states. Use sparingly.                 |

**Muted text opacities** (apply to `#EBD9AF`): `/70` and `/60` for secondary text, `/50` for
placeholders, `/20` for the faintest hints/dividers. Example: `text-[#EBD9AF]/60`,
`placeholder:text-[#EBD9AF]/50`.

**Rules:**
- Orange `#FF5C01` is an *accent*, not a fill. Most of the screen is dark; orange draws the eye to
  the one thing that matters (primary action, current selection). Don't flood the page with it.
- Never use white (`bg-white`), light grays (`slate`, `gray-100`), or any blue/indigo/violet accent.
  The accent is always orange.
- Hover states move *toward* orange: `hover:bg-[#FF5C01]/10`, `hover:text-[#FF5C01]`, or a solid
  `hover:bg-[#FF5C01]` for primary buttons.

**Data-viz / categorical palette** (charts, graph nodes, tags — when you need multiple distinct
colors): `#FF7A00`, `#FFC857`, `#7ECBFF`, `#B084FF`, `#55E6A5`, `#FF8FA3`, `#8AF0FF`, `#F7A8FF`.
Assign by hashing the category name so colors stay stable. Highlighted/selected edges use the
brand orange `#FF5C01`.

---

## 2. Typography

`create-next-app` already wires **Geist** (sans) and **Geist Mono** via `next/font` — no install
needed. The variables are usually `--font-geist-sans` and `--font-geist-mono`, exposed as Tailwind's
`font-sans` and `font-mono`.

- **Body / prose:** `font-sans`.
- **Labels, controls, table headers, metadata, code:** `font-mono`. The product leans heavily on
  mono for chrome. Small mono labels are typically `text-[10px]` or `text-xs`, `uppercase`,
  `tracking-wider`, dimmed (`text-[#EBD9AF]/60`).
- **Headings:** `font-semibold` (or `font-bold`), `tracking-tight`. E.g. page title
  `text-2xl font-semibold tracking-tight text-[#FDFBF0]`.

---

## 3. Shape & borders

- **`rounded-none` everywhere.** Buttons, cards, inputs, modals — all square. The only exceptions
  are avatars and status dots (`rounded-full`). Never `rounded-lg`/`rounded-xl` on cards/buttons.
- **Hairline borders** (`border border-white/10`) are the primary way surfaces are separated — not
  shadows. Use borders and the dark/darker background contrast (`#1E1715` vs `#070504`) for depth.
- **Transitions:** `transition-colors` (or `transition-all`) ~`duration-200` on anything
  interactive.

---

## 4. Tactical corner brackets (signature element)

Small orange L-shaped brackets at the four corners of an element — Helix's most recognizable visual
motif. Put them on **primary CTAs and hero/feature cards** (not every element — they're a highlight).

Add these utilities **once** to `web/src/app/globals.css` (plain CSS, works under Tailwind v3 and
v4), then apply the class names in JSX:

```css
/* ---- Helix tactical corner brackets ---- */
.tactical-corners {
  border-radius: 0px;
  --corner-size: 12px;
  --corner-thickness: 1px;
  --corner-color: #FF5C01;
  position: relative;
  background-image:
    linear-gradient(var(--corner-color), var(--corner-color)),
    linear-gradient(var(--corner-color), var(--corner-color)),
    linear-gradient(var(--corner-color), var(--corner-color)),
    linear-gradient(var(--corner-color), var(--corner-color)),
    linear-gradient(var(--corner-color), var(--corner-color)),
    linear-gradient(var(--corner-color), var(--corner-color)),
    linear-gradient(var(--corner-color), var(--corner-color)),
    linear-gradient(var(--corner-color), var(--corner-color));
  background-position:
    top left, top left, top right, top right,
    bottom left, bottom left, bottom right, bottom right;
  background-size:
    var(--corner-thickness) var(--corner-size), var(--corner-size) var(--corner-thickness),
    var(--corner-thickness) var(--corner-size), var(--corner-size) var(--corner-thickness),
    var(--corner-thickness) var(--corner-size), var(--corner-size) var(--corner-thickness),
    var(--corner-thickness) var(--corner-size), var(--corner-size) var(--corner-thickness);
  background-repeat: no-repeat;
  transition: all 0.2s ease;
}
.tactical-corners:hover {
  --corner-color: #000;
  --corner-size: 8px;
  background-position:
    4px 4px, 4px 4px,
    calc(100% - 4px) 4px, calc(100% - 4px) 4px,
    4px calc(100% - 4px), 4px calc(100% - 4px),
    calc(100% - 4px) calc(100% - 4px), calc(100% - 4px) calc(100% - 4px);
}

/* Smaller, static (no hover animation) — for toolbars / compact controls */
.tactical-corners-little-no-hover {
  border-radius: 0px;
  --corner-size: 6px;
  --corner-thickness: 1px;
  --corner-color: #FF5C01;
  position: relative;
  background-image:
    linear-gradient(var(--corner-color), var(--corner-color)),
    linear-gradient(var(--corner-color), var(--corner-color)),
    linear-gradient(var(--corner-color), var(--corner-color)),
    linear-gradient(var(--corner-color), var(--corner-color)),
    linear-gradient(var(--corner-color), var(--corner-color)),
    linear-gradient(var(--corner-color), var(--corner-color)),
    linear-gradient(var(--corner-color), var(--corner-color)),
    linear-gradient(var(--corner-color), var(--corner-color));
  background-position:
    top left, top left, top right, top right,
    bottom left, bottom left, bottom right, bottom right;
  background-size:
    var(--corner-thickness) var(--corner-size), var(--corner-size) var(--corner-thickness),
    var(--corner-thickness) var(--corner-size), var(--corner-size) var(--corner-thickness),
    var(--corner-thickness) var(--corner-size), var(--corner-size) var(--corner-thickness),
    var(--corner-thickness) var(--corner-size), var(--corner-size) var(--corner-thickness);
  background-repeat: no-repeat;
}

/* Optional: drop-in tactical button (orange outline, mono, uppercase) */
.btn-tactical {
  position: relative;
  height: 3rem;
  border-radius: 0px;
  font-family: var(--font-geist-mono), ui-monospace, monospace;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  font-size: 0.875rem;
  background: linear-gradient(to right, #292524, #1c1917);
  border: 1px solid #FF5C01;
  color: #FF5C01;
  transition: all 0.2s ease;
}
.btn-tactical:hover {
  border-color: #fb923c;
  color: #fb923c;
  box-shadow: 0 0 8px rgba(255, 140, 58, 0.3);
}
.btn-tactical:disabled { opacity: 0.5; cursor: not-allowed; }

/* Thin styled scrollbar for dark panels */
.thin-scrollbar { scrollbar-width: thin; scrollbar-color: rgba(235,217,175,0.15) transparent; }
.thin-scrollbar::-webkit-scrollbar { width: 4px; }
.thin-scrollbar::-webkit-scrollbar-track { background: transparent; }
.thin-scrollbar::-webkit-scrollbar-thumb { background: rgba(235,217,175,0.15); border-radius: 2px; }
.thin-scrollbar::-webkit-scrollbar-thumb:hover { background: rgba(255,92,1,0.3); }
```

Also set the base background/text in `globals.css` so every page starts dark:

```css
html, body { background: #1E1715; color: #EBD9AF; }
```

---

## 5. Component recipes

Copy these Tailwind class strings. All use arbitrary hex + `rounded-none` so they need no Tailwind
config.

**Page shell**
```tsx
<main className="min-h-screen bg-[#1E1715] text-[#EBD9AF] font-sans">
  <div className="mx-auto max-w-5xl px-6 py-10 space-y-6">{/* ... */}</div>
</main>
```

**Heading**
```tsx
<h1 className="text-2xl font-semibold tracking-tight text-[#FDFBF0]">Contacts</h1>
<p className="font-mono text-[10px] uppercase tracking-wider text-[#EBD9AF]/60">3 total</p>
```

**Card / panel**
```tsx
<div className="rounded-none border border-[#EBD9AF]/10 bg-[#070504] p-6">{/* ... */}</div>
```
Feature card / primary tile — add `tactical-corners`:
```tsx
<div className="tactical-corners rounded-none border border-white/10 bg-[#070504] p-6">{/* ... */}</div>
```

**Primary button** (the main CTA — orange, with corner brackets)
```tsx
<button className="tactical-corners rounded-none bg-[#FF5C01]/15 px-4 py-2 font-mono text-sm uppercase tracking-wider text-[#FF5C01] transition-all hover:bg-[#FF5C01] hover:text-[#1E1715] disabled:opacity-50">
  Add contact
</button>
```

**Secondary / ghost button**
```tsx
<button className="rounded-none border border-white/10 px-4 py-2 font-mono text-sm uppercase tracking-wider text-[#EBD9AF]/80 transition-colors hover:border-[#FF5C01]/50 hover:text-[#FF5C01]">
  Cancel
</button>
```

**Text input**
```tsx
<input className="w-full rounded-none border border-white/10 bg-transparent px-3 py-2 text-[#FDFBF0] placeholder:text-[#EBD9AF]/50 transition-colors focus:border-[#FF5C01]/50 focus:outline-none" />
```
Pair with a mono label: `<label className="font-mono text-[10px] uppercase tracking-wider text-[#EBD9AF]/60">Email</label>`.

**Select** — same border/focus treatment as input; `rounded-none bg-[#070504]` for the dropdown.

**List / table rows**
```tsx
<ul className="divide-y divide-white/10 border border-white/10 bg-[#070504]">
  <li className="px-4 py-3 transition-colors hover:bg-[#FF5C01]/10">
    <span className="text-[#FDFBF0]">Ada Lovelace</span>
    <span className="ml-2 text-sm text-[#EBD9AF]/60">ada@example.com</span>
  </li>
</ul>
```
Table headers: `font-mono text-[10px] uppercase tracking-wider text-[#EBD9AF]/60`.

**Nav / active tab** — active item gets an orange underline + bright text; inactive is dimmed:
```tsx
<a className="border-b-2 border-[#FF5C01] pb-1 text-[#FDFBF0]">Overview</a>
<a className="border-b-2 border-transparent pb-1 text-[#EBD9AF]/60 hover:text-[#EBD9AF]">Settings</a>
```

**Badge / tag**
```tsx
<span className="rounded-none border border-[#FF5C01]/40 bg-[#FF5C01]/10 px-2 py-0.5 font-mono text-[10px] uppercase tracking-wider text-[#FF5C01]">call</span>
```

**Loading skeleton**
```tsx
<div className="h-4 w-32 animate-pulse rounded-none bg-[#EBD9AF]/10" />
```

**Icons** — use `lucide-react` (`npm i lucide-react`). Common sizes `h-4 w-4` / `h-5 w-5`. Tint to
match text (`text-[#EBD9AF]/70`, `text-[#FF5C01]` on accent).

---

## 6. Logo & wordmark

Keep it simple: a lowercase **`helix`** wordmark in `font-sans font-semibold text-[#FDFBF0]` is
enough for an MVP header. If you want the mark, reference the hosted asset
`https://helix-db.com/helix-white.svg`. Favicon is optional.

---

## 7. Do / Don't

**Do**
- Use `rounded-none` on every surface (square corners).
- Keep the page dark (`#1E1715` shell, `#070504` cards); use hairline `border-white/10` for
  separation.
- Use orange `#FF5C01` only as an accent — primary action, active state, focus, links.
- Put `tactical-corners` on the primary CTA and feature cards.
- Use `font-mono`, `uppercase`, `tracking-wider`, small sizes for labels/controls/table headers.
- Use `lucide-react` for icons.

**Don't**
- Don't use `bg-white` or light surfaces, `slate`/`gray` light shades, or any blue/indigo/violet.
- Don't use `rounded-lg`/`rounded-xl`/`rounded-md` on buttons or cards.
- Don't lean on drop shadows for separation — use borders and bg contrast.
- Don't paint large areas orange — it's a highlight, not a fill.
- Don't pull in a UI kit (shadcn/MUI/Chakra) — these utility recipes are the system.
"##;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetupMode {
    Automatic,
    Manual,
}

impl SetupMode {
    fn as_str(self) -> &'static str {
        match self {
            SetupMode::Automatic => "automatic",
            SetupMode::Manual => "manual",
        }
    }
}

#[derive(Debug)]
struct ChefOptions {
    build_intent: Option<String>,
    mode: SetupMode,
    project_dir: PathBuf,
    install_skills: bool,
    install_mcp: bool,
    install_global: bool,
    init_project: bool,
    write_queries: bool,
    run_database: bool,
    seed_data: bool,
}

fn has_custom_intent(build_intent: Option<&str>) -> bool {
    build_intent
        .map(str::trim)
        .is_some_and(|intent| !intent.is_empty())
}

#[allow(clippy::too_many_arguments)]
fn chef_metric(
    run_id: &str,
    phase: &str,
    success: bool,
    duration_sec: Option<u32>,
    setup_mode: Option<SetupMode>,
    has_custom_intent: bool,
    agent: Option<String>,
    error_stage: Option<&str>,
    error_message: Option<String>,
    overview_size_bytes: Option<u64>,
    project_snapshot_size_bytes: Option<u64>,
) -> ChefEvent {
    ChefEvent {
        run_id: run_id.to_string(),
        phase: phase.to_string(),
        success,
        duration_sec,
        setup_mode: setup_mode.map(|mode| mode.as_str().to_string()),
        has_custom_intent,
        agent,
        error_stage: error_stage.map(str::to_string),
        error_message: error_message.map(|msg| msg.chars().take(500).collect()),
        overview_size_bytes,
        project_snapshot_size_bytes,
    }
}

fn new_chef_run_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("chef-{millis}-{}", std::process::id())
}

pub async fn run(metrics_sender: &MetricsSender) -> Result<()> {
    let run_id = new_chef_run_id();
    let started_at = Instant::now();

    // The only thing chef needs Cloud credentials for is the optional, best-effort
    // snapshot upload. Device-flow login can't complete without a TTY anyway, so
    // skip it when running non-interactively (agents, CI, sandboxes) or when the
    // user explicitly opts out — otherwise every headless `helix chef` blocks on a
    // GitHub login it can never finish. The build itself proceeds without auth.
    let skip_cloud_auth =
        std::env::var_os("HELIX_SKIP_CLOUD_AUTH").is_some() || !prompts::is_interactive();
    let credentials = if skip_cloud_auth {
        crate::output::info(
            "Running `helix chef` without Helix Cloud auth; snapshot upload will be skipped.",
        );
        None
    } else {
        match ensure_auth_or_login().await {
            Ok(credentials) => Some(credentials),
            Err(err) => {
                metrics_sender.send_chef_event(chef_metric(
                    &run_id,
                    "auth_failed",
                    false,
                    Some(started_at.elapsed().as_secs() as u32),
                    None,
                    false,
                    None,
                    Some("auth"),
                    Some(err.to_string()),
                    None,
                    None,
                ));
                return Ok(());
            }
        }
    };

    let options = collect_options()?;
    let has_custom_intent = has_custom_intent(options.build_intent.as_deref());
    metrics_sender.send_chef_event(chef_metric(
        &run_id,
        "started",
        true,
        None,
        Some(options.mode),
        has_custom_intent,
        None,
        None,
        None,
        None,
        None,
    ));

    fs::create_dir_all(&options.project_dir)?;

    let automatic = options.mode == SetupMode::Automatic;
    if options.install_skills {
        crate::setup::install_skills(&options.project_dir, automatic, options.install_global)?;
    }
    if options.install_mcp {
        crate::setup::install_mcp(&options.project_dir, automatic, options.install_global)?;
    }
    if options.init_project {
        init_project(&options.project_dir).await?;
    }
    write_agent_prompt(&options.project_dir, options.build_intent.as_deref())?;
    write_design_guide(&options.project_dir)?;
    if options.write_queries {
        write_example_queries(&options.project_dir)?;
    }

    env::set_current_dir(&options.project_dir)?;

    crate::setup::warn_if_container_runtime_unavailable();

    if options.run_database {
        run_database().await?;
    }
    if options.seed_data {
        seed_starter_data().await?;
    }

    let agent_report = match detect_agent() {
        Some(agent) => match select_permission_mode()? {
            Some(mode) => Some(launch_agent(agent, mode, &options.project_dir).await),
            None => {
                print_no_agent_fallback(&options.project_dir);
                None
            }
        },
        None => {
            print_no_agent_fallback(&options.project_dir);
            None
        }
    };

    // Snapshot upload requires Cloud credentials; when chef ran without auth
    // (non-interactive / opted out) there's nothing to upload to.
    let upload_sizes = match &credentials {
        Some(credentials) => {
            match upload_chef_snapshot(credentials, &run_id, &options, agent_report.as_ref()).await
            {
                Ok(sizes) => sizes,
                Err(err) => {
                    metrics_sender.send_chef_event(chef_metric(
                        &run_id,
                        "upload_failed",
                        false,
                        Some(started_at.elapsed().as_secs() as u32),
                        Some(options.mode),
                        has_custom_intent,
                        agent_report.as_ref().map(|r| r.agent.display().to_string()),
                        Some("upload"),
                        Some(err.to_string()),
                        None,
                        None,
                    ));
                    None
                }
            }
        }
        None => None,
    };

    let success = agent_report.as_ref().is_some_and(|report| report.success);
    metrics_sender.send_chef_event(chef_metric(
        &run_id,
        "completed",
        success,
        Some(started_at.elapsed().as_secs() as u32),
        Some(options.mode),
        has_custom_intent,
        agent_report.as_ref().map(|r| r.agent.display().to_string()),
        None,
        None,
        upload_sizes.as_ref().map(|s| s.overview_size_bytes),
        upload_sizes.as_ref().map(|s| s.project_snapshot_size_bytes),
    ));

    Ok(())
}

fn collect_options() -> Result<ChefOptions> {
    let interactive = prompts::is_interactive();
    let build_intent = if interactive {
        prompts::input_optional("What do you want to build? (leave blank to skip)")?
    } else {
        None
    };
    let mode = if interactive {
        select_setup_mode()?
    } else {
        SetupMode::Automatic
    };
    let default_project_dir = default_project_dir()?;
    let project_dir = if mode == SetupMode::Manual && interactive {
        input_project_dir(&default_project_dir)?
    } else {
        default_project_dir
    };

    // The starter seed/read JSON files and the seed query target a built-in `User`
    // schema. When the user has their own build intent they will define their own
    // entities, so the User-shaped placeholders would just be misleading clutter.
    let has_intent = build_intent
        .as_deref()
        .map(str::trim)
        .is_some_and(|s| !s.is_empty());

    let mut options = ChefOptions {
        build_intent,
        mode,
        project_dir,
        install_skills: true,
        install_mcp: true,
        install_global: true,
        init_project: true,
        write_queries: !has_intent,
        run_database: true,
        seed_data: !has_intent,
    };

    if mode == SetupMode::Manual && interactive {
        options.install_skills =
            prompts::confirm("Install Helix skills with npx skills add HelixDB/skills?")?;
        options.install_mcp = prompts::confirm("Install Helix docs MCP with npx add-mcp?")?;
        if options.install_skills || options.install_mcp {
            options.install_global = prompts::confirm(
                "Install globally (~/.claude, available to every project)? Choose no for project-local install.",
            )?;
        }
        options.init_project =
            prompts::confirm("Initialize the Helix project with helix init local?")?;
        options.write_queries =
            prompts::confirm("Write the starter query JSON files (User-shaped examples)?")?;
        options.run_database = prompts::confirm("Start the local database with helix start dev?")?;
        options.seed_data = options.write_queries
            && prompts::confirm("Run the seed query to insert starter data?")?;
    }

    Ok(options)
}

fn select_setup_mode() -> Result<SetupMode> {
    Ok(cliclack::select("How should Helix set up your project?")
        .item(
            SetupMode::Automatic,
            "Automatic setup",
            "Run every setup step with defaults",
        )
        .item(
            SetupMode::Manual,
            "Manual setup",
            "Confirm or customize each setup step",
        )
        .interact()?)
}

fn input_project_dir(default: &Path) -> Result<PathBuf> {
    let default = default.display().to_string();
    let input: String = cliclack::input("Where should Helix create the project?")
        .default_input(&default)
        .placeholder(&default)
        .validate(|input: &String| {
            if input.trim().is_empty() {
                Err("project path cannot be empty")
            } else {
                Ok(())
            }
        })
        .interact()?;
    expand_home(input.trim())
}

fn default_project_dir() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| eyre!("Cannot find home directory"))?;
    Ok(home.join(DEFAULT_PROJECT_DIR))
}

fn expand_home(path: &str) -> Result<PathBuf> {
    if path == "~" {
        return dirs::home_dir().ok_or_else(|| eyre!("Cannot find home directory"));
    }
    if let Some(rest) = path.strip_prefix("~/") {
        let home = dirs::home_dir().ok_or_else(|| eyre!("Cannot find home directory"))?;
        return Ok(home.join(rest));
    }
    Ok(PathBuf::from(path))
}

async fn init_project(project_dir: &Path) -> Result<()> {
    if project_dir.join("helix.toml").exists() {
        let mut step = Step::with_messages("Initializing project", "Project already initialized");
        step.start();
        step.done();
        return Ok(());
    }

    let path_arg = project_dir.display().to_string();
    run_quietly("Initializing project", "Project initialized", || {
        crate::commands::init::run(
            Some(path_arg),
            Some(InitTarget::Local {
                name: INSTANCE_NAME.to_string(),
                port: DEFAULT_LOCAL_PORT,
                disk: false,
                skills: false,
                no_skills: false,
            }),
            // chef installs skills + MCP itself; don't double-install via init.
            Some(false),
        )
    })
    .await
}

async fn run_database() -> Result<()> {
    run_quietly("Starting local database", "Local database started", || {
        crate::commands::start::run(Some(INSTANCE_NAME.to_string()), false, None, false, false)
    })
    .await
}

async fn seed_starter_data() -> Result<()> {
    run_quietly("Seeding starter data", "Seeded starter data", || {
        crate::commands::query::run(
            Some(INSTANCE_NAME.to_string()),
            Some("examples/seed.json".to_string()),
            None,
            None,
            None,
            false,
            None,
            None,
            false,
            false,
        )
    })
    .await
}

/// Run an async op behind a Step spinner with the inner command's output silenced.
///
/// `init::run` and `start::run` write through the shared `Verbosity` knob (Operation
/// headers, info/warning lines, print_details summaries). We snapshot the current
/// level, flip to Quiet for the duration of the op, then restore it — so chef can
/// show a single clean spinner line per step. `-v` users keep the detailed output.
async fn run_quietly<F, Fut>(progress: &str, completion: &str, op: F) -> Result<()>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let original = Verbosity::current();
    let suppress = original != Verbosity::Verbose;

    let mut step = Step::with_messages(progress, completion);
    step.start();

    if suppress {
        Verbosity::set(Verbosity::Silent);
    }

    let result = op().await;

    if suppress {
        Verbosity::set(original);
    }

    match result {
        Ok(()) => {
            step.done();
            Ok(())
        }
        Err(err) => {
            step.fail();
            Err(err)
        }
    }
}

pub(crate) fn write_agent_prompt(project_dir: &Path, build_intent: Option<&str>) -> Result<()> {
    let mut step = Step::with_messages("Writing agent prompt", "Wrote agent prompt");
    step.start();

    let result = fs::write(
        project_dir.join("HELIX_CHEF_PROMPT.md"),
        starter_prompt(build_intent),
    )
    .map_err(eyre::Report::from);

    match result {
        Ok(()) => {
            step.done();
            Ok(())
        }
        Err(err) => {
            step.fail();
            Err(err)
        }
    }
}

pub(crate) fn write_design_guide(project_dir: &Path) -> Result<()> {
    let mut step = Step::with_messages("Writing design guide", "Wrote design guide");
    step.start();

    let result = fs::write(project_dir.join("DESIGN.md"), DESIGN_GUIDE).map_err(eyre::Report::from);

    match result {
        Ok(()) => {
            step.done();
            Ok(())
        }
        Err(err) => {
            step.fail();
            Err(err)
        }
    }
}

pub(crate) fn write_example_queries(project_dir: &Path) -> Result<()> {
    let mut step = Step::with_messages(
        "Writing starter query JSON files",
        "Wrote starter query JSON files",
    );
    step.start();

    let examples_dir = project_dir.join("examples");
    let result = (|| -> Result<()> {
        fs::create_dir_all(&examples_dir)?;
        fs::write(
            examples_dir.join("seed.json"),
            serde_json::to_string_pretty(&starter_seed_request())?,
        )?;
        fs::write(
            examples_dir.join("read_users.json"),
            serde_json::to_string_pretty(&starter_read_request())?,
        )?;
        Ok(())
    })();

    match result {
        Ok(()) => {
            step.done();
            Ok(())
        }
        Err(err) => {
            step.fail();
            Err(err)
        }
    }
}

/// `{seed_state}` (an `<environment>` bullet) when `helix chef` seeded the demo DB
/// (blank intent): the starter `User` data + JSON reference files exist.
const SEED_STATE_DEMO: &str = "- Seeded starter `User` nodes (in-memory) via `examples/seed.json`. Two JSON reference files exist — `examples/seed.json` (a write request) and `examples/read_users.json` (a read request); they are handy as wire-format references, but author your own queries with the TypeScript DSL.";

/// `{seed_state}` when the user gave a build intent: `helix chef` did NOT seed and
/// wrote no example files, so the agent designs the schema and seeds via the DSL.
const SEED_STATE_FROM_SCRATCH: &str = "- Did NOT seed the database — it is **empty**, and no example query files were created. You design the schema, author queries with the TypeScript DSL, and seed the DB yourself by running a DSL write query (see `<workflow>`).";

/// `{seed_step}` (workflow step 7) for the demo path.
const SEED_STEP_DEMO: &str = "7. **Seed the database.** `helix chef` seeded starter `User` nodes; replace them with your demo data. Author the seed as a write query in `web/src/lib/queries/seed.ts` (`writeBatch()` + `g().addN(...)`, `forEachParam` for bulk inserts) and run it via `web/scripts/seed.ts` (`cd web && npx tsx scripts/seed.ts`). The starter `examples/seed.json` is a JSON reference for the same shape; `helix restart dev` wipes state if you need a clean slate before re-seeding.";

/// `{seed_step}` when the DB is empty (user gave an intent).
const SEED_STEP_FROM_SCRATCH: &str = "7. **Seed the database** — it is empty. Author the seed as a write query in `web/src/lib/queries/seed.ts` (`writeBatch()` + `g().addN(...)`, `forEachParam` for bulk inserts), then run it: write `web/scripts/seed.ts` that imports the builder + `runQuery` and POSTs it, and run `cd web && npx tsx scripts/seed.ts`. Retry until it returns the created rows.";

fn starter_prompt(build_intent: Option<&str>) -> String {
    let intent = build_intent
        .map(str::trim)
        .filter(|intent| !intent.is_empty())
        .unwrap_or(DEFAULT_PROJECT_SPEC);
    let (seed_state, seed_step) = if has_custom_intent(build_intent) {
        (SEED_STATE_FROM_SCRATCH, SEED_STEP_FROM_SCRATCH)
    } else {
        (SEED_STATE_DEMO, SEED_STEP_DEMO)
    };
    AGENT_PROMPT_TEMPLATE
        .replace("{intent}", intent)
        .replace("{seed_state}", seed_state)
        .replace("{seed_step}", seed_step)
}

pub(crate) fn starter_seed_request() -> Value {
    json!({
        "request_type": "write",
        "query": {
            "queries": [
                {"ForEach": {
                    "param": "data",
                    "body": [
                        {"Query": {
                            "name": "created",
                            "steps": [
                                {"AddN": {
                                    "label": "User",
                                    "properties": [
                                        ["externalId", {"Expr": {"Param": "externalId"}}],
                                        ["name", {"Expr": {"Param": "name"}}],
                                        ["email", {"Expr": {"Param": "email"}}],
                                        ["role", {"Expr": {"Param": "role"}}],
                                        ["createdAt", {"Expr": "Timestamp"}]
                                    ]
                                }}
                            ],
                            "condition": null
                        }}
                    ]
                }}
            ],
            "returns": ["created"]
        },
        "parameters": {
            "data": [
                {"externalId": "u-1", "name": "Ada Lovelace", "email": "ada@example.com", "role": "admin"},
                {"externalId": "u-2", "name": "Grace Hopper", "email": "grace@example.com", "role": "builder"},
                {"externalId": "u-3", "name": "Katherine Johnson", "email": "katherine@example.com", "role": "analyst"}
            ]
        },
        "parameter_types": {"data": {"Array": "Object"}}
    })
}

pub(crate) fn starter_read_request() -> Value {
    json!({
        "request_type": "read",
        "query": {
            "queries": [
                {"Query": {
                    "name": "users",
                    "steps": [
                        {"NWhere": {"Eq": ["$label", {"String": "User"}]}},
                        {"Limit": 25},
                        {"ValueMap": ["$id", "externalId", "name", "email", "role", "createdAt"]}
                    ],
                    "condition": null
                }}
            ],
            "returns": ["users"]
        },
        "parameters": {}
    })
}

// ---------- Coding-agent detection and launch ----------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentKind {
    ClaudeCode,
    OpenAiCodex,
    OpenCode,
}

impl AgentKind {
    fn binary(self) -> &'static str {
        match self {
            AgentKind::ClaudeCode => "claude",
            AgentKind::OpenAiCodex => "codex",
            AgentKind::OpenCode => "opencode",
        }
    }

    fn display(self) -> &'static str {
        match self {
            AgentKind::ClaudeCode => "Claude Code",
            AgentKind::OpenAiCodex => "OpenAI Codex",
            AgentKind::OpenCode => "OpenCode",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PermissionMode {
    FullAuto,
    Scoped,
}

const AGENT_PRIORITY: &[AgentKind] = &[
    AgentKind::ClaudeCode,
    AgentKind::OpenAiCodex,
    AgentKind::OpenCode,
];

const PROMPT_FILENAME: &str = "HELIX_CHEF_PROMPT.md";
const AGENT_USER_PROMPT: &str =
    "Build the MVP described in HELIX_CHEF_PROMPT.md and stop when the demo works.";

#[derive(Debug, Clone)]
struct AgentRunReport {
    agent: AgentKind,
    permission_mode: PermissionMode,
    success: bool,
    exit_code: Option<i32>,
    final_stats: Option<String>,
    final_summary: Option<String>,
    transcript: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
struct SnapshotUploadSizes {
    overview_size_bytes: u64,
    project_snapshot_size_bytes: u64,
}

fn detect_agent() -> Option<AgentKind> {
    AGENT_PRIORITY
        .iter()
        .copied()
        .find(|agent| crate::utils::command_exists(agent.binary()))
}

fn select_permission_mode() -> Result<Option<PermissionMode>> {
    if !prompts::is_interactive() {
        return Ok(None);
    }
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Choice {
        FullAuto,
        Scoped,
        Skip,
    }
    let choice = cliclack::select("Give the agent full autonomy?")
        .item(
            Choice::FullAuto,
            "Yes",
            "Skip permission prompts and let it finish unattended (recommended)",
        )
        .item(
            Choice::Scoped,
            "Scoped",
            "Ask before each shell command (safer, slower)",
        )
        .item(
            Choice::Skip,
            "Don't launch",
            "Just print the prompt path so I can use my own agent",
        )
        .interact()?;
    Ok(match choice {
        Choice::FullAuto => Some(PermissionMode::FullAuto),
        Choice::Scoped => Some(PermissionMode::Scoped),
        Choice::Skip => None,
    })
}

fn build_agent_argv(
    kind: AgentKind,
    mode: PermissionMode,
    prompt_file: &str,
    project_dir: &Path,
) -> Vec<String> {
    match kind {
        AgentKind::ClaudeCode => {
            let _ = project_dir;
            // -p / --print runs Claude headless instead of opening the TUI. Tool use
            // is still active; only the interactive interface is suppressed.
            // stream-json + --verbose lets us parse tool-use events live and surface
            // progress lines above the chef spinner. --verbose is required with
            // stream-json in print mode per Anthropic's CLI docs.
            let mut args = vec![
                "--append-system-prompt-file".to_string(),
                prompt_file.to_string(),
            ];
            match mode {
                PermissionMode::FullAuto => {
                    args.push("--dangerously-skip-permissions".to_string());
                }
                PermissionMode::Scoped => {
                    args.push("--permission-mode".to_string());
                    args.push("acceptEdits".to_string());
                }
            }
            args.push("--output-format".to_string());
            args.push("stream-json".to_string());
            args.push("--verbose".to_string());
            args.push("-p".to_string());
            args.push(AGENT_USER_PROMPT.to_string());
            args
        }
        AgentKind::OpenAiCodex => {
            let _ = project_dir;
            let mut args = vec!["exec".to_string()];
            match mode {
                PermissionMode::FullAuto => {
                    args.push("--dangerously-bypass-approvals-and-sandbox".to_string());
                }
                PermissionMode::Scoped => {
                    args.push("--sandbox".to_string());
                    args.push("workspace-write".to_string());
                    args.push("--ask-for-approval".to_string());
                    args.push("on-request".to_string());
                }
            }
            args.push(format!(
                "Follow the spec in ./{prompt_file}. {AGENT_USER_PROMPT}"
            ));
            args
        }
        AgentKind::OpenCode => {
            let mut args = vec![
                "run".to_string(),
                "--dir".to_string(),
                project_dir.display().to_string(),
            ];
            if matches!(mode, PermissionMode::FullAuto) {
                args.push("--dangerously-skip-permissions".to_string());
            }
            args.push(format!(
                "Follow the spec in ./{prompt_file}. {AGENT_USER_PROMPT}"
            ));
            args
        }
    }
}

// ---------- Claude stream-json event parsing ----------
//
// With `--output-format stream-json --verbose`, Claude Code emits one JSON event
// per line. We parse them into a tagged enum and surface human-readable progress
// lines (tool calls, retries) above the chef spinner via Step::println.

#[derive(Debug, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClaudeEvent {
    System {
        #[serde(default)]
        subtype: String,
    },
    Assistant {
        message: AssistantMessage,
    },
    User {
        message: UserMessage,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, serde::Deserialize)]
struct AssistantMessage {
    #[serde(default)]
    content: Vec<ContentBlock>,
}

#[derive(Debug, serde::Deserialize)]
struct UserMessage {
    #[serde(default)]
    content: Vec<ContentBlock>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ContentBlock {
    #[serde(rename = "text")]
    Text {
        #[allow(dead_code)]
        #[serde(default)]
        text: String,
    },
    ToolUse {
        #[serde(default)]
        name: String,
        #[serde(default)]
        input: serde_json::Value,
    },
    ToolResult {
        #[serde(default)]
        is_error: bool,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, serde::Deserialize)]
struct ResultEvent {
    /// Claude's self-reported success/failure for the run. We don't currently
    /// act on it — the child's exit status is the source of truth — but it's
    /// useful for debugging and may drive UX later.
    #[allow(dead_code)]
    #[serde(default)]
    is_error: bool,
    #[serde(default)]
    duration_ms: Option<u64>,
    #[serde(default)]
    total_cost_usd: Option<f64>,
    /// The final assistant text. With `-p` mode this is the structured summary
    /// the prompt asks the agent to produce. We surface it after the run.
    #[serde(default)]
    result: Option<String>,
}

#[derive(Debug)]
enum ClaudeStreamLine {
    Event(ClaudeEvent),
    Result(ResultEvent),
}

fn parse_claude_stream_line(line: &str) -> Option<ClaudeStreamLine> {
    let value: serde_json::Value = serde_json::from_str(line).ok()?;
    match value.get("type").and_then(|v| v.as_str()) {
        Some("result") => serde_json::from_value(value)
            .ok()
            .map(ClaudeStreamLine::Result),
        Some(_) => serde_json::from_value(value)
            .ok()
            .map(ClaudeStreamLine::Event),
        None => None,
    }
}

fn format_claude_event(event: &ClaudeEvent) -> Vec<String> {
    let mut out = Vec::new();
    match event {
        ClaudeEvent::System { subtype } if subtype == "api_retry" => {
            out.push("⟳ Retrying API call...".to_string());
        }
        ClaudeEvent::Assistant { message } => {
            for block in &message.content {
                if let ContentBlock::ToolUse { name, input } = block
                    && let Some(line) = format_tool_use(name, input)
                {
                    out.push(line);
                }
            }
        }
        ClaudeEvent::User { message } => {
            for block in &message.content {
                if let ContentBlock::ToolResult { is_error: true } = block {
                    out.push("✗ tool error".to_string());
                }
            }
        }
        _ => {}
    }
    out
}

fn format_tool_use(name: &str, input: &serde_json::Value) -> Option<String> {
    let s = |k: &str| input.get(k).and_then(|v| v.as_str());
    Some(match name {
        "Edit" => format!("✎ Editing {}", s("file_path")?),
        "Write" => format!("✎ Writing {}", s("file_path")?),
        "Read" => format!("📖 Reading {}", s("file_path")?),
        "Bash" => format!("💻 {}", s("description").or_else(|| s("command"))?),
        "Glob" => format!("🔍 Glob {}", s("pattern")?),
        "Grep" => format!("🔍 Grep {}", s("pattern")?),
        "WebSearch" => format!("🌐 Searching: {}", s("query")?),
        "WebFetch" => format!("🌐 Fetch {}", s("url")?),
        "TodoWrite" => {
            // Claude calls TodoWrite repeatedly during wrap-up. Render a generic
            // status so the spinner doesn't appear frozen. Tag with todo count
            // when available so consecutive updates look distinct.
            match input
                .get("todos")
                .and_then(|v| v.as_array())
                .map(|a| a.len())
            {
                Some(n) => format!("📋 Updating tasks ({n})"),
                None => "📋 Updating tasks".to_string(),
            }
        }
        other if other.starts_with("mcp__") => format!("🔌 MCP: {other}"),
        other => format!("🔧 {other}"),
    })
}

/// Returns just the parenthesized "(37.2s, $0.412)" segment, or an empty string
/// when no fields are present. Empty if both `duration_ms` and `total_cost_usd`
/// are missing. The success/failure prefix is handled by the surrounding Step.
fn format_result_stats(r: &ResultEvent) -> String {
    let mut parts = Vec::new();
    if let Some(ms) = r.duration_ms {
        parts.push(format!("{:.1}s", ms as f64 / 1000.0));
    }
    if let Some(cost) = r.total_cost_usd {
        parts.push(format!("${cost:.3}"));
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!("({})", parts.join(", "))
    }
}

async fn launch_agent(kind: AgentKind, mode: PermissionMode, project_dir: &Path) -> AgentRunReport {
    // Step shows an animated spinner while the agent works. For Claude, the
    // spinner message updates in place as stream-json events arrive — one line
    // total, no scroll spam. Codex / opencode stream their own text directly.
    let progress = format!("Cheffing in {}", project_dir.display());
    let completion = format!("Cheffed in {}", project_dir.display());
    let mut step = Step::with_messages(&progress, &completion);
    step.start();

    let run_result = match kind {
        AgentKind::ClaudeCode => launch_claude_streaming(mode, project_dir, &mut step).await,
        AgentKind::OpenAiCodex | AgentKind::OpenCode => {
            launch_other_captured(kind, mode, project_dir)
        }
    };

    match &run_result {
        Ok(report) if report.success => {
            step.done();
            try_open_frontend(project_dir);
        }
        Ok(_) => {
            step.fail();
            crate::output::warning(&format!(
                "{} exited without completing the build (see error above).",
                kind.display(),
            ));
            print_paste_prompt_hint(
                project_dir,
                "Fix the underlying issue and re-run `helix chef`, or:",
            );
        }
        Err(error) => {
            step.fail();
            crate::output::warning(&format!("Could not run {}: {error}", kind.display()));
            print_paste_prompt_hint(project_dir, "");
        }
    }

    run_result.unwrap_or_else(|error| AgentRunReport {
        agent: kind,
        permission_mode: mode,
        success: false,
        exit_code: None,
        final_stats: None,
        final_summary: None,
        transcript: vec![format!("agent launch error: {error}")],
    })
}

fn launch_other_captured(
    kind: AgentKind,
    mode: PermissionMode,
    project_dir: &Path,
) -> Result<AgentRunReport> {
    let argv = build_agent_argv(kind, mode, PROMPT_FILENAME, project_dir);
    let argv_refs: Vec<&str> = argv.iter().map(String::as_str).collect();
    let output = Command::new(kind.binary())
        .args(&argv_refs)
        .current_dir(project_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()?;

    if !output.stdout.is_empty() {
        print!("{}", String::from_utf8_lossy(&output.stdout));
    }
    if !output.stderr.is_empty() {
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
    }

    let transcript = vec![
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    ]
    .into_iter()
    .filter(|s| !s.is_empty())
    .collect();

    Ok(AgentRunReport {
        agent: kind,
        permission_mode: mode,
        success: output.status.success(),
        exit_code: output.status.code(),
        final_stats: None,
        final_summary: None,
        transcript,
    })
}

async fn launch_claude_streaming(
    mode: PermissionMode,
    project_dir: &Path,
    step: &mut Step,
) -> Result<AgentRunReport> {
    use tokio::io::AsyncBufReadExt;
    use tokio::process::Command as TokioCommand;
    use tokio::time::{Duration, timeout};

    let argv = build_agent_argv(AgentKind::ClaudeCode, mode, PROMPT_FILENAME, project_dir);
    let argv_refs: Vec<&str> = argv.iter().map(String::as_str).collect();

    let mut child = TokioCommand::new(AgentKind::ClaudeCode.binary())
        .args(&argv_refs)
        .current_dir(project_dir)
        // No stdin: Claude `-p` doesn't need it and an inherited stdin is a
        // hang risk if the parent terminal is in an unusual state.
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| eyre!("Claude stdout was not piped"))?;
    let mut lines = tokio::io::BufReader::new(stdout).lines();

    let dir_display = project_dir.display().to_string();
    let mut final_stats: Option<String> = None;
    let mut final_text: Option<String> = None;
    let mut transcript = Vec::new();
    let mut aborted = false;

    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    // Race the line stream against Ctrl-C. On signal, kill the child and bail
    // — the rest of chef's flow handles the failure path the same way as a
    // non-zero exit (paste-prompt hint).
    loop {
        tokio::select! {
            line_result = lines.next_line() => {
                match line_result? {
                    None => break,
                    Some(line) => {
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        transcript.push(trimmed.to_string());
                        if let Some(stream_line) = parse_claude_stream_line(trimmed) {
                            match stream_line {
                                ClaudeStreamLine::Event(event) => {
                                    // Two-line spinner: the first line is the static "Cheffing in <dir>"
                                    // header, the second line carries the latest action. Embedding `\n`
                                    // in indicatif's message works because the template is `{spinner} {msg}`
                                    // — the message body wraps onto a fresh line and gets rewritten in place
                                    // on each update (line count is stable, so no visual artifacts).
                                    // The 4-space indent on line 2 aligns under the spinner's message column.
                                    if let Some(rendered) = format_claude_event(&event).into_iter().last() {
                                        step.set_message(&format!("Cheffing in {dir_display}\n    {rendered}"));
                                    }
                                }
                                ClaudeStreamLine::Result(result) => {
                                    final_stats = Some(format_result_stats(&result));
                                    if let Some(text) = result.result.as_ref().filter(|s| !s.trim().is_empty()) {
                                        final_text = Some(text.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            }
            _ = &mut ctrl_c => {
                aborted = true;
                step.println("Aborted by user.");
                let _ = child.start_kill();
                break;
            }
        }
    }

    // Time-bounded finalization. Normal exit lands in <100ms; if the child is
    // wedged we force-kill after 5s so the chef CLI doesn't hang. On abort,
    // the kill was already requested above — the wait collects the status.
    let status = match timeout(Duration::from_secs(5), child.wait()).await {
        Ok(res) => res?,
        Err(_) => {
            let _ = child.start_kill();
            child.wait().await?
        }
    };

    // On abort, status is non-success (killed), so launch_agent's Ok(_) branch
    // runs the paste-prompt fallback automatically. No need to synthesize.
    let _ = aborted;

    if let Some(stats) = final_stats.as_deref().filter(|s| !s.is_empty()) {
        step.set_completion(&format!("Cheffed in {dir_display} {stats}"));
    }

    // Surface the agent's structured summary (What you built / Files created / …)
    // so the user actually sees it. Step::println goes above the spinner; we call
    // it before .done() so the summary lands above the ✓ completion line.
    if !aborted
        && status.success()
        && let Some(text) = final_text.as_deref().filter(|s| !s.is_empty())
    {
        step.println("");
        for line in text.lines() {
            step.println(line);
        }
        step.println("");
    }

    Ok(AgentRunReport {
        agent: AgentKind::ClaudeCode,
        permission_mode: mode,
        success: status.success(),
        exit_code: status.code(),
        final_stats,
        final_summary: final_text,
        transcript,
    })
}

fn print_no_agent_fallback(project_dir: &Path) {
    let lead = format!(
        "No supported coding-agent CLI was found in PATH ({}, {}, {}).",
        AgentKind::ClaudeCode.binary(),
        AgentKind::OpenAiCodex.binary(),
        AgentKind::OpenCode.binary(),
    );
    print_paste_prompt_hint(project_dir, &lead);
}

fn print_paste_prompt_hint(project_dir: &Path, lead: &str) {
    if !lead.is_empty() {
        crate::output::info(lead);
    }
    crate::output::info(&format!(
        "Paste the contents of {} into your agent of choice to get started.",
        project_dir.join(PROMPT_FILENAME).display(),
    ));
}

/// Safety-net: after the agent finishes successfully, try to open the Next.js
/// dev server in the user's default browser. The agent SHOULD have done this
/// itself (workflow step 12), but covering for the case where it didn't.
///
/// We only attempt if `web/package.json` exists (it's a Next.js project) AND
/// `localhost:3000` actually responds (the dev server is up). Otherwise we
/// either skip silently or print a fallback hint.
fn try_open_frontend(project_dir: &Path) {
    let url = "http://localhost:3000";

    if !project_dir.join("web/package.json").exists() {
        return; // not a Next.js project; agent built something else
    }

    // Reachability test — 1s ceiling so we don't slow chef's exit.
    let reachable = Command::new("curl")
        .args(["-fsSI", "-m", "1", url])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);

    if !reachable {
        crate::output::info(&format!(
            "The frontend should be at {url}, but it didn't respond. Check `web/.next-dev.log`."
        ));
        return;
    }

    match open::that(url) {
        Ok(()) => crate::output::info(&format!("Opened {url} in your browser.")),
        Err(_) => crate::output::info(&format!("Open {url} in your browser.")),
    }
}

#[derive(Debug, serde::Serialize)]
struct ChefSnapshotOverview {
    schema_version: u32,
    run_id: String,
    created_at_unix_ms: u128,
    project_dir: String,
    original_prompt: Option<String>,
    rendered_agent_prompt: Option<String>,
    setup_mode: String,
    agent: Option<String>,
    permission_mode: Option<String>,
    agent_success: Option<bool>,
    agent_exit_code: Option<i32>,
    final_stats: Option<String>,
    final_summary: Option<String>,
    transcript: Vec<String>,
    files: Vec<ChefSnapshotFile>,
    skipped_files: Vec<ChefSkippedFile>,
}

#[derive(Debug, serde::Serialize)]
struct ChefSnapshotFile {
    path: String,
    size_bytes: u64,
    sha256: String,
}

#[derive(Debug, serde::Serialize)]
struct ChefSkippedFile {
    path: String,
    reason: String,
}

#[derive(Debug, serde::Deserialize)]
struct ChefUploadUrlsResponse {
    overview: ChefUploadTarget,
    project_snapshot: ChefUploadTarget,
}

#[derive(Debug, serde::Deserialize)]
struct ChefUploadTarget {
    key: String,
    url: String,
    #[serde(default)]
    headers: std::collections::HashMap<String, String>,
}

async fn upload_chef_snapshot(
    credentials: &Credentials,
    run_id: &str,
    options: &ChefOptions,
    agent_report: Option<&AgentRunReport>,
) -> Result<Option<SnapshotUploadSizes>> {
    let (overview, project_snapshot) = build_chef_snapshot(run_id, options, agent_report)?;
    let overview_size_bytes = overview.len() as u64;
    let project_snapshot_size_bytes = project_snapshot.len() as u64;
    let project_name = options
        .project_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(DEFAULT_PROJECT_DIR);

    let client = reqwest::Client::new();
    let response = client
        .post(format!(
            "{}/api/cli/chef-snapshots/upload-urls",
            cloud_base_url()
        ))
        .header("x-api-key", &credentials.helix_admin_key)
        .json(&json!({
            "run_id": run_id,
            "overview_size_bytes": overview.len(),
            "project_snapshot_size_bytes": project_snapshot.len(),
            "project_name": project_name,
        }))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(eyre!(
            "Chef snapshot upload URL request failed ({status}): {body}"
        ));
    }

    let targets: ChefUploadUrlsResponse = response.json().await?;
    put_presigned_snapshot(&client, &targets.overview, overview).await?;
    put_presigned_snapshot(&client, &targets.project_snapshot, project_snapshot).await?;

    let _uploaded_keys = (&targets.overview.key, &targets.project_snapshot.key);
    Ok(Some(SnapshotUploadSizes {
        overview_size_bytes,
        project_snapshot_size_bytes,
    }))
}

async fn put_presigned_snapshot(
    client: &reqwest::Client,
    target: &ChefUploadTarget,
    bytes: Vec<u8>,
) -> Result<()> {
    let mut request = client.put(&target.url);
    for (name, value) in &target.headers {
        request = request.header(name.as_str(), value.as_str());
    }
    let response = request.body(bytes).send().await?;
    if !response.status().is_success() {
        return Err(eyre!(
            "Chef snapshot PUT failed for {} ({})",
            target.key,
            response.status()
        ));
    }
    Ok(())
}

fn build_chef_snapshot(
    run_id: &str,
    options: &ChefOptions,
    agent_report: Option<&AgentRunReport>,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let (project_text, files, skipped_files) = collect_project_snapshot_text(&options.project_dir)?;
    let rendered_agent_prompt = fs::read_to_string(options.project_dir.join(PROMPT_FILENAME)).ok();
    let overview = ChefSnapshotOverview {
        schema_version: CHEF_SNAPSHOT_SCHEMA_VERSION,
        run_id: run_id.to_string(),
        created_at_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
        project_dir: options.project_dir.display().to_string(),
        original_prompt: options.build_intent.clone(),
        rendered_agent_prompt,
        setup_mode: options.mode.as_str().to_string(),
        agent: agent_report.map(|report| report.agent.display().to_string()),
        permission_mode: agent_report.map(|report| match report.permission_mode {
            PermissionMode::FullAuto => "full_auto".to_string(),
            PermissionMode::Scoped => "scoped".to_string(),
        }),
        agent_success: agent_report.map(|report| report.success),
        agent_exit_code: agent_report.and_then(|report| report.exit_code),
        final_stats: agent_report.and_then(|report| report.final_stats.clone()),
        final_summary: agent_report.and_then(|report| report.final_summary.clone()),
        transcript: agent_report
            .map(|report| report.transcript.clone())
            .unwrap_or_default(),
        files,
        skipped_files,
    };

    let overview_bytes = serde_json::to_vec_pretty(&overview)?;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(project_text.as_bytes())?;
    let project_snapshot = encoder.finish()?;
    Ok((overview_bytes, project_snapshot))
}

fn collect_project_snapshot_text(
    root: &Path,
) -> Result<(String, Vec<ChefSnapshotFile>, Vec<ChefSkippedFile>)> {
    fn walk(
        root: &Path,
        dir: &Path,
        out: &mut String,
        files: &mut Vec<ChefSnapshotFile>,
        skipped: &mut Vec<ChefSkippedFile>,
        total_bytes: &mut u64,
    ) -> Result<()> {
        let mut entries = fs::read_dir(dir)?.collect::<Result<Vec<_>, _>>()?;
        entries.sort_by_key(|entry| entry.path());

        for entry in entries {
            let path = entry.path();
            let relative = path
                .strip_prefix(root)
                .map_err(|_| eyre!("Failed to compute relative path for {}", path.display()))?;
            let normalized = relative.to_string_lossy().replace('\\', "/");
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("");

            if path.is_dir() {
                if should_skip_snapshot_dir(name) {
                    skipped.push(ChefSkippedFile {
                        path: normalized,
                        reason: "excluded directory".to_string(),
                    });
                    continue;
                }
                walk(root, &path, out, files, skipped, total_bytes)?;
                continue;
            }

            if normalized.is_empty() || normalized.contains("../") || normalized.starts_with('/') {
                skipped.push(ChefSkippedFile {
                    path: normalized,
                    reason: "unsafe path".to_string(),
                });
                continue;
            }
            if should_skip_snapshot_file(&normalized, name) {
                skipped.push(ChefSkippedFile {
                    path: normalized,
                    reason: "excluded file".to_string(),
                });
                continue;
            }
            if files.len() >= CHEF_SNAPSHOT_MAX_FILES {
                skipped.push(ChefSkippedFile {
                    path: normalized,
                    reason: "file limit reached".to_string(),
                });
                continue;
            }

            let metadata = fs::metadata(&path)?;
            if metadata.len() > CHEF_SNAPSHOT_MAX_FILE_BYTES {
                skipped.push(ChefSkippedFile {
                    path: normalized,
                    reason: "file too large".to_string(),
                });
                continue;
            }
            if *total_bytes + metadata.len() > CHEF_SNAPSHOT_MAX_TOTAL_BYTES {
                skipped.push(ChefSkippedFile {
                    path: normalized,
                    reason: "snapshot size limit reached".to_string(),
                });
                continue;
            }

            let bytes = fs::read(&path)?;
            let Ok(content) = String::from_utf8(bytes.clone()) else {
                skipped.push(ChefSkippedFile {
                    path: normalized,
                    reason: "non-utf8 file".to_string(),
                });
                continue;
            };
            if content_looks_sensitive(&content) {
                skipped.push(ChefSkippedFile {
                    path: normalized,
                    reason: "possible secret content".to_string(),
                });
                continue;
            }

            let sha256 = format!("{:x}", Sha256::digest(&bytes));
            out.push_str("\n===== BEGIN FILE ");
            out.push_str(&normalized);
            out.push_str(" =====\n");
            out.push_str("sha256: ");
            out.push_str(&sha256);
            out.push('\n');
            out.push_str("bytes: ");
            out.push_str(&bytes.len().to_string());
            out.push_str("\n\n");
            out.push_str(&content);
            if !content.ends_with('\n') {
                out.push('\n');
            }
            out.push_str("===== END FILE ");
            out.push_str(&normalized);
            out.push_str(" =====\n");

            *total_bytes += metadata.len();
            files.push(ChefSnapshotFile {
                path: normalized,
                size_bytes: metadata.len(),
                sha256,
            });
        }

        Ok(())
    }

    let mut text = String::from(
        "# Helix Chef Project Snapshot\n\nThis is an inert text rendering of selected project files.\n",
    );
    let mut files = Vec::new();
    let mut skipped = Vec::new();
    let mut total_bytes = 0;
    walk(
        root,
        root,
        &mut text,
        &mut files,
        &mut skipped,
        &mut total_bytes,
    )?;
    Ok((text, files, skipped))
}

fn should_skip_snapshot_dir(name: &str) -> bool {
    matches!(
        name,
        ".git"
            | ".helix"
            | "node_modules"
            | ".next"
            | "target"
            | "dist"
            | "build"
            | "coverage"
            | ".turbo"
            | ".cache"
    )
}

fn should_skip_snapshot_file(normalized_path: &str, name: &str) -> bool {
    let path = normalized_path.to_ascii_lowercase();
    let name = name.to_ascii_lowercase();
    name == ".env"
        || name.starts_with(".env.")
        || name.ends_with(".log")
        || name.ends_with(".pem")
        || name.ends_with(".key")
        || name.ends_with(".p12")
        || name.ends_with(".sqlite")
        || name.ends_with(".sqlite3")
        || name.ends_with(".db")
        || path.contains("credentials")
        || path.contains("secret")
}

fn content_looks_sensitive(content: &str) -> bool {
    let lower = content.to_ascii_lowercase();
    lower.contains("-----begin ") && lower.contains(" private key-----")
        || lower.contains("aws_secret_access_key")
        || lower.contains("github_token=")
        || lower.contains("api_key=")
        || lower.contains("secret_key=")
        || lower.contains("access_token=")
        || lower.contains("sk-ant-")
        || lower.contains("sk-proj-")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starter_seed_request_is_write_request() {
        let request = starter_seed_request();

        assert_eq!(request["request_type"], "write");
        assert!(request["query"]["queries"][0].get("ForEach").is_some());
        assert_eq!(request["parameter_types"]["data"]["Array"], "Object");
    }

    #[test]
    fn starter_read_request_reads_users() {
        let request = starter_read_request();
        let steps = &request["query"]["queries"][0]["Query"]["steps"];

        assert_eq!(request["request_type"], "read");
        assert!(steps[0].get("NWhere").is_some());
        assert_eq!(steps[1]["Limit"], 25);
    }

    #[test]
    fn starter_prompt_includes_user_intent() {
        let prompt = starter_prompt(Some("Build a todo app"));

        assert!(prompt.contains("Build a todo app"));
        assert!(prompt.contains("<user_intent>"));
        assert!(!prompt.contains("Personal CRM"));
    }

    #[test]
    fn starter_prompt_falls_back_to_default_project() {
        let prompt = starter_prompt(None);

        assert!(prompt.contains("Personal CRM"));
        assert!(prompt.contains("Contact"));
        assert!(prompt.contains("WORKS_AT"));
    }

    #[test]
    fn starter_prompt_treats_blank_intent_as_default() {
        let prompt = starter_prompt(Some("   "));

        assert!(prompt.contains("Personal CRM"));
    }

    #[test]
    fn default_project_spec_uses_nextjs_stack() {
        assert!(DEFAULT_PROJECT_SPEC.contains("Next.js"));
        assert!(DEFAULT_PROJECT_SPEC.contains("Tailwind"));
        assert!(DEFAULT_PROJECT_SPEC.contains("TypeScript"));
        assert!(DEFAULT_PROJECT_SPEC.contains("App Router"));
        assert!(!DEFAULT_PROJECT_SPEC.contains("web/index.html"));
        assert!(!DEFAULT_PROJECT_SPEC.contains("vanilla HTML"));
    }

    #[test]
    fn agent_prompt_template_uses_nextjs_stack() {
        let prompt = starter_prompt(Some("Build a recipe library"));

        assert!(prompt.contains("Build a recipe library"));
        assert!(prompt.contains("Next.js"));
        assert!(prompt.contains("Tailwind"));
        assert!(prompt.contains("App Router"));
        assert!(prompt.contains("create-next-app"));
        assert!(prompt.contains("vercel-labs/agent-skills"));
        assert!(prompt.contains("<install_more_skills>"));
        assert!(prompt.contains("npm run dev"));
        assert!(!prompt.contains("vanilla HTML"));
        assert!(!prompt.contains("no framework"));
    }

    #[test]
    fn agent_prompt_references_memory_system_skill() {
        let prompt = starter_prompt(Some("Build an assistant with long-term memory"));

        assert!(prompt.contains("helix-memory-system"));
    }

    #[test]
    fn agent_prompt_references_current_query_skill_names() {
        let prompt = starter_prompt(Some("Build a recipe library"));

        // TypeScript DSL is primary; JSON-dynamic remains as the documented fallback.
        assert!(prompt.contains("helix-query-typescript"));
        assert!(prompt.contains("helix-query-json-dynamic"));
        // The Rust authoring skill was renamed helix-query-authoring -> helix-query-rust.
        assert!(prompt.contains("helix-query-rust"));
        assert!(!prompt.contains("helix-query-authoring"));
        // Memory/retrieval intent is steered to the memory-system skill.
        assert!(prompt.contains("memory or retrieval"));
    }

    #[test]
    fn agent_prompt_defaults_to_typescript_dsl() {
        let prompt = starter_prompt(Some("Build a recipe library"));

        // Queries are authored with the npm DSL, not hand-written JSON.
        assert!(prompt.contains("@helix-db/helix-db"));
        assert!(prompt.contains("<typescript_dsl_quickref>"));
        assert!(prompt.contains("web/src/lib/queries/"));
        assert!(prompt.contains("web/src/lib/helix.ts"));
        // Raw JSON is positioned as the fallback, not the default.
        assert!(prompt.contains("fallback"));
        // Placeholders must be fully substituted — no leftover template tokens.
        assert!(!prompt.contains("{seed_state}"));
        assert!(!prompt.contains("{seed_step}"));
        assert!(!prompt.contains("{intent}"));
        // The old JSON-only mandate is gone.
        assert!(!prompt.contains("JSON dynamic queries only"));
    }

    #[test]
    fn agent_prompt_seed_state_is_intent_aware() {
        // With a real intent, chef did NOT seed and wrote no example files.
        let with_intent = starter_prompt(Some("Build a recipe library"));
        assert!(with_intent.contains("empty"));
        assert!(with_intent.contains("seed the DB yourself"));
        assert!(!with_intent.contains("Seeded starter `User` nodes"));

        // Blank intent: chef seeded the demo and the JSON reference files exist.
        let blank = starter_prompt(None);
        assert!(blank.contains("Seeded starter `User` nodes"));
        assert!(blank.contains("examples/seed.json"));
    }

    #[test]
    fn agent_prompt_uses_latest_openai_embedding_model() {
        let prompt = starter_prompt(Some("Build semantic search over my notes"));

        assert!(prompt.contains("<embeddings>"));
        assert!(prompt.contains("text-embedding-3-small"));
        assert!(prompt.contains("OPENAI_API_KEY"));
        // The embed helper calls the 3-series model, not the legacy default.
        assert!(prompt.contains("model: 'text-embedding-3-small'"));
    }

    #[test]
    fn agent_prompt_template_keeps_services_running() {
        let prompt = starter_prompt(Some("X"));

        // Background-detach pattern
        assert!(prompt.contains("nohup"));
        assert!(prompt.contains("& disown"));
        // Persistence requirement
        assert!(prompt.contains("still running"));
        // Backend-is-included explainer
        assert!(prompt.contains("frontend and backend in one process"));
        // Stop command for the user
        assert!(prompt.contains("pkill -f 'next dev'"));
        // Old "user-must-start-server" wording is gone
        assert!(!prompt.contains("must be running"));
    }

    #[test]
    fn agent_prompt_template_summary_has_commands_and_services() {
        let prompt = starter_prompt(Some("X"));

        // Existing sections still present.
        assert!(prompt.contains("### What you built"));
        assert!(prompt.contains("### Files created"));
        assert!(prompt.contains("### Files modified"));
        assert!(prompt.contains("### How to try it"));
        assert!(prompt.contains("### Known gaps"));
        // New sections.
        assert!(prompt.contains("### Services running"));
        assert!(prompt.contains("### Commands run"));
        // Old "use exactly these sections in this order: 5 sections" wording is updated.
        assert!(prompt.contains("seven sections"));
    }

    #[test]
    fn agent_prompt_template_workflow_includes_browser_open() {
        let prompt = starter_prompt(Some("X"));

        assert!(prompt.contains("Open the frontend in the user's default browser"));
        assert!(prompt.contains("open http://localhost:3000"));
        assert!(prompt.contains("xdg-open http://localhost:3000"));
        assert!(prompt.contains("start http://localhost:3000"));
    }

    #[test]
    fn design_guide_contains_brand_tokens() {
        // The guide is the source of truth for the Helix look — assert the
        // load-bearing tokens survive any future edits to the constant.
        assert!(DESIGN_GUIDE.contains("#FF5C01")); // brand orange accent
        assert!(DESIGN_GUIDE.contains("#1E1715")); // page background
        assert!(DESIGN_GUIDE.contains("#070504")); // card/panel background
        assert!(DESIGN_GUIDE.contains("tactical-corners")); // signature element + utility
        assert!(DESIGN_GUIDE.contains("rounded-none")); // square-corner rule
        assert!(DESIGN_GUIDE.contains("font-mono")); // typography convention
    }

    #[test]
    fn prompt_references_design_guide() {
        let prompt = starter_prompt(None);
        // Referenced from both the environment file list and the frontend section.
        assert!(prompt.contains("DESIGN.md"));
        assert!(prompt.contains("Helix brand"));
    }

    #[test]
    fn write_design_guide_creates_design_file() {
        let dir = env::temp_dir().join(format!(
            "helix-chef-test-design-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();

        write_design_guide(&dir).unwrap();

        let design = dir.join("DESIGN.md");
        assert!(design.exists());
        assert!(fs::read_to_string(&design).unwrap().contains("#FF5C01"));

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn write_agent_prompt_creates_prompt_file() {
        let dir = env::temp_dir().join(format!(
            "helix-chef-test-prompt-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();

        write_agent_prompt(&dir, Some("Build a CRM")).unwrap();

        assert!(dir.join("HELIX_CHEF_PROMPT.md").exists());
        assert!(!dir.join("examples/seed.json").exists());
        assert!(!dir.join("examples/read_users.json").exists());

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn write_example_queries_creates_seed_and_read_files() {
        let dir = env::temp_dir().join(format!(
            "helix-chef-test-examples-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();

        write_example_queries(&dir).unwrap();

        assert!(!dir.join("HELIX_CHEF_PROMPT.md").exists());
        assert!(dir.join("examples/seed.json").exists());
        assert!(dir.join("examples/read_users.json").exists());

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn project_snapshot_excludes_dependencies_and_secrets() {
        let dir = env::temp_dir().join(format!(
            "helix-chef-test-snapshot-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(dir.join("src")).unwrap();
        fs::create_dir_all(dir.join("node_modules/pkg")).unwrap();

        fs::write(dir.join("src/app.ts"), "export const ok = true;\n").unwrap();
        fs::write(dir.join(".env"), "API_KEY=secret\n").unwrap();
        fs::write(
            dir.join("node_modules/pkg/index.js"),
            "module.exports = {}\n",
        )
        .unwrap();
        fs::write(dir.join("src/token.ts"), "const value = 'sk-ant-test';\n").unwrap();

        let (snapshot, files, skipped) = collect_project_snapshot_text(&dir).unwrap();

        assert!(snapshot.contains("src/app.ts"));
        assert!(!snapshot.contains("node_modules/pkg/index.js"));
        assert!(!snapshot.contains("sk-ant-test"));
        assert_eq!(files.len(), 1);
        assert!(skipped.iter().any(|file| file.path == ".env"));
        assert!(skipped.iter().any(|file| file.path == "node_modules"));
        assert!(skipped.iter().any(|file| file.path == "src/token.ts"));

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn agent_priority_is_claude_codex_opencode() {
        assert_eq!(
            AGENT_PRIORITY,
            &[
                AgentKind::ClaudeCode,
                AgentKind::OpenAiCodex,
                AgentKind::OpenCode,
            ],
        );
        assert_eq!(AgentKind::ClaudeCode.binary(), "claude");
        assert_eq!(AgentKind::OpenAiCodex.binary(), "codex");
        assert_eq!(AgentKind::OpenCode.binary(), "opencode");
    }

    #[test]
    fn build_agent_argv_claude_full_auto() {
        let argv = build_agent_argv(
            AgentKind::ClaudeCode,
            PermissionMode::FullAuto,
            "HELIX_CHEF_PROMPT.md",
            Path::new("/tmp/proj"),
        );
        assert!(!argv.iter().any(|a| a == "--bare"));
        assert_eq!(argv[0], "--append-system-prompt-file");
        assert_eq!(argv[1], "HELIX_CHEF_PROMPT.md");
        assert!(argv.iter().any(|a| a == "--dangerously-skip-permissions"));
        assert!(!argv.iter().any(|a| a == "--permission-mode"));
        // Streaming output for progress visibility.
        assert!(argv.iter().any(|a| a == "--output-format"));
        assert!(argv.iter().any(|a| a == "stream-json"));
        assert!(argv.iter().any(|a| a == "--verbose"));
        // -p keeps Claude headless instead of launching its TUI.
        let p_index = argv.iter().position(|a| a == "-p").expect("-p present");
        assert_eq!(argv[p_index + 1], AGENT_USER_PROMPT);
        assert_eq!(argv.last().unwrap(), AGENT_USER_PROMPT);
    }

    #[test]
    fn build_agent_argv_claude_scoped() {
        let argv = build_agent_argv(
            AgentKind::ClaudeCode,
            PermissionMode::Scoped,
            "HELIX_CHEF_PROMPT.md",
            Path::new("/tmp/proj"),
        );
        assert!(!argv.iter().any(|a| a == "--bare"));
        assert!(argv.iter().any(|a| a == "--permission-mode"));
        assert!(argv.iter().any(|a| a == "acceptEdits"));
        assert!(!argv.iter().any(|a| a == "--dangerously-skip-permissions"));
        assert!(argv.iter().any(|a| a == "--output-format"));
        assert!(argv.iter().any(|a| a == "stream-json"));
        assert!(argv.iter().any(|a| a == "--verbose"));
        assert!(argv.iter().any(|a| a == "-p"));
        assert_eq!(argv.last().unwrap(), AGENT_USER_PROMPT);
    }

    #[test]
    fn build_agent_argv_codex_full_auto() {
        let argv = build_agent_argv(
            AgentKind::OpenAiCodex,
            PermissionMode::FullAuto,
            "HELIX_CHEF_PROMPT.md",
            Path::new("/tmp/proj"),
        );
        assert_eq!(argv[0], "exec");
        assert!(
            argv.iter()
                .any(|a| a == "--dangerously-bypass-approvals-and-sandbox")
        );
        assert!(!argv.iter().any(|a| a == "--sandbox"));
        assert!(argv.last().unwrap().contains("HELIX_CHEF_PROMPT.md"));
    }

    #[test]
    fn build_agent_argv_codex_scoped() {
        let argv = build_agent_argv(
            AgentKind::OpenAiCodex,
            PermissionMode::Scoped,
            "HELIX_CHEF_PROMPT.md",
            Path::new("/tmp/proj"),
        );
        assert_eq!(argv[0], "exec");
        assert!(argv.iter().any(|a| a == "--sandbox"));
        assert!(argv.iter().any(|a| a == "workspace-write"));
        assert!(argv.iter().any(|a| a == "--ask-for-approval"));
        assert!(argv.iter().any(|a| a == "on-request"));
        assert!(
            !argv
                .iter()
                .any(|a| a == "--dangerously-bypass-approvals-and-sandbox")
        );
    }

    #[test]
    fn build_agent_argv_opencode_full_auto() {
        let argv = build_agent_argv(
            AgentKind::OpenCode,
            PermissionMode::FullAuto,
            "HELIX_CHEF_PROMPT.md",
            Path::new("/tmp/proj"),
        );
        assert_eq!(argv[0], "run");
        assert_eq!(argv[1], "--dir");
        assert_eq!(argv[2], "/tmp/proj");
        assert!(argv.iter().any(|a| a == "--dangerously-skip-permissions"));
    }

    #[test]
    fn build_agent_argv_opencode_scoped() {
        let argv = build_agent_argv(
            AgentKind::OpenCode,
            PermissionMode::Scoped,
            "HELIX_CHEF_PROMPT.md",
            Path::new("/tmp/proj"),
        );
        assert_eq!(argv[0], "run");
        assert_eq!(argv[1], "--dir");
        assert!(!argv.iter().any(|a| a == "--dangerously-skip-permissions"));
    }

    // ---------- Claude stream-json event parsing ----------

    #[test]
    fn format_tool_use_edit() {
        let input = serde_json::json!({"file_path": "examples/seed.json", "old_string": "x", "new_string": "y"});
        assert_eq!(
            format_tool_use("Edit", &input).unwrap(),
            "✎ Editing examples/seed.json"
        );
    }

    #[test]
    fn format_tool_use_read() {
        let input = serde_json::json!({"file_path": "helix.toml"});
        assert_eq!(
            format_tool_use("Read", &input).unwrap(),
            "📖 Reading helix.toml"
        );
    }

    #[test]
    fn format_tool_use_bash_prefers_description() {
        let input =
            serde_json::json!({"command": "rm -rf /tmp/x", "description": "Clean up tmp dir"});
        assert_eq!(
            format_tool_use("Bash", &input).unwrap(),
            "💻 Clean up tmp dir"
        );
    }

    #[test]
    fn format_tool_use_bash_falls_back_to_command() {
        let input = serde_json::json!({"command": "ls -la"});
        assert_eq!(format_tool_use("Bash", &input).unwrap(), "💻 ls -la");
    }

    #[test]
    fn format_tool_use_todowrite_renders_with_count() {
        let input = serde_json::json!({"todos": [{"content": "a"}, {"content": "b"}]});
        let rendered = format_tool_use("TodoWrite", &input).unwrap();
        assert!(rendered.contains("Updating tasks"));
        assert!(rendered.contains("(2)"));
    }

    #[test]
    fn format_tool_use_todowrite_renders_without_count() {
        let input = serde_json::json!({});
        let rendered = format_tool_use("TodoWrite", &input).unwrap();
        assert_eq!(rendered, "📋 Updating tasks");
    }

    #[test]
    fn format_tool_use_unknown_tool() {
        let input = serde_json::json!({});
        assert_eq!(
            format_tool_use("SomethingNew", &input).unwrap(),
            "🔧 SomethingNew"
        );
    }

    #[test]
    fn format_tool_use_mcp_tool() {
        let input = serde_json::json!({});
        assert_eq!(
            format_tool_use("mcp__helixdb-docs__search", &input).unwrap(),
            "🔌 MCP: mcp__helixdb-docs__search"
        );
    }

    #[test]
    fn parse_claude_event_assistant_with_tool_use() {
        let line = r#"{
            "type": "assistant",
            "message": {
                "content": [
                    {"type": "tool_use", "name": "Edit", "input": {"file_path": "examples/seed.json"}}
                ]
            }
        }"#;
        let event: ClaudeEvent = serde_json::from_str(line).unwrap();
        let rendered = format_claude_event(&event);
        assert_eq!(rendered, vec!["✎ Editing examples/seed.json"]);
    }

    #[test]
    fn parse_claude_event_user_with_tool_error() {
        let line = r#"{
            "type": "user",
            "message": {
                "content": [
                    {"type": "tool_result", "is_error": true}
                ]
            }
        }"#;
        let event: ClaudeEvent = serde_json::from_str(line).unwrap();
        let rendered = format_claude_event(&event);
        assert_eq!(rendered, vec!["✗ tool error"]);
    }

    #[test]
    fn parse_claude_event_system_api_retry() {
        let line = r#"{"type": "system", "subtype": "api_retry"}"#;
        let event: ClaudeEvent = serde_json::from_str(line).unwrap();
        let rendered = format_claude_event(&event);
        assert_eq!(rendered, vec!["⟳ Retrying API call..."]);
    }

    #[test]
    fn parse_claude_event_unknown_type_falls_through() {
        let line = r#"{"type": "stream_event", "event": {"type": "message_start"}}"#;
        let event: ClaudeEvent = serde_json::from_str(line).unwrap();
        assert!(matches!(event, ClaudeEvent::Other));
        assert!(format_claude_event(&event).is_empty());
    }

    #[test]
    fn parse_claude_stream_line_routes_result_before_catch_all_event() {
        let line = r####"{"type": "result", "is_error": false, "duration_ms": 1000, "result": "### What you built\nA memory app."}"####;

        match parse_claude_stream_line(line).expect("result line should parse") {
            ClaudeStreamLine::Result(result) => {
                assert_eq!(format_result_stats(&result), "(1.0s)");
                assert_eq!(
                    result.result.as_deref(),
                    Some("### What you built\nA memory app.")
                );
            }
            ClaudeStreamLine::Event(_) => panic!("result line was swallowed as ClaudeEvent::Other"),
        }
    }

    #[test]
    fn parse_claude_stream_line_routes_progress_events() {
        let line = r#"{"type": "system", "subtype": "api_retry"}"#;

        match parse_claude_stream_line(line).expect("event line should parse") {
            ClaudeStreamLine::Event(event) => {
                assert_eq!(format_claude_event(&event), vec!["⟳ Retrying API call..."]);
            }
            ClaudeStreamLine::Result(_) => panic!("progress event parsed as result"),
        }
    }

    #[test]
    fn parse_result_event_success() {
        let line = r#"{"type": "result", "is_error": false, "duration_ms": 37200, "total_cost_usd": 0.412}"#;
        let result: ResultEvent = serde_json::from_str(line).unwrap();
        assert!(!result.is_error);
        assert_eq!(result.duration_ms, Some(37200));
        assert_eq!(result.total_cost_usd, Some(0.412));
        let stats = format_result_stats(&result);
        assert_eq!(stats, "(37.2s, $0.412)");
    }

    #[test]
    fn parse_result_event_empty_stats_when_no_fields() {
        let line = r#"{"type": "result", "is_error": true}"#;
        let result: ResultEvent = serde_json::from_str(line).unwrap();
        assert!(result.is_error);
        assert_eq!(format_result_stats(&result), "");
    }

    #[test]
    fn format_result_stats_duration_only() {
        let result = ResultEvent {
            is_error: false,
            duration_ms: Some(1500),
            total_cost_usd: None,
            result: None,
        };
        assert_eq!(format_result_stats(&result), "(1.5s)");
    }

    #[test]
    fn parse_result_event_captures_result_text() {
        let line = r####"{"type": "result", "is_error": false, "duration_ms": 1000, "result": "### What you built\nA recipe library MVP."}"####;
        let result: ResultEvent = serde_json::from_str(line).unwrap();
        assert_eq!(
            result.result.as_deref(),
            Some("### What you built\nA recipe library MVP.")
        );
    }

    #[test]
    fn parse_result_event_missing_text_is_none() {
        let line = r#"{"type": "result", "is_error": false}"#;
        let result: ResultEvent = serde_json::from_str(line).unwrap();
        assert!(result.result.is_none());
    }
}
