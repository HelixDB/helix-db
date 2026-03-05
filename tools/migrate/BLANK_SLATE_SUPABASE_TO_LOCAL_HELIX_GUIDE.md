# Blank-Slate Guide: Supabase -> Local Helix Migration

This guide takes an engineer from zero setup to a successful migration from a fresh Supabase project into a local Helix instance running via the Helix CLI.

It uses:

- Supabase Cloud (free tier is fine)
- Local Helix via `helix push dev`
- The migration tool in this repo (`tools/migrate`)

---

## 1) Prerequisites

Install these first:

- `git`
- Node.js `>=18` and `npm`
- Helix CLI

Install Helix CLI:

```bash
curl -sSL "https://install.helix-db.com" | bash
helix --version
```

switch to branch and install migration dependencies:

```bash
git switch claude/supabase-helix-migration-zqJvP
cd helix-db
npm --prefix tools/migrate ci
npm --prefix tools/migrate run build
```

---

## 2) Create a Supabase Project

1. Go to https://supabase.com and create a new project.
2. Wait for it to finish provisioning.
3. Open **Project Settings -> Database -> Connection string -> URI**.
4. Copy the URI and keep it handy. It should look like:

```text
postgresql://postgres:<PASSWORD>@<HOST>:5432/postgres?sslmode=require
```

Set it in your shell:

```bash
export SUPABASE_DB_URL='postgresql://postgres:<PASSWORD>@<HOST>:5432/postgres?sslmode=require'
```

---

## 3) Seed Supabase with Test Data

In Supabase, open **SQL Editor** and run the script below.

This creates:

- `profiles` (users)
- `posts` (FK to profiles)
- `documents` (FK to profiles + vector embeddings)

```sql
create extension if not exists pgcrypto;
create extension if not exists vector;

drop table if exists documents cascade;
drop table if exists posts cascade;
drop table if exists profiles cascade;

create table profiles (
  id uuid primary key,
  email text not null unique,
  full_name text not null,
  age integer not null,
  metadata jsonb not null,
  created_at timestamptz not null default now()
);

create table posts (
  id uuid primary key,
  author_id uuid not null references profiles(id),
  title text not null,
  body text not null,
  published boolean not null default false,
  created_at timestamptz not null default now()
);

create table documents (
  id uuid primary key,
  owner_id uuid not null references profiles(id),
  content text not null,
  embedding vector(3) not null,
  created_at timestamptz not null default now()
);

insert into profiles (id, email, full_name, age, metadata) values
  ('11111111-1111-1111-1111-111111111111', 'alice@example.com', 'Alice Doe', 31, '{"plan":"pro","region":"us"}'),
  ('22222222-2222-2222-2222-222222222222', 'bob@example.com', 'Bob Doe', 27, '{"plan":"free","region":"eu"}'),
  ('33333333-3333-3333-3333-333333333333', 'carol@example.com', 'Carol Doe', 35, '{"plan":"team","region":"us"}');

insert into posts (id, author_id, title, body, published) values
  ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1', '11111111-1111-1111-1111-111111111111', 'Hello Helix', 'Migrating from Supabase', true),
  ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2', '11111111-1111-1111-1111-111111111111', 'Second Post', 'Still testing', false),
  ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa3', '22222222-2222-2222-2222-222222222222', 'Bob Post', 'Hi there', true),
  ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa4', '33333333-3333-3333-3333-333333333333', 'Carol Post', 'Ship it', true);

insert into documents (id, owner_id, content, embedding) values
  ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb1', '11111111-1111-1111-1111-111111111111', 'Alice document', '[0.10, 0.20, 0.30]'::vector),
  ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb2', '22222222-2222-2222-2222-222222222222', 'Bob document',   '[0.30, 0.10, 0.50]'::vector),
  ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb3', '33333333-3333-3333-3333-333333333333', 'Carol document', '[0.90, 0.40, 0.20]'::vector);
```

---

## 4) Generate Helix Project from Supabase Schema

From repo root:

```bash
node tools/migrate/dist/index.js supabase \
  --connection-string "$SUPABASE_DB_URL" \
  --output ./tmp/helix-project \
  --export-dir ./tmp/helix-export \
  --non-interactive \
  --introspect-only
```

What this does:

- introspects Supabase schema
- generates `schema.hx`, `queries.hx`, `import.hx`
- writes migration manifest at `./tmp/helix-project/.helix-migrate/manifest.json`
- runs `helix check` on generated project (unless you pass `--skip-helix-check`)

---

## 5) Start Local Helix

In terminal A:

```bash
cd ./tmp/helix-project
helix push dev
```

Keep this terminal running.

---

## 6) Run Full Migration

In terminal B (repo root):

```bash
node tools/migrate/dist/index.js supabase \
  --connection-string "$SUPABASE_DB_URL" \
  --output ./tmp/helix-project \
  --export-dir ./tmp/helix-export \
  --helix-url http://localhost:6969 \
  --non-interactive
```

Notes:

- Strict mode is on by default. The command fails if warnings/errors occur.
- If you explicitly want partial migration behavior, add `--no-strict`.

---

## 7) Verify Migration

Check migration summary:

```bash
jq '{nodesImported, edgesImported, vectorsImported, errorCount, warnings}' ./tmp/helix-export/migration-report.json
```

For the seed data above, expected totals are:

- `nodesImported`: `10` (`3 profiles + 4 posts + 3 documents`)
- `edgesImported`: `7` (`posts.author_id + documents.owner_id`)
- `vectorsImported`: `3`

Check original->new ID mapping:

```bash
jq 'keys' ./tmp/helix-export/id_mapping.json
```

Fetch one migrated profile through Helix API:

```bash
PROFILE_ID=$(jq -r '."public.profiles"["[\"11111111-1111-1111-1111-111111111111\"]"]' ./tmp/helix-export/id_mapping.json)

curl -s http://localhost:6969/GetProfile \
  -H 'content-type: application/json' \
  -d "{\"id\":\"$PROFILE_ID\"}" | jq
```

---

## 8) Test Import-Only Re-run

You can re-import without re-introspecting:

```bash
node tools/migrate/dist/index.js supabase \
  --import-only \
  --output ./tmp/helix-project \
  --export-dir ./tmp/helix-export \
  --helix-url http://localhost:6969 \
  --non-interactive
```

This uses only generated artifacts + exported JSON.

---

## 9) Useful Flags

- `--bigint-mode string|i64` (default `string`)
- `--include-tables public.profiles,public.posts`
- `--exclude-tables public.audit_logs`
- `--skip-helix-check` (skip compile gate)
- `--no-strict` (allow partial migration)

---

## 10) Troubleshooting

- **`helix check` fails**
  - open `./tmp/helix-project/db/schema.hx` and `./tmp/helix-project/db/queries.hx`, fix schema/query mismatches, rerun migration.

- **Strict mode fails with warnings/errors**
  - inspect `./tmp/helix-export/migration-report.json`.
  - fix root cause and rerun, or use `--no-strict` if partial migration is acceptable.

- **Cannot connect to Supabase**
  - confirm DB URL password/host/port.
  - ensure `sslmode=require` is present.

- **Helix API connection refused**
  - ensure `helix push dev` is running and the URL matches `--helix-url`.
