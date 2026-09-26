# HelixDB documentation

This directory is the canonical source for the public HelixDB documentation.
Mintlify reads `docs.json`; `llms.txt` and `llms-full.txt` are generated artifacts.

## Page contract

Every route in `docs.json` must have exactly one custom `pageType`:

- `Tutorial`
- `Guide`
- `Concept`
- `Reference`
- `Troubleshooting`

Render the same value as the first badge after frontmatter. Optional maturity
`status` values are `Preview`, `Beta`, or `Deprecated` and require a second matching
badge. Do not use Mintlify's native `tag` field because it adds the type to the
sidebar.

Database page paths mirror the sidebar hierarchy under
`database/helix-db/<group>/` and `database/helix-cloud/<group>/`. Keep redirects
when moving an existing public route.

`docs.json` also redirects short, guessable paths to their pages: product and SDK
names (`/helix-ts`, `/helixrs`, `/python`), topics (`/fts`, `/vector-search`,
`/rate-limits`), and routes of removed pages. When you add a page, add a shortcut for
any name a reader is likely to type. Shortcut sources must not match a live route.

## Site structure

| Tab | Groups | Covers |
| --- | --- | --- |
| HelixDB | Start Here, Core Concepts, Query Guides | The engine in every run mode: setup, SDKs, data model, queries, indexes and search, HTTP API, error codes, troubleshooting |
| Helix Cloud | Start Here, Connect and automate, Operate | Managed deployments only: account setup, connecting, architecture, MCP, security, tenancy, limits, gateway errors |
| CLI Reference | Using the helix CLI, CLI Command Reference | The `helix` CLI: workflows, configuration, and one page per command |

Put engine behavior that applies outside Cloud in the HelixDB tab, even when Cloud
users also need it. Link to it from Cloud pages instead of duplicating it.

## Style conventions

- Use sentence case for titles, sidebar labels, and headings. Keep product names
  capitalized: HelixDB, Helix Cloud, WorkOS.
- Give every page a frontmatter `description`; it feeds search, SEO, and `llms.txt`.
- Badge colors: `Tutorial` green, `Guide` blue, `Concept` purple, `Reference` gray,
  `Troubleshooting` orange.
- Open each page with one or two sentences saying what the page covers and when to use
  it, then show code before long prose.
- Show SDK behavior in a validated `CodeGroup` instead of describing code in prose.
- End guides with a `## Next steps` card group.

## Local checks

```bash
npm install
npm run generate-llms
npm run check
npx mint broken-links --check-anchors --check-redirects --check-snippets
npx mint dev --no-open
```

`npm run check-docs` validates navigation, page metadata, badges, redirects, legacy
AST/API markers, JSON examples, and Rust/TypeScript/Go/Python/JSON code groups.
Client-construction groups may omit JSON when immediately marked with
`{/* client-setup: no JSON representation */}`.
Package-install groups use Bash snippets for each SDK and
`{/* package-install: no JSON representation */}`.

The shared SDK parity suite verifies that Rust, TypeScript, Go, and Python serialize
the same operation-tree requests:

```bash
cd ../sdks/typescript
npm run parity:generate
npm run parity:compare-json
```

## Generated files

Run `npm run generate-llms` after changing navigation or page content. The generators
write page type and maturity as plain text so metadata remains available after JSX is
removed.
