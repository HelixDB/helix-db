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
| Learn | One group per topic | Vendor-neutral explainers that answer one broad question each, such as "What is BM25?" |

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

### Learn pages

Learn pages target the questions people ask search engines and AI assistants, so their
structure is fixed:

- One main topic per page, named by a question in the title. A neighboring concept gets
  2-4 sentences and a link to its own page, not an in-depth section.
- Answer the title question completely in the first two or three sentences, then add the
  learning-objectives block (`<div className="learn-objectives">` wrapping a `Card` titled
  "Learning objectives").
- Phrase every `##` section as a question, with the answer directly below it. The right-hand
  "On this page" list then reads as the questions the page answers.
- Teach the concept vendor-neutrally. Do not name or characterize other vendors.
- Keep HelixDB to one `## How does HelixDB ...?` section near the end, stating only facts the
  product docs already support, and link to the guides.
- End with `## Frequently asked questions` (question-phrased `###` headings with visible
  answers, not accordions) and `## Related topics`.
- Add every new page as a question link on the `/learn` tile page, link it from the related
  guide, and add shortcut redirects for its obvious names.

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
