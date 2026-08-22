#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, '..');
const DOCS_JSON = path.join(REPO_ROOT, 'docs.json');
const OUT = path.join(REPO_ROOT, 'llms.txt');
const BASE_URL = 'https://docs.helix-db.com';

const TITLE = '# HelixDB';
const SUMMARY =
  '> HelixDB combines a property graph, approximate vector search, and BM25 full-text search behind one operation-tree query model. Requests run through Cloud, a local server, or the embedded runtime.';

// Inline starter recipe so an agent's first fetch of llms.txt already contains
// the install command and a runnable first write — without it, agents must
// guess which page holds the setup commands.
const QUICKSTART = [
  'Quickstart (local instance requires Docker or Podman):',
  '',
  '```bash',
  'curl -sSL "https://install.helix-db.com" | bash   # install the helix CLI',
  'helix init local                                  # scaffold helix.toml + examples/',
  'helix start dev                                   # start a local instance',
  'helix query dev -e \'writeBatch().varAs("alice", g().addN("User", { username: "alice" })).returning(["alice"])\'',
  'helix query dev -e \'readBatch().varAs("users", g().nWithLabel("User")).returning(["users"])\'',
  '```',
  '',
  'Full walkthrough: https://docs.helix-db.com/database/helix-db/start-here/quickstart',
].join('\n');

const OPTIONAL_PREFIXES = ['change-log/', 'benchmarks/'];

function readFrontmatter(slug) {
  const file = path.join(REPO_ROOT, slug + '.mdx');
  if (!fs.existsSync(file)) return { missing: true, slug };
  const content = fs.readFileSync(file, 'utf8');
  const match = content.match(/^---\n([\s\S]*?)\n---/);
  if (!match) return { title: slugToTitle(slug), description: null };
  const body = match[1];
  const titleLine = body.match(/^title:\s*["']?(.+?)["']?\s*$/m);
  const descLine = body.match(/^description:\s*["']?(.+?)["']?\s*$/m);
  const pageTypeLine = body.match(/^pageType:\s*["']?(.+?)["']?\s*$/m);
  const statusLine = body.match(/^status:\s*["']?(.+?)["']?\s*$/m);
  return {
    title: titleLine ? titleLine[1] : slugToTitle(slug),
    description: descLine ? descLine[1] : null,
    pageType: pageTypeLine ? pageTypeLine[1] : null,
    status: statusLine ? statusLine[1] : null,
  };
}

function slugToTitle(slug) {
  return slug
    .split('/')
    .pop()
    .replace(/[-_]/g, ' ')
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function flattenPages(node, acc = []) {
  if (typeof node === 'string') {
    acc.push(node);
  } else if (node && Array.isArray(node.pages)) {
    for (const child of node.pages) flattenPages(child, acc);
  }
  return acc;
}

function isOptional(slug) {
  return OPTIONAL_PREFIXES.some((p) => slug.startsWith(p));
}

function makeBullet(slug, missingSlugs) {
  const fm = readFrontmatter(slug);
  if (fm.missing) {
    missingSlugs.push(slug);
    return null;
  }
  const url = `${BASE_URL}/${slug}`;
  const desc = fm.description ? `: ${fm.description}` : '';
  const metadata = [
    fm.pageType ? `Page type: ${fm.pageType}` : null,
    fm.status ? `Status: ${fm.status}` : null,
  ].filter(Boolean);
  const suffix = metadata.length > 0 ? ` — ${metadata.join('; ')}` : '';
  return `- [${fm.title}](${url})${desc}${suffix}`;
}

function makeSection(name, slugs, missingSlugs) {
  const bullets = slugs.map((s) => makeBullet(s, missingSlugs)).filter(Boolean);
  if (bullets.length === 0) return null;
  return `## ${name}\n${bullets.join('\n')}`;
}

function build() {
  const docs = JSON.parse(fs.readFileSync(DOCS_JSON, 'utf8'));
  const sections = [];
  const optionalSlugs = [];
  const seenOptional = new Set();
  const missingSlugs = [];

  const groupTabs = new Map();
  for (const tab of docs.navigation.tabs) {
    for (const group of tab.groups) {
      if (!groupTabs.has(group.group)) groupTabs.set(group.group, new Set());
      groupTabs.get(group.group).add(tab.tab);
    }
  }

  for (const tab of docs.navigation.tabs) {
    for (const group of tab.groups) {
      const slugs = flattenPages(group);
      const nonOptional = [];
      for (const s of slugs) {
        if (isOptional(s)) {
          if (!seenOptional.has(s)) {
            seenOptional.add(s);
            optionalSlugs.push(s);
          }
        } else {
          nonOptional.push(s);
        }
      }
      if (nonOptional.length === 0) continue;
      let name = group.group;
      if (groupTabs.get(group.group).size > 1) {
        name = `${tab.tab} — ${group.group}`;
      }
      const section = makeSection(name, nonOptional, missingSlugs);
      if (section) sections.push(section);
    }
  }

  if (optionalSlugs.length > 0) {
    const optSection = makeSection('Optional', optionalSlugs, missingSlugs);
    if (optSection) sections.push(optSection);
  }

  const lines = [TITLE, '', SUMMARY, '', QUICKSTART, ''];
  for (const s of sections) {
    lines.push(s, '');
  }
  const output = lines.join('\n').trimEnd() + '\n';

  return { output, missingSlugs, sectionCount: sections.length };
}

const args = process.argv.slice(2);
const { output, missingSlugs, sectionCount } = build();

if (missingSlugs.length > 0) {
  console.error('Missing .mdx files for the following slugs in docs.json:');
  for (const s of missingSlugs) console.error(`  - ${s}`);
}

if (args.includes('--check')) {
  const existing = fs.existsSync(OUT) ? fs.readFileSync(OUT, 'utf8') : '';
  if (existing !== output) {
    console.error('\nllms.txt is out of date. Run `npm run generate-llms` to regenerate.');
    process.exit(1);
  }
  console.log('llms.txt is up to date.');
} else {
  fs.writeFileSync(OUT, output);
  console.log(
    `Wrote ${path.relative(REPO_ROOT, OUT)} — ${output.length} chars, ${sectionCount} sections${
      missingSlugs.length > 0 ? `, ${missingSlugs.length} missing slugs (see above)` : ''
    }`
  );
}
