#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, '..');
const DOCS_JSON = path.join(REPO_ROOT, 'docs.json');
const OUT = path.join(REPO_ROOT, 'llms-full.txt');

const HEADER = `# HelixDB

> HelixDB combines a property graph, approximate vector search, and BM25 full-text search behind one operation-tree query model. Requests run through Cloud, a local server, or the embedded runtime. This file is the full markdown corpus for AI agents.

`;

function flattenPages(node, acc = []) {
  if (typeof node === 'string') {
    acc.push(node);
  } else if (node && Array.isArray(node.pages)) {
    for (const child of node.pages) flattenPages(child, acc);
  }
  return acc;
}

function slugToTitle(slug) {
  return slug
    .split('/')
    .pop()
    .replace(/[-_]/g, ' ')
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function loadPage(slug) {
  const file = path.join(REPO_ROOT, slug + '.mdx');
  if (!fs.existsSync(file)) return { missing: true, slug };
  const raw = fs.readFileSync(file, 'utf8');

  let title = slugToTitle(slug);
  let pageType = null;
  let status = null;
  let body = raw;

  const fmMatch = body.match(/^---\n([\s\S]*?)\n---\n?/);
  if (fmMatch) {
    const fm = fmMatch[1];
    const titleLine = fm.match(/^title:\s*["']?(.+?)["']?\s*$/m);
    const pageTypeLine = fm.match(/^pageType:\s*["']?(.+?)["']?\s*$/m);
    const statusLine = fm.match(/^status:\s*["']?(.+?)["']?\s*$/m);
    if (titleLine) title = titleLine[1];
    if (pageTypeLine) pageType = pageTypeLine[1];
    if (statusLine) status = statusLine[1];
    body = body.slice(fmMatch[0].length);
  }

  body = body
    .split(/(```[\s\S]*?```)/g)
    .map((segment) => {
      if (segment.startsWith('```')) return segment;

      let markdown = segment.replace(/^import\s+[^\n]*\n/gm, '');
      markdown = markdown.replace(
        /^\{\/\* (?:(?:client-setup|package-install): no JSON representation|single-sdk-examples: TypeScript) \*\/\}\s*$/gm,
        '',
      );
      markdown = markdown.replace(
        /^>\s*For the complete documentation index optimized for AI agents,\s*see \[llms\.txt\]\(\/llms\.txt\)\.\s*$/gm,
        '',
      );

      // Remove JSX presentation while retaining nested Markdown. Code fences
      // are split out above so language imports and examples remain exact.
      markdown = markdown.replace(/^[ \t]*<[A-Z][A-Za-z0-9]*\b[\s\S]*?\/>\s*$/gm, '');
      markdown = markdown.replace(
        /^[ \t]*<([A-Z][A-Za-z0-9]*)\b[^>]*>.*<\/\1>\s*$/gm,
        '',
      );
      markdown = markdown.replace(/^[ \t]*<\/?[A-Z][A-Za-z0-9]*\b[^>]*>\s*$/gm, '');
      // Layout wrappers and the rendered page-type badge; the page type is
      // already written as plain-text metadata above the body.
      markdown = markdown.replace(
        /^\s*<\/?div\b[^>]*>(?:<Badge\b[^>]*>[^<]*<\/Badge>)*(?:<\/div>)?\s*$/gm,
        '',
      );
      return markdown;
    })
    .join('');

  body = body.replace(/\n{3,}/g, '\n\n').trim();

  return { title, pageType, status, body };
}

function build() {
  const docs = JSON.parse(fs.readFileSync(DOCS_JSON, 'utf8'));
  const missingSlugs = [];
  const seenSlugs = new Set();
  const parts = [HEADER];

  for (const tab of docs.navigation.tabs) {
    for (const group of tab.groups) {
      const slugs = flattenPages(group);
      const pages = [];
      for (const slug of slugs) {
        if (seenSlugs.has(slug)) continue;
        seenSlugs.add(slug);
        const page = loadPage(slug);
        if (page.missing) {
          missingSlugs.push(slug);
          continue;
        }
        pages.push(page);
      }
      if (pages.length === 0) continue;
      for (const p of pages) {
        const metadata = [
          `Page type: ${p.pageType ?? 'Unknown'}`,
          p.status ? `Status: ${p.status}` : null,
        ].filter(Boolean);
        parts.push(`# ${p.title}\n\n${metadata.join('\n')}\n\n${p.body}\n`);
      }
    }
  }

  return { output: parts.join('\n') + '\n', missingSlugs };
}

const args = process.argv.slice(2);
const { output, missingSlugs } = build();

if (missingSlugs.length > 0) {
  console.error('Missing .mdx files for the following slugs in docs.json:');
  for (const s of missingSlugs) console.error(`  - ${s}`);
}

if (args.includes('--check')) {
  const existing = fs.existsSync(OUT) ? fs.readFileSync(OUT, 'utf8') : '';
  if (existing !== output) {
    console.error('\nllms-full.txt is out of date. Run `npm run generate-llms` to regenerate.');
    process.exit(1);
  }
  console.log('llms-full.txt is up to date.');
} else {
  fs.writeFileSync(OUT, output);
  console.log(
    `Wrote ${path.relative(REPO_ROOT, OUT)} — ${output.length} chars${
      missingSlugs.length > 0 ? `, ${missingSlugs.length} missing slugs (see above)` : ''
    }`
  );
}
