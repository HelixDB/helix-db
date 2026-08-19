#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DOCS_ROOT = path.resolve(__dirname, '..');
const DOCS_JSON = path.join(DOCS_ROOT, 'docs.json');

const PAGE_TYPES = new Set([
  'Tutorial',
  'Guide',
  'Concept',
  'Reference',
  'Troubleshooting',
]);
const MATURITY_STATUSES = new Set(['Preview', 'Beta', 'Deprecated']);
const CORE_MULTI_SDK_PAGES = [
  'database/helix-db/core-concepts/overview',
  'database/helix-db/query-guides/reading-data',
  'database/helix-db/query-guides/writing-data',
  'database/helix-db/query-guides/secondary-indexes',
  'database/helix-db/query-guides/vector-indexes',
  'database/helix-db/query-guides/text-indexes',
  'database/helix-db/query-guides/traversals',
  'database/helix-db/query-guides/filtering',
  'database/helix-db/query-guides/projections',
  'database/helix-db/query-guides/parameters',
  'database/helix-db/query-guides/error-handling',
];
const SDK_SETUP_FILES = new Set([
  'database/helix-db/start-here/sdk-setup/rust-project-setup.mdx',
  'database/helix-db/start-here/sdk-setup/typescript-project-setup.mdx',
  'database/helix-db/start-here/sdk-setup/go-project-setup.mdx',
  'database/helix-db/start-here/sdk-setup/python-project-setup.mdx',
]);
const DATABASE_GROUP_PREFIXES = new Map([
  ['HelixDB/Start Here', 'database/helix-db/start-here/'],
  ['HelixDB/Core Concepts', 'database/helix-db/core-concepts/'],
  ['HelixDB/Query Guides', 'database/helix-db/query-guides/'],
  ['Helix Cloud/Start Here', 'database/helix-cloud/start-here/'],
  ['Helix Cloud/Connect and automate', 'database/helix-cloud/connect/'],
  ['Helix Cloud/Operate', 'database/helix-cloud/operate/'],
]);
const CLIENT_SETUP_MARKER =
  '{/* client-setup: no JSON representation */}';
const PACKAGE_INSTALL_MARKER =
  '{/* package-install: no JSON representation */}';
const SDK_COMMANDS_MARKER =
  '{/* sdk-commands: no JSON representation */}';
const SINGLE_SDK_EXAMPLES_MARKER =
  '{/* single-sdk-examples: TypeScript */}';
const EXAMPLE_LANGUAGES = [
  ['Rust', '```rust Rust'],
  ['TypeScript', '```ts TypeScript'],
  ['Go', '```go Go'],
  ['Python', '```python Python'],
  ['JSON', '```json JSON'],
];
const PACKAGE_INSTALL_LANGUAGES = [
  ['Rust', '```bash Rust'],
  ['TypeScript', '```bash TypeScript'],
  ['Go', '```bash Go'],
  ['Python', '```bash Python'],
];
const LEGACY_MARKERS = [
  ['legacy queries array', /"queries"\s*:/g],
  ['legacy steps array', /"steps"\s*:/g],
  ['legacy PascalCase Query tag', /"Query"\s*:/g],
  ['legacy PascalCase source operation', /"(?:NWhere|EWhere)"\s*:/g],
  ['legacy PascalCase value or operation', /"(?:String|Limit)"\s*:/g],
  ['legacy register attribute', /#\[register\]/g],
  ['legacy route map', /\b(?:read_routes|write_routes)\s*\./g],
  ['legacy generated-query API', /\b(?:generate_to_path|toDynamicJson|to_dynamic_json)\b/g],
  ['stored-procedure diagram', /\bStored Procedures?\b/g],
];

const errors = [];

function collectPages(node, output = []) {
  if (typeof node === 'string') {
    output.push(node);
    return output;
  }
  if (Array.isArray(node)) {
    for (const child of node) collectPages(child, output);
    return output;
  }
  if (node && typeof node === 'object' && Array.isArray(node.pages)) {
    collectPages(node.pages, output);
  }
  return output;
}

function filesUnder(directory) {
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const file = path.join(directory, entry.name);
    return entry.isDirectory() ? filesUnder(file) : [file];
  });
}

function unquote(value) {
  const trimmed = value.trim();
  if (
    trimmed.length >= 2 &&
    ((trimmed.startsWith('"') && trimmed.endsWith('"')) ||
      (trimmed.startsWith("'") && trimmed.endsWith("'")))
  ) {
    return trimmed.slice(1, -1);
  }
  return trimmed;
}

function frontmatterValue(lines, key, slug) {
  const matches = lines
    .filter((line) => new RegExp(`^${key}:\\s*`).test(line))
    .map((line) => unquote(line.replace(new RegExp(`^${key}:\\s*`), '')));
  if (matches.length > 1) {
    errors.push(`${slug}: frontmatter contains ${matches.length} ${key} values`);
  }
  return matches[0] ?? null;
}

function countBadge(body, value) {
  const escaped = value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return [...body.matchAll(new RegExp(`>\\s*${escaped}\\s*</Badge>`, 'g'))].length;
}

function lineNumber(content, offset) {
  return content.slice(0, offset).split('\n').length;
}

const config = JSON.parse(fs.readFileSync(DOCS_JSON, 'utf8'));
const tabNames = (config.navigation?.tabs ?? []).map((tab) => tab.tab);
const expectedTabs = ['HelixDB', 'Helix Cloud', 'CLI Reference'];
if (JSON.stringify(tabNames) !== JSON.stringify(expectedTabs)) {
  errors.push(`docs.json: tabs must be exactly ${expectedTabs.join(', ')}`);
}
const navigable = [];
for (const tab of config.navigation?.tabs ?? []) {
  for (const group of tab.groups ?? []) {
    const groupPages = collectPages(group);
    navigable.push(...groupPages);

    const groupKey = `${tab.tab}/${group.group}`;
    const expectedPrefix = DATABASE_GROUP_PREFIXES.get(groupKey);
    if (expectedPrefix !== undefined) {
      for (const route of groupPages) {
        if (!route.startsWith(expectedPrefix)) {
          errors.push(
            `docs.json: ${groupKey} page ${route} must live under ${expectedPrefix}`,
          );
        }
      }
    }
  }
}

const routeCounts = new Map();
for (const route of navigable) {
  routeCounts.set(route, (routeCounts.get(route) ?? 0) + 1);
}
for (const [route, count] of routeCounts) {
  if (count > 1) errors.push(`docs.json: duplicate navigable route ${route}`);
}

for (const slug of navigable) {
  const file = path.join(DOCS_ROOT, `${slug}.mdx`);
  if (!fs.existsSync(file)) {
    errors.push(`docs.json: missing page ${slug}.mdx`);
    continue;
  }
  const content = fs.readFileSync(file, 'utf8');
  const match = content.match(/^---\n([\s\S]*?)\n---\n?/);
  if (!match) {
    errors.push(`${slug}: missing frontmatter`);
    continue;
  }

  const frontmatter = match[1].split('\n');
  const nativeTag = frontmatterValue(frontmatter, 'tag', slug);
  if (nativeTag !== null) {
    errors.push(`${slug}: native tag renders in the sidebar; use pageType instead`);
  }
  const pageType = frontmatterValue(frontmatter, 'pageType', slug);
  if (!PAGE_TYPES.has(pageType)) {
    errors.push(
      `${slug}: pageType must be exactly one of ${[...PAGE_TYPES].join(', ')}`,
    );
  }

  const status = frontmatterValue(frontmatter, 'status', slug);
  if (status !== null && !MATURITY_STATUSES.has(status)) {
    errors.push(
      `${slug}: status must be one of ${[...MATURITY_STATUSES].join(', ')}`,
    );
  }

  const body = content.slice(match[0].length);
  const bodyLines = body.split('\n').filter((line) => line.trim().length > 0);
  if (pageType !== null) {
    if (countBadge(body, pageType) !== 1) {
      errors.push(`${slug}: rendered page-type badge must match pageType exactly once`);
    }
    if (!bodyLines[0]?.includes(`>${pageType}</Badge>`)) {
      errors.push(`${slug}: page-type badge must be the first rendered body line`);
    }
  }
  for (const maturity of MATURITY_STATUSES) {
    const expected = status === maturity ? 1 : 0;
    if (countBadge(body, maturity) !== expected) {
      errors.push(`${slug}: rendered ${maturity} badge does not match frontmatter`);
    }
  }
  if (status !== null) {
    const typeBadgePosition = body.indexOf(`>${pageType}</Badge>`);
    const statusBadgePosition = body.indexOf(`>${status}</Badge>`);
    if (statusBadgePosition <= typeBadgePosition) {
      errors.push(`${slug}: maturity badge must follow the page-type badge`);
    }
  }
}

const redirects = config.redirects ?? [];
const redirectSources = new Set();
for (const redirect of redirects) {
  const source = redirect.source?.replace(/^\/|\/$/g, '');
  const destination = redirect.destination?.split('#')[0].replace(/^\/|\/$/g, '');
  if (!source || !destination) {
    errors.push('docs.json: every redirect requires source and destination');
    continue;
  }
  if (redirectSources.has(source)) {
    errors.push(`docs.json: duplicate redirect source /${source}`);
  }
  redirectSources.add(source);
  if (routeCounts.has(source)) {
    errors.push(`docs.json: redirect source /${source} is also a live route`);
  }
  if (fs.existsSync(path.join(DOCS_ROOT, `${source}.mdx`))) {
    errors.push(`docs.json: redirect source /${source} still has a page`);
  }
  if (!fs.existsSync(path.join(DOCS_ROOT, `${destination}.mdx`))) {
    errors.push(`docs.json: redirect destination /${destination} does not exist`);
  }
}

const docsFiles = filesUnder(DOCS_ROOT).filter(
  (file) => file.endsWith('.mdx') || file.endsWith('.jsx'),
);
for (const file of docsFiles) {
  const relative = path.relative(DOCS_ROOT, file);
  if (relative.startsWith('database/') && relative.endsWith('.mdx')) {
    const route = relative.slice(0, -'.mdx'.length);
    if (!routeCounts.has(route)) {
      errors.push(`${relative}: database page is not present in sidebar navigation`);
    }
  }
}

for (const file of docsFiles) {
  const relative = path.relative(DOCS_ROOT, file);
  const content = fs.readFileSync(file, 'utf8');

  for (const [name, marker] of LEGACY_MARKERS) {
    marker.lastIndex = 0;
    const match = marker.exec(content);
    if (match) {
      errors.push(
        `${relative}:${lineNumber(content, match.index)}: found ${name}`,
      );
    }
  }

  if (file.endsWith('.mdx')) {
    for (const match of content.matchAll(/```json(?: [^\n]+)?\n([\s\S]*?)\n```/g)) {
      try {
        JSON.parse(match[1]);
      } catch (error) {
        errors.push(
          `${relative}:${lineNumber(content, match.index)}: invalid JSON example: ${error.message}`,
        );
      }
    }

    for (const [index, match] of [
      ...content.matchAll(/<CodeGroup>([\s\S]*?)<\/CodeGroup>/g),
    ].entries()) {
      const beforeCodeGroup = content.slice(0, match.index).trimEnd();
      const isClientSetup = beforeCodeGroup.endsWith(CLIENT_SETUP_MARKER);
      const isPackageInstall = beforeCodeGroup.endsWith(PACKAGE_INSTALL_MARKER);
      const isSdkCommands = beforeCodeGroup.endsWith(SDK_COMMANDS_MARKER);
      const isSingleSdkExamples = beforeCodeGroup.endsWith(
        SINGLE_SDK_EXAMPLES_MARKER,
      );
      if (isSingleSdkExamples) {
        const fences = [
          ...match[1].matchAll(/^```([^\s\n]+)(?:\s[^\n]+)?$/gm),
        ].map((fence) => fence[1]);
        if (fences.length === 0 || fences.some((language) => language !== 'ts')) {
          errors.push(
            `${relative}: CodeGroup ${index + 1} single-SDK examples must contain only TypeScript fences`,
          );
        }
        continue;
      }
      const expectedLanguages = isPackageInstall || isSdkCommands
        ? PACKAGE_INSTALL_LANGUAGES
        : EXAMPLE_LANGUAGES;
      for (const [language, fence] of expectedLanguages) {
        const count = match[1].split(fence).length - 1;
        const expected = isClientSetup && language === 'JSON' ? 0 : 1;
        if (count !== expected) {
          errors.push(
            `${relative}: CodeGroup ${index + 1} must contain exactly ${expected} ${language} example(s)`,
          );
        }
      }
      if (
        (isPackageInstall || isSdkCommands) &&
        match[1].includes('```json JSON')
      ) {
        errors.push(
          `${relative}: CodeGroup ${index + 1} SDK commands must not contain a JSON example`,
        );
      }
    }

    if (relative.startsWith('database/') && !SDK_SETUP_FILES.has(relative)) {
      const outsideCodeGroups = content.replace(
        /<CodeGroup>[\s\S]*?<\/CodeGroup>/g,
        '',
      );
      const standalone = outsideCodeGroups.match(
        /^```(?:rust|ts|go|python)(?:\s[^\n]*)?$/m,
      );
      if (standalone) {
        errors.push(
          `${relative}:${lineNumber(content, content.indexOf(standalone[0]))}: SDK examples must use a validated CodeGroup`,
        );
      }
    }
  }
}

for (const slug of CORE_MULTI_SDK_PAGES) {
  const file = path.join(DOCS_ROOT, `${slug}.mdx`);
  const content = fs.readFileSync(file, 'utf8');
  for (const [, fence] of EXAMPLE_LANGUAGES) {
    if (!content.includes(fence)) {
      errors.push(`${slug}: core guide is missing ${fence.slice(3)} example`);
    }
  }
}

const errorCodeSource = fs.readFileSync(
  path.resolve(DOCS_ROOT, '..', 'crates/ast/src/error_code.rs'),
  'utf8',
);
const asStrStart = errorCodeSource.indexOf('pub const fn as_str');
const asStrEnd = errorCodeSource.indexOf('impl core::fmt::Display', asStrStart);
if (asStrStart < 0 || asStrEnd < 0) {
  errors.push('crates/ast/src/error_code.rs: cannot locate QueryErrorCode::as_str');
} else {
  const sourceCodes = [
    ...errorCodeSource.slice(asStrStart, asStrEnd).matchAll(/"([a-z0-9_]+)"/g),
  ].map((match) => match[1]);
  const errorReference = fs.readFileSync(
    path.join(DOCS_ROOT, 'database/helix-db/query-guides/error-handling.mdx'),
    'utf8',
  );
  const documentedCodes = [
    ...errorReference.matchAll(
      /^\| `([a-z0-9_]+)` \| [^|]+ \| [^|]+ \| [^|]+ \| [^|]+ \|$/gm,
    ),
  ].map((match) => match[1]);
  const duplicateCodes = documentedCodes.filter(
    (code, index) => documentedCodes.indexOf(code) !== index,
  );
  if (duplicateCodes.length > 0) {
    errors.push(
      `error-handling: duplicate catalog code(s): ${[...new Set(duplicateCodes)].join(', ')}`,
    );
  }
  const sourceSet = new Set(sourceCodes);
  const documentedSet = new Set(documentedCodes);
  const missingCodes = [...sourceSet].filter((code) => !documentedSet.has(code));
  const unknownCodes = [...documentedSet].filter((code) => !sourceSet.has(code));
  if (missingCodes.length > 0) {
    errors.push(`error-handling: missing catalog code(s): ${missingCodes.join(', ')}`);
  }
  if (unknownCodes.length > 0) {
    errors.push(`error-handling: unknown catalog code(s): ${unknownCodes.join(', ')}`);
  }
}

if (errors.length > 0) {
  console.error(`Documentation validation failed with ${errors.length} error(s):`);
  for (const error of errors) console.error(`  - ${error}`);
  process.exit(1);
}

console.log(
  `Validated ${navigable.length} navigable pages, ${redirects.length} redirect(s), ` +
    `${CORE_MULTI_SDK_PAGES.length} cross-SDK guides, and all JSON examples.`,
);
