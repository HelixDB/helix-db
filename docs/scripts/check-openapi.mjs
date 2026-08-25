#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DOCS_ROOT = path.resolve(__dirname, '..');
const SPEC_PATH = path.join(DOCS_ROOT, 'openapi.json');
const ROUTER_PATH = path.resolve(DOCS_ROOT, '../crates/server/src/http.rs');

const errors = [];
let spec;

try {
  spec = JSON.parse(fs.readFileSync(SPEC_PATH, 'utf8'));
} catch (error) {
  console.error(`openapi.json is not valid JSON: ${error.message}`);
  process.exit(1);
}

if (spec.openapi !== '3.1.0') {
  errors.push('openapi.json: openapi must be exactly 3.1.0');
}
if (!spec.info?.title?.includes('HelixDB')) {
  errors.push('openapi.json: info.title must include HelixDB');
}
if (
  spec.servers?.length !== 1 ||
  spec.servers[0]?.url !== 'http://localhost:6969'
) {
  errors.push('openapi.json: root servers must describe only the local server');
}

const router = fs.readFileSync(ROUTER_PATH, 'utf8');
const routerOperations = new Set(
  [...router.matchAll(/\.route\("([^"]+)",\s*(get|post)\(/g)].map(
    ([, route, method]) => `${method} ${route}`,
  ),
);
const documentedOperations = new Set(
  Object.entries(spec.paths ?? {}).flatMap(([route, pathItem]) =>
    ['get', 'post', 'put', 'patch', 'delete'].flatMap((method) =>
      pathItem[method] ? [`${method} ${route}`] : [],
    ),
  ),
);

for (const operation of routerOperations) {
  if (!documentedOperations.has(operation)) {
    errors.push(`openapi.json: missing server operation ${operation}`);
  }
}
for (const operation of documentedOperations) {
  if (!routerOperations.has(operation)) {
    errors.push(`openapi.json: documents nonexistent server operation ${operation}`);
  }
}

const operationIds = [];
for (const [route, pathItem] of Object.entries(spec.paths ?? {})) {
  for (const method of ['get', 'post', 'put', 'patch', 'delete']) {
    const operation = pathItem[method];
    if (!operation) continue;
    if (!operation.operationId) {
      errors.push(`openapi.json: ${method.toUpperCase()} ${route} needs operationId`);
    } else {
      operationIds.push(operation.operationId);
    }
    if (!operation.responses || Object.keys(operation.responses).length === 0) {
      errors.push(`openapi.json: ${method.toUpperCase()} ${route} needs responses`);
    }
  }
}
if (new Set(operationIds).size !== operationIds.length) {
  errors.push('openapi.json: operationId values must be unique');
}

const querySchema = spec.components?.schemas?.QueryRequest;
if (!querySchema?.oneOf || querySchema.oneOf.length !== 2) {
  errors.push('openapi.json: QueryRequest must close over read and write variants');
}
const examples = spec.paths?.['/v2/query']?.post?.requestBody?.content?.[
  'application/json'
]?.examples;
if (!examples?.read?.value || !examples?.write?.value) {
  errors.push('openapi.json: /v2/query needs read and write request examples');
}

const queryOperation = spec.paths?.['/v2/query']?.post;
const queryServerUrls = queryOperation?.servers?.map(({ url }) => url) ?? [];
if (
  queryServerUrls.length !== 2 ||
  !queryServerUrls.includes('http://localhost:6969') ||
  !queryServerUrls.includes('https://{gatewayHost}')
) {
  errors.push(
    'openapi.json: /v2/query must advertise the local server and Helix Cloud gateway',
  );
}
for (const route of ['/healthz', '/readyz']) {
  if (spec.paths?.[route]?.get?.servers) {
    errors.push(`openapi.json: ${route} must inherit the local root server`);
  }
}

const documentedQueryStatuses = Object.keys(queryOperation?.responses ?? {}).sort();
const expectedQueryStatuses = [
  '200',
  '204',
  '400',
  '401',
  '402',
  '403',
  '408',
  '409',
  '413',
  '429',
  '500',
  '503',
];
if (documentedQueryStatuses.join(',') !== expectedQueryStatuses.join(',')) {
  errors.push(
    `openapi.json: /v2/query response codes must be ${expectedQueryStatuses.join(', ')}`,
  );
}
const expectedQueryResponseRefs = {
  400: 'BadRequest',
  401: 'Unauthorized',
  402: 'PaymentRequired',
  403: 'Forbidden',
  408: 'RequestTimeout',
  409: 'Conflict',
  413: 'PayloadTooLarge',
  429: 'RateLimited',
  500: 'InternalError',
  503: 'Unavailable',
};
for (const [status, name] of Object.entries(expectedQueryResponseRefs)) {
  if (
    queryOperation.responses?.[status]?.$ref !==
    `#/components/responses/${name}`
  ) {
    errors.push(`openapi.json: ${status} must reference ${name}`);
  }
}

const responseSchemaRef = (name) =>
  spec.components?.responses?.[name]?.content?.['application/json']?.schema?.$ref;
for (const name of [
  'BadRequest',
  'Unauthorized',
  'PaymentRequired',
  'Forbidden',
  'RequestTimeout',
  'Conflict',
  'PayloadTooLarge',
  'RateLimited',
  'InternalError',
  'Unavailable',
]) {
  if (responseSchemaRef(name) !== '#/components/schemas/QueryError') {
    errors.push(`openapi.json: ${name} must use QueryError`);
  }
}
const bodyLimits = queryOperation?.['x-helix-request-body-limits'];
if (
  bodyLimits?.localBytes !== 16 * 1024 * 1024 ||
  bodyLimits?.helixCloudBytes !== 2 * 1024 * 1024
) {
  errors.push(
    'openapi.json: /v2/query must distinguish the 16 MiB local and 2 MiB Cloud body limits',
  );
}
const payloadTooLarge = spec.components?.responses?.PayloadTooLarge;
const payloadTooLargeJson = payloadTooLarge?.content?.['application/json'];
if (
  payloadTooLargeJson?.schema?.$ref !== '#/components/schemas/QueryError' ||
  payloadTooLargeJson?.example?.error !== 'payload_too_large' ||
  payloadTooLargeJson?.example?.msg !== 'request body exceeds the maximum allowed size' ||
  payloadTooLarge?.content?.['text/plain']
) {
  errors.push('openapi.json: PayloadTooLarge must match the gateway JSON envelope');
}

const queryError = spec.components?.schemas?.QueryError;
if (
  queryError?.additionalProperties !== false ||
  queryError.required?.join(',') !== 'error,msg' ||
  !queryError.properties?.error ||
  !queryError.properties?.msg
) {
  errors.push('openapi.json: QueryError must require only error and msg');
}

const scalarParameterTypes = spec.components?.schemas?.QueryParameterType?.oneOf?.find(
  (variant) => Array.isArray(variant.enum),
)?.enum;
const expectedScalarParameterTypes = [
  'bool',
  'date_time',
  'i64',
  'object',
  'string',
  'value',
];
if (
  !scalarParameterTypes ||
  scalarParameterTypes.toSorted().join(',') !== expectedScalarParameterTypes.join(',')
) {
  errors.push(
    'openapi.json: typed JSON parameters must omit bytes, f32, and f64',
  );
}

if (errors.length > 0) {
  for (const error of errors) console.error(error);
  process.exit(1);
}

console.log(
  `openapi.json matches ${routerOperations.size} local operations and validates the Helix Cloud query contract.`,
);
