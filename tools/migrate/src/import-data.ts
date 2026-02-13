/**
 * Imports exported Supabase data into a running HelixDB instance.
 */

import * as fs from "fs";
import * as path from "path";
import {
  GeneratedSchema,
  NodeSchema,
  EdgeSchema,
  FieldSchema,
  VectorSchema,
} from "./generate-schema";
import { nodeNullableSetterQueryName } from "./generate-queries";
import { TableInfo } from "./introspect";
import { exportFileNameForTable, makeTableKey } from "./export-data";

export interface ImportOptions {
  helixUrl: string;
  exportDir: string;
  schema: GeneratedSchema;
  tables: TableInfo[];
  concurrency: number;
  onProgress?: (table: string, imported: number, total: number) => void;
}

export interface ImportError {
  table: string;
  row: number;
  error: string;
}

export interface ImportEntityStats {
  attempted: number;
  imported: number;
  failed: number;
  skipped: number;
  unresolved: number;
}

export interface ImportResult {
  nodesImported: number;
  edgesImported: number;
  vectorsImported: number;
  errors: ImportError[];
  warnings: string[];
  idMap: Map<string, Map<string, string>>;
  nodeStats: Record<string, ImportEntityStats>;
  edgeStats: Record<string, ImportEntityStats>;
  vectorStats: Record<string, ImportEntityStats>;
}

const HELIX_TIMEOUT_MS = 30_000;
const HELIX_MAX_ATTEMPTS = 4;

export async function importData(options: ImportOptions): Promise<ImportResult> {
  const { helixUrl, exportDir, schema, tables, concurrency, onProgress } = options;

  const safeConcurrency = Math.max(1, Math.floor(concurrency));

  const result: ImportResult = {
    nodesImported: 0,
    edgesImported: 0,
    vectorsImported: 0,
    errors: [],
    warnings: [],
    idMap: new Map(),
    nodeStats: {},
    edgeStats: {},
    vectorStats: {},
  };

  const tableInfoByKey = new Map<string, TableInfo>();
  for (const table of tables) {
    tableInfoByKey.set(makeTableKey(table.schema, table.name), table);
  }

  const tableLookupMaps = new Map<string, Map<string, Map<string, string>>>();
  const sortedNodes = topologicalSort(schema.nodes, schema.edges);

  for (const node of sortedNodes) {
    const tableKey = node.tableKey;
    const stats = getStats(result.nodeStats, tableKey);

    const exportFile = path.join(
      exportDir,
      exportFileNameForTable(node.originalSchema, node.originalTable)
    );
    if (!fs.existsSync(exportFile)) {
      result.warnings.push(`Missing export file for ${tableKey}: ${exportFile}`);
      continue;
    }

    const rows = readRows(exportFile);
    const tableInfo = tableInfoByKey.get(tableKey);
    if (!tableInfo) {
      result.warnings.push(`Missing table metadata for ${tableKey}; skipping node import`);
      continue;
    }

    const pkColumns = tableInfo.primaryKeys;
    const uniqueKeySets = getUniqueColumnSets(tableInfo);

    const lookupByKeySet = initializeLookupMaps(tableLookupMaps, tableKey, uniqueKeySets);

    const requiredFields = node.fields.filter((field) => !field.isNullable);
    const nullableFields = node.fields.filter((field) => field.isNullable);

    const tableIdMap = new Map<string, string>();
    result.idMap.set(tableKey, tableIdMap);

    let importedInTable = 0;
    const batches = chunk(rows, safeConcurrency);

    for (const batch of batches) {
      const promises = batch.map(async (row, batchIdx) => {
        const rowIdx = importedInTable + batchIdx;
        stats.attempted += 1;

        try {
          const body: Record<string, unknown> = {};
          for (const field of requiredFields) {
            const raw = row[field.originalColumn];
            if (raw === null || raw === undefined) {
              throw new Error(
                `Missing required column ${field.originalColumn} for non-nullable field ${field.name}`
              );
            }

            body[field.name] = coerceFieldValue(
              raw,
              field,
              `${tableKey} row ${rowIdx} column ${field.originalColumn}`
            );
          }

          const response = await callHelix(helixUrl, `Import${node.name}`, body);
          const newId = extractId(response);
          if (!newId) {
            throw new Error(
              `Import${node.name} did not return an id; cannot complete import for this row`
            );
          }

          if (pkColumns.length > 0) {
            const oldPkKey = buildCompositeKeyFromColumns(row, pkColumns);
            if (oldPkKey === null) {
              throw new Error(
                `Primary key columns missing for ${tableKey} (${pkColumns.join(", ")})`
              );
            }
            tableIdMap.set(oldPkKey, newId);
          }

          for (const keySet of uniqueKeySets) {
            const lookupKey = buildCompositeKeyFromColumns(row, keySet);
            if (lookupKey === null) {
              continue;
            }

            const keySetMap = lookupByKeySet.get(columnSignature(keySet));
            keySetMap?.set(lookupKey, newId);
          }

          for (const field of nullableFields) {
            const raw = row[field.originalColumn];
            if (raw === null || raw === undefined) {
              continue;
            }

            const setterName = nodeNullableSetterQueryName(node.name, field.name);
            const value = coerceFieldValue(
              raw,
              field,
              `${tableKey} row ${rowIdx} nullable column ${field.originalColumn}`
            );

            await callHelix(helixUrl, setterName, {
              id: newId,
              value,
            });
          }

          stats.imported += 1;
          result.nodesImported += 1;
        } catch (err) {
          stats.failed += 1;
          result.errors.push({
            table: tableKey,
            row: rowIdx,
            error: err instanceof Error ? err.message : String(err),
          });
        }
      });

      await Promise.all(promises);
      importedInTable += batch.length;
      onProgress?.(tableKey, importedInTable, rows.length);
    }

    if (pkColumns.length > 0 && stats.imported > 0 && tableIdMap.size !== stats.imported) {
      throw new Error(
        `ID mapping mismatch for ${tableKey}: imported ${stats.imported} rows but mapped ${tableIdMap.size} IDs`
      );
    }
  }

  for (const edge of schema.edges) {
    const edgeStatKey = `${edge.fromTableKey}::${edge.name}`;
    const stats = getStats(result.edgeStats, edgeStatKey);

    const sourceNode = schema.nodes.find((node) => node.tableKey === edge.fromTableKey);
    if (!sourceNode) {
      result.warnings.push(`Skipping edge ${edge.name}: missing source node ${edge.fromTableKey}`);
      continue;
    }

    const sourceInfo = tableInfoByKey.get(edge.fromTableKey);
    const targetInfo = tableInfoByKey.get(edge.toTableKey);
    if (!sourceInfo || !targetInfo) {
      result.warnings.push(
        `Skipping edge ${edge.name}: missing table metadata for ${edge.fromTableKey} or ${edge.toTableKey}`
      );
      continue;
    }

    if (sourceInfo.primaryKeys.length === 0) {
      result.warnings.push(
        `Skipping edge ${edge.name}: source table ${edge.fromTableKey} has no primary key`
      );
      continue;
    }

    const sourceIdMap = result.idMap.get(edge.fromTableKey);
    if (!sourceIdMap) {
      result.warnings.push(
        `Skipping edge ${edge.name}: no source ID map found for ${edge.fromTableKey}`
      );
      continue;
    }

    const targetLookup = tableLookupMaps
      .get(edge.toTableKey)
      ?.get(columnSignature(edge.referencedColumns));
    if (!targetLookup) {
      result.warnings.push(
        `Skipping edge ${edge.name}: target columns (${edge.referencedColumns.join(", ")}) are not a known unique key on ${edge.toTableKey}`
      );
      continue;
    }

    const exportFile = path.join(
      exportDir,
      exportFileNameForTable(sourceNode.originalSchema, sourceNode.originalTable)
    );
    if (!fs.existsSync(exportFile)) {
      result.warnings.push(`Missing export file for edge ${edge.name}: ${exportFile}`);
      continue;
    }

    const rows = readRows(exportFile);
    const deferred: Array<{ row: Record<string, unknown>; rowIdx: number }> = [];

    let importedInEdge = 0;
    const batches = chunk(rows, safeConcurrency);

    for (const batch of batches) {
      const promises = batch.map(async (row, batchIdx) => {
        const rowIdx = importedInEdge + batchIdx;
        stats.attempted += 1;

        try {
          const sourcePkKey = buildCompositeKeyFromColumns(row, sourceInfo.primaryKeys);
          if (sourcePkKey === null) {
            stats.failed += 1;
            result.errors.push({
              table: edgeStatKey,
              row: rowIdx,
              error: `Missing source primary key values (${sourceInfo.primaryKeys.join(", ")})`,
            });
            return;
          }

          const fkKey = buildCompositeKeyFromColumns(row, edge.originalColumns);
          if (fkKey === null) {
            stats.skipped += 1;
            return;
          }

          const fromId = sourceIdMap.get(sourcePkKey);
          const toId = targetLookup.get(fkKey);
          if (!fromId || !toId) {
            deferred.push({ row, rowIdx });
            return;
          }

          await callHelix(helixUrl, `Import${edge.name}`, {
            from_id: fromId,
            to_id: toId,
          });

          stats.imported += 1;
          result.edgesImported += 1;
        } catch (err) {
          stats.failed += 1;
          result.errors.push({
            table: edgeStatKey,
            row: rowIdx,
            error: err instanceof Error ? err.message : String(err),
          });
        }
      });

      await Promise.all(promises);
      importedInEdge += batch.length;
      onProgress?.(`Edge: ${edge.name}`, importedInEdge, rows.length);
    }

    for (const { row, rowIdx } of deferred) {
      try {
        const sourcePkKey = buildCompositeKeyFromColumns(row, sourceInfo.primaryKeys);
        const fkKey = buildCompositeKeyFromColumns(row, edge.originalColumns);

        if (!sourcePkKey || !fkKey) {
          stats.unresolved += 1;
          continue;
        }

        const fromId = sourceIdMap.get(sourcePkKey);
        const toId = targetLookup.get(fkKey);
        if (!fromId || !toId) {
          stats.unresolved += 1;
          result.errors.push({
            table: edgeStatKey,
            row: rowIdx,
            error: `Unresolved FK mapping for edge ${edge.name}`,
          });
          continue;
        }

        await callHelix(helixUrl, `Import${edge.name}`, {
          from_id: fromId,
          to_id: toId,
        });

        stats.imported += 1;
        result.edgesImported += 1;
      } catch (err) {
        stats.failed += 1;
        result.errors.push({
          table: edgeStatKey,
          row: rowIdx,
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }
  }

  for (const vector of schema.vectors) {
    const vectorStatKey = `${vector.tableKey}::${vector.name}`;
    const stats = getStats(result.vectorStats, vectorStatKey);

    const exportFile = path.join(
      exportDir,
      exportFileNameForTable(vector.originalSchema, vector.originalTable)
    );
    if (!fs.existsSync(exportFile)) {
      result.warnings.push(
        `Missing export file for vector ${vector.name}: ${exportFile}`
      );
      continue;
    }

    const rows = readRows(exportFile);
    const requiredMetadata = vector.metadataFields.filter((field) => !field.isNullable);
    const nullableMetadata = vector.metadataFields.filter((field) => field.isNullable);

    if (nullableMetadata.length > 0) {
      result.warnings.push(
        `Vector ${vector.name} has nullable metadata fields that are not imported (${nullableMetadata
          .map((field) => field.name)
          .join(", ")})`
      );
    }

    let importedInVector = 0;
    const batches = chunk(rows, safeConcurrency);

    for (const batch of batches) {
      const promises = batch.map(async (row, batchIdx) => {
        const rowIdx = importedInVector + batchIdx;
        stats.attempted += 1;

        try {
          const vectorValue = row[vector.vectorColumn];
          if (vectorValue === null || vectorValue === undefined) {
            stats.skipped += 1;
            return;
          }

          const body: Record<string, unknown> = {
            vector: parseVectorValue(vectorValue),
          };

          for (const field of requiredMetadata) {
            const raw = row[field.originalColumn];
            if (raw === null || raw === undefined) {
              throw new Error(
                `Missing required metadata column ${field.originalColumn} for vector ${vector.name}`
              );
            }

            body[field.name] = coerceFieldValue(
              raw,
              field,
              `${vector.tableKey} row ${rowIdx} vector metadata ${field.originalColumn}`
            );
          }

          for (const field of nullableMetadata) {
            const raw = row[field.originalColumn];
            if (raw !== null && raw !== undefined) {
              stats.skipped += 1;
              break;
            }
          }

          await callHelix(helixUrl, `Import${vector.name}`, body);

          stats.imported += 1;
          result.vectorsImported += 1;
        } catch (err) {
          stats.failed += 1;
          result.errors.push({
            table: vectorStatKey,
            row: rowIdx,
            error: err instanceof Error ? err.message : String(err),
          });
        }
      });

      await Promise.all(promises);
      importedInVector += batch.length;
      onProgress?.(`Vector: ${vector.name}`, importedInVector, rows.length);
    }
  }

  return result;
}

async function callHelix(
  baseUrl: string,
  queryName: string,
  body: Record<string, unknown>
): Promise<unknown> {
  const url = `${baseUrl.replace(/\/+$/, "")}/${queryName}`;
  let lastError: unknown;

  for (let attempt = 1; attempt <= HELIX_MAX_ATTEMPTS; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), HELIX_TIMEOUT_MS);

    try {
      const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        signal: controller.signal,
      });

      clearTimeout(timeout);

      if (!response.ok) {
        const text = await response.text();
        const retryable = response.status >= 500 || response.status === 429;

        if (retryable && attempt < HELIX_MAX_ATTEMPTS) {
          await wait(backoffMs(attempt));
          continue;
        }

        throw new Error(`HelixDB API error (${response.status}): ${text}`);
      }

      const raw = await response.text();
      if (!raw.trim()) {
        return {};
      }

      try {
        return JSON.parse(raw);
      } catch {
        return raw;
      }
    } catch (err) {
      clearTimeout(timeout);
      lastError = err;

      const isAbort = err instanceof Error && err.name === "AbortError";
      const isNetwork = err instanceof TypeError;
      if ((isAbort || isNetwork) && attempt < HELIX_MAX_ATTEMPTS) {
        await wait(backoffMs(attempt));
        continue;
      }

      if (attempt >= HELIX_MAX_ATTEMPTS) {
        break;
      }
    }
  }

  throw lastError instanceof Error
    ? lastError
    : new Error("Failed to call HelixDB API");
}

export function extractId(response: unknown): string | null {
  if (typeof response === "string") {
    return response;
  }

  const seen = new Set<unknown>();

  function visit(value: unknown, depth: number): string | null {
    if (depth > 10 || value === null || value === undefined) {
      return null;
    }

    if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      return null;
    }

    if (typeof value !== "object") {
      return null;
    }

    if (seen.has(value)) {
      return null;
    }
    seen.add(value);

    if (Array.isArray(value)) {
      for (const item of value) {
        const found = visit(item, depth + 1);
        if (found) {
          return found;
        }
      }
      return null;
    }

    const objectValue = value as Record<string, unknown>;

    if (typeof objectValue.id === "string") {
      return objectValue.id;
    }
    if (typeof objectValue.id === "number") {
      return String(objectValue.id);
    }

    for (const nested of Object.values(objectValue)) {
      const found = visit(nested, depth + 1);
      if (found) {
        return found;
      }
    }

    return null;
  }

  return visit(response, 0);
}

function topologicalSort(nodes: NodeSchema[], edges: EdgeSchema[]): NodeSchema[] {
  const deps = new Map<string, Set<string>>();
  for (const node of nodes) {
    deps.set(node.tableKey, new Set());
  }

  for (const edge of edges) {
    if (edge.fromTableKey !== edge.toTableKey) {
      deps.get(edge.fromTableKey)?.add(edge.toTableKey);
    }
  }

  const sorted: NodeSchema[] = [];
  const visited = new Set<string>();
  const visiting = new Set<string>();

  function visit(tableKey: string): void {
    if (visited.has(tableKey)) {
      return;
    }
    if (visiting.has(tableKey)) {
      return;
    }

    visiting.add(tableKey);
    const dependencies = deps.get(tableKey);
    if (dependencies) {
      for (const dep of dependencies) {
        visit(dep);
      }
    }
    visiting.delete(tableKey);
    visited.add(tableKey);

    const node = nodes.find((candidate) => candidate.tableKey === tableKey);
    if (node) {
      sorted.push(node);
    }
  }

  for (const node of nodes) {
    visit(node.tableKey);
  }

  return sorted;
}

function chunk<T>(arr: T[], size: number): T[][] {
  const safeSize = Math.max(1, size);
  const chunks: T[][] = [];

  for (let i = 0; i < arr.length; i += safeSize) {
    chunks.push(arr.slice(i, i + safeSize));
  }

  return chunks;
}

function readRows(filePath: string): Record<string, unknown>[] {
  return JSON.parse(fs.readFileSync(filePath, "utf-8")) as Record<string, unknown>[];
}

export function buildCompositeKeyFromColumns(
  row: Record<string, unknown>,
  columns: string[]
): string | null {
  if (columns.length === 0) {
    return null;
  }

  const values: unknown[] = [];

  for (const column of columns) {
    const value = row[column];
    if (value === null || value === undefined) {
      return null;
    }
    values.push(normalizeKeyValue(value));
  }

  return JSON.stringify(values);
}

function normalizeKeyValue(value: unknown): unknown {
  if (value instanceof Date) {
    return value.toISOString();
  }

  if (typeof value === "bigint") {
    return value.toString();
  }

  if (Buffer.isBuffer(value)) {
    return value.toString("base64");
  }

  if (Array.isArray(value)) {
    return value.map((entry) => normalizeKeyValue(entry));
  }

  if (value && typeof value === "object") {
    return JSON.stringify(value);
  }

  return value;
}

function parseVectorValue(value: unknown): number[] {
  let parsed: unknown = value;

  if (typeof value === "string") {
    parsed = JSON.parse(value);
  }

  if (!Array.isArray(parsed)) {
    throw new Error("Vector value must be an array");
  }

  return parsed.map((entry) => toFiniteNumber(entry));
}

function coerceFieldValue(
  value: unknown,
  field: FieldSchema,
  context: string
): unknown {
  try {
    return coerceForHelix(value, field);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    throw new Error(`Failed to coerce ${context}: ${message}`);
  }
}

function coerceForHelix(value: unknown, field: FieldSchema): unknown {
  if (field.helixType.startsWith("[")) {
    return coerceArray(value, field.helixType);
  }

  switch (field.helixType) {
    case "I8":
    case "I16":
    case "I32":
    case "I64":
    case "U8":
    case "U16":
    case "U32":
    case "U64":
    case "U128":
      return toInteger(value);
    case "F32":
    case "F64":
      return toFiniteNumber(value);
    case "Boolean":
      return toBoolean(value);
    case "Date":
      if (value instanceof Date) {
        return value.toISOString();
      }
      return value;
    case "String":
      if (typeof value === "string") {
        return value;
      }
      if (value instanceof Date) {
        return value.toISOString();
      }
      if (field.needsSerialization && typeof value === "object") {
        return JSON.stringify(value);
      }
      return String(value);
    default:
      return value;
  }
}

function coerceArray(value: unknown, helixType: string): unknown[] {
  const innerType = helixType.slice(1, -1).trim();
  let arr: unknown[];

  if (Array.isArray(value)) {
    arr = value;
  } else if (typeof value === "string") {
    const parsed = JSON.parse(value);
    if (!Array.isArray(parsed)) {
      throw new Error(`Expected array for ${helixType}`);
    }
    arr = parsed;
  } else {
    throw new Error(`Expected array for ${helixType}`);
  }

  return arr.map((entry) => coerceSimpleValue(entry, innerType));
}

function coerceSimpleValue(value: unknown, helixType: string): unknown {
  switch (helixType) {
    case "I8":
    case "I16":
    case "I32":
    case "I64":
    case "U8":
    case "U16":
    case "U32":
    case "U64":
    case "U128":
      return toInteger(value);
    case "F32":
    case "F64":
      return toFiniteNumber(value);
    case "Boolean":
      return toBoolean(value);
    default:
      return value;
  }
}

function toInteger(value: unknown): number {
  const maxSafe = BigInt(Number.MAX_SAFE_INTEGER);
  const minSafe = BigInt(Number.MIN_SAFE_INTEGER);

  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error("Expected finite integer value");
    }
    if (!Number.isSafeInteger(value)) {
      throw new Error("Unsafe integer; use --bigint-mode string to avoid precision loss");
    }
    return Math.trunc(value);
  }

  if (typeof value === "string") {
    if (!/^-?\d+$/.test(value.trim())) {
      throw new Error(`Invalid integer literal: ${value}`);
    }
    const asBigInt = BigInt(value);
    if (asBigInt > maxSafe || asBigInt < minSafe) {
      throw new Error(
        `Integer ${value} exceeds JS safe range; use --bigint-mode string`
      );
    }
    return Number(asBigInt);
  }

  throw new Error("Expected integer-compatible value");
}

function toFiniteNumber(value: unknown): number {
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error("Expected finite number");
    }
    return value;
  }

  if (typeof value === "string") {
    const parsed = Number(value);
    if (!Number.isFinite(parsed)) {
      throw new Error(`Invalid numeric literal: ${value}`);
    }
    return parsed;
  }

  throw new Error("Expected numeric-compatible value");
}

function toBoolean(value: unknown): boolean {
  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "number") {
    if (value === 1) return true;
    if (value === 0) return false;
  }

  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (["true", "t", "1"].includes(normalized)) return true;
    if (["false", "f", "0"].includes(normalized)) return false;
  }

  throw new Error("Expected boolean-compatible value");
}

function getUniqueColumnSets(table: TableInfo): string[][] {
  const keySets: string[][] = [];

  if (table.primaryKeys.length > 0) {
    keySets.push([...table.primaryKeys]);
  }

  const grouped = new Map<string, typeof table.indexes>();
  for (const index of table.indexes) {
    const list = grouped.get(index.indexName) ?? [];
    list.push(index);
    grouped.set(index.indexName, list);
  }

  for (const group of grouped.values()) {
    if (!group[0]?.isUnique) {
      continue;
    }

    const ordered = [...group].sort((a, b) => a.columnPosition - b.columnPosition);
    const columns = ordered.map((index) => index.columnName);
    const signature = columnSignature(columns);
    if (!keySets.some((existing) => columnSignature(existing) === signature)) {
      keySets.push(columns);
    }
  }

  return keySets;
}

function initializeLookupMaps(
  tableLookupMaps: Map<string, Map<string, Map<string, string>>>,
  tableKey: string,
  keySets: string[][]
): Map<string, Map<string, string>> {
  const lookupByKeySet = new Map<string, Map<string, string>>();

  for (const keySet of keySets) {
    lookupByKeySet.set(columnSignature(keySet), new Map());
  }

  tableLookupMaps.set(tableKey, lookupByKeySet);
  return lookupByKeySet;
}

function columnSignature(columns: string[]): string {
  return JSON.stringify(columns);
}

function getStats(
  bucket: Record<string, ImportEntityStats>,
  key: string
): ImportEntityStats {
  if (!bucket[key]) {
    bucket[key] = {
      attempted: 0,
      imported: 0,
      failed: 0,
      skipped: 0,
      unresolved: 0,
    };
  }

  return bucket[key];
}

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function backoffMs(attempt: number): number {
  const base = 250;
  return Math.min(4_000, base * 2 ** Math.max(0, attempt - 1));
}

export function saveIdMapping(
  idMap: Map<string, Map<string, string>>,
  outputPath: string
): void {
  const serializable: Record<string, Record<string, string>> = {};

  for (const [tableKey, mapping] of idMap) {
    serializable[tableKey] = Object.fromEntries(mapping);
  }

  fs.writeFileSync(outputPath, JSON.stringify(serializable, null, 2));
}
