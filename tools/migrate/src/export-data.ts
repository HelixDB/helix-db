/**
 * Exports data from a Supabase/PostgreSQL database as JSON.
 *
 * Reads all rows from each table, handles pagination for large tables,
 * and serializes complex types (JSON, arrays) appropriately.
 */

import { Client } from "pg";
import * as fs from "fs";
import * as path from "path";
import { TableInfo, ColumnInfo } from "./introspect";
import { mapPgType, TypeMappingOptions } from "./type-map";

export interface ExportOptions {
  connectionString: string;
  tables: TableInfo[];
  outputDir: string;
  batchSize: number; // rows per batch for large tables
  typeMappingOptions: TypeMappingOptions;
}

export interface ExportResult {
  table: string;
  rowCount: number;
  filePath: string;
}

/**
 * Export all data from the specified tables to JSON files.
 * Each table gets its own JSON file: <outputDir>/<table_name>.json
 */
export async function exportData(
  options: ExportOptions
): Promise<ExportResult[]> {
  const { connectionString, tables, outputDir, batchSize, typeMappingOptions } = options;

  // Ensure output directory exists
  fs.mkdirSync(outputDir, { recursive: true });

  const client = new Client({ connectionString });
  await client.connect();

  const results: ExportResult[] = [];

  try {
    for (const table of tables) {
      const filePath = path.join(
        outputDir,
        exportFileNameForTable(table.schema, table.name)
      );
      const rowCount = await exportTable(
        client,
        table,
        filePath,
        batchSize,
        typeMappingOptions
      );

      results.push({
        table: makeTableKey(table.schema, table.name),
        rowCount,
        filePath,
      });
    }
  } finally {
    await client.end();
  }

  return results;
}

/**
 * Export a single table to a JSON file.
 * Uses cursor-based pagination with the PK for large tables.
 */
async function exportTable(
  client: Client,
  table: TableInfo,
  filePath: string,
  batchSize: number,
  typeMappingOptions: TypeMappingOptions
): Promise<number> {
  const schema = table.schema;
  const tableName = table.name;

  // Determine which columns to export (all of them, with type info for serialization)
  const columns = table.columns;
  const columnNames = columns.map((c) => `"${c.name}"`).join(", ");

  // For small tables, just SELECT all
  if (table.rowCount <= batchSize) {
    const result = await client.query(
      `SELECT ${columnNames} FROM "${schema}"."${tableName}"`
    );

    const rows = result.rows.map((row, idx) =>
      transformRow(row, columns, makeTableKey(schema, tableName), idx, typeMappingOptions)
    );
    fs.writeFileSync(filePath, JSON.stringify(rows, null, 2));
    return rows.length;
  }

  // For large tables, use OFFSET pagination and stream to file
  const writeStream = fs.createWriteStream(filePath);
  writeStream.write("[\n");

  let offset = 0;
  let totalRows = 0;
  let isFirst = true;

  // Use a primary key or ctid for ordering
  const orderBy = table.primaryKeys.length > 0
    ? table.primaryKeys.map((pk) => `"${pk}"`).join(", ")
    : "ctid";

  while (true) {
    const result = await client.query(
      `SELECT ${columnNames} FROM "${schema}"."${tableName}" ORDER BY ${orderBy} LIMIT $1 OFFSET $2`,
      [batchSize, offset]
    );

    if (result.rows.length === 0) break;

    for (let rowOffset = 0; rowOffset < result.rows.length; rowOffset += 1) {
      const row = result.rows[rowOffset];
      const transformed = transformRow(
        row,
        columns,
        makeTableKey(schema, tableName),
        offset + rowOffset,
        typeMappingOptions
      );
      if (!isFirst) {
        writeStream.write(",\n");
      }
      writeStream.write("  " + JSON.stringify(transformed));
      isFirst = false;
    }

    totalRows += result.rows.length;
    offset += batchSize;

    if (result.rows.length < batchSize) break;
  }

  writeStream.write("\n]");
  writeStream.end();

  // Wait for stream to finish
  await new Promise<void>((resolve, reject) => {
    writeStream.on("finish", resolve);
    writeStream.on("error", reject);
  });

  return totalRows;
}

/**
 * Transform a row from PG format to a format suitable for HelixDB import.
 * Handles JSON serialization, type coercion, etc.
 */
function transformRow(
  row: Record<string, unknown>,
  columns: ColumnInfo[],
  tableKey: string,
  rowIndex: number,
  typeMappingOptions: TypeMappingOptions
): Record<string, unknown> {
  const result: Record<string, unknown> = {};

  for (const col of columns) {
    const value = row[col.name];

    if (value === null || value === undefined) {
      result[col.name] = null;
      continue;
    }

    const mapped = mapPgType(col.dataType, col.udtName, typeMappingOptions);

    try {
      if (mapped.needsSerialization) {
        result[col.name] = serializeComplex(value);
      } else if (mapped.isVector) {
        result[col.name] = normalizeVector(value);
      } else {
        result[col.name] = coerceScalar(value, mapped.helixType);
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      throw new Error(
        `Failed to coerce ${tableKey} row ${rowIndex} column ${col.name} (${col.dataType} -> ${mapped.helixType}): ${message}`
      );
    }
  }

  return result;
}

/**
 * Read exported JSON data from a file.
 */
export function readExportedData(
  filePath: string
): Record<string, unknown>[] {
  const content = fs.readFileSync(filePath, "utf-8");
  return JSON.parse(content);
}

export function makeTableKey(schema: string, table: string): string {
  return `${schema}.${table}`;
}

export function exportFileNameForTable(schema: string, table: string): string {
  const safeSchema = schema.replace(/[^A-Za-z0-9_-]+/g, "_");
  const safeTable = table.replace(/[^A-Za-z0-9_-]+/g, "_");
  return `${safeSchema}__${safeTable}.json`;
}

function serializeComplex(value: unknown): string {
  if (Buffer.isBuffer(value)) {
    return value.toString("base64");
  }

  if (typeof value === "string") {
    return value;
  }

  return JSON.stringify(value);
}

function normalizeVector(value: unknown): number[] {
  if (Array.isArray(value)) {
    return value.map((entry) => toFiniteNumber(entry));
  }

  if (typeof value === "string") {
    const parsed = JSON.parse(value);
    if (!Array.isArray(parsed)) {
      throw new Error("Expected vector column to be an array");
    }
    return parsed.map((entry) => toFiniteNumber(entry));
  }

  throw new Error("Unsupported vector value format");
}

function coerceScalar(value: unknown, helixType: string): unknown {
  if (helixType.startsWith("[")) {
    return normalizeArrayValue(value, helixType);
  }

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
      return toInteger(value, helixType);
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
      if (value instanceof Date) {
        return value.toISOString();
      }
      if (typeof value === "string") {
        return value;
      }
      return String(value);
    default:
      return value;
  }
}

function normalizeArrayValue(value: unknown, helixType: string): unknown {
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

  return arr.map((entry) => coerceScalar(entry, innerType));
}

const JS_MAX_SAFE_BIGINT = BigInt(Number.MAX_SAFE_INTEGER);
const JS_MIN_SAFE_BIGINT = BigInt(Number.MIN_SAFE_INTEGER);

function toInteger(value: unknown, helixType: string): number {
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error("Expected finite number for integer field");
    }
    if (!Number.isSafeInteger(value)) {
      throw new Error(
        `Unsafe integer for ${helixType}; use --bigint-mode string to avoid precision loss`
      );
    }
    return Math.trunc(value);
  }

  if (typeof value === "string") {
    if (!/^-?\d+$/.test(value.trim())) {
      throw new Error(`Invalid integer literal: ${value}`);
    }
    const bigint = BigInt(value);
    if (bigint > JS_MAX_SAFE_BIGINT || bigint < JS_MIN_SAFE_BIGINT) {
      throw new Error(
        `Integer ${value} exceeds JS safe range for ${helixType}; use --bigint-mode string`
      );
    }
    return Number(bigint);
  }

  throw new Error("Expected integer-compatible value");
}

function toFiniteNumber(value: unknown): number {
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error("Expected finite numeric value");
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

  throw new Error("Expected numeric value");
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
