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
import { mapPgType } from "./type-map";

export interface ExportOptions {
  connectionString: string;
  tables: TableInfo[];
  outputDir: string;
  batchSize: number; // rows per batch for large tables
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
  const { connectionString, tables, outputDir, batchSize } = options;

  // Ensure output directory exists
  fs.mkdirSync(outputDir, { recursive: true });

  const client = new Client({ connectionString });
  await client.connect();

  const results: ExportResult[] = [];

  try {
    for (const table of tables) {
      const filePath = path.join(outputDir, `${table.name}.json`);
      const rowCount = await exportTable(client, table, filePath, batchSize);

      results.push({
        table: table.name,
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
  batchSize: number
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

    const rows = result.rows.map((row) => transformRow(row, columns));
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

    for (const row of result.rows) {
      const transformed = transformRow(row, columns);
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
  columns: ColumnInfo[]
): Record<string, unknown> {
  const result: Record<string, unknown> = {};

  for (const col of columns) {
    const value = row[col.name];

    if (value === null || value === undefined) {
      result[col.name] = null;
      continue;
    }

    const mapped = mapPgType(col.dataType, col.udtName);

    if (mapped.needsSerialization) {
      // Serialize complex types to JSON string
      result[col.name] =
        typeof value === "string" ? value : JSON.stringify(value);
    } else if (mapped.isVector) {
      // Ensure vector is an array of numbers
      if (typeof value === "string") {
        // pgvector returns vectors as strings like "[1,2,3]"
        result[col.name] = JSON.parse(value);
      } else {
        result[col.name] = value;
      }
    } else {
      result[col.name] = value;
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
