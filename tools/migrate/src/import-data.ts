/**
 * Imports exported Supabase data into a running HelixDB instance.
 *
 * Reads JSON export files and calls the generated Import* queries
 * via the HelixDB HTTP API to load data into the graph.
 *
 * Also maintains a mapping from old PG primary keys to new HelixDB IDs
 * so that edges (foreign key relationships) can be correctly linked.
 */

import * as fs from "fs";
import * as path from "path";
import { GeneratedSchema, NodeSchema, EdgeSchema } from "./generate-schema";
import { TableInfo } from "./introspect";

export interface ImportOptions {
  helixUrl: string; // e.g., http://localhost:6969
  exportDir: string; // directory with exported JSON files
  schema: GeneratedSchema;
  tables: TableInfo[]; // original PG tables for FK resolution
  concurrency: number; // parallel requests
  onProgress?: (table: string, imported: number, total: number) => void;
}

export interface ImportResult {
  nodesImported: number;
  edgesImported: number;
  vectorsImported: number;
  errors: ImportError[];
  idMap: Map<string, Map<string, string>>; // table -> oldPK -> newHelixID
}

export interface ImportError {
  table: string;
  row: number;
  error: string;
}

/**
 * Import all exported data into HelixDB.
 *
 * Order of operations:
 * 1. Import all Nodes (tables without FK dependencies first, then dependent tables)
 * 2. Import all Edges (using the ID mapping from step 1)
 * 3. Import all Vectors
 */
export async function importData(
  options: ImportOptions
): Promise<ImportResult> {
  const { helixUrl, exportDir, schema, tables, concurrency, onProgress } = options;

  const result: ImportResult = {
    nodesImported: 0,
    edgesImported: 0,
    vectorsImported: 0,
    errors: [],
    idMap: new Map(),
  };

  // Build table info map
  const tableInfoMap = new Map<string, TableInfo>();
  for (const t of tables) {
    tableInfoMap.set(t.name, t);
  }

  // Step 1: Import nodes (topologically sorted by FK dependencies)
  const sortedNodes = topologicalSort(schema.nodes, schema.edges);

  for (const node of sortedNodes) {
    const exportFile = path.join(exportDir, `${node.originalTable}.json`);
    if (!fs.existsSync(exportFile)) continue;

    const rows: Record<string, unknown>[] = JSON.parse(
      fs.readFileSync(exportFile, "utf-8")
    );

    const tableInfo = tableInfoMap.get(node.originalTable);
    const pkColumns = tableInfo?.primaryKeys ?? [];

    // Initialize ID map for this table
    const tableIdMap = new Map<string, string>();
    result.idMap.set(node.originalTable, tableIdMap);

    // Import rows in batches
    const batches = chunk(rows, concurrency);
    let imported = 0;

    for (const batch of batches) {
      const promises = batch.map(async (row, batchIdx) => {
        const rowIdx = imported + batchIdx;
        try {
          // Build the request body from node fields
          const body: Record<string, unknown> = {};
          for (const field of node.fields) {
            const value = row[field.originalColumn];
            if (value !== null && value !== undefined) {
              body[field.name] = value;
            }
          }

          // Call the Import query
          const response = await callHelix(
            helixUrl,
            `Import${node.name}`,
            body
          );

          // Store the ID mapping (old PK -> new Helix ID)
          if (response && pkColumns.length > 0) {
            const oldPk = pkColumns.map((pk) => String(row[pk])).join(":");
            const newId = extractId(response);
            if (newId) {
              tableIdMap.set(oldPk, newId);
            }
          }

          result.nodesImported++;
        } catch (err) {
          result.errors.push({
            table: node.originalTable,
            row: rowIdx,
            error: err instanceof Error ? err.message : String(err),
          });
        }
      });

      await Promise.all(promises);
      imported += batch.length;
      onProgress?.(node.originalTable, imported, rows.length);
    }
  }

  // Step 2: Import edges using the ID mapping
  for (const edge of schema.edges) {
    // Find the source table for this edge
    const sourceNode = schema.nodes.find((n) => n.name === edge.fromNode);
    if (!sourceNode) continue;

    const exportFile = path.join(exportDir, `${sourceNode.originalTable}.json`);
    if (!fs.existsSync(exportFile)) continue;

    const rows: Record<string, unknown>[] = JSON.parse(
      fs.readFileSync(exportFile, "utf-8")
    );

    const tableInfo = tableInfoMap.get(sourceNode.originalTable);
    const pkColumns = tableInfo?.primaryKeys ?? [];

    // Find the FK column and target table
    const fk = tableInfo?.foreignKeys.find(
      (fk) => fk.constraintName === edge.originalConstraint
    );
    if (!fk) continue;

    const sourceIdMap = result.idMap.get(sourceNode.originalTable);
    const targetIdMap = result.idMap.get(fk.foreignTableName);
    if (!sourceIdMap || !targetIdMap) continue;

    let imported = 0;
    const batches = chunk(rows, concurrency);

    for (const batch of batches) {
      const promises = batch.map(async (row, batchIdx) => {
        const rowIdx = imported + batchIdx;
        try {
          // Get the old FK value
          const oldFkValue = String(row[fk.columnName]);
          if (!oldFkValue || oldFkValue === "null" || oldFkValue === "undefined") return;

          // Look up the new Helix IDs
          const oldSourcePk = pkColumns.map((pk) => String(row[pk])).join(":");
          const fromId = sourceIdMap.get(oldSourcePk);
          const toId = targetIdMap.get(oldFkValue);

          if (!fromId || !toId) return;

          await callHelix(helixUrl, `Import${edge.name}`, {
            from_id: fromId,
            to_id: toId,
          });

          result.edgesImported++;
        } catch (err) {
          result.errors.push({
            table: `${edge.fromNode}->${edge.toNode}`,
            row: rowIdx,
            error: err instanceof Error ? err.message : String(err),
          });
        }
      });

      await Promise.all(promises);
      imported += batch.length;
      onProgress?.(`Edge: ${edge.name}`, imported, rows.length);
    }
  }

  // Step 3: Import vectors
  for (const vec of schema.vectors) {
    const exportFile = path.join(exportDir, `${vec.originalTable}.json`);
    if (!fs.existsSync(exportFile)) continue;

    const rows: Record<string, unknown>[] = JSON.parse(
      fs.readFileSync(exportFile, "utf-8")
    );

    let imported = 0;
    const batches = chunk(rows, concurrency);

    for (const batch of batches) {
      const promises = batch.map(async (row, batchIdx) => {
        const rowIdx = imported + batchIdx;
        try {
          const body: Record<string, unknown> = {};

          // Extract vector data
          const vectorData = row[vec.vectorColumn];
          if (!vectorData) return;
          body.vector = Array.isArray(vectorData)
            ? vectorData
            : JSON.parse(String(vectorData));

          // Extract metadata
          for (const field of vec.metadataFields) {
            const value = row[field.originalColumn];
            if (value !== null && value !== undefined) {
              body[field.name] = value;
            }
          }

          await callHelix(helixUrl, `Import${vec.name}`, body);
          result.vectorsImported++;
        } catch (err) {
          result.errors.push({
            table: vec.originalTable,
            row: rowIdx,
            error: err instanceof Error ? err.message : String(err),
          });
        }
      });

      await Promise.all(promises);
      imported += batch.length;
      onProgress?.(`Vector: ${vec.name}`, imported, rows.length);
    }
  }

  return result;
}

/**
 * Call a HelixDB query via its HTTP API.
 */
async function callHelix(
  baseUrl: string,
  queryName: string,
  body: Record<string, unknown>
): Promise<unknown> {
  const url = `${baseUrl}/${queryName}`;

  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`HelixDB API error (${response.status}): ${errorText}`);
  }

  return response.json();
}

/**
 * Extract the HelixDB ID from a query response.
 */
function extractId(response: unknown): string | null {
  if (!response || typeof response !== "object") return null;

  const obj = response as Record<string, unknown>;

  // HelixDB returns node data with an "id" field
  if (typeof obj.id === "string") return obj.id;

  // Check nested response formats
  if (Array.isArray(response) && response.length > 0) {
    const first = response[0] as Record<string, unknown>;
    if (typeof first?.id === "string") return first.id;
  }

  return null;
}

/**
 * Topologically sort nodes so that tables with no FK dependencies come first.
 */
function topologicalSort(
  nodes: NodeSchema[],
  edges: EdgeSchema[]
): NodeSchema[] {
  // Build adjacency: fromNode depends on toNode (toNode must be imported first)
  const deps = new Map<string, Set<string>>();
  for (const node of nodes) {
    deps.set(node.name, new Set());
  }
  for (const edge of edges) {
    if (edge.fromNode !== edge.toNode) {
      // fromNode depends on toNode
      deps.get(edge.fromNode)?.add(edge.toNode);
    }
  }

  const sorted: NodeSchema[] = [];
  const visited = new Set<string>();
  const visiting = new Set<string>();

  function visit(name: string): void {
    if (visited.has(name)) return;
    if (visiting.has(name)) return; // cycle, skip

    visiting.add(name);
    const nodeDeps = deps.get(name);
    if (nodeDeps) {
      for (const dep of nodeDeps) {
        visit(dep);
      }
    }
    visiting.delete(name);
    visited.add(name);

    const node = nodes.find((n) => n.name === name);
    if (node) sorted.push(node);
  }

  for (const node of nodes) {
    visit(node.name);
  }

  return sorted;
}

/**
 * Split an array into chunks of a given size.
 */
function chunk<T>(arr: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

/**
 * Generate an ID mapping file for reference.
 * Maps old Supabase PKs to new HelixDB IDs.
 */
export function saveIdMapping(
  idMap: Map<string, Map<string, string>>,
  outputPath: string
): void {
  const serializable: Record<string, Record<string, string>> = {};
  for (const [table, mapping] of idMap) {
    serializable[table] = Object.fromEntries(mapping);
  }
  fs.writeFileSync(outputPath, JSON.stringify(serializable, null, 2));
}
