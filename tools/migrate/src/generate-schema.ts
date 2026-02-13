/**
 * Generates HelixDB .hx schema files from introspected PostgreSQL schema.
 *
 * Mapping strategy:
 * - Each PG table -> N::NodeType (Node)
 * - Each PG foreign key -> E::EdgeType (Edge)
 * - Tables with pgvector columns -> V::VectorType (Vector) + N::NodeType for metadata
 * - PG indexes -> INDEX / UNIQUE INDEX field modifiers
 * - PG default values -> DEFAULT modifiers where applicable
 */

import { TableInfo, SchemaIntrospection } from "./introspect";
import { mapPgType, toPascalCase, toFieldName } from "./type-map";

export interface NodeSchema {
  name: string; // PascalCase node type name
  originalTable: string; // original PG table name
  fields: FieldSchema[];
  hasVectorColumn: boolean;
}

export interface FieldSchema {
  name: string;
  helixType: string;
  isIndexed: boolean;
  isUnique: boolean;
  hasDefault: boolean;
  defaultValue: string | null;
  needsSerialization: boolean;
  originalColumn: string;
  isPrimaryKey: boolean;
  isForeignKey: boolean; // excluded from Node fields, becomes an Edge
}

export interface EdgeSchema {
  name: string; // PascalCase edge type name
  fromNode: string;
  toNode: string;
  originalConstraint: string;
  originalColumn: string;
  isUnique: boolean; // if the FK column has a unique constraint
}

export interface VectorSchema {
  name: string; // PascalCase vector type name
  originalTable: string;
  vectorColumn: string;
  metadataFields: FieldSchema[];
}

export interface GeneratedSchema {
  nodes: NodeSchema[];
  edges: EdgeSchema[];
  vectors: VectorSchema[];
  schemaHx: string; // the complete .hx schema file content
}

/**
 * Generate a complete HelixDB schema from introspected PostgreSQL tables.
 */
export function generateSchema(
  introspection: SchemaIntrospection
): GeneratedSchema {
  const nodes: NodeSchema[] = [];
  const edges: EdgeSchema[] = [];
  const vectors: VectorSchema[] = [];

  // Track which tables are in scope for FK resolution
  const tableMap = new Map<string, TableInfo>();
  for (const table of introspection.tables) {
    tableMap.set(table.name, table);
  }

  // Build a set of FK column names per table for exclusion from Node fields
  const fkColumns = new Map<string, Set<string>>();
  for (const table of introspection.tables) {
    const fkCols = new Set<string>();
    for (const fk of table.foreignKeys) {
      fkCols.add(fk.columnName);
    }
    fkColumns.set(table.name, fkCols);
  }

  // Track indexed columns per table
  const indexedColumns = new Map<string, Map<string, { isUnique: boolean }>>();
  for (const table of introspection.tables) {
    const idxMap = new Map<string, { isUnique: boolean }>();
    for (const idx of table.indexes) {
      idxMap.set(idx.columnName, { isUnique: idx.isUnique });
    }
    indexedColumns.set(table.name, idxMap);
  }

  for (const table of introspection.tables) {
    // Skip Supabase internal tables
    if (isSupabaseInternal(table.name)) continue;

    const tableFkCols = fkColumns.get(table.name) ?? new Set();
    const tableIndexes = indexedColumns.get(table.name) ?? new Map();

    // Check if this table has vector columns
    const vectorColumns = table.columns.filter((col) => {
      const mapped = mapPgType(col.dataType, col.udtName);
      return mapped.isVector;
    });

    // Build field list (excluding PK and FK columns)
    const fields: FieldSchema[] = [];
    for (const col of table.columns) {
      // Skip primary key columns (HelixDB auto-generates IDs)
      if (col.isPrimaryKey) continue;

      const isFk = tableFkCols.has(col.name);
      const mapped = mapPgType(col.dataType, col.udtName);
      const idxInfo = tableIndexes.get(col.name);

      // Skip vector columns (they'll be handled as V:: types)
      if (mapped.isVector) continue;

      fields.push({
        name: toFieldName(col.name),
        helixType: mapped.helixType,
        isIndexed: idxInfo !== undefined,
        isUnique: idxInfo?.isUnique ?? false,
        hasDefault: col.columnDefault !== null && !col.columnDefault.startsWith("nextval"),
        defaultValue: mapDefault(col.columnDefault, mapped.helixType),
        needsSerialization: mapped.needsSerialization,
        originalColumn: col.name,
        isPrimaryKey: false,
        isForeignKey: isFk,
      });
    }

    const nodeName = toPascalCase(table.name);

    // Non-FK fields go into the Node definition
    const nodeFields = fields.filter((f) => !f.isForeignKey);

    nodes.push({
      name: nodeName,
      originalTable: table.name,
      fields: nodeFields,
      hasVectorColumn: vectorColumns.length > 0,
    });

    // Vector columns become separate V:: types
    for (const vecCol of vectorColumns) {
      const metaFields = fields.filter(
        (f) => !f.isForeignKey && f.originalColumn !== vecCol.name
      );
      vectors.push({
        name: `${nodeName}Embedding`,
        originalTable: table.name,
        vectorColumn: vecCol.name,
        metadataFields: metaFields,
      });
    }

    // Foreign keys become edges
    for (const fk of table.foreignKeys) {
      const targetTable = tableMap.get(fk.foreignTableName);
      if (!targetTable) continue;

      const fromNode = nodeName;
      const toNode = toPascalCase(fk.foreignTableName);

      // Generate edge name from relationship
      const edgeName = generateEdgeName(fromNode, toNode, fk.columnName);

      // Check if the FK column has a unique constraint
      const fkIdx = tableIndexes.get(fk.columnName);
      const isUniqueEdge = fkIdx?.isUnique ?? false;

      edges.push({
        name: edgeName,
        fromNode,
        toNode,
        originalConstraint: fk.constraintName,
        originalColumn: fk.columnName,
        isUnique: isUniqueEdge,
      });
    }
  }

  // Deduplicate edge names
  deduplicateEdges(edges);

  const schemaHx = renderSchemaHx(nodes, edges, vectors, introspection.enums);

  return { nodes, edges, vectors, schemaHx };
}

/**
 * Generate an edge type name from the relationship.
 * e.g., FK column "author_id" on Post -> User becomes "HasAuthor"
 */
function generateEdgeName(
  _fromNode: string,
  toNode: string,
  fkColumn: string
): string {
  // Strip common suffixes from FK column name
  let relationship = fkColumn
    .replace(/_id$/, "")
    .replace(/_uuid$/, "")
    .replace(/_fk$/, "");

  // If the column name matches the target table, use "Has" prefix
  const relPascal = toPascalCase(relationship);
  if (relPascal === toNode) {
    return `Has${toNode}`;
  }

  // Otherwise use the column-derived name
  return `Has${relPascal}`;
}

/**
 * Deduplicate edge names by appending a suffix when there are conflicts.
 */
function deduplicateEdges(edges: EdgeSchema[]): void {
  const nameCount = new Map<string, number>();
  for (const edge of edges) {
    nameCount.set(edge.name, (nameCount.get(edge.name) ?? 0) + 1);
  }

  const nameIndex = new Map<string, number>();
  for (const edge of edges) {
    if ((nameCount.get(edge.name) ?? 0) > 1) {
      const idx = (nameIndex.get(edge.name) ?? 0) + 1;
      nameIndex.set(edge.name, idx);
      if (idx > 1) {
        edge.name = `${edge.name}${idx}`;
      }
    }
  }
}

/**
 * Map a PostgreSQL default value to a HelixDB DEFAULT expression.
 */
function mapDefault(
  pgDefault: string | null,
  _helixType: string
): string | null {
  if (!pgDefault) return null;

  // NOW() / CURRENT_TIMESTAMP -> NOW
  if (
    pgDefault.includes("now()") ||
    pgDefault.includes("CURRENT_TIMESTAMP") ||
    pgDefault.includes("current_timestamp")
  ) {
    return "NOW";
  }

  // Skip nextval (serial/auto-increment)
  if (pgDefault.startsWith("nextval")) return null;

  // Skip gen_random_uuid (HelixDB generates IDs automatically)
  if (pgDefault.includes("gen_random_uuid") || pgDefault.includes("uuid_generate")) {
    return null;
  }

  return null; // HelixDB has limited DEFAULT support; skip complex defaults
}

/**
 * Render the complete .hx schema file.
 */
function renderSchemaHx(
  nodes: NodeSchema[],
  edges: EdgeSchema[],
  vectors: VectorSchema[],
  enums: Record<string, string[]>
): string {
  const lines: string[] = [];

  lines.push("// ============================================");
  lines.push("// HelixDB Schema - Auto-generated from Supabase");
  lines.push("// ============================================");
  lines.push("//");
  lines.push("// Review this schema and adjust as needed before running `helix push`.");
  lines.push("// See: https://docs.helix-db.com for HelixQL documentation.");
  lines.push("");

  // Emit enum comments (HelixDB doesn't have native enums, so we document them)
  if (Object.keys(enums).length > 0) {
    lines.push("// --- PostgreSQL Enums (mapped to String fields) ---");
    for (const [enumName, values] of Object.entries(enums)) {
      lines.push(`// Enum ${enumName}: ${values.map((v) => `"${v}"`).join(" | ")}`);
    }
    lines.push("");
  }

  // Emit Node types
  if (nodes.length > 0) {
    lines.push("// --- Nodes ---");
    lines.push("");
  }

  for (const node of nodes) {
    lines.push(`// Source table: ${node.originalTable}`);
    lines.push(`N::${node.name} {`);

    for (const field of node.fields) {
      let line = "    ";

      // Add INDEX / UNIQUE INDEX prefix
      if (field.isUnique) {
        line += "UNIQUE INDEX ";
      } else if (field.isIndexed) {
        line += "INDEX ";
      }

      line += `${field.name}: ${field.helixType}`;

      // Add DEFAULT if applicable
      if (field.hasDefault && field.defaultValue) {
        line += ` DEFAULT ${field.defaultValue}`;
      }

      line += ",";

      // Add comment for serialized fields
      if (field.needsSerialization) {
        line += " // JSON-serialized from PostgreSQL";
      }

      lines.push(line);
    }

    lines.push("}");
    lines.push("");
  }

  // Emit Edge types
  if (edges.length > 0) {
    lines.push("// --- Edges (from foreign key relationships) ---");
    lines.push("");
  }

  for (const edge of edges) {
    lines.push(`// Source: ${edge.originalConstraint} (${edge.originalColumn})`);
    if (edge.isUnique) {
      lines.push(`UNIQUE E::${edge.name} {`);
    } else {
      lines.push(`E::${edge.name} {`);
    }
    lines.push(`    From: ${edge.fromNode},`);
    lines.push(`    To: ${edge.toNode},`);
    lines.push("}");
    lines.push("");
  }

  // Emit Vector types
  if (vectors.length > 0) {
    lines.push("// --- Vectors (from pgvector columns) ---");
    lines.push("");
  }

  for (const vec of vectors) {
    lines.push(`// Source table: ${vec.originalTable}, column: ${vec.vectorColumn}`);
    lines.push(`V::${vec.name} {`);

    for (const field of vec.metadataFields) {
      let line = `    ${field.name}: ${field.helixType},`;
      if (field.needsSerialization) {
        line += " // JSON-serialized";
      }
      lines.push(line);
    }

    lines.push("}");
    lines.push("");
  }

  return lines.join("\n");
}

/**
 * Check if a table is a Supabase internal table that should be skipped.
 */
function isSupabaseInternal(tableName: string): boolean {
  const internalTables = new Set([
    "schema_migrations",
    "supabase_migrations",
    "supabase_functions",
    "_realtime_subscription",
    "buckets",
    "objects",
    "s3_multipart_uploads",
    "s3_multipart_uploads_parts",
    "migrations",
    "hooks",
    "mfa_factors",
    "mfa_challenges",
    "mfa_amr_claims",
    "sso_providers",
    "sso_domains",
    "saml_providers",
    "saml_relay_states",
    "flow_state",
    "one_time_tokens",
    "audit_log_entries",
    "refresh_tokens",
    "instances",
    "sessions",
    "identities",
  ]);
  return internalTables.has(tableName);
}
