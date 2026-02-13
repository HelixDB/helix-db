/**
 * Generates HelixDB .hx schema files from introspected PostgreSQL schema.
 *
 * Mapping strategy:
 * - Each PG table -> N::NodeType (Node)
 * - Each resolvable PG foreign key -> E::EdgeType (Edge)
 * - Tables with pgvector columns -> V::VectorType (Vector)
 * - PG indexes -> INDEX / UNIQUE INDEX field modifiers
 */

import { TableInfo, SchemaIntrospection, IndexInfo } from "./introspect";
import {
  mapPgType,
  toPascalCase,
  toFieldName,
  TypeMappingOptions,
} from "./type-map";

export interface NodeSchema {
  name: string;
  originalSchema: string;
  originalTable: string;
  tableKey: string;
  fields: FieldSchema[];
  hasVectorColumn: boolean;
}

export interface FieldSchema {
  name: string;
  helixType: string;
  isNullable: boolean;
  isIndexed: boolean;
  isUnique: boolean;
  hasDefault: boolean;
  defaultValue: string | null;
  needsSerialization: boolean;
  originalColumn: string;
  isPrimaryKey: boolean;
  isForeignKey: boolean;
}

export interface EdgeSchema {
  name: string;
  fromNode: string;
  toNode: string;
  fromTableKey: string;
  toTableKey: string;
  originalConstraint: string;
  originalColumns: string[];
  referencedColumns: string[];
  isUnique: boolean;
}

export interface VectorSchema {
  name: string;
  originalSchema: string;
  originalTable: string;
  tableKey: string;
  vectorColumn: string;
  metadataFields: FieldSchema[];
}

export interface GeneratedSchema {
  nodes: NodeSchema[];
  edges: EdgeSchema[];
  vectors: VectorSchema[];
  schemaHx: string;
}

export function generateSchema(
  introspection: SchemaIntrospection,
  typeOptions: TypeMappingOptions
): GeneratedSchema {
  const nodes: NodeSchema[] = [];
  const edges: EdgeSchema[] = [];
  const vectors: VectorSchema[] = [];

  const userTables = introspection.tables.filter(
    (table) => !isSupabaseInternal(table.name)
  );

  const nodeNameByTableKey = buildNodeNameMap(userTables);
  const usedEdgeNames = new Set<string>();
  const usedVectorNames = new Set<string>();

  for (const table of userTables) {
    const tableKey = makeTableKey(table.schema, table.name);
    const nodeName = nodeNameByTableKey.get(tableKey);
    if (!nodeName) {
      continue;
    }

    const indexMetadata = buildIndexMetadata(table.indexes);
    const fkColumnsMappedToEdges = new Set<string>();

    for (const fk of table.foreignKeys) {
      const targetKey = makeTableKey(fk.foreignTableSchema, fk.foreignTableName);
      const toNode = nodeNameByTableKey.get(targetKey);
      if (!toNode) {
        continue;
      }

      const baseEdgeName = generateEdgeName(nodeName, toNode, fk.columnNames);
      const edgeName = uniquifyName(baseEdgeName, usedEdgeNames);

      edges.push({
        name: edgeName,
        fromNode: nodeName,
        toNode,
        fromTableKey: tableKey,
        toTableKey: targetKey,
        originalConstraint: fk.constraintName,
        originalColumns: [...fk.columnNames],
        referencedColumns: [...fk.foreignColumnNames],
        isUnique: isForeignKeyUnique(table, indexMetadata.uniqueIndexGroups, fk.columnNames),
      });

      for (const fkColumn of fk.columnNames) {
        fkColumnsMappedToEdges.add(fkColumn);
      }
    }

    const fields: FieldSchema[] = [];
    const vectorColumns = table.columns.filter((col) => {
      const mapped = mapPgType(col.dataType, col.udtName, typeOptions);
      return mapped.isVector;
    });

    for (const col of table.columns) {
      if (col.isPrimaryKey) {
        continue;
      }

      const mapped = mapPgType(col.dataType, col.udtName, typeOptions);
      if (mapped.isVector) {
        continue;
      }

      const isMappedFk = fkColumnsMappedToEdges.has(col.name);
      const columnIndexInfo = indexMetadata.byColumn.get(col.name);

      fields.push({
        name: toFieldName(col.name),
        helixType: mapped.helixType,
        isNullable: col.isNullable,
        isIndexed: columnIndexInfo?.isIndexed ?? false,
        isUnique: columnIndexInfo?.isSingleColumnUnique ?? false,
        hasDefault:
          col.columnDefault !== null && !col.columnDefault.startsWith("nextval"),
        defaultValue: mapDefault(col.columnDefault),
        needsSerialization: mapped.needsSerialization,
        originalColumn: col.name,
        isPrimaryKey: false,
        isForeignKey: isMappedFk,
      });
    }

    nodes.push({
      name: nodeName,
      originalSchema: table.schema,
      originalTable: table.name,
      tableKey,
      fields,
      hasVectorColumn: vectorColumns.length > 0,
    });

    for (const vecColumn of vectorColumns) {
      const vectorNameBase = `${nodeName}${toPascalCase(vecColumn.name)}Embedding`;
      const vectorName = uniquifyName(vectorNameBase, usedVectorNames);

      vectors.push({
        name: vectorName,
        originalSchema: table.schema,
        originalTable: table.name,
        tableKey,
        vectorColumn: vecColumn.name,
        metadataFields: fields,
      });
    }
  }

  const schemaHx = renderSchemaHx(nodes, edges, vectors, introspection.enums);
  return { nodes, edges, vectors, schemaHx };
}

function makeTableKey(schema: string, table: string): string {
  return `${schema}.${table}`;
}

function buildNodeNameMap(tables: TableInfo[]): Map<string, string> {
  const tableKeyToName = new Map<string, string>();
  const usedNames = new Set<string>();

  for (const table of tables) {
    const key = makeTableKey(table.schema, table.name);
    const baseName = toPascalCase(table.name);

    let candidate = baseName;
    if (usedNames.has(candidate)) {
      candidate = `${toPascalCase(table.schema)}${baseName}`;
    }
    candidate = uniquifyName(candidate, usedNames);

    tableKeyToName.set(key, candidate);
  }

  return tableKeyToName;
}

function buildIndexMetadata(indexes: IndexInfo[]): {
  byColumn: Map<string, { isIndexed: boolean; isSingleColumnUnique: boolean }>;
  uniqueIndexGroups: string[][];
} {
  const byColumn = new Map<
    string,
    { isIndexed: boolean; isSingleColumnUnique: boolean }
  >();

  const grouped = new Map<string, IndexInfo[]>();
  for (const index of indexes) {
    const entries = grouped.get(index.indexName) ?? [];
    entries.push(index);
    grouped.set(index.indexName, entries);

    const existing = byColumn.get(index.columnName);
    if (existing) {
      existing.isIndexed = true;
    } else {
      byColumn.set(index.columnName, {
        isIndexed: true,
        isSingleColumnUnique: false,
      });
    }
  }

  const uniqueIndexGroups: string[][] = [];

  for (const group of grouped.values()) {
    const ordered = [...group].sort((a, b) => a.columnPosition - b.columnPosition);
    if (!ordered[0]?.isUnique) {
      continue;
    }

    const columns = ordered.map((entry) => entry.columnName);
    uniqueIndexGroups.push(columns);

    if (columns.length === 1) {
      const col = columns[0];
      const existing = byColumn.get(col);
      if (existing) {
        existing.isSingleColumnUnique = true;
      }
    }
  }

  return { byColumn, uniqueIndexGroups };
}

function isForeignKeyUnique(
  table: TableInfo,
  uniqueIndexGroups: string[][],
  fkColumns: string[]
): boolean {
  if (sameColumns(table.primaryKeys, fkColumns)) {
    return true;
  }

  for (const uniqueColumns of uniqueIndexGroups) {
    if (sameColumns(uniqueColumns, fkColumns)) {
      return true;
    }
  }

  return false;
}

function sameColumns(a: string[], b: string[]): boolean {
  if (a.length !== b.length) {
    return false;
  }
  return a.every((column, index) => column === b[index]);
}

function generateEdgeName(
  _fromNode: string,
  toNode: string,
  fkColumns: string[]
): string {
  if (fkColumns.length === 1) {
    const relationship = fkColumns[0]
      .replace(/_id$/, "")
      .replace(/_uuid$/, "")
      .replace(/_fk$/, "");

    const relPascal = toPascalCase(relationship);
    if (relPascal === toNode) {
      return `Has${toNode}`;
    }

    return `Has${relPascal}`;
  }

  const suffix = fkColumns
    .map((column) =>
      toPascalCase(column.replace(/_id$/, "").replace(/_uuid$/, "").replace(/_fk$/, ""))
    )
    .join("");

  return `Has${toNode}By${suffix}`;
}

function uniquifyName(baseName: string, usedNames: Set<string>): string {
  if (!usedNames.has(baseName)) {
    usedNames.add(baseName);
    return baseName;
  }

  let suffix = 2;
  while (usedNames.has(`${baseName}${suffix}`)) {
    suffix += 1;
  }

  const finalName = `${baseName}${suffix}`;
  usedNames.add(finalName);
  return finalName;
}

function mapDefault(pgDefault: string | null): string | null {
  if (!pgDefault) {
    return null;
  }

  if (
    pgDefault.includes("now()") ||
    pgDefault.includes("CURRENT_TIMESTAMP") ||
    pgDefault.includes("current_timestamp")
  ) {
    return "NOW";
  }

  if (pgDefault.startsWith("nextval")) {
    return null;
  }

  if (pgDefault.includes("gen_random_uuid") || pgDefault.includes("uuid_generate")) {
    return null;
  }

  return null;
}

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

  if (Object.keys(enums).length > 0) {
    lines.push("// --- PostgreSQL Enums (mapped to String fields) ---");
    for (const [enumName, values] of Object.entries(enums)) {
      lines.push(`// Enum ${enumName}: ${values.map((value) => `\"${value}\"`).join(" | ")}`);
    }
    lines.push("");
  }

  if (nodes.length > 0) {
    lines.push("// --- Nodes ---");
    lines.push("");
  }

  for (const node of nodes) {
    lines.push(`// Source table: ${node.originalSchema}.${node.originalTable}`);
    lines.push(`N::${node.name} {`);

    for (const field of node.fields) {
      let line = "    ";

      if (field.isUnique) {
        line += "UNIQUE INDEX ";
      } else if (field.isIndexed) {
        line += "INDEX ";
      }

      line += `${field.name}: ${field.helixType}`;

      if (field.hasDefault && field.defaultValue) {
        line += ` DEFAULT ${field.defaultValue}`;
      }

      line += ",";

      if (field.needsSerialization) {
        line += " // JSON-serialized from PostgreSQL";
      }

      lines.push(line);
    }

    lines.push("}");
    lines.push("");
  }

  if (edges.length > 0) {
    lines.push("// --- Edges (from foreign key relationships) ---");
    lines.push("");
  }

  for (const edge of edges) {
    lines.push(
      `// Source: ${edge.originalConstraint} (${edge.originalColumns.join(", ")} -> ${edge.referencedColumns.join(", ")})`
    );
    lines.push(edge.isUnique ? `E::${edge.name} UNIQUE {` : `E::${edge.name} {`);
    lines.push(`    From: ${edge.fromNode},`);
    lines.push(`    To: ${edge.toNode},`);
    lines.push("}");
    lines.push("");
  }

  if (vectors.length > 0) {
    lines.push("// --- Vectors (from pgvector columns) ---");
    lines.push("");
  }

  for (const vector of vectors) {
    lines.push(
      `// Source table: ${vector.originalSchema}.${vector.originalTable}, column: ${vector.vectorColumn}`
    );
    lines.push(`V::${vector.name} {`);

    for (const field of vector.metadataFields) {
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
