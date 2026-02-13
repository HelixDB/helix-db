/**
 * Introspects a PostgreSQL (Supabase) database schema.
 *
 * Reads table definitions, columns, types, foreign keys, indexes,
 * and primary keys from the information_schema and pg_catalog.
 */

import { Client } from "pg";

export interface ColumnInfo {
  name: string;
  dataType: string;
  udtName: string;
  isNullable: boolean;
  columnDefault: string | null;
  characterMaxLength: number | null;
  ordinalPosition: number;
  isPrimaryKey: boolean;
}

export interface ForeignKey {
  constraintName: string;
  columnNames: string[];
  foreignTableSchema: string;
  foreignTableName: string;
  foreignColumnNames: string[];
}

export interface IndexInfo {
  indexName: string;
  columnName: string;
  isUnique: boolean;
  columnPosition: number;
}

export interface TableInfo {
  schema: string;
  name: string;
  columns: ColumnInfo[];
  primaryKeys: string[];
  foreignKeys: ForeignKey[];
  indexes: IndexInfo[];
  rowCount: number;
}

export interface UnsupportedFeature {
  kind: "view" | "trigger" | "function" | "policy";
  schema: string;
  name: string;
  detail?: string;
}

export interface SchemaIntrospection {
  tables: TableInfo[];
  enums: Record<string, string[]>; // enum_name -> values
  unsupportedFeatures: UnsupportedFeature[];
}

/**
 * Introspect a Supabase/PostgreSQL database and return full schema info.
 */
export async function introspectDatabase(
  connectionString: string,
  schemas: string[] = ["public"]
): Promise<SchemaIntrospection> {
  const client = new Client({ connectionString });
  await client.connect();

  try {
    const [tables, enums, unsupportedFeatures] = await Promise.all([
      getTables(client, schemas),
      getEnums(client, schemas),
      getUnsupportedFeatures(client, schemas),
    ]);

    return { tables, enums, unsupportedFeatures };
  } finally {
    await client.end();
  }
}

async function getTables(
  client: Client,
  schemas: string[]
): Promise<TableInfo[]> {
  const tablesResult = await client.query(
    `
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema = ANY($1::text[])
      AND table_type = 'BASE TABLE'
    ORDER BY table_schema, table_name
  `,
    [schemas]
  );

  const tables: TableInfo[] = [];

  for (const row of tablesResult.rows) {
    const schema = row.table_schema;
    const tableName = row.table_name;

    // Fetch columns, primary keys, foreign keys, indexes, and row count in parallel
    const [columns, primaryKeys, foreignKeys, indexes, rowCount] =
      await Promise.all([
        getColumns(client, schema, tableName),
        getPrimaryKeys(client, schema, tableName),
        getForeignKeys(client, schema, tableName),
        getIndexes(client, schema, tableName),
        getRowCount(client, schema, tableName),
      ]);

    // Mark primary key columns
    for (const col of columns) {
      col.isPrimaryKey = primaryKeys.includes(col.name);
    }

    tables.push({
      schema,
      name: tableName,
      columns,
      primaryKeys,
      foreignKeys,
      indexes,
      rowCount,
    });
  }

  return tables;
}

async function getColumns(
  client: Client,
  schema: string,
  tableName: string
): Promise<ColumnInfo[]> {
  const result = await client.query(
    `
    SELECT
      column_name,
      data_type,
      udt_name,
      is_nullable,
      column_default,
      character_maximum_length,
      ordinal_position
    FROM information_schema.columns
    WHERE table_schema = $1 AND table_name = $2
    ORDER BY ordinal_position
  `,
    [schema, tableName]
  );

  return result.rows.map((row) => ({
    name: row.column_name,
    dataType: row.data_type,
    udtName: row.udt_name,
    isNullable: row.is_nullable === "YES",
    columnDefault: row.column_default,
    characterMaxLength: row.character_maximum_length,
    ordinalPosition: row.ordinal_position,
    isPrimaryKey: false, // set later
  }));
}

async function getPrimaryKeys(
  client: Client,
  schema: string,
  tableName: string
): Promise<string[]> {
  const result = await client.query(
    `
    SELECT kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    WHERE tc.table_schema = $1
      AND tc.table_name = $2
      AND tc.constraint_type = 'PRIMARY KEY'
    ORDER BY kcu.ordinal_position
  `,
    [schema, tableName]
  );

  return result.rows.map((row) => row.column_name);
}

async function getForeignKeys(
  client: Client,
  schema: string,
  tableName: string
): Promise<ForeignKey[]> {
  const result = await client.query(
    `
    SELECT
      tc.constraint_name,
      kcu.column_name,
      ccu.table_schema AS foreign_table_schema,
      ccu.table_name AS foreign_table_name,
      ccu.column_name AS foreign_column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    JOIN information_schema.referential_constraints rc
      ON tc.constraint_name = rc.constraint_name
      AND tc.table_schema = rc.constraint_schema
    JOIN information_schema.key_column_usage ccu
      ON ccu.constraint_name = rc.unique_constraint_name
      AND ccu.constraint_schema = rc.unique_constraint_schema
      AND ccu.ordinal_position = kcu.position_in_unique_constraint
    WHERE tc.table_schema = $1
      AND tc.table_name = $2
      AND tc.constraint_type = 'FOREIGN KEY'
    ORDER BY tc.constraint_name, kcu.ordinal_position
  `,
    [schema, tableName]
  );

  const grouped = new Map<string, ForeignKey>();

  for (const row of result.rows) {
    const existing = grouped.get(row.constraint_name);
    if (existing) {
      existing.columnNames.push(row.column_name);
      existing.foreignColumnNames.push(row.foreign_column_name);
      continue;
    }

    grouped.set(row.constraint_name, {
      constraintName: row.constraint_name,
      columnNames: [row.column_name],
      foreignTableSchema: row.foreign_table_schema,
      foreignTableName: row.foreign_table_name,
      foreignColumnNames: [row.foreign_column_name],
    });
  }

  return Array.from(grouped.values());
}

async function getIndexes(
  client: Client,
  schema: string,
  tableName: string
): Promise<IndexInfo[]> {
  const result = await client.query(
    `
    SELECT
      i.relname AS index_name,
      a.attname AS column_name,
      ix.indisunique AS is_unique,
      key_ord.ordinality AS column_position
    FROM pg_catalog.pg_class t
    JOIN pg_catalog.pg_index ix ON t.oid = ix.indrelid
    JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
    JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS key_ord(attnum, ordinality) ON TRUE
    JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = key_ord.attnum
    JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = $1
      AND t.relname = $2
      AND NOT ix.indisprimary
    ORDER BY i.relname, key_ord.ordinality
  `,
    [schema, tableName]
  );

  return result.rows.map((row) => ({
    indexName: row.index_name,
    columnName: row.column_name,
    isUnique: row.is_unique,
    columnPosition: row.column_position,
  }));
}

async function getRowCount(
  client: Client,
  schema: string,
  tableName: string
): Promise<number> {
  // Use estimate for large tables, exact for small ones
  const result = await client.query(
    `
    SELECT reltuples::bigint AS estimate
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = $1 AND c.relname = $2
  `,
    [schema, tableName]
  );

  const estimate = parseInt(result.rows[0]?.estimate ?? "0", 10);

  // If estimate is small or negative (never analyzed), do exact count
  if (estimate < 10000) {
    const countResult = await client.query(
      `SELECT COUNT(*) AS count FROM ${quoteIdent(schema)}.${quoteIdent(tableName)}`
    );
    return parseInt(countResult.rows[0].count, 10);
  }

  return estimate;
}

async function getEnums(
  client: Client,
  schemas: string[]
): Promise<Record<string, string[]>> {
  const result = await client.query(
    `
    SELECT
      t.typname AS enum_name,
      e.enumlabel AS enum_value
    FROM pg_catalog.pg_type t
    JOIN pg_catalog.pg_enum e ON t.oid = e.enumtypid
    JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname = ANY($1::text[])
    ORDER BY t.typname, e.enumsortorder
  `,
    [schemas]
  );

  const enums: Record<string, string[]> = {};
  for (const row of result.rows) {
    if (!enums[row.enum_name]) {
      enums[row.enum_name] = [];
    }
    enums[row.enum_name].push(row.enum_value);
  }

  return enums;
}

function quoteIdent(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`;
}

async function getUnsupportedFeatures(
  client: Client,
  schemas: string[]
): Promise<UnsupportedFeature[]> {
  const [viewsResult, triggersResult, functionsResult, policiesResult] =
    await Promise.all([
      client.query(
        `
        SELECT table_schema AS schema_name, table_name
        FROM information_schema.views
        WHERE table_schema = ANY($1::text[])
        ORDER BY table_schema, table_name
      `,
        [schemas]
      ),
      client.query(
        `
        SELECT trigger_schema, trigger_name, event_object_table
        FROM information_schema.triggers
        WHERE trigger_schema = ANY($1::text[])
        ORDER BY trigger_schema, event_object_table, trigger_name
      `,
        [schemas]
      ),
      client.query(
        `
        SELECT routine_schema, routine_name
        FROM information_schema.routines
        WHERE routine_schema = ANY($1::text[])
          AND routine_type = 'FUNCTION'
        ORDER BY routine_schema, routine_name
      `,
        [schemas]
      ),
      client.query(
        `
        SELECT schemaname, tablename, policyname
        FROM pg_policies
        WHERE schemaname = ANY($1::text[])
        ORDER BY schemaname, tablename, policyname
      `,
        [schemas]
      ),
    ]);

  const unsupported: UnsupportedFeature[] = [];

  for (const row of viewsResult.rows) {
    unsupported.push({
      kind: "view",
      schema: row.schema_name,
      name: row.table_name,
    });
  }

  for (const row of triggersResult.rows) {
    unsupported.push({
      kind: "trigger",
      schema: row.trigger_schema,
      name: row.trigger_name,
      detail: `table ${row.event_object_table}`,
    });
  }

  for (const row of functionsResult.rows) {
    unsupported.push({
      kind: "function",
      schema: row.routine_schema,
      name: row.routine_name,
    });
  }

  for (const row of policiesResult.rows) {
    unsupported.push({
      kind: "policy",
      schema: row.schemaname,
      name: row.policyname,
      detail: `table ${row.tablename}`,
    });
  }

  return unsupported;
}
