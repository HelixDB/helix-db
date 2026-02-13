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
  columnName: string;
  foreignTableSchema: string;
  foreignTableName: string;
  foreignColumnName: string;
}

export interface IndexInfo {
  indexName: string;
  columnName: string;
  isUnique: boolean;
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

export interface SchemaIntrospection {
  tables: TableInfo[];
  enums: Record<string, string[]>; // enum_name -> values
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
    const [tables, enums] = await Promise.all([
      getTables(client, schemas),
      getEnums(client, schemas),
    ]);

    return { tables, enums };
  } finally {
    await client.end();
  }
}

async function getTables(
  client: Client,
  schemas: string[]
): Promise<TableInfo[]> {
  // Get all tables in the specified schemas
  const schemaList = schemas.map((s) => `'${s}'`).join(",");

  const tablesResult = await client.query(`
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema IN (${schemaList})
      AND table_type = 'BASE TABLE'
    ORDER BY table_schema, table_name
  `);

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
    JOIN information_schema.constraint_column_usage ccu
      ON tc.constraint_name = ccu.constraint_name
      AND tc.table_schema = ccu.table_schema
    WHERE tc.table_schema = $1
      AND tc.table_name = $2
      AND tc.constraint_type = 'FOREIGN KEY'
  `,
    [schema, tableName]
  );

  return result.rows.map((row) => ({
    constraintName: row.constraint_name,
    columnName: row.column_name,
    foreignTableSchema: row.foreign_table_schema,
    foreignTableName: row.foreign_table_name,
    foreignColumnName: row.foreign_column_name,
  }));
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
      ix.indisunique AS is_unique
    FROM pg_catalog.pg_class t
    JOIN pg_catalog.pg_index ix ON t.oid = ix.indrelid
    JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
    JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
    JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = $1
      AND t.relname = $2
      AND NOT ix.indisprimary
    ORDER BY i.relname
  `,
    [schema, tableName]
  );

  return result.rows.map((row) => ({
    indexName: row.index_name,
    columnName: row.column_name,
    isUnique: row.is_unique,
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
      `SELECT COUNT(*) AS count FROM "${schema}"."${tableName}"`
    );
    return parseInt(countResult.rows[0].count, 10);
  }

  return estimate;
}

async function getEnums(
  client: Client,
  schemas: string[]
): Promise<Record<string, string[]>> {
  const schemaList = schemas.map((s) => `'${s}'`).join(",");

  const result = await client.query(`
    SELECT
      t.typname AS enum_name,
      e.enumlabel AS enum_value
    FROM pg_catalog.pg_type t
    JOIN pg_catalog.pg_enum e ON t.oid = e.enumtypid
    JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname IN (${schemaList})
    ORDER BY t.typname, e.enumsortorder
  `);

  const enums: Record<string, string[]> = {};
  for (const row of result.rows) {
    if (!enums[row.enum_name]) {
      enums[row.enum_name] = [];
    }
    enums[row.enum_name].push(row.enum_value);
  }

  return enums;
}
