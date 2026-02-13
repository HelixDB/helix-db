#!/usr/bin/env node

/**
 * @helix-db/migrate - White-glove migration tool for Supabase -> HelixDB
 *
 * Usage:
 *   npx @helix-db/migrate supabase
 *   npx @helix-db/migrate supabase --connection-string "postgresql://..."
 *   npx @helix-db/migrate supabase --introspect-only
 *   npx @helix-db/migrate supabase --import-only --helix-url http://localhost:6969
 */

import { Command } from "commander";
import prompts from "prompts";
import chalk from "chalk";
import ora from "ora";
import * as fs from "fs";
import * as path from "path";
import { introspectDatabase, SchemaIntrospection, TableInfo } from "./introspect";
import { generateSchema, GeneratedSchema } from "./generate-schema";
import { generateQueries, GeneratedQueries } from "./generate-queries";
import { exportData } from "./export-data";
import { importData, saveIdMapping } from "./import-data";

const program = new Command();

program
  .name("helix-migrate")
  .description("White-glove migration tool for moving from Supabase to HelixDB")
  .version("0.1.0");

program
  .command("supabase")
  .description("Migrate a Supabase project to HelixDB")
  .option("-c, --connection-string <string>", "Supabase PostgreSQL connection string")
  .option("-o, --output <dir>", "Output directory for the generated HelixDB project", "./helix-project")
  .option("--schemas <schemas>", "Comma-separated list of PostgreSQL schemas to migrate", "public")
  .option("--introspect-only", "Only introspect and generate schema (no data migration)")
  .option("--import-only", "Only import data (assumes schema is already deployed)")
  .option("--helix-url <url>", "HelixDB instance URL for data import", "http://localhost:6969")
  .option("--batch-size <n>", "Rows per export batch", "5000")
  .option("--concurrency <n>", "Parallel import requests", "10")
  .option("--export-dir <dir>", "Directory for exported JSON data", "./helix-export")
  .action(migrateSupabase);

program.parse();

async function migrateSupabase(options: {
  connectionString?: string;
  output: string;
  schemas: string;
  introspectOnly?: boolean;
  importOnly?: boolean;
  helixUrl: string;
  batchSize: string;
  concurrency: string;
  exportDir: string;
}) {
  console.log("");
  console.log(chalk.bold("  Supabase → HelixDB Migration Tool"));
  console.log(chalk.gray("  ─────────────────────────────────"));
  console.log("");

  // Step 1: Get connection string
  let connectionString = options.connectionString;

  if (!connectionString && !options.importOnly) {
    const response = await prompts({
      type: "text",
      name: "connectionString",
      message: "Supabase PostgreSQL connection string:",
      hint: "Found in Supabase Dashboard → Settings → Database → Connection string (URI)",
      validate: (value: string) =>
        value.startsWith("postgresql://") || value.startsWith("postgres://")
          ? true
          : "Must be a PostgreSQL connection string (starts with postgresql:// or postgres://)",
    });

    if (!response.connectionString) {
      console.log(chalk.red("\nAborted."));
      process.exit(1);
    }

    connectionString = response.connectionString;
  }

  const schemas = options.schemas.split(",").map((s) => s.trim());

  // ─── Phase 1: Introspect ───────────────────────────────────────

  if (options.importOnly) {
    // Skip directly to import
    await runImport(options);
    return;
  }

  const spinner = ora("Connecting to Supabase database...").start();

  let introspection: SchemaIntrospection;
  try {
    introspection = await introspectDatabase(connectionString!, schemas);
    spinner.succeed(
      `Connected. Found ${introspection.tables.length} tables across schemas: ${schemas.join(", ")}`
    );
  } catch (err) {
    spinner.fail("Failed to connect to Supabase database");
    console.error(chalk.red(`\n  ${err instanceof Error ? err.message : err}`));
    console.log(chalk.gray("\n  Tip: Make sure your connection string is correct and the database is accessible."));
    console.log(chalk.gray("  Find it in: Supabase Dashboard → Settings → Database → Connection string (URI)"));
    process.exit(1);
  }

  // Show discovered schema summary
  console.log("");
  console.log(chalk.bold("  Discovered Schema:"));
  console.log("");

  const userTables = introspection.tables.filter(
    (t) => !isSupabaseInternal(t.name)
  );

  for (const table of userTables) {
    const fkCount = table.foreignKeys.length;
    const idxCount = table.indexes.length;
    const hasVector = table.columns.some((c) => c.udtName === "vector");

    console.log(
      `  ${chalk.cyan("┃")} ${chalk.bold(table.name)} ${chalk.gray(`(${table.rowCount} rows, ${table.columns.length} cols` +
        (fkCount > 0 ? `, ${fkCount} FK` : "") +
        (idxCount > 0 ? `, ${idxCount} idx` : "") +
        (hasVector ? ", vectors" : "") +
        ")")}`
    );
  }
  console.log("");

  if (Object.keys(introspection.enums).length > 0) {
    console.log(chalk.bold("  Enums:"));
    for (const [name, values] of Object.entries(introspection.enums)) {
      console.log(`  ${chalk.cyan("┃")} ${name}: ${values.join(", ")}`);
    }
    console.log("");
  }

  // ─── Phase 2: Generate Schema ──────────────────────────────────

  const schemaSpinner = ora("Generating HelixDB schema...").start();

  const generatedSchema = generateSchema(introspection);
  const generatedQueries = generateQueries(generatedSchema);

  schemaSpinner.succeed(
    `Generated ${generatedSchema.nodes.length} Nodes, ${generatedSchema.edges.length} Edges, ${generatedSchema.vectors.length} Vectors`
  );

  // ─── Phase 3: Write Project Files ──────────────────────────────

  const outputDir = path.resolve(options.output);
  const writeSpinner = ora(`Writing HelixDB project to ${outputDir}...`).start();

  try {
    writeHelixProject(
      outputDir,
      generatedSchema,
      generatedQueries,
      introspection
    );
    writeSpinner.succeed(`HelixDB project written to ${outputDir}`);
  } catch (err) {
    writeSpinner.fail("Failed to write project files");
    console.error(chalk.red(`\n  ${err instanceof Error ? err.message : err}`));
    process.exit(1);
  }

  // Show generated files
  console.log("");
  console.log(chalk.bold("  Generated Files:"));
  console.log(`  ${chalk.cyan("┃")} ${path.join(outputDir, "helix.toml")} ${chalk.gray("(project config)")}`);
  console.log(`  ${chalk.cyan("┃")} ${path.join(outputDir, "db", "schema.hx")} ${chalk.gray("(schema definitions)")}`);
  console.log(`  ${chalk.cyan("┃")} ${path.join(outputDir, "db", "queries.hx")} ${chalk.gray("(CRUD queries)")}`);
  console.log(`  ${chalk.cyan("┃")} ${path.join(outputDir, "db", "import.hx")} ${chalk.gray("(import queries)")}`);
  console.log(`  ${chalk.cyan("┃")} ${path.join(outputDir, "MIGRATION_GUIDE.md")} ${chalk.gray("(API mapping guide)")}`);
  console.log("");

  if (options.introspectOnly) {
    console.log(chalk.green("  Schema generation complete (--introspect-only mode)."));
    console.log("");
    console.log(chalk.bold("  Next steps:"));
    console.log(`  1. Review the generated schema in ${chalk.cyan(path.join(outputDir, "db", "schema.hx"))}`);
    console.log(`  2. Start HelixDB: ${chalk.cyan("cd " + outputDir + " && helix push dev")}`);
    console.log(`  3. Run the data import: ${chalk.cyan("helix-migrate supabase --import-only")}`);
    console.log("");
    return;
  }

  // ─── Phase 4: Export Data ──────────────────────────────────────

  const { proceed } = await prompts({
    type: "confirm",
    name: "proceed",
    message: "Export data from Supabase?",
    initial: true,
  });

  if (!proceed) {
    console.log(chalk.yellow("\n  Data export skipped. You can run it later with --import-only."));
    printNextSteps(outputDir);
    return;
  }

  const exportDir = path.resolve(options.exportDir);
  const exportSpinner = ora("Exporting data from Supabase...").start();

  try {
    const exportResults = await exportData({
      connectionString: connectionString!,
      tables: userTables,
      outputDir: exportDir,
      batchSize: parseInt(options.batchSize, 10),
    });

    const totalRows = exportResults.reduce((sum, r) => sum + r.rowCount, 0);
    exportSpinner.succeed(`Exported ${totalRows} rows from ${exportResults.length} tables to ${exportDir}`);

    for (const result of exportResults) {
      console.log(
        `  ${chalk.cyan("┃")} ${result.table}: ${result.rowCount} rows → ${result.filePath}`
      );
    }
    console.log("");
  } catch (err) {
    exportSpinner.fail("Failed to export data");
    console.error(chalk.red(`\n  ${err instanceof Error ? err.message : err}`));
    process.exit(1);
  }

  // ─── Phase 5: Import Data ─────────────────────────────────────

  const { doImport } = await prompts({
    type: "confirm",
    name: "doImport",
    message: `Import data into HelixDB at ${options.helixUrl}?`,
    initial: false,
  });

  if (!doImport) {
    console.log(chalk.yellow("\n  Data import skipped."));
    printNextSteps(outputDir);
    return;
  }

  await runImportWithProgress(
    options.helixUrl,
    exportDir,
    generatedSchema,
    userTables,
    parseInt(options.concurrency, 10)
  );

  // ─── Done ─────────────────────────────────────────────────────

  console.log("");
  console.log(chalk.green.bold("  Migration complete!"));
  printNextSteps(outputDir);
}

async function runImport(options: {
  helixUrl: string;
  exportDir: string;
  concurrency: string;
  output: string;
  schemas: string;
}) {
  // Read the generated schema from the project directory
  const schemaPath = path.join(options.output, "db", "schema.hx");
  if (!fs.existsSync(schemaPath)) {
    console.error(chalk.red(`\n  Schema file not found: ${schemaPath}`));
    console.error(chalk.gray("  Run without --import-only first to generate the schema."));
    process.exit(1);
  }

  console.log(chalk.yellow("  --import-only mode: skipping introspection and schema generation."));
  console.log(chalk.yellow("  Make sure HelixDB is running with the generated schema deployed."));
  console.log("");
}

async function runImportWithProgress(
  helixUrl: string,
  exportDir: string,
  schema: GeneratedSchema,
  tables: TableInfo[],
  concurrency: number
) {
  const importSpinner = ora("Importing data into HelixDB...").start();

  try {
    const importResult = await importData({
      helixUrl,
      exportDir,
      schema,
      tables,
      concurrency,
      onProgress: (table, imported, total) => {
        importSpinner.text = `Importing ${table}: ${imported}/${total}`;
      },
    });

    importSpinner.succeed(
      `Imported ${importResult.nodesImported} nodes, ${importResult.edgesImported} edges, ${importResult.vectorsImported} vectors`
    );

    if (importResult.errors.length > 0) {
      console.log(chalk.yellow(`\n  ${importResult.errors.length} errors during import:`));
      for (const err of importResult.errors.slice(0, 10)) {
        console.log(`  ${chalk.red("✗")} ${err.table} row ${err.row}: ${err.error}`);
      }
      if (importResult.errors.length > 10) {
        console.log(chalk.gray(`  ... and ${importResult.errors.length - 10} more`));
      }
    }

    // Save ID mapping for reference
    const mappingPath = path.join(exportDir, "id_mapping.json");
    saveIdMapping(importResult.idMap, mappingPath);
    console.log(chalk.gray(`\n  ID mapping saved to ${mappingPath}`));
  } catch (err) {
    importSpinner.fail("Failed to import data");
    console.error(chalk.red(`\n  ${err instanceof Error ? err.message : err}`));
    process.exit(1);
  }
}

function writeHelixProject(
  outputDir: string,
  schema: GeneratedSchema,
  queries: GeneratedQueries,
  introspection: SchemaIntrospection
) {
  const dbDir = path.join(outputDir, "db");
  fs.mkdirSync(dbDir, { recursive: true });

  // helix.toml
  const projectName = path.basename(outputDir);
  const helixToml = `[project]
name = "${projectName}"
queries = "db/"

[local.dev]
port = 6969
build_mode = "debug"
`;
  fs.writeFileSync(path.join(outputDir, "helix.toml"), helixToml);

  // db/schema.hx
  fs.writeFileSync(path.join(dbDir, "schema.hx"), schema.schemaHx);

  // db/queries.hx
  fs.writeFileSync(path.join(dbDir, "queries.hx"), queries.queriesHx);

  // db/import.hx
  fs.writeFileSync(path.join(dbDir, "import.hx"), queries.importQueriesHx);

  // MIGRATION_GUIDE.md
  const guide = generateMigrationGuide(schema, introspection);
  fs.writeFileSync(path.join(outputDir, "MIGRATION_GUIDE.md"), guide);
}

function generateMigrationGuide(
  schema: GeneratedSchema,
  introspection: SchemaIntrospection
): string {
  const lines: string[] = [];

  lines.push("# Supabase to HelixDB Migration Guide");
  lines.push("");
  lines.push("This guide maps your Supabase tables and operations to their HelixDB equivalents.");
  lines.push("");
  lines.push("## Schema Mapping");
  lines.push("");
  lines.push("| Supabase Table | HelixDB Type | Notes |");
  lines.push("|---|---|---|");

  for (const node of schema.nodes) {
    const table = introspection.tables.find((t) => t.name === node.originalTable);
    const notes = node.hasVectorColumn ? "Has vector embeddings" : "";
    lines.push(
      `| \`${node.originalTable}\` | \`N::${node.name}\` | ${notes} |`
    );
  }
  lines.push("");

  if (schema.edges.length > 0) {
    lines.push("## Relationship Mapping");
    lines.push("");
    lines.push("| Supabase FK | HelixDB Edge | From | To |");
    lines.push("|---|---|---|---|");
    for (const edge of schema.edges) {
      lines.push(
        `| \`${edge.originalColumn}\` | \`E::${edge.name}\` | \`${edge.fromNode}\` | \`${edge.toNode}\` |`
      );
    }
    lines.push("");
  }

  lines.push("## API Mapping");
  lines.push("");
  lines.push("### Supabase JS SDK → HelixDB TypeScript SDK");
  lines.push("");
  lines.push("```typescript");
  lines.push('import HelixDB from "helix-ts";');
  lines.push("const helix = new HelixDB();");
  lines.push("```");
  lines.push("");

  for (const node of schema.nodes) {
    const table = introspection.tables.find((t) => t.name === node.originalTable);
    if (!table) continue;

    lines.push(`### ${table.name}`);
    lines.push("");

    // INSERT
    lines.push("**Insert:**");
    lines.push("```typescript");
    lines.push("// Before (Supabase)");
    lines.push(`// const { data } = await supabase.from('${table.name}').insert({ ... });`);
    lines.push("");
    lines.push("// After (HelixDB)");
    lines.push(`const data = await helix.query("Add${node.name}", { ... });`);
    lines.push("```");
    lines.push("");

    // SELECT by ID
    lines.push("**Get by ID:**");
    lines.push("```typescript");
    lines.push("// Before (Supabase)");
    lines.push(`// const { data } = await supabase.from('${table.name}').select().eq('id', id);`);
    lines.push("");
    lines.push("// After (HelixDB)");
    lines.push(`const data = await helix.query("Get${node.name}", { id });`);
    lines.push("```");
    lines.push("");

    // DELETE
    lines.push("**Delete:**");
    lines.push("```typescript");
    lines.push("// Before (Supabase)");
    lines.push(`// const { data } = await supabase.from('${table.name}').delete().eq('id', id);`);
    lines.push("");
    lines.push("// After (HelixDB)");
    lines.push(`const data = await helix.query("Delete${node.name}", { id });`);
    lines.push("```");
    lines.push("");
  }

  if (schema.vectors.length > 0) {
    lines.push("## Vector Search");
    lines.push("");
    for (const vec of schema.vectors) {
      lines.push(`### ${vec.name}`);
      lines.push("```typescript");
      lines.push("// Before (Supabase with pgvector)");
      lines.push(`// const { data } = await supabase.rpc('match_${vec.originalTable}', { query_embedding: [...], match_count: 10 });`);
      lines.push("");
      lines.push("// After (HelixDB - built-in vector search with auto-embedding)");
      lines.push(`const results = await helix.query("Search${vec.name}", { query: "search text", limit: 10 });`);
      lines.push("```");
      lines.push("");
    }
  }

  lines.push("## Next Steps");
  lines.push("");
  lines.push("1. Review and customize the generated schema in `db/schema.hx`");
  lines.push("2. Review and extend the generated queries in `db/queries.hx`");
  lines.push("3. Start HelixDB locally: `helix push dev`");
  lines.push("4. Update your application code using the mappings above");
  lines.push("5. Delete `db/import.hx` after migration is complete");
  lines.push("");

  return lines.join("\n");
}

function printNextSteps(outputDir: string) {
  console.log("");
  console.log(chalk.bold("  Next steps:"));
  console.log("");
  console.log(`  1. Review the generated schema:`);
  console.log(chalk.cyan(`     ${path.join(outputDir, "db", "schema.hx")}`));
  console.log("");
  console.log(`  2. Start HelixDB:`);
  console.log(chalk.cyan(`     cd ${outputDir} && helix push dev`));
  console.log("");
  console.log(`  3. Update your app code using the migration guide:`);
  console.log(chalk.cyan(`     ${path.join(outputDir, "MIGRATION_GUIDE.md")}`));
  console.log("");
  console.log(`  4. Clean up import queries after migration:`);
  console.log(chalk.cyan(`     rm ${path.join(outputDir, "db", "import.hx")}`));
  console.log("");
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
