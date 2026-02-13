#!/usr/bin/env node

/**
 * @helix-db/migrate - White-glove migration tool for Supabase -> HelixDB
 */

import { Command } from "commander";
import prompts from "prompts";
import chalk from "chalk";
import ora from "ora";
import * as fs from "fs";
import * as path from "path";
import * as net from "net";
import { spawnSync } from "child_process";
import {
  introspectDatabase,
  SchemaIntrospection,
  TableInfo,
} from "./introspect";
import { generateSchema, GeneratedSchema } from "./generate-schema";
import { generateQueries, GeneratedQueries } from "./generate-queries";
import { exportData } from "./export-data";
import { importData, saveIdMapping } from "./import-data";
import {
  resolveTypeMappingOptions,
  TypeMappingOptions,
} from "./type-map";

interface MigrationManifest {
  version: number;
  generatedAt: string;
  schema: GeneratedSchema;
  tables: TableInfo[];
  typeMappingOptions: TypeMappingOptions;
  unsupportedFeatures: SchemaIntrospection["unsupportedFeatures"];
}

const MANIFEST_RELATIVE_PATH = path.join(".helix-migrate", "manifest.json");

const program = new Command();

program
  .name("helix-migrate")
  .description("White-glove migration tool for moving from Supabase to HelixDB")
  .version("0.1.0");

program
  .command("supabase")
  .description("Migrate a Supabase project to HelixDB")
  .option(
    "-c, --connection-string <string>",
    "Supabase PostgreSQL connection string"
  )
  .option(
    "-o, --output <dir>",
    "Output directory for the generated HelixDB project",
    "./helix-project"
  )
  .option(
    "--schemas <schemas>",
    "Comma-separated list of PostgreSQL schemas to migrate",
    "public"
  )
  .option(
    "--introspect-only",
    "Only introspect and generate schema (no data migration)"
  )
  .option(
    "--import-only",
    "Only import data (assumes schema is already deployed)"
  )
  .option(
    "--helix-url <url>",
    "HelixDB instance URL for data import",
    "http://localhost:6969"
  )
  .option("--helix-api-key <key>", "Helix API key (defaults to HELIX_API_KEY from env/.env)")
  .option("--batch-size <n>", "Rows per export batch", "5000")
  .option("--concurrency <n>", "Parallel import requests", "10")
  .option(
    "--bigint-mode <mode>",
    "How to map PostgreSQL bigint/int8 values: string (safe) or i64",
    "string"
  )
  .option(
    "--include-tables <tables>",
    "Comma-separated table allowlist (schema.table or table)"
  )
  .option(
    "--exclude-tables <tables>",
    "Comma-separated table blocklist (schema.table or table)"
  )
  .option("--instance <name>", "Helix instance name for deployment", "dev")
  .option("--skip-deploy", "Skip `helix push` and use existing Helix instance")
  .option("--reset-instance", "Delete Helix instance before deploy (fresh run)")
  .option("-y, --yes", "Auto-confirm destructive operations")
  .option("--non-interactive", "Run without prompts")
  .option("--no-strict", "Allow partial import with warnings/errors")
  .option("--skip-helix-check", "Skip running `helix check` on generated project")
  .option(
    "--export-dir <dir>",
    "Directory for exported JSON data",
    "./helix-export"
  )
  .action(migrateSupabase);

program.parse();

async function migrateSupabase(options: {
  connectionString?: string;
  output: string;
  schemas: string;
  introspectOnly?: boolean;
  importOnly?: boolean;
  helixUrl: string;
  helixApiKey?: string;
  batchSize: string;
  concurrency: string;
  bigintMode: string;
  includeTables?: string;
  excludeTables?: string;
  instance: string;
  skipDeploy?: boolean;
  resetInstance?: boolean;
  yes?: boolean;
  nonInteractive?: boolean;
  strict?: boolean;
  skipHelixCheck?: boolean;
  exportDir: string;
}) {
  console.log("");
  console.log(chalk.bold("  Supabase -> HelixDB Migration Tool"));
  console.log(chalk.gray("  ---------------------------------"));
  console.log("");

  const batchSize = parsePositiveInteger(options.batchSize, "--batch-size");
  const concurrency = parsePositiveInteger(options.concurrency, "--concurrency");
  const typeMappingOptions = parseTypeMappingOptions(options.bigintMode);
  const strictMode = options.strict !== false;
  const instanceName = options.instance?.trim() || "dev";
  const shouldSkipDeploy = options.skipDeploy === true;
  const shouldResetInstance = options.resetInstance === true;
  const autoConfirm = options.yes === true;
  const schemas = options.schemas
    .split(",")
    .map((schema) => schema.trim())
    .filter(Boolean);
  const outputDir = path.resolve(options.output);

  if (shouldSkipDeploy && shouldResetInstance) {
    console.error(chalk.red("\n  --reset-instance cannot be used with --skip-deploy."));
    process.exit(1);
  }

  if (shouldResetInstance && options.nonInteractive && !autoConfirm) {
    console.error(
      chalk.red("\n  --reset-instance in --non-interactive mode requires --yes.")
    );
    process.exit(1);
  }

  let connectionString = options.connectionString;

  if (!connectionString && !options.importOnly) {
    if (options.nonInteractive) {
      console.error(
        chalk.red(
          "\n  Missing --connection-string in --non-interactive mode."
        )
      );
      process.exit(1);
    }

    const response = await prompts({
      type: "text",
      name: "connectionString",
      message: "Supabase PostgreSQL connection string:",
      hint: "Found in Supabase Dashboard -> Settings -> Database -> Connection string (URI)",
      validate: (value: string) =>
        value.startsWith("postgresql://") || value.startsWith("postgres://")
          ? true
          : "Must start with postgresql:// or postgres://",
    });

    if (!response.connectionString) {
      console.log(chalk.red("\nAborted."));
      process.exit(1);
    }

    connectionString = response.connectionString;
  }

  if (connectionString) {
    connectionString = normalizeSupabaseConnectionString(connectionString);
  }

  let effectiveHelixUrl = options.helixUrl;
  let helixPort = parseHelixPort(effectiveHelixUrl);

  if (!options.importOnly && !shouldSkipDeploy && isLocalHelixUrl(effectiveHelixUrl)) {
    const availablePort = await findAvailableLocalPort(helixPort);
    if (availablePort !== helixPort) {
      effectiveHelixUrl = withUpdatedUrlPort(effectiveHelixUrl, availablePort);
      helixPort = availablePort;
      console.log(
        chalk.yellow(
          `  Port ${parseHelixPort(options.helixUrl)} is in use; deploying to ${effectiveHelixUrl} instead.`
        )
      );
      console.log("");
    }
  }

  const helixApiKey = resolveHelixApiKey(options.helixApiKey, outputDir);
  const apiKeyRequired = requiresHelixApiKey(effectiveHelixUrl, outputDir, instanceName);
  if (apiKeyRequired && !helixApiKey) {
    console.error(chalk.red("\n  HELIX_API_KEY is required for cloud/prod targets."));
    console.error(
      chalk.gray(
        "  Set HELIX_API_KEY in .env or pass --helix-api-key <key> and rerun."
      )
    );
    process.exit(1);
  }

  if (options.importOnly) {
    await runImport({
      helixUrl: options.helixUrl,
      exportDir: path.resolve(options.exportDir),
      output: outputDir,
      concurrency,
      strict: strictMode,
      helixApiKey,
    });
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
    process.exit(1);
    return;
  }

  const includeFilters = parseTableFilters(options.includeTables);
  const excludeFilters = parseTableFilters(options.excludeTables);

  const userTables = introspection.tables
    .filter((table) => !isSupabaseInternal(table.name))
    .filter((table) => matchesTableFilter(table, includeFilters, excludeFilters));

  if (userTables.length === 0) {
    console.error(chalk.red("\n  No tables selected for migration."));
    console.error(
      chalk.gray(
        "  Check --schemas / --include-tables / --exclude-tables filters and try again."
      )
    );
    process.exit(1);
  }

  console.log("");
  console.log(chalk.bold("  Discovered Schema:"));
  console.log("");
  for (const table of userTables) {
    const fkCount = table.foreignKeys.length;
    const idxCount = table.indexes.length;
    const hasVector = table.columns.some((column) => column.udtName === "vector");

    console.log(
      `  ${chalk.cyan("|")} ${chalk.bold(`${table.schema}.${table.name}`)} ${chalk.gray(
        `(${table.rowCount} rows, ${table.columns.length} cols${
          fkCount > 0 ? `, ${fkCount} FK` : ""
        }${idxCount > 0 ? `, ${idxCount} idx` : ""}${
          hasVector ? ", vectors" : ""
        })`
      )}`
    );
  }
  console.log("");

  if (introspection.unsupportedFeatures.length > 0) {
    const byKind = new Map<string, number>();
    for (const feature of introspection.unsupportedFeatures) {
      byKind.set(feature.kind, (byKind.get(feature.kind) ?? 0) + 1);
    }

    console.log(chalk.yellow("  Unsupported objects detected (manual migration required):"));
    for (const [kind, count] of byKind.entries()) {
      console.log(`  ${chalk.cyan("|")} ${kind}: ${count}`);
    }

    for (const feature of introspection.unsupportedFeatures.slice(0, 10)) {
      const detail = feature.detail ? ` (${feature.detail})` : "";
      console.log(
        `  ${chalk.cyan("|")} ${feature.schema}.${feature.name} [${feature.kind}]${detail}`
      );
    }

    if (introspection.unsupportedFeatures.length > 10) {
      console.log(
        chalk.gray(
          `  ... and ${introspection.unsupportedFeatures.length - 10} more unsupported objects`
        )
      );
    }

    console.log("");
  }

  const filteredIntrospection: SchemaIntrospection = {
    ...introspection,
    tables: userTables,
  };

  const schemaSpinner = ora("Generating HelixDB schema...").start();
  const generatedSchema = generateSchema(filteredIntrospection, typeMappingOptions);
  const generatedQueries = generateQueries(generatedSchema);
  schemaSpinner.succeed(
    `Generated ${generatedSchema.nodes.length} Nodes, ${generatedSchema.edges.length} Edges, ${generatedSchema.vectors.length} Vectors`
  );

  const writeSpinner = ora(`Writing HelixDB project to ${outputDir}...`).start();

  try {
    writeHelixProject(
      outputDir,
      generatedSchema,
      generatedQueries,
      filteredIntrospection,
      userTables,
      typeMappingOptions,
      helixPort
    );
    writeSpinner.succeed(`HelixDB project written to ${outputDir}`);
  } catch (err) {
    writeSpinner.fail("Failed to write project files");
    console.error(chalk.red(`\n  ${err instanceof Error ? err.message : err}`));
    process.exit(1);
  }

  if (!options.skipHelixCheck) {
    await runHelixCheck(outputDir, strictMode);
  }

  console.log("");
  console.log(chalk.bold("  Generated Files:"));
  console.log(
    `  ${chalk.cyan("|")} ${path.join(outputDir, "helix.toml")} ${chalk.gray("(project config)")}`
  );
  console.log(
    `  ${chalk.cyan("|")} ${path.join(outputDir, "db", "schema.hx")} ${chalk.gray("(schema definitions)")}`
  );
  console.log(
    `  ${chalk.cyan("|")} ${path.join(outputDir, "db", "queries.hx")} ${chalk.gray("(CRUD queries)")}`
  );
  console.log(
    `  ${chalk.cyan("|")} ${path.join(outputDir, "db", "import.hx")} ${chalk.gray("(import queries)")}`
  );
  console.log(
    `  ${chalk.cyan("|")} ${path.join(outputDir, "MIGRATION_GUIDE.md")} ${chalk.gray("(API mapping guide)")}`
  );
  console.log(
    `  ${chalk.cyan("|")} ${path.join(outputDir, MANIFEST_RELATIVE_PATH)} ${chalk.gray("(import-only manifest)")}`
  );
  console.log("");

  if (options.introspectOnly) {
    console.log(chalk.green("  Schema generation complete (--introspect-only mode)."));
    console.log("");
    console.log(chalk.bold("  Next steps:"));
    console.log(
      `  1. Review schema: ${chalk.cyan(path.join(outputDir, "db", "schema.hx"))}`
    );
    console.log(
      `  2. Deploy schema: ${chalk.cyan(`cd ${outputDir} && helix push dev`)}`
    );
    console.log(
      `  3. Import later: ${chalk.cyan(
        `helix-migrate supabase --import-only --output ${outputDir} --export-dir ${path.resolve(
          options.exportDir
        )} --helix-url ${effectiveHelixUrl}`
      )}`
    );
    console.log("");
    return;
  }

  if (!shouldSkipDeploy) {
    try {
      await runHelixDeploy({
        outputDir,
        instanceName,
        resetInstance: shouldResetInstance,
        autoConfirm,
        nonInteractive: options.nonInteractive === true,
      });
    } catch (err) {
      console.error(chalk.red(`\n  ${err instanceof Error ? err.message : err}`));
      process.exit(1);
    }
  } else {
    console.log(chalk.yellow("  --skip-deploy enabled: using existing Helix instance."));
    console.log("");
  }

  const exportDir = path.resolve(options.exportDir);
  const exportSpinner = ora("Exporting data from Supabase...").start();

  try {
    const exportResults = await exportData({
      connectionString: connectionString!,
      tables: userTables,
      outputDir: exportDir,
      batchSize,
      typeMappingOptions,
    });

    const totalRows = exportResults.reduce((sum, current) => sum + current.rowCount, 0);
    exportSpinner.succeed(
      `Exported ${totalRows} rows from ${exportResults.length} tables to ${exportDir}`
    );

    for (const exportResult of exportResults) {
      console.log(
        `  ${chalk.cyan("|")} ${exportResult.table}: ${exportResult.rowCount} rows -> ${exportResult.filePath}`
      );
    }
    console.log("");
  } catch (err) {
    exportSpinner.fail("Failed to export data");
    console.error(chalk.red(`\n  ${err instanceof Error ? err.message : err}`));
    process.exit(1);
  }

  await runImportWithProgress(
    effectiveHelixUrl,
    exportDir,
    generatedSchema,
    userTables,
    concurrency,
    introspection.unsupportedFeatures,
    strictMode,
    helixApiKey
  );

  console.log("");
  console.log(chalk.green.bold("  Migration complete!"));
  printNextSteps(outputDir);
}

async function runImport(options: {
  helixUrl: string;
  exportDir: string;
  output: string;
  concurrency: number;
  strict: boolean;
  helixApiKey?: string;
}) {
  const manifestPath = path.join(options.output, MANIFEST_RELATIVE_PATH);
  if (!fs.existsSync(manifestPath)) {
    console.error(chalk.red(`\n  Migration manifest not found: ${manifestPath}`));
    console.error(
      chalk.gray("  Run without --import-only first so migration artifacts are generated.")
    );
    process.exit(1);
  }

  let manifest: MigrationManifest;
  try {
    const parsed = JSON.parse(fs.readFileSync(manifestPath, "utf-8")) as Partial<MigrationManifest>;
    if (!parsed.schema || !parsed.tables) {
      throw new Error("manifest is missing required schema/tables content");
    }

    manifest = {
      version: parsed.version ?? 1,
      generatedAt: parsed.generatedAt ?? new Date(0).toISOString(),
      schema: parsed.schema,
      tables: parsed.tables,
      typeMappingOptions: parsed.typeMappingOptions ?? resolveTypeMappingOptions(),
      unsupportedFeatures: parsed.unsupportedFeatures ?? [],
    };
  } catch (err) {
    console.error(
      chalk.red(`\n  Failed to read migration manifest: ${err instanceof Error ? err.message : err}`)
    );
    process.exit(1);
    return;
  }

  console.log(
    chalk.yellow("  --import-only mode: skipping introspection/schema generation and using manifest.")
  );
  console.log(chalk.yellow("  Make sure HelixDB is running with the generated schema deployed."));
  console.log("");

  await runImportWithProgress(
    options.helixUrl,
    options.exportDir,
    manifest.schema,
    manifest.tables,
    options.concurrency,
    manifest.unsupportedFeatures,
    options.strict,
    options.helixApiKey
  );

  console.log("");
  console.log(chalk.green.bold("  Import complete!"));
  printNextSteps(options.output);
}

async function runImportWithProgress(
  helixUrl: string,
  exportDir: string,
  schema: GeneratedSchema,
  tables: TableInfo[],
  concurrency: number,
  unsupportedFeatures: SchemaIntrospection["unsupportedFeatures"],
  strictMode: boolean,
  helixApiKey?: string
) {
  const importSpinner = ora("Importing data into HelixDB...").start();

  try {
    const importResult = await importData({
      helixUrl,
      helixApiKey,
      exportDir,
      schema,
      tables,
      concurrency,
      onProgress: (table, imported, total) => {
        importSpinner.text = `Importing ${table}: ${imported}/${total}`;
      },
    });

    if (importResult.errors.length > 0) {
      console.log(chalk.yellow(`\n  ${importResult.errors.length} errors during import:`));
      for (const err of importResult.errors.slice(0, 10)) {
        const rowLabel = err.row >= 0 ? `row ${err.row}` : "schema";
        console.log(`  ${chalk.red("x")} ${err.table} ${rowLabel}: ${err.error}`);
      }
      if (importResult.errors.length > 10) {
        console.log(chalk.gray(`  ... and ${importResult.errors.length - 10} more`));
      }
    }

    if (importResult.warnings.length > 0) {
      console.log(chalk.yellow(`\n  ${importResult.warnings.length} warnings during import:`));
      for (const warning of importResult.warnings.slice(0, 10)) {
        console.log(`  ${chalk.yellow("!")} ${warning}`);
      }
      if (importResult.warnings.length > 10) {
        console.log(chalk.gray(`  ... and ${importResult.warnings.length - 10} more`));
      }
    }

    const mappingPath = path.join(exportDir, "id_mapping.json");
    saveIdMapping(importResult.idMap, mappingPath);
    console.log(chalk.gray(`\n  ID mapping saved to ${mappingPath}`));

    const reportPath = path.join(exportDir, "migration-report.json");
    fs.writeFileSync(
      reportPath,
      JSON.stringify(
        {
          generatedAt: new Date().toISOString(),
          helixUrl,
          exportDir,
          nodesImported: importResult.nodesImported,
          edgesImported: importResult.edgesImported,
          vectorsImported: importResult.vectorsImported,
          nodeStats: importResult.nodeStats,
          edgeStats: importResult.edgeStats,
          vectorStats: importResult.vectorStats,
          warnings: importResult.warnings,
          errorCount: importResult.errors.length,
          errors: importResult.errors,
          unsupportedFeatures,
        },
        null,
        2
      )
    );
    console.log(chalk.gray(`  Migration report saved to ${reportPath}`));

    const unresolvedEdges = Object.values(importResult.edgeStats).reduce(
      (sum, stats) => sum + stats.unresolved,
      0
    );

    if (strictMode) {
      const strictFailures: string[] = [];
      if (importResult.errors.length > 0) {
        strictFailures.push(`${importResult.errors.length} import errors`);
      }
      if (importResult.warnings.length > 0) {
        strictFailures.push(`${importResult.warnings.length} warnings`);
      }
      if (unresolvedEdges > 0) {
        strictFailures.push(`${unresolvedEdges} unresolved edge mappings`);
      }

      if (strictFailures.length > 0) {
        throw new Error(
          `Strict mode failed due to ${strictFailures.join(", ")}. See ${reportPath}. Re-run with --no-strict to allow partial migration.`
        );
      }
    } else if (unresolvedEdges > 0) {
      console.log(
        chalk.yellow(
          `\n  ${unresolvedEdges} unresolved edge mappings were recorded (non-strict mode).`
        )
      );
    }

    importSpinner.succeed(
      `Imported ${importResult.nodesImported} nodes, ${importResult.edgesImported} edges, ${importResult.vectorsImported} vectors`
    );
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
  introspection: SchemaIntrospection,
  userTables: TableInfo[],
  typeMappingOptions: TypeMappingOptions,
  helixPort: number
) {
  const dbDir = path.join(outputDir, "db");
  fs.mkdirSync(dbDir, { recursive: true });

  const projectName = path.basename(outputDir);
  const helixToml = `[project]
name = "${projectName}"
queries = "db/"

[local.dev]
port = ${helixPort}
build_mode = "dev"
`;
  fs.writeFileSync(path.join(outputDir, "helix.toml"), helixToml);

  fs.writeFileSync(path.join(dbDir, "schema.hx"), schema.schemaHx);
  fs.writeFileSync(path.join(dbDir, "queries.hx"), queries.queriesHx);
  fs.writeFileSync(path.join(dbDir, "import.hx"), queries.importQueriesHx);

  const guide = generateMigrationGuide(schema, introspection);
  fs.writeFileSync(path.join(outputDir, "MIGRATION_GUIDE.md"), guide);

  const manifestDir = path.join(outputDir, ".helix-migrate");
  fs.mkdirSync(manifestDir, { recursive: true });

  const manifest: MigrationManifest = {
    version: 1,
    generatedAt: new Date().toISOString(),
    schema,
    tables: userTables,
    typeMappingOptions,
    unsupportedFeatures: introspection.unsupportedFeatures,
  };

  fs.writeFileSync(
    path.join(outputDir, MANIFEST_RELATIVE_PATH),
    JSON.stringify(manifest, null, 2)
  );
}

function generateMigrationGuide(
  schema: GeneratedSchema,
  introspection: SchemaIntrospection
): string {
  const lines: string[] = [];

  lines.push("# Supabase to HelixDB Migration Guide");
  lines.push("");
  lines.push(
    "This guide maps your Supabase tables and operations to their HelixDB equivalents."
  );
  lines.push("");
  lines.push("## Schema Mapping");
  lines.push("");
  lines.push("| Supabase Table | HelixDB Type | Notes |");
  lines.push("|---|---|---|");

  for (const node of schema.nodes) {
    const table = introspection.tables.find(
      (t) => t.schema === node.originalSchema && t.name === node.originalTable
    );
    const notes = node.hasVectorColumn ? "Has vector embeddings" : "";
    if (!table) {
      continue;
    }
    lines.push(
      `| \`${table.schema}.${table.name}\` | \`N::${node.name}\` | ${notes} |`
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
        `| \`${edge.originalColumns.join(", ")}\` | \`E::${edge.name}\` | \`${edge.fromNode}\` | \`${edge.toNode}\` |`
      );
    }
    lines.push("");
  }

  lines.push("## API Mapping");
  lines.push("");
  lines.push("### Supabase JS SDK -> HelixDB TypeScript SDK");
  lines.push("");
  lines.push("```typescript");
  lines.push('import HelixDB from "helix-ts";');
  lines.push("const client = new HelixDB();");
  lines.push("```");
  lines.push("");

  for (const node of schema.nodes) {
    const table = introspection.tables.find(
      (t) => t.schema === node.originalSchema && t.name === node.originalTable
    );
    if (!table) {
      continue;
    }

    lines.push(`### ${table.schema}.${table.name}`);
    lines.push("");

    lines.push("**Insert:**");
    lines.push("```typescript");
    lines.push("// Before (Supabase)");
    lines.push(
      `// const { data } = await supabase.from('${table.name}').insert({ ... });`
    );
    lines.push("");
    lines.push("// After (HelixDB)");
    lines.push(`const result = await client.query("Add${node.name}", { ... });`);
    lines.push("// Result shape matches RETURN values from the query");
    lines.push("```");
    lines.push("");

    lines.push("**Get by ID:**");
    lines.push("```typescript");
    lines.push("// Before (Supabase)");
    lines.push(
      `// const { data } = await supabase.from('${table.name}').select().eq('id', id);`
    );
    lines.push("");
    lines.push("// After (HelixDB)");
    lines.push(`const result = await client.query("Get${node.name}", { id });`);
    lines.push("```");
    lines.push("");

    lines.push("**Delete:**");
    lines.push("```typescript");
    lines.push("// Before (Supabase)");
    lines.push(
      `// const { data } = await supabase.from('${table.name}').delete().eq('id', id);`
    );
    lines.push("");
    lines.push("// After (HelixDB)");
    lines.push(`const result = await client.query("Delete${node.name}", { id });`);
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
      lines.push(
        `// const { data } = await supabase.rpc('match_${vec.originalTable}', { query_embedding: [...], match_count: 10 });`
      );
      lines.push("");
      lines.push("// After (HelixDB)");
      lines.push(
        `const results = await client.query("Search${vec.name}", { query: "search text", limit: 10 });`
      );
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
  console.log("  1. Review the generated schema:");
  console.log(chalk.cyan(`     ${path.join(outputDir, "db", "schema.hx")}`));
  console.log("");
  console.log("  2. Start HelixDB:");
  console.log(chalk.cyan(`     cd ${outputDir} && helix push dev`));
  console.log("");
  console.log("  3. Update your app code using the migration guide:");
  console.log(chalk.cyan(`     ${path.join(outputDir, "MIGRATION_GUIDE.md")}`));
  console.log("");
  console.log("  4. Keep migration manifest for re-runs:");
  console.log(chalk.cyan(`     ${path.join(outputDir, MANIFEST_RELATIVE_PATH)}`));
  console.log("");
  console.log("  5. Clean up import queries after migration:");
  console.log(chalk.cyan(`     rm ${path.join(outputDir, "db", "import.hx")}`));
  console.log("");
}

async function runHelixDeploy(options: {
  outputDir: string;
  instanceName: string;
  resetInstance: boolean;
  autoConfirm: boolean;
  nonInteractive: boolean;
}) {
  const { outputDir, instanceName, resetInstance, autoConfirm, nonInteractive } = options;

  if (resetInstance) {
    const shouldReset = await confirmResetInstance(autoConfirm, nonInteractive, instanceName);
    if (!shouldReset) {
      console.error(chalk.red("\n  Aborted: reset confirmation declined."));
      process.exit(1);
    }

    const resetSpinner = ora(`Resetting Helix instance '${instanceName}'...`).start();
    const deleteResult = runHelixCommand(["delete", instanceName], outputDir, "y\n");
    if (deleteResult.status !== 0) {
      resetSpinner.fail(`Failed to reset instance '${instanceName}'`);
      throw new Error((deleteResult.stderr || deleteResult.stdout || "helix delete failed").trim());
    }
    resetSpinner.succeed(`Reset instance '${instanceName}'`);
  }

  const deploySpinner = ora(`Deploying Helix instance '${instanceName}'...`).start();
  const pushResult = runHelixCommand(["push", instanceName], outputDir);
  if (pushResult.status !== 0) {
    deploySpinner.fail(`Failed to deploy instance '${instanceName}'`);
    throw new Error((pushResult.stderr || pushResult.stdout || "helix push failed").trim());
  }

  deploySpinner.succeed(`Helix instance '${instanceName}' deployed`);
}

function runHelixCommand(args: string[], cwd: string, input?: string) {
  return spawnSync("helix", args, {
    cwd,
    input,
    encoding: "utf-8",
  });
}

async function confirmResetInstance(
  autoConfirm: boolean,
  nonInteractive: boolean,
  instanceName: string
): Promise<boolean> {
  if (autoConfirm) {
    return true;
  }

  if (nonInteractive) {
    return false;
  }

  const answer = await prompts({
    type: "confirm",
    name: "confirmed",
    message: `Delete Helix instance '${instanceName}' before deploy? This will remove all instance data.`,
    initial: false,
  });

  return answer.confirmed === true;
}

function normalizeSupabaseConnectionString(raw: string): string {
  try {
    const url = new URL(raw);
    const isPooler = url.hostname.includes("pooler.supabase.com");
    if (isPooler && !url.searchParams.has("uselibpqcompat")) {
      url.searchParams.set("uselibpqcompat", "true");
      console.log(chalk.yellow("  Added uselibpqcompat=true for Supabase pooler SSL compatibility."));
    }
    return url.toString();
  } catch {
    return raw;
  }
}

function parseHelixPort(helixUrl: string): number {
  try {
    const parsed = new URL(helixUrl);
    const host = parsed.hostname.toLowerCase();
    const isLocal = host === "localhost" || host === "127.0.0.1" || host === "::1";
    if (!isLocal) {
      return 6969;
    }
    if (parsed.port) {
      return Number.parseInt(parsed.port, 10);
    }
    return 6969;
  } catch {
    return 6969;
  }
}

function withUpdatedUrlPort(helixUrl: string, port: number): string {
  try {
    const parsed = new URL(helixUrl);
    parsed.port = String(port);
    return parsed.toString();
  } catch {
    return `http://localhost:${port}`;
  }
}

async function findAvailableLocalPort(preferredPort: number): Promise<number> {
  const maxChecks = 25;

  for (let offset = 0; offset < maxChecks; offset += 1) {
    const candidate = preferredPort + offset;
    if (await isPortAvailable(candidate)) {
      return candidate;
    }
  }

  return preferredPort;
}

async function isPortAvailable(port: number): Promise<boolean> {
  return await new Promise((resolve) => {
    const server = net
      .createServer()
      .once("error", () => resolve(false))
      .once("listening", () => {
        server.close(() => resolve(true));
      })
      .listen(port, "0.0.0.0");
  });
}

function requiresHelixApiKey(
  helixUrl: string,
  outputDir: string,
  instanceName: string
): boolean {
  if (!isLocalHelixUrl(helixUrl)) {
    return true;
  }

  return helixTomlHasProdCloudInstance(outputDir, instanceName);
}

function isLocalHelixUrl(helixUrl: string): boolean {
  try {
    const parsed = new URL(helixUrl);
    const host = parsed.hostname.toLowerCase();
    return host === "localhost" || host === "127.0.0.1" || host === "::1";
  } catch {
    return false;
  }
}

function helixTomlHasProdCloudInstance(outputDir: string, instanceName: string): boolean {
  const tomlPath = path.join(outputDir, "helix.toml");
  if (!fs.existsSync(tomlPath)) {
    return false;
  }

  const lines = fs.readFileSync(tomlPath, "utf-8").split(/\r?\n/);
  let inTargetCloudSection = false;
  const cloudSectionPrefix = `[cloud.${instanceName}.`;

  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) {
      continue;
    }

    if (line.startsWith("[") && line.endsWith("]")) {
      inTargetCloudSection = line.startsWith(cloudSectionPrefix);
      continue;
    }

    if (!inTargetCloudSection) {
      continue;
    }

    if (/^build_mode\s*=\s*"release"\s*$/.test(line)) {
      return true;
    }
  }

  return false;
}

function resolveHelixApiKey(explicitKey: string | undefined, outputDir: string): string | undefined {
  if (explicitKey?.trim()) {
    return explicitKey.trim();
  }

  if (process.env.HELIX_API_KEY?.trim()) {
    return process.env.HELIX_API_KEY.trim();
  }

  const cwdEnv = readEnvFile(path.join(process.cwd(), ".env"));
  if (cwdEnv.HELIX_API_KEY?.trim()) {
    return cwdEnv.HELIX_API_KEY.trim();
  }

  const outputEnv = readEnvFile(path.join(outputDir, ".env"));
  if (outputEnv.HELIX_API_KEY?.trim()) {
    return outputEnv.HELIX_API_KEY.trim();
  }

  return undefined;
}

function readEnvFile(filePath: string): Record<string, string> {
  if (!fs.existsSync(filePath)) {
    return {};
  }

  const out: Record<string, string> = {};
  const lines = fs.readFileSync(filePath, "utf-8").split(/\r?\n/);
  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) {
      continue;
    }

    const idx = line.indexOf("=");
    if (idx <= 0) {
      continue;
    }

    const key = line.slice(0, idx).trim();
    let value = line.slice(idx + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    out[key] = value;
  }

  return out;
}

async function runHelixCheck(outputDir: string, strictMode: boolean) {
  const checkSpinner = ora("Running `helix check` on generated project...").start();

  const result = spawnSync("helix", ["check"], {
    cwd: outputDir,
    encoding: "utf-8",
  });

  if (result.error) {
    if ((result.error as NodeJS.ErrnoException).code === "ENOENT") {
      const message =
        "Helix CLI not found in PATH. Install it or rerun with --skip-helix-check.";
      if (strictMode) {
        checkSpinner.fail("`helix check` unavailable");
        throw new Error(message);
      }

      checkSpinner.warn("Skipping `helix check` (helix CLI not found)");
      console.log(chalk.yellow(`  ${message}`));
      return;
    }

    checkSpinner.fail("`helix check` failed to execute");
    throw result.error;
  }

  if (result.status !== 0) {
    checkSpinner.fail("Generated project failed `helix check`");
    const details = (result.stderr || result.stdout || "Unknown helix check error").trim();
    throw new Error(details);
  }

  checkSpinner.succeed("Generated project passes `helix check`");
}

function parsePositiveInteger(rawValue: string, flagName: string): number {
  const parsed = Number.parseInt(rawValue, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    console.error(chalk.red(`\n  Invalid ${flagName}: ${rawValue}`));
    console.error(chalk.gray(`  ${flagName} must be a positive integer.`));
    process.exit(1);
  }
  return parsed;
}

function parseTypeMappingOptions(bigintMode: string): TypeMappingOptions {
  try {
    return resolveTypeMappingOptions({ bigintMode });
  } catch (err) {
    console.error(chalk.red(`\n  ${err instanceof Error ? err.message : err}`));
    console.error(chalk.gray("  Valid values for --bigint-mode: string, i64"));
    process.exit(1);
  }
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

function parseTableFilters(raw: string | undefined): string[] {
  if (!raw) {
    return [];
  }

  return raw
    .split(",")
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean);
}

function matchesTableFilter(
  table: TableInfo,
  includeFilters: string[],
  excludeFilters: string[]
): boolean {
  const schemaQualified = `${table.schema}.${table.name}`.toLowerCase();
  const tableOnly = table.name.toLowerCase();

  const matches = (filter: string): boolean =>
    filter === schemaQualified || filter === tableOnly;

  if (includeFilters.length > 0 && !includeFilters.some(matches)) {
    return false;
  }

  if (excludeFilters.some(matches)) {
    return false;
  }

  return true;
}
