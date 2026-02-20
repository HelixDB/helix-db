const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const { generateSchema } = require("../dist/generate-schema.js");
const { generateQueries } = require("../dist/generate-queries.js");
const { resolveTypeMappingOptions } = require("../dist/type-map.js");

function makeColumn({
  name,
  dataType = "text",
  udtName = "text",
  isNullable = false,
  isPrimaryKey = false,
}) {
  return {
    name,
    dataType,
    udtName,
    isNullable,
    columnDefault: null,
    characterMaxLength: null,
    ordinalPosition: 1,
    isPrimaryKey,
  };
}

test("generated fixture project passes helix check", (t) => {
  const helix = spawnSync("helix", ["--version"], { encoding: "utf-8" });
  if (helix.status !== 0) {
    t.skip("helix CLI not available");
    return;
  }

  const introspection = {
    tables: [
      {
        schema: "public",
        name: "docs",
        columns: [
          makeColumn({ name: "id", dataType: "uuid", udtName: "uuid", isPrimaryKey: true }),
          makeColumn({ name: "content", dataType: "text", udtName: "text" }),
          makeColumn({ name: "note", dataType: "text", udtName: "text", isNullable: true }),
          makeColumn({
            name: "embedding",
            dataType: "USER-DEFINED",
            udtName: "vector",
            isNullable: true,
          }),
        ],
        primaryKeys: ["id"],
        foreignKeys: [],
        indexes: [],
        rowCount: 0,
      },
    ],
    enums: {},
    unsupportedFeatures: [],
  };

  const generatedSchema = generateSchema(
    introspection,
    resolveTypeMappingOptions({ bigintMode: "string" })
  );
  const generatedQueries = generateQueries(generatedSchema);

  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "helix-migrate-compile-"));
  fs.mkdirSync(path.join(tempDir, "db"), { recursive: true });

  const helixToml = `[project]
name = "helix-migrate-compile"
queries = "db/"

[local.dev]
port = 6969
build_mode = "dev"
`;

  fs.writeFileSync(path.join(tempDir, "helix.toml"), helixToml);
  fs.writeFileSync(path.join(tempDir, "db", "schema.hx"), generatedSchema.schemaHx);
  fs.writeFileSync(path.join(tempDir, "db", "queries.hx"), generatedQueries.queriesHx);
  fs.writeFileSync(path.join(tempDir, "db", "import.hx"), generatedQueries.importQueriesHx);

  const check = spawnSync("helix", ["check"], {
    cwd: tempDir,
    encoding: "utf-8",
  });

  assert.equal(
    check.status,
    0,
    `helix check failed:\n${check.stdout || ""}\n${check.stderr || ""}`
  );
});
