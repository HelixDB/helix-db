const test = require("node:test");
const assert = require("node:assert/strict");

const { generateSchema } = require("../dist/generate-schema.js");
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

test("schema generation keeps FK columns and emits UNIQUE edges", () => {
  const introspection = {
    tables: [
      {
        schema: "public",
        name: "authors",
        columns: [
          makeColumn({ name: "id", dataType: "uuid", udtName: "uuid", isPrimaryKey: true }),
          makeColumn({ name: "name" }),
        ],
        primaryKeys: ["id"],
        foreignKeys: [],
        indexes: [],
        rowCount: 1,
      },
      {
        schema: "public",
        name: "posts",
        columns: [
          makeColumn({ name: "id", dataType: "uuid", udtName: "uuid", isPrimaryKey: true }),
          makeColumn({ name: "author_id", dataType: "uuid", udtName: "uuid" }),
          makeColumn({ name: "title" }),
        ],
        primaryKeys: ["id"],
        foreignKeys: [
          {
            constraintName: "posts_author_fkey",
            columnNames: ["author_id"],
            foreignTableSchema: "public",
            foreignTableName: "authors",
            foreignColumnNames: ["id"],
          },
        ],
        indexes: [
          {
            indexName: "posts_author_id_unique",
            columnName: "author_id",
            isUnique: true,
            columnPosition: 1,
          },
        ],
        rowCount: 1,
      },
    ],
    enums: {},
    unsupportedFeatures: [],
  };

  const schema = generateSchema(
    introspection,
    resolveTypeMappingOptions({ bigintMode: "string" })
  );

  const postNode = schema.nodes.find((node) => node.name === "Post");
  assert.ok(postNode, "Post node should exist");
  assert.ok(
    postNode.fields.some((field) => field.name === "author_id"),
    "FK column should remain on node fields"
  );

  assert.match(schema.schemaHx, /E::HasAuthor UNIQUE \{/);
});
