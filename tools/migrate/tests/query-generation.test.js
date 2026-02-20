const test = require("node:test");
const assert = require("node:assert/strict");

const { generateQueries } = require("../dist/generate-queries.js");

test("import query uses required params and emits nullable setters", () => {
  const schema = {
    nodes: [
      {
        name: "User",
        originalSchema: "public",
        originalTable: "users",
        tableKey: "public.users",
        hasVectorColumn: false,
        fields: [
          {
            name: "email",
            helixType: "String",
            isNullable: false,
            isIndexed: false,
            isUnique: false,
            hasDefault: false,
            defaultValue: null,
            needsSerialization: false,
            originalColumn: "email",
            isPrimaryKey: false,
            isForeignKey: false,
          },
          {
            name: "bio",
            helixType: "String",
            isNullable: true,
            isIndexed: false,
            isUnique: false,
            hasDefault: false,
            defaultValue: null,
            needsSerialization: false,
            originalColumn: "bio",
            isPrimaryKey: false,
            isForeignKey: false,
          },
        ],
      },
    ],
    edges: [],
    vectors: [],
    schemaHx: "",
  };

  const generated = generateQueries(schema);

  assert.match(generated.importQueriesHx, /QUERY ImportUser\(email: String\)/);
  assert.doesNotMatch(generated.importQueriesHx, /QUERY ImportUser\([^)]*bio: String/);
  assert.match(generated.importQueriesHx, /QUERY ImportSetUserBio\(id: ID, value: String\)/);
});
