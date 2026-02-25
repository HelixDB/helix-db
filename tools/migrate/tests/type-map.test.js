const test = require("node:test");
const assert = require("node:assert/strict");

const {
  mapPgType,
  resolveTypeMappingOptions,
  toFieldName,
  toPascalCase,
} = require("../dist/type-map.js");

test("bigint maps to String in safe mode", () => {
  const options = resolveTypeMappingOptions({ bigintMode: "string" });
  const mapping = mapPgType("bigint", "int8", options);
  assert.equal(mapping.helixType, "String");
});

test("bigint maps to I64 in i64 mode", () => {
  const options = resolveTypeMappingOptions({ bigintMode: "i64" });
  const mapping = mapPgType("bigint", "int8", options);
  assert.equal(mapping.helixType, "I64");
});

test("identifier sanitization avoids reserved words", () => {
  assert.equal(toFieldName("RETURN"), "RETURN_value");
  assert.equal(toPascalCase("query"), "QueryType");
});
