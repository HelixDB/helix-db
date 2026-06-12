import assert from "node:assert/strict";
import { StreamBound } from "../src/index.js";

// ── StreamBound.literal ─────────────────────────────────────────

// Upper bound: bigint above MAX_SAFE_INTEGER should throw
assert.throws(
  () => StreamBound.literal(BigInt(Number.MAX_SAFE_INTEGER) + 1n),
  TypeError,
  "should reject bigint above MAX_SAFE_INTEGER",
);

// Lower bound: bigint below MIN_SAFE_INTEGER should throw
assert.throws(
  () => StreamBound.literal(BigInt(Number.MIN_SAFE_INTEGER) - 1n),
  TypeError,
  "should reject bigint below MIN_SAFE_INTEGER",
);

// Boundary: exact MAX_SAFE_INTEGER as bigint should NOT throw
assert.doesNotThrow(
  () => StreamBound.literal(BigInt(Number.MAX_SAFE_INTEGER)),
  "should accept bigint equal to MAX_SAFE_INTEGER",
);

// Boundary: exact MIN_SAFE_INTEGER as bigint should NOT throw
assert.doesNotThrow(
  () => StreamBound.literal(BigInt(Number.MIN_SAFE_INTEGER)),
  "should accept bigint equal to MIN_SAFE_INTEGER",
);

// Regular number should NOT throw
assert.doesNotThrow(
  () => StreamBound.literal(42),
  "should accept regular numbers",
);

console.log("All StreamBound tests passed.");
