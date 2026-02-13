/**
 * Maps PostgreSQL data types to HelixDB (HelixQL) data types.
 *
 * Supabase uses standard PostgreSQL, so we map all common PG types
 * to their closest HelixDB equivalents.
 */

export interface TypeMapping {
  helixType: string;
  isVector: boolean;
  needsSerialization: boolean; // for JSON/JSONB -> String
}

export type BigIntMode = "string" | "i64";

export interface TypeMappingOptions {
  bigintMode: BigIntMode;
}

const DEFAULT_TYPE_MAPPING_OPTIONS: TypeMappingOptions = {
  bigintMode: "string",
};

const PG_TO_HELIX: Record<string, TypeMapping> = {
  // Text types
  text: { helixType: "String", isVector: false, needsSerialization: false },
  varchar: { helixType: "String", isVector: false, needsSerialization: false },
  "character varying": { helixType: "String", isVector: false, needsSerialization: false },
  char: { helixType: "String", isVector: false, needsSerialization: false },
  character: { helixType: "String", isVector: false, needsSerialization: false },
  name: { helixType: "String", isVector: false, needsSerialization: false },
  citext: { helixType: "String", isVector: false, needsSerialization: false },

  // Integer types
  smallint: { helixType: "I16", isVector: false, needsSerialization: false },
  int2: { helixType: "I16", isVector: false, needsSerialization: false },
  integer: { helixType: "I32", isVector: false, needsSerialization: false },
  int4: { helixType: "I32", isVector: false, needsSerialization: false },
  int: { helixType: "I32", isVector: false, needsSerialization: false },
  bigint: { helixType: "I64", isVector: false, needsSerialization: false },
  int8: { helixType: "I64", isVector: false, needsSerialization: false },
  serial: { helixType: "I32", isVector: false, needsSerialization: false },
  bigserial: { helixType: "I64", isVector: false, needsSerialization: false },
  smallserial: { helixType: "I16", isVector: false, needsSerialization: false },

  // Float types
  real: { helixType: "F32", isVector: false, needsSerialization: false },
  float4: { helixType: "F32", isVector: false, needsSerialization: false },
  "double precision": { helixType: "F64", isVector: false, needsSerialization: false },
  float8: { helixType: "F64", isVector: false, needsSerialization: false },
  numeric: { helixType: "F64", isVector: false, needsSerialization: false },
  decimal: { helixType: "F64", isVector: false, needsSerialization: false },
  money: { helixType: "F64", isVector: false, needsSerialization: false },

  // Boolean
  boolean: { helixType: "Boolean", isVector: false, needsSerialization: false },
  bool: { helixType: "Boolean", isVector: false, needsSerialization: false },

  // Date/Time types
  timestamp: { helixType: "Date", isVector: false, needsSerialization: false },
  "timestamp without time zone": { helixType: "Date", isVector: false, needsSerialization: false },
  "timestamp with time zone": { helixType: "Date", isVector: false, needsSerialization: false },
  timestamptz: { helixType: "Date", isVector: false, needsSerialization: false },
  date: { helixType: "Date", isVector: false, needsSerialization: false },
  time: { helixType: "String", isVector: false, needsSerialization: false },
  "time without time zone": { helixType: "String", isVector: false, needsSerialization: false },
  "time with time zone": { helixType: "String", isVector: false, needsSerialization: false },
  interval: { helixType: "String", isVector: false, needsSerialization: false },

  // UUID
  uuid: { helixType: "String", isVector: false, needsSerialization: false },

  // JSON types -> serialized as String
  json: { helixType: "String", isVector: false, needsSerialization: true },
  jsonb: { helixType: "String", isVector: false, needsSerialization: true },

  // Binary
  bytea: { helixType: "String", isVector: false, needsSerialization: true },

  // Network types
  inet: { helixType: "String", isVector: false, needsSerialization: false },
  cidr: { helixType: "String", isVector: false, needsSerialization: false },
  macaddr: { helixType: "String", isVector: false, needsSerialization: false },

  // pgvector type -> HelixDB Vector
  vector: { helixType: "[F64]", isVector: true, needsSerialization: false },

  // Enum types get mapped to String
  "USER-DEFINED": { helixType: "String", isVector: false, needsSerialization: false },

  // Array types (we'll handle these specially in mapPgType)
  ARRAY: { helixType: "String", isVector: false, needsSerialization: true },
};

const RESERVED_IDENTIFIERS = new Set([
  "QUERY",
  "RETURN",
  "DROP",
  "FOR",
  "IN",
  "UPDATE",
  "NOW",
  "EXISTS",
  "N",
  "E",
  "V",
  "ADDN",
  "ADDE",
  "ADDV",
]);

/**
 * Map a PostgreSQL column type to the corresponding HelixDB type.
 */
export function mapPgType(
  pgType: string,
  udtName?: string,
  options: TypeMappingOptions = DEFAULT_TYPE_MAPPING_OPTIONS
): TypeMapping {
  // Normalize
  const normalized = pgType.toLowerCase().trim();

  // Handle array types (e.g., _text, _int4, text[], integer[])
  if (normalized === "array" || normalized.endsWith("[]") || (udtName && udtName.startsWith("_"))) {
    // Check if it's a vector-like array of floats
    const baseType = udtName ? udtName.replace(/^_/, "") : normalized.replace(/\[\]$/, "");
    if (["float4", "float8", "real", "double precision", "numeric"].includes(baseType)) {
      return { helixType: "[F64]", isVector: false, needsSerialization: false };
    }
    if (["int4", "integer"].includes(baseType)) {
      return { helixType: "[I32]", isVector: false, needsSerialization: false };
    }
    if (["int8", "bigint"].includes(baseType)) {
      return options.bigintMode === "string"
        ? { helixType: "[String]", isVector: false, needsSerialization: false }
        : { helixType: "[I64]", isVector: false, needsSerialization: false };
    }
    if (["text", "varchar", "character varying"].includes(baseType)) {
      return { helixType: "[String]", isVector: false, needsSerialization: false };
    }
    // Default: serialize as JSON string
    return { helixType: "String", isVector: false, needsSerialization: true };
  }

  // Handle vector type from pgvector (udt_name = 'vector')
  if (udtName === "vector") {
    return { helixType: "[F64]", isVector: true, needsSerialization: false };
  }

  if (["bigint", "int8", "bigserial"].includes(normalized)) {
    return options.bigintMode === "string"
      ? { helixType: "String", isVector: false, needsSerialization: false }
      : { helixType: "I64", isVector: false, needsSerialization: false };
  }

  // Direct lookup
  if (PG_TO_HELIX[normalized]) {
    return PG_TO_HELIX[normalized];
  }

  // Check if it's a user-defined enum
  if (normalized === "user-defined") {
    return { helixType: "String", isVector: false, needsSerialization: false };
  }

  // Fallback: treat unknown types as String with serialization
  return { helixType: "String", isVector: false, needsSerialization: true };
}

export function resolveTypeMappingOptions(input?: {
  bigintMode?: string;
}): TypeMappingOptions {
  const mode = input?.bigintMode?.toLowerCase();
  if (!mode || mode === "string") {
    return { bigintMode: "string" };
  }
  if (mode === "i64") {
    return { bigintMode: "i64" };
  }
  throw new Error(`Invalid bigint mode: ${input?.bigintMode}`);
}

/**
 * Convert a PostgreSQL table name to a HelixDB Node type name.
 * e.g., "user_profiles" -> "UserProfile"
 */
export function toPascalCase(snakeCase: string): string {
  // Remove trailing 's' for simple plurals (users -> User, posts -> Post)
  let name = snakeCase;
  if (name.endsWith("ies")) {
    name = name.slice(0, -3) + "y";
  } else if (name.endsWith("ses") || name.endsWith("xes") || name.endsWith("zes")) {
    name = name.slice(0, -2);
  } else if (name.endsWith("s") && !name.endsWith("ss") && !name.endsWith("us")) {
    name = name.slice(0, -1);
  }

  const pascal = name
    .split(/[_\-\s]+/)
    .map((word) =>
      word.length === 0
        ? ""
        : word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()
    )
    .join("");

  return sanitizeTypeName(pascal);
}

/**
 * Convert a PostgreSQL column name to a HelixDB field name.
 * Keeps snake_case as-is (HelixDB supports it).
 */
export function toFieldName(pgColumn: string): string {
  let value = pgColumn
    .trim()
    .replace(/[^A-Za-z0-9_]+/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "");

  if (!value) {
    value = "field";
  }

  if (!/^[A-Za-z]/.test(value)) {
    value = `field_${value}`;
  }

  if (RESERVED_IDENTIFIERS.has(value.toUpperCase())) {
    value = `${value}_value`;
  }

  return value;
}

function sanitizeTypeName(rawName: string): string {
  let value = rawName
    .replace(/[^A-Za-z0-9]+/g, "")
    .replace(/^_+|_+$/g, "");

  if (!value) {
    value = "Type";
  }

  if (!/^[A-Za-z]/.test(value)) {
    value = `T${value}`;
  }

  if (RESERVED_IDENTIFIERS.has(value.toUpperCase())) {
    value = `${value}Type`;
  }

  return value;
}
