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

/**
 * Map a PostgreSQL column type to the corresponding HelixDB type.
 */
export function mapPgType(
  pgType: string,
  udtName?: string
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
    if (["int4", "int8", "integer", "bigint"].includes(baseType)) {
      return { helixType: "[I64]", isVector: false, needsSerialization: false };
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

  return name
    .split(/[_\-\s]+/)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join("");
}

/**
 * Convert a PostgreSQL column name to a HelixDB field name.
 * Keeps snake_case as-is (HelixDB supports it).
 */
export function toFieldName(pgColumn: string): string {
  return pgColumn;
}
