/**
 * Generates HelixDB .hx query files for CRUD operations on each Node/Edge/Vector type.
 *
 * For each Node type, generates:
 * - Add (insert) query
 * - Get by ID query
 * - Get all (with pagination) query
 * - Update query
 * - Delete query
 *
 * For each Edge type, generates:
 * - Add edge query
 * - Traverse outgoing query
 *
 * For each Vector type, generates:
 * - Add vector query
 * - Search query
 *
 * Also generates bulk import queries used by the data migration step.
 */

import { NodeSchema, EdgeSchema, VectorSchema, GeneratedSchema } from "./generate-schema";

export interface GeneratedQueries {
  queriesHx: string; // the complete queries.hx file content
  importQueriesHx: string; // bulk import queries for data migration
}

/**
 * Generate HelixQL query files from the generated schema.
 */
export function generateQueries(schema: GeneratedSchema): GeneratedQueries {
  const queryLines: string[] = [];
  const importLines: string[] = [];

  queryLines.push("// ============================================");
  queryLines.push("// HelixDB Queries - Auto-generated from Supabase");
  queryLines.push("// ============================================");
  queryLines.push("//");
  queryLines.push("// These queries provide basic CRUD operations for your migrated data.");
  queryLines.push("// Customize and extend these as needed for your application.");
  queryLines.push("");

  importLines.push("// ============================================");
  importLines.push("// HelixDB Import Queries - Used during data migration");
  importLines.push("// ============================================");
  importLines.push("//");
  importLines.push("// These queries are used by the migration tool to bulk-import data.");
  importLines.push("// You can safely delete this file after migration is complete.");
  importLines.push("");

  // Generate Node queries
  for (const node of schema.nodes) {
    queryLines.push(...generateNodeQueries(node));
    importLines.push(...generateNodeImportQuery(node));
  }

  // Generate Edge queries
  for (const edge of schema.edges) {
    queryLines.push(...generateEdgeQueries(edge));
    importLines.push(...generateEdgeImportQuery(edge));
  }

  // Generate Vector queries
  for (const vec of schema.vectors) {
    queryLines.push(...generateVectorQueries(vec));
    importLines.push(...generateVectorImportQuery(vec));
  }

  return {
    queriesHx: queryLines.join("\n"),
    importQueriesHx: importLines.join("\n"),
  };
}

function generateNodeQueries(node: NodeSchema): string[] {
  const lines: string[] = [];
  const name = node.name;
  const fields = node.fields;

  // --- Add Node ---
  const addParams = fields
    .filter((f) => !f.hasDefault || !f.defaultValue)
    .map((f) => `${f.name}: ${f.helixType}`)
    .join(", ");

  const addFields = fields
    .filter((f) => !f.hasDefault || !f.defaultValue)
    .map((f) => `${f.name}: ${f.name}`)
    .join(", ");

  lines.push(`// --- ${name} CRUD ---`);
  lines.push("");

  if (addParams) {
    lines.push(`QUERY Add${name}(${addParams}) =>`);
    lines.push(`    node <- AddN<${name}>({${addFields}})`);
    lines.push(`    RETURN node`);
  } else {
    lines.push(`QUERY Add${name}() =>`);
    lines.push(`    node <- AddN<${name}>({})`);
    lines.push(`    RETURN node`);
  }
  lines.push("");

  // --- Get by ID ---
  lines.push(`QUERY Get${name}(id: ID) =>`);
  lines.push(`    node <- N<${name}>(id)`);
  lines.push(`    RETURN node`);
  lines.push("");

  // --- Delete ---
  lines.push(`QUERY Delete${name}(id: ID) =>`);
  lines.push(`    node <- N<${name}>(id)`);
  lines.push(`    DROP node`);
  lines.push(`    RETURN "deleted"`);
  lines.push("");

  return lines;
}

function generateEdgeQueries(edge: EdgeSchema): string[] {
  const lines: string[] = [];

  // --- Add Edge ---
  lines.push(`// --- ${edge.name} Edge ---`);
  lines.push("");
  lines.push(
    `QUERY Add${edge.name}(from_id: ID, to_id: ID) =>`
  );
  lines.push(`    from_node <- N<${edge.fromNode}>(from_id)`);
  lines.push(`    to_node <- N<${edge.toNode}>(to_id)`);
  lines.push(`    edge <- AddE<${edge.name}>::From(from_node)::To(to_node)`);
  lines.push(`    RETURN edge`);
  lines.push("");

  // --- Traverse outgoing ---
  lines.push(
    `QUERY Get${edge.fromNode}${edge.toNode}Via${edge.name}(id: ID) =>`
  );
  lines.push(`    source <- N<${edge.fromNode}>(id)`);
  lines.push(`    targets <- source::Out<${edge.name}>`);
  lines.push(`    RETURN targets`);
  lines.push("");

  return lines;
}

function generateVectorQueries(vec: VectorSchema): string[] {
  const lines: string[] = [];

  // --- Search vectors ---
  lines.push(`// --- ${vec.name} Vector Search ---`);
  lines.push("");
  lines.push(`QUERY Search${vec.name}(query: String, limit: I32) =>`);
  lines.push(`    results <- SearchV<${vec.name}>(Embed(query), limit)`);
  lines.push(`    RETURN results`);
  lines.push("");

  return lines;
}

function generateNodeImportQuery(node: NodeSchema): string[] {
  const lines: string[] = [];
  const fields = node.fields;

  // Build parameter list for single-row import
  const params = fields.map((f) => `${f.name}: ${f.helixType}`).join(", ");
  const fieldAssign = fields.map((f) => `${f.name}: ${f.name}`).join(", ");

  lines.push(`// Import query for ${node.originalTable}`);

  if (params) {
    lines.push(`QUERY Import${node.name}(${params}) =>`);
    lines.push(`    node <- AddN<${node.name}>({${fieldAssign}})`);
    lines.push(`    RETURN node`);
  } else {
    lines.push(`QUERY Import${node.name}() =>`);
    lines.push(`    node <- AddN<${node.name}>({})`);
    lines.push(`    RETURN node`);
  }
  lines.push("");

  return lines;
}

function generateEdgeImportQuery(edge: EdgeSchema): string[] {
  const lines: string[] = [];

  lines.push(`// Import query for ${edge.originalConstraint}`);
  lines.push(
    `QUERY Import${edge.name}(from_id: ID, to_id: ID) =>`
  );
  lines.push(`    from_node <- N<${edge.fromNode}>(from_id)`);
  lines.push(`    to_node <- N<${edge.toNode}>(to_id)`);
  lines.push(`    edge <- AddE<${edge.name}>::From(from_node)::To(to_node)`);
  lines.push(`    RETURN edge`);
  lines.push("");

  return lines;
}

function generateVectorImportQuery(vec: VectorSchema): string[] {
  const lines: string[] = [];

  const metaParams = vec.metadataFields.map((f) => `${f.name}: ${f.helixType}`).join(", ");
  const metaFields = vec.metadataFields.map((f) => `${f.name}: ${f.name}`).join(", ");

  lines.push(`// Import query for ${vec.originalTable} vectors`);
  if (metaParams) {
    lines.push(`QUERY Import${vec.name}(vector: [F64], ${metaParams}) =>`);
    lines.push(`    v <- AddV<${vec.name}>(vector, {${metaFields}})`);
  } else {
    lines.push(`QUERY Import${vec.name}(vector: [F64]) =>`);
    lines.push(`    v <- AddV<${vec.name}>(vector, {})`);
  }
  lines.push(`    RETURN v`);
  lines.push("");

  return lines;
}
