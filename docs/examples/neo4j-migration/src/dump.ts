import { createHash } from "node:crypto";
import { basename } from "node:path";
import { readFile, writeFile } from "node:fs/promises";
import { parseMovieGraphDump, type MovieGraphDumpV1, type MovieGraphNode, type MovieGraphRelationship } from "./model.js";

function nodeId(node: MovieGraphNode): string {
  return node.kind === "person" ? node.personId : node.movieId;
}

export function normalizeDump(dump: MovieGraphDumpV1): MovieGraphDumpV1 {
  return {
    ...dump,
    nodes: [...dump.nodes].sort((left, right) => left.kind.localeCompare(right.kind) || nodeId(left).localeCompare(nodeId(right))),
    relationships: [...dump.relationships].sort(
      (left: MovieGraphRelationship, right: MovieGraphRelationship) =>
        left.kind.localeCompare(right.kind) || left.relationshipId.localeCompare(right.relationshipId),
    ),
  };
}

export function serializeDump(dump: MovieGraphDumpV1): string {
  return `${JSON.stringify(normalizeDump(parseMovieGraphDump(dump)), null, 2)}\n`;
}

export function checksum(contents: string): string {
  return createHash("sha256").update(contents).digest("hex");
}

export async function writeDump(path: string, dump: MovieGraphDumpV1): Promise<string> {
  const contents = serializeDump(dump);
  const digest = checksum(contents);
  await writeFile(path, contents, "utf8");
  await writeFile(`${path}.sha256`, `${digest}  ${basename(path)}\n`, "utf8");
  return digest;
}

export async function readDump(path: string): Promise<MovieGraphDumpV1> {
  const [contents, checksumFile] = await Promise.all([readFile(path, "utf8"), readFile(`${path}.sha256`, "utf8")]);
  const match = checksumFile.match(/^([0-9a-f]{64})  ([^\n]+)\n?$/);
  if (match === null || match[2] !== basename(path)) throw new TypeError(`invalid checksum file: ${path}.sha256`);
  const actual = checksum(contents);
  if (match[1] !== actual) throw new TypeError(`checksum mismatch for ${path}`);
  return parseMovieGraphDump(JSON.parse(contents) as unknown);
}

export function chunks<T>(items: readonly T[], size: number): T[][] {
  if (!Number.isSafeInteger(size) || size <= 0) throw new TypeError(`batch size must be a positive integer: ${size}`);
  return Array.from({ length: Math.ceil(items.length / size) }, (_, index) => items.slice(index * size, index * size + size));
}
