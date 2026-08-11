import { resolve } from "node:path";
import { readDump } from "../dump.js";

const dumpPath = resolve(process.argv[2] ?? "movie-graph.v1.json");
const dump = await readDump(dumpPath);
console.log(
  `Validated version ${dump.version}: ${dump.nodes.length} nodes and ${dump.relationships.length} relationships.`,
);
