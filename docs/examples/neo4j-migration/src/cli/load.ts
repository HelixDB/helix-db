import { resolve } from "node:path";
import { configFromEnv } from "../config.js";
import { readDump } from "../dump.js";
import { HelixHttpClient, waitForHelix } from "../helix-http.js";
import { loadDump } from "../loader.js";

const dumpPath = resolve(
  process.argv.find(
    (argument) =>
      !argument.startsWith("--") &&
      argument !== process.argv[0] &&
      argument !== process.argv[1],
  ) ?? "movie-graph.v1.json",
);
const stopAfterNodes = process.argv.includes("--stop-after-nodes");
const config = configFromEnv();
const dump = await readDump(dumpPath);
await waitForHelix(config.helixUrl);
const progress = await loadDump(dump, new HelixHttpClient(config.helixUrl), {
  batchSize: config.batchSize,
  stopAfterNodes,
});
console.log(
  `Loaded ${progress.people} people, ${progress.movies} movies, and ${progress.relationships} relationships.`,
);
if (progress.stoppedAfterNodes)
  console.log(
    "Stopped after the node phase. Re-run without --stop-after-nodes to continue.",
  );
