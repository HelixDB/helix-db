import { existsSync } from "node:fs";
import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import {
  canonicalizeJson,
  Client,
  g,
  parseJson,
  parseJsonStructural,
  QueryRequest,
  readBatch,
  stringifyJson,
  structuralJsonEqual,
} from "../../src/index.js";
import { goGeneratedRoot, resultsRoot, rustGeneratedRoot, typescriptGeneratedRoot, workspaceRoot } from "./paths.js";
import { cypherFixturePath, readCypherCases, verifyCypherResults } from "./cypher-results.js";
import { writeCypherCase } from "./cypher-client.js";

const EXPECTED_RUNTIME = 233;
const cypherCases = await readCypherCases();
const TRANSACTION_CONFLICT_ATTEMPTS = 8;

type Instance = {
  label: "rust" | "typescript" | "go";
  generatedRoot: string;
  results: string;
  port: number;
};

type RunningServer = {
  child: ChildProcess;
  output: () => string;
};

const instances: Instance[] = [
  {
    label: "rust",
    generatedRoot: rustGeneratedRoot,
    results: join(resultsRoot, "rust"),
    port: 18080,
  },
  {
    label: "typescript",
    generatedRoot: typescriptGeneratedRoot,
    results: join(resultsRoot, "typescript"),
    port: 18081,
  },
  {
    label: "go",
    generatedRoot: goGeneratedRoot,
    results: join(resultsRoot, "go"),
    port: 18082,
  },
];

const serverBinary = process.env.HELIX_PARITY_SERVER_BIN ?? join(workspaceRoot, "target", "debug", "server");
if (process.env.HELIX_PARITY_SERVER_BIN === undefined) run("cargo", ["build", "-p", "server"], workspaceRoot, 900_000);
if (!existsSync(serverBinary)) throw new Error(`Helix parity server binary does not exist: ${serverBinary}`);

const temp = await mkdtemp(join(tmpdir(), "helixdb-server-disk-parity-"));
try {
  for (const instance of instances) await runInstance(instance, join(temp, instance.label));
  await compareResults(instances[0]!, instances[1]!);
  await compareResults(instances[0]!, instances[2]!);
  run(
    pythonCommand(),
    [
      join(workspaceRoot, "sdks", "python", "scripts", "run_server_parity.py"),
      "--server-binary",
      serverBinary,
      "--baseline-results",
      instances[0]!.results,
    ],
    workspaceRoot,
    900_000,
  );
  await runCypherHttpSuite(temp);
  console.log(
    `server disk runtime parity passed for ${EXPECTED_RUNTIME} DSL and ${cypherCases.length} Cypher fixtures with restart coverage`,
  );
} finally {
  await rm(temp, { recursive: true, force: true });
}

/** Every SDK makes real HTTP calls on both sides of a local disk server restart. */
async function runCypherHttpSuite(temp: string): Promise<void> {
  run(
    "cargo",
    [
      "build",
      "--locked",
      "--manifest-path",
      join(workspaceRoot, "sdks/rust/Cargo.toml"),
      "--features",
      "embedded",
      "--example",
      "generate_parity_fixtures",
    ],
    workspaceRoot,
    900_000,
  );
  const goBinary = join(temp, "cypher-go-parity");
  run("go", ["build", "-o", goBinary, "./cmd/generate-parity-fixtures"], join(workspaceRoot, "sdks/go"), 900_000);
  const instance = instances[0]!;
  for (const sdk of ["rust", "typescript", "go", "python", "python-async"] as const) {
    const root = join(temp, "cypher-http", sdk);
    const data = join(root, "data");
    await mkdir(join(root, "cypher"), { recursive: true });
    await mkdir(data, { recursive: true });
    let server = startServer(instance, data);
    try {
      await waitReady(instance, server);
      for (const phase of ["before", "after"] as const) {
        if (phase === "after") {
          await stopServer(server.child);
          server = startServer(instance, data);
          await waitReady(instance, server);
        }
        const url = `http://127.0.0.1:${instance.port}`;
        const env = {
          ...process.env,
          HELIX_CYPHER_PARITY_URL: url,
          HELIX_CYPHER_PARITY_RESULTS: root,
          HELIX_CYPHER_PARITY_PHASE: phase,
          HELIX_CYPHER_PARITY_FIXTURES: cypherFixturePath,
          HELIX_PYTHON_PARITY_MODE: sdk === "python-async" ? "async" : "sync",
        };
        if (sdk === "typescript") {
          const client = new Client(url);
          try {
            let after = false;
            for (const fixture of cypherCases) {
              after ||= fixture.afterDiskReopen;
              if (after === (phase === "after")) await writeCypherCase(client, fixture, join(root, "cypher"));
            }
          } finally {
            await client.close();
          }
        } else if (sdk === "rust") {
          run(
            join(process.env.CARGO_TARGET_DIR ?? join(workspaceRoot, "target"), "debug/examples/generate_parity_fixtures"),
            [],
            workspaceRoot,
            120_000,
            env,
          );
        } else if (sdk === "go") {
          run(goBinary, [], workspaceRoot, 120_000, env);
        } else {
          run(pythonCommand(), [join(workspaceRoot, "sdks/python/scripts/run_cypher_http.py")], workspaceRoot, 120_000, env);
        }
      }
      await verifyCypherResults(join(root, "cypher"), cypherCases);
      console.log(`${sdk} Cypher HTTP runtime and disk restart passed (${cypherCases.length} cases)`);
    } finally {
      await stopServer(server.child);
    }
  }
}

async function runInstance(instance: Instance, dataRoot: string) {
  await rm(instance.results, { recursive: true, force: true });
  await Promise.all([mkdir(instance.results, { recursive: true }), mkdir(dataRoot, { recursive: true })]);

  const files = await jsonFiles(join(instance.generatedRoot, "runtime"));
  if (files.length !== EXPECTED_RUNTIME) {
    throw new Error(`${instance.label} runtime fixture count was ${files.length}, expected ${EXPECTED_RUNTIME}`);
  }

  let server = startServer(instance, dataRoot);
  try {
    await waitReady(instance, server);
    console.log(`running ${files.length} ${instance.label} fixture(s) against disk server on port ${instance.port}`);
    for (const file of files) {
      const json = await readFile(join(instance.generatedRoot, "runtime", file), "utf8");
      const response = await executeQuery(instance, json).catch((error: unknown) => {
        throw new Error(`${instance.label} HTTP fixture ${file} failed\n${server.output()}`, { cause: error });
      });
      await awaitIndexOperations(instance, response);
      const output = stringifyJson(normalizeOperationIds(response));
      await writeFile(join(instance.results, file), output);

      if (file.startsWith("905-read-text-drop-candidates")) {
        await stopServer(server.child);
        server = startServer(instance, dataRoot);
        await waitReady(instance, server);
        const reopened = await executeQuery(instance, json);
        if (!structuralJsonEqual(output, stringifyJson(normalizeOperationIds(reopened)))) {
          throw new Error(`${instance.label} fixture ${file} changed after server restart`);
        }
      }
    }

    for (const file of ["025-read-text-search-nodes.json", "027-read-text-search-edges.json"]) {
      const json = await readFile(join(instance.generatedRoot, "runtime", file), "utf8");
      const response = await postQuery(instance, json);
      if (response.ok) throw new Error(`${instance.label} ${file} unexpectedly succeeded after index DROP`);
      if (!response.body.includes("index_not_found")) {
        throw new Error(`${instance.label} ${file} returned the wrong post-DROP error: ${response.status} ${response.body}`);
      }
    }
  } finally {
    await stopServer(server.child);
  }
}

function startServer(instance: Instance, dataRoot: string): RunningServer {
  const serverEnv = { ...process.env };
  delete serverEnv.S3_BUCKET;
  const child = spawn(serverBinary, [], {
    cwd: workspaceRoot,
    env: {
      ...serverEnv,
      HELIX_HTTP_ADDR: `127.0.0.1:${instance.port}`,
      HELIX_GRPC_ADDR: `127.0.0.1:${instance.port + 1000}`,
      HELIX_DATA_DIR: dataRoot,
      DB_PATH: `parity-${instance.label}/`,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let output = "";
  const append = (chunk: unknown) => {
    output = `${output}${String(chunk)}`.slice(-1024 * 1024);
  };
  child.stdout.on("data", append);
  child.stderr.on("data", append);
  child.on("error", (error) => append(error.stack ?? error.message));
  return { child, output: () => output };
}

async function executeQuery(instance: Instance, json: string): Promise<unknown> {
  for (let attempt = 0; attempt < TRANSACTION_CONFLICT_ATTEMPTS; attempt += 1) {
    const response = await postQuery(instance, json);
    if (response.ok) return parseJson(response.body);
    // The HTTP server reserves 409 for retryable transaction conflicts. The
    // losing transaction did not commit, so replaying the fixture is safe.
    if (response.status === 409 && attempt + 1 < TRANSACTION_CONFLICT_ATTEMPTS) {
      await delay(10 * 2 ** attempt);
      continue;
    }
    throw new Error(`${instance.label} query failed with HTTP ${response.status}: ${response.body}`);
  }
  throw new Error(`${instance.label} transaction conflict retry loop exhausted unexpectedly`);
}

async function postQuery(instance: Instance, json: string): Promise<{ ok: boolean; status: number; body: string }> {
  const response = await fetch(`http://127.0.0.1:${instance.port}/v2/query`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: json,
    signal: AbortSignal.timeout(120_000),
  });
  return { ok: response.ok, status: response.status, body: await response.text() };
}

async function awaitIndexOperations(instance: Instance, response: unknown): Promise<void> {
  for (const operationId of collectOperationIds(response)) {
    const deadline = Date.now() + 60_000;
    for (;;) {
      const request = QueryRequest.read(readBatch().varAs("status", g().getIndexOperation(operationId)).returning(["status"]));
      const statusResponse = await executeQuery(instance, request.toJsonString());
      const status = objectField(objectField(statusResponse, "status"), "status");
      if (status === "succeeded") break;
      if (status !== "queued" && status !== "running") {
        throw new Error(`operation ${operationId} reached unexpected status ${String(status)}: ${stringifyJson(statusResponse)}`);
      }
      if (Date.now() >= deadline) throw new Error(`operation ${operationId} did not finish within 60s`);
      await delay(10);
    }
  }
}

function collectOperationIds(value: unknown, ids = new Set<string>()): Set<string> {
  if (Array.isArray(value)) {
    for (const entry of value) collectOperationIds(entry, ids);
  } else if (value !== null && typeof value === "object") {
    const object = value as Record<string, unknown>;
    if ((object.kind === "accepted" || object.kind === "existing_operation") && typeof object.operation_id === "string") {
      ids.add(object.operation_id);
    }
    for (const entry of Object.values(object)) collectOperationIds(entry, ids);
  }
  return ids;
}

function normalizeOperationIds(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(normalizeOperationIds);
  if (value === null || typeof value !== "object") return value;
  const normalized = Object.fromEntries(Object.entries(value).map(([key, entry]) => [key, normalizeOperationIds(entry)]));
  if ((normalized.kind === "accepted" || normalized.kind === "existing_operation") && "operation_id" in normalized) {
    normalized.operation_id = "<operation-id>";
  }
  return normalized;
}

function objectField(value: unknown, field: string): unknown {
  if (value === null || typeof value !== "object" || !(field in value)) throw new Error(`server response is missing ${field}`);
  return (value as Record<string, unknown>)[field];
}

async function waitReady(instance: Instance, server: RunningServer) {
  const url = `http://127.0.0.1:${instance.port}/readyz`;
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline) {
    if (server.child.exitCode !== null) {
      throw new Error(`${instance.label} parity server exited with ${server.child.exitCode}:\n${server.output()}`);
    }
    try {
      const response = await fetch(url, { signal: AbortSignal.timeout(1_000) });
      if (response.ok) return;
    } catch {
      // The server is still starting.
    }
    await delay(100);
  }
  throw new Error(`${instance.label} parity server did not become ready at ${url}:\n${server.output()}`);
}

async function stopServer(server: ChildProcess) {
  if (server.exitCode !== null) return;
  const exited = once(server, "exit");
  server.kill("SIGTERM");
  const stopped = await Promise.race([exited.then(() => true), delay(5_000).then(() => false)]);
  if (!stopped && server.exitCode === null) {
    server.kill("SIGKILL");
    await exited;
  }
}

async function compareResults(baseline: Instance, candidate: Instance) {
  const rustFiles = await jsonFiles(baseline.results);
  const candidateFiles = await jsonFiles(candidate.results);
  const candidateSet = new Set(candidateFiles);
  const rustSet = new Set(rustFiles);
  const missingInCandidate = rustFiles.filter((file) => !candidateSet.has(file));
  const extraInCandidate = candidateFiles.filter((file) => !rustSet.has(file));
  if (missingInCandidate.length || extraInCandidate.length) {
    throw new Error(
      [
        missingInCandidate.length ? `missing ${candidate.label} runtime results:\n${missingInCandidate.join("\n")}` : "",
        extraInCandidate.length ? `extra ${candidate.label} runtime results:\n${extraInCandidate.join("\n")}` : "",
      ]
        .filter(Boolean)
        .join("\n\n"),
    );
  }

  const mismatches: string[] = [];
  for (const file of rustFiles) {
    const rustJson = await readFile(join(baseline.results, file), "utf8");
    const candidateJson = await readFile(join(candidate.results, file), "utf8");
    if (!structuralJsonEqual(rustJson, candidateJson)) {
      mismatches.push(
        `${file}\nRust: ${JSON.stringify(canonicalizeJson(parseJsonStructural(rustJson)))}\n${candidate.label}: ${JSON.stringify(canonicalizeJson(parseJsonStructural(candidateJson)))}`,
      );
    }
  }

  if (mismatches.length) {
    throw new Error(`Helix output parity failed for ${mismatches.length} ${candidate.label} fixture(s):\n\n${mismatches.join("\n\n")}`);
  }
  console.log(`server disk output parity passed for ${rustFiles.length} ${candidate.label} fixture(s)`);
}

async function jsonFiles(root: string, dir = ""): Promise<string[]> {
  const entries = await readdir(join(root, dir), { withFileTypes: true });
  const files = await Promise.all(
    entries.map(async (entry) => {
      const rel = join(dir, entry.name);
      if (entry.isDirectory()) return jsonFiles(root, rel);
      if (entry.isFile() && entry.name.endsWith(".json")) return [rel];
      return [];
    }),
  );
  return files.flat().sort((a, b) => a.localeCompare(b));
}

function run(command: string, args: string[], cwd: string, timeout: number, env: NodeJS.ProcessEnv = process.env) {
  const result = spawnSync(command, args, { cwd, env, encoding: "utf8", timeout, maxBuffer: 1024 * 1024 * 20 });
  if (result.error === undefined && result.status === 0) return;
  throw new Error(
    [
      `command failed: ${command} ${args.map((arg) => (arg.includes(" ") ? JSON.stringify(arg) : arg)).join(" ")}`,
      `cwd: ${cwd}`,
      result.error === undefined ? "" : `error: ${result.error.message}`,
      result.stdout ? `stdout:\n${result.stdout}` : "",
      result.stderr ? `stderr:\n${result.stderr}` : "",
    ]
      .filter(Boolean)
      .join("\n"),
  );
}

function pythonCommand(): string {
  return process.env.HELIX_PYTHON ?? (process.platform === "win32" ? "python" : "python3");
}
