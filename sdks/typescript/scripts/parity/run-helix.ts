import { mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import { canonicalizeJson, parseJsonStructural, structuralJsonEqual } from "../../src/index.js";
import { goGeneratedRoot, resultsRoot, rustGeneratedRoot, typescriptGeneratedRoot, workspaceRoot, workspacesRoot } from "./paths.js";

type Instance = {
  label: "rust" | "typescript" | "go";
  generatedRoot: string;
  workspace: string;
  results: string;
  port: number;
};

const rust: Instance = {
  label: "rust",
  generatedRoot: rustGeneratedRoot,
  workspace: join(workspacesRoot, "rust"),
  results: join(resultsRoot, "rust"),
  port: 18080,
};

const typescript: Instance = {
  label: "typescript",
  generatedRoot: typescriptGeneratedRoot,
  workspace: join(workspacesRoot, "typescript"),
  results: join(resultsRoot, "typescript"),
  port: 18081,
};

const go: Instance = {
  label: "go",
  generatedRoot: goGeneratedRoot,
  workspace: join(workspacesRoot, "go"),
  results: join(resultsRoot, "go"),
  port: 18082,
};

await assertCommand("helix", ["--version"]);
const serverBinary = process.env.HELIX_PARITY_SERVER_BIN ?? join(workspaceRoot, "target", "debug", "server");
if (!process.env.HELIX_PARITY_SERVER_BIN) run("cargo", ["build", "-p", "server"], workspaceRoot, 300_000);
if (!existsSync(serverBinary)) throw new Error(`Helix parity server binary does not exist: ${serverBinary}`);

await runInstance(rust);
await runInstance(typescript);
await runInstance(go);
await compareResults(rust, typescript);
await compareResults(rust, go);

console.log("Helix runtime parity passed");

async function runInstance(instance: Instance) {
  await rm(instance.results, { recursive: true, force: true });
  await mkdir(instance.results, { recursive: true });
  await mkdir(instance.workspace, { recursive: true });

  if (!existsSync(join(instance.workspace, "helix.toml"))) {
    run("helix", ["init", "--path", instance.workspace, "local", "--name", "dev", "--port", String(instance.port)], process.cwd(), 120_000);
  }

  run("helix", ["stop", "dev"], instance.workspace, 120_000, true);
  run("helix", ["prune", "dev", "--yes"], instance.workspace, 120_000, true);

  const serverEnv = { ...process.env };
  delete serverEnv.S3_BUCKET;
  const server = spawn(serverBinary, [], {
    cwd: workspaceRoot,
    env: {
      ...serverEnv,
      HELIX_HTTP_ADDR: `127.0.0.1:${instance.port}`,
      HELIX_GRPC_ADDR: `127.0.0.1:${instance.port + 1000}`,
      DB_PATH: `parity-${instance.label}/`,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let serverOutput = "";
  server.stdout.on("data", (chunk) => {
    serverOutput = `${serverOutput}${String(chunk)}`.slice(-1024 * 1024);
  });
  server.stderr.on("data", (chunk) => {
    serverOutput = `${serverOutput}${String(chunk)}`.slice(-1024 * 1024);
  });
  server.on("error", (error) => {
    serverOutput = `${serverOutput}${error.stack ?? error.message}`.slice(-1024 * 1024);
  });

  try {
    await waitReady(instance, server, () => serverOutput);
    const files = await jsonFiles(join(instance.generatedRoot, "runtime"));
    console.log(`running ${files.length} ${instance.label} fixture(s) against Helix on port ${instance.port}`);
    for (const file of files) {
      const json = await readFile(join(instance.generatedRoot, "runtime", file), "utf8");
      const result = run("helix", ["query", "dev", "--json", json, "--compact"], instance.workspace, 120_000);
      const output = result.stdout.trim() || "null";
      const outputPath = join(instance.results, file);
      await mkdir(join(outputPath, ".."), { recursive: true });
      await writeFile(outputPath, output);
    }
  } finally {
    await stopServer(server);
  }
}

async function waitReady(instance: Instance, server: ChildProcess, output: () => string) {
  const url = `http://127.0.0.1:${instance.port}/readyz`;
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline) {
    if (server.exitCode !== null) throw new Error(`${instance.label} parity server exited with ${server.exitCode}:\n${output()}`);
    try {
      const response = await fetch(url, { signal: AbortSignal.timeout(1_000) });
      if (response.ok) return;
    } catch {
      // The server is still starting.
    }
    await delay(100);
  }
  throw new Error(`${instance.label} parity server did not become ready at ${url}:\n${output()}`);
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

  console.log(`Helix output parity passed for ${rustFiles.length} ${candidate.label} fixture(s)`);
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

async function assertCommand(command: string, args: string[]) {
  run(command, args, process.cwd(), 30_000);
}

function run(command: string, args: string[], cwd: string, timeout: number, allowFailure = false) {
  const result = spawnSync(command, args, { cwd, encoding: "utf8", timeout, maxBuffer: 1024 * 1024 * 20 });
  if (!allowFailure && (result.error || result.status !== 0)) {
    throw new Error(
      [
        `command failed: ${command} ${args.map((arg) => (arg.includes(" ") ? JSON.stringify(arg) : arg)).join(" ")}`,
        `cwd: ${cwd}`,
        result.error ? `error: ${result.error.message}` : "",
        result.stdout ? `stdout:\n${result.stdout}` : "",
        result.stderr ? `stderr:\n${result.stderr}` : "",
      ]
        .filter(Boolean)
        .join("\n"),
    );
  }
  return { stdout: result.stdout ?? "", stderr: result.stderr ?? "" };
}
