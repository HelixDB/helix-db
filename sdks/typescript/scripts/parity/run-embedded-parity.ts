import { copyFile, cp, mkdir, mkdtemp, readFile, readdir, rename, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, delimiter, dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import { spawnSync } from "node:child_process";
import { structuralJsonEqual } from "../../src/index.js";
import { workspaceRoot } from "./paths.js";

const EXPECTED_RUNTIME = 224;
const typescriptRoot = join(workspaceRoot, "sdks", "typescript");
const pythonRoot = join(workspaceRoot, "sdks", "python");
const goRoot = join(workspaceRoot, "sdks", "go");
const rustManifest = join(workspaceRoot, "sdks", "rust", "Cargo.toml");
const bindingManifest = join(workspaceRoot, "bindings", "uniffi", "Cargo.toml");
const bindingConfig = join(workspaceRoot, "bindings", "uniffi", "uniffi.toml");
const generatorManifest = join(workspaceRoot, "bindings", "uniffi-bindgen", "Cargo.toml");
const nativeLibrary = join(workspaceRoot, "target", "debug", nativeLibraryName());
const temp = await mkdtemp(join(tmpdir(), "helixdb-embedded-parity-"));

try {
  const native = join(temp, "native");
  const pythonBindings = join(native, "python");
  const nodeBindings = join(native, "node");
  const goSdk = join(temp, "go-sdk");
  const goBindings = join(goSdk, "internal", "uniffi");
  const fixtures = join(temp, "fixtures");
  const results = Object.fromEntries(["rust", "typescript", "go", "python"].map((sdk) => [sdk, join(temp, "results", sdk)])) as Record<
    "rust" | "typescript" | "go" | "python",
    string
  >;

  await Promise.all([
    mkdir(pythonBindings, { recursive: true }),
    mkdir(nodeBindings, { recursive: true }),
    cp(goRoot, goSdk, { recursive: true }),
  ]);
  run("cargo", ["build", "--locked", "-p", "helixdb-uniffi"], workspaceRoot, 900_000);

  run(
    "cargo",
    [
      "run",
      "--locked",
      "-p",
      "helixdb-uniffi",
      "--features",
      "bindgen",
      "--bin",
      "helixdb-uniffi-bindgen",
      "--",
      "generate",
      nativeLibrary,
      "--language",
      "python",
      "--out-dir",
      pythonBindings,
      "--config",
      bindingConfig,
    ],
    workspaceRoot,
    900_000,
  );
  await rename(join(pythonBindings, "helixdb.py"), join(pythonBindings, "helixdb_uniffi.py"));
  await copyFile(nativeLibrary, join(pythonBindings, basename(nativeLibrary)));

  run(
    "cargo",
    [
      "run",
      "--locked",
      "--manifest-path",
      generatorManifest,
      "--bin",
      "helixdb-uniffi-bindgen-node",
      "--",
      "generate",
      nativeLibrary,
      "--manifest-path",
      bindingManifest,
      "--out-dir",
      nodeBindings,
      "--package-name",
      "@helix-db/uniffi-test",
    ],
    workspaceRoot,
    900_000,
  );
  await copyFile(nativeLibrary, join(nodeBindings, basename(nativeLibrary)));
  await cp(join(typescriptRoot, "node_modules", "koffi"), join(nodeBindings, "node_modules", "koffi"), { recursive: true });
  await cp(join(typescriptRoot, "node_modules", "@koromix"), join(nodeBindings, "node_modules", "@koromix"), { recursive: true });

  run(
    "cargo",
    [
      "run",
      "--locked",
      "--manifest-path",
      generatorManifest,
      "--bin",
      "helixdb-uniffi-bindgen-go",
      "--",
      nativeLibrary,
      "--out-dir",
      goBindings,
      "--config",
      bindingConfig,
      "--library",
    ],
    workspaceRoot,
    900_000,
  );
  const goPackage = join(goBindings, "helixdb");
  await copyFile(nativeLibrary, join(goPackage, basename(nativeLibrary)));

  run(
    "cargo",
    [
      "run",
      "--locked",
      "--manifest-path",
      rustManifest,
      "--features",
      "embedded",
      "--example",
      "generate_parity_fixtures",
      "--",
      join(fixtures, "rust"),
    ],
    workspaceRoot,
    900_000,
    embeddedEnv(results.rust, "rust-sdk-embedded-parity", dirname(nativeLibrary)),
  );
  run(
    pythonCommand(),
    [join(pythonRoot, "scripts", "run_embedded_parity.py")],
    pythonRoot,
    900_000,
    embeddedEnv(results.python, "python-sdk-embedded-parity", pythonBindings, {
      PYTHONPATH: appendPath(pythonBindings, process.env.PYTHONPATH),
      PYTHONDONTWRITEBYTECODE: "1",
    }),
  );
  run(
    process.execPath,
    [join(typescriptRoot, "dist-dev", "scripts", "parity", "run-embedded-client.js")],
    typescriptRoot,
    900_000,
    embeddedEnv(results.typescript, "typescript-sdk-embedded-parity", nodeBindings, {
      HELIXDB_UNIFFI_NODE_PACKAGE: pathToFileURL(join(nodeBindings, "index.js")).href,
    }),
  );
  run(
    "go",
    ["run", "-tags", "helixdb_uniffi", "./cmd/generate-parity-fixtures", join(fixtures, "go")],
    goSdk,
    900_000,
    embeddedEnv(results.go, "go-sdk-embedded-parity", goPackage, {
      CGO_ENABLED: "1",
      CGO_LDFLAGS: `-L${goPackage} -lhelixdb_uniffi`,
      GOCACHE: join(temp, "go-build-cache"),
    }),
  );

  const baseline = await jsonFiles(results.rust);
  assertFixtureCount("Rust", baseline);
  for (const candidate of ["typescript", "go", "python"] as const) {
    await compareResults(results.rust, results[candidate], baseline, candidate);
  }
  console.log(`embedded runtime parity passed for ${EXPECTED_RUNTIME} fixtures across Rust, TypeScript, Go, and Python`);
} finally {
  await rm(temp, { recursive: true, force: true });
}

function nativeLibraryName(): string {
  if (process.platform === "darwin") return "libhelixdb_uniffi.dylib";
  if (process.platform === "win32") return "helixdb_uniffi.dll";
  return "libhelixdb_uniffi.so";
}

function pythonCommand(): string {
  return process.platform === "win32" ? "python" : "python3";
}

function embeddedEnv(results: string, database: string, libraryDir: string, extra: NodeJS.ProcessEnv = {}): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    ...extra,
    HELIX_EMBEDDED_PARITY_RESULTS: results,
    HELIX_EMBEDDED_PARITY_DATABASE: database,
  };
  if (process.platform === "darwin") env.DYLD_LIBRARY_PATH = appendPath(libraryDir, process.env.DYLD_LIBRARY_PATH);
  else if (process.platform === "win32") env.PATH = appendPath(libraryDir, process.env.PATH);
  else env.LD_LIBRARY_PATH = appendPath(libraryDir, process.env.LD_LIBRARY_PATH);
  return env;
}

function appendPath(path: string, current: string | undefined): string {
  return current === undefined || current.length === 0 ? path : `${path}${delimiter}${current}`;
}

async function compareResults(baselineRoot: string, candidateRoot: string, baselineFiles: string[], candidate: string) {
  const candidateFiles = await jsonFiles(candidateRoot);
  assertFixtureCount(candidate, candidateFiles);
  if (baselineFiles.join("\n") !== candidateFiles.join("\n")) throw new Error(`${candidate} embedded result filenames do not match Rust`);

  const mismatches: string[] = [];
  for (const file of baselineFiles) {
    const [baseline, value] = await Promise.all([readFile(join(baselineRoot, file), "utf8"), readFile(join(candidateRoot, file), "utf8")]);
    if (!structuralJsonEqual(baseline, value)) mismatches.push(file);
  }
  if (mismatches.length > 0) throw new Error(`${candidate} embedded result mismatches:\n${mismatches.join("\n")}`);
}

function assertFixtureCount(label: string, files: string[]) {
  if (files.length !== EXPECTED_RUNTIME)
    throw new Error(`${label} embedded result count was ${files.length}, expected ${EXPECTED_RUNTIME}`);
}

async function jsonFiles(root: string): Promise<string[]> {
  const entries = await readdir(root, { withFileTypes: true });
  return entries
    .filter((entry) => entry.isFile() && entry.name.endsWith(".json"))
    .map((entry) => entry.name)
    .sort((a, b) => a.localeCompare(b));
}

function run(command: string, args: string[], cwd: string, timeout: number, env: NodeJS.ProcessEnv = process.env) {
  const result = spawnSync(command, args, { cwd, env, encoding: "utf8", timeout, maxBuffer: 1024 * 1024 * 20, stdio: "pipe" });
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
