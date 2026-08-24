import { copyFile, cp, mkdir, mkdtemp, readFile, readdir, rename, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, delimiter, dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import { spawnSync } from "node:child_process";
import { canonicalizeJson, parseJsonStructural, structuralJsonEqual } from "../../src/index.js";
import { workspaceRoot } from "./paths.js";

const EXPECTED_RUNTIME = 233;
const typescriptRoot = join(workspaceRoot, "sdks", "typescript");
const pythonRoot = join(workspaceRoot, "sdks", "python");
const goRoot = join(workspaceRoot, "sdks", "go");
const goEmbeddedTemplates = join(goRoot, "internal", "embedded_templates");
const rustManifest = join(workspaceRoot, "sdks", "rust", "Cargo.toml");
const bindingManifest = join(workspaceRoot, "bindings", "uniffi", "Cargo.toml");
const bindingConfig = join(workspaceRoot, "bindings", "uniffi", "uniffi.toml");
const generatorManifest = join(workspaceRoot, "bindings", "uniffi-bindgen", "Cargo.toml");
const cargoTarget = process.env.CARGO_TARGET_DIR ?? join(workspaceRoot, "target");
const nativeLibrary = join(cargoTarget, "debug", nativeLibraryName());
const temp = await mkdtemp(join(tmpdir(), "helixdb-embedded-parity-"));
const sdks = ["rust", "typescript", "go", "python", "python-async"] as const;
const storageModes = ["memory", "disk"] as const;
type Sdk = (typeof sdks)[number];
type StorageMode = (typeof storageModes)[number];

try {
  const native = join(temp, "native");
  const pythonBindings = join(native, "python");
  const nodeBindings = join(native, "node");
  const goSdk = join(temp, "go-sdk");
  const goBindings = join(goSdk, "internal", "uniffi");
  const fixtures = join(temp, "fixtures");
  const results = Object.fromEntries(
    storageModes.map((mode) => [mode, Object.fromEntries(sdks.map((sdk) => [sdk, join(temp, "results", mode, sdk)]))]),
  ) as Record<StorageMode, Record<Sdk, string>>;
  const disks = Object.fromEntries(sdks.map((sdk) => [sdk, join(temp, "disks", sdk)])) as Record<Sdk, string>;

  await Promise.all([
    mkdir(pythonBindings, { recursive: true }),
    mkdir(nodeBindings, { recursive: true }),
    cp(goRoot, goSdk, { recursive: true }),
    ...Object.values(disks).map((root) => mkdir(root, { recursive: true })),
  ]);
  await Promise.all(
    ["embedded_uniffi.go", "native_graph_uniffi.go", "embedded_generated_test.go", "graph_generated_test.go"].map((file) =>
      copyFile(join(goEmbeddedTemplates, `${file}.tmpl`), join(goSdk, file)),
    ),
  );
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
      "--no-default-features",
      "--features",
      "node",
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
      "@helix-db/helix-db-embedded-test",
    ],
    workspaceRoot,
    900_000,
  );
  await copyFile(nativeLibrary, join(nodeBindings, basename(nativeLibrary)));
  await cp(join(typescriptRoot, "node_modules", "koffi"), join(nodeBindings, "node_modules", "koffi"), { recursive: true });
  await cp(join(typescriptRoot, "node_modules", "@koromix"), join(nodeBindings, "node_modules", "@koromix"), { recursive: true });

  const goBindgen = process.env.HELIXDB_UNIFFI_GO_BINDGEN;
  if (goBindgen === undefined) {
    run(
      "cargo",
      [
        "run",
        "--locked",
        "--manifest-path",
        generatorManifest,
        "--no-default-features",
        "--features",
        "go",
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
  } else {
    run(goBindgen, [nativeLibrary, "--out-dir", goBindings, "--config", bindingConfig, "--library"], workspaceRoot, 900_000);
  }
  const goPackage = join(goBindings, "helixdb");
  await copyFile(nativeLibrary, join(goPackage, basename(nativeLibrary)));
  run(
    "go",
    ["test", "-tags", "helixdb_uniffi", "./..."],
    goSdk,
    900_000,
    embeddedEnv(join(temp, "go-test-results"), "go-sdk-binding-tests", goPackage, "memory", disks.go, {
      CGO_ENABLED: "1",
      CGO_LDFLAGS: `-L${goPackage} -lhelixdb_uniffi`,
      GOCACHE: join(temp, "go-build-cache"),
    }),
  );

  for (const storage of storageModes) {
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
      embeddedEnv(results[storage].rust, `rust-sdk-${storage}-parity`, dirname(nativeLibrary), storage, disks.rust),
    );
    run(
      pythonCommand(),
      [join(pythonRoot, "scripts", "run_embedded_parity.py")],
      pythonRoot,
      900_000,
      embeddedEnv(results[storage].python, `python-sdk-${storage}-parity`, pythonBindings, storage, disks.python, {
        PYTHONPATH: appendPath(pythonBindings, process.env.PYTHONPATH),
        PYTHONDONTWRITEBYTECODE: "1",
      }),
    );
    run(
      pythonCommand(),
      [join(pythonRoot, "scripts", "run_embedded_parity.py")],
      pythonRoot,
      900_000,
      embeddedEnv(results[storage]["python-async"], `python-async-sdk-${storage}-parity`, pythonBindings, storage, disks["python-async"], {
        HELIX_PYTHON_PARITY_MODE: "async",
        PYTHONPATH: appendPath(pythonBindings, process.env.PYTHONPATH),
        PYTHONDONTWRITEBYTECODE: "1",
      }),
    );
    run(
      process.execPath,
      [join(typescriptRoot, "dist-dev", "scripts", "parity", "run-embedded-client.js")],
      typescriptRoot,
      900_000,
      embeddedEnv(results[storage].typescript, `typescript-sdk-${storage}-parity`, nodeBindings, storage, disks.typescript, {
        HELIXDB_EMBEDDED_NODE_PACKAGE: pathToFileURL(join(nodeBindings, "index.js")).href,
      }),
    );
    run(
      "go",
      ["run", "-tags", "helixdb_uniffi", "./cmd/generate-parity-fixtures", join(fixtures, "go")],
      goSdk,
      900_000,
      embeddedEnv(results[storage].go, `go-sdk-${storage}-parity`, goPackage, storage, disks.go, {
        CGO_ENABLED: "1",
        CGO_LDFLAGS: `-L${goPackage} -lhelixdb_uniffi`,
        GOCACHE: join(temp, "go-build-cache"),
      }),
    );
  }

  for (const storage of storageModes) {
    const baseline = await jsonFiles(results[storage].rust);
    assertFixtureCount(`Rust ${storage}`, baseline);
    for (const candidate of ["typescript", "go", "python", "python-async"] as const) {
      await compareResults(results[storage].rust, results[storage][candidate], baseline, `${candidate} ${storage}`);
    }
  }
  console.log(
    `embedded memory and disk runtime parity passed for ${EXPECTED_RUNTIME} fixtures across Rust, TypeScript, Go, synchronous Python, and asynchronous Python`,
  );
} finally {
  await rm(temp, { recursive: true, force: true });
}

function nativeLibraryName(): string {
  if (process.platform === "darwin") return "libhelixdb_uniffi.dylib";
  if (process.platform === "win32") return "helixdb_uniffi.dll";
  return "libhelixdb_uniffi.so";
}

function pythonCommand(): string {
  return process.env.HELIX_PYTHON ?? (process.platform === "win32" ? "python" : "python3");
}

function embeddedEnv(
  results: string,
  database: string,
  libraryDir: string,
  storage: StorageMode,
  diskRoot: string,
  extra: NodeJS.ProcessEnv = {},
): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    ...extra,
    HELIX_EMBEDDED_PARITY_RESULTS: results,
    HELIX_EMBEDDED_PARITY_DATABASE: database,
    HELIX_EMBEDDED_PARITY_STORAGE: storage,
  };
  if (storage === "disk") env.HELIX_EMBEDDED_PARITY_DISK_ROOT = diskRoot;
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
    if (!structuralJsonEqual(baseline, value)) {
      mismatches.push(
        `${file}\nRust: ${JSON.stringify(canonicalizeJson(parseJsonStructural(baseline)))}\n${candidate}: ${JSON.stringify(canonicalizeJson(parseJsonStructural(value)))}`,
      );
    }
  }
  if (mismatches.length > 0) throw new Error(`${candidate} embedded result mismatches:\n\n${mismatches.join("\n\n")}`);
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
      `status: ${String(result.status)}`,
      `signal: ${String(result.signal)}`,
      result.error === undefined ? "" : `error: ${result.error.message}`,
      result.stdout ? `stdout:\n${result.stdout}` : "",
      result.stderr ? `stderr:\n${result.stderr}` : "",
    ]
      .filter(Boolean)
      .join("\n"),
  );
}
