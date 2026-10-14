/** Install the release artifact in isolation and check its public runtime and types. */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { copyFile, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { setTimeout } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

const { values } = parseArgs({
  options: { package: { type: "string" }, image: { type: "string" }, platform: { type: "string" } },
});
assert.ok(!values.platform || values.image, "--platform requires --image");
const sdk = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const metadata = JSON.parse(await readFile(join(sdk, "package.json"), "utf8"));
const consumer = await mkdtemp(join(tmpdir(), "helix-typescript-package-"));
const npm = process.platform === "win32" ? "npm.cmd" : "npm";
let container;

try {
  let artifact = values.package;
  if (artifact === undefined) {
    const packed = JSON.parse(
      execFileSync(npm, ["pack", "--ignore-scripts", "--json", "--pack-destination", consumer], { cwd: sdk, encoding: "utf8" }),
    );
    assert.equal(packed.length, 1);
    artifact = join(consumer, packed[0].filename);
  } else if (artifact.endsWith(".tgz")) {
    artifact = resolve(artifact);
  }
  await writeFile(join(consumer, "package.json"), JSON.stringify({ private: true, type: "module" }));
  execFileSync(npm, ["install", "--ignore-scripts", "--no-audit", "--no-fund", artifact], { cwd: consumer, stdio: "inherit" });
  const installed = JSON.parse(await readFile(join(consumer, "node_modules/@helix-db/helix-db/package.json"), "utf8"));
  assert.equal(installed.version, metadata.version);
  assert.equal(installed.name, metadata.name);
  await copyFile(join(sdk, "test/package/consumer.mjs"), join(consumer, "consumer.mjs"));
  await copyFile(join(sdk, "test/package/types.mts"), join(consumer, "types.mts"));
  execFileSync(
    process.execPath,
    [
      join(sdk, "node_modules/typescript/bin/tsc"),
      "--strict",
      "--noEmit",
      "--skipLibCheck",
      "--module",
      "nodenext",
      "--target",
      "es2022",
      "types.mts",
    ],
    { cwd: consumer, stdio: "inherit" },
  );

  const env = { ...process.env };
  delete env.HELIX_PACKAGE_TEST_URL;
  if (values.image !== undefined) {
    container = execFileSync(
      "docker",
      [
        "run",
        "--detach",
        "--rm",
        "--publish",
        "127.0.0.1::8080",
        ...(values.platform === undefined ? [] : ["--platform", values.platform]),
        values.image,
      ],
      { encoding: "utf8" },
    ).trim();
    const port = execFileSync("docker", ["port", container, "8080/tcp"], { encoding: "utf8" }).trim();
    env.HELIX_PACKAGE_TEST_URL = `http://${port}`;
    const deadline = Date.now() + 30_000;
    let ready = false;
    while (Date.now() < deadline) {
      try {
        const response = await fetch(`${env.HELIX_PACKAGE_TEST_URL}/readyz`, { signal: AbortSignal.timeout(1_000) });
        ready = response.ok;
        await response.arrayBuffer();
        if (ready) break;
      } catch {
        // A newly started container may accept a connection before HTTP is ready.
      }
      await setTimeout(250);
    }
    assert.ok(ready, "release image did not become ready within 30 seconds");
    const [state] = JSON.parse(execFileSync("docker", ["inspect", container], { encoding: "utf8" }));
    const [image] = JSON.parse(execFileSync("docker", ["image", "inspect", state.Image], { encoding: "utf8" }));
    console.log(JSON.stringify({ image: values.image, platform: `${image.Os}/${image.Architecture}`, digests: image.RepoDigests }));
  }
  execFileSync(process.execPath, ["consumer.mjs"], { cwd: consumer, env, stdio: "inherit" });
  console.log(`Verified installed ${metadata.name}@${metadata.version}`);
} finally {
  try {
    if (container !== undefined) execFileSync("docker", ["rm", "--force", container], { stdio: "ignore" });
  } finally {
    await rm(consumer, { recursive: true, force: true });
  }
}
