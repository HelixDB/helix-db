import { spawnSync } from "node:child_process";
import { join } from "node:path";
import { workspaceRoot } from "./paths.js";

const command = process.platform === "win32" ? "python" : "python3";
const script = join(workspaceRoot, "sdks", "python", "scripts", "generate_parity_fixtures.py");
const result = spawnSync(command, [script], {
  cwd: join(workspaceRoot, "sdks", "python"),
  env: { ...process.env, PYTHONDONTWRITEBYTECODE: "1" },
  encoding: "utf8",
  stdio: "inherit",
});
if (result.error !== undefined) throw result.error;
if (result.status !== 0) throw new Error(`Python parity generator exited with ${result.status}`);
