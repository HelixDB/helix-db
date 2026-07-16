import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

export const packageRoot = resolve(fileURLToPath(new URL("../../..", import.meta.url)));
export const repoRoot = resolve(packageRoot, "..");
export const workspaceRoot = resolve(repoRoot, "..");
export const parityRoot = resolve(repoRoot, "tests/parity");
export const generatedRoot = resolve(parityRoot, "generated");
export const rustGeneratedRoot = resolve(generatedRoot, "rust");
export const typescriptGeneratedRoot = resolve(generatedRoot, "typescript");
export const goGeneratedRoot = resolve(generatedRoot, "go");
export const pythonGeneratedRoot = resolve(generatedRoot, "python");
export const resultsRoot = resolve(parityRoot, "results");
export const workspacesRoot = resolve(parityRoot, "workspaces");
