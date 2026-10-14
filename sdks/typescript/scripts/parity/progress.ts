/** Local opt-in diagnostics remain separate from fixture result files. */
export function parityProgress(stage: string): void {
  if (process.env.HELIX_PARITY_PROGRESS === "1") process.stderr.write(`parity progress: ${stage}\n`);
}
