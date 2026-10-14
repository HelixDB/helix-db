/** Public npm-package contract: run only in the isolated package-smoke consumer. */
import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import { Client, HelixError, g, readBatch, writeBatch } from "@helix-db/helix-db";

const request = writeBatch()
  .varAs("asset", g().addN("Asset", { name: "updated" }))
  .toQueryRequest();
const cases = [
  { status: 200, body: '{"asset":[]}' },
  { status: 204, body: "" },
  {
    status: 409,
    body: '{"error":"transaction_conflict","msg":"concurrent edit"}',
    code: "transaction_conflict",
    message: "concurrent edit",
  },
  {
    status: 409,
    body: '{"error":"transaction_conflict","msg":"diagnostic wording changed"}',
    code: "transaction_conflict",
    message: "diagnostic wording changed",
  },
  {
    status: 503,
    body: '{"error":"writer_fenced_commit_outcome_unknown","msg":"unknown outcome","retryable":false}',
    code: "writer_fenced_commit_outcome_unknown",
    message: "unknown outcome",
    retryable: false,
  },
  {
    status: 503,
    body: '{"code":"WRITE_OUTCOME_UNKNOWN","error":"unknown outcome","retryable":false}',
    code: "WRITE_OUTCOME_UNKNOWN",
    message: "unknown outcome",
    retryable: false,
  },
  { status: 500, body: '{"error":"legacy diagnostic","code":"legacy_code"}', code: "legacy_code", message: "legacy diagnostic" },
  { status: 500, body: '{"error":"future_code","msg":"future diagnostic"}', code: "future_code", message: "future diagnostic" },
  { status: 500, body: '{"error":"message without a code"}', message: "message without a code" },
  { status: 502, body: "not JSON", message: "not JSON" },
  { dropResponse: true },
];

for (const fixture of cases) {
  let requests = 0;
  const server = createServer((req, res) => {
    requests += 1;
    req.resume();
    req.on("end", () => {
      if (fixture.dropResponse) return res.destroy();
      res.writeHead(fixture.status, { "content-type": "application/json" });
      res.end(fixture.body);
    });
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  try {
    const client = new Client(`http://127.0.0.1:${server.address().port}`);
    if (fixture.status === 200 || fixture.status === 204) {
      assert.deepEqual(await client.query(request).send(), fixture.status === 200 ? { asset: [] } : undefined);
    } else {
      await assert.rejects(client.query(request).send(), (error) => {
        assert.ok(error instanceof HelixError);
        assert.equal(error.kind, fixture.dropResponse ? "Network" : "Remote");
        assert.equal(error.code, fixture.code);
        assert.equal(error.statusCode, fixture.status);
        assert.equal(error.serverMessage, fixture.message);
        assert.equal(error.rawBody, fixture.body);
        assert.equal(error.retryable, fixture.retryable);
        assert.equal(error.isConflict(), fixture.status === 409);
        assert.equal(error.isRetryable(), false);
        return true;
      });
    }
    assert.equal(requests, 1, "the SDK must not replay a mutation");
  } finally {
    server.closeAllConnections();
    await new Promise((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
  }
}

const url = process.env.HELIX_PACKAGE_TEST_URL;
if (url !== undefined) {
  const client = new Client(url);
  let seed = writeBatch();
  for (let index = 0; index < 256; index++) {
    seed = seed.varAs(`asset_${index}`, g().addN("PackageConflictProbe", { value: 0n }));
  }
  await client.query(seed.toQueryRequest()).send();
  let conflicts = 0;
  let successes = 0;
  for (let round = 0; round < 3 && conflicts === 0; round++) {
    const results = await Promise.allSettled(
      Array.from({ length: 16 }, (_, index) =>
        new Client(url)
          .query(
            writeBatch()
              .varAs("assets", g().nWithLabel("PackageConflictProbe").setProperty("value", BigInt(index)))
              .toQueryRequest(),
          )
          .send(),
      ),
    );
    for (const result of results) {
      if (result.status === "fulfilled") {
        successes += 1;
        continue;
      }
      const error = result.reason;
      assert.ok(error instanceof HelixError);
      assert.equal(error.statusCode, 409);
      assert.equal(error.code, "transaction_conflict");
      assert.ok(error.serverMessage);
      assert.equal(JSON.parse(error.rawBody).msg, error.serverMessage);
      assert.ok(error.isConflict());
      conflicts += 1;
    }
  }
  assert.ok(conflicts > 0, "concurrent writes must exercise a real conflict");
  assert.ok(successes > 0, "at least one transaction must commit");
  const result = await client
    .query(
      readBatch()
        .varAs("assets", g().nWithLabel("PackageConflictProbe").valueMap(["value"]))
        .returning(["assets"])
        .toQueryRequest(),
    )
    .send();
  assert.equal(result.assets.length, 256);
  assert.equal(
    new Set(result.assets.map((asset) => String(asset.value))).size,
    1,
    "conflicted transactions must not leave partial updates",
  );
  console.log(JSON.stringify({ concurrentWrites: { successes, conflicts }, finalAssets: result.assets.length }));
}
