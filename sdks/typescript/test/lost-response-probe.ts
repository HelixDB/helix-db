import assert from "node:assert/strict";
import { Client, HelixError, QueryRequest, g, writeBatch } from "../src/index.js";

const baseUrl = process.env.HELIX_LOST_RESPONSE_GATEWAY_URL;
const probeId = process.env.HELIX_LOST_RESPONSE_PROBE_ID;

if (baseUrl !== undefined) {
  assert.ok(probeId, "lost-response probe ID is required");
  const client = new Client(baseUrl).withApiKey(process.env.HELIX_LOST_RESPONSE_API_KEY);
  const request = QueryRequest.write(
    writeBatch()
      .varAs("created", g().addN("LostResponseProbe", { business_id: probeId }))
      .returning(["created"]),
  );
  try {
    await client.query(request).send();
    assert.fail("lost response must be terminal");
  } catch (error) {
    assert.ok(error instanceof HelixError);
    assert.equal(error.statusCode, 503);
    assert.equal(error.code, "WRITE_OUTCOME_UNKNOWN");
    assert.equal(error.retryable, false);
    assert.equal(error.isConflict(), false);
    assert.equal(error.isRetryable(), false);
  }
}
