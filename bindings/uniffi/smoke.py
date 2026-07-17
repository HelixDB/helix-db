"""Installed-wheel smoke test for the embedded Python runtime."""

from __future__ import annotations

import importlib.metadata

import helixdb_uniffi
from helixdb import Client, InMemory, QueryRequest, g, read_batch


assert importlib.metadata.version("helix-db") == "0.2.0b1"
assert importlib.metadata.version("helix-db-embedded") == "0.2.0b1"
for name in (
    "HelixDb",
    "HelixDbSource",
    "NativeGraph",
    "NativeGraphLoadSpec",
    "graph_from_query_response",
):
    assert hasattr(helixdb_uniffi, name), name

client = Client.embedded(InMemory("python-wheel-smoke"))
try:
    request = QueryRequest.read(
        read_batch()
        .var_as("nodes", g().n_with_label("Missing").count())
        .returning(["nodes"])
    )
    assert client.query(request) == {"nodes": 0}
finally:
    client.close()
