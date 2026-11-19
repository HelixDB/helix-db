"""Installed-wheel smoke test for the embedded Python runtime."""

from __future__ import annotations

import importlib.metadata

import helixdb_uniffi
from helixdb import (
    Client,
    FoundPath,
    GraphSelection,
    IdentitySelection,
    InMemory,
    NodeRef,
    QueryRequest,
    g,
    write_batch,
)


assert importlib.metadata.version("helix-db") == "0.4.0"
assert importlib.metadata.version("helix-db-embedded") == "0.3.3"
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
    request = QueryRequest.write(
        write_batch()
        .var_as("alice", g().add_n("WheelUser", {"externalId": "alice"}))
        .var_as("bob", g().add_n("WheelUser", {"externalId": "bob"}))
        .var_as(
            "follows",
            g().n(NodeRef.var("alice")).add_e("FOLLOWS", NodeRef.var("bob")),
        )
        .returning(["alice", "bob", "follows"])
    )
    response = client.query(request)
    assert set(response) == {"alice", "bob", "follows"}

    graph = client.graph(
        GraphSelection(
            node_traversal=g().n_with_label("WheelUser"),
            edge_traversal=g().e_with_label("FOLLOWS"),
            kind="digraph",
            node_identity=IdentitySelection.scalar_property("externalId"),
        )
    )
    assert graph.node_count == 2
    assert graph.edge_count == 1
    assert graph.successors("alice") == ("bob",)
    path = graph.shortest_path("alice", "bob", direction="out")
    assert isinstance(path, FoundPath)
    assert path.node_ids == ("alice", "bob")
finally:
    client.close()
