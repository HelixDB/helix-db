from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from time import monotonic, sleep

PYTHON_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PYTHON_ROOT / "src"))

import cypher_parity  # noqa: E402
from parity_runtime_fixtures import (  # noqa: E402
    base_runtime_fixtures,
    node_permutation_fixtures,
)

from helixdb import (  # noqa: E402
    AsyncClient,
    Client,
    Disk,
    EmbeddedCacheConfig,
    FoundPath,
    GraphEdgeId,
    GraphMetadataSelection,
    GraphSelection,
    HelixError,
    IdentitySelection,
    InMemory,
    LeidenOptions,
    MemoryCache,
    NodeRef,
    NoPath,
    PropertyInput,
    PropertyValue,
    QueryRequest,
    SourcePredicate,
    TraversalOptions,
    external_id_from_json,
    external_id_to_json,
    g,
    read_batch,
    write_batch,
)

TRANSACTION_CONFLICT_ATTEMPTS = 8
TRANSACTION_CONFLICT_MESSAGE = "Storage error: Transaction error: transaction conflict"


def main() -> None:
    results_value = os.environ.get("HELIX_EMBEDDED_PARITY_RESULTS")
    if results_value is None:
        raise RuntimeError("HELIX_EMBEDDED_PARITY_RESULTS is required")
    results = Path(results_value)
    results.mkdir(parents=True, exist_ok=True)
    for path in results.glob("*.json"):
        path.unlink()

    database = os.environ.get("HELIX_EMBEDDED_PARITY_DATABASE", "python-sdk-embedded-parity")
    storage = os.environ.get("HELIX_EMBEDDED_PARITY_STORAGE", "memory")
    if storage == "memory":
        source = InMemory(database)
    elif storage == "disk":
        disk_root = os.environ.get("HELIX_EMBEDDED_PARITY_DISK_ROOT")
        if disk_root is None:
            raise RuntimeError("HELIX_EMBEDDED_PARITY_DISK_ROOT is required for disk parity")
        source = Disk(disk_root, database)
    else:
        raise RuntimeError(f"unsupported embedded parity storage {storage}")
    cache = EmbeddedCacheConfig(256 * 1024 * 1024, MemoryCache())
    fixtures = sorted(
        [
            *base_runtime_fixtures(),
            *node_permutation_fixtures(),
        ],
        key=lambda fixture: fixture[0],
    )
    client = Client.embedded(source, cache=cache)
    try:
        for name, request in fixtures:
            if storage == "disk" and name == "900-write-active-text-items":
                client.close()
                reader = Client.embedded_reader(source, cache=cache)
                try:
                    for search_name in (
                        "025-read-text-search-nodes",
                        "027-read-text-search-edges",
                    ):
                        search_request = _required_fixture(fixtures, search_name)
                        actual = reader.query(search_request)
                        expected = json.loads(
                            (results / f"{search_name}.json").read_text(encoding="utf-8")
                        )
                        if actual != expected:
                            raise RuntimeError(
                                f"{search_name} changed after reopening a disk reader"
                            )
                finally:
                    reader.close()
                client = Client.embedded(source, cache=cache)
            for attempt in range(TRANSACTION_CONFLICT_ATTEMPTS):
                try:
                    response = client.query(request)
                    break
                except HelixError as error:
                    is_transaction_conflict = (
                        error.kind == "Embedded"
                        and error.details is not None
                        and TRANSACTION_CONFLICT_MESSAGE in error.details
                    )
                    if not is_transaction_conflict or attempt + 1 == TRANSACTION_CONFLICT_ATTEMPTS:
                        raise
                    # Embedded storage reports retryable transaction conflicts in
                    # the error details; the losing transaction did not commit.
                    sleep(0.01 * 2**attempt)
            _await_index_operations(client, response)
            (results / f"{name}.json").write_text(
                json.dumps(_normalize_operation_ids(response), separators=(",", ":")),
                encoding="utf-8",
            )

        for search_name in (
            "025-read-text-search-nodes",
            "027-read-text-search-edges",
        ):
            try:
                client.query(_required_fixture(fixtures, search_name))
            except HelixError as error:
                if "index_not_found" not in str(error):
                    raise RuntimeError(
                        f"{search_name} returned the wrong post-DROP error: {error}"
                    ) from error
            else:
                raise RuntimeError(f"{search_name} unexpectedly succeeded after index DROP")

        graph = client.graph(
            GraphSelection(
                node_traversal=g().n_with_label("ParityUser"),
                edge_traversal=g().e_with_label("FOLLOWS"),
                kind="digraph",
                node_identity=IdentitySelection.scalar_property("externalId"),
                edge_properties=("since",),
                weight_property="weight",
                max_nodes=3,
                max_edges=2,
                allow_full_scan=True,
            )
        )
        assert graph.node_count == 3
        assert graph.edge_count == 2
        assert {(edge.source, edge.target) for edge in graph.edges()} == {
            ("user-alice", "user-bob"),
            ("user-bob", "user-carol"),
        }
        _native_graph_acceptance(client)
    finally:
        client.close()
    cypher_parity.run_embedded(source, cache, results)


async def async_main() -> None:
    """Run the complete executable corpus through the asynchronous client."""

    results_value = os.environ.get("HELIX_EMBEDDED_PARITY_RESULTS")
    if results_value is None:
        raise RuntimeError("HELIX_EMBEDDED_PARITY_RESULTS is required")
    results = Path(results_value)
    results.mkdir(parents=True, exist_ok=True)
    for path in results.glob("*.json"):
        path.unlink()

    database = os.environ.get("HELIX_EMBEDDED_PARITY_DATABASE", "python-async-sdk-embedded-parity")
    storage = os.environ.get("HELIX_EMBEDDED_PARITY_STORAGE", "memory")
    if storage == "memory":
        source = InMemory(database)
    elif storage == "disk":
        disk_root = os.environ.get("HELIX_EMBEDDED_PARITY_DISK_ROOT")
        if disk_root is None:
            raise RuntimeError("HELIX_EMBEDDED_PARITY_DISK_ROOT is required for disk parity")
        source = Disk(disk_root, database)
    else:
        raise RuntimeError(f"unsupported embedded parity storage {storage}")

    cache = EmbeddedCacheConfig(256 * 1024 * 1024, MemoryCache())
    fixtures = sorted(
        [*base_runtime_fixtures(), *node_permutation_fixtures()],
        key=lambda fixture: fixture[0],
    )
    client = await AsyncClient.embedded(source, cache=cache)
    try:
        for name, request in fixtures:
            if storage == "disk" and name == "900-write-active-text-items":
                await client.close()
                reader = await AsyncClient.embedded_reader(source, cache=cache)
                try:
                    for search_name in (
                        "025-read-text-search-nodes",
                        "027-read-text-search-edges",
                    ):
                        search_request = _required_fixture(fixtures, search_name)
                        actual = await reader.query(search_request)
                        expected = json.loads(
                            (results / f"{search_name}.json").read_text(encoding="utf-8")
                        )
                        if actual != expected:
                            raise RuntimeError(
                                f"{search_name} changed after reopening an async disk reader"
                            )
                finally:
                    await reader.close()
                client = await AsyncClient.embedded(source, cache=cache)

            response = await _query_with_conflict_retry_async(client, request)
            await _await_index_operations_async(client, response)
            (results / f"{name}.json").write_text(
                json.dumps(_normalize_operation_ids(response), separators=(",", ":")),
                encoding="utf-8",
            )

        for search_name in (
            "025-read-text-search-nodes",
            "027-read-text-search-edges",
        ):
            try:
                await client.query(_required_fixture(fixtures, search_name))
            except HelixError as error:
                if "index_not_found" not in str(error):
                    raise RuntimeError(
                        f"{search_name} returned the wrong post-DROP error: {error}"
                    ) from error
            else:
                raise RuntimeError(f"{search_name} unexpectedly succeeded after index DROP")

        overlap_request = _required_fixture(fixtures, "002-read-count-all-users")
        overlap_results = await asyncio.gather(*(client.query(overlap_request) for _ in range(8)))
        if any(result != overlap_results[0] for result in overlap_results[1:]):
            raise RuntimeError("overlapping async embedded reads returned different results")
    finally:
        await client.close()

    await cypher_parity.run_embedded_async(source, cache, results)
    concurrency_client = await AsyncClient.embedded(InMemory(f"{database}-concurrency"))
    try:
        writes = [
            QueryRequest.write(
                write_batch()
                .var_as(
                    "created",
                    g().add_n(
                        "AsyncParityConcurrent",
                        {"sequence": sequence},
                    ),
                )
                .returning(["created"])
            )
            for sequence in range(8)
        ]
        await asyncio.gather(
            *(_query_with_conflict_retry_async(concurrency_client, request) for request in writes)
        )
        concurrent_read = QueryRequest.read(
            read_batch()
            .var_as(
                "count",
                g().n_with_label("AsyncParityConcurrent").count(),
            )
            .returning(["count"])
        )
        reads = await asyncio.gather(*(concurrency_client.query(concurrent_read) for _ in range(8)))
        if any(result != reads[0] for result in reads[1:]):
            raise RuntimeError("overlapping async embedded reads were inconsistent")
    finally:
        await concurrency_client.close()


async def _query_with_conflict_retry_async(client: AsyncClient, request: QueryRequest) -> object:
    for attempt in range(TRANSACTION_CONFLICT_ATTEMPTS):
        try:
            return await client.query(request)
        except HelixError as error:
            is_transaction_conflict = (
                error.kind == "Embedded"
                and error.details is not None
                and TRANSACTION_CONFLICT_MESSAGE in error.details
            )
            if not is_transaction_conflict or attempt + 1 == TRANSACTION_CONFLICT_ATTEMPTS:
                raise
            await asyncio.sleep(0.01 * 2**attempt)
    raise AssertionError("async transaction conflict retry loop exhausted")


async def _await_index_operations_async(client: AsyncClient, response: object) -> None:
    for operation_id in sorted(_collect_operation_ids(response)):
        deadline = monotonic() + 60.0
        while True:
            status_response = await client.query(
                QueryRequest.read(
                    read_batch()
                    .var_as("status", g().get_index_operation(operation_id))
                    .returning(["status"])
                )
            )
            status = status_response.get("status", {}).get("status")
            if status == "succeeded":
                break
            if status not in {"queued", "running"}:
                raise RuntimeError(
                    f"operation {operation_id} reached unexpected status {status}: "
                    f"{status_response}"
                )
            if monotonic() >= deadline:
                raise TimeoutError(f"operation {operation_id} did not finish within 60s")
            await asyncio.sleep(0.01)


def _required_fixture(fixtures: list[tuple[str, QueryRequest]], name: str) -> QueryRequest:
    for fixture_name, request in fixtures:
        if fixture_name == name:
            return request
    raise RuntimeError(f"missing fixture {name}")


def _await_index_operations(client: Client, response: object) -> None:
    """Wait for asynchronous DDL receipts before later fixtures use their indexes."""

    for operation_id in sorted(_collect_operation_ids(response)):
        deadline = monotonic() + 60.0
        while True:
            status_response = client.query(
                QueryRequest.read(
                    read_batch()
                    .var_as("status", g().get_index_operation(operation_id))
                    .returning(["status"])
                )
            )
            status = status_response.get("status", {}).get("status")
            if status == "succeeded":
                break
            if status not in {"queued", "running"}:
                raise RuntimeError(
                    f"operation {operation_id} reached unexpected status {status}: "
                    f"{status_response}"
                )
            if monotonic() >= deadline:
                raise TimeoutError(f"operation {operation_id} did not finish within 60s")
            sleep(0.01)


def _collect_operation_ids(value: object, ids: set[str] | None = None) -> set[str]:
    """Collect operation IDs only from DDL receipt objects."""

    ids = set() if ids is None else ids
    if isinstance(value, list):
        for entry in value:
            _collect_operation_ids(entry, ids)
    elif isinstance(value, dict):
        if value.get("kind") in {"accepted", "existing_operation"} and isinstance(
            value.get("operation_id"), str
        ):
            ids.add(value["operation_id"])
        for entry in value.values():
            _collect_operation_ids(entry, ids)
    return ids


def _normalize_operation_ids(value: object) -> object:
    """Replace random operation UUIDs while retaining the compared receipt shape."""

    if isinstance(value, list):
        return [_normalize_operation_ids(entry) for entry in value]
    if not isinstance(value, dict):
        return value
    normalized = {key: _normalize_operation_ids(entry) for key, entry in value.items()}
    if normalized.get("kind") in {"accepted", "existing_operation"} and (
        "operation_id" in normalized
    ):
        normalized["operation_id"] = "<operation-id>"
    return normalized


def _native_graph_acceptance(client: Client) -> None:
    batch = write_batch()
    returned: list[str] = []
    nodes = [
        (
            "native_metadata",
            "NativeGraphMetadata",
            {"owner": "graphify", "version": 7},
        ),
        (
            "typed_a",
            "NativeTypedNode",
            {
                "graphScope": "typed",
                "taggedIdentity": external_id_to_json(("typed", 1)),
                "color": "red",
            },
        ),
        (
            "typed_b",
            "NativeTypedNode",
            {
                "graphScope": "typed",
                "taggedIdentity": external_id_to_json(b"\x00\xff"),
                "color": "blue",
            },
        ),
        (
            "scalar_int",
            "NativeScalarNode",
            {"graphScope": "scalar", "scalarIdentity": 1},
        ),
        (
            "scalar_string",
            "NativeScalarNode",
            {"graphScope": "scalar", "scalarIdentity": "1"},
        ),
        *[
            (
                f"filter_{name}",
                "NativeFilterNode",
                {"graphScope": "filter", "externalId": f"filter-{name}"},
            )
            for name in ("a", "b", "c")
        ],
        *[
            (
                f"leiden_{name}",
                "NativeLeidenNode",
                {"graphScope": "leiden", "externalId": name},
            )
            for name in ("a", "b", "c", "d", "e", "f")
        ],
    ]
    for variable, label, properties in nodes:
        batch = batch.var_as(
            variable,
            g().add_n(
                label,
                [(name, PropertyInput.value(value)) for name, value in properties.items()],
            ),
        )
        returned.append(variable)

    edges = [
        (
            "typed_rel_a",
            "typed_a",
            "typed_b",
            "REL_A",
            {
                "graphScope": "typed",
                "edgeKey": external_id_to_json(frozenset({"first", 1})),
                "generation": 1,
                "weight": PropertyValue.f64(2.0),
            },
        ),
        (
            "typed_rel_b",
            "typed_a",
            "typed_b",
            "REL_B",
            {
                "graphScope": "typed",
                "edgeKey": external_id_to_json(10**100),
                "generation": 2,
                "weight": PropertyValue.f64(3.0),
            },
        ),
        (
            "scalar_rel",
            "scalar_int",
            "scalar_string",
            "SCALAR_REL",
            {"graphScope": "scalar"},
        ),
        (
            "filter_allowed",
            "filter_a",
            "filter_b",
            "ALLOWED",
            {"graphScope": "filter"},
        ),
        (
            "filter_blocked",
            "filter_b",
            "filter_c",
            "BLOCKED",
            {"graphScope": "filter"},
        ),
        *[
            (
                f"leiden_edge_{index}",
                f"leiden_{source}",
                f"leiden_{target}",
                "COMMUNITY_LINK",
                {
                    "graphScope": "leiden",
                    "weight": PropertyValue.f64(weight),
                },
            )
            for index, (source, target, weight) in enumerate(
                (
                    ("a", "b", 2.0),
                    ("b", "c", 2.0),
                    ("c", "a", 2.0),
                    ("d", "e", 2.0),
                    ("e", "f", 2.0),
                    ("f", "d", 2.0),
                    ("c", "d", 0.1),
                )
            )
        ],
    ]
    for variable, source, target, label, properties in edges:
        batch = batch.var_as(
            variable,
            g()
            .n(NodeRef.var(source))
            .add_e(
                label,
                NodeRef.var(target),
                [(name, PropertyInput.value(value)) for name, value in properties.items()],
            ),
        )
        returned.append(variable)
    client.query(QueryRequest.write(batch.returning(returned)))

    for kind, directed, multigraph in (
        ("graph", False, False),
        ("digraph", True, False),
        ("multigraph", False, True),
        ("multidigraph", True, True),
    ):
        declared = client.graph(
            GraphSelection(
                node_traversal=g().n_with_label("ParityUser"),
                edge_traversal=g().e_with_label("FOLLOWS"),
                kind=kind,
                node_identity=IdentitySelection.scalar_property("externalId"),
            )
        )
        assert declared.directed is directed
        assert declared.multigraph is multigraph

    empty = client.graph(
        GraphSelection(
            node_traversal=g().n_with_label("MissingNativeGraphNode"),
            edge_traversal=g().e_with_label("MissingNativeGraphEdge"),
            kind="multigraph",
        )
    )
    assert empty.node_count == 0 and empty.edge_count == 0 and empty.multigraph

    scalar = client.graph(
        GraphSelection(
            node_traversal=g().n_where(SourcePredicate.eq("graphScope", "scalar")),
            edge_traversal=g().e_where(SourcePredicate.eq("graphScope", "scalar")),
            kind="graph",
            node_identity=IdentitySelection.scalar_property("scalarIdentity"),
        )
    )
    assert {node.id for node in scalar.nodes()} == {1, "1"}

    typed_selection = GraphSelection(
        node_traversal=g().n_where(SourcePredicate.eq("graphScope", "typed")),
        edge_traversal=g().e_where(SourcePredicate.eq("graphScope", "typed")),
        kind="multigraph",
        metadata=GraphMetadataSelection(
            g().n_with_label("NativeGraphMetadata"), ("owner", "version")
        ),
        node_properties=("color",),
        edge_properties=("generation",),
        node_identity=IdentitySelection.tagged_property("taggedIdentity"),
        graphify_edge_key=IdentitySelection.tagged_property("edgeKey"),
        weight_property="weight",
    )
    typed = client.graph(typed_selection)
    assert {node.id for node in typed.nodes()} == {("typed", 1), b"\x00\xff"}
    assert {edge.graphify_key for edge in typed.edges()} == {
        frozenset({"first", 1}),
        10**100,
    }
    assert typed.attributes == {"owner": "graphify", "version": 7}
    assert typed.copy().attributes == typed.attributes
    assert typed.induced_subgraph((("typed", 1), b"\x00\xff")).attributes == typed.attributes
    assert typed.degree(("typed", 1)).node_id == ("typed", 1)
    assert {degree.node_id for degree in typed.degrees()} == {
        ("typed", 1),
        b"\x00\xff",
    }
    assert {score.node_id for score in typed.betweenness_centrality()} == {
        ("typed", 1),
        b"\x00\xff",
    }
    assert {score.graphify_key for score in typed.edge_betweenness_centrality()} == {
        frozenset({"first", 1}),
        10**100,
    }
    cycles = typed.simple_cycles(2)
    assert len(cycles.cycles) == 1
    assert set(cycles.cycles[0].node_ids) == {("typed", 1), b"\x00\xff"}
    assert all(isinstance(edge_id, GraphEdgeId) for edge_id in cycles.cycles[0].edge_ids)
    assert {position.node_id for position in typed.spring_layout()} == {
        ("typed", 1),
        b"\x00\xff",
    }
    directed = typed.to_directed()
    assert directed.directed and directed.multigraph and directed.edge_count == 4
    assert directed.attributes == typed.attributes
    assert {edge.id.reverse_generation for edge in directed.edges()} == {0, 1}
    undirected = directed.to_undirected()
    assert not undirected.directed and undirected.multigraph
    assert undirected.attributes == typed.attributes
    assert {edge.attributes["generation"] for edge in undirected.edges()} == {1, 2}

    try:
        client.graph(
            GraphSelection(
                node_traversal=typed_selection.node_traversal,
                edge_traversal=typed_selection.edge_traversal,
                kind="graph",
                node_identity=typed_selection.node_identity,
            )
        )
    except HelixError as error:
        assert "does not permit parallel edges" in str(error).lower()
    else:
        raise AssertionError("simple graph accepted duplicate endpoint pairs")

    leiden_graph = client.graph(
        GraphSelection(
            node_traversal=g().n_where(SourcePredicate.eq("graphScope", "leiden")),
            edge_traversal=g().e_where(SourcePredicate.eq("graphScope", "leiden")),
            kind="graph",
            node_identity=IdentitySelection.scalar_property("externalId"),
            weight_property="weight",
        )
    )
    leiden = leiden_graph.leiden(LeidenOptions(seed=42, trials=1))
    assert tuple(community.node_ids for community in leiden.communities) == (
        ("a", "b", "c"),
        ("d", "e", "f"),
    )
    assert abs(leiden.modularity - 0.4917355371900826) < 1e-12
    assert leiden_graph.attributes == {}

    filter_graph = client.graph(
        GraphSelection(
            node_traversal=g().n_where(SourcePredicate.eq("graphScope", "filter")),
            edge_traversal=g().e_where(SourcePredicate.eq("graphScope", "filter")),
            kind="graph",
            node_identity=IdentitySelection.scalar_property("externalId"),
        )
    )
    traversal = filter_graph.traverse(
        TraversalOptions(seeds=("filter-a",), max_depth=3, allowed_labels=("ALLOWED",))
    )
    assert tuple(visit.node_id for visit in traversal.visits) == (
        "filter-a",
        "filter-b",
    )
    assert isinstance(
        filter_graph.shortest_path("filter-a", "filter-c", allowed_labels=("ALLOWED",)),
        NoPath,
    )
    found = filter_graph.shortest_path(
        "filter-a", "filter-c", allowed_labels=("ALLOWED", "BLOCKED")
    )
    assert isinstance(found, FoundPath)
    assert found.node_ids == ("filter-a", "filter-b", "filter-c")

    identities = (
        None,
        True,
        10**100,
        -0.0,
        "identity",
        b"\x00\xff",
        (1, "1", b"1"),
        frozenset({"a", "b"}),
    )
    for identity in identities:
        encoded = external_id_to_json(identity)
        decoded = external_id_from_json(json.loads(json.dumps(encoded)))
        assert external_id_to_json(decoded) == encoded
    edge_id = GraphEdgeId("reverse(user-value)", 3)
    assert GraphEdgeId.from_json(json.loads(json.dumps(edge_id.to_json()))) == edge_id
    request_json = typed_selection.to_query_request().to_json_string()
    assert json.loads(json.dumps(json.loads(request_json))) == json.loads(request_json)


if __name__ == "__main__":
    if os.environ.get("HELIX_PYTHON_PARITY_MODE", "sync") == "async":
        asyncio.run(async_main())
    else:
        main()
