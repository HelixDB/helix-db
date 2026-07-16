from __future__ import annotations

import json
import os
from pathlib import Path
import sys

PYTHON_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PYTHON_ROOT / "src"))

from helixdb import (  # noqa: E402
    Client,
    FoundPath,
    GraphEdgeId,
    GraphMetadataSelection,
    GraphSelection,
    HelixError,
    IdentitySelection,
    InMemory,
    LeidenOptions,
    NoPath,
    NodeRef,
    PropertyInput,
    PropertyValue,
    QueryRequest,
    SourcePredicate,
    TraversalOptions,
    external_id_from_json,
    external_id_to_json,
    g,
    write_batch,
)
from parity_runtime_fixtures import (  # noqa: E402
    base_runtime_fixtures,
    node_permutation_fixtures,
)


def main() -> None:
    results_value = os.environ.get("HELIX_EMBEDDED_PARITY_RESULTS")
    if results_value is None:
        raise RuntimeError("HELIX_EMBEDDED_PARITY_RESULTS is required")
    results = Path(results_value)
    results.mkdir(parents=True, exist_ok=True)
    for path in results.glob("*.json"):
        path.unlink()

    client = Client.embedded(
        InMemory(
            os.environ.get(
                "HELIX_EMBEDDED_PARITY_DATABASE", "python-sdk-embedded-parity"
            )
        )
    )
    try:
        for name, request in [
            *base_runtime_fixtures(),
            *node_permutation_fixtures(),
        ]:
            response = client.query(request)
            (results / f"{name}.json").write_text(
                json.dumps(response, separators=(",", ":")), encoding="utf-8"
            )

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
                [
                    (name, PropertyInput.value(value))
                    for name, value in properties.items()
                ],
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
                [
                    (name, PropertyInput.value(value))
                    for name, value in properties.items()
                ],
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
    assert (
        typed.induced_subgraph((("typed", 1), b"\x00\xff")).attributes
        == typed.attributes
    )
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
    assert all(
        isinstance(edge_id, GraphEdgeId) for edge_id in cycles.cycles[0].edge_ids
    )
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
    main()
