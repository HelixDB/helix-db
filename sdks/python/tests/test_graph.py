import json
import unittest

from helixdb import EdgeRef, NodeRef, SourcePredicate, g
from helixdb.graph import (
    BetweennessOptions,
    EDGE_SOURCE,
    EXTERNAL_ID,
    GraphMetadataSelection,
    GraphEdgeId,
    GraphSelection,
    IdentitySelection,
    LayoutOptions,
    LeidenOptions,
    LouvainOptions,
    NativeGraph,
    TraversalOptions,
    _decode_external_id,
    _decode_edge_id,
    _encode_edge_id,
    _encode_external_id,
    external_id_from_json,
    external_id_to_json,
)


class _Record:
    def __init__(self, **values):
        self.__dict__.update(values)


class _ExternalIdRecord:
    def __init__(self, *, encoded):
        self.encoded = encoded


class _EdgeIdRecord:
    def __init__(self, *, encoded):
        self.encoded = encoded


class _Module:
    NativeExternalId = _ExternalIdRecord
    NativeEdgeId = _EdgeIdRecord


class _NativeGraph:
    def node_count(self):
        return 2

    def edge_count(self):
        return 1

    def is_directed(self):
        return True

    def is_multigraph(self):
        return False

    def graph_attributes_json(self):
        return b'{"selection":"files"}'

    def contains_node(self, node_id):
        return _decode_external_id(node_id) in {"a", "b"}

    def contains_edge(self, edge_id):
        return _decode_edge_id(edge_id) == GraphEdgeId.original("e1")

    def nodes(self):
        return [
            _Record(
                id=_ExternalIdRecord(encoded=_encode_external_id("a")),
                label="File",
                attributes_json=b'{"path":"a.py"}',
            ),
            _Record(
                id=_ExternalIdRecord(encoded=_encode_external_id("b")),
                label="File",
                attributes_json=b'{"path":"b.py"}',
            ),
        ]

    def edges(self):
        return [
            _Record(
                id=_EdgeIdRecord(encoded=_encode_edge_id(GraphEdgeId.original("e1"))),
                source=_ExternalIdRecord(encoded=_encode_external_id("a")),
                target=_ExternalIdRecord(encoded=_encode_external_id("b")),
                graphify_key=_ExternalIdRecord(encoded=_encode_external_id("imports")),
                label="DEPENDS_ON",
                weight=1.0,
                attributes_json=b'{"line":3}',
            )
        ]

    def node(self, node_id):
        return self.nodes()[0] if _decode_external_id(node_id) == "a" else None

    def edge(self, edge_id):
        return (
            self.edges()[0]
            if _decode_edge_id(edge_id) == GraphEdgeId.original("e1")
            else None
        )

    def louvain_communities(self, resolution, threshold, seed, max_levels):
        return _Record(
            communities=[
                _Record(
                    id=_ExternalIdRecord(encoded=_encode_external_id((1, "1"))),
                    node_ids=[
                        _ExternalIdRecord(encoded=_encode_external_id((1, "1"))),
                        _ExternalIdRecord(encoded=_encode_external_id(b"node")),
                    ],
                )
            ],
            modularity=0.25,
            levels=2,
        )

    def leiden(
        self,
        resolution,
        randomness,
        seed,
        trials,
        max_iterations,
        max_levels,
    ):
        result = self.louvain_communities(resolution, 0, seed, max_levels)
        result.winning_trial = 1
        return result


class GraphSdkTests(unittest.TestCase):
    def selection(self, **overrides) -> GraphSelection:
        values = {
            "node_traversal": g().n_where(SourcePredicate.has_key("$id")),
            "edge_traversal": g().e_where(SourcePredicate.has_key("$id")),
            "kind": "digraph",
            "node_properties": ("path",),
            "edge_properties": ("line",),
            "node_identity": IdentitySelection.scalar_property("external_id"),
            "graphify_edge_key": IdentitySelection.scalar_property("key"),
            "weight_property": "weight",
            "max_nodes": 2,
            "max_edges": 3,
            "allow_full_scan": True,
        }
        values.update(overrides)
        return GraphSelection(**values)

    def test_selection_builds_one_query_with_private_aliases(self) -> None:
        request = json.loads(self.selection().to_query_request().to_json_string())
        payload = json.dumps(request)
        self.assertIn(EXTERNAL_ID, payload)
        self.assertIn(EDGE_SOURCE, payload)
        self.assertIn('"literal": 3', payload)
        self.assertIn('"literal": 4', payload)
        self.assertEqual(request["query"]["read"]["returns"], ["nodes", "edges"])

    def test_selection_adds_bounded_metadata_as_third_projection(self) -> None:
        request = json.loads(
            self.selection(
                metadata=GraphMetadataSelection(
                    g().n_where(SourcePredicate.has_key("graph_name")),
                    ("graph_name", "version", "graph_name"),
                )
            )
            .to_query_request()
            .to_json_string()
        )
        payload = json.dumps(request)
        self.assertEqual(
            request["query"]["read"]["returns"], ["nodes", "edges", "metadata"]
        )
        self.assertIn('"literal": 2', payload)
        self.assertEqual(payload.count('"alias": "graph_name"'), 1)

    def test_selection_rejects_invalid_properties_and_limits(self) -> None:
        with self.assertRaises(ValueError):
            self.selection(node_properties=("",))
        with self.assertRaises(ValueError):
            self.selection(edge_properties=("__helix_graph_collision",))
        with self.assertRaises(ValueError):
            self.selection(max_nodes=0)
        with self.assertRaises(ValueError):
            self.selection(kind="directed")
        with self.assertRaises(ValueError):
            self.selection(
                node_identity=IdentitySelection.tagged_property(
                    "__helix_graph_collision"
                )
            )
        with self.assertRaises(ValueError):
            self.selection(graphify_edge_key=IdentitySelection.internal_id())
        with self.assertRaises(ValueError):
            GraphMetadataSelection(g().n_where(SourcePredicate.has_key("name")), ())
        with self.assertRaises(ValueError):
            GraphMetadataSelection(
                g().n_where(SourcePredicate.has_key("name")),
                ("__helix_graph_collision",),
            )

    def test_selection_requires_explicit_full_scan_opt_in(self) -> None:
        with self.assertRaisesRegex(ValueError, "allow_full_scan"):
            self.selection(
                node_traversal=g().n(NodeRef.all()).has_label("File"),
                edge_traversal=g().e(EdgeRef.all()).has_label("DEPENDS_ON"),
                allow_full_scan=False,
            )

    def test_python_wrapper_keeps_attributes_lazy_and_delegates_to_native(self) -> None:
        graph = NativeGraph(_NativeGraph(), _Module())
        self.assertEqual(graph.node_count, 2)
        self.assertEqual(graph.edge_count, 1)
        self.assertTrue(graph.directed)
        self.assertEqual(graph.attributes, {"selection": "files"})
        self.assertEqual(graph.node("a").attributes, {"path": "a.py"})
        self.assertEqual(graph.edge("e1").attributes, {"line": 3})
        self.assertTrue(graph.contains_node("b"))
        self.assertTrue(graph.contains_edge("e1"))
        self.assertEqual(graph.louvain_communities().communities[0].id, (1, "1"))
        leiden = graph.leiden(LeidenOptions(trials=2))
        self.assertEqual(leiden.communities[0].node_ids, ((1, "1"), b"node"))
        self.assertEqual(leiden.winning_trial, 1)

    def test_structural_edge_identity_round_trips_without_string_collisions(
        self,
    ) -> None:
        original = GraphEdgeId("reverse(e1)")
        reverse = GraphEdgeId("e1", 1)
        self.assertNotEqual(original, reverse)
        for edge_id in (original, reverse, GraphEdgeId("e1", 42)):
            record = _EdgeIdRecord(encoded=_encode_edge_id(edge_id))
            self.assertEqual(_decode_edge_id(record), edge_id)
            self.assertEqual(
                GraphEdgeId.from_json(json.loads(json.dumps(edge_id.to_json()))),
                edge_id,
            )
        with self.assertRaises(ValueError):
            GraphEdgeId("", 0)

    def test_algorithm_options_reject_invalid_states_before_ffi(self) -> None:
        with self.assertRaises(ValueError):
            BetweennessOptions(sample_count=0)
        with self.assertRaises(ValueError):
            TraversalOptions(seeds=(), max_depth=1)
        with self.assertRaises(ValueError):
            LouvainOptions(resolution=0)
        with self.assertRaises(ValueError):
            LeidenOptions(randomness=0)
        with self.assertRaises(ValueError):
            LeidenOptions(trials=0)
        with self.assertRaises(ValueError):
            LayoutOptions(k=float("nan"))

    def test_external_identity_codec_is_typed_canonical_and_recursive(self) -> None:
        values = (
            None,
            True,
            1,
            10**100,
            -0.0,
            "",
            b"\x00\xff",
            (1, "1", b"1"),
            frozenset({"a", "b"}),
        )
        for value in values:
            record = _ExternalIdRecord(encoded=_encode_external_id(value))
            decoded = _decode_external_id(record)
            if isinstance(value, float):
                self.assertEqual(
                    _encode_external_id(decoded), _encode_external_id(value)
                )
            else:
                self.assertEqual(decoded, value)
            json_value = json.loads(json.dumps(external_id_to_json(value)))
            self.assertEqual(
                external_id_to_json(external_id_from_json(json_value)), json_value
            )
        self.assertNotEqual(_encode_external_id(1), _encode_external_id("1"))
        self.assertNotEqual(_encode_external_id(True), _encode_external_id("true"))


if __name__ == "__main__":
    unittest.main()
