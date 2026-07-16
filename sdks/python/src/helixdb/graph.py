"""Native immutable graph loading and algorithm wrappers.

The database is queried once by :meth:`Client.graph`. Every method on the
returned graph executes in Rust over the retained immutable topology.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
import json
import math
import struct
from typing import Any, Literal, TypeAlias, Union

from .client import Client, HelixError
from .dsl import Projection, QueryRequest, read_batch

PRIVATE_PREFIX = "__helix_graph_"
NODE_ID = "__helix_graph_node_id"
EXTERNAL_ID = "__helix_graph_external_id"
NODE_LABEL = "__helix_graph_node_label"
EDGE_ID = "__helix_graph_edge_id"
EDGE_KEY = "__helix_graph_edge_key"
EDGE_SOURCE = "__helix_graph_edge_source"
EDGE_TARGET = "__helix_graph_edge_target"
EDGE_LABEL = "__helix_graph_edge_label"
EDGE_WEIGHT = "__helix_graph_edge_weight"

GraphKind = Literal["graph", "digraph", "multigraph", "multidigraph"]
TraversalDirection = Literal["out", "in", "both"]
EdgeTraversalDirection = Literal["forward", "reverse"]
DegreeKind = Literal["in", "out", "total"]
ExternalId: TypeAlias = Union[
    None,
    bool,
    int,
    float,
    str,
    bytes,
    tuple["ExternalId", ...],
    frozenset["ExternalId"],
]


@dataclass(frozen=True)
class GraphEdgeId:
    """Structural identity for an original edge or synthesized reversal."""

    stored_id: str
    reverse_generation: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.stored_id, str) or not self.stored_id:
            raise ValueError("stored edge ID must be a non-empty string")
        _non_negative_int(self.reverse_generation, "reverse_generation")

    @classmethod
    def original(cls, stored_id: str) -> "GraphEdgeId":
        return cls(stored_id)

    def to_json(self) -> dict[str, Any]:
        return {
            "stored_id": self.stored_id,
            "reverse_generation": self.reverse_generation,
        }

    @classmethod
    def from_json(cls, value: Any) -> "GraphEdgeId":
        if not isinstance(value, dict) or set(value) != {
            "stored_id",
            "reverse_generation",
        }:
            raise ValueError("edge identity fields are not canonical")
        return cls(value["stored_id"], value["reverse_generation"])


@dataclass(frozen=True)
class IdentitySelection:
    """Lossless source for external node identities or Graphify edge keys."""

    mode: Literal["internal_id", "scalar_property", "tagged_property"]
    property: str | None = None

    def __post_init__(self) -> None:
        if self.mode == "internal_id":
            if self.property is not None:
                raise ValueError("internal identity must not specify a property")
            return
        if self.mode not in {"scalar_property", "tagged_property"}:
            raise ValueError("invalid identity selection mode")
        _property_name(self.property)

    @classmethod
    def internal_id(cls) -> "IdentitySelection":
        return cls("internal_id")

    @classmethod
    def scalar_property(cls, property: str) -> "IdentitySelection":
        return cls("scalar_property", property)

    @classmethod
    def tagged_property(cls, property: str) -> "IdentitySelection":
        return cls("tagged_property", property)


@dataclass(frozen=True)
class GraphMetadataSelection:
    """One traversal row whose projected properties become graph attributes."""

    traversal: Any
    properties: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.properties:
            raise ValueError("graph metadata selection requires at least one property")
        for name in self.properties:
            _property_name(name)


@dataclass(frozen=True)
class GraphSelection:
    """Typed inputs for the one graph-loading read batch."""

    node_traversal: Any
    edge_traversal: Any
    kind: GraphKind
    metadata: GraphMetadataSelection | None = None
    node_properties: tuple[str, ...] = ()
    edge_properties: tuple[str, ...] = ()
    node_identity: IdentitySelection = field(
        default_factory=IdentitySelection.internal_id
    )
    graphify_edge_key: IdentitySelection | None = None
    weight_property: str | None = None
    max_nodes: int | None = None
    max_edges: int | None = None
    allow_full_scan: bool = False

    def __post_init__(self) -> None:
        if self.kind not in {"graph", "digraph", "multigraph", "multidigraph"}:
            raise ValueError("invalid graph kind")
        if (
            self.graphify_edge_key is not None
            and self.graphify_edge_key.mode == "internal_id"
        ):
            raise ValueError("Graphify edge keys must be selected from a property")
        properties = (
            *self.node_properties,
            *self.edge_properties,
            self.weight_property,
        )
        for name in properties:
            if name is None:
                continue
            if not name:
                raise ValueError("graph property names must not be empty")
            if name.startswith(PRIVATE_PREFIX):
                raise ValueError(f"graph property uses reserved prefix: {name}")
        for name, limit in (
            ("max_nodes", self.max_nodes),
            ("max_edges", self.max_edges),
        ):
            if limit is not None and (
                isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0
            ):
                raise ValueError(f"{name} must be a positive integer")
        starts = (
            self.node_traversal.into_steps()[:1],
            self.edge_traversal.into_steps()[:1],
            *(
                ()
                if self.metadata is None
                else (self.metadata.traversal.into_steps()[:1],)
            ),
        )
        if not self.allow_full_scan and any(
            steps
            and steps[0].variant in {"N", "E"}
            and getattr(steps[0].payload, "variant", None) == "All"
            for steps in starts
        ):
            raise ValueError("full graph scans require allow_full_scan=True")

    def to_query_request(self) -> QueryRequest:
        node_projection = [
            Projection.property("$id", NODE_ID),
            Projection.property(
                self.node_identity.property or "$id",
                EXTERNAL_ID,
            ),
            Projection.property("$label", NODE_LABEL),
            *(
                Projection.property(name, name)
                for name in sorted(set(self.node_properties))
            ),
        ]
        edge_projection = [
            Projection.property("$id", EDGE_ID),
            Projection.from_endpoint("$id", EDGE_SOURCE),
            Projection.to_endpoint("$id", EDGE_TARGET),
            Projection.property("$label", EDGE_LABEL),
        ]
        if self.graphify_edge_key is not None:
            edge_projection.append(
                Projection.property(self.graphify_edge_key.property, EDGE_KEY)
            )
        if self.weight_property is not None:
            edge_projection.append(
                Projection.property(self.weight_property, EDGE_WEIGHT)
            )
        edge_projection.extend(
            Projection.property(name, name)
            for name in sorted(set(self.edge_properties))
        )
        nodes = self.node_traversal
        edges = self.edge_traversal
        if self.max_nodes is not None:
            nodes = nodes.limit(self.max_nodes + 1)
        if self.max_edges is not None:
            edges = edges.limit(self.max_edges + 1)
        batch = (
            read_batch()
            .var_as("nodes", nodes.project(node_projection))
            .var_as("edges", edges.project(edge_projection))
        )
        returns = ["nodes", "edges"]
        if self.metadata is not None:
            metadata_projection = [
                Projection.property(name, name)
                for name in sorted(set(self.metadata.properties))
            ]
            batch = batch.var_as(
                "metadata",
                self.metadata.traversal.limit(2).project(metadata_projection),
            )
            returns.append("metadata")
        return batch.returning(returns).to_query_request()


@dataclass(frozen=True)
class GraphNode:
    id: ExternalId
    label: str | None
    attributes: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class GraphEdge:
    id: GraphEdgeId
    source: ExternalId
    target: ExternalId
    graphify_key: ExternalId | None
    label: str | None
    weight: float | None
    attributes: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class NodeDegree:
    node_id: ExternalId
    degree: int
    weighted_degree: float


@dataclass(frozen=True)
class NodeScore:
    node_id: ExternalId
    score: float


@dataclass(frozen=True)
class EdgeScore:
    edge_id: GraphEdgeId
    graphify_key: ExternalId | None
    source: ExternalId
    target: ExternalId
    score: float


@dataclass(frozen=True)
class GraphCycle:
    node_ids: tuple[ExternalId, ...]
    edge_ids: tuple[GraphEdgeId, ...]


@dataclass(frozen=True)
class CycleResult:
    cycles: tuple[GraphCycle, ...]
    truncated: bool


@dataclass(frozen=True)
class GraphVisit:
    node_id: ExternalId
    depth: int
    discovery_order: int


@dataclass(frozen=True)
class TraversedEdge:
    edge_id: GraphEdgeId
    graphify_key: ExternalId | None
    source: ExternalId
    target: ExternalId
    traversal_direction: EdgeTraversalDirection
    label: str | None


@dataclass(frozen=True)
class TraversalResult:
    visits: tuple[GraphVisit, ...]
    discovery_edges: tuple[TraversedEdge, ...]


@dataclass(frozen=True)
class PathEdge:
    edge_id: GraphEdgeId
    graphify_key: ExternalId | None
    source: ExternalId
    target: ExternalId
    traversal_direction: EdgeTraversalDirection
    label: str | None
    attributes: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MissingSourcePath:
    pass


@dataclass(frozen=True)
class MissingTargetPath:
    pass


@dataclass(frozen=True)
class NoPath:
    pass


@dataclass(frozen=True)
class FoundPath:
    node_ids: tuple[ExternalId, ...]
    edges: tuple[PathEdge, ...]


PathResult: TypeAlias = Union[
    MissingSourcePath,
    MissingTargetPath,
    NoPath,
    FoundPath,
]


@dataclass(frozen=True)
class NodePosition:
    node_id: ExternalId
    x: float
    y: float


@dataclass(frozen=True)
class BetweennessOptions:
    mode: Literal["exact", "sampled", "auto"] = "exact"
    sample_count: int = 100
    seed: int = 42
    exact_through: int = 1_000
    normalized: bool = True
    endpoints: bool = False
    weighted: bool = False

    def __post_init__(self) -> None:
        if self.mode not in {"exact", "sampled", "auto"}:
            raise ValueError("betweenness mode must be 'exact', 'sampled', or 'auto'")
        _positive_int(self.sample_count, "sample_count")
        _non_negative_int(self.exact_through, "exact_through")
        _non_negative_int(self.seed, "seed")

    @classmethod
    def graphify_default(cls) -> "BetweennessOptions":
        return cls(mode="auto")


@dataclass(frozen=True)
class TraversalOptions:
    seeds: tuple[ExternalId, ...]
    max_depth: int
    strategy: Literal["breadth_first", "depth_first"] = "breadth_first"
    direction: TraversalDirection = "both"
    allowed_labels: tuple[str, ...] = ()
    stop_non_seed_at_or_above_degree: int | None = None

    def __post_init__(self) -> None:
        if not self.seeds:
            raise ValueError("traversal requires at least one seed")
        _non_negative_int(self.max_depth, "max_depth")
        if self.strategy not in {"breadth_first", "depth_first"}:
            raise ValueError("strategy must be 'breadth_first' or 'depth_first'")
        if self.direction not in {"out", "in", "both"}:
            raise ValueError("direction must be 'out', 'in', or 'both'")
        if self.stop_non_seed_at_or_above_degree is not None:
            _non_negative_int(
                self.stop_non_seed_at_or_above_degree,
                "stop_non_seed_at_or_above_degree",
            )


@dataclass(frozen=True)
class LouvainOptions:
    resolution: float = 1.0
    threshold: float = 1e-4
    seed: int = 42
    max_levels: int = 10

    def __post_init__(self) -> None:
        if not math.isfinite(self.resolution) or self.resolution <= 0:
            raise ValueError("resolution must be finite and positive")
        if not math.isfinite(self.threshold) or self.threshold < 0:
            raise ValueError("threshold must be finite and non-negative")
        _non_negative_int(self.seed, "seed")
        _positive_int(self.max_levels, "max_levels")


@dataclass(frozen=True)
class LeidenOptions:
    """Weighted Leiden controls using the audited Graphify defaults."""

    resolution: float = 1.0
    randomness: float = 0.001
    seed: int = 42
    trials: int = 1
    max_iterations: int = 100
    max_levels: int = 10

    def __post_init__(self) -> None:
        if not math.isfinite(self.resolution) or self.resolution <= 0:
            raise ValueError("resolution must be finite and positive")
        if not math.isfinite(self.randomness) or self.randomness <= 0:
            raise ValueError("randomness must be finite and positive")
        _non_negative_int(self.seed, "seed")
        _positive_int(self.trials, "trials")
        _positive_int(self.max_iterations, "max_iterations")
        _positive_int(self.max_levels, "max_levels")


@dataclass(frozen=True)
class GraphCommunity:
    id: ExternalId
    node_ids: tuple[ExternalId, ...]


@dataclass(frozen=True)
class CommunityResult:
    communities: tuple[GraphCommunity, ...]
    modularity: float
    levels: int


@dataclass(frozen=True)
class LeidenResult:
    communities: tuple[GraphCommunity, ...]
    modularity: float
    levels: int
    winning_trial: int


@dataclass(frozen=True)
class LayoutOptions:
    k: float | None = None
    iterations: int = 50
    seed: int = 42
    weighted: bool = True
    initial_positions: tuple[tuple[ExternalId, float, float], ...] = ()

    def __post_init__(self) -> None:
        if self.k is not None and (not math.isfinite(self.k) or self.k <= 0):
            raise ValueError("k must be finite and positive")
        _positive_int(self.iterations, "iterations")
        _non_negative_int(self.seed, "seed")
        seen: set[bytes] = set()
        for node_id, x, y in self.initial_positions:
            encoded = _encode_external_id(node_id)
            if encoded in seen or not math.isfinite(x) or not math.isfinite(y):
                raise ValueError(
                    "initial positions require unique IDs and finite coordinates"
                )
            seen.add(encoded)


class NativeGraph:
    """Pythonic façade over the generated immutable Rust graph object."""

    def __init__(self, native: Any, module: Any) -> None:
        self._native = native
        self._module = module

    @property
    def node_count(self) -> int:
        return int(self._native.node_count())

    @property
    def edge_count(self) -> int:
        return int(self._native.edge_count())

    @property
    def directed(self) -> bool:
        return bool(self._native.is_directed())

    @property
    def multigraph(self) -> bool:
        return bool(self._native.is_multigraph())

    @property
    def attributes(self) -> Mapping[str, Any]:
        return _decode_json(self._native.graph_attributes_json())

    def contains_node(self, node_id: ExternalId) -> bool:
        return bool(
            self._native.contains_node(_native_external_id(self._module, node_id))
        )

    def contains_edge(self, edge_id: GraphEdgeId | str) -> bool:
        return bool(self._native.contains_edge(_native_edge_id(self._module, edge_id)))

    def nodes(self) -> tuple[GraphNode, ...]:
        return tuple(_node(record) for record in self._native.nodes())

    def edges(self) -> tuple[GraphEdge, ...]:
        return tuple(_edge(record) for record in self._native.edges())

    def node(self, node_id: ExternalId) -> GraphNode | None:
        record = self._native.node(_native_external_id(self._module, node_id))
        return None if record is None else _node(record)

    def edge(self, edge_id: GraphEdgeId | str) -> GraphEdge | None:
        record = self._native.edge(_native_edge_id(self._module, edge_id))
        return None if record is None else _edge(record)

    def neighbors(
        self, node_id: ExternalId, direction: TraversalDirection = "both"
    ) -> tuple[ExternalId, ...]:
        return tuple(
            _decode_external_id(value)
            for value in self._native.neighbors(
                _native_external_id(self._module, node_id),
                _direction(self._module, direction),
            )
        )

    def successors(self, node_id: ExternalId) -> tuple[ExternalId, ...]:
        return tuple(
            _decode_external_id(value)
            for value in self._native.successors(
                _native_external_id(self._module, node_id)
            )
        )

    def predecessors(self, node_id: ExternalId) -> tuple[ExternalId, ...]:
        return tuple(
            _decode_external_id(value)
            for value in self._native.predecessors(
                _native_external_id(self._module, node_id)
            )
        )

    def out_edge_ids(self, node_id: ExternalId) -> tuple[GraphEdgeId, ...]:
        return tuple(
            _decode_edge_id(edge_id)
            for edge_id in self._native.out_edge_ids(
                _native_external_id(self._module, node_id)
            )
        )

    def in_edge_ids(self, node_id: ExternalId) -> tuple[GraphEdgeId, ...]:
        return tuple(
            _decode_edge_id(edge_id)
            for edge_id in self._native.in_edge_ids(
                _native_external_id(self._module, node_id)
            )
        )

    def incident_edge_ids(self, node_id: ExternalId) -> tuple[GraphEdgeId, ...]:
        return tuple(
            _decode_edge_id(edge_id)
            for edge_id in self._native.incident_edge_ids(
                _native_external_id(self._module, node_id)
            )
        )

    def edges_between(
        self,
        source: ExternalId,
        target: ExternalId,
        direction: TraversalDirection = "both",
    ) -> tuple[GraphEdgeId, ...]:
        return tuple(
            _decode_edge_id(edge_id)
            for edge_id in self._native.edges_between(
                _native_external_id(self._module, source),
                _native_external_id(self._module, target),
                _direction(self._module, direction),
            )
        )

    def degree(self, node_id: ExternalId, kind: DegreeKind = "total") -> NodeDegree:
        degree = self._native.degree(
            _native_external_id(self._module, node_id), _degree_kind(self._module, kind)
        )
        return NodeDegree(
            _decode_external_id(degree.node_id),
            int(degree.degree),
            float(degree.weighted_degree),
        )

    def degrees(self, kind: DegreeKind = "total") -> tuple[NodeDegree, ...]:
        return tuple(
            NodeDegree(
                _decode_external_id(degree.node_id),
                int(degree.degree),
                float(degree.weighted_degree),
            )
            for degree in self._native.degrees(_degree_kind(self._module, kind))
        )

    def has_edge_between(
        self,
        source: ExternalId,
        target: ExternalId,
        direction: TraversalDirection = "both",
    ) -> bool:
        return bool(
            self._native.has_edge_between(
                _native_external_id(self._module, source),
                _native_external_id(self._module, target),
                _direction(self._module, direction),
            )
        )

    def betweenness_centrality(
        self, options: BetweennessOptions | None = None
    ) -> tuple[NodeScore, ...]:
        options = options or BetweennessOptions()
        return tuple(
            NodeScore(_decode_external_id(score.node_id), float(score.score))
            for score in self._native.betweenness_centrality(
                _betweenness(self._module, options)
            )
        )

    def edge_betweenness_centrality(
        self, options: BetweennessOptions | None = None
    ) -> tuple[EdgeScore, ...]:
        options = options or BetweennessOptions()
        return tuple(
            EdgeScore(
                _decode_edge_id(score.edge_id),
                (
                    None
                    if score.graphify_key is None
                    else _decode_external_id(score.graphify_key)
                ),
                _decode_external_id(score.source),
                _decode_external_id(score.target),
                float(score.score),
            )
            for score in self._native.edge_betweenness_centrality(
                _betweenness(self._module, options)
            )
        )

    def simple_cycles(
        self, length_bound: int, max_cycles: int | None = None
    ) -> CycleResult:
        _positive_int(length_bound, "length_bound")
        if max_cycles is not None:
            _positive_int(max_cycles, "max_cycles")
        result = self._native.simple_cycles(length_bound, max_cycles)
        return CycleResult(
            tuple(
                GraphCycle(
                    tuple(_decode_external_id(node_id) for node_id in cycle.node_ids),
                    tuple(_decode_edge_id(edge_id) for edge_id in cycle.edge_ids),
                )
                for cycle in result.cycles
            ),
            bool(result.truncated),
        )

    def traverse(self, options: TraversalOptions) -> TraversalResult:
        native = self._module
        strategy = (
            native.NativeTraversalStrategy.BREADTH_FIRST
            if options.strategy == "breadth_first"
            else native.NativeTraversalStrategy.DEPTH_FIRST
        )
        hub = (
            native.NativeHubExpansionPolicy.EXPAND_ALL()
            if options.stop_non_seed_at_or_above_degree is None
            else native.NativeHubExpansionPolicy.STOP_NON_SEED_AT_OR_ABOVE(
                degree=options.stop_non_seed_at_or_above_degree
            )
        )
        result = self._native.traverse(
            native.NativeTraversalOptions(
                strategy=strategy,
                seeds=[_native_external_id(native, seed) for seed in options.seeds],
                max_depth=options.max_depth,
                direction=_direction(native, options.direction),
                allowed_labels=list(options.allowed_labels),
                hub_policy=hub,
            )
        )
        return TraversalResult(
            tuple(
                GraphVisit(
                    _decode_external_id(visit.node_id),
                    int(visit.depth),
                    int(visit.discovery_order),
                )
                for visit in result.visits
            ),
            tuple(
                TraversedEdge(
                    _decode_edge_id(edge.edge_id),
                    (
                        None
                        if edge.graphify_key is None
                        else _decode_external_id(edge.graphify_key)
                    ),
                    _decode_external_id(edge.source),
                    _decode_external_id(edge.target),
                    _edge_traversal_direction(native, edge.traversal_direction),
                    edge.label,
                )
                for edge in result.discovery_edges
            ),
        )

    def shortest_path(
        self,
        source: ExternalId,
        target: ExternalId,
        *,
        direction: TraversalDirection = "both",
        allowed_labels: Iterable[str] = (),
        max_depth: int | None = None,
    ) -> PathResult:
        if direction not in {"out", "in", "both"}:
            raise ValueError("direction must be 'out', 'in', or 'both'")
        if max_depth is not None:
            _non_negative_int(max_depth, "max_depth")
        result = self._native.shortest_path(
            _native_external_id(self._module, source),
            _native_external_id(self._module, target),
            _direction(self._module, direction),
            list(allowed_labels),
            max_depth,
        )
        if result.is_missing_source():
            return MissingSourcePath()
        if result.is_missing_target():
            return MissingTargetPath()
        if result.is_no_path():
            return NoPath()
        assert result.is_found(), "generated binding returned an unknown path result"
        return FoundPath(
            tuple(_decode_external_id(node_id) for node_id in result.node_ids),
            tuple(
                PathEdge(
                    _decode_edge_id(edge.edge_id),
                    (
                        None
                        if edge.graphify_key is None
                        else _decode_external_id(edge.graphify_key)
                    ),
                    _decode_external_id(edge.source),
                    _decode_external_id(edge.target),
                    _edge_traversal_direction(self._module, edge.traversal_direction),
                    edge.label,
                    _decode_json(edge.attributes_json),
                )
                for edge in result.edges
            ),
        )

    def louvain_communities(
        self, options: LouvainOptions | None = None
    ) -> CommunityResult:
        options = options or LouvainOptions()
        result = self._native.louvain_communities(
            options.resolution, options.threshold, options.seed, options.max_levels
        )
        return CommunityResult(
            _communities(result.communities),
            float(result.modularity),
            int(result.levels),
        )

    def leiden(self, options: LeidenOptions | None = None) -> LeidenResult:
        options = options or LeidenOptions()
        result = self._native.leiden(
            options.resolution,
            options.randomness,
            options.seed,
            options.trials,
            options.max_iterations,
            options.max_levels,
        )
        return LeidenResult(
            _communities(result.communities),
            float(result.modularity),
            int(result.levels),
            int(result.winning_trial),
        )

    def spring_layout(
        self, options: LayoutOptions | None = None
    ) -> tuple[NodePosition, ...]:
        options = options or LayoutOptions()
        return tuple(
            NodePosition(_decode_external_id(position.node_id), position.x, position.y)
            for position in self._native.spring_layout(
                options.k,
                options.iterations,
                options.seed,
                options.weighted,
                [
                    self._module.NativeNodePosition(
                        node_id=_native_external_id(self._module, node_id), x=x, y=y
                    )
                    for node_id, x, y in options.initial_positions
                ],
            )
        )

    def induced_subgraph(self, node_ids: Iterable[ExternalId]) -> "NativeGraph":
        return NativeGraph(
            self._native.induced_subgraph(
                [_native_external_id(self._module, node_id) for node_id in node_ids]
            ),
            self._module,
        )

    def to_directed(self) -> "NativeGraph":
        return NativeGraph(self._native.to_directed(), self._module)

    def to_undirected(self) -> "NativeGraph":
        return NativeGraph(self._native.to_undirected(), self._module)

    def copy(self) -> "NativeGraph":
        return NativeGraph(self._native.copy(), self._module)

    def compose(self, right: "NativeGraph") -> "NativeGraph":
        return NativeGraph(self._native.compose(right._native), self._module)

    def relabel(self, mapping: Mapping[ExternalId, ExternalId]) -> "NativeGraph":
        records = [
            self._module.NativeRelabel(
                _from=_native_external_id(self._module, source),
                to=_native_external_id(self._module, target),
            )
            for source, target in mapping.items()
        ]
        return NativeGraph(self._native.relabel(records), self._module)


def load_graph(client: Client, selection: GraphSelection) -> NativeGraph:
    """Execute one normal read query and return a reusable native graph."""

    native = _native_graph_module()
    kinds = {
        "graph": native.NativeGraphKind.GRAPH,
        "digraph": native.NativeGraphKind.DI_GRAPH,
        "multigraph": native.NativeGraphKind.MULTI_GRAPH,
        "multidigraph": native.NativeGraphKind.MULTI_DI_GRAPH,
    }
    encodings = {
        "internal_id": native.NativeIdentityEncoding.SCALAR,
        "scalar_property": native.NativeIdentityEncoding.SCALAR,
        "tagged_property": native.NativeIdentityEncoding.TAGGED,
    }
    spec = native.NativeGraphLoadSpec(
        kind=kinds[selection.kind],
        node_identity=encodings[selection.node_identity.mode],
        edge_key_identity=(
            None
            if selection.graphify_edge_key is None
            else encodings[selection.graphify_edge_key.mode]
        ),
        node_limit=selection.max_nodes,
        edge_limit=selection.max_edges,
    )
    response = client._graph_response(selection.to_query_request(), spec)
    if client._mode == "embedded":
        return NativeGraph(response, native)
    try:
        return NativeGraph(native.graph_from_query_response(spec, response), native)
    except Exception as exc:
        raise HelixError("NativeGraph", str(exc), cause=exc) from exc


def _native_graph_module() -> Any:
    try:
        import helixdb_uniffi as native
    except (
        ImportError
    ) as exc:  # pragma: no cover - native package is platform-specific.
        raise HelixError.embedded_unavailable(
            "native graph bindings are not installed", cause=exc
        ) from exc
    required = (
        "NativeExternalId",
        "NativeEdgeId",
        "NativeGraphKind",
        "NativeGraphLoadSpec",
        "NativeIdentityEncoding",
        "graph_from_query_response",
    )
    missing = [name for name in required if not hasattr(native, name)]
    if missing:
        raise HelixError.embedded_unavailable(
            f"native graph bindings are missing: {', '.join(missing)}"
        )
    return native


def _decode_json(value: Any) -> Mapping[str, Any]:
    return json.loads(bytes(value).decode("utf-8"))


def _node(record: Any) -> GraphNode:
    return GraphNode(
        _decode_external_id(record.id),
        record.label,
        _decode_json(record.attributes_json),
    )


def _edge(record: Any) -> GraphEdge:
    return GraphEdge(
        _decode_edge_id(record.id),
        _decode_external_id(record.source),
        _decode_external_id(record.target),
        (
            None
            if record.graphify_key is None
            else _decode_external_id(record.graphify_key)
        ),
        record.label,
        record.weight,
        _decode_json(record.attributes_json),
    )


def _communities(records: Iterable[Any]) -> tuple[GraphCommunity, ...]:
    return tuple(
        GraphCommunity(
            _decode_external_id(record.id),
            tuple(_decode_external_id(node_id) for node_id in record.node_ids),
        )
        for record in records
    )


def _native_external_id(native: Any, value: ExternalId) -> Any:
    return native.NativeExternalId(encoded=_encode_external_id(value))


def _native_edge_id(native: Any, value: GraphEdgeId | str) -> Any:
    edge_id = GraphEdgeId.original(value) if isinstance(value, str) else value
    if not isinstance(edge_id, GraphEdgeId):
        raise TypeError("edge identity must be a GraphEdgeId or string")
    return native.NativeEdgeId(encoded=_encode_edge_id(edge_id))


def _encode_edge_id(value: GraphEdgeId) -> bytes:
    return json.dumps(
        value.to_json(),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _decode_edge_id(record: Any) -> GraphEdgeId:
    encoded = bytes(record.encoded)
    try:
        value = json.loads(encoded.decode("utf-8"))
        edge_id = GraphEdgeId.from_json(value)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise ValueError(f"invalid structural edge identity: {exc}") from exc
    if _encode_edge_id(edge_id) != encoded:
        raise ValueError("edge identity is not canonically encoded")
    return edge_id


def _decode_external_id(record: Any) -> ExternalId:
    encoded = bytes(record.encoded)
    try:
        value = _decode_tagged_identity(json.loads(encoded.decode("utf-8")), 0)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise ValueError(f"invalid native external identity: {exc}") from exc
    if _encode_external_id(value) != encoded:
        raise ValueError("native external identity is not canonically encoded")
    return value


def _encode_external_id(value: ExternalId) -> bytes:
    encoded = json.dumps(
        external_id_to_json(value),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(encoded) > 64 * 1024:
        raise ValueError("encoded external identity exceeds 65536 bytes")
    return encoded


def external_id_to_json(value: ExternalId) -> dict[str, Any]:
    """Encode an identity as the canonical tagged-property JSON envelope."""

    return _tagged_identity(value, 0)


def external_id_from_json(value: Any) -> ExternalId:
    """Decode and verify one canonical tagged-property JSON envelope."""

    decoded = _decode_tagged_identity(value, 0)
    if external_id_to_json(decoded) != value:
        raise ValueError("external identity is not canonically encoded")
    return decoded


def _tagged_identity(value: ExternalId, depth: int) -> dict[str, Any]:
    if depth > 64:
        raise ValueError("external identity nesting exceeds 64 levels")
    if value is None:
        payload: dict[str, Any] = {"type": "null"}
    elif isinstance(value, bool):
        payload = {"type": "boolean", "value": value}
    elif isinstance(value, int):
        payload = {"type": "integer", "value": str(value)}
    elif isinstance(value, float):
        payload = {"type": "float", "value": struct.pack(">d", value).hex()}
    elif isinstance(value, str):
        payload = {"type": "string", "value": value}
    elif isinstance(value, bytes):
        payload = {"type": "bytes", "value": value.hex()}
    elif isinstance(value, tuple):
        payload = {
            "type": "tuple",
            "value": [_tagged_identity(item, depth + 1) for item in value],
        }
    elif isinstance(value, frozenset):
        items = sorted(value, key=_external_id_sort_key)
        payload = {
            "type": "frozenset",
            "value": [_tagged_identity(item, depth + 1) for item in items],
        }
    else:
        raise TypeError(f"unsupported external identity type: {type(value).__name__}")
    return {"__helix_external_id_v1": payload}


def _decode_tagged_identity(value: Any, depth: int) -> ExternalId:
    if depth > 64:
        raise ValueError("external identity nesting exceeds 64 levels")
    if not isinstance(value, dict) or set(value) != {"__helix_external_id_v1"}:
        raise ValueError("external identity must contain exactly one envelope")
    payload = value["__helix_external_id_v1"]
    if not isinstance(payload, dict) or not isinstance(payload.get("type"), str):
        raise ValueError("external identity payload requires a string type")
    kind = payload["type"]
    expected = {"type"} if kind == "null" else {"type", "value"}
    if set(payload) != expected:
        raise ValueError("external identity payload fields are not canonical")
    item = payload.get("value")
    if kind == "null":
        return None
    if kind == "boolean" and isinstance(item, bool):
        return item
    if kind == "integer" and isinstance(item, str):
        parsed = int(item)
        if str(parsed) != item:
            raise ValueError("integer external identity is not canonical")
        return parsed
    if kind == "float" and isinstance(item, str) and len(item) == 16:
        return struct.unpack(">d", bytes.fromhex(item))[0]
    if kind == "string" and isinstance(item, str):
        return item
    if kind == "bytes" and isinstance(item, str):
        if item.lower() != item or len(item) % 2 != 0:
            raise ValueError("bytes external identity is not canonical hexadecimal")
        return bytes.fromhex(item)
    if kind == "tuple" and isinstance(item, list):
        return tuple(_decode_tagged_identity(child, depth + 1) for child in item)
    if kind == "frozenset" and isinstance(item, list):
        decoded = [_decode_tagged_identity(child, depth + 1) for child in item]
        if decoded != sorted(decoded, key=_external_id_sort_key):
            raise ValueError("frozenset external identity is not sorted")
        result = frozenset(decoded)
        if len(result) != len(decoded):
            raise ValueError("frozenset external identity contains duplicates")
        return result
    raise ValueError("external identity type and value do not match")


def _external_id_sort_key(value: ExternalId) -> tuple[Any, ...]:
    if value is None:
        return (0,)
    if isinstance(value, bool):
        return (1, value)
    if isinstance(value, int):
        return (2, str(value))
    if isinstance(value, float):
        return (3, int.from_bytes(struct.pack(">d", value), "big"))
    if isinstance(value, str):
        return (4, value)
    if isinstance(value, bytes):
        return (5, value)
    if isinstance(value, tuple):
        return (6, tuple(_external_id_sort_key(item) for item in value))
    if isinstance(value, frozenset):
        return (7, tuple(sorted((_external_id_sort_key(item) for item in value))))
    raise TypeError(f"unsupported external identity type: {type(value).__name__}")


def _property_name(value: str | None) -> str:
    if value is None or not value:
        raise ValueError("graph property names must not be empty")
    if value.startswith(PRIVATE_PREFIX):
        raise ValueError(f"graph property uses reserved prefix: {value}")
    return value


def _direction(native: Any, direction: TraversalDirection) -> Any:
    values = {
        "out": native.NativeTraversalDirection.OUT,
        "in": native.NativeTraversalDirection.IN,
        "both": native.NativeTraversalDirection.BOTH,
    }
    try:
        return values[direction]
    except KeyError as exc:
        raise ValueError("direction must be 'out', 'in', or 'both'") from exc


def _edge_traversal_direction(native: Any, direction: Any) -> EdgeTraversalDirection:
    if direction == native.NativeEdgeTraversalDirection.FORWARD:
        return "forward"
    assert (
        direction == native.NativeEdgeTraversalDirection.REVERSE
    ), "generated binding returned an unknown edge traversal direction"
    return "reverse"


def _degree_kind(native: Any, kind: DegreeKind) -> Any:
    values = {
        "in": native.NativeDegreeKind.IN,
        "out": native.NativeDegreeKind.OUT,
        "total": native.NativeDegreeKind.TOTAL,
    }
    try:
        return values[kind]
    except KeyError as exc:
        raise ValueError("degree kind must be 'in', 'out', or 'total'") from exc


def _betweenness(native: Any, options: BetweennessOptions) -> Any:
    if options.mode == "exact":
        mode = native.NativeBetweennessMode.EXACT()
    elif options.mode == "sampled":
        mode = native.NativeBetweennessMode.SAMPLED(
            sample_count=options.sample_count, seed=options.seed
        )
    elif options.mode == "auto":
        mode = native.NativeBetweennessMode.AUTO(
            exact_through=options.exact_through,
            sample_count=options.sample_count,
            seed=options.seed,
        )
    else:
        raise ValueError("betweenness mode must be 'exact', 'sampled', or 'auto'")
    return native.NativeBetweennessOptions(
        mode=mode,
        normalized=options.normalized,
        endpoints=options.endpoints,
        weighted=options.weighted,
    )


def _positive_int(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")


def _non_negative_int(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")


__all__ = [
    "BetweennessOptions",
    "CommunityResult",
    "CycleResult",
    "EdgeScore",
    "ExternalId",
    "FoundPath",
    "GraphEdge",
    "GraphEdgeId",
    "GraphCommunity",
    "GraphCycle",
    "GraphKind",
    "GraphMetadataSelection",
    "GraphNode",
    "GraphSelection",
    "GraphVisit",
    "IdentitySelection",
    "LayoutOptions",
    "LeidenOptions",
    "LeidenResult",
    "LouvainOptions",
    "MissingSourcePath",
    "MissingTargetPath",
    "NativeGraph",
    "NoPath",
    "NodeDegree",
    "NodePosition",
    "NodeScore",
    "PathEdge",
    "PathResult",
    "TraversalResult",
    "TraversalOptions",
    "TraversedEdge",
    "external_id_from_json",
    "external_id_to_json",
    "load_graph",
]
