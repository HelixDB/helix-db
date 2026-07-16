package helix

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

const (
	graphPrivatePrefix = "__helix_graph_"
	graphNodeID        = "__helix_graph_node_id"
	graphExternalID    = "__helix_graph_external_id"
	graphNodeLabel     = "__helix_graph_node_label"
	graphEdgeID        = "__helix_graph_edge_id"
	graphEdgeKey       = "__helix_graph_edge_key"
	graphEdgeSource    = "__helix_graph_edge_source"
	graphEdgeTarget    = "__helix_graph_edge_target"
	graphEdgeLabel     = "__helix_graph_edge_label"
	graphEdgeWeight    = "__helix_graph_edge_weight"
)

var ErrNativeGraphUnavailable = errors.New("helix: native graph bindings are not linked")

// GraphDirection controls whether algorithms preserve stored edge direction.
type GraphDirection uint8

const (
	GraphDirected GraphDirection = iota + 1
	GraphUndirected
)

// GraphSelection builds the single ordinary read used by Client.Graph.
type GraphSelection struct {
	NodeTraversal            *Traversal
	EdgeTraversal            *Traversal
	Direction                GraphDirection
	NodeProperties           []string
	EdgeProperties           []string
	ExternalIdentityProperty string
	GraphifyEdgeKeyProperty  string
	WeightProperty           string
	MaxNodes                 uint64
	MaxEdges                 uint64
	AllowFullScan            bool
}

func (s GraphSelection) request() (Request, graphLoadSpec, error) {
	if s.NodeTraversal == nil || s.EdgeTraversal == nil {
		return nil, graphLoadSpec{}, errors.New("helix: graph selection requires node and edge traversals")
	}
	if s.Direction != GraphDirected && s.Direction != GraphUndirected {
		return nil, graphLoadSpec{}, errors.New("helix: graph direction must be directed or undirected")
	}
	startsWithFullScan := func(traversal *Traversal) bool {
		steps := traversal.Steps()
		if len(steps) == 0 || (steps[0].kind != "N" && steps[0].kind != "E") {
			return false
		}
		switch reference := steps[0].value.(type) {
		case NodeRef:
			return reference.kind == "All"
		case EdgeRef:
			return reference.kind == "All"
		default:
			return false
		}
	}
	if !s.AllowFullScan && (startsWithFullScan(s.NodeTraversal) || startsWithFullScan(s.EdgeTraversal)) {
		return nil, graphLoadSpec{}, errors.New("helix: full graph scans require AllowFullScan")
	}
	properties := append(append([]string{}, s.NodeProperties...), s.EdgeProperties...)
	properties = append(properties, s.ExternalIdentityProperty, s.GraphifyEdgeKeyProperty, s.WeightProperty)
	for _, property := range properties {
		if property == "" {
			continue
		}
		if len(property) >= len(graphPrivatePrefix) && property[:len(graphPrivatePrefix)] == graphPrivatePrefix {
			return nil, graphLoadSpec{}, fmt.Errorf("helix: graph property uses reserved prefix: %s", property)
		}
	}
	nodeProperties := uniqueSorted(s.NodeProperties)
	edgeProperties := uniqueSorted(s.EdgeProperties)
	nodeProjection := []Projection{
		ProjectPropAs("$id", graphNodeID),
		ProjectPropAs(valueOr(s.ExternalIdentityProperty, "$id"), graphExternalID),
		ProjectPropAs("$label", graphNodeLabel),
	}
	for _, property := range nodeProperties {
		if property == "" {
			return nil, graphLoadSpec{}, errors.New("helix: graph property names must not be empty")
		}
		nodeProjection = append(nodeProjection, ProjectPropAs(property, property))
	}
	edgeProjection := []Projection{
		ProjectPropAs("$id", graphEdgeID),
		ProjectFromEndpoint("$id", graphEdgeSource),
		ProjectToEndpoint("$id", graphEdgeTarget),
		ProjectPropAs("$label", graphEdgeLabel),
	}
	if s.GraphifyEdgeKeyProperty != "" {
		edgeProjection = append(edgeProjection, ProjectPropAs(s.GraphifyEdgeKeyProperty, graphEdgeKey))
	}
	if s.WeightProperty != "" {
		edgeProjection = append(edgeProjection, ProjectPropAs(s.WeightProperty, graphEdgeWeight))
	}
	for _, property := range edgeProperties {
		if property == "" {
			return nil, graphLoadSpec{}, errors.New("helix: graph property names must not be empty")
		}
		edgeProjection = append(edgeProjection, ProjectPropAs(property, property))
	}
	nodes := TraversalFromSteps(s.NodeTraversal.Steps())
	edges := TraversalFromSteps(s.EdgeTraversal.Steps())
	if s.MaxNodes > 0 {
		nodes.Limit(s.MaxNodes + 1)
	}
	if s.MaxEdges > 0 {
		edges.Limit(s.MaxEdges + 1)
	}
	request := NewReadQueryRequest(
		Read().
			VarAs("nodes", nodes.Project(nodeProjection...)).
			VarAs("edges", edges.Project(edgeProjection...)).
			Returning("nodes", "edges"),
	)
	return request, graphLoadSpec{Direction: s.Direction, NodeLimit: optionalLimit(s.MaxNodes), EdgeLimit: optionalLimit(s.MaxEdges)}, nil
}

type graphLoadSpec struct {
	Direction GraphDirection
	NodeLimit *uint64
	EdgeLimit *uint64
}

// Graph executes exactly one ordinary read and returns a reusable native graph.
func (c *Client) Graph(ctx context.Context, selection GraphSelection) (*NativeGraph, error) {
	request, spec, err := selection.request()
	if err != nil {
		return nil, err
	}
	if !nativeGraphAvailable() {
		return nil, ErrNativeGraphUnavailable
	}
	requestBytes, err := MarshalRequest(request)
	if err != nil {
		return nil, &HelixError{Kind: ErrorSerialization, Err: err, Details: err.Error()}
	}
	if c != nil && c.embedded != nil {
		if direct, ok := c.embedded.(interface {
			Graph([]byte, graphLoadSpec) (graphBackend, error)
		}); ok {
			backend, err := direct.Graph(requestBytes, spec)
			if err != nil {
				return nil, &HelixError{Kind: ErrorEmbedded, Err: err, Details: err.Error()}
			}
			return &NativeGraph{backend: backend}, nil
		}
	}
	var response json.RawMessage
	if err := c.Exec(ctx, request, &response); err != nil {
		return nil, err
	}
	backend, err := graphFromQueryResponse(spec, response)
	if err != nil {
		return nil, err
	}
	return &NativeGraph{backend: backend}, nil
}

// GraphNode keeps selected attributes as lazy JSON bytes.
type GraphNode struct {
	ID             string
	Label          *string
	AttributesJSON []byte
}

func (n GraphNode) Attributes() (map[string]any, error) {
	var attributes map[string]any
	err := json.Unmarshal(n.AttributesJSON, &attributes)
	return attributes, err
}

// GraphEdge preserves stable Helix identity and optional Graphify key.
type GraphEdge struct {
	ID             string
	GraphifyKey    *string
	Source         string
	Target         string
	Label          *string
	Weight         *float64
	AttributesJSON []byte
}

func (e GraphEdge) Attributes() (map[string]any, error) {
	var attributes map[string]any
	err := json.Unmarshal(e.AttributesJSON, &attributes)
	return attributes, err
}

type BetweennessMode uint8

const (
	BetweennessExact BetweennessMode = iota + 1
	BetweennessSampled
	BetweennessAuto
)

type BetweennessOptions struct {
	Mode         BetweennessMode
	SampleCount  uint64
	Seed         uint64
	ExactThrough uint64
	Normalized   bool
	Endpoints    bool
	Weighted     bool
}

func GraphifyBetweennessOptions() BetweennessOptions {
	return BetweennessOptions{Mode: BetweennessAuto, SampleCount: 100, Seed: 42, ExactThrough: 1_000, Normalized: true}
}

type NodeScore struct {
	NodeID string
	Score  float64
}
type EdgeScore struct {
	EdgeID      string
	GraphifyKey *string
	Source      string
	Target      string
	Score       float64
}
type Cycle struct {
	NodeIDs []string
	EdgeIDs []string
}
type CycleResult struct {
	Cycles    []Cycle
	Truncated bool
}

type TraversalDirection uint8

const (
	TraversalOut TraversalDirection = iota + 1
	TraversalIn
	TraversalBoth
)

type TraversalStrategy uint8

const (
	TraversalBreadthFirst TraversalStrategy = iota + 1
	TraversalDepthFirst
)

type TraversalOptions struct {
	Strategy                   TraversalStrategy
	Seeds                      []string
	MaxDepth                   uint64
	Direction                  TraversalDirection
	AllowedLabels              []string
	StopNonSeedAtOrAboveDegree *uint64
}

type Visit struct {
	NodeID         string
	Depth          uint64
	DiscoveryOrder uint64
}
type EdgeTraversalDirection uint8

const (
	EdgeTraversalForward EdgeTraversalDirection = iota + 1
	EdgeTraversalReverse
)

type TraversedEdge struct {
	EdgeID             string
	GraphifyKey        *string
	Source             string
	Target             string
	TraversalDirection EdgeTraversalDirection
	Label              *string
}
type TraversalResult struct {
	Visits         []Visit
	DiscoveryEdges []TraversedEdge
}

type DegreeKind uint8

const (
	DegreeIn DegreeKind = iota + 1
	DegreeOut
	DegreeTotal
)

type NodeDegree struct {
	NodeID         string
	Degree         uint64
	WeightedDegree float64
}
type PathEdge struct {
	EdgeID             string
	GraphifyKey        *string
	Source             string
	Target             string
	TraversalDirection EdgeTraversalDirection
	Label              *string
	AttributesJSON     []byte
}
type PathResultKind uint8

const (
	PathMissingSource PathResultKind = iota + 1
	PathMissingTarget
	PathNoPath
	PathFound
)

type PathResult struct {
	Kind    PathResultKind
	NodeIDs []string
	Edges   []PathEdge
}
type LouvainOptions struct {
	Resolution float64
	Threshold  float64
	Seed       uint64
	MaxLevels  uint64
}
type Community struct {
	ID      string
	NodeIDs []string
}
type CommunityResult struct {
	Communities []Community
	Modularity  float64
	Levels      uint64
}
type NodePosition struct {
	NodeID string
	X      float64
	Y      float64
}
type LayoutOptions struct {
	K                *float64
	Iterations       uint64
	Seed             uint64
	Weighted         bool
	InitialPositions []NodePosition
}

// NativeGraph is immutable; algorithms never query Helix after construction.
type NativeGraph struct{ backend graphBackend }

func (g *NativeGraph) NodeCount() uint64                  { return g.backend.NodeCount() }
func (g *NativeGraph) EdgeCount() uint64                  { return g.backend.EdgeCount() }
func (g *NativeGraph) IsDirected() bool                   { return g.backend.IsDirected() }
func (g *NativeGraph) IsMultigraph() bool                 { return g.backend.IsMultigraph() }
func (g *NativeGraph) AttributesJSON() ([]byte, error)    { return g.backend.AttributesJSON() }
func (g *NativeGraph) ContainsNode(id string) bool        { return g.backend.ContainsNode(id) }
func (g *NativeGraph) ContainsEdge(id string) bool        { return g.backend.ContainsEdge(id) }
func (g *NativeGraph) Nodes() ([]GraphNode, error)        { return g.backend.Nodes() }
func (g *NativeGraph) Edges() ([]GraphEdge, error)        { return g.backend.Edges() }
func (g *NativeGraph) Node(id string) (*GraphNode, error) { return g.backend.Node(id) }
func (g *NativeGraph) Edge(id string) (*GraphEdge, error) { return g.backend.Edge(id) }
func (g *NativeGraph) Neighbors(id string, direction TraversalDirection) ([]string, error) {
	return g.backend.Neighbors(id, direction)
}
func (g *NativeGraph) Successors(id string) ([]string, error)   { return g.backend.Successors(id) }
func (g *NativeGraph) Predecessors(id string) ([]string, error) { return g.backend.Predecessors(id) }
func (g *NativeGraph) OutEdgeIDs(id string) ([]string, error)   { return g.backend.OutEdgeIDs(id) }
func (g *NativeGraph) InEdgeIDs(id string) ([]string, error)    { return g.backend.InEdgeIDs(id) }
func (g *NativeGraph) IncidentEdgeIDs(id string) ([]string, error) {
	return g.backend.IncidentEdgeIDs(id)
}
func (g *NativeGraph) EdgesBetween(source, target string, direction TraversalDirection) ([]string, error) {
	return g.backend.EdgesBetween(source, target, direction)
}
func (g *NativeGraph) HasEdgeBetween(source, target string, direction TraversalDirection) (bool, error) {
	return g.backend.HasEdgeBetween(source, target, direction)
}
func (g *NativeGraph) Degree(id string, kind DegreeKind) (NodeDegree, error) {
	return g.backend.Degree(id, kind)
}
func (g *NativeGraph) Degrees(kind DegreeKind) []NodeDegree { return g.backend.Degrees(kind) }
func (g *NativeGraph) BetweennessCentrality(options BetweennessOptions) ([]NodeScore, error) {
	return g.backend.BetweennessCentrality(options)
}
func (g *NativeGraph) EdgeBetweennessCentrality(options BetweennessOptions) ([]EdgeScore, error) {
	return g.backend.EdgeBetweennessCentrality(options)
}
func (g *NativeGraph) SimpleCycles(lengthBound uint64, maxCycles *uint64) (CycleResult, error) {
	return g.backend.SimpleCycles(lengthBound, maxCycles)
}
func (g *NativeGraph) Traverse(options TraversalOptions) (TraversalResult, error) {
	return g.backend.Traverse(options)
}
func (g *NativeGraph) ShortestPath(source, target string, direction TraversalDirection, labels []string, maxDepth *uint64) (PathResult, error) {
	return g.backend.ShortestPath(source, target, direction, labels, maxDepth)
}
func (g *NativeGraph) LouvainCommunities(options LouvainOptions) (CommunityResult, error) {
	return g.backend.LouvainCommunities(options)
}
func (g *NativeGraph) SpringLayout(options LayoutOptions) ([]NodePosition, error) {
	return g.backend.SpringLayout(options)
}
func (g *NativeGraph) InducedSubgraph(ids []string) (*NativeGraph, error) {
	backend, err := g.backend.InducedSubgraph(ids)
	if err != nil {
		return nil, err
	}
	return &NativeGraph{backend: backend}, nil
}
func (g *NativeGraph) ToUndirected() (*NativeGraph, error) {
	backend, err := g.backend.ToUndirected()
	if err != nil {
		return nil, err
	}
	return &NativeGraph{backend: backend}, nil
}
func (g *NativeGraph) Copy() *NativeGraph { return &NativeGraph{backend: g.backend.Copy()} }
func (g *NativeGraph) Compose(right *NativeGraph) (*NativeGraph, error) {
	backend, err := g.backend.Compose(right.backend)
	if err != nil {
		return nil, err
	}
	return &NativeGraph{backend: backend}, nil
}
func (g *NativeGraph) Relabel(mapping map[string]string) (*NativeGraph, error) {
	backend, err := g.backend.Relabel(mapping)
	if err != nil {
		return nil, err
	}
	return &NativeGraph{backend: backend}, nil
}

type graphBackend interface {
	NodeCount() uint64
	EdgeCount() uint64
	IsDirected() bool
	IsMultigraph() bool
	AttributesJSON() ([]byte, error)
	ContainsNode(string) bool
	ContainsEdge(string) bool
	Nodes() ([]GraphNode, error)
	Edges() ([]GraphEdge, error)
	Node(string) (*GraphNode, error)
	Edge(string) (*GraphEdge, error)
	Neighbors(string, TraversalDirection) ([]string, error)
	Successors(string) ([]string, error)
	Predecessors(string) ([]string, error)
	OutEdgeIDs(string) ([]string, error)
	InEdgeIDs(string) ([]string, error)
	IncidentEdgeIDs(string) ([]string, error)
	EdgesBetween(string, string, TraversalDirection) ([]string, error)
	HasEdgeBetween(string, string, TraversalDirection) (bool, error)
	Degree(string, DegreeKind) (NodeDegree, error)
	Degrees(DegreeKind) []NodeDegree
	BetweennessCentrality(BetweennessOptions) ([]NodeScore, error)
	EdgeBetweennessCentrality(BetweennessOptions) ([]EdgeScore, error)
	SimpleCycles(uint64, *uint64) (CycleResult, error)
	Traverse(TraversalOptions) (TraversalResult, error)
	ShortestPath(string, string, TraversalDirection, []string, *uint64) (PathResult, error)
	LouvainCommunities(LouvainOptions) (CommunityResult, error)
	SpringLayout(LayoutOptions) ([]NodePosition, error)
	InducedSubgraph([]string) (graphBackend, error)
	ToUndirected() (graphBackend, error)
	Copy() graphBackend
	Compose(graphBackend) (graphBackend, error)
	Relabel(map[string]string) (graphBackend, error)
}

func uniqueSorted(values []string) []string {
	result := append([]string{}, values...)
	sort.Strings(result)
	return compact(result)
}
func compact(values []string) []string {
	if len(values) == 0 {
		return values
	}
	result := values[:1]
	for _, value := range values[1:] {
		if value != result[len(result)-1] {
			result = append(result, value)
		}
	}
	return result
}
func valueOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
func optionalLimit(value uint64) *uint64 {
	if value == 0 {
		return nil
	}
	return &value
}
