//go:build helixdb_uniffi

package helix

import (
	"errors"
	"fmt"
	"sort"

	native "github.com/helixdb/helix-db/sdks/go/internal/uniffi/helixdb"
)

type uniffiGraph struct{ graph *native.NativeGraph }

func nativeGraphAvailable() bool { return true }

func graphFromQueryResponse(spec graphLoadSpec, response []byte) (graphBackend, error) {
	graph, err := native.GraphFromQueryResponse(nativeGraphSpec(spec), response)
	if err != nil {
		return nil, err
	}
	return &uniffiGraph{graph: graph}, nil
}

func nativeGraphSpec(spec graphLoadSpec) native.NativeGraphLoadSpec {
	direction := native.NativeGraphDirectionDirected
	if spec.Direction == GraphUndirected {
		direction = native.NativeGraphDirectionUndirected
	}
	return native.NativeGraphLoadSpec{Direction: direction, NodeLimit: spec.NodeLimit, EdgeLimit: spec.EdgeLimit}
}

func (g *uniffiGraph) NodeCount() uint64               { return g.graph.NodeCount() }
func (g *uniffiGraph) EdgeCount() uint64               { return g.graph.EdgeCount() }
func (g *uniffiGraph) IsDirected() bool                { return g.graph.IsDirected() }
func (g *uniffiGraph) IsMultigraph() bool              { return g.graph.IsMultigraph() }
func (g *uniffiGraph) AttributesJSON() ([]byte, error) { return g.graph.GraphAttributesJson() }
func (g *uniffiGraph) ContainsNode(id string) bool     { return g.graph.ContainsNode(id) }
func (g *uniffiGraph) ContainsEdge(id string) bool     { return g.graph.ContainsEdge(id) }

func (g *uniffiGraph) Nodes() ([]GraphNode, error) {
	values, err := g.graph.Nodes()
	if err != nil {
		return nil, err
	}
	result := make([]GraphNode, len(values))
	for i, value := range values {
		result[i] = graphNode(value)
	}
	return result, nil
}
func (g *uniffiGraph) Edges() ([]GraphEdge, error) {
	values, err := g.graph.Edges()
	if err != nil {
		return nil, err
	}
	result := make([]GraphEdge, len(values))
	for i, value := range values {
		result[i] = graphEdge(value)
	}
	return result, nil
}
func (g *uniffiGraph) Node(id string) (*GraphNode, error) {
	value, err := g.graph.Node(id)
	if err != nil || value == nil {
		return nil, err
	}
	result := graphNode(*value)
	return &result, nil
}
func (g *uniffiGraph) Edge(id string) (*GraphEdge, error) {
	value, err := g.graph.Edge(id)
	if err != nil || value == nil {
		return nil, err
	}
	result := graphEdge(*value)
	return &result, nil
}
func graphNode(value native.NativeGraphNode) GraphNode {
	return GraphNode{ID: value.Id, Label: value.Label, AttributesJSON: value.AttributesJson}
}
func graphEdge(value native.NativeGraphEdge) GraphEdge {
	return GraphEdge{ID: value.Id, GraphifyKey: value.GraphifyKey, Source: value.Source, Target: value.Target, Label: value.Label, Weight: value.Weight, AttributesJSON: value.AttributesJson}
}

func (g *uniffiGraph) Neighbors(id string, direction TraversalDirection) ([]string, error) {
	value, err := nativeDirection(direction)
	if err != nil {
		return nil, err
	}
	return g.graph.Neighbors(id, value)
}
func (g *uniffiGraph) Successors(id string) ([]string, error)   { return g.graph.Successors(id) }
func (g *uniffiGraph) Predecessors(id string) ([]string, error) { return g.graph.Predecessors(id) }
func (g *uniffiGraph) OutEdgeIDs(id string) ([]string, error)   { return g.graph.OutEdgeIds(id) }
func (g *uniffiGraph) InEdgeIDs(id string) ([]string, error)    { return g.graph.InEdgeIds(id) }
func (g *uniffiGraph) IncidentEdgeIDs(id string) ([]string, error) {
	return g.graph.IncidentEdgeIds(id)
}
func (g *uniffiGraph) EdgesBetween(source, target string, direction TraversalDirection) ([]string, error) {
	value, err := nativeDirection(direction)
	if err != nil {
		return nil, err
	}
	return g.graph.EdgesBetween(source, target, value)
}
func (g *uniffiGraph) HasEdgeBetween(source, target string, direction TraversalDirection) (bool, error) {
	value, err := nativeDirection(direction)
	if err != nil {
		return false, err
	}
	return g.graph.HasEdgeBetween(source, target, value)
}

func (g *uniffiGraph) Degree(id string, kind DegreeKind) (NodeDegree, error) {
	value, err := nativeDegree(kind)
	if err != nil {
		return NodeDegree{}, err
	}
	result, err := g.graph.Degree(id, value)
	return nodeDegree(result), err
}
func (g *uniffiGraph) Degrees(kind DegreeKind) []NodeDegree {
	value, err := nativeDegree(kind)
	if err != nil {
		return nil
	}
	values := g.graph.Degrees(value)
	result := make([]NodeDegree, len(values))
	for i, degree := range values {
		result[i] = nodeDegree(degree)
	}
	return result
}
func nodeDegree(value native.NativeNodeDegree) NodeDegree {
	return NodeDegree{NodeID: value.NodeId, Degree: value.Degree, WeightedDegree: value.WeightedDegree}
}

func (g *uniffiGraph) BetweennessCentrality(options BetweennessOptions) ([]NodeScore, error) {
	value, err := nativeBetweenness(options)
	if err != nil {
		return nil, err
	}
	scores, err := g.graph.BetweennessCentrality(value)
	if err != nil {
		return nil, err
	}
	result := make([]NodeScore, len(scores))
	for i, score := range scores {
		result[i] = NodeScore{NodeID: score.NodeId, Score: score.Score}
	}
	return result, nil
}
func (g *uniffiGraph) EdgeBetweennessCentrality(options BetweennessOptions) ([]EdgeScore, error) {
	value, err := nativeBetweenness(options)
	if err != nil {
		return nil, err
	}
	scores, err := g.graph.EdgeBetweennessCentrality(value)
	if err != nil {
		return nil, err
	}
	result := make([]EdgeScore, len(scores))
	for i, score := range scores {
		result[i] = EdgeScore{EdgeID: score.EdgeId, GraphifyKey: score.GraphifyKey, Source: score.Source, Target: score.Target, Score: score.Score}
	}
	return result, nil
}
func nativeBetweenness(options BetweennessOptions) (native.NativeBetweennessOptions, error) {
	var mode native.NativeBetweennessMode
	switch options.Mode {
	case 0, BetweennessExact:
		mode = native.NativeBetweennessModeExact{}
	case BetweennessSampled:
		mode = native.NativeBetweennessModeSampled{SampleCount: options.SampleCount, Seed: options.Seed}
	case BetweennessAuto:
		mode = native.NativeBetweennessModeAuto{ExactThrough: options.ExactThrough, SampleCount: options.SampleCount, Seed: options.Seed}
	default:
		return native.NativeBetweennessOptions{}, fmt.Errorf("helix: invalid betweenness mode %d", options.Mode)
	}
	return native.NativeBetweennessOptions{Mode: mode, Normalized: options.Normalized, Endpoints: options.Endpoints, Weighted: options.Weighted}, nil
}

func (g *uniffiGraph) SimpleCycles(length uint64, maximum *uint64) (CycleResult, error) {
	value, err := g.graph.SimpleCycles(length, maximum)
	if err != nil {
		return CycleResult{}, err
	}
	cycles := make([]Cycle, len(value.Cycles))
	for i, cycle := range value.Cycles {
		cycles[i] = Cycle{NodeIDs: cycle.NodeIds, EdgeIDs: cycle.EdgeIds}
	}
	return CycleResult{Cycles: cycles, Truncated: value.Truncated}, nil
}
func (g *uniffiGraph) Traverse(options TraversalOptions) (TraversalResult, error) {
	direction, err := nativeDirection(options.Direction)
	if err != nil {
		return TraversalResult{}, err
	}
	strategy := native.NativeTraversalStrategyBreadthFirst
	if options.Strategy == TraversalDepthFirst {
		strategy = native.NativeTraversalStrategyDepthFirst
	} else if options.Strategy != 0 && options.Strategy != TraversalBreadthFirst {
		return TraversalResult{}, fmt.Errorf("helix: invalid traversal strategy %d", options.Strategy)
	}
	var hub native.NativeHubExpansionPolicy = native.NativeHubExpansionPolicyExpandAll{}
	if options.StopNonSeedAtOrAboveDegree != nil {
		hub = native.NativeHubExpansionPolicyStopNonSeedAtOrAbove{Degree: *options.StopNonSeedAtOrAboveDegree}
	}
	value, err := g.graph.Traverse(native.NativeTraversalOptions{Strategy: strategy, Seeds: options.Seeds, MaxDepth: options.MaxDepth, Direction: direction, AllowedLabels: options.AllowedLabels, HubPolicy: hub})
	if err != nil {
		return TraversalResult{}, err
	}
	visits := make([]Visit, len(value.Visits))
	for i, visit := range value.Visits {
		visits[i] = Visit{NodeID: visit.NodeId, Depth: visit.Depth, DiscoveryOrder: visit.DiscoveryOrder}
	}
	edges := make([]TraversedEdge, len(value.DiscoveryEdges))
	for i, edge := range value.DiscoveryEdges {
		edges[i] = TraversedEdge{EdgeID: edge.EdgeId, GraphifyKey: edge.GraphifyKey, Source: edge.Source, Target: edge.Target, TraversalDirection: edgeDirection(edge.TraversalDirection), Label: edge.Label}
	}
	return TraversalResult{Visits: visits, DiscoveryEdges: edges}, nil
}
func (g *uniffiGraph) ShortestPath(source, target string, direction TraversalDirection, labels []string, maximum *uint64) (PathResult, error) {
	nativeDirection, err := nativeDirection(direction)
	if err != nil {
		return PathResult{}, err
	}
	value, err := g.graph.ShortestPath(source, target, nativeDirection, labels, maximum)
	if err != nil {
		return PathResult{}, err
	}
	switch result := value.(type) {
	case native.NativePathResultMissingSource:
		return PathResult{Kind: PathMissingSource}, nil
	case native.NativePathResultMissingTarget:
		return PathResult{Kind: PathMissingTarget}, nil
	case native.NativePathResultNoPath:
		return PathResult{Kind: PathNoPath}, nil
	case native.NativePathResultFound:
		edges := make([]PathEdge, len(result.Edges))
		for i, edge := range result.Edges {
			edges[i] = PathEdge{EdgeID: edge.EdgeId, GraphifyKey: edge.GraphifyKey, Source: edge.Source, Target: edge.Target, TraversalDirection: edgeDirection(edge.TraversalDirection), Label: edge.Label, AttributesJSON: edge.AttributesJson}
		}
		return PathResult{Kind: PathFound, NodeIDs: result.NodeIds, Edges: edges}, nil
	default:
		return PathResult{}, fmt.Errorf("helix: unknown native path result %T", value)
	}
}
func edgeDirection(value native.NativeEdgeTraversalDirection) EdgeTraversalDirection {
	if value == native.NativeEdgeTraversalDirectionReverse {
		return EdgeTraversalReverse
	}
	return EdgeTraversalForward
}

func (g *uniffiGraph) LouvainCommunities(options LouvainOptions) (CommunityResult, error) {
	value, err := g.graph.LouvainCommunities(options.Resolution, options.Threshold, options.Seed, options.MaxLevels)
	if err != nil {
		return CommunityResult{}, err
	}
	communities := make([]Community, len(value.Communities))
	for i, community := range value.Communities {
		communities[i] = Community{ID: community.Id, NodeIDs: community.NodeIds}
	}
	return CommunityResult{Communities: communities, Modularity: value.Modularity, Levels: value.Levels}, nil
}
func (g *uniffiGraph) SpringLayout(options LayoutOptions) ([]NodePosition, error) {
	positions := make([]native.NativeNodePosition, len(options.InitialPositions))
	for i, position := range options.InitialPositions {
		positions[i] = native.NativeNodePosition{NodeId: position.NodeID, X: position.X, Y: position.Y}
	}
	values, err := g.graph.SpringLayout(options.K, options.Iterations, options.Seed, options.Weighted, positions)
	if err != nil {
		return nil, err
	}
	result := make([]NodePosition, len(values))
	for i, value := range values {
		result[i] = NodePosition{NodeID: value.NodeId, X: value.X, Y: value.Y}
	}
	return result, nil
}
func (g *uniffiGraph) InducedSubgraph(ids []string) (graphBackend, error) {
	value, err := g.graph.InducedSubgraph(ids)
	if err != nil {
		return nil, err
	}
	return &uniffiGraph{graph: value}, nil
}
func (g *uniffiGraph) ToUndirected() (graphBackend, error) {
	value, err := g.graph.ToUndirected()
	if err != nil {
		return nil, err
	}
	return &uniffiGraph{graph: value}, nil
}
func (g *uniffiGraph) Copy() graphBackend { return &uniffiGraph{graph: g.graph.Copy()} }
func (g *uniffiGraph) Compose(right graphBackend) (graphBackend, error) {
	other, ok := right.(*uniffiGraph)
	if !ok {
		return nil, errors.New("helix: graphs use different native binding backends")
	}
	value, err := g.graph.Compose(other.graph)
	if err != nil {
		return nil, err
	}
	return &uniffiGraph{graph: value}, nil
}
func (g *uniffiGraph) Relabel(mapping map[string]string) (graphBackend, error) {
	keys := make([]string, 0, len(mapping))
	for key := range mapping {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	values := make([]native.NativeRelabel, len(keys))
	for i, key := range keys {
		values[i] = native.NativeRelabel{From: key, To: mapping[key]}
	}
	result, err := g.graph.Relabel(values)
	if err != nil {
		return nil, err
	}
	return &uniffiGraph{graph: result}, nil
}

func nativeDirection(value TraversalDirection) (native.NativeTraversalDirection, error) {
	switch value {
	case 0, TraversalBoth:
		return native.NativeTraversalDirectionBoth, nil
	case TraversalOut:
		return native.NativeTraversalDirectionOut, nil
	case TraversalIn:
		return native.NativeTraversalDirectionIn, nil
	default:
		return 0, fmt.Errorf("helix: invalid traversal direction %d", value)
	}
}
func nativeDegree(value DegreeKind) (native.NativeDegreeKind, error) {
	switch value {
	case 0, DegreeTotal:
		return native.NativeDegreeKindTotal, nil
	case DegreeIn:
		return native.NativeDegreeKindIn, nil
	case DegreeOut:
		return native.NativeDegreeKindOut, nil
	default:
		return 0, fmt.Errorf("helix: invalid degree kind %d", value)
	}
}
