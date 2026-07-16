//go:build helixdb_uniffi

package helix

import (
	"encoding/json"
	"testing"
)

func TestGeneratedNativeGraphBindingsExecuteRustAlgorithms(t *testing.T) {
	prefix := graphPrivatePrefix
	response, err := json.Marshal(map[string]any{
		"nodes": []map[string]any{
			{prefix + "node_id": "n1", prefix + "external_id": "a", prefix + "node_label": "File"},
			{prefix + "node_id": "n2", prefix + "external_id": "b", prefix + "node_label": "File"},
			{prefix + "node_id": "n3", prefix + "external_id": "c", prefix + "node_label": "File"},
		},
		"edges": []map[string]any{
			{prefix + "edge_id": "e1", prefix + "edge_source": "n1", prefix + "edge_target": "n2", prefix + "edge_label": "REL"},
			{prefix + "edge_id": "e2", prefix + "edge_source": "n2", prefix + "edge_target": "n3", prefix + "edge_label": "REL"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	backend, err := graphFromQueryResponse(graphLoadSpec{Direction: GraphDirected}, response)
	if err != nil {
		t.Fatal(err)
	}
	graph := &NativeGraph{backend: backend}
	scores, err := graph.BetweennessCentrality(BetweennessOptions{Mode: BetweennessExact})
	if err != nil {
		t.Fatal(err)
	}
	if len(scores) != 3 || scores[1].NodeID != "b" || scores[1].Score != 1 {
		t.Fatalf("unexpected centrality: %+v", scores)
	}
	path, err := graph.ShortestPath("a", "c", TraversalOut, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if path.Kind != PathFound {
		t.Fatalf("unexpected path: %+v", path)
	}
}
