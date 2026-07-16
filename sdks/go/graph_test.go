package helix

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func graphSelection() GraphSelection {
	return GraphSelection{
		NodeTraversal:            G().NWhere(SourceHasKey("$id")),
		EdgeTraversal:            G().EWhere(SourceHasKey("$id")),
		Direction:                GraphDirected,
		NodeProperties:           []string{"path"},
		EdgeProperties:           []string{"line"},
		ExternalIdentityProperty: "external_id",
		GraphifyEdgeKeyProperty:  "key",
		WeightProperty:           "weight",
		MaxNodes:                 2,
		MaxEdges:                 3,
		AllowFullScan:            true,
	}
}

func TestGraphSelectionBuildsOneTwoResultRead(t *testing.T) {
	request, spec, err := graphSelection().request()
	if err != nil {
		t.Fatal(err)
	}
	body, err := MarshalRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	encoded := string(body)
	for _, alias := range []string{graphExternalID, graphEdgeSource, graphEdgeTarget} {
		if !contains(encoded, alias) {
			t.Fatalf("query does not contain %s: %s", alias, encoded)
		}
	}
	if spec.NodeLimit == nil || *spec.NodeLimit != 2 || spec.EdgeLimit == nil || *spec.EdgeLimit != 3 {
		t.Fatalf("unexpected load spec: %+v", spec)
	}
}

func TestGraphSelectionRejectsInvalidContracts(t *testing.T) {
	selection := graphSelection()
	selection.NodeProperties = []string{graphPrivatePrefix + "bad"}
	if _, _, err := selection.request(); err == nil {
		t.Fatal("expected reserved property failure")
	}
	selection = graphSelection()
	selection.Direction = 99
	if _, _, err := selection.request(); err == nil {
		t.Fatal("expected direction failure")
	}
	selection = graphSelection()
	selection.NodeTraversal = G().N(AllNodes()).HasLabel("File")
	selection.EdgeTraversal = G().E(AllEdges()).HasLabel("DEPENDS_ON")
	selection.AllowFullScan = false
	if _, _, err := selection.request(); err == nil {
		t.Fatal("expected full-scan opt-in failure")
	}
}

func TestGraphRequiresNativeBindingsInDefaultBuild(t *testing.T) {
	if nativeGraphAvailable() {
		t.Skip("native binding acceptance is covered by generated-binding tests")
	}
	client, err := NewClient("http://localhost:1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Graph(context.Background(), graphSelection())
	if err == nil || !errors.Is(err, ErrNativeGraphUnavailable) {
		t.Fatalf("expected native binding error, got %v", err)
	}
}

func contains(value, fragment string) bool {
	for index := 0; index+len(fragment) <= len(value); index++ {
		if value[index:index+len(fragment)] == fragment {
			return true
		}
	}
	return false
}
