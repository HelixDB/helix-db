// Package paritycypher executes the shared runtime corpus through the public SDK.
// Assertions are performed independently by the cross-language parity checker.
package paritycypher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	helix "github.com/helixdb/helix-db/sdks/go"
)

type fixture struct {
	Name            string         `json:"name"`
	Query           string         `json:"query"`
	Parameters      map[string]any `json:"parameters"`
	AfterDiskReopen bool           `json:"after_disk_reopen"`
}

func load() ([]fixture, error) {
	path := os.Getenv("HELIX_CYPHER_PARITY_FIXTURES")
	if path == "" {
		path = "../tests/cypher/runtime.json"
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var corpus struct {
		SchemaVersion int       `json:"schema_version"`
		Cases         []fixture `json:"cases"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&corpus); err != nil {
		return nil, err
	}
	if corpus.SchemaVersion != 1 || len(corpus.Cases) != 10 {
		return nil, fmt.Errorf("unsupported or incomplete Cypher parity corpus")
	}
	valid := regexp.MustCompile(`^[a-z0-9-]+$`)
	seen := map[string]bool{}
	for _, f := range corpus.Cases {
		if !valid.MatchString(f.Name) || seen[f.Name] {
			return nil, fmt.Errorf("Cypher parity names must be unique safe basenames")
		}
		seen[f.Name] = true
	}
	return corpus.Cases, nil
}

// writeCase records actual responses and errors; it never retries a write.
func writeCase(client *helix.Client, f fixture, root string) error {
	response, err := client.Cypher(context.Background(), helix.CypherRequest{
		Query: f.Query, Parameters: f.Parameters, QueryName: f.Name,
	})
	output := map[string]any{"result": response}
	if err != nil {
		output = map[string]any{"error": err.Error()}
		var sdkError *helix.HelixError
		if errors.As(err, &sdkError) {
			output["code"] = sdkError.Code
			output["details"] = sdkError.ServerDetails
		}
	}
	body, err := json.Marshal(output)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(root, f.Name+".json"), body, 0o644)
}

// RunEmbedded verifies disk persistence by using a newly opened reader.
func RunEmbedded(source helix.HelixDbSource, cache helix.EmbeddedCacheConfig, results string) error {
	cases, err := load()
	if err != nil {
		return err
	}
	root := filepath.Join(results, "cypher")
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	client, err := helix.NewEmbeddedClientWithConfig(source, cache)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()
	_, disk := source.(helix.DiskSource)
	for _, f := range cases {
		if disk && f.AfterDiskReopen {
			if err := client.Close(); err != nil {
				return err
			}
			reader, err := helix.NewEmbeddedReaderClientWithConfig(source, cache)
			if err != nil {
				return err
			}
			executed := writeCase(reader, f, root)
			closed := reader.Close()
			if executed != nil {
				return executed
			}
			if closed != nil {
				return closed
			}
			reopened, err := helix.NewEmbeddedClientWithConfig(source, cache)
			if err != nil {
				return err
			}
			client = reopened
		} else if err := writeCase(client, f, root); err != nil {
			return err
		}
	}
	return client.Close()
}

// RunHTTP runs either side of a server restart controlled by the parent harness.
func RunHTTP(url, results, phase string) error {
	cases, err := load()
	if err != nil {
		return err
	}
	if phase != "before" && phase != "after" {
		return fmt.Errorf("invalid Cypher HTTP phase")
	}
	root := filepath.Join(results, "cypher")
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	client, err := helix.NewClient(url)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()
	after := false
	for _, f := range cases {
		after = after || f.AfterDiskReopen
		if after == (phase == "after") {
			if err := writeCase(client, f, root); err != nil {
				return err
			}
		}
	}
	return nil
}
