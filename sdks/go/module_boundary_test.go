package helix_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestExternalConsumerCanTidyModule(t *testing.T) {
	moduleDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	consumer := t.TempDir()
	moduleDir = filepath.ToSlash(moduleDir)
	goMod := fmt.Sprintf(`module example.com/helix-sdk-consumer

go 1.22

require github.com/helixdb/helix-db/sdks/go v0.0.0

replace github.com/helixdb/helix-db/sdks/go => %s
`, moduleDir)
	if err := os.WriteFile(filepath.Join(consumer, "go.mod"), []byte(goMod), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(consumer, "main.go"),
		[]byte("package main\n\nimport _ \"github.com/helixdb/helix-db/sdks/go\"\n\nfunc main() {}\n"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}

	command := exec.Command("go", "mod", "tidy")
	command.Dir = consumer
	command.Env = append(os.Environ(), "GOWORK=off", "GOPROXY=off")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("external go mod tidy failed: %v\n%s", err, output)
	}
}
