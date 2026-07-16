//go:build !helixdb_uniffi

package helix

import (
	"errors"
	"testing"
)

func TestNewEmbeddedClientWithoutBindingsReturnsUnavailable(t *testing.T) {
	_, err := NewEmbeddedClient(InMemorySource{Database: "go-sdk-embedded"})
	if !errors.Is(err, ErrNativeBindingsUnavailable) {
		t.Fatalf("expected native bindings unavailable, got %v", err)
	}
	var helixErr *HelixError
	if !errors.As(err, &helixErr) || helixErr.Kind != ErrorEmbeddedUnavailable {
		t.Fatalf("expected embedded unavailable HelixError, got %T %v", err, err)
	}

	_, err = NewEmbeddedReaderClient(DiskSource{Root: "/tmp/helix", Database: "go-sdk-reader"})
	if !errors.Is(err, ErrNativeBindingsUnavailable) {
		t.Fatalf("expected reader native bindings unavailable, got %v", err)
	}
}
