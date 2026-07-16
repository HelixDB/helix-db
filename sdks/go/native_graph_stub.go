//go:build !helixdb_uniffi

package helix

func nativeGraphAvailable() bool { return false }
