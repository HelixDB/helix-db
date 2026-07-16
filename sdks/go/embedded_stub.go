//go:build !helixdb_uniffi

package helix

func openEmbedded(source HelixDbSource, reader bool) (nativeDB, error) {
	return nil, ErrNativeBindingsUnavailable
}

func graphFromQueryResponse(spec graphLoadSpec, response []byte) (graphBackend, error) {
	return nil, ErrNativeGraphUnavailable
}
