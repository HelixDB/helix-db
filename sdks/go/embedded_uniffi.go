//go:build helixdb_uniffi

package helix

import (
	"errors"
	"fmt"

	native "github.com/helixdb/helix-db/sdks/go/internal/uniffi/helixdb"
)

func openEmbedded(source HelixDbSource, reader bool) (nativeDB, error) {
	nativeSource, err := toNativeSource(source)
	if err != nil {
		return nil, err
	}
	if reader {
		db, err := native.HelixDbOpenReader(nativeSource)
		if err != nil {
			return nil, err
		}
		return &uniffiDatabase{db: db}, nil
	}
	db, err := native.HelixDbOpen(nativeSource)
	if err != nil {
		return nil, err
	}
	return &uniffiDatabase{db: db}, nil
}

type uniffiDatabase struct{ db *native.HelixDb }

func (d *uniffiDatabase) QueryJson(request []byte) ([]byte, error) { return d.db.QueryJson(request) }
func (d *uniffiDatabase) Close() error                             { return d.db.Close() }
func (d *uniffiDatabase) Graph(request []byte, spec graphLoadSpec) (graphBackend, error) {
	value, err := d.db.Graph(request, nativeGraphSpec(spec))
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, errors.New("helix: native graph loader returned nil")
	}
	return &uniffiGraph{graph: value}, nil
}

func toNativeSource(source HelixDbSource) (native.HelixDbSource, error) {
	switch source := source.(type) {
	case InMemorySource:
		return native.HelixDbSourceInMemory{Database: source.Database}, nil
	case DiskSource:
		return native.HelixDbSourceDisk{Root: source.Root, Database: source.Database}, nil
	case ObjectStorageSource:
		var endpoint *string
		if source.Endpoint != "" {
			endpoint = &source.Endpoint
		}
		return native.HelixDbSourceObjectStorage{
			Database:  source.Database,
			Bucket:    source.Bucket,
			Region:    source.Region,
			Endpoint:  endpoint,
			AllowHttp: source.AllowHTTP,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported HelixDbSource %T", source)
	}
}
