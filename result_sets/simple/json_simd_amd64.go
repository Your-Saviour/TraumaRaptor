//go:build amd64

package simple

import (
	"github.com/Velocidex/ordereddict"
	simdjson "github.com/minio/simdjson-go"
)

// parseRow decodes raw JSONL bytes into dst. When useSimdjson is true and the
// CPU supports AVX2, simdjson-go is used for faster parsing; otherwise the
// standard ordereddict JSON unmarshaller is used.
func parseRow(raw []byte, dst *ordereddict.Dict, useSimdjson bool) error {
	if useSimdjson && simdjson.SupportedCPU() {
		if trySimdjson(raw, dst) {
			return nil
		}
		// simdjson failed or produced incomplete output — fall through to stdlib.
		resetDict(dst)
	}
	return dst.UnmarshalJSON(raw)
}

// trySimdjson attempts a simdjson parse into dst. Returns false on any error
// so the caller can fall back to stdlib without dropping the row.
func trySimdjson(raw []byte, dst *ordereddict.Dict) bool {
	pj, err := simdjson.Parse(raw, nil)
	if err != nil {
		return false
	}
	iter := pj.Iter()
	iter.Advance()
	obj, err := iter.Object(nil)
	if err != nil {
		return false
	}
	var elem simdjson.Iter
	for {
		name, typ, err := obj.NextElement(&elem)
		if err != nil || typ == simdjson.TypeNone {
			break
		}
		val, err := elem.Interface()
		if err != nil {
			return false
		}
		dst.Set(name, val)
	}
	return true
}
