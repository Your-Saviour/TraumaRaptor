//go:build !amd64

package simple

import "github.com/Velocidex/ordereddict"

// parseRow decodes raw JSONL bytes into dst using the standard
// ordereddict JSON unmarshaller. The useSimdjson flag is accepted but
// silently ignored on non-amd64 platforms.
func parseRow(raw []byte, dst *ordereddict.Dict, useSimdjson bool) error {
	return dst.UnmarshalJSON(raw)
}
