// LazyJsonRow — a vfilter.LazyRow that defers JSON parsing until a field is
// first accessed. Rows that are filtered out by WHERE clauses before any
// field access incur zero parse cost.
//
// Implements types.LazyRow so vfilter's AssociativeDispatcher dispatches
// directly via the LazyRow case (protocol_associative.go:64-65), avoiding
// the full ordereddict.Dict allocation path.

package simple

import (
	"context"
	"sync"

	"github.com/Velocidex/ordereddict"
	vfilter_types "www.velocidex.com/golang/vfilter/types"
)

// LazyJsonRow holds raw JSONL bytes and parses them on demand.
// It satisfies vfilter/types.LazyRow so vfilter calls Get() directly.
type LazyJsonRow struct {
	raw         []byte
	useSimdjson bool

	mu      sync.Mutex
	parsed  bool
	backing *ordereddict.Dict
}

func newLazyJsonRow(raw []byte, useSimdjson bool) *LazyJsonRow {
	return &LazyJsonRow{raw: raw, useSimdjson: useSimdjson}
}

// ensureParsed fully parses the raw bytes into backing on first call.
func (self *LazyJsonRow) ensureParsed() {
	self.mu.Lock()
	defer self.mu.Unlock()
	if !self.parsed {
		self.backing = ordereddict.NewDict()
		_ = parseRow(self.raw, self.backing, self.useSimdjson)
		self.parsed = true
	}
}

// AddColumn is a no-op for LazyJsonRow; we don't support synthetic getters.
func (self *LazyJsonRow) AddColumn(
	name string,
	getter func(ctx context.Context, scope vfilter_types.Scope) vfilter_types.Any,
) vfilter_types.LazyRow {
	return self
}

// Has checks whether the JSON object contains the given key.
// Triggers a full parse on first call.
func (self *LazyJsonRow) Has(name string) bool {
	self.ensureParsed()
	_, pres := self.backing.Get(name)
	return pres
}

// Get returns the value for a key, triggering a full parse on first call.
// Once parsed, subsequent calls hit the in-memory backing dict directly.
func (self *LazyJsonRow) Get(name string) (vfilter_types.Any, bool) {
	self.ensureParsed()
	return self.backing.Get(name)
}

// Columns returns all column names, triggering a full parse on first call.
func (self *LazyJsonRow) Columns() []string {
	self.ensureParsed()
	return self.backing.Keys()
}

// LazyRowsReader is implemented by ResultSetReaderImpl to signal that it
// supports lazy row emission. Callers (e.g. the source() plugin) can
// type-assert the reader to this interface and use LazyRows() instead of
// Rows() when lazy parsing is desired.
type LazyRowsReader interface {
	LazyRows(ctx context.Context) <-chan vfilter_types.LazyRow
	IsLazyJson() bool
}

// LazyRows emits rows as LazyJsonRow objects. Parsing is deferred until a
// field is first accessed on each row.
func (self *ResultSetReaderImpl) LazyRows(ctx context.Context) <-chan vfilter_types.LazyRow {
	output := make(chan vfilter_types.LazyRow)

	go func() {
		defer close(output)

		// Iterate raw bytes without decoding using the JSON() channel.
		raw_chan, err := self.JSON(ctx)
		if err != nil {
			return
		}

		for raw := range raw_chan {
			if len(raw) < 2 {
				continue
			}
			row := newLazyJsonRow(raw, self.useSimdjson)
			select {
			case <-ctx.Done():
				return
			case output <- row:
			}
		}
	}()
	return output
}

// IsLazyJson reports whether the lazy JSON mode is enabled on this reader.
func (self *ResultSetReaderImpl) IsLazyJson() bool {
	return self.useLazyJson
}
