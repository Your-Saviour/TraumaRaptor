# Notebook Query Performance Ideas

Based on profiling a 593K row query: CPU-bound at ~53% of 4 cores (1 core VQL pipeline + 1 core GC), 490MB of allocations, 33 seconds wall time. IO is not the bottleneck — raw virtiofs sequential read takes 0.08s.

## 1. Parallel `source()` scan

**File:** `result_sets/simple/simple.go`

The `.json.index` file contains a byte offset for every row. The reader currently processes sequentially in one goroutine. Split into N goroutines using the index:

```
index → divide into N equal chunks by row offset
goroutine 1: rows 0–148K    → channel
goroutine 2: rows 148K–296K → channel
goroutine 3: rows 296K–444K → channel
goroutine 4: rows 444K–593K → channel
merge channels → VQL pipeline
```

The infrastructure is already in place (`SeekToRow` + index offsets). Would reduce 33s → ~9s on 4 cores with no query changes required.

**Effort:** Medium | **Speedup:** ~75% (linear with core count)

---

## 2. Lazy / zero-copy JSON parsing

**File:** `result_sets/simple/simple.go`, `vql/` row handling

Every row is currently fully parsed into an `ordereddict.Dict` — 593K allocations causing 490MB of GC pressure. For queries that only access a subset of fields (or `SELECT *` passthroughs), defer parsing until a field is actually accessed:

```go
// Current: parse everything upfront
row := ordereddict.NewDict()
json.Unmarshal(line, row)  // allocates all keys/values

// Lazy: defer parsing
row := LazyRow{raw: line}
row.Get("OSPath")  // only parses this field on first access
```

Eliminates most of the 490MB allocation for filtered queries, dropping GC from ~1 extra core to near zero.

**Effort:** Medium | **Speedup:** ~40–60% for filtered queries

---

## 3. simdjson-go

**File:** anywhere `encoding/json` is used in the result set read path

Drop-in replacement for `encoding/json` using SIMD CPU instructions. Benchmarks show 3–5× faster JSON parsing on x86_64.

```go
// Replace
import "encoding/json"

// With
import "github.com/minio/simdjson-go"
```

Shaves ~5–8s off raw parsing time with minimal code change.

**Effort:** Small (dependency swap) | **Speedup:** ~30–40%

---

## 4. `sync.Pool` for ordereddict.Dict

**File:** `result_sets/simple/simple.go` row allocation path

Reuse `ordereddict.Dict` objects instead of allocating 593K new ones:

```go
var dictPool = sync.Pool{
    New: func() any { return ordereddict.NewDict() },
}

row := dictPool.Get().(*ordereddict.Dict)
defer dictPool.Put(row.Reset())
```

Reduces GC pressure significantly without changing parsing semantics. Requires `ordereddict.Dict` to implement a `Reset()` method that clears keys without freeing the backing memory.

**Effort:** Small | **Speedup:** ~15%

---

## 5. `parallelize()` VQL function

**File:** `vql/functions/` or `vql/tools/`

Higher-level than #1 — a VQL function users can call explicitly to split a query across N workers:

```sql
SELECT * FROM parallelize(
    source="F.D7O9IR0LR0JHI",
    workers=4,
    query="SELECT * FROM source() WHERE OSPath =~ '^/etc'"
)
```

Each worker is assigned a row range from the index and processes independently. Results are merged and deduplicated at the end. Gives users explicit control over parallelism without modifying the core VQL engine. Works across minion notebook workers too — each worker can be dispatched to a different node.

**Effort:** Large | **Speedup:** ~75% (user-controlled)

---

## 6. Larger bufio buffer

**File:** `result_sets/simple/simple.go:407`

One-line fix. The default `bufio.NewReader` uses a 4KB buffer, causing ~54K system calls to read a 198MB file. Increase to 1MB:

```go
// Current
reader := bufio.NewReader(self.fd)

// Fix
reader := bufio.NewReaderSize(self.fd, 1<<20) // 1MB buffer
```

Reduces virtiofs system calls from ~54K to ~200. Marginal gain since IO is not the bottleneck, but it is a free improvement.

**Effort:** 1 line | **Speedup:** ~5%

---

## Summary

| # | Change | File | Effort | Speedup |
|---|--------|------|--------|---------|
| 1 | Parallel `source()` scan | `result_sets/simple/simple.go` | Medium | ~75% |
| 2 | Lazy JSON parsing | `result_sets/simple/simple.go` | Medium | ~40–60% |
| 3 | simdjson-go | result set read path | Small | ~30–40% |
| 4 | `sync.Pool` for Dict | `result_sets/simple/simple.go` | Small | ~15% |
| 5 | `parallelize()` VQL function | `vql/functions/` | Large | ~75% |
| 6 | Larger bufio buffer | `result_sets/simple/simple.go:407` | 1 line | ~5% |

Combining #1 + #2 + #3 + #4 would theoretically bring the 33s query down to ~3–4s on the current 4-core hardware without any query changes.
