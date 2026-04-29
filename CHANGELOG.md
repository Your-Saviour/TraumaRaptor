# TraumaRaptor Changelog

Changes from upstream [Velociraptor](https://github.com/Velocidex/velociraptor) v0.76.1.

## 2026-04-29

### `parallelize()`: order-preserving k-way merge

`parallelize()` gains an opt-in `order_by` (and `order_desc`) argument. When set,
each batch's sub-query MUST emit rows already sorted by that column (e.g. include
`ORDER BY <col>`) and the plugin performs a streaming k-way merge across batches —
removing the need for a serial outer `ORDER BY` and keeping the sort cost
parallelised. K = number of batches (not number of workers). Works with `hunt_id`:
the hunt's flow set is snapshotted from the on-disk hunt-clients table at query
time, the same way other VQL sources behave (re-run for fresh data).
Files: `vql/server/flows/parallel.go`.

### `parallelize()`: per-batch GROUP BY combine

`parallelize()` gains `group_by` and `aggregates` arguments. The inner sub-query
runs `GROUP BY` per batch as before; `parallelize()` then combines per-batch
partial aggregates into a single global row per key, eliminating the serial outer
`GROUP BY` that previously neutralised the parallelism gain. Combiner kinds map to
VQL's five aggregators (each associative + commutative on partials):

```vql
SELECT * FROM parallelize(
    artifact='X', client_id=ClientId, flow_id=FlowId,
    group_by=['key1', 'key2'],
    aggregates=dict(c='count', total='sum',
                    first_ts='min', last_ts='max', names='enumerate'),
    query={ SELECT key1, key2,
                   count() AS c, sum(item=amount) AS total,
                   min(item=ts) AS first_ts, max(item=ts) AS last_ts,
                   enumerate(items=name) AS names
            FROM source() GROUP BY key1, key2 })
```

Compose with `order_by` to sort the combined output. Bare projection columns
(neither group key nor declared aggregate) follow last-seen semantics, matching
vfilter's existing `GROUP BY` grouper. Mixed-type group keys form distinct buckets.
Output is fully buffered before emission (fundamental to GROUP BY). Works with
`hunt_id`; rows are tagged with `ClientId`/`FlowId` so those can appear in
`group_by` for per-client aggregation. Files: `vql/server/flows/parallel.go`.

## 2026-04-28

### Performance: Result-Set Read Path

Profiling of a 593K-row notebook query showed CPU-bound behaviour at ~53% of 4 cores
(~1 core VQL pipeline, ~1 core GC), 490 MB of allocations, and 33 s wall time. IO was
not the bottleneck (raw file read: 0.08 s). Six targeted improvements address this.

All new tunables live under `defaults:` in the server config and are safe to deploy
without any config changes — existing behaviour is preserved by default.

#### #6 — Larger bufio read buffer

**Files:** `result_sets/simple/simple.go`

The default `bufio.NewReader` used a 4 KB buffer, causing ~54 K syscalls for a 198 MB
result-set. Now configurable, defaulting to 1 MiB (~200 syscalls).

**Config:** `defaults.notebook_source_read_buffer_size` (bytes, default `1048576`)  
**Speedup:** ~5%

#### #4 — `sync.Pool` reuse of `ordereddict.Dict`

**Files:** `result_sets/simple/simple.go`

Dicts are now obtained from a package-level `sync.Pool` and cleared via `resetDict()`
before reuse, reducing the 593 K per-query allocations and GC pressure.

**Config:** `defaults.notebook_source_dict_pool` (bool, default `false` — opt-in)  
**Speedup:** ~15%

#### #3 — simdjson-go SIMD JSON parsing

**Files:** `result_sets/simple/json_simd_amd64.go`, `result_sets/simple/json_fallback.go`

Optional fast JSON parsing via [simdjson-go](https://github.com/minio/simdjson-go)
using AVX2 SIMD instructions (3–5× faster than stdlib). amd64 only; other platforms
fall back silently via build tags.

**Config:** `defaults.notebook_source_use_simdjson` (bool, default `false` — opt-in)  
**Speedup:** ~30–40% on eligible hardware

#### #1 — Parallel `source()` scan with index chunking

**Files:** `result_sets/simple/simple.go`

When an index file is present and `notebook_source_workers > 1`, the result set is
split into N equal row chunks, each read by an independent goroutine with its own file
descriptor. Results are merged into a single output channel. Row order is **not**
preserved across chunks. Default `workers=1` retains sequential byte-identical behaviour.

**Config:** `defaults.notebook_source_workers` (uint, default `1`)  
**Speedup:** ~75% (linear with core count)

#### #2 — Lazy / deferred JSON parsing

**Files:** `result_sets/simple/lazy.go`, `vql/server/flows/results.go`

Added `LazyJsonRow` implementing `vfilter/types.LazyRow`. Rows are emitted as raw JSONL
bytes; JSON is only parsed on first field access. Rows discarded by a `WHERE` clause
before any field access incur zero parse cost. The `source()` plugin selects between
`Rows()` and `LazyRows()` based on the config flag.

**Config:** `defaults.notebook_source_lazy_json` (bool, default `false` — opt-in)  
**Speedup:** ~40–60% for filtered queries

#### #5 — `parallelize()` VQL plugin

**Files:** `vql/tools/parallelize.go`

New built-in VQL plugin that explicitly splits a result set across N parallel reader
goroutines. Accepts the same source arguments as `source()`. Row order is not preserved.

```sql
SELECT * FROM parallelize(
    notebook_id="N.XXXX",
    notebook_cell_id="XXXX",
    workers=4
)
```

**Config:** `defaults.notebook_parallelize_default_workers` (uint, default `4`)  
**Speedup:** ~75% (user-controlled)

#### Config summary

```yaml
defaults:
  notebook_source_read_buffer_size: 1048576   # on by default (1 MiB)
  notebook_source_workers: 4                  # parallel reader (breaks row order)
  notebook_source_dict_pool: true             # sync.Pool dict reuse
  notebook_source_use_simdjson: true          # SIMD JSON (amd64 + AVX2 only)
  notebook_source_lazy_json: true             # defer JSON parsing until field access
  notebook_parallelize_default_workers: 4     # parallelize() default workers
```

### Build Pipeline

**Files:** `Docker/build/Dockerfile`, `magefile.go`

`make linux` now performs the complete build per the official README:
1. `npm ci` — install React GUI dependencies
2. `npm run build` — compile webpack bundle
3. `fileb0x` — embed GUI assets into Go
4. `go build` — compile Linux amd64 binary

Node.js LTS is now included in the Docker build image. No local Node.js required.

```bash
cd Docker/build
docker compose build                          # rebuild image when Dockerfile or go.mod change
docker compose run --rm build make linux      # full build: GUI + binary
```

---

## 2026-03-28

### Bugfix: `notebook_default_new_cell_rows` not applied to non-event notebook cells

The `notebook_default_new_cell_rows` config option only applied to event-type artifact notebooks (client events, server events). For all other artifact types, `LIMIT 50` was hard-coded, ignoring the config.

**Fix:**
- `services/notebook/initial.go` — The `default` case in notebook cell template generation now uses the configured `default_limit` instead of hard-coded `LIMIT 50`, matching the event-type case.
