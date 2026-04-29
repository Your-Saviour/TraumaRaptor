# Design: Order-Preserving `parallelize()` via k-way Merge

## Status
Proposed. Targets `vql/server/flows/parallel.go`.

## Background

The upstream `parallelize()` plugin (`vql/server/flows/parallel.go:80`) splits a
result set into row-range batches and runs the sub-query in N worker goroutines.
Each worker writes rows into a single shared `output_chan` (line ~155). There is
no awareness of row order, so even when each worker emits a sorted stream
(e.g. `query={ SELECT ... FROM source() ORDER BY ts }`), the rows arrive at the
caller arbitrarily interleaved.

Today the only way to get a globally sorted parallelized result is to wrap the
`parallelize()` call in a final `ORDER BY`:

```sql
SELECT * FROM parallelize(query={ SELECT ... FROM source() WHERE ... }) ORDER BY ts
```

The outer `ORDER BY` runs single-threaded on the master goroutine and is
O(N log N) over all rows — the same complexity as if we had not parallelized at
all. For a 590K-row query where the sort is a meaningful fraction of total
work, this neutralises a non-trivial chunk of the win.

## Goals

1. Add an opt-in `order_by` argument to `parallelize()` that returns rows in
   global sort order without an extra outer `ORDER BY`.
2. Total wall-clock cost: per-worker sort O((N/K) log (N/K)) (parallel) + final
   k-way merge O(N log K). For K=4 this is effectively a 4× speedup of the sort
   on top of the existing 4× speedup of the filter/project work.
3. Memory: bounded — we only buffer one row per worker in the merge heap.

## Non-Goals

- Multi-column sort. v1 supports a single column. Multi-column sort can be
  layered on later by changing the comparator.
- Sorting that the inner sub-query did not produce. The contract is: each
  worker MUST emit its rows already sorted by `order_by`. If the caller forgets
  the inner `ORDER BY`, output is undefined (we will not detect this).
- External merge to disk. The k-way merge is in-memory only. K is small (the
  worker count, typically 4–16) so this is fine.

## API

New plugin args, both optional:

| Arg | Type | Default | Meaning |
|---|---|---|---|
| `order_by` | `string` | `""` | Column name to merge on. When empty, behaviour is unchanged from today. |
| `order_desc` | `bool` | `false` | Descending merge. |

Caller contract when `order_by` is set:

```sql
SELECT * FROM parallelize(
    notebook_id="N.X",
    notebook_cell_id="...",
    workers=4,
    order_by="ts",
    query={
        SELECT * FROM source() WHERE ... ORDER BY ts
    }
)
```

The inner `ORDER BY ts` is required; without it each worker emits unsorted
rows and the merge produces garbage. We document this; we do not detect it.

## Approach

### Per-worker channels

Today every worker writes to one shared `output_chan`. To merge, each worker
needs its own channel so the merger can pull head-of-stream from a specific
worker after popping its row.

Change the worker dispatch loop (`parallel.go:129` onward):

```go
worker_chans := make([]chan vfilter.Row, workers)
for i := range worker_chans {
    worker_chans[i] = make(chan vfilter.Row, 64) // small buffer for backpressure
}

for i := int64(0); i < workers; i++ {
    go func(idx int64) {
        defer close(worker_chans[idx])
        for job := range job_chan {
            // ... existing sub-query eval, but write to worker_chans[idx]
            // instead of output_chan ...
        }
    }(i)
}
```

When `order_by == ""`, fall back to the existing fan-in (or fan-in via a single
goroutine that drains all worker channels into `output_chan` without sorting).

### Merge heap

A `container/heap`-backed priority queue keyed on `order_by`. Each heap entry
holds:

```go
type mergeEntry struct {
    row    *ordereddict.Dict
    sortValue any
    workerIdx int
}
```

Initialisation: read one row from each worker channel and push onto the heap
(skip workers whose channel is already closed and empty).

Loop:

```go
for heap.Len() > 0 {
    entry := heap.Pop(h).(*mergeEntry)
    select {
    case <-ctx.Done():
        return
    case output_chan <- entry.row:
    }
    // refill from same worker
    next, ok := <-worker_chans[entry.workerIdx]
    if ok {
        heap.Push(h, &mergeEntry{
            row:       next.(*ordereddict.Dict),
            sortValue: getColumn(next, order_by),
            workerIdx: entry.workerIdx,
        })
    }
}
```

### Comparator

Reuse `vfilter/sort.DefaultSorterCtx`'s `Less` to keep behaviour consistent
with the `SORT` plugin (NULL handling, string vs numeric coercion, etc.). Or
plug in `scope.Lt(a, b)` directly — same semantics. Do NOT roll a fresh one.

### Reusable building blocks already in the codebase

- `vql/sorter/mergesort.go` — `MergeSorter` does external file-backed merge
  sort. Its `provider` interface (`mergesort.go:91`) is "give me a stream of
  rows already sorted by key, exposed as Read/Peek/Close." That abstraction is
  exactly what an in-memory channel-backed `provider` would look like, and
  the existing `Merge()` (line 123) already does the k-way step.

  **Recommendation**: write a `chanProvider` adapter that wraps a worker
  channel as a `provider`, and call `MergeSorter.Merge()` on a list of them.
  We get the comparator, NULL handling, ctx cancellation, and the heap loop
  for free. Probably 30–50 lines instead of 80–100.

  Risk: `MergeSorter` was built for on-disk chunks and may assume providers
  are seekable / re-readable. Need to verify the `provider` interface only
  requires forward iteration. Check `mergesort.go` lines 91–122.

## Edge cases

| Case | Behaviour |
|---|---|
| Worker emits 0 rows | That worker contributes nothing; merge proceeds with the rest. |
| All workers emit 0 rows | Empty output channel, no error. |
| `order_by` column missing on some rows | `vfilter.Lt` treats missing as NULL; NULLs sort first (asc) / last (desc) — same as `SORT` plugin. Document this. |
| Mixed types in `order_by` (string vs int) | Use `scope.Lt`'s coercion rules; same as `SORT`. Cross-type ordering is well-defined but quirky — call this out in docs, not code. |
| Worker errors mid-stream | Existing behaviour: worker logs and exits. The closed channel is treated as end-of-stream by the merger. We do NOT propagate the error to the caller (matches today's `parallelize()`). |
| Caller forgets inner `ORDER BY` | Output is wrong and we don't detect it. Documented caveat. |
| `order_by` set with `workers=1` | Trivially correct: one stream, no merge needed; can short-circuit to plain pass-through. |

## Performance expectations

For a 590K-row query, sort column moderately selective:

- **Today**: parallelize 4× speedup on filter/project, then serial sort
  ~O(N log N) ≈ tens of ms for in-memory sort, but it's all on the master
  goroutine and competes with VQL processing for CPU.
- **With merge**: each worker sorts ~150K rows in parallel (4× faster sort
  step), then merge is O(N log 4) ≈ O(2N) — basically channel-copy speed.

The win shows up when sort cost is non-trivial relative to filter/project. For
a tight `WHERE` that drops most rows, sort cost is small either way and this
patch is marginal. For a query that sorts most of what it reads, this patch
roughly doubles the speedup parallelize already gives.

## Testing

Add tests to `vql/server/flows/parallel_test.go`:

1. **Correctness — ascending**: build a result set with known column values
   in random order, run `parallelize(order_by="x", query={... ORDER BY x})`,
   assert output is sorted and contains every row exactly once.
2. **Correctness — descending**: same with `order_desc=true`.
3. **Empty workers**: pick `workers > total_rows` so several workers get
   zero-row chunks; assert output is correct and complete.
4. **Mixed-type column**: include `null`, ints, and strings in `x`; assert
   ordering matches the `SORT` plugin's output for the same dataset.
5. **Backpressure**: slow consumer (sleep on each row); assert no goroutine
   leaks and bounded memory.
6. **Cancellation**: cancel ctx mid-stream; assert all worker goroutines exit
   within ~100ms and no panic.

For #4 specifically: write a parallel test that pipes the same dataset
through both `SORT` and `parallelize(order_by=...)` and asserts equality. This
catches comparator drift between the two paths.

## Sequencing

1. Land per-worker channels + plain fan-in (no merge yet). This is a refactor
   that keeps current behaviour byte-identical for `order_by=""`. Verify
   golden tests still pass.
2. Add the merge code path behind the `order_by` flag. Default unset → no
   behaviour change.
3. Add tests #1–#6 above.
4. Document the inner-`ORDER BY` requirement in the plugin's `Doc` field.
5. Bench against a 100K-row sorted query and report numbers in the PR.

## Open Questions

1. **Reuse `MergeSorter` or roll fresh?** Need a 15-min spike on the
   `provider` interface to decide. If reuse is clean, do that — fewer lines,
   shared comparator, free maintenance.
2. **Multi-column sort.** Defer or include in v1? The comparator is the only
   thing that changes, and `SORT` already supports multi-column. Probably
   cheap to include if reusing `DefaultSorterCtx`. Otherwise punt.
3. **Implicit inner-sort?** Could `parallelize()` inject `ORDER BY <col>`
   into the sub-query when `order_by` is set, so the caller doesn't have to.
   Pro: less foot-gun. Con: surprising to anyone reading the query verbatim.
   Probably leave it explicit and rely on documentation.

## Risks

- **Subtle comparator drift** vs the `SORT` plugin if we don't reuse its
  comparator. Mitigated by test #4.
- **Worker imbalance**: if one chunk is much slower than others (e.g. dense
  matches in one byte range), the merge stalls waiting for it. The k-way
  merge always pops the global minimum, which means the slowest worker gates
  the entire output. This is fundamental to merge-sort, not specific to this
  design — same caveat applies to any sorted parallel reduction.
- **Memory spike if a single worker buffers ahead**: bounded by the per-worker
  channel capacity (64 in the sketch above). Total: K × 64 rows in flight.
  For K=4 that's 256 rows — negligible.

## Out of scope but related

- **Merge-aware planner**: detecting `parallelize(...) ORDER BY x` at the
  query layer and rewriting it to use `order_by=x` automatically. Belongs in
  VQL parser/planner, not here.
- **Streaming `LIMIT N` with `order_by`**: a top-N variant where the merge
  stops after N rows. Trivially correct on top of this design (cap the merge
  loop), worth doing in a follow-up.
