package flows

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/velociraptor/acls"
	config_proto "www.velocidex.com/golang/velociraptor/config/proto"
	"www.velocidex.com/golang/velociraptor/result_sets"
	"www.velocidex.com/golang/velociraptor/services"
	"www.velocidex.com/golang/velociraptor/utils"
	"www.velocidex.com/golang/velociraptor/vql"
	vql_subsystem "www.velocidex.com/golang/velociraptor/vql"
	"www.velocidex.com/golang/vfilter"
	"www.velocidex.com/golang/vfilter/arg_parser"
	"www.velocidex.com/golang/vfilter/types"
)

// This is very similar to the source plugin, but runs the query over
// subsets of the sources in parallel, combining results.
type ParallelPluginArgs struct {
	// Collected artifacts from clients should specify the client
	// id and flow id as well as the artifact and source.
	ClientId string `vfilter:"optional,field=client_id,doc=The client id to extract"`
	FlowId   string `vfilter:"optional,field=flow_id,doc=A flow ID (client or server artifacts)"`

	// Specifying the hunt id will retrieve all rows in this hunt
	// (from all clients). You still need to specify the artifact
	// name.
	HuntId string `vfilter:"optional,field=hunt_id,doc=Retrieve sources from this hunt (combines all results from all clients)"`

	// Artifacts are specified by name and source. Name may
	// include the source following the artifact name by a slash -
	// e.g. Custom.Artifact/SourceName.
	Artifact string `vfilter:"optional,field=artifact,doc=The name of the artifact collection to fetch"`
	Source   string `vfilter:"optional,field=source,doc=An optional named source within the artifact"`

	// If the artifact name specifies an event artifact, you may
	// also specify start and end times to return.
	StartTime int64 `vfilter:"optional,field=start_time,doc=Start return events from this date (for event sources)"`
	EndTime   int64 `vfilter:"optional,field=end_time,doc=Stop end events reach this time (event sources)."`

	// A source may specify a notebook cell to read from - this
	// allows post processing in multiple stages - one query
	// reduces the data into a result set and subsequent queries
	// operate on that reduced set.
	NotebookId        string `vfilter:"optional,field=notebook_id,doc=The notebook to read from (should also include cell id)"`
	NotebookCellId    string `vfilter:"optional,field=notebook_cell_id,doc=The notebook cell read from (should also include notebook id)"`
	NotebookCellTable int64  `vfilter:"optional,field=notebook_cell_table,doc=A notebook cell can have multiple tables.)"`

	Query vfilter.StoredQuery `vfilter:"required,field=query,doc=The query will be run in parallel over batches."`

	Workers   int64 `vfilter:"optional,field=workers,doc=Number of workers to spawn.)"`
	BatchSize int64 `vfilter:"optional,field=batch,doc=Number of rows in each batch.)"`

	// When OrderBy is set, the inner sub-query MUST emit rows already
	// sorted by this column (e.g. include ORDER BY <col>). Each batch
	// becomes a sorted stream and parallelize() does a streaming k-way
	// merge across the batches. Not supported with hunt_id (the hunt
	// flow stream is unbounded; merge requires a bounded provider set).
	OrderBy   string `vfilter:"optional,field=order_by,doc=Column to merge sorted batch streams on. The inner sub-query MUST include ORDER BY <order_by> — output is undefined otherwise."`
	OrderDesc bool   `vfilter:"optional,field=order_desc,doc=Descending merge order."`

	// When GroupBy is set, the inner sub-query is expected to GROUP BY the
	// same columns. Each batch produces its per-batch aggregated rows;
	// parallelize() collects them into a hash table keyed on the GroupBy
	// columns and combines aggregate columns according to Aggregates,
	// emitting one row per unique key after all batches finish. Combiner
	// kinds are exact map-reduce reductions of VQL's five aggregators
	// (count, sum, min, max, enumerate). Not supported with hunt_id.
	GroupBy    []string          `vfilter:"optional,field=group_by,doc=Columns to group across batches. Must match the inner query's GROUP BY exactly."`
	Aggregates *ordereddict.Dict `vfilter:"optional,field=aggregates,doc=Map of output column -> combiner kind: count|sum|min|max|enumerate."`

	source_arg *SourcePluginArgs
}

func (self *ParallelPluginArgs) DetermineMode(
	ctx context.Context, config_obj *config_proto.Config,
	scope vfilter.Scope, args *ordereddict.Dict) error {
	self.source_arg = &SourcePluginArgs{
		ClientId:          self.ClientId,
		FlowId:            self.FlowId,
		HuntId:            self.HuntId,
		Artifact:          self.Artifact,
		Source:            self.Source,
		StartTime:         self.StartTime,
		EndTime:           self.EndTime,
		NotebookId:        self.NotebookId,
		NotebookCellId:    self.NotebookCellId,
		NotebookCellTable: self.NotebookCellTable,
	}
	return self.source_arg.DetermineMode(ctx, config_obj, scope, args)
}

type ParallelPlugin struct{}

func (self ParallelPlugin) Call(
	ctx context.Context,
	scope vfilter.Scope,
	args *ordereddict.Dict) <-chan vfilter.Row {
	output_chan := make(chan vfilter.Row)

	go func() {
		defer close(output_chan)
		defer vql_subsystem.RegisterMonitor(ctx, "parallel", args)()

		err := vql_subsystem.CheckAccess(scope, acls.READ_RESULTS)
		if err != nil {
			scope.Log("parallel: %s", err)
			return
		}

		arg := &ParallelPluginArgs{}
		config_obj, ok := vql_subsystem.GetServerConfig(scope)
		if !ok {
			scope.Log("parallel: Command can only run on the server")
			return
		}

		err = arg_parser.ExtractArgsWithContext(ctx, scope, args, arg)
		if err != nil {
			scope.Log("parallel: %v", err)
			return
		}

		// Determine the mode based on the args passed.
		err = arg.DetermineMode(ctx, config_obj, scope, args)
		if err != nil {
			scope.Log("parallel: %v", err)
			return
		}

		workers := arg.Workers
		if workers == 0 {
			// By default use all the cpus.
			workers = int64(runtime.NumCPU())
		}

		groupByOn := len(arg.GroupBy) > 0
		aggregatesOn := arg.Aggregates != nil && arg.Aggregates.Len() > 0
		if groupByOn != aggregatesOn {
			scope.Log("parallel: group_by and aggregates must be specified together")
			return
		}

		if groupByOn {
			combiners, err := parseCombiners(arg.GroupBy, arg.Aggregates)
			if err != nil {
				scope.Log("parallel: %v", err)
				return
			}
			runGroupByCombine(ctx, config_obj, scope, arg,
				combiners, workers, output_chan)
			return
		}

		if arg.OrderBy != "" {
			runOrderedMerge(ctx, config_obj, scope, arg, workers, output_chan)
			return
		}

		job_chan, err := breakIntoScopes(ctx, config_obj, scope, arg)
		if err != nil {
			scope.Log("parallel: %v", err)
			return
		}

		wg := sync.WaitGroup{}
		for i := int64(0); i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				for job := range job_chan {
					client_id, _ := job.GetString("ClientId")
					flow_id, _ := job.GetString("FlowId")

					subscope := scope.Copy()
					subscope.AppendVars(job)

					for row := range arg.Query.Eval(ctx, subscope) {
						// When operating on a hunt we tag each row
						// with its client id and flow id
						if arg.HuntId != "" {
							row_dict, ok := row.(*ordereddict.Dict)
							if ok {
								row_dict.Set("ClientId", client_id).
									Set("FlowId", flow_id)
							}
						}

						select {
						case <-ctx.Done():
							return
						case output_chan <- row:
						}
					}
				}
			}()
		}

		wg.Wait()
	}()

	return output_chan
}

func (self ParallelPlugin) Info(
	scope vfilter.Scope, type_map *vfilter.TypeMap) *vfilter.PluginInfo {
	return &vfilter.PluginInfo{
		Name: "parallelize",
		Doc: "Runs query on result batches in parallel. " +
			"When order_by is set, each batch's sub-query MUST emit rows " +
			"sorted by that column (e.g. include ORDER BY <col>); the plugin " +
			"performs a streaming k-way merge across batches so the global " +
			"output is sorted without a serial outer ORDER BY. Single-column " +
			"only. Works with hunt_id — the hunt's flow set is snapshotted " +
			"at query time (re-run the query for fresh data). " +
			"When group_by is set (with aggregates), the inner sub-query " +
			"should GROUP BY the same columns and use only the supported " +
			"aggregates (count, sum, min, max, enumerate); parallelize() " +
			"combines per-batch partial aggregates into a single global row " +
			"per key. Bare projections (columns neither group_by nor " +
			"aggregates) follow last-seen semantics matching vfilter's " +
			"GROUP BY. Mixed-type group keys form distinct buckets. Output " +
			"is fully buffered before emission. Works with hunt_id; rows " +
			"are tagged with ClientId/FlowId so those can appear in " +
			"group_by for per-client aggregation. " +
			"Compose group_by with order_by to sort the combined output.",
		ArgType:  type_map.AddType(scope, &ParallelPluginArgs{}),
		Metadata: vql.VQLMetadata().Permissions(acls.READ_RESULTS).Build(),
	}
}

// A testable utility function that breaks the request into a set of
// args to the source() plugins.
func breakIntoScopes(
	ctx context.Context,
	config_obj *config_proto.Config,
	scope vfilter.Scope,
	arg *ParallelPluginArgs) (<-chan *ordereddict.Dict, error) {

	// Handle hunts especially.
	if arg.source_arg.mode == MODE_HUNT_ARTIFACT {
		return breakHuntIntoScopes(ctx, config_obj, scope, arg)
	}

	// Other sources are strored in a single reader.  Depending on
	// the parameters, we need to get the reader from different
	// places.
	var err error
	var result_set_reader result_sets.ResultSetReader
	output_chan := make(chan *ordereddict.Dict)

	if arg.source_arg.mode == MODE_NOTEBOOK {
		result_set_reader, err = getNotebookResultSetReader(
			ctx, config_obj, scope, arg.source_arg)

	} else if arg.source_arg.mode == MODE_FLOW_ARTIFACT {
		result_set_reader, err = getFlowResultSetReader(
			ctx, config_obj, scope, arg.source_arg)

	} else {
		err = errors.New("Unknown mode")
	}

	if err != nil {
		close(output_chan)
		return output_chan, err
	}

	// Figure how large the result set is.
	total_rows := result_set_reader.TotalRows()
	result_set_reader.Close()

	go func() {
		defer close(output_chan)

		step_size := arg.BatchSize
		if step_size == 0 {
			step_size = total_rows / 10
			if step_size < 1000 {
				step_size = 1000
			}
		}

		for i := int64(0); i < total_rows; i += step_size {
			select {
			case <-ctx.Done():
				return

			case output_chan <- ordereddict.NewDict().
				Set("ClientId", arg.source_arg.ClientId).
				Set("FlowId", arg.source_arg.FlowId).

				// Mask hunt id since we already take
				// care of it in breakHuntIntoScopes
				// and we dont want source() plugin to
				// pick it up.
				Set("HuntId", "").
				Set("ArtifactName", arg.source_arg.Artifact).
				Set("StartTime", arg.source_arg.StartTime).
				Set("EndTime", arg.source_arg.EndTime).
				Set("NotebookId", arg.source_arg.NotebookId).
				Set("NotebookCellId", arg.source_arg.NotebookCellId).
				Set("NotebookCellTable", arg.source_arg.NotebookCellTable).
				Set("StartRow", i).
				Set("Limit", step_size):
			}
		}

	}()

	return output_chan, nil
}

func breakHuntIntoScopes(
	ctx context.Context,
	config_obj *config_proto.Config,
	scope vfilter.Scope,
	arg *ParallelPluginArgs) (<-chan *ordereddict.Dict, error) {

	output_chan := make(chan *ordereddict.Dict)
	go func() {
		defer close(output_chan)

		hunt_dispatcher, err := services.GetHuntDispatcher(config_obj)
		if err != nil {
			return
		}

		options := services.FlowSearchOptions{BasicInformation: true}
		flow_chan, _, err := hunt_dispatcher.GetFlows(
			ctx, config_obj, options, scope, arg.source_arg.HuntId, 0)
		if err != nil {
			return
		}

		for flow_details := range flow_chan {
			if flow_details == nil || flow_details.Context == nil {
				continue
			}

			client_id := flow_details.Context.ClientId
			flow_id := flow_details.Context.SessionId
			arg := &ParallelPluginArgs{
				Artifact:  arg.source_arg.Artifact,
				ClientId:  client_id,
				FlowId:    flow_id,
				Workers:   arg.Workers,
				BatchSize: arg.BatchSize,
			}

			err = arg.DetermineMode(ctx, config_obj, scope, nil)
			if err != nil {
				continue
			}

			flow_job, err := breakIntoScopes(ctx, config_obj, scope, arg)
			if err == nil {
				for job := range flow_job {
					select {
					case <-ctx.Done():
						return
					case output_chan <- job:
					}
				}
			}
		}

	}()

	return output_chan, nil
}

// runOrderedMerge handles the order_by code path: per-batch sorted streams
// fanned through N workers, k-way merged into output_chan. K = number of
// batches (NOT number of workers): each Query.Eval call produces one
// already-sorted run, and the merge contract requires each provider to be
// globally sorted on its own. Comparator semantics mirror
// vql/sorter/mergesort.go (scope.Associative + NIL→"" + scope.Lt/Gt) so
// behaviour matches the SORT plugin.
func runOrderedMerge(
	ctx context.Context,
	config_obj *config_proto.Config,
	scope vfilter.Scope,
	arg *ParallelPluginArgs,
	workers int64,
	output_chan chan vfilter.Row) {

	job_chan, err := breakIntoScopes(ctx, config_obj, scope, arg)
	if err != nil {
		scope.Log("parallel: %v", err)
		return
	}

	// Pre-enumerate batches. K is bounded (default ~10 from breakIntoScopes;
	// tunable via batch). Dict-only, memory is trivial.
	var jobs []*ordereddict.Dict
	for job := range job_chan {
		jobs = append(jobs, job)
	}

	if len(jobs) == 0 {
		return
	}

	// The merger seeds by reading one row from EVERY batch channel before
	// it can emit anything. If workers < len(jobs), the unstarted batches
	// never produce a row and the started ones eventually block writing to
	// their cap-64 channels — deadlock. So in the order_by path we promote
	// workers to at least len(jobs). K is bounded by breakIntoScopes
	// (default ~10), so the extra goroutines are cheap.
	if int(workers) < len(jobs) {
		workers = int64(len(jobs))
	}

	// One sorted stream per batch.
	batch_chans := make([]chan vfilter.Row, len(jobs))
	for i := range batch_chans {
		batch_chans[i] = make(chan vfilter.Row, 64)
	}

	// Internal job queue carries (batch_idx, dict). N workers drain it,
	// each writing into the matching batch channel and closing it on
	// completion. Eval per batch produces one sorted run.
	type indexedJob struct {
		idx int
		job *ordereddict.Dict
	}
	internal_jobs := make(chan indexedJob)
	go func() {
		defer close(internal_jobs)
		for i, j := range jobs {
			select {
			case <-ctx.Done():
				return
			case internal_jobs <- indexedJob{idx: i, job: j}:
			}
		}
	}()

	wg := sync.WaitGroup{}
	for i := int64(0); i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ij := range internal_jobs {
				func() {
					defer close(batch_chans[ij.idx])

					client_id, _ := ij.job.GetString("ClientId")
					flow_id, _ := ij.job.GetString("FlowId")

					subscope := scope.Copy()
					subscope.AppendVars(ij.job)

					for row := range arg.Query.Eval(ctx, subscope) {
						if arg.HuntId != "" {
							if rd, ok := row.(*ordereddict.Dict); ok {
								rd.Set("ClientId", client_id).
									Set("FlowId", flow_id)
							}
						}
						select {
						case <-ctx.Done():
							return
						case batch_chans[ij.idx] <- row:
						}
					}
				}()
			}
		}()
	}

	// Spawn a closer so the merge below sees channel closes even if a
	// batch is never picked up (shouldn't happen, but defensive).
	go func() {
		wg.Wait()
	}()

	mergeBatches(ctx, scope, arg.OrderBy, arg.OrderDesc,
		batch_chans, output_chan)
}

type mergeEntry struct {
	row      vfilter.Row
	key      types.Any
	batchIdx int
}

// readKey extracts the sort key from a row, normalising NIL → "" to match
// vql/sorter/mergesort.go:160-170.
func readKey(scope vfilter.Scope, row vfilter.Row, order_by string) types.Any {
	value, pres := scope.Associative(row, order_by)
	if !pres {
		scope.Log("parallel: order_by column %v not present in row", order_by)
	}
	if utils.IsNil(value) {
		return ""
	}
	return value
}

// mergeBatches consumes one row at a time from each batch channel, picks
// the global min (or max if desc) using scope.Lt/Gt, emits it, and refills
// from the same batch. Linear scan over K entries (K typically 10–100).
func mergeBatches(
	ctx context.Context,
	scope vfilter.Scope,
	order_by string,
	desc bool,
	batch_chans []chan vfilter.Row,
	output_chan chan vfilter.Row) {

	// Seed the active set: one row per batch, skipping any already empty.
	active := make([]*mergeEntry, 0, len(batch_chans))
	for i, ch := range batch_chans {
		select {
		case <-ctx.Done():
			return
		case row, ok := <-ch:
			if !ok {
				continue
			}
			active = append(active, &mergeEntry{
				row:      row,
				key:      readKey(scope, row, order_by),
				batchIdx: i,
			})
		}
	}

	for len(active) > 0 {
		// Find the winning entry. Mirrors mergesort.go:181-194 — desc
		// uses Gt, asc uses Lt.
		win := 0
		for i := 1; i < len(active); i++ {
			if desc {
				if scope.Gt(active[i].key, active[win].key) {
					win = i
				}
			} else {
				if scope.Lt(active[i].key, active[win].key) {
					win = i
				}
			}
		}

		select {
		case <-ctx.Done():
			return
		case output_chan <- active[win].row:
		}

		// Refill from the same batch.
		select {
		case <-ctx.Done():
			return
		case row, ok := <-batch_chans[active[win].batchIdx]:
			if !ok {
				// Batch exhausted: swap-delete.
				last := len(active) - 1
				active[win] = active[last]
				active = active[:last]
			} else {
				active[win].row = row
				active[win].key = readKey(scope, row, order_by)
			}
		}
	}
}

// combinerKind identifies one of VQL's five aggregators. Each is associative
// + commutative on its per-batch partial value, so parallelize() can fold
// partials from multiple batches into a single global value.
type combinerKind int

const (
	combinerCount combinerKind = iota
	combinerSum
	combinerMin
	combinerMax
	combinerEnumerate
)

type combinerSpec struct {
	groupBy []string
	aggs    []aggSpec
	aggSet  map[string]struct{} // for fast last-seen check on bare projections
	keySet  map[string]struct{} // group-by columns
}

type aggSpec struct {
	col  string
	kind combinerKind
}

func parseCombiners(group_by []string, aggregates *ordereddict.Dict) (
	*combinerSpec, error) {

	spec := &combinerSpec{
		groupBy: group_by,
		aggSet:  make(map[string]struct{}),
		keySet:  make(map[string]struct{}),
	}

	for _, k := range group_by {
		if _, dup := spec.keySet[k]; dup {
			return nil, fmt.Errorf("group_by column %q listed twice", k)
		}
		spec.keySet[k] = struct{}{}
	}

	for _, key := range aggregates.Keys() {
		if _, clash := spec.keySet[key]; clash {
			return nil, fmt.Errorf(
				"column %q appears in both group_by and aggregates", key)
		}
		raw, _ := aggregates.Get(key)
		name, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf(
				"aggregate %q value must be a string combiner kind, got %T",
				key, raw)
		}
		var kind combinerKind
		switch strings.ToLower(strings.TrimSpace(name)) {
		case "count":
			kind = combinerCount
		case "sum":
			kind = combinerSum
		case "min":
			kind = combinerMin
		case "max":
			kind = combinerMax
		case "enumerate":
			kind = combinerEnumerate
		default:
			return nil, fmt.Errorf(
				"aggregate %q: unknown combiner %q (want count|sum|min|max|enumerate)",
				key, name)
		}
		spec.aggs = append(spec.aggs, aggSpec{col: key, kind: kind})
		spec.aggSet[key] = struct{}{}
	}

	return spec, nil
}

// groupKey builds a stable hash key from the group-by tuple. JSON marshalling
// gives a unique stable representation: NULL → JSON null (its own bucket,
// matching SQL semantics) and mixed types remain distinct ("1" ≠ 1).
func groupKey(scope vfilter.Scope, row vfilter.Row, cols []string) (string, error) {
	tuple := make([]interface{}, len(cols))
	for i, c := range cols {
		v, _ := scope.Associative(row, c)
		if utils.IsNil(v) {
			tuple[i] = nil
			continue
		}
		tuple[i] = v
	}
	b, err := json.Marshal(tuple)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// combine folds two partial values for the same column. Mirrors the
// per-batch types produced by vfilter's aggregator implementations
// (functions/aggregates.go): count→uint64, sum→int64, min/max→raw, enumerate→[]types.Any.
func combine(scope vfilter.Scope, kind combinerKind, existing, incoming types.Any) types.Any {
	switch kind {
	case combinerCount:
		return toUint64(existing) + toUint64(incoming)
	case combinerSum:
		// Prefer int64 (vfilter's _SumFunction stores int64); fall back to
		// float64 if either side is non-integer numeric.
		if isFloat(existing) || isFloat(incoming) {
			return toFloat64(existing) + toFloat64(incoming)
		}
		return toInt64(existing) + toInt64(incoming)
	case combinerMin:
		if utils.IsNil(existing) {
			return incoming
		}
		if utils.IsNil(incoming) {
			return existing
		}
		if scope.Lt(incoming, existing) {
			return incoming
		}
		return existing
	case combinerMax:
		if utils.IsNil(existing) {
			return incoming
		}
		if utils.IsNil(incoming) {
			return existing
		}
		if scope.Gt(incoming, existing) {
			return incoming
		}
		return existing
	case combinerEnumerate:
		return appendEnumerate(existing, incoming)
	}
	return incoming
}

func toUint64(v types.Any) uint64 {
	if utils.IsNil(v) {
		return 0
	}
	switch n := v.(type) {
	case uint64:
		return n
	case int64:
		if n < 0 {
			return 0
		}
		return uint64(n)
	case int:
		if n < 0 {
			return 0
		}
		return uint64(n)
	case float64:
		if n < 0 {
			return 0
		}
		return uint64(n)
	}
	return 0
}

func toInt64(v types.Any) int64 {
	if utils.IsNil(v) {
		return 0
	}
	switch n := v.(type) {
	case int64:
		return n
	case uint64:
		return int64(n)
	case int:
		return int64(n)
	case float64:
		return int64(n)
	case float32:
		return int64(n)
	}
	return 0
}

func toFloat64(v types.Any) float64 {
	if utils.IsNil(v) {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int64:
		return float64(n)
	case uint64:
		return float64(n)
	case int:
		return float64(n)
	}
	return 0
}

func isFloat(v types.Any) bool {
	switch v.(type) {
	case float32, float64:
		return true
	}
	return false
}

func appendEnumerate(existing, incoming types.Any) types.Any {
	out := toAnySlice(existing)
	out = append(out, toAnySlice(incoming)...)
	return out
}

func toAnySlice(v types.Any) []types.Any {
	if utils.IsNil(v) {
		return nil
	}
	switch s := v.(type) {
	case []types.Any:
		return s
	case []interface{}:
		out := make([]types.Any, len(s))
		for i, x := range s {
			out[i] = x
		}
		return out
	}
	return []types.Any{v}
}

// runGroupByCombine handles the group_by code path. Per-batch worker dispatch
// mirrors runOrderedMerge: pre-enumerate batches, fan one Query.Eval per
// batch through N workers writing to per-batch channels. The consumer then
// drains every channel into a single hash table keyed on the group_by tuple,
// applying each declared combiner column-wise. After all batches close, the
// map is materialised, optionally sorted by order_by, and emitted.
//
// Memory: O(unique keys × row size). All output is buffered before emit —
// fundamental to GROUP BY (we cannot emit a key while another batch may
// still contribute to it). Hunt mode is supported: breakHuntIntoScopes
// reads a snapshot of the on-disk hunt-clients table at query time, so
// enumeration terminates like any other source.
func runGroupByCombine(
	ctx context.Context,
	config_obj *config_proto.Config,
	scope vfilter.Scope,
	arg *ParallelPluginArgs,
	spec *combinerSpec,
	workers int64,
	output_chan chan vfilter.Row) {

	job_chan, err := breakIntoScopes(ctx, config_obj, scope, arg)
	if err != nil {
		scope.Log("parallel: %v", err)
		return
	}

	var jobs []*ordereddict.Dict
	for job := range job_chan {
		jobs = append(jobs, job)
	}

	if len(jobs) == 0 {
		return
	}

	// One channel per batch. Unlike the order_by path, the consumer drains
	// channels independently (no k-way seed), so workers can be < len(jobs)
	// without deadlock.
	batch_chans := make([]chan vfilter.Row, len(jobs))
	for i := range batch_chans {
		batch_chans[i] = make(chan vfilter.Row, 64)
	}

	type indexedJob struct {
		idx int
		job *ordereddict.Dict
	}
	internal_jobs := make(chan indexedJob)
	go func() {
		defer close(internal_jobs)
		for i, j := range jobs {
			select {
			case <-ctx.Done():
				return
			case internal_jobs <- indexedJob{idx: i, job: j}:
			}
		}
	}()

	wg := sync.WaitGroup{}
	for i := int64(0); i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ij := range internal_jobs {
				func() {
					defer close(batch_chans[ij.idx])

					client_id, _ := ij.job.GetString("ClientId")
					flow_id, _ := ij.job.GetString("FlowId")

					subscope := scope.Copy()
					subscope.AppendVars(ij.job)

					for row := range arg.Query.Eval(ctx, subscope) {
						// Mirror the default path's hunt tagging so users
						// can include ClientId/FlowId in group_by.
						if arg.HuntId != "" {
							if rd, ok := row.(*ordereddict.Dict); ok {
								rd.Set("ClientId", client_id).
									Set("FlowId", flow_id)
							}
						}
						select {
						case <-ctx.Done():
							return
						case batch_chans[ij.idx] <- row:
						}
					}
				}()
			}
		}()
	}

	// Drain all batch channels into the combined map. We fan-in via a
	// single goroutine per batch chan into one local map; per-key writes
	// happen on the current goroutine after the fan-in goroutines exit.
	combined := make(map[string]*ordereddict.Dict)
	keyOrder := []string{}

	type item struct {
		key string
		row *ordereddict.Dict
	}
	merged := make(chan item)
	mwg := sync.WaitGroup{}
	for _, ch := range batch_chans {
		mwg.Add(1)
		go func(ch chan vfilter.Row) {
			defer mwg.Done()
			for row := range ch {
				dict, ok := row.(*ordereddict.Dict)
				if !ok {
					continue
				}
				key, err := groupKey(scope, dict, spec.groupBy)
				if err != nil {
					scope.Log("parallel: group_by key build: %v", err)
					continue
				}
				select {
				case <-ctx.Done():
					return
				case merged <- item{key: key, row: dict}:
				}
			}
		}(ch)
	}

	go func() {
		mwg.Wait()
		close(merged)
	}()

	for it := range merged {
		existing, ok := combined[it.key]
		if !ok {
			combined[it.key] = it.row
			keyOrder = append(keyOrder, it.key)
			continue
		}
		// Combine each declared aggregate column.
		for _, a := range spec.aggs {
			ev, _ := existing.Get(a.col)
			nv, _ := it.row.Get(a.col)
			existing.Update(a.col, combine(scope, a.kind, ev, nv))
		}
		// Last-seen for any column that is neither a group key nor a
		// declared aggregate (mirrors vfilter grouper.go:74-76).
		for _, c := range it.row.Keys() {
			if _, isKey := spec.keySet[c]; isKey {
				continue
			}
			if _, isAgg := spec.aggSet[c]; isAgg {
				continue
			}
			nv, _ := it.row.Get(c)
			existing.Update(c, nv)
		}
	}

	wg.Wait()

	// Emit. Optional order_by sorts the materialised slice using the same
	// scope.Lt/Gt comparator the merge path uses for parity with SORT.
	if arg.OrderBy != "" {
		sort.SliceStable(keyOrder, func(i, j int) bool {
			ai, _ := combined[keyOrder[i]].Get(arg.OrderBy)
			aj, _ := combined[keyOrder[j]].Get(arg.OrderBy)
			if utils.IsNil(ai) {
				ai = ""
			}
			if utils.IsNil(aj) {
				aj = ""
			}
			if arg.OrderDesc {
				return scope.Gt(ai, aj)
			}
			return scope.Lt(ai, aj)
		})
	}

	for _, k := range keyOrder {
		select {
		case <-ctx.Done():
			return
		case output_chan <- combined[k]:
		}
	}
}

func init() {
	vql_subsystem.RegisterPlugin(&ParallelPlugin{})
}
