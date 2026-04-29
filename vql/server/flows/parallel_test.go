package flows

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Velocidex/ordereddict"
	"github.com/stretchr/testify/suite"
	actions_proto "www.velocidex.com/golang/velociraptor/actions/proto"
	api_proto "www.velocidex.com/golang/velociraptor/api/proto"
	"www.velocidex.com/golang/velociraptor/file_store"
	"www.velocidex.com/golang/velociraptor/file_store/test_utils"
	flows_proto "www.velocidex.com/golang/velociraptor/flows/proto"
	"www.velocidex.com/golang/velociraptor/logging"
	"www.velocidex.com/golang/velociraptor/paths"
	"www.velocidex.com/golang/velociraptor/paths/artifacts"
	"www.velocidex.com/golang/velociraptor/result_sets"
	"www.velocidex.com/golang/velociraptor/services"
	"www.velocidex.com/golang/velociraptor/utils"
	"www.velocidex.com/golang/velociraptor/vql/acl_managers"
	"www.velocidex.com/golang/velociraptor/vtesting/assert"
	"www.velocidex.com/golang/velociraptor/vtesting/goldie"
	"www.velocidex.com/golang/vfilter"

	_ "www.velocidex.com/golang/velociraptor/result_sets/simple"
)

const (
	FORCE_REFRESH = true
)

var (
	testArtifact = `
name: Test.Artifact
`
)

type TestSuite struct {
	test_utils.TestSuite
	client_id, flow_id string
}

func (self *TestSuite) SetupTest() {
	self.ConfigObj = self.LoadConfig()
	self.ConfigObj.Services.HuntDispatcher = true

	self.TestSuite.SetupTest()
}

func (self *TestSuite) TestArtifactSource() {
	manager, err := services.GetRepositoryManager(self.ConfigObj)
	assert.NoError(self.T(), err)

	repository, err := manager.GetGlobalRepository(self.ConfigObj)
	assert.NoError(self.T(), err)

	_, err = repository.LoadYaml(testArtifact,
		services.ArtifactOptions{
			ValidateArtifact:  true,
			ArtifactIsBuiltIn: true})

	assert.NoError(self.T(), err)

	file_store_factory := file_store.GetFileStore(self.ConfigObj)

	path_manager, err := artifacts.NewArtifactPathManager(self.Ctx,
		self.ConfigObj, self.client_id, self.flow_id,
		"Test.Artifact")
	assert.NoError(self.T(), err)

	// Append logs to messages from previous packets.
	rs_writer, err := result_sets.NewResultSetWriter(
		file_store_factory, path_manager.Path(),
		nil, utils.SyncCompleter, true /* truncate */)
	assert.NoError(self.T(), err)

	for i := 0; i < 100; i++ {
		rs_writer.Write(ordereddict.NewDict().Set("Foo", i))
	}
	rs_writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	builder := services.ScopeBuilder{
		Config:     self.ConfigObj,
		ACLManager: acl_managers.NullACLManager{},
		Logger:     logging.NewPlainLogger(self.ConfigObj, &logging.FrontendComponent),
		Env: ordereddict.NewDict().
			Set("ClientId", self.client_id).
			Set("FlowId", self.flow_id),
	}
	scope := manager.BuildScope(builder)
	defer scope.Close()

	arg := &ParallelPluginArgs{
		Artifact:  "Test.Artifact",
		FlowId:    self.flow_id,
		ClientId:  self.client_id,
		BatchSize: 10,
	}
	err = arg.DetermineMode(ctx, self.ConfigObj, scope, nil)
	assert.NoError(self.T(), err)

	row_chan, err := breakIntoScopes(
		ctx, self.ConfigObj, scope, arg)
	assert.NoError(self.T(), err)

	for args := range row_chan {
		start_row, _ := args.Get("StartRow")
		limit, _ := args.Get("Limit")
		fmt.Printf("Section %v-%v\n", start_row, limit)
	}

	vql, err := vfilter.Parse(`
SELECT * FROM parallelize(
     artifact='Test.Artifact',
     client_id=ClientId, flow_id=FlowId,
     batch=10, workers=10,
     query={
        SELECT Foo FROM source()
     })
`)
	assert.NoError(self.T(), err)

	result := make([]*ordereddict.Dict, 0)
	for row := range vql.Eval(ctx, scope) {
		result = append(result, row.(*ordereddict.Dict))
	}

	assert.Equal(self.T(), 100, len(result))

	// test_utils.GetMemoryFileStore(self.T(), self.ConfigObj).Debug()
}

func (self *TestSuite) TestHuntsSource() {
	manager, err := services.GetRepositoryManager(self.ConfigObj)
	assert.NoError(self.T(), err)

	repository, err := manager.GetGlobalRepository(self.ConfigObj)
	assert.NoError(self.T(), err)

	_, err = repository.LoadYaml(testArtifact, services.ArtifactOptions{
		ValidateArtifact:  true,
		ArtifactIsBuiltIn: true})

	assert.NoError(self.T(), err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	hunt_dispatcher, err := services.GetHuntDispatcher(self.ConfigObj)
	assert.NoError(self.T(), err)

	new_hunt, err := hunt_dispatcher.CreateHunt(ctx,
		self.ConfigObj, acl_managers.NullACLManager{},
		&api_proto.Hunt{
			StartRequest: &flows_proto.ArtifactCollectorArgs{
				Artifacts: []string{"Test.Artifact"},
			},
		})
	assert.NoError(self.T(), err)

	launcher, err := services.GetLauncher(self.ConfigObj)
	assert.NoError(self.T(), err)

	hunt_id := new_hunt.HuntId
	file_store_factory := file_store.GetFileStore(self.ConfigObj)
	hunt_path_manager := paths.NewHuntPathManager(hunt_id).Clients()
	hunt_rs_writer, err := result_sets.NewResultSetWriter(
		file_store_factory, hunt_path_manager, nil,
		utils.SyncCompleter, true /* truncate */)

	gen := &ConstantIdGenerator{}
	defer utils.SetIdGenerator(gen)()

	client_info_manager, err := services.GetClientInfoManager(self.ConfigObj)
	assert.NoError(self.T(), err)

	for client_number := 0; client_number < 10; client_number++ {
		gen.SetId(fmt.Sprintf("%s_%v", self.flow_id, client_number))

		client_id := fmt.Sprintf("%s_%v", self.client_id, client_number)
		err = client_info_manager.Set(self.Ctx, &services.ClientInfo{
			&actions_proto.ClientInfo{
				ClientId: client_id,
			}})
		assert.NoError(self.T(), err)

		flow_id, err := launcher.ScheduleArtifactCollection(self.Ctx,
			self.ConfigObj, acl_managers.NullACLManager{},
			repository, &flows_proto.ArtifactCollectorArgs{
				ClientId:  client_id,
				Creator:   utils.GetSuperuserName(self.ConfigObj),
				Artifacts: []string{"Test.Artifact"},
			}, nil)
		assert.NoError(self.T(), err)

		hunt_rs_writer.Write(ordereddict.NewDict().
			Set("ClientId", client_id).
			Set("HuntId", hunt_id).
			Set("FlowId", flow_id).
			Set("_ts", 0).
			Set("Timestamp", 0))

		path_manager, err := artifacts.NewArtifactPathManager(self.Ctx,
			self.ConfigObj, client_id, flow_id, "Test.Artifact")
		assert.NoError(self.T(), err)

		// Append logs to messages from previous packets.
		rs_writer, err := result_sets.NewResultSetWriter(
			file_store_factory, path_manager.Path(),
			nil, utils.SyncCompleter, true /* truncate */)
		assert.NoError(self.T(), err)

		for i := 0; i < 100; i++ {
			rs_writer.Write(ordereddict.NewDict().
				Set("Foo", fmt.Sprintf("%v-%v", flow_id, i)))
		}
		rs_writer.Close()
	}

	hunt_rs_writer.Close()
	hunt_dispatcher.Refresh(self.Ctx, self.ConfigObj, FORCE_REFRESH)

	builder := services.ScopeBuilder{
		Config:     self.ConfigObj,
		ACLManager: acl_managers.NullACLManager{},
		Logger:     logging.NewPlainLogger(self.ConfigObj, &logging.FrontendComponent),
		Env:        ordereddict.NewDict().Set("MyHuntId", hunt_id),
	}
	scope := manager.BuildScope(builder)
	defer scope.Close()

	arg := &ParallelPluginArgs{
		Artifact:  "Test.Artifact",
		HuntId:    hunt_id,
		BatchSize: 10,
	}
	err = arg.DetermineMode(ctx, self.ConfigObj, scope, nil)
	assert.NoError(self.T(), err)

	row_chan, err := breakIntoScopes(ctx, self.ConfigObj, scope, arg)
	assert.NoError(self.T(), err)

	sections := []string{}
	for args := range row_chan {
		start_row, _ := args.Get("StartRow")
		limit, _ := args.Get("Limit")
		flow_id, _ := args.Get("FlowId")
		sections = append(sections,
			fmt.Sprintf("Section %v: %v-%v\n", flow_id, start_row, limit))
	}

	// Stable sort the section list so we can goldie it.
	sort.Strings(sections)

	goldie.AssertJson(self.T(), "TestHuntsSource", sections)

	vql, err := vfilter.Parse(`
SELECT * FROM parallelize(
     artifact='Test.Artifact',
     hunt_id=MyHuntId,
     batch=10, workers=10,
     query={
        SELECT Foo FROM source()
     })
`)
	assert.NoError(self.T(), err)

	result := make([]*ordereddict.Dict, 0)
	for row := range vql.Eval(ctx, scope) {
		result = append(result, row.(*ordereddict.Dict))
	}

	assert.Equal(self.T(), 1000, len(result))
}

// orderByEnv prepares a result set with the given rows and returns a scope
// suitable for running parallelize() against Test.Artifact.
func (self *TestSuite) orderByEnv(ctx context.Context, rows []*ordereddict.Dict) vfilter.Scope {
	manager, err := services.GetRepositoryManager(self.ConfigObj)
	assert.NoError(self.T(), err)

	repository, err := manager.GetGlobalRepository(self.ConfigObj)
	assert.NoError(self.T(), err)

	_, err = repository.LoadYaml(testArtifact, services.ArtifactOptions{
		ValidateArtifact:  true,
		ArtifactIsBuiltIn: true})
	assert.NoError(self.T(), err)

	file_store_factory := file_store.GetFileStore(self.ConfigObj)
	path_manager, err := artifacts.NewArtifactPathManager(self.Ctx,
		self.ConfigObj, self.client_id, self.flow_id, "Test.Artifact")
	assert.NoError(self.T(), err)

	rs_writer, err := result_sets.NewResultSetWriter(
		file_store_factory, path_manager.Path(),
		nil, utils.SyncCompleter, true)
	assert.NoError(self.T(), err)

	for _, r := range rows {
		rs_writer.Write(r)
	}
	rs_writer.Close()

	builder := services.ScopeBuilder{
		Config:     self.ConfigObj,
		ACLManager: acl_managers.NullACLManager{},
		Logger:     logging.NewPlainLogger(self.ConfigObj, &logging.FrontendComponent),
		Env: ordereddict.NewDict().
			Set("ClientId", self.client_id).
			Set("FlowId", self.flow_id),
	}
	return manager.BuildScope(builder)
}

// TestOrderByParityWithSort runs the same dataset through SORT and through
// parallelize(order_by=...) and asserts byte-equal output. This is the
// load-bearing test: it forces num_batches > num_workers so each worker
// handles multiple sorted batches — the case the per-batch channel
// refactor exists to fix.
func (self *TestSuite) TestOrderByParityWithSort() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	// Mixed types + nulls, shuffled.
	values := []interface{}{
		"banana", 3, nil, "apple", 1, "cherry", 2, nil, 5, "date", 4, nil,
		7, "elderberry", 6, "fig", 8, 9, "grape", 10,
	}
	rows := make([]*ordereddict.Dict, 0, len(values))
	for i, v := range values {
		rows = append(rows, ordereddict.NewDict().Set("X", v).Set("Idx", i))
	}

	scope := self.orderByEnv(ctx, rows)
	defer scope.Close()

	// SORT baseline.
	sortQ, err := vfilter.Parse(`
SELECT * FROM source(artifact='Test.Artifact',
                     client_id=ClientId, flow_id=FlowId)
ORDER BY X
`)
	assert.NoError(self.T(), err)

	expected := []*ordereddict.Dict{}
	for row := range sortQ.Eval(ctx, scope) {
		expected = append(expected, row.(*ordereddict.Dict))
	}

	// parallelize with batch=4 over 20 rows → K=5; workers=2 → forces
	// multi-batch-per-worker.
	parQ, err := vfilter.Parse(`
SELECT * FROM parallelize(
    artifact='Test.Artifact',
    client_id=ClientId, flow_id=FlowId,
    batch=4, workers=2,
    order_by='X',
    query={
        SELECT * FROM source() ORDER BY X
    })
`)
	assert.NoError(self.T(), err)

	got := []*ordereddict.Dict{}
	for row := range parQ.Eval(ctx, scope) {
		got = append(got, row.(*ordereddict.Dict))
	}

	assert.Equal(self.T(), len(expected), len(got))
	for i := range expected {
		ev, _ := expected[i].Get("X")
		gv, _ := got[i].Get("X")
		assert.Equal(self.T(),
			fmt.Sprintf("%v", ev), fmt.Sprintf("%v", gv),
			fmt.Sprintf("row %d: expected X=%v got X=%v", i, ev, gv))
	}
}

func (self *TestSuite) TestOrderByAsc() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	const N = 1000
	rows := make([]*ordereddict.Dict, 0, N)
	for i := 0; i < N; i++ {
		// Shuffled order via prime stride.
		rows = append(rows, ordereddict.NewDict().Set("X", (i*37)%N))
	}

	scope := self.orderByEnv(ctx, rows)
	defer scope.Close()

	q, err := vfilter.Parse(`
SELECT X FROM parallelize(
    artifact='Test.Artifact',
    client_id=ClientId, flow_id=FlowId,
    batch=100, workers=4,
    order_by='X',
    query={ SELECT X FROM source() ORDER BY X })
`)
	assert.NoError(self.T(), err)

	got := []int{}
	for row := range q.Eval(ctx, scope) {
		x, _ := row.(*ordereddict.Dict).GetInt64("X")
		got = append(got, int(x))
	}

	assert.Equal(self.T(), N, len(got))
	for i := 1; i < len(got); i++ {
		if got[i] < got[i-1] {
			self.T().Fatalf("not sorted at %d: %d < %d", i, got[i], got[i-1])
		}
	}
	// All values present exactly once.
	seen := make(map[int]bool)
	for _, v := range got {
		assert.False(self.T(), seen[v], fmt.Sprintf("duplicate: %d", v))
		seen[v] = true
	}
}

func (self *TestSuite) TestOrderByDesc() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	const N = 200
	rows := make([]*ordereddict.Dict, 0, N)
	for i := 0; i < N; i++ {
		rows = append(rows, ordereddict.NewDict().Set("X", (i*17)%N))
	}

	scope := self.orderByEnv(ctx, rows)
	defer scope.Close()

	q, err := vfilter.Parse(`
SELECT X FROM parallelize(
    artifact='Test.Artifact',
    client_id=ClientId, flow_id=FlowId,
    batch=20, workers=3,
    order_by='X', order_desc=TRUE,
    query={ SELECT X FROM source() ORDER BY X DESC })
`)
	assert.NoError(self.T(), err)

	got := []int{}
	for row := range q.Eval(ctx, scope) {
		x, _ := row.(*ordereddict.Dict).GetInt64("X")
		got = append(got, int(x))
	}

	assert.Equal(self.T(), N, len(got))
	for i := 1; i < len(got); i++ {
		if got[i] > got[i-1] {
			self.T().Fatalf("not desc-sorted at %d: %d > %d", i, got[i], got[i-1])
		}
	}
}

// TestOrderByEmptyAndUndersized covers (a) no rows and (b) more workers
// than batches — the worker-imbalance corner the design called out.
func (self *TestSuite) TestOrderByEmptyAndUndersized() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	// Three rows over workers=8 — most workers idle.
	rows := []*ordereddict.Dict{
		ordereddict.NewDict().Set("X", 3),
		ordereddict.NewDict().Set("X", 1),
		ordereddict.NewDict().Set("X", 2),
	}
	scope := self.orderByEnv(ctx, rows)
	defer scope.Close()

	q, err := vfilter.Parse(`
SELECT X FROM parallelize(
    artifact='Test.Artifact',
    client_id=ClientId, flow_id=FlowId,
    batch=1, workers=8,
    order_by='X',
    query={ SELECT X FROM source() ORDER BY X })
`)
	assert.NoError(self.T(), err)

	got := []int{}
	for row := range q.Eval(ctx, scope) {
		x, _ := row.(*ordereddict.Dict).GetInt64("X")
		got = append(got, int(x))
	}
	assert.Equal(self.T(), []int{1, 2, 3}, got)
}

// TestOrderByWorkersOne — workers=1 still runs the merge (K=num_batches,
// not num_workers); single-worker output across multiple batches must
// still be globally sorted.
func (self *TestSuite) TestOrderByWorkersOne() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	const N = 100
	rows := make([]*ordereddict.Dict, 0, N)
	for i := 0; i < N; i++ {
		rows = append(rows, ordereddict.NewDict().Set("X", (i*7)%N))
	}
	scope := self.orderByEnv(ctx, rows)
	defer scope.Close()

	q, err := vfilter.Parse(`
SELECT X FROM parallelize(
    artifact='Test.Artifact',
    client_id=ClientId, flow_id=FlowId,
    batch=10, workers=1,
    order_by='X',
    query={ SELECT X FROM source() ORDER BY X })
`)
	assert.NoError(self.T(), err)

	got := []int{}
	for row := range q.Eval(ctx, scope) {
		x, _ := row.(*ordereddict.Dict).GetInt64("X")
		got = append(got, int(x))
	}
	assert.Equal(self.T(), N, len(got))
	for i := 1; i < len(got); i++ {
		if got[i] < got[i-1] {
			self.T().Fatalf("not sorted at %d: %d < %d", i, got[i], got[i-1])
		}
	}
}

// TestOrderByCancellation — cancelling ctx mid-stream must not deadlock or
// panic.
func (self *TestSuite) TestOrderByCancellation() {
	ctx, cancel := context.WithCancel(context.Background())

	const N = 1000
	rows := make([]*ordereddict.Dict, 0, N)
	for i := 0; i < N; i++ {
		rows = append(rows, ordereddict.NewDict().Set("X", i))
	}
	scope := self.orderByEnv(ctx, rows)
	defer scope.Close()

	q, err := vfilter.Parse(`
SELECT X FROM parallelize(
    artifact='Test.Artifact',
    client_id=ClientId, flow_id=FlowId,
    batch=10, workers=4,
    order_by='X',
    query={ SELECT X FROM source() ORDER BY X })
`)
	assert.NoError(self.T(), err)

	count := 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range q.Eval(ctx, scope) {
			count++
			if count == 5 {
				cancel()
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		cancel()
		self.T().Fatal("query did not exit within 10s of cancel")
	}
}

// TestOrderByEmptyHunt — order_by with a hunt that has no flows must not
// hang; it just emits zero rows. (Order_by is allowed with hunt_id.)
func (self *TestSuite) TestOrderByEmptyHunt() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	scope := self.orderByEnv(ctx, []*ordereddict.Dict{
		ordereddict.NewDict().Set("X", 1),
	})
	defer scope.Close()

	q, err := vfilter.Parse(`
SELECT X FROM parallelize(
    artifact='Test.Artifact',
    hunt_id='H.NONEXISTENT',
    batch=10, workers=2,
    order_by='X',
    query={ SELECT X FROM source() ORDER BY X })
`)
	assert.NoError(self.T(), err)

	got := []*ordereddict.Dict{}
	for row := range q.Eval(ctx, scope) {
		got = append(got, row.(*ordereddict.Dict))
	}
	assert.Equal(self.T(), 0, len(got))
}

// TestGroupByCount — most fundamental case. 1000 rows split across batches;
// each unique key must emit exactly once with the global count.
func (self *TestSuite) TestGroupByCount() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	const N = 1000
	rows := make([]*ordereddict.Dict, 0, N)
	for i := 0; i < N; i++ {
		rows = append(rows, ordereddict.NewDict().Set("K", i%4))
	}
	scope := self.orderByEnv(ctx, rows)
	defer scope.Close()

	q, err := vfilter.Parse(`
SELECT K, C FROM parallelize(
    artifact='Test.Artifact',
    client_id=ClientId, flow_id=FlowId,
    batch=100, workers=4,
    group_by=['K'],
    aggregates=dict(C='count'),
    query={ SELECT K, count() AS C FROM source() GROUP BY K })
`)
	assert.NoError(self.T(), err)

	got := map[int64]uint64{}
	for row := range q.Eval(ctx, scope) {
		k, _ := row.(*ordereddict.Dict).GetInt64("K")
		v, _ := row.(*ordereddict.Dict).Get("C")
		got[k] = toU64(v)
	}
	assert.Equal(self.T(), 4, len(got))
	for k, v := range got {
		assert.Equal(self.T(), uint64(N/4), v,
			fmt.Sprintf("key %d count %d", k, v))
	}
}

// TestGroupByAllAggregators — exercise every combiner in one query against
// a serial-GROUP BY baseline.
func (self *TestSuite) TestGroupByAllAggregators() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	const N = 600
	rows := make([]*ordereddict.Dict, 0, N)
	for i := 0; i < N; i++ {
		rows = append(rows, ordereddict.NewDict().
			Set("K", i%5).
			Set("V", int64(i)))
	}
	scope := self.orderByEnv(ctx, rows)
	defer scope.Close()

	baselineQ, err := vfilter.Parse(`
SELECT K,
       count()       AS C,
       sum(item=V)   AS S,
       min(item=V)   AS Mn,
       max(item=V)   AS Mx
FROM source(artifact='Test.Artifact', client_id=ClientId, flow_id=FlowId)
GROUP BY K
`)
	assert.NoError(self.T(), err)

	type bucket struct {
		c        uint64
		s, mn, mx int64
	}
	expected := map[int64]*bucket{}
	for row := range baselineQ.Eval(ctx, scope) {
		k, _ := row.(*ordereddict.Dict).GetInt64("K")
		c, _ := row.(*ordereddict.Dict).Get("C")
		s, _ := row.(*ordereddict.Dict).Get("S")
		mn, _ := row.(*ordereddict.Dict).Get("Mn")
		mx, _ := row.(*ordereddict.Dict).Get("Mx")
		expected[k] = &bucket{toU64(c), toI64(s), toI64(mn), toI64(mx)}
	}

	parQ, err := vfilter.Parse(`
SELECT K, C, S, Mn, Mx FROM parallelize(
    artifact='Test.Artifact', client_id=ClientId, flow_id=FlowId,
    batch=50, workers=3,
    group_by=['K'],
    aggregates=dict(C='count', S='sum', Mn='min', Mx='max'),
    query={ SELECT K, count() AS C, sum(item=V) AS S,
                   min(item=V) AS Mn, max(item=V) AS Mx
            FROM source() GROUP BY K })
`)
	assert.NoError(self.T(), err)

	got := map[int64]*bucket{}
	for row := range parQ.Eval(ctx, scope) {
		k, _ := row.(*ordereddict.Dict).GetInt64("K")
		c, _ := row.(*ordereddict.Dict).Get("C")
		s, _ := row.(*ordereddict.Dict).Get("S")
		mn, _ := row.(*ordereddict.Dict).Get("Mn")
		mx, _ := row.(*ordereddict.Dict).Get("Mx")
		got[k] = &bucket{toU64(c), toI64(s), toI64(mn), toI64(mx)}
	}

	assert.Equal(self.T(), len(expected), len(got))
	for k, e := range expected {
		g, ok := got[k]
		assert.True(self.T(), ok, fmt.Sprintf("missing key %d", k))
		assert.Equal(self.T(), e.c, g.c, fmt.Sprintf("count k=%d", k))
		assert.Equal(self.T(), e.s, g.s, fmt.Sprintf("sum k=%d", k))
		assert.Equal(self.T(), e.mn, g.mn, fmt.Sprintf("min k=%d", k))
		assert.Equal(self.T(), e.mx, g.mx, fmt.Sprintf("max k=%d", k))
	}
}

// TestGroupByEnumerate — concatenated lists must contain all values across
// all batches (order within group is not guaranteed).
func (self *TestSuite) TestGroupByEnumerate() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	const N = 100
	rows := make([]*ordereddict.Dict, 0, N)
	for i := 0; i < N; i++ {
		rows = append(rows, ordereddict.NewDict().
			Set("K", i%4).
			Set("Name", fmt.Sprintf("n%d", i)))
	}
	scope := self.orderByEnv(ctx, rows)
	defer scope.Close()

	q, err := vfilter.Parse(`
SELECT K, Names FROM parallelize(
    artifact='Test.Artifact', client_id=ClientId, flow_id=FlowId,
    batch=10, workers=3,
    group_by=['K'],
    aggregates=dict(Names='enumerate'),
    query={ SELECT K, enumerate(items=Name) AS Names FROM source() GROUP BY K })
`)
	assert.NoError(self.T(), err)

	got := map[int64]int{}
	for row := range q.Eval(ctx, scope) {
		k, _ := row.(*ordereddict.Dict).GetInt64("K")
		v, _ := row.(*ordereddict.Dict).Get("Names")
		switch s := v.(type) {
		case []interface{}:
			got[k] = len(s)
		case []vfilter.Any:
			got[k] = len(s)
		default:
			self.T().Fatalf("Names not a slice: %T", v)
		}
	}
	for k, n := range got {
		assert.Equal(self.T(), N/4, n, fmt.Sprintf("k=%d", k))
	}
}

// TestGroupByMultiColumn — group_by tuple of two columns.
func (self *TestSuite) TestGroupByMultiColumn() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	rows := []*ordereddict.Dict{}
	for i := 0; i < 200; i++ {
		rows = append(rows, ordereddict.NewDict().
			Set("A", i%2).
			Set("B", i%3))
	}
	scope := self.orderByEnv(ctx, rows)
	defer scope.Close()

	q, err := vfilter.Parse(`
SELECT A, B, C FROM parallelize(
    artifact='Test.Artifact', client_id=ClientId, flow_id=FlowId,
    batch=20, workers=2,
    group_by=['A','B'],
    aggregates=dict(C='count'),
    query={ SELECT A, B, count() AS C FROM source() GROUP BY A, B })
`)
	assert.NoError(self.T(), err)

	keys := map[string]uint64{}
	for row := range q.Eval(ctx, scope) {
		a, _ := row.(*ordereddict.Dict).GetInt64("A")
		b, _ := row.(*ordereddict.Dict).GetInt64("B")
		c, _ := row.(*ordereddict.Dict).Get("C")
		keys[fmt.Sprintf("%d,%d", a, b)] = toU64(c)
	}
	assert.Equal(self.T(), 6, len(keys)) // 2 × 3
	var total uint64
	for _, v := range keys {
		total += v
	}
	assert.Equal(self.T(), uint64(200), total)
}

// TestGroupByComposeOrderBy — group_by + order_by sorts the combined output.
func (self *TestSuite) TestGroupByComposeOrderBy() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	rows := []*ordereddict.Dict{}
	weights := map[int]int{0: 50, 1: 30, 2: 80, 3: 10}
	for k, n := range weights {
		for i := 0; i < n; i++ {
			rows = append(rows, ordereddict.NewDict().Set("K", k))
		}
	}
	scope := self.orderByEnv(ctx, rows)
	defer scope.Close()

	q, err := vfilter.Parse(`
SELECT K, C FROM parallelize(
    artifact='Test.Artifact', client_id=ClientId, flow_id=FlowId,
    batch=20, workers=2,
    group_by=['K'],
    aggregates=dict(C='count'),
    order_by='C',
    query={ SELECT K, count() AS C FROM source() GROUP BY K })
`)
	assert.NoError(self.T(), err)

	counts := []uint64{}
	for row := range q.Eval(ctx, scope) {
		c, _ := row.(*ordereddict.Dict).Get("C")
		counts = append(counts, toU64(c))
	}
	assert.Equal(self.T(), 4, len(counts))
	for i := 1; i < len(counts); i++ {
		if counts[i] < counts[i-1] {
			self.T().Fatalf("not sorted: %v", counts)
		}
	}
}

// TestGroupByValidation — misuse must error out with a log message and
// emit no rows.
func (self *TestSuite) TestGroupByValidation() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	scope := self.orderByEnv(ctx, []*ordereddict.Dict{
		ordereddict.NewDict().Set("K", 1),
	})
	defer scope.Close()

	cases := []string{
		// group_by without aggregates
		`SELECT * FROM parallelize(
            artifact='Test.Artifact', client_id=ClientId, flow_id=FlowId,
            group_by=['K'],
            query={ SELECT K, count() AS C FROM source() GROUP BY K })`,
		// aggregates without group_by
		`SELECT * FROM parallelize(
            artifact='Test.Artifact', client_id=ClientId, flow_id=FlowId,
            aggregates=dict(C='count'),
            query={ SELECT K, count() AS C FROM source() GROUP BY K })`,
		// unknown combiner
		`SELECT * FROM parallelize(
            artifact='Test.Artifact', client_id=ClientId, flow_id=FlowId,
            group_by=['K'], aggregates=dict(C='avg'),
            query={ SELECT K, count() AS C FROM source() GROUP BY K })`,
		// column in both lists
		`SELECT * FROM parallelize(
            artifact='Test.Artifact', client_id=ClientId, flow_id=FlowId,
            group_by=['K'], aggregates=dict(K='count'),
            query={ SELECT K, count() AS C FROM source() GROUP BY K })`,
	}

	for i, src := range cases {
		q, err := vfilter.Parse(src)
		assert.NoError(self.T(), err)
		got := 0
		for range q.Eval(ctx, scope) {
			got++
		}
		assert.Equal(self.T(), 0, got, fmt.Sprintf("case %d emitted rows", i))
	}
}

func toU64(v interface{}) uint64 {
	switch n := v.(type) {
	case uint64:
		return n
	case int64:
		return uint64(n)
	case int:
		return uint64(n)
	}
	return 0
}

func toI64(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case uint64:
		return int64(n)
	case int:
		return int64(n)
	}
	return 0
}

func TestParallelPlugin(t *testing.T) {
	suite.Run(t, &TestSuite{
		client_id: "C.123",
		flow_id:   "F.123",
	})
}

type ConstantIdGenerator struct {
	mu sync.Mutex
	id string
}

func (self *ConstantIdGenerator) Next(client_id string) string {
	self.mu.Lock()
	defer self.mu.Unlock()

	return self.id
}

func (self *ConstantIdGenerator) SetId(id string) {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.id = id
}
