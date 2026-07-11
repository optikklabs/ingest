package servicegraph

import (
	"sync"

	"github.com/optikklabs/ingest/internal/modules/spanaggregator/common"
)

// EdgeAgg accumulates both latency histograms for one edge: Server (callee
// compute time) and Client (caller-perceived time, incl. network + queue), plus
// request/failure counts emitted as dedicated counters.
type EdgeAgg struct {
	Server *common.AggState
	Client *common.AggState
	Total  uint64
	Failed uint64
}

func newEdgeAgg() *EdgeAgg {
	return &EdgeAgg{
		Server: common.NewAggState(),
		Client: common.NewAggState(),
	}
}

type Aggregator struct {
	mu    sync.Mutex
	state map[EdgeKey]*EdgeAgg
}

func NewAggregator() *Aggregator {
	return &Aggregator{
		state: make(map[EdgeKey]*EdgeAgg),
	}
}

func (a *Aggregator) edge(key EdgeKey) *EdgeAgg {
	e, ok := a.state[key]
	if !ok {
		e = newEdgeAgg()
		a.state[key] = e
	}
	return e
}

// AddServer records the callee-side latency for an edge.
func (a *Aggregator) AddServer(key EdgeKey, durMs float64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.edge(key).Server.Add(durMs)
}

// AddClient records the caller-side latency for an edge.
func (a *Aggregator) AddClient(key EdgeKey, durMs float64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.edge(key).Client.Add(durMs)
}

// Record counts one request on an edge, marking it failed when applicable.
func (a *Aggregator) Record(key EdgeKey, failed bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	e := a.edge(key)
	e.Total++
	if failed {
		e.Failed++
	}
}

// Drain atomically returns the accumulated edge state and resets it, so the
// caller can publish a batch without blocking further aggregation.
func (a *Aggregator) Drain() map[EdgeKey]*EdgeAgg {
	a.mu.Lock()
	defer a.mu.Unlock()

	state := a.state
	a.state = make(map[EdgeKey]*EdgeAgg)
	return state
}
