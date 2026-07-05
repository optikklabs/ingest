package servicegraph

import (
	"sync"
	"time"

	"github.com/optikklabs/ingest/internal/modules/spanaggregator/common"
)

type EdgeKey struct {
	TenantId   uint32
	Client     string
	Server     string
	StatusCode string
}

type Aggregator struct {
	mu        sync.Mutex
	lastFlush time.Time
	state     map[EdgeKey]*common.AggState
}

func NewAggregator() *Aggregator {
	return &Aggregator{
		lastFlush: time.Now(),
		state:     make(map[EdgeKey]*common.AggState),
	}
}

func (a *Aggregator) Add(key EdgeKey, durMs float64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	s, ok := a.state[key]
	if !ok {
		s = common.NewAggState()
		a.state[key] = s
	}

	s.Add(durMs)
}

func (a *Aggregator) FlushIfReady(interval time.Duration) map[EdgeKey]*common.AggState {
	a.mu.Lock()
	defer a.mu.Unlock()

	if time.Since(a.lastFlush) < interval {
		return nil
	}

	stateToFlush := a.state
	a.state = make(map[EdgeKey]*common.AggState)
	a.lastFlush = time.Now()
	
	return stateToFlush
}
