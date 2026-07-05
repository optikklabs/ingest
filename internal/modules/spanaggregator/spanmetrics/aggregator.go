package spanmetrics

import (
	"sync"
	"time"

	"github.com/optikklabs/ingest/internal/modules/spanaggregator/common"
	spansschema "github.com/optikklabs/ingest/internal/ingestion/spans/schema"
)

type AggKey struct {
	TenantId       uint32
	Service        string
	SpanName       string
	SpanKind       string
	StatusCode     string
	HttpRoute      string
	HttpStatusCode string
	DbSystem       string
}

type Aggregator struct {
	mu        sync.Mutex
	lastFlush time.Time
	state     map[AggKey]*common.AggState
}

func NewAggregator() *Aggregator {
	return &Aggregator{
		lastFlush: time.Now(),
		state:     make(map[AggKey]*common.AggState),
	}
}

func (a *Aggregator) Add(row *spansschema.Row) {
	a.mu.Lock()
	defer a.mu.Unlock()

	key := AggKey{
		TenantId:       row.GetTenantId(),
		Service:        row.GetService(),
		SpanName:       row.GetName(),
		SpanKind:       row.GetKindString(),
		StatusCode:     row.GetStatusCodeString(),
		HttpRoute:      row.GetHttpRoute(),
		HttpStatusCode: row.GetResponseStatusCode(),
		DbSystem:       row.GetDbSystem(),
	}

	s, ok := a.state[key]
	if !ok {
		s = common.NewAggState()
		a.state[key] = s
	}

	durMs := float64(row.GetDurationNano()) / 1000000.0
	s.Add(durMs)
}

func (a *Aggregator) FlushIfReady(interval time.Duration) map[AggKey]*common.AggState {
	a.mu.Lock()
	defer a.mu.Unlock()

	if time.Since(a.lastFlush) < interval {
		return nil
	}

	stateToFlush := a.state
	a.state = make(map[AggKey]*common.AggState)
	a.lastFlush = time.Now()
	
	return stateToFlush
}
