package spanmetrics

import (
	"sync"

	spansschema "github.com/optikklabs/ingest/internal/ingestion/spans/schema"
	"github.com/optikklabs/ingest/internal/modules/spanaggregator/common"
)

type AggKey struct {
	TenantId               uint32
	Service                string
	Host                   string
	Pod                    string
	SpanName               string
	SpanKind               string
	StatusCode             string
	HttpRoute              string
	HttpStatusCode         string
	DbSystem               string
	MessagingSystem        string
	MessagingDestination   string
	MessagingConsumerGroup string
}

type Aggregator struct {
	mu    sync.Mutex
	state map[AggKey]*common.AggState
}

func NewAggregator() *Aggregator {
	return &Aggregator{
		state: make(map[AggKey]*common.AggState),
	}
}

func (a *Aggregator) Add(row *spansschema.Row) {
	a.mu.Lock()
	defer a.mu.Unlock()

	attrs := row.GetAttributes()
	key := AggKey{
		TenantId:               row.GetTenantId(),
		Service:                row.GetService(),
		Host:                   row.GetHost(),
		Pod:                    row.GetPod(),
		SpanName:               row.GetName(),
		SpanKind:               row.GetKindString(),
		StatusCode:             row.GetStatusCodeString(),
		HttpRoute:              row.GetHttpRoute(),
		HttpStatusCode:         row.GetResponseStatusCode(),
		DbSystem:               row.GetDbSystem(),
		MessagingSystem:        attrs["messaging.system"],
		MessagingDestination:   attrs["messaging.destination.name"],
		MessagingConsumerGroup: attrs["messaging.consumer.group.name"],
	}

	s, ok := a.state[key]
	if !ok {
		s = common.NewAggState()
		a.state[key] = s
	}

	durMs := float64(row.GetDurationNano()) / 1000000.0
	s.Add(durMs)
}

func (a *Aggregator) Drain() map[AggKey]*common.AggState {
	a.mu.Lock()
	defer a.mu.Unlock()
	stateToFlush := a.state
	a.state = make(map[AggKey]*common.AggState)
	return stateToFlush
}
