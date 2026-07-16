package servicegraph

import (
	"context"
	"testing"
	"time"

	spansschema "github.com/optikklabs/ingest/internal/ingestion/spans/schema"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

func rec(t *testing.T, row *spansschema.Row) *kgo.Record {
	t.Helper()
	b, err := proto.Marshal(row)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return &kgo.Record{Value: b}
}

func newTestConsumer() *Consumer {
	return &Consumer{store: NewStore(), aggregator: NewAggregator()}
}

// A CLIENT+SERVER pair must produce one edge carrying both the server-side and
// client-side latency, each in its own histogram with exact count and sum.
func TestPairEmitsServerAndClientHistograms(t *testing.T) {
	c := newTestConsumer()

	client := &spansschema.Row{
		TenantId: 7, TraceId: "trace-1", SpanId: "span-client",
		Service: "gateway", KindString: "CLIENT", DurationNano: 30_000_000, // 30ms
	}
	server := &spansschema.Row{
		TenantId: 7, TraceId: "trace-1", SpanId: "span-server", ParentSpanId: "span-client",
		Service: "orders", KindString: "SERVER", DurationNano: 20_000_000, // 20ms
		StatusCodeString: "STATUS_CODE_OK",
	}

	if err := c.handle(context.Background(), []*kgo.Record{rec(t, client), rec(t, server)}); err != nil {
		t.Fatalf("handle: %v", err)
	}

	state := c.aggregator.Drain()
	if len(state) != 1 {
		t.Fatalf("want 1 edge, got %d", len(state))
	}

	key := EdgeKey{TenantId: 7, Client: "gateway", Server: "orders"}
	e, ok := state[key]
	if !ok {
		t.Fatalf("edge %+v not found; got %+v", key, state)
	}
	if e.Server.Count != 1 || e.Server.Sum != 20 {
		t.Errorf("server hist: want count=1 sum=20, got count=%d sum=%v", e.Server.Count, e.Server.Sum)
	}
	if e.Client.Count != 1 || e.Client.Sum != 30 {
		t.Errorf("client hist: want count=1 sum=30, got count=%d sum=%v", e.Client.Count, e.Client.Sum)
	}
	if e.Total != 1 || e.Failed != 0 {
		t.Errorf("counters: want total=1 failed=0, got total=%d failed=%d", e.Total, e.Failed)
	}
}

// An errored pair must count one failed request; edge attrs must not carry a
// status.code dimension (error lives in the failed counter, not a label).
func TestErroredPairCountsFailure(t *testing.T) {
	c := newTestConsumer()

	client := &spansschema.Row{
		TenantId: 7, TraceId: "trace-err", SpanId: "span-client",
		Service: "gateway", KindString: "CLIENT", DurationNano: 30_000_000,
	}
	server := &spansschema.Row{
		TenantId: 7, TraceId: "trace-err", SpanId: "span-server", ParentSpanId: "span-client",
		Service: "orders", KindString: "SERVER", DurationNano: 20_000_000,
		StatusCodeString: "STATUS_CODE_ERROR",
	}

	if err := c.handle(context.Background(), []*kgo.Record{rec(t, client), rec(t, server)}); err != nil {
		t.Fatalf("handle: %v", err)
	}

	state := c.aggregator.Drain()
	key := EdgeKey{TenantId: 7, Client: "gateway", Server: "orders"}
	e, ok := state[key]
	if !ok {
		t.Fatalf("edge %+v not found; got %+v", key, state)
	}
	if e.Total != 1 || e.Failed != 1 {
		t.Errorf("counters: want total=1 failed=1, got total=%d failed=%d", e.Total, e.Failed)
	}

	if _, ok := edgeAttrs(key)["status.code"]; ok {
		t.Errorf("edge attrs must not carry status.code, got %+v", edgeAttrs(key))
	}
}

// An unpaired CLIENT span pointing at an uninstrumented peer must, on expiry,
// synthesize a virtual server edge tagged with the connection type.
func TestVirtualNodeOnExpiry(t *testing.T) {
	c := newTestConsumer()

	client := &spansschema.Row{
		TenantId: 7, TraceId: "trace-2", SpanId: "span-db",
		Service: "orders", KindString: "CLIENT", DurationNano: 12_000_000, // 12ms
		DbSystem: "postgresql",
	}
	if err := c.handle(context.Background(), []*kgo.Record{rec(t, client)}); err != nil {
		t.Fatalf("handle: %v", err)
	}

	// Nothing emitted until the span expires unpaired.
	if got := len(c.aggregator.Drain()); got != 0 {
		t.Fatalf("want 0 edges before expiry, got %d", got)
	}

	c.store.EvictExpired(time.Now().Add(pairingTTL+time.Second), c.onExpire)

	state := c.aggregator.Drain()
	key := EdgeKey{
		TenantId: 7, Client: "orders", Server: "postgresql",
		VirtualNode: virtualNodeServer, ConnectionType: connDatabase,
	}
	e, ok := state[key]
	if !ok {
		t.Fatalf("virtual edge %+v not found; got %+v", key, state)
	}
	if e.Server.Count != 1 || e.Server.Sum != 12 {
		t.Errorf("virtual server hist: want count=1 sum=12, got count=%d sum=%v", e.Server.Count, e.Server.Sum)
	}
	if e.Client.Count != 0 {
		t.Errorf("virtual edge should have no client histogram, got count=%d", e.Client.Count)
	}
	if e.Total != 1 || e.Failed != 0 {
		t.Errorf("virtual counters: want total=1 failed=0, got total=%d failed=%d", e.Total, e.Failed)
	}
}

func TestResolvePeerPriority(t *testing.T) {
	cases := []struct {
		name         string
		row          *spansschema.Row
		wantName     string
		wantConnType string
	}{
		{"peer.service wins", &spansschema.Row{PeerService: "payments", DbSystem: "mysql"}, "payments", ""},
		{"db.system", &spansschema.Row{DbSystem: "mysql"}, "mysql", connDatabase},
		{"db.name", &spansschema.Row{DbName: "orders_db"}, "orders_db", connDatabase},
		{"server.address", &spansschema.Row{Attributes: map[string]string{"server.address": "api.ext"}}, "api.ext", ""},
		{"messaging", &spansschema.Row{Attributes: map[string]string{"messaging.destination.name": "q.orders"}}, "q.orders", connMessaging},
		{"none", &spansschema.Row{}, "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			name, conn := resolvePeer(tc.row)
			if name != tc.wantName || conn != tc.wantConnType {
				t.Errorf("resolvePeer = (%q,%q), want (%q,%q)", name, conn, tc.wantName, tc.wantConnType)
			}
		})
	}
}

func TestExpiryCallbackRunsWithoutHoldingStoreLock(t *testing.T) {
	store := NewStore()
	key := spanKey{TraceID: "trace", SpanID: "span"}
	store.Add(key, CachedSpan{ExpiresAt: time.Now().Add(-time.Second)})

	done := make(chan struct{})
	go func() {
		store.EvictExpired(time.Now(), func(CachedSpan) {
			store.Add(spanKey{TraceID: "new", SpanID: "span"}, CachedSpan{})
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expiry callback was called while the store lock was held")
	}
}
