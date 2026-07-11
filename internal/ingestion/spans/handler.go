package spans

import (
	"context"
	"log/slog"
	"time"

	"github.com/optikklabs/ingest/internal/auth"
	"github.com/optikklabs/ingest/internal/infra/metrics"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/ingestionstats"
	"github.com/optikklabs/ingest/internal/ingestion/spans/schema"
	spansresourceschema "github.com/optikklabs/ingest/internal/ingestion/spansresource/schema"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Handler struct {
	tracepb.UnimplementedTraceServiceServer
	producer           *core.Producer[*schema.Row]
	tracegraphProducer *core.Producer[*schema.Row]
	resourceProducer   *core.Producer[*spansresourceschema.ResourceRow]
	resourceCache      *core.ResourceCache
	stats              ingestionstats.Recorder
}

func NewHandler(p *core.Producer[*schema.Row], tp *core.Producer[*schema.Row], rp *core.Producer[*spansresourceschema.ResourceRow], cache *core.ResourceCache, stats ingestionstats.Recorder) *Handler {
	return &Handler{
		producer:           p,
		tracegraphProducer: tp,
		resourceProducer:   rp,
		resourceCache:      cache,
		stats:              stats,
	}
}

func (h *Handler) Export(ctx context.Context, req *tracepb.ExportTraceServiceRequest) (*tracepb.ExportTraceServiceResponse, error) {
	tenantID, ok := auth.TenantIDFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "team id missing from context")
	}
	mapStart := time.Now()
	rows := mapRequest(tenantID, req)
	metrics.MapperDuration.WithLabelValues("spans").Observe(time.Since(mapStart).Seconds())
	metrics.MapperRowsPerRequest.WithLabelValues("spans").Observe(float64(len(rows)))
	if len(rows) == 0 {
		return &tracepb.ExportTraceServiceResponse{}, nil
	}

	// Meter usage best-effort; never blocks or fails ingestion.
	ingestionstats.Emit(h.stats, statRows(uint32(tenantID), req))

	// Re-publish active resources once per day so the resource TTL stays aligned
	// with the raw-span retention window.
	var resourceRows []*spansresourceschema.ResourceRow
	var newKeys []core.ResourceKey
	resourceDay := uint32(time.Now().Unix() / 86_400)
	for _, row := range rows {
		if row.GetFingerprint() == 0 {
			continue
		}
		key := core.ResourceKey{TenantID: row.GetTenantId(), Fingerprint: row.GetFingerprint()}
		if h.resourceCache.CheckAndUpdateBucket(key, resourceDay) {
			newKeys = append(newKeys, key)
			resourceRows = append(resourceRows, &spansresourceschema.ResourceRow{
				TenantId:    row.GetTenantId(),
				Fingerprint: row.GetFingerprint(),
				Service:     row.GetService(),
				Host:        row.GetHost(),
				Pod:         row.GetPod(),
				Environment: row.GetEnvironment(),
			})
		}
	}
	core.PublishResources(h.resourceProducer, h.resourceCache, newKeys, resourceRows, "spans")

	pubStart := time.Now()
	if err := h.producer.Publish(ctx, rows); err != nil {
		metrics.HandlerPublishDuration.WithLabelValues("spans", "err").Observe(time.Since(pubStart).Seconds())
		slog.ErrorContext(ctx, "spans handler: publish failed", slog.Any("error", err))
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	metrics.HandlerPublishDuration.WithLabelValues("spans", "ok").Observe(time.Since(pubStart).Seconds())

	tgRows := tracegraphRows(rows)
	metrics.TracegraphRowsPublished.Add(float64(len(tgRows)))
	metrics.TracegraphRowsFiltered.Add(float64(len(rows) - len(tgRows)))
	if len(tgRows) > 0 {
		publishCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := h.tracegraphProducer.Publish(publishCtx, tgRows); err != nil {
			slog.WarnContext(publishCtx, "spans handler: failed to publish to tracegraph", slog.Any("error", err))
		}
		cancel()
	}

	return &tracepb.ExportTraceServiceResponse{}, nil
}

// tracegraphRows keeps only spans that can form a service-graph edge,
// mirroring the servicegraph pairer's kind filter so INTERNAL spans never
// ship over the tracegraph topic.
func tracegraphRows(rows []*schema.Row) []*schema.Row {
	out := make([]*schema.Row, 0, len(rows))
	for _, row := range rows {
		switch row.GetKindString() {
		case "CLIENT", "SERVER", "PRODUCER", "CONSUMER":
			out = append(out, row)
		}
	}
	return out
}
