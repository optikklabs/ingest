package logs

import (
	"context"
	"log/slog"
	"time"

	"github.com/optikklabs/ingest/internal/auth"
	"github.com/optikklabs/ingest/internal/infra/metrics"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/ingestionstats"
	"github.com/optikklabs/ingest/internal/ingestion/logs/schema"
	logsresourceschema "github.com/optikklabs/ingest/internal/ingestion/logsresource/schema"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Handler struct {
	logspb.UnimplementedLogsServiceServer
	producer          *core.Producer[*schema.Row]
	resourcePublisher *core.AsyncPublisher[*logsresourceschema.ResourceRow]
	resourceCache     *core.ResourceCache
	stats             ingestionstats.Recorder
}

func NewHandler(p *core.Producer[*schema.Row], rp *core.AsyncPublisher[*logsresourceschema.ResourceRow], cache *core.ResourceCache, stats ingestionstats.Recorder) *Handler {
	return &Handler{
		producer:          p,
		resourcePublisher: rp,
		resourceCache:     cache,
		stats:             stats,
	}
}

func (h *Handler) Export(ctx context.Context, req *logspb.ExportLogsServiceRequest) (*logspb.ExportLogsServiceResponse, error) {
	tenantID, ok := auth.TenantIDFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "team id missing from context")
	}
	mapStart := time.Now()
	rows := mapRequest(tenantID, req)
	metrics.MapperDuration.WithLabelValues("logs").Observe(time.Since(mapStart).Seconds())
	metrics.MapperRowsPerRequest.WithLabelValues("logs").Observe(float64(len(rows)))
	if len(rows) == 0 {
		return &logspb.ExportLogsServiceResponse{}, nil
	}

	// Extract unique resources and filter using rolling cache
	var resourceRows []*logsresourceschema.ResourceRow
	var newKeys []core.ResourceKey
	for _, row := range rows {
		if row.GetFingerprint() == 0 {
			continue
		}
		key := core.ResourceKey{TenantID: row.GetTenantId(), Fingerprint: row.GetFingerprint()}
		if h.resourceCache.CheckAndUpdateBucket(key, row.GetTsBucket()) {
			newKeys = append(newKeys, key)
			resourceRows = append(resourceRows, &logsresourceschema.ResourceRow{
				TenantId:    row.GetTenantId(),
				Fingerprint: row.GetFingerprint(),
				TsBucket:    row.GetTsBucket(),
				Service:     row.GetService(),
				Host:        row.GetHost(),
				Pod:         row.GetPod(),
				Container:   row.GetContainer(),
				Environment: row.GetEnvironment(),
			})
		}
	}
	// Resource re-publish is best-effort and off the request path. Roll the
	// cache key back only if the async publish is dropped or fails.
	h.resourcePublisher.Enqueue(resourceRows, func() {
		for _, k := range newKeys {
			h.resourceCache.Remove(k)
		}
	})

	pubStart := time.Now()
	if err := h.producer.Publish(ctx, rows); err != nil {
		metrics.HandlerPublishDuration.WithLabelValues("logs", "err").Observe(time.Since(pubStart).Seconds())
		slog.ErrorContext(ctx, "logs handler: publish failed", slog.Any("error", err))
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	metrics.HandlerPublishDuration.WithLabelValues("logs", "ok").Observe(time.Since(pubStart).Seconds())
	ingestionstats.Emit(h.stats, statRows(uint32(tenantID), req))
	return &logspb.ExportLogsServiceResponse{}, nil
}
