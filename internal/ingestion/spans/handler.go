// Package spans provides the OTLP spans ingestion path: gRPC handler to
// Kafka producer.
package spans

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/optikklabs/ingest/internal/auth"
	"github.com/optikklabs/ingest/internal/infra/metrics"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/spans/schema"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Handler struct {
	tracepb.UnimplementedTraceServiceServer
	producer         *core.Producer[*schema.Row]
	resourceProducer *core.Producer[*schema.Row]
	resourceCache    *core.ResourceCache
}

func NewHandler(p *core.Producer[*schema.Row], rp *core.Producer[*schema.Row], cache *core.ResourceCache) *Handler {
	return &Handler{
		producer:         p,
		resourceProducer: rp,
		resourceCache:    cache,
	}
}

func (h *Handler) Export(ctx context.Context, req *tracepb.ExportTraceServiceRequest) (*tracepb.ExportTraceServiceResponse, error) {
	teamID, ok := auth.TeamIDFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "team id missing from context")
	}
	mapStart := time.Now()
	rows := mapRequest(teamID, req)
	metrics.MapperDuration.WithLabelValues("spans").Observe(time.Since(mapStart).Seconds())
	metrics.MapperRowsPerRequest.WithLabelValues("spans").Observe(float64(len(rows)))
	if len(rows) == 0 {
		return &tracepb.ExportTraceServiceResponse{}, nil
	}

	// Extract unique resources and filter using LRU cache
	var resourceRows []*schema.Row
	for _, row := range rows {
		if row.GetFingerprint() == 0 {
			continue
		}
		key := fmt.Sprintf("%d:%d", row.GetTeamId(), row.GetFingerprint())
		if h.resourceCache.Add(key) {
			resourceRows = append(resourceRows, &schema.Row{
				TeamId:      row.GetTeamId(),
				Fingerprint: row.GetFingerprint(),
				Service:     row.GetService(),
				Host:        row.GetHost(),
				Pod:         row.GetPod(),
				Environment: row.GetEnvironment(),
			})
		}
	}

	if len(resourceRows) > 0 {
		go func() {
			publishCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := h.resourceProducer.Publish(publishCtx, resourceRows); err != nil {
				slog.WarnContext(publishCtx, "spans handler: failed to publish resources", slog.Any("error", err))
			}
		}()
	}

	pubStart := time.Now()
	if err := h.producer.Publish(ctx, rows); err != nil {
		metrics.HandlerPublishDuration.WithLabelValues("spans", "err").Observe(time.Since(pubStart).Seconds())
		slog.ErrorContext(ctx, "spans handler: publish failed", slog.Any("error", err))
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	metrics.HandlerPublishDuration.WithLabelValues("spans", "ok").Observe(time.Since(pubStart).Seconds())
	return &tracepb.ExportTraceServiceResponse{}, nil
}
