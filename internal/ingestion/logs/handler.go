// Package logs is the OTLP logs ingestion path. Same shape as ingestion/spans:
// handler → mapper → producer → consumer → writer, plus dlq + module.
package logs

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/optikklabs/ingest/internal/auth"
	"github.com/optikklabs/ingest/internal/infra/metrics"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/logs/schema"
	logsresourceschema "github.com/optikklabs/ingest/internal/ingestion/logsresource/schema"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Handler struct {
	logspb.UnimplementedLogsServiceServer
	producer         *core.Producer[*schema.Row]
	resourceProducer *core.Producer[*logsresourceschema.ResourceRow]
	resourceCache    *core.ResourceCache
}

func NewHandler(p *core.Producer[*schema.Row], rp *core.Producer[*logsresourceschema.ResourceRow], cache *core.ResourceCache) *Handler {
	return &Handler{
		producer:         p,
		resourceProducer: rp,
		resourceCache:    cache,
	}
}

func (h *Handler) Export(ctx context.Context, req *logspb.ExportLogsServiceRequest) (*logspb.ExportLogsServiceResponse, error) {
	teamID, ok := auth.TeamIDFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "team id missing from context")
	}
	mapStart := time.Now()
	rows := mapRequest(teamID, req)
	metrics.MapperDuration.WithLabelValues("logs").Observe(time.Since(mapStart).Seconds())
	metrics.MapperRowsPerRequest.WithLabelValues("logs").Observe(float64(len(rows)))
	if len(rows) == 0 {
		return &logspb.ExportLogsServiceResponse{}, nil
	}

	// Extract unique resources and filter using rolling cache
	var resourceRows []*logsresourceschema.ResourceRow
	for _, row := range rows {
		if row.GetFingerprint() == 0 {
			continue
		}
		key := fmt.Sprintf("%d:%d", row.GetTeamId(), row.GetFingerprint())
		if h.resourceCache.CheckAndUpdateBucket(key, row.GetTsBucket()) {
			resourceRows = append(resourceRows, &logsresourceschema.ResourceRow{
				TeamId:      row.GetTeamId(),
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

	if len(resourceRows) > 0 {
		go func() {
			publishCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := h.resourceProducer.Publish(publishCtx, resourceRows); err != nil {
				slog.WarnContext(publishCtx, "logs handler: failed to publish resources", slog.Any("error", err))
			}
		}()
	}

	pubStart := time.Now()
	if err := h.producer.Publish(ctx, rows); err != nil {
		metrics.HandlerPublishDuration.WithLabelValues("logs", "err").Observe(time.Since(pubStart).Seconds())
		slog.ErrorContext(ctx, "logs handler: publish failed", slog.Any("error", err))
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	metrics.HandlerPublishDuration.WithLabelValues("logs", "ok").Observe(time.Since(pubStart).Seconds())
	return &logspb.ExportLogsServiceResponse{}, nil
}

