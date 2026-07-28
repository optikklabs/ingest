package logs

import (
	"context"

	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/ingestionstats"
	"github.com/optikklabs/ingest/internal/ingestion/logs/schema"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
)

type Handler struct {
	logspb.UnimplementedLogsServiceServer
	producer *core.Producer[*schema.Row]
	stats    ingestionstats.Recorder
}

func NewHandler(p *core.Producer[*schema.Row], stats ingestionstats.Recorder) *Handler {
	return &Handler{
		producer: p,
		stats:    stats,
	}
}

func (h *Handler) Export(ctx context.Context, req *logspb.ExportLogsServiceRequest) (*logspb.ExportLogsServiceResponse, error) {
	_, usage, tenantID, err := core.ExportOTLP(ctx, "logs",
		func(tenantID int64) ([]*schema.Row, []ingestionstats.ResourceUsage) { return mapRequest(tenantID, req) },
		h.producer.Publish,
	)
	if err != nil {
		return nil, err
	}
	ingestionstats.EmitUsage(h.stats, tenantID, ingestionstats.SignalLogs, usage, req)
	return &logspb.ExportLogsServiceResponse{}, nil
}
