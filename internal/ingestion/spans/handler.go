package spans

import (
	"context"

	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/ingestionstats"
	"github.com/optikklabs/ingest/internal/ingestion/llmscores"
	llmscoresschema "github.com/optikklabs/ingest/internal/ingestion/llmscores/schema"
	"github.com/optikklabs/ingest/internal/ingestion/spans/schema"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

type Handler struct {
	tracepb.UnimplementedTraceServiceServer
	producer        *core.Producer[*schema.Row]
	scoresPublisher *core.AsyncPublisher[*llmscoresschema.ScoreRow]
	stats           ingestionstats.Recorder
}

func NewHandler(p *core.Producer[*schema.Row], sp *core.AsyncPublisher[*llmscoresschema.ScoreRow], stats ingestionstats.Recorder) *Handler {
	return &Handler{
		producer:        p,
		scoresPublisher: sp,
		stats:           stats,
	}
}

func (h *Handler) Export(ctx context.Context, req *tracepb.ExportTraceServiceRequest) (*tracepb.ExportTraceServiceResponse, error) {
	rows, usage, tenantID, err := core.ExportOTLP(ctx, "spans",
		func(tenantID int64) ([]*schema.Row, []ingestionstats.ResourceUsage) { return mapRequest(tenantID, req) },
		h.producer.Publish,
	)
	if err != nil {
		return nil, err
	}
	ingestionstats.EmitUsage(h.stats, tenantID, ingestionstats.SignalSpans, usage, req)

	if scores := llmscores.ExtractFromSpans(rows); len(scores) > 0 {
		h.scoresPublisher.Enqueue(scores, nil)
	}
	return &tracepb.ExportTraceServiceResponse{}, nil
}
