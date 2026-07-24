package spans

import (
	"context"
	"log/slog"
	"time"

	"github.com/optikklabs/ingest/internal/auth"
	"github.com/optikklabs/ingest/internal/infra/metrics"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/ingestionstats"
	"github.com/optikklabs/ingest/internal/ingestion/llmscores"
	llmscoresschema "github.com/optikklabs/ingest/internal/ingestion/llmscores/schema"
	"github.com/optikklabs/ingest/internal/ingestion/spans/schema"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Handler struct {
	tracepb.UnimplementedTraceServiceServer
	producer            *core.Producer[*schema.Row]
	tracegraphPublisher *core.AsyncPublisher[*schema.Row]
	scoresPublisher     *core.AsyncPublisher[*llmscoresschema.ScoreRow]
	stats               ingestionstats.Recorder
}

func NewHandler(p *core.Producer[*schema.Row], tp *core.AsyncPublisher[*schema.Row], sp *core.AsyncPublisher[*llmscoresschema.ScoreRow], stats ingestionstats.Recorder) *Handler {
	return &Handler{
		producer:            p,
		tracegraphPublisher: tp,
		scoresPublisher:     sp,
		stats:               stats,
	}
}

func (h *Handler) Export(ctx context.Context, req *tracepb.ExportTraceServiceRequest) (*tracepb.ExportTraceServiceResponse, error) {
	tenantID, ok := auth.TenantIDFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "team id missing from context")
	}
	if !auth.RateLimiter.Allow(tenantID) {
		metrics.OTLPRateLimitedTotal.WithLabelValues("spans").Inc()
		return nil, status.Error(codes.ResourceExhausted, "tenant rate limit exceeded")
	}
	mapStart := time.Now()
	rows := mapRequest(tenantID, req)
	metrics.MapperDuration.WithLabelValues("spans").Observe(time.Since(mapStart).Seconds())
	metrics.MapperRowsPerRequest.WithLabelValues("spans").Observe(float64(len(rows)))
	if len(rows) == 0 {
		return &tracepb.ExportTraceServiceResponse{}, nil
	}

	pubStart := time.Now()
	if err := h.producer.Publish(ctx, rows); err != nil {
		metrics.HandlerPublishDuration.WithLabelValues("spans", "err").Observe(time.Since(pubStart).Seconds())
		slog.ErrorContext(ctx, "spans handler: publish failed", slog.Any("error", err))
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	metrics.HandlerPublishDuration.WithLabelValues("spans", "ok").Observe(time.Since(pubStart).Seconds())
	ingestionstats.Emit(h.stats, statRows(uint32(tenantID), req))

	tgRows := tracegraphRows(rows)
	metrics.TracegraphRowsPublished.Add(float64(len(tgRows)))
	metrics.TracegraphRowsFiltered.Add(float64(len(rows) - len(tgRows)))
	// Tracegraph is best-effort with no cache state, so no rollback hook.
	h.tracegraphPublisher.Enqueue(tgRows, nil)

	// Evaluation scores carried on span events, off the request path.
	if scores := llmscores.ExtractFromSpans(rows); len(scores) > 0 {
		h.scoresPublisher.Enqueue(scores, nil)
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
