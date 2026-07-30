package metrics

import (
	"net/http"

	"github.com/optikklabs/ingest/internal/auth"
	"github.com/optikklabs/ingest/internal/ingestion/otlphttp"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
)

type Deps struct {
	Handler *Handler
}

func NewModule(d Deps) *Module {
	return &Module{handler: d.Handler}
}

type Module struct {
	handler *Handler
}

func (m *Module) RegisterGRPC(srv *grpc.Server) {
	metricspb.RegisterMetricsServiceServer(srv, m.handler)
}

func (m *Module) RegisterOTLPHTTP(mux *http.ServeMux, resolver auth.TeamResolver, limiter *auth.TenantRateLimiter) {
	mux.Handle("/v1/metrics", otlphttp.Export("metrics", resolver, limiter, func() *metricspb.ExportMetricsServiceRequest { return &metricspb.ExportMetricsServiceRequest{} }, m.handler.Export))
}

