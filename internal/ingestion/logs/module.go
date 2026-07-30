package logs

import (
	"net/http"

	"github.com/optikklabs/ingest/internal/auth"
	"github.com/optikklabs/ingest/internal/ingestion/otlphttp"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
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
	logspb.RegisterLogsServiceServer(srv, m.handler)
}

func (m *Module) RegisterOTLPHTTP(mux *http.ServeMux, resolver auth.TeamResolver, limiter *auth.TenantRateLimiter) {
	mux.Handle("/v1/logs", otlphttp.Export("logs", resolver, limiter, func() *logspb.ExportLogsServiceRequest { return &logspb.ExportLogsServiceRequest{} }, m.handler.Export))
}

