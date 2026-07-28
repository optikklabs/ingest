package spans

import (
	"net/http"

	"github.com/optikklabs/ingest/internal/app/registry"
	"github.com/optikklabs/ingest/internal/auth"
	"github.com/optikklabs/ingest/internal/ingestion/otlphttp"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

type Deps struct {
	Handler *Handler
}

func NewModule(d Deps) registry.Module {
	return &Module{handler: d.Handler}
}

type Module struct {
	handler *Handler
}

func (m *Module) RegisterGRPC(srv *grpc.Server) {
	tracepb.RegisterTraceServiceServer(srv, m.handler)
}

func (m *Module) RegisterOTLPHTTP(mux *http.ServeMux, resolver auth.TeamResolver, limiter *auth.TenantRateLimiter) {
	mux.Handle("/v1/traces", otlphttp.Export("spans", resolver, limiter, func() *tracepb.ExportTraceServiceRequest { return &tracepb.ExportTraceServiceRequest{} }, m.handler.Export))
}

var _ registry.Module = (*Module)(nil)
