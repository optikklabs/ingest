package metrics

import (
	"github.com/optikklabs/ingest/internal/app/registry"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
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

func (m *Module) Name() string { return "metrics-ingest" }

func (m *Module) RegisterGRPC(srv *grpc.Server) {
	metricspb.RegisterMetricsServiceServer(srv, m.handler)
}

var _ registry.Module = (*Module)(nil)
