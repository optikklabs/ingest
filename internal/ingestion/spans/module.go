package spans

import (
	"github.com/optikklabs/ingest/internal/app/registry"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

// Deps is everything NewModule needs. The handler is constructed by
// app/ingest.go and handed in fully wired.
type Deps struct {
	Handler *Handler
}

func NewModule(d Deps) registry.Module {
	return &Module{handler: d.Handler}
}

type Module struct {
	handler *Handler
}

func (m *Module) Name() string { return "spans" }

func (m *Module) RegisterGRPC(srv *grpc.Server) {
	tracepb.RegisterTraceServiceServer(srv, m.handler)
}

var _ registry.Module = (*Module)(nil)
