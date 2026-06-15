package logs

import (
	"github.com/optikklabs/ingest/internal/app/registry"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
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

func (m *Module) Name() string { return "logs" }

func (m *Module) RegisterGRPC(srv *grpc.Server) {
	logspb.RegisterLogsServiceServer(srv, m.handler)
}

var _ registry.Module = (*Module)(nil)
