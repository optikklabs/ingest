package registry

import (
	"net/http"

	"github.com/optikklabs/ingest/internal/auth"
	"google.golang.org/grpc"
)

// Module is the interface every ingest signal module implements.
type Module interface {
	Name() string
	RegisterGRPC(srv *grpc.Server)
}

// HTTPModule is optional because only OTLP signal modules expose HTTP.
type HTTPModule interface {
	RegisterOTLPHTTP(*http.ServeMux, auth.TeamResolver)
}
