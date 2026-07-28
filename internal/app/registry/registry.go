package registry

import (
	"net/http"

	"github.com/optikklabs/ingest/internal/auth"
	"google.golang.org/grpc"
)

type Module interface {
	Name() string
	RegisterGRPC(srv *grpc.Server)
}

type HTTPModule interface {
	RegisterOTLPHTTP(*http.ServeMux, auth.TeamResolver)
}
