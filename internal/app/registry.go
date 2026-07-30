package app

import (
	"net/http"

	"github.com/optikklabs/ingest/internal/auth"
	"google.golang.org/grpc"
)

type Module interface {
	RegisterGRPC(srv *grpc.Server)
}

type HTTPModule interface {
	RegisterOTLPHTTP(*http.ServeMux, auth.TeamResolver, *auth.TenantRateLimiter)
}
