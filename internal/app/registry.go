package app

import (
	"net/http"

	"google.golang.org/grpc"

	"github.com/optikklabs/ingest/internal/auth"
)

type Module interface {
	RegisterGRPC(srv *grpc.Server)
}

type HTTPModule interface {
	RegisterOTLPHTTP(*http.ServeMux, auth.TenantResolver)
}
