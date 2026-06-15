package registry

import "google.golang.org/grpc"

// Module is the interface every ingest signal module implements. Ingest modules
// serve OTLP over gRPC only — there is no HTTP API surface.
type Module interface {
	Name() string
	RegisterGRPC(srv *grpc.Server)
}
