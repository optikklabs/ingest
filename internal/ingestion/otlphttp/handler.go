package otlphttp

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"sync"

	"github.com/optikklabs/ingest/internal/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const maxBodyBytes = 16 << 20

func Export[Req proto.Message, Resp proto.Message](resolver auth.TeamResolver, newReq func() Req, export func(context.Context, Req) (Resp, error)) http.HandlerFunc {
	pool := sync.Pool{
		New: func() any { return newReq() },
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		key := r.Header.Get("x-api-key")
		tenant, err := resolver.ResolveTenantID(r.Context(), key)
		if err != nil {
			if errors.Is(err, auth.ErrMissingAPIKey) || errors.Is(err, auth.ErrInvalidAPIKey) {
				http.Error(w, "unauthenticated", http.StatusUnauthorized)
			} else if errors.Is(err, auth.ErrAuthRateLimited) {
				w.Header().Set("Retry-After", "1")
				http.Error(w, "authentication rate limited", http.StatusTooManyRequests)
			} else {
				http.Error(w, "authentication service unavailable", http.StatusInternalServerError)
			}
			return
		}
		contentType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || (contentType != "application/x-protobuf" && contentType != "application/json") {
			http.Error(w, "unsupported OTLP content type", http.StatusUnsupportedMediaType)
			return
		}
		req := pool.Get().(Req)
		defer func() {
			proto.Reset(req)
			pool.Put(req)
		}()
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
		if err != nil {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		if contentType == "application/json" {
			err = protojson.Unmarshal(body, req)
		} else {
			err = proto.Unmarshal(body, req)
		}
		if err != nil {
			http.Error(w, "invalid OTLP request", http.StatusBadRequest)
			return
		}
		resp, err := export(auth.WithTenantID(r.Context(), tenant), req)
		if err != nil {
			writeError(w, err)
			return
		}
		if contentType == "application/x-protobuf" {
			b, _ := proto.Marshal(resp)
			w.Header().Set("Content-Type", "application/x-protobuf")
			_, _ = w.Write(b)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(protojson.Format(resp)))
	}
}

func writeError(w http.ResponseWriter, err error) {
	code := status.Code(err)
	httpCode := http.StatusInternalServerError
	switch code {
	case codes.Unauthenticated:
		httpCode = http.StatusUnauthorized
	case codes.InvalidArgument:
		httpCode = http.StatusBadRequest
	case codes.ResourceExhausted:
		httpCode = http.StatusTooManyRequests
	case codes.Unavailable:
		httpCode = http.StatusServiceUnavailable
	}
	http.Error(w, status.Convert(err).Message(), httpCode)
}
