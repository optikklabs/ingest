package otlphttp

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/optikklabs/ingest/internal/auth"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

type fakeResolver struct{ err error }

func (r fakeResolver) ResolveTenantID(context.Context, string) (int64, error) { return 1, r.err }

func traceHandler(resolver auth.TeamResolver) http.Handler {
	return Export(resolver,
		func() *tracepb.ExportTraceServiceRequest { return &tracepb.ExportTraceServiceRequest{} },
		func(context.Context, *tracepb.ExportTraceServiceRequest) (*tracepb.ExportTraceServiceResponse, error) {
			return &tracepb.ExportTraceServiceResponse{}, nil
		},
	)
}

func TestResolverFailureMatchesGRPCServerError(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/traces", nil)
	req.Header.Set("x-api-key", "valid-key")
	res := httptest.NewRecorder()
	traceHandler(fakeResolver{err: errors.New("mysql unavailable")}).ServeHTTP(res, req)
	if res.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", res.Code, http.StatusInternalServerError)
	}
}

func TestInvalidAPIKeyIsUnauthenticated(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/traces", nil)
	req.Header.Set("x-api-key", "invalid")
	res := httptest.NewRecorder()
	traceHandler(fakeResolver{err: auth.ErrInvalidAPIKey}).ServeHTTP(res, req)
	if res.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", res.Code, http.StatusUnauthorized)
	}
}

func TestUnsupportedContentTypeIsRejected(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/traces", http.NoBody)
	req.Header.Set("x-api-key", "valid")
	req.Header.Set("Content-Type", "text/plain")
	res := httptest.NewRecorder()
	traceHandler(fakeResolver{}).ServeHTTP(res, req)
	if res.Code != http.StatusUnsupportedMediaType {
		t.Errorf("status = %d, want %d", res.Code, http.StatusUnsupportedMediaType)
	}
}
