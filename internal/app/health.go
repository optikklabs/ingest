package app

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

const healthCacheTTL = 5 * time.Second

type healthResult struct {
	ready           bool
	mysqlReady      bool
	tenantTableOK   bool
	clickhouseReady bool
	expiresAt       time.Time
}

type healthCache struct {
	mu       sync.Mutex
	current  *healthResult
	inFlight bool
	cond     *sync.Cond
}

func newHealthCache() *healthCache {
	hc := &healthCache{}
	hc.cond = sync.NewCond(&hc.mu)
	return hc
}

var readyCache = newHealthCache()

func (h *healthCache) get(ctx context.Context, probe func(ctx context.Context) *healthResult) *healthResult {
	h.mu.Lock()
	if h.current != nil && time.Now().Before(h.current.expiresAt) {
		res := h.current
		h.mu.Unlock()
		return res
	}
	for h.inFlight {
		h.cond.Wait()
		if h.current != nil && time.Now().Before(h.current.expiresAt) {
			res := h.current
			h.mu.Unlock()
			return res
		}
	}
	h.inFlight = true
	h.mu.Unlock()

	var res *healthResult
	defer func() {
		h.mu.Lock()
		if res != nil {
			res.expiresAt = time.Now().Add(healthCacheTTL)
			h.current = res
		}
		h.inFlight = false
		h.cond.Broadcast()
		h.mu.Unlock()
	}()

	res = probe(ctx)
	return res
}

func (a *App) healthLive(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *App) healthReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	res := readyCache.get(ctx, a.probeReady)

	if !res.ready {
		payload := map[string]string{"status": "not_ready"}
		if !res.mysqlReady {
			payload["mysql"] = "error"
		}
		if res.mysqlReady && !res.tenantTableOK {
			payload["tenant_table"] = "error"
		}
		if !res.clickhouseReady {
			payload["clickhouse"] = "error"
		}
		writeJSON(w, http.StatusServiceUnavailable, payload)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ready", "mysql": "ok", "clickhouse": "ok"})
}

func (a *App) probeReady(ctx context.Context) *healthResult {
	res := &healthResult{}
	if err := a.Infra.DB.PingContext(ctx); err != nil {
		slog.ErrorContext(ctx, "health check failed", slog.String("service", "mysql"), slog.Any("error", err))
		return res
	}
	res.mysqlReady = true
	// Cross-repo seam: query owns the tenant table ingest auth reads. A
	// query-side schema change must flip readiness, not fail silently.
	if err := a.Infra.AuthRepo.ProbeSchema(ctx); err != nil {
		slog.ErrorContext(ctx, "health check failed", slog.String("service", "tenant_table"), slog.Any("error", err))
		return res
	}
	res.tenantTableOK = true
	if err := a.Infra.CH.Ping(ctx); err != nil {
		slog.ErrorContext(ctx, "health check failed", slog.String("service", "clickhouse"), slog.Any("error", err))
		return res
	}
	res.clickhouseReady = true
	res.ready = true
	return res
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
