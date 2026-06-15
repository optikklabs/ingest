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
	ready     bool
	mysqlErr  string
	chErr     string
	expiresAt time.Time
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

// get returns a cached or freshly-computed health snapshot. Concurrent
// callers with an expired cache share a single refresh via Cond.
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

	res := probe(ctx)
	res.expiresAt = time.Now().Add(healthCacheTTL)

	h.mu.Lock()
	h.current = res
	h.inFlight = false
	h.cond.Broadcast()
	h.mu.Unlock()
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
		if res.mysqlErr != "" {
			payload["mysql"] = res.mysqlErr
		}
		if res.chErr != "" {
			payload["clickhouse"] = res.chErr
		}
		writeJSON(w, http.StatusServiceUnavailable, payload)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ready", "mysql": "ok", "clickhouse": "ok"})
}

func (a *App) probeReady(ctx context.Context) *healthResult {
	res := &healthResult{}
	if err := a.Infra.DB.Ping(); err != nil {
		res.mysqlErr = err.Error()
		return res
	}
	if err := a.Infra.CH.Ping(ctx); err != nil {
		slog.ErrorContext(ctx, "health check failed", slog.String("service", "clickhouse"), slog.String("error", err.Error()))
		res.chErr = err.Error()
		return res
	}
	res.ready = true
	return res
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
