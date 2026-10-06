package app

import "net/http"

// health answers liveness and readiness probes. Dependencies are dialled
// during startup, so a serving process is ready by construction.
func (a *App) health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}` + "\n"))
}
