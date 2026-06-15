package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	AuthDenied = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "auth",
		Name:      "denied_total",
		Help:      "Authentication/authorisation denials by reason (unauthorized, missing_team, forbidden_team, forbidden_role).",
	}, []string{"reason"})

	AuthAuthenticated = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "auth",
		Name:      "authenticated_total",
		Help:      "Successful auth resolutions (tenant attached to the request).",
	})
)
