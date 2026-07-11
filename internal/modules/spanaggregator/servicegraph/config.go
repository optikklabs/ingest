package servicegraph

import "time"

// Timing and capacity knobs for the service-graph pipeline. Kept in one place
// so the pairing, flushing, and store concerns share a single source of truth.
const (
	// pairingTTL bounds how long an unpaired span waits for its peer. It must
	// cover async messaging: produce->consume lag plus each service's
	// BatchSpanProcessor export delay (~5s per side), which reliably exceeds a
	// synchronous window.
	pairingTTL = 15 * time.Second

	// flushInterval is how often aggregated edges are published and the pair
	// store is swept for expired spans. Driving this from a timer (not Kafka
	// batch arrival) keeps low-traffic edges fresh and expires unpaired spans
	// on schedule.
	flushInterval = 10 * time.Second

	// maxPendingSpans caps the pair store so a trace storm cannot grow it
	// unbounded. Spans beyond the cap are dropped rather than admitted.
	maxPendingSpans = 10000
)
