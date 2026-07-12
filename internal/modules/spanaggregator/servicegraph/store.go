package servicegraph

import (
	"sync"
	"time"
)

// CachedSpan holds the fields of an unpaired span needed to build an edge once
// its peer arrives, or to synthesize a virtual node if it expires unpaired.
type CachedSpan struct {
	TenantId   uint32
	Service    string
	IsClient   bool
	IsServer   bool
	StatusCode string
	Duration   float64
	ExpiresAt  time.Time

	// Peer identity resolved at insert time for client spans, used to
	// synthesize a virtual server node if the span expires unpaired.
	PeerName       string
	ConnectionType string
}

// spanKey identifies a span by trace + span id. A comparable struct key avoids
// the per-span string format the map lookups would otherwise allocate.
type spanKey struct {
	TraceID string
	SpanID  string
}

// Store buffers unpaired spans keyed by trace+span id. Access is serialized by
// a single mutex so GetAndRemove is an atomic match-and-consume.
type Store struct {
	mu    sync.Mutex
	cache map[spanKey]CachedSpan
}

func NewStore() *Store {
	return &Store{
		cache: make(map[spanKey]CachedSpan),
	}
}

// Add buffers a span, dropping it when the store is at capacity.
func (s *Store) Add(key spanKey, span CachedSpan) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.cache) < maxPendingSpans {
		s.cache[key] = span
	}
}

// GetAndRemove atomically returns and deletes a buffered span, marking a
// successful pairing so it is never seen as an expiry.
func (s *Store) GetAndRemove(key spanKey) (CachedSpan, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	span, ok := s.cache[key]
	if ok {
		delete(s.cache, key)
	}
	return span, ok
}

// EvictExpired removes spans whose TTL has passed, invoking onExpire for each so
// callers can react to spans that never found a peer.
func (s *Store) EvictExpired(now time.Time, onExpire func(CachedSpan)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range s.cache {
		if now.After(v.ExpiresAt) {
			delete(s.cache, k)
			if onExpire != nil {
				onExpire(v)
			}
		}
	}
}
