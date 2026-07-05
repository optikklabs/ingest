package servicegraph

import (
	"sync"
	"time"
)

type CachedSpan struct {
	TenantId   uint32
	Service    string
	IsClient   bool
	IsServer   bool
	StatusCode string
	Duration   float64
	ExpiresAt  time.Time
}

type Store struct {
	mu    sync.Mutex
	cache map[string]CachedSpan
}

func NewStore() *Store {
	return &Store{
		cache: make(map[string]CachedSpan),
	}
}

func (s *Store) Add(key string, span CachedSpan) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.cache) < 10000 {
		s.cache[key] = span
	}
}

func (s *Store) GetAndRemove(key string) (CachedSpan, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	span, ok := s.cache[key]
	if ok {
		delete(s.cache, key)
	}
	return span, ok
}

func (s *Store) EvictExpired(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range s.cache {
		if now.After(v.ExpiresAt) {
			delete(s.cache, k)
		}
	}
}
