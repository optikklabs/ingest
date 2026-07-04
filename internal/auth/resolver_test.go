package auth

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeFinder struct {
	calls int
	id    int64
	err   error
}

func (f *fakeFinder) FindTenantIDByAPIKey(_ context.Context, _ string) (int64, error) {
	f.calls++
	return f.id, f.err
}

// Genuine not-found is negative-cached: only one finder call across two lookups.
func TestResolveCachesInvalidKey(t *testing.T) {
	f := &fakeFinder{err: ErrInvalidAPIKey}
	a := &Authenticator{finder: f}

	for i := 0; i < 2; i++ {
		if _, err := a.ResolveTenantID(context.Background(), "k"); !errors.Is(err, ErrInvalidAPIKey) {
			t.Fatalf("want ErrInvalidAPIKey, got %v", err)
		}
	}
	if f.calls != 1 {
		t.Fatalf("invalid key not cached: %d finder calls, want 1", f.calls)
	}
}

// Not-found uses the short TTL so a key tried before its team exists recovers
// quickly; a valid key keeps the long TTL.
func TestNegativeCacheUsesShortTTL(t *testing.T) {
	a := &Authenticator{finder: &fakeFinder{err: ErrInvalidAPIKey}}
	_, _ = a.ResolveTenantID(context.Background(), "bad")
	a.finder = &fakeFinder{id: 42}
	_, _ = a.ResolveTenantID(context.Background(), "good")

	for key, wantTTL := range map[string]time.Duration{"bad": negativeCacheTTL, "good": cacheTTL} {
		val, ok := a.cache.Load(apiKeyCacheKey(key))
		if !ok {
			t.Fatalf("%q not cached", key)
		}
		ttl := time.Until(val.(cacheEntry).expiresAt)
		if ttl > wantTTL || ttl < wantTTL-time.Second {
			t.Errorf("%q ttl = %v, want ~%v", key, ttl, wantTTL)
		}
	}
}

// Transient errors must NOT be cached, or a blip locks the tenant out for the TTL.
func TestResolveDoesNotCacheTransientError(t *testing.T) {
	f := &fakeFinder{err: errors.New("connection refused")}
	a := &Authenticator{finder: f}

	for i := 0; i < 2; i++ {
		if _, err := a.ResolveTenantID(context.Background(), "k"); err == nil {
			t.Fatal("expected error")
		}
	}
	if f.calls != 2 {
		t.Fatalf("transient error was cached: %d finder calls, want 2", f.calls)
	}
}

// A canceled request must not poison the cache for later healthy callers.
func TestResolveDoesNotCacheContextCanceled(t *testing.T) {
	f := &fakeFinder{err: context.Canceled}
	a := &Authenticator{finder: f}

	_, _ = a.ResolveTenantID(context.Background(), "k")
	f.err, f.id = nil, 42
	id, err := a.ResolveTenantID(context.Background(), "k")
	if err != nil || id != 42 {
		t.Fatalf("canceled result was cached: got id=%d err=%v", id, err)
	}
}
