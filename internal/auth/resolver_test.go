package auth

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// blockingFinder holds the first lookup open until released, so concurrent
// callers pile up behind singleflight instead of racing to the DB.
type blockingFinder struct {
	calls   int64
	release chan struct{}
}

func (b *blockingFinder) FindTenantIDByAPIKey(_ context.Context, _ string) (int64, error) {
	atomic.AddInt64(&b.calls, 1)
	<-b.release
	return 7, nil
}

// Concurrent lookups for the same cold key collapse into one finder call.
func TestResolveCollapsesConcurrentLookups(t *testing.T) {
	f := &blockingFinder{release: make(chan struct{})}
	a := newAuthenticator(f, 0, 0, rate.NewLimiter(rate.Inf, 1))

	const n = 20
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if id, err := a.ResolveTenantID(context.Background(), "k"); err != nil || id != 7 {
				t.Errorf("got id=%d err=%v", id, err)
			}
		}()
	}
	time.Sleep(20 * time.Millisecond) // let goroutines reach the finder
	close(f.release)
	wg.Wait()

	if got := atomic.LoadInt64(&f.calls); got != 1 {
		t.Fatalf("stampede not collapsed: %d finder calls, want 1", got)
	}
}

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
	a := newAuthenticator(f, 0, 0, rate.NewLimiter(rate.Inf, 1))

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
	a := newAuthenticator(&fakeFinder{err: ErrInvalidAPIKey}, 0, 0, rate.NewLimiter(rate.Inf, 1))
	_, _ = a.ResolveTenantID(context.Background(), "bad")
	a.finder = &fakeFinder{id: 42}
	_, _ = a.ResolveTenantID(context.Background(), "good")

	for key, wantTTL := range map[string]time.Duration{"bad": negativeCacheTTL, "good": defaultCacheTTL} {
		val, ok := a.cache.Peek(apiKeyCacheKey(key))
		if !ok {
			t.Fatalf("%q not cached", key)
		}
		ttl := time.Until(val.expiresAt)
		if ttl > wantTTL || ttl < wantTTL-time.Second {
			t.Errorf("%q ttl = %v, want ~%v", key, ttl, wantTTL)
		}
	}
}

// Transient errors must NOT be cached, or a blip locks the tenant out for the TTL.
func TestResolveDoesNotCacheTransientError(t *testing.T) {
	f := &fakeFinder{err: errors.New("connection refused")}
	a := newAuthenticator(f, 0, 0, rate.NewLimiter(rate.Inf, 1))

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
	a := newAuthenticator(f, 0, 0, rate.NewLimiter(rate.Inf, 1))

	_, _ = a.ResolveTenantID(context.Background(), "k")
	f.err, f.id = nil, 42
	id, err := a.ResolveTenantID(context.Background(), "k")
	if err != nil || id != 42 {
		t.Fatalf("canceled result was cached: got id=%d err=%v", id, err)
	}
}

func TestAuthenticatorCacheIsBounded(t *testing.T) {
	f := &fakeFinder{id: 42}
	a := newAuthenticator(f, 0, 2, rate.NewLimiter(rate.Inf, 1))

	for _, key := range []string{"one", "two", "three"} {
		if _, err := a.ResolveTenantID(context.Background(), key); err != nil {
			t.Fatal(err)
		}
	}
	if got := a.cache.Len(); got != 2 {
		t.Fatalf("cache length = %d, want 2", got)
	}
	if _, ok := a.cache.Peek(apiKeyCacheKey("one")); ok {
		t.Fatal("least recently used key was not evicted")
	}
}

func TestAuthenticatorLimitsColdLookups(t *testing.T) {
	f := &fakeFinder{id: 42}
	a := newAuthenticator(f, 0, 10, rate.NewLimiter(0, 0))

	if _, err := a.ResolveTenantID(context.Background(), "cold"); !errors.Is(err, ErrAuthRateLimited) {
		t.Fatalf("error = %v, want ErrAuthRateLimited", err)
	}
	if f.calls != 0 {
		t.Fatalf("finder calls = %d, want 0", f.calls)
	}
}
