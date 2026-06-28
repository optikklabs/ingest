package auth

import (
	"context"
	"errors"
	"testing"
)

type fakeFinder struct {
	calls int
	id    int64
	err   error
}

func (f *fakeFinder) FindTeamIDByAPIKey(_ context.Context, _ string) (int64, error) {
	f.calls++
	return f.id, f.err
}

// Genuine not-found is negative-cached: only one finder call across two lookups.
func TestResolveCachesInvalidKey(t *testing.T) {
	f := &fakeFinder{err: ErrInvalidAPIKey}
	a := &Authenticator{finder: f}

	for i := 0; i < 2; i++ {
		if _, err := a.ResolveTeamID(context.Background(), "k"); !errors.Is(err, ErrInvalidAPIKey) {
			t.Fatalf("want ErrInvalidAPIKey, got %v", err)
		}
	}
	if f.calls != 1 {
		t.Fatalf("invalid key not cached: %d finder calls, want 1", f.calls)
	}
}

// Transient errors must NOT be cached, or a blip locks the tenant out for the TTL.
func TestResolveDoesNotCacheTransientError(t *testing.T) {
	f := &fakeFinder{err: errors.New("connection refused")}
	a := &Authenticator{finder: f}

	for i := 0; i < 2; i++ {
		if _, err := a.ResolveTeamID(context.Background(), "k"); err == nil {
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

	_, _ = a.ResolveTeamID(context.Background(), "k")
	f.err, f.id = nil, 42
	id, err := a.ResolveTeamID(context.Background(), "k")
	if err != nil || id != 42 {
		t.Fatalf("canceled result was cached: got id=%d err=%v", id, err)
	}
}
