package app

import (
	"context"
	"testing"
)

// A panicking probe must not wedge readiness: inFlight is cleared and later
// callers can still refresh.
func TestHealthGetRecoversFromPanic(t *testing.T) {
	h := newHealthCache()

	func() {
		defer func() { _ = recover() }()
		h.get(context.Background(), func(context.Context) *healthResult {
			panic("probe boom")
		})
	}()

	if h.inFlight {
		t.Fatal("inFlight stuck true after probe panic")
	}

	res := h.get(context.Background(), func(context.Context) *healthResult {
		return &healthResult{ready: true}
	})
	if res == nil || !res.ready {
		t.Fatal("readiness refresh did not recover after a prior probe panic")
	}
}
