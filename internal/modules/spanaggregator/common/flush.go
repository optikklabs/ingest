package common

import (
	"context"
	"time"
)

// RunPeriodic invokes flush on a fixed cadence until ctx is cancelled. It is
// deliberately independent of Kafka polling so quiet streams still publish.
func RunPeriodic(ctx context.Context, interval time.Duration, flush func(context.Context)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			flush(ctx)
		}
	}
}
