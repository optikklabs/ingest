package core

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
)

// PublishMetricPair publishes metadata and payloads concurrently. Both topics
// use the same thread-safe Kafka producer, so independent acknowledgement waits
// do not need to add to request latency.
func PublishMetricPair[S Row, M Row](ctx context.Context, series Publisher[S], seriesRows []S, metrics Publisher[M], metricRows []M) error {
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		if err := series.Publish(groupCtx, seriesRows); err != nil {
			return fmt.Errorf("metric series publish: %w", err)
		}
		return nil
	})
	group.Go(func() error {
		if err := metrics.Publish(groupCtx, metricRows); err != nil {
			return fmt.Errorf("metric payload publish: %w", err)
		}
		return nil
	})
	return group.Wait()
}
