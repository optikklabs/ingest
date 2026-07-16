package core

import (
	"context"
	"fmt"
)

// PublishMetricPair publishes metadata before payloads and gives every caller
// identical error semantics.
func PublishMetricPair[S Row, M Row](ctx context.Context, series Publisher[S], seriesRows []S, metrics Publisher[M], metricRows []M) error {
	if err := series.Publish(ctx, seriesRows); err != nil {
		return fmt.Errorf("metric series publish: %w", err)
	}
	if err := metrics.Publish(ctx, metricRows); err != nil {
		return fmt.Errorf("metric payload publish: %w", err)
	}
	return nil
}
