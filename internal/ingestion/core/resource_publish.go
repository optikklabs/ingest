package core

import (
	"context"
	"log/slog"
	"time"
)

// resourcePublishTimeout bounds the best-effort resource publish so it
// never stalls the OTLP request path.
const resourcePublishTimeout = 5 * time.Second

// resourcePublisher is the subset of Producer used for resource rows.
type resourcePublisher[T Row] interface {
	Publish(ctx context.Context, rows []T) error
}

// PublishResources publishes deduped resource rows best-effort. On failure
// it evicts the freshly-cached keys so a later request re-emits the same
// resources, keeping the cache consistent with what reached Kafka.
func PublishResources[T Row](p resourcePublisher[T], cache *ResourceCache, keys []ResourceKey, rows []T, signal string) {
	if len(rows) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), resourcePublishTimeout)
	defer cancel()
	if err := p.Publish(ctx, rows); err != nil {
		for _, k := range keys {
			cache.Remove(k)
		}
		slog.WarnContext(ctx, "core: resource publish failed, cache rolled back",
			slog.String("signal", signal),
			slog.Int("rows", len(rows)),
			slog.Any("error", err),
		)
	}
}
