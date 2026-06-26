package core

import "context"

// Publisher defines how a batch of rows is published to the ingestion queue.
type Publisher[T Row] interface {
	Publish(ctx context.Context, rows []T) error
}

var _ Publisher[Row] = (*Producer[Row])(nil)
