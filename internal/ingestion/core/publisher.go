package core

import "context"

type Publisher[T Row] interface {
	Publish(ctx context.Context, rows []T) error
}

var _ Publisher[Row] = (*Producer[Row])(nil)
