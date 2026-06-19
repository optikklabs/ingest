package core

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Writer defines how a batch of rows is inserted into the destination.
type Writer[T Row] interface {
	Insert(ctx context.Context, rows []T) error
}

// ClickHouseWriter batch-inserts generic Rows into ClickHouse using async inserts.
type ClickHouseWriter[T Row] struct {
	ch        clickhouse.Conn
	query     string
	rowMapper func(T) []any
}

func NewClickHouseWriter[T Row](ch clickhouse.Conn, table string, columns []string, rowMapper func(T) []any) *ClickHouseWriter[T] {
	return &ClickHouseWriter[T]{
		ch:        ch,
		query:     "INSERT INTO " + table + " (" + strings.Join(columns, ", ") + ")",
		rowMapper: rowMapper,
	}
}

func (w *ClickHouseWriter[T]) Insert(ctx context.Context, rows []T) error {
	if len(rows) == 0 {
		return nil
	}
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"async_insert":          uint8(1),
		"wait_for_async_insert": uint8(0),
	}))
	batch, err := w.ch.PrepareBatch(ctx, w.query)
	if err != nil {
		return fmt.Errorf("core writer: prepare: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(w.rowMapper(r)...); err != nil {
			return fmt.Errorf("core writer: append: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("core writer: send: %w", err)
	}
	return nil
}
