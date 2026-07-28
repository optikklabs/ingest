package core

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/optikklabs/ingest/internal/infra/metrics"
)

type RowMapper[T Row] func(r T, dst []any)

type Writer[T Row] interface {
	Insert(ctx context.Context, rows []T) error
}

type permanentInsertError struct {
	err error
}

func (e *permanentInsertError) Error() string { return e.err.Error() }
func (e *permanentInsertError) Unwrap() error { return e.err }

type ClickHouseWriter[T Row] struct {
	ch        clickhouse.Conn
	query     string
	signal    string
	colCount  int
	rowMapper RowMapper[T]
}

func NewClickHouseWriter[T Row](ch clickhouse.Conn, table string, columns []string, rowMapper RowMapper[T]) *ClickHouseWriter[T] {

	signal := table
	if i := strings.LastIndex(table, "."); i >= 0 {
		signal = table[i+1:]
	}
	return &ClickHouseWriter[T]{
		ch:        ch,
		query:     "INSERT INTO " + table + " (" + strings.Join(columns, ", ") + ")",
		signal:    signal,
		colCount:  len(columns),
		rowMapper: rowMapper,
	}
}

func (w *ClickHouseWriter[T]) Insert(ctx context.Context, rows []T) error {
	if len(rows) == 0 {
		return nil
	}

	batch, err := w.ch.PrepareBatch(ctx, w.query)
	if err != nil {
		return fmt.Errorf("core writer: prepare: %w", err)
	}

	vals := make([]any, w.colCount)
	for _, r := range rows {
		w.rowMapper(r, vals)
		if err := batch.Append(vals...); err != nil {
			return &permanentInsertError{err: fmt.Errorf("core writer: append: %w", err)}
		}
	}
	sendStart := time.Now()
	if err := batch.Send(); err != nil {
		metrics.CHInsertDuration.WithLabelValues(w.signal, "err").Observe(time.Since(sendStart).Seconds())
		return fmt.Errorf("core writer: send: %w", err)
	}
	metrics.CHInsertDuration.WithLabelValues(w.signal, "ok").Observe(time.Since(sendStart).Seconds())
	return nil
}
