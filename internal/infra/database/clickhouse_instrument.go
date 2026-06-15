package database

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/optikklabs/ingest/internal/infra/metrics"
)

func SelectCH(ctx context.Context, conn clickhouse.Conn, op string, dest any, query string, args ...any) error {
	done := startCHOp(ctx)
	start := time.Now()
	err := conn.Select(ctx, dest, query, args...)
	done(err, start, op)
	return err
}

func QueryRowCH(ctx context.Context, conn clickhouse.Conn, op string, dest any, query string, args ...any) error {
	done := startCHOp(ctx)
	start := time.Now()
	err := conn.QueryRow(ctx, query, args...).ScanStruct(dest)
	if err != nil && isNoRows(err) {
		done(nil, start, op)
		return nil
	}
	done(err, start, op)
	return err
}

func isNoRows(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sql.ErrNoRows) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "no rows") || strings.Contains(msg, "EOF")
}

func startCHOp(ctx context.Context) func(error, time.Time, string) {
	return func(err error, start time.Time, op string) {
		dur := time.Since(start).Seconds()
		metrics.DBQueryDuration.WithLabelValues("clickhouse", op).Observe(dur)
		metrics.DBQueriesTotal.WithLabelValues("clickhouse", op, resultLabel(err)).Inc()
		if err != nil {
			slog.ErrorContext(ctx, "clickhouse query failed",
				slog.String("op", op),
				slog.Float64("duration_s", dur),
				slog.Any("error", err),
			)
		}
	}
}
