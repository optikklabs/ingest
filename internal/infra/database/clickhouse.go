package database

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const (
	chConnMaxLifetime = 30 * time.Minute
	chDialTimeout     = 5 * time.Second
)

// OpenClickHouseConn opens the shared insert pool. Pool sizing is
// config-driven (clickhouse.max_open_conns / max_idle_conns): the writer
// path batches inserts, so it needs far fewer connections than a query pool.
func OpenClickHouseConn(dsn string, maxOpenConns, maxIdleConns int) (clickhouse.Conn, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: parse DSN: %w", err)
	}

	opts.Compression = &clickhouse.Compression{Method: clickhouse.CompressionLZ4}
	opts.MaxOpenConns = maxOpenConns
	opts.MaxIdleConns = maxIdleConns
	opts.ConnMaxLifetime = chConnMaxLifetime
	opts.DialTimeout = chDialTimeout
	opts.ConnOpenStrategy = clickhouse.ConnOpenRoundRobin

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, err
	}

	pingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := conn.Ping(pingCtx); err != nil {
		return nil, err
	}

	return conn, nil
}
