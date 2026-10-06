package database

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const (
	chConnMaxLifetime = 30 * time.Minute
	chDialTimeout     = 5 * time.Second
)

// OpenClickHouseConn connects with opts plus the shared compression and dial
// settings, and verifies the connection.
func OpenClickHouseConn(opts *clickhouse.Options) (clickhouse.Conn, error) {
	opts.Compression = &clickhouse.Compression{Method: clickhouse.CompressionLZ4}
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
		_ = conn.Close()
		return nil, err
	}

	return conn, nil
}
