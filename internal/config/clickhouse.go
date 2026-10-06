package config

import (
	"crypto/tls"
	"net"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type ClickHouseConfig struct {
	Host         string `yaml:"host"`
	Port         string `yaml:"port"`
	Database     string `yaml:"database"`
	User         string `yaml:"user"`
	Password     string `yaml:"password"`
	Secure       bool   `yaml:"secure"`
	MaxOpenConns int    `yaml:"max_open_conns"`
	MaxIdleConns int    `yaml:"max_idle_conns"`
}

func (c Config) ClickHouseMaxOpenConns() int {
	if n := c.ClickHouse.MaxOpenConns; n > 0 {
		return n
	}
	return 5
}

func (c Config) ClickHouseMaxIdleConns() int {
	if n := c.ClickHouse.MaxIdleConns; n > 0 {
		return n
	}
	return 3
}

// ClickHouseOptions builds the client settings for the configured server.
func (c Config) ClickHouseOptions() *clickhouse.Options {
	opts := &clickhouse.Options{
		Addr: []string{net.JoinHostPort(c.ClickHouse.Host, c.ClickHouse.Port)},
		Auth: clickhouse.Auth{
			Database: c.ClickHouse.Database,
			Username: c.ClickHouse.User,
			Password: c.ClickHouse.Password,
		},
		MaxOpenConns: c.ClickHouseMaxOpenConns(),
		MaxIdleConns: c.ClickHouseMaxIdleConns(),
	}
	if c.ClickHouse.Secure {
		opts.TLS = &tls.Config{}
	}
	return opts
}
